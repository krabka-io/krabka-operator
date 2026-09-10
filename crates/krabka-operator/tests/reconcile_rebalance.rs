//! Reconcile-level tests for the `KafkaRebalance` controller.
//!
//! These tests drive the annotation-driven state machine of the controller
//! against a faked `crabka-rebalancer`, the `FakeRebalancerClient`. They
//! assert on the Connect-RPC sequence, and on the status patches and the
//! annotation patches on the kube side.

use std::{collections::BTreeMap, sync::Arc};

use assert2::{assert, check};
use http::{Method, Request};
use hyper::body::Bytes;
use krabka_operator::{
    controller::rebalance::reconcile,
    crd::{
        KafkaCondition, KafkaRebalance, KafkaRebalanceMode, KafkaRebalanceSpec,
        KafkaRebalanceStatus, RebalancerAuthorizationSecretRef,
    },
    rebalancer_client::{ConnectRebalancerClient, ProposalStatus},
};
use krabka_units::{mebibytes_per_sec, secs};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[path = "shared/mod.rs"]
mod shared;

use shared::{
    MockRule, build_ctx, build_ctx_with_config, fake_rebalance_body,
    fake_rebalancer::{FakeRebalancerClient, FakeResp, RebalCall, fake_proposal},
    json_response, op_config,
};

const NS: &str = "kafka";
const ENDPOINT: &str = "http://r-rebalancer.kafka.svc.cluster.local:9300";

fn rebalance(name: &str) -> KafkaRebalance {
    let mut kr = KafkaRebalance::new(
        name,
        KafkaRebalanceSpec {
            endpoint: Some(ENDPOINT.into()),
            ..Default::default()
        },
    );
    kr.metadata.namespace = Some(NS.into());
    kr.metadata.uid = Some("rebalance-uid".into());
    kr.metadata.generation = Some(1);
    kr
}

fn cond(type_: &str) -> KafkaCondition {
    KafkaCondition {
        type_: type_.into(),
        status: "True".into(),
        reason: type_.into(),
        message: String::new(),
        last_transition_time: "2026-05-22T00:00:00Z".into(),
    }
}

fn with_state(mut kr: KafkaRebalance, state: &str, session: Option<&str>) -> KafkaRebalance {
    kr.status = Some(KafkaRebalanceStatus {
        conditions: vec![cond(state)],
        session_id: session.map(str::to_string),
        ..Default::default()
    });
    kr
}

fn annotate(mut kr: KafkaRebalance, command: &str) -> KafkaRebalance {
    let mut a = BTreeMap::new();
    a.insert("krabka.io/rebalance".to_string(), command.to_string());
    kr.metadata.annotations = Some(a);
    kr
}

fn status_rule(name: &str) -> MockRule {
    MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkarebalances/{name}/status"),
        response: json_response(200, &fake_rebalance_body(name, NS)),
    }
}

fn annotation_rule(name: &str) -> MockRule {
    MockRule {
        method: Method::PATCH,
        path_substr: format!("/kafkarebalances/{name}?"),
        response: json_response(200, &fake_rebalance_body(name, NS)),
    }
}

fn auth_secret_rule(name: &str) -> MockRule {
    MockRule {
        method: Method::GET,
        path_substr: format!("/api/v1/namespaces/{NS}/secrets/{name}"),
        response: json_response(
            200,
            &serde_json::json!({
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": { "name": name, "namespace": NS },
                "data": { "token": "dGVzdC10b2tlbg==" }
            }),
        ),
    }
}

fn use_remove_brokers_auth(kr: &mut KafkaRebalance) {
    kr.spec.mode = KafkaRebalanceMode::RemoveBrokers;
    kr.spec.authorization_secret_ref = Some(RebalancerAuthorizationSecretRef {
        name: "demo-rebalancer-auth".into(),
        key: "token".into(),
    });
}

fn status_patch_body(observed: &[Request<Bytes>], name: &str) -> serde_json::Value {
    let suffix = format!("/kafkarebalances/{name}/status");
    let req = observed
        .iter()
        .find(|r| r.uri().to_string().contains(&suffix))
        .expect("status PATCH must have been captured");
    serde_json::from_slice(req.body()).expect("status body is JSON")
}

/// A new rebalance with no status leads to `CreateProposal` and then to
/// `ProposalReady`. The controller records the goals and the session id
/// that the rebalancer returned.
#[tokio::test]
async fn new_rebalance_creates_proposal() {
    let (ctx, state) = build_ctx(
        NS,
        vec![
            auth_secret_rule("demo-rebalancer-auth"),
            status_rule("demo"),
        ],
    );
    let fake = Arc::new(
        FakeRebalancerClient::new().with_create(FakeResp::Ok(fake_proposal(
            "p-new",
            ProposalStatus::Computed,
        ))),
    );
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let mut kr = rebalance("demo");
    use_remove_brokers_auth(&mut kr);
    kr.spec.brokers = vec![3, 4];
    reconcile(Arc::new(kr), ctx).await.unwrap();

    assert!(
        fake.calls()
            == vec![RebalCall::CreateProposal {
                mode: KafkaRebalanceMode::RemoveBrokers,
                brokers: vec![3, 4],
                goals: vec![],
                authenticated: true,
            }]
    );

    let body = status_patch_body(&state.take_observed(), "demo");
    check!(body["status"]["conditions"][0]["type"] == "ProposalReady");
    check!(body["status"]["sessionId"] == "p-new");
    check!(body["status"]["optimizationResult"]["replicaMovements"] == 2);
    check!(body["status"]["observedGeneration"] == 1);
}

#[tokio::test]
async fn remove_brokers_secret_reaches_real_client_as_bearer_header() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_endpoint = format!("http://{}", listener.local_addr().unwrap());
    let resource_endpoint = "http://test-rebalancer.kafka.svc:9300";
    let server = tokio::spawn(async move {
        let (mut connection, _) = listener.accept().await.unwrap();
        let mut request = vec![0; 8_192];
        let read = connection.read(&mut request).await.unwrap();
        request.truncate(read);
        let body = r#"{"id":"p-auth","status":"PROPOSAL_STATUS_COMPUTED"}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        connection.write_all(response.as_bytes()).await.unwrap();
        String::from_utf8(request).unwrap()
    });
    let (ctx, state) = build_ctx(
        NS,
        vec![
            auth_secret_rule("demo-rebalancer-auth"),
            status_rule("authenticated"),
        ],
    );
    ctx.insert_rebalancer_client_for_test(
        resource_endpoint,
        Arc::new(ConnectRebalancerClient::new(&server_endpoint, secs(5))),
    )
    .await;
    let mut kr = rebalance("authenticated");
    kr.spec.endpoint = Some(resource_endpoint.into());
    use_remove_brokers_auth(&mut kr);
    kr.spec.brokers = vec![3];

    tokio::time::timeout(
        core::time::Duration::from_secs(5),
        reconcile(Arc::new(kr), ctx),
    )
    .await
    .expect("authenticated reconcile completes")
    .unwrap();
    let observed = state.take_observed();
    let body = status_patch_body(&observed, "authenticated");
    assert!(
        body["status"]["conditions"][0]["type"] == "ProposalReady",
        "status = {body}"
    );

    let request = tokio::time::timeout(core::time::Duration::from_secs(5), server)
        .await
        .expect("rebalancer receives request")
        .unwrap();
    assert!(
        request
            .to_ascii_lowercase()
            .contains("\r\nauthorization: bearer test-token\r\n")
    );
    assert!(request.contains(r#""mode":"PROPOSAL_MODE_REMOVE_BROKERS""#));
}

/// `approve` on a `ProposalReady` proposal leads to `ExecuteProposal`,
/// with the configured throttle, and then to `Rebalancing`. The controller
/// consumes the annotation.
#[tokio::test]
async fn approve_executes_and_enters_rebalancing() {
    let (ctx, state) = build_ctx(
        NS,
        vec![
            auth_secret_rule("demo-rebalancer-auth"),
            annotation_rule("demo"),
            status_rule("demo"),
        ],
    );
    let fake = Arc::new(
        FakeRebalancerClient::new()
            .with_execute(FakeResp::Ok(fake_proposal("p1", ProposalStatus::Executing))),
    );
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let mut kr = with_state(rebalance("demo"), "ProposalReady", Some("p1"));
    use_remove_brokers_auth(&mut kr);
    kr.spec.throttle_bytes_per_sec = Some(mebibytes_per_sec(50));
    let kr = annotate(kr, "approve");
    reconcile(Arc::new(kr), ctx).await.unwrap();

    assert!(
        fake.calls()
            == vec![RebalCall::ExecuteProposal {
                id: "p1".into(),
                throttle: Some(mebibytes_per_sec(50)),
                authenticated: true,
            }]
    );

    let observed = state.take_observed();
    // Annotation consumed via a merge-null patch on the object (not /status).
    let annotation_patch = observed
        .iter()
        .find(|r| {
            let u = r.uri().to_string();
            u.contains("/kafkarebalances/demo?") && r.method() == Method::PATCH
        })
        .expect("annotation removal PATCH must have been captured");
    let ann_body: serde_json::Value = serde_json::from_slice(annotation_patch.body()).unwrap();
    assert!(
        ann_body["metadata"]["annotations"]["krabka.io/rebalance"].is_null(),
        "expected annotation merge-null, got {ann_body}"
    );

    let body = status_patch_body(&observed, "demo");
    assert!(body["status"]["conditions"][0]["type"] == "Rebalancing");
    // Session carried forward through the execute pass.
    assert!(body["status"]["sessionId"] == "p1");
}

/// A poll of an execution that is in flight and has finished gives
/// `Ready`.
#[tokio::test]
async fn poll_completes_to_ready() {
    let (ctx, state) = build_ctx(NS, vec![status_rule("demo")]);
    let fake = Arc::new(
        FakeRebalancerClient::new()
            .with_get(FakeResp::Ok(fake_proposal("p1", ProposalStatus::Completed))),
    );
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let kr = with_state(rebalance("demo"), "Rebalancing", Some("p1"));
    reconcile(Arc::new(kr), ctx).await.unwrap();

    assert!(fake.calls() == vec![RebalCall::GetProposal("p1".into())]);
    let body = status_patch_body(&state.take_observed(), "demo");
    assert!(body["status"]["conditions"][0]["type"] == "Ready");
    assert!(body["status"]["sessionId"] == "p1");
}

/// `stop` during `Rebalancing` leads to `CancelExecution` and then to
/// `Stopped`.
#[tokio::test]
async fn stop_cancels_to_stopped() {
    let (ctx, state) = build_ctx(NS, vec![annotation_rule("demo"), status_rule("demo")]);
    let fake = Arc::new(
        FakeRebalancerClient::new()
            .with_cancel(FakeResp::Ok(fake_proposal("p1", ProposalStatus::Cancelled))),
    );
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let kr = annotate(
        with_state(rebalance("demo"), "Rebalancing", Some("p1")),
        "stop",
    );
    reconcile(Arc::new(kr), ctx).await.unwrap();

    assert!(fake.calls() == vec![RebalCall::CancelExecution("p1".into())]);
    let body = status_patch_body(&state.take_observed(), "demo");
    assert!(body["status"]["conditions"][0]["type"] == "Stopped");
}

/// An execution that failed gives `NotReady` with the reason from the
/// broker.
#[tokio::test]
async fn poll_failure_surfaces_not_ready() {
    let (ctx, state) = build_ctx(NS, vec![status_rule("demo")]);
    let mut failed = fake_proposal("p1", ProposalStatus::Failed);
    failed.failure_reason = Some("broker 3 unreachable".into());
    let fake = Arc::new(FakeRebalancerClient::new().with_get(FakeResp::Ok(failed)));
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let kr = with_state(rebalance("demo"), "Rebalancing", Some("p1"));
    reconcile(Arc::new(kr), ctx).await.unwrap();

    let body = status_patch_body(&state.take_observed(), "demo");
    assert!(body["status"]["conditions"][0]["type"] == "NotReady");
    assert!(body["status"]["conditions"][0]["message"] == "broker 3 unreachable");
}

/// No `spec.endpoint` and no `krabka.io/cluster` label gives `NotReady`
/// with `MissingEndpoint` and zero Connect-RPCs.
#[tokio::test]
async fn missing_endpoint_sets_not_ready() {
    let (ctx, state) = build_ctx(NS, vec![status_rule("demo")]);
    // No fake injected — the controller must not reach the client.

    let mut kr = KafkaRebalance::new("demo", KafkaRebalanceSpec::default());
    kr.metadata.namespace = Some(NS.into());
    kr.metadata.uid = Some("rebalance-uid".into());
    reconcile(Arc::new(kr), ctx).await.unwrap();

    let body = status_patch_body(&state.take_observed(), "demo");
    assert!(body["status"]["conditions"][0]["type"] == "NotReady");
    assert!(body["status"]["conditions"][0]["reason"] == "MissingEndpoint");
}

#[tokio::test]
async fn remove_brokers_without_authorization_secret_fails_closed() {
    let missing_secret = MockRule {
        method: Method::GET,
        path_substr: format!("/api/v1/namespaces/{NS}/secrets/demo-rebalancer-auth"),
        response: json_response(
            404,
            &serde_json::json!({
                "apiVersion": "v1",
                "kind": "Status",
                "status": "Failure",
                "reason": "NotFound",
                "code": 404
            }),
        ),
    };
    let (ctx, state) = build_ctx(NS, vec![missing_secret, status_rule("demo")]);
    let fake = Arc::new(FakeRebalancerClient::new());
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;
    let mut kr = rebalance("demo");
    use_remove_brokers_auth(&mut kr);
    kr.spec.brokers = vec![3];

    reconcile(Arc::new(kr), ctx).await.unwrap();

    assert!(fake.calls().is_empty());
    let body = status_patch_body(&state.take_observed(), "demo");
    assert!(body["status"]["conditions"][0]["type"] == "NotReady");
    assert!(body["status"]["conditions"][0]["reason"] == "MissingAuthorizationSecret");
}

/// A transport error leaves the status unchanged and writes nothing to
/// kube, so that the next reconcile tries again. A short transient failure
/// therefore does not lose the proposal computation.
#[tokio::test]
async fn transport_error_leaves_status_untouched() {
    // Zero rules: any kube call would 404 and surface as an unexpected
    // request. The reconcile must short-circuit before patching.
    let mut config = op_config(NS);
    config.controller_error_requeue = krabka_units::millis(1_234);
    let (ctx, state) = build_ctx_with_config(NS, vec![], config);
    let fake = Arc::new(
        FakeRebalancerClient::new().with_create(FakeResp::Transport("connection refused".into())),
    );
    ctx.insert_rebalancer_client_for_test(ENDPOINT, fake.clone())
        .await;

    let kr = rebalance("demo");
    let action = reconcile(Arc::new(kr), ctx).await.unwrap();

    assert!(
        fake.calls()
            == vec![RebalCall::CreateProposal {
                mode: KafkaRebalanceMode::Full,
                brokers: vec![],
                goals: vec![],
                authenticated: false,
            }]
    );
    assert!(
        action
            == kube::runtime::controller::Action::requeue(std::time::Duration::from_millis(1_234))
    );
    assert!(
        state.take_observed().is_empty(),
        "transport error must not issue any kube requests"
    );
}
