use k8s_openapi::{
    api::coordination::v1::{Lease, LeaseSpec},
    apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta},
    jiff,
};
use krabka_units::{Time, convert::TimeExt as _};
use kube::{
    Client,
    api::{Api, PostParams},
};

fn now() -> jiff::Timestamp {
    jiff::Timestamp::now()
}

/// The Lease duration as the Kubernetes `leaseDurationSeconds` field, of type
/// `Option<i32>`. The field belongs to the `LeaseSpec` that `k8s_openapi`
/// generates, so the validated extent narrows to whole seconds here.
fn lease_duration_seconds(extent: Time) -> anyhow::Result<i32> {
    i32::try_from(extent.secs_i64()).map_err(Into::into)
}

/// The result of [`Leadership::release`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Release {
    /// This replica held the lease. The update cleared the holder, so a
    /// standby replica takes the lease on its next retry.
    Released,
    /// The lease does not exist, or another replica holds it. The release
    /// did not change the lease.
    NotHeld,
    /// The lease changed between the read and the update. The release did not
    /// change the lease, so it cannot overwrite a new holder.
    Conflict,
}

/// A held Kubernetes lease. Its background task renews the lease until
/// leadership is lost, the holder releases the lease, or the guard is dropped.
pub struct Leadership {
    api: Api<Lease>,
    name: String,
    identity: String,
    renewer: tokio::task::JoinHandle<anyhow::Result<()>>,
}

impl Leadership {
    /// Wait until the lease can no longer be renewed safely.
    ///
    /// # Errors
    ///
    /// Returns the reason leadership was lost or the renewal task failed.
    pub async fn wait(&mut self) -> anyhow::Result<()> {
        (&mut self.renewer)
            .await
            .map_err(|error| anyhow::anyhow!("leader-election renewal task failed: {error}"))?
    }

    /// Stop the renewal and give the lease back. A standby replica then takes
    /// the lease on its next retry. It does not wait for the lease to expire.
    ///
    /// Call this after the controllers stop, so that two replicas never
    /// reconcile at the same time. The update is optimistic: it carries the
    /// `resourceVersion` of the lease that it read. A replica that lost the
    /// lease therefore cannot overwrite the new holder. This is the
    /// `ReleaseOnCancel` behavior of client-go.
    ///
    /// # Errors
    ///
    /// Returns the Kubernetes API error if the read or the update of the lease
    /// fails for a reason other than a conflict.
    pub async fn release(mut self) -> Result<Release, kube::Error> {
        self.renewer.abort();
        // The renewal can end with a cancel, a lost lease, or a missed
        // deadline. The read in `release_lease` finds the true holder in each
        // case, so the result of the renewal is not necessary here.
        drop((&mut self.renewer).await);
        release_lease(&self.api, &self.name, &self.identity, now()).await
    }
}

impl Drop for Leadership {
    fn drop(&mut self) {
        self.renewer.abort();
    }
}

/// Block until this process holds the Lease, then keep it renewed.
///
/// # Errors
///
/// Returns an error if the Kubernetes API call fails for a reason other
/// than a 409 create-race. The function retries internally on a 409
/// create-race.
pub async fn acquire(
    client: Client,
    namespace: &str,
    name: &str,
    identity: &str,
    lease_duration: Time,
    retry_interval: Time,
) -> anyhow::Result<Leadership> {
    let api: Api<Lease> = Api::namespaced(client, namespace);
    let lease_duration_seconds = lease_duration_seconds(lease_duration)?;
    loop {
        match api.get_opt(name).await? {
            None => {
                let lease = Lease {
                    metadata: ObjectMeta {
                        name: Some(name.into()),
                        ..Default::default()
                    },
                    spec: Some(LeaseSpec {
                        holder_identity: Some(identity.into()),
                        lease_duration_seconds: Some(lease_duration_seconds),
                        acquire_time: Some(MicroTime(now())),
                        renew_time: Some(MicroTime(now())),
                        lease_transitions: Some(1),
                        ..Default::default()
                    }),
                };
                match api.create(&PostParams::default(), &lease).await {
                    Ok(_) => {
                        tracing::info!(%name, %identity, "acquired lease (created)");
                        break;
                    }
                    Err(kube::Error::Api(e)) if e.code == 409 => {
                        // Race; another replica created it. Retry.
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            Some(mut existing) => {
                if held_by_us(&existing, identity) {
                    tracing::info!(%name, %identity, "re-confirmed lease ownership");
                    break;
                }
                if is_claimable(&existing, lease_duration) {
                    claim(&mut existing, identity, lease_duration_seconds);
                    match api.replace(name, &PostParams::default(), &existing).await {
                        Ok(_) => {
                            tracing::info!(%name, %identity, "acquired released or expired lease");
                            break;
                        }
                        Err(kube::Error::Api(error)) if error.code == 409 => {
                            tracing::debug!(%name, "lease takeover raced; retrying");
                        }
                        Err(error) => {
                            tracing::warn!(%error, %name, "lease takeover failed; will retry");
                        }
                    }
                }
                tracing::debug!(%name, "lease held by another replica, waiting");
                tokio::time::sleep(retry_interval.to_std()).await;
            }
        }
    }

    let renewer = tokio::spawn(renew_loop(
        api.clone(),
        name.to_owned(),
        identity.to_owned(),
        lease_duration,
        retry_interval,
        lease_duration_seconds,
    ));
    Ok(Leadership {
        api,
        name: name.to_owned(),
        identity: identity.to_owned(),
        renewer,
    })
}

/// Clear the holder of the lease `name` if `identity` holds it. The update
/// carries the `resourceVersion` of the read, so a concurrent change makes
/// the API server refuse it with 409.
async fn release_lease(
    api: &Api<Lease>,
    name: &str,
    identity: &str,
    now: jiff::Timestamp,
) -> Result<Release, kube::Error> {
    let Some(lease) = api.get_opt(name).await? else {
        return Ok(Release::NotHeld);
    };
    let Some(released) = released(&lease, identity, now) else {
        return Ok(Release::NotHeld);
    };
    match api.replace(name, &PostParams::default(), &released).await {
        Ok(_) => Ok(Release::Released),
        Err(kube::Error::Api(status)) if status.code == 409 => Ok(Release::Conflict),
        Err(error) => Err(error),
    }
}

/// The lease to write back when `identity` releases `lease`, or `None` if
/// `identity` does not hold it.
///
/// The holder is cleared and the lease duration is 1 second, as client-go
/// does. The metadata, and with it the `resourceVersion`, stays as it was
/// read. The transition count stays too: the next holder adds one.
fn released(lease: &Lease, identity: &str, now: jiff::Timestamp) -> Option<Lease> {
    if !held_by_us(lease, identity) {
        return None;
    }
    let mut released = lease.clone();
    let spec = released.spec.get_or_insert_with(LeaseSpec::default);
    spec.holder_identity = None;
    spec.lease_duration_seconds = Some(1);
    spec.acquire_time = Some(MicroTime(now));
    spec.renew_time = Some(MicroTime(now));
    Some(released)
}

fn claim(lease: &mut Lease, identity: &str, lease_duration_seconds: i32) {
    let spec = lease.spec.get_or_insert_with(LeaseSpec::default);
    let changed_holder = spec.holder_identity.as_deref() != Some(identity);
    let timestamp = MicroTime(now());
    spec.holder_identity = Some(identity.to_owned());
    spec.lease_duration_seconds = Some(lease_duration_seconds);
    spec.acquire_time = Some(timestamp.clone());
    spec.renew_time = Some(timestamp);
    if changed_holder {
        spec.lease_transitions = Some(spec.lease_transitions.unwrap_or(0).saturating_add(1));
    }
}

async fn renew_loop(
    api: Api<Lease>,
    name: String,
    identity: String,
    lease_duration: Time,
    retry_interval: Time,
    lease_duration_seconds: i32,
) -> anyhow::Result<()> {
    let mut last_success = tokio::time::Instant::now();
    loop {
        tokio::time::sleep(retry_interval.to_std()).await;
        match renew_once(&api, &name, &identity, lease_duration_seconds).await {
            Ok(()) => last_success = tokio::time::Instant::now(),
            Err(RenewError::Lost(reason)) => {
                anyhow::bail!("lost leader-election lease {name}: {reason}");
            }
            Err(RenewError::Retry(error)) => {
                if last_success.elapsed() >= lease_duration.to_std() {
                    anyhow::bail!(
                        "could not renew leader-election lease {name} before its deadline: {error}"
                    );
                }
                tracing::warn!(%error, %name, "lease renewal failed; retrying before deadline");
            }
        }
    }
}

enum RenewError {
    Lost(String),
    Retry(kube::Error),
}

async fn renew_once(
    api: &Api<Lease>,
    name: &str,
    identity: &str,
    lease_duration_seconds: i32,
) -> Result<(), RenewError> {
    let Some(mut lease) = api.get_opt(name).await.map_err(RenewError::Retry)? else {
        return Err(RenewError::Lost("lease was deleted".to_owned()));
    };
    if !held_by_us(&lease, identity) {
        let holder = lease
            .spec
            .as_ref()
            .and_then(|spec| spec.holder_identity.as_deref())
            .unwrap_or("<none>");
        return Err(RenewError::Lost(format!("holder changed to {holder}")));
    }
    let spec = lease.spec.get_or_insert_with(LeaseSpec::default);
    spec.lease_duration_seconds = Some(lease_duration_seconds);
    spec.renew_time = Some(MicroTime(now()));
    api.replace(name, &PostParams::default(), &lease)
        .await
        .map(|_| ())
        .map_err(RenewError::Retry)
}

fn held_by_us(lease: &Lease, identity: &str) -> bool {
    lease
        .spec
        .as_ref()
        .and_then(|s| s.holder_identity.as_deref())
        == Some(identity)
}

/// Whether another replica can take `lease` now: it has no holder, or its
/// holder did not renew it in time.
fn is_claimable(lease: &Lease, fallback_duration: Time) -> bool {
    is_vacant(lease) || is_expired(lease, fallback_duration)
}

/// Whether `lease` has no holder. A released lease has none.
fn is_vacant(lease: &Lease) -> bool {
    lease
        .spec
        .as_ref()
        .and_then(|spec| spec.holder_identity.as_deref())
        .is_none_or(str::is_empty)
}

fn is_expired(lease: &Lease, fallback_duration: Time) -> bool {
    let Some(spec) = lease.spec.as_ref() else {
        return true;
    };
    let Some(renew) = spec.renew_time.as_ref() else {
        return true;
    };
    let lease_duration = spec
        .lease_duration_seconds
        .map_or(fallback_duration, |raw| Time::from_secs(i64::from(raw)));
    let elapsed = Time::from_secs(now().as_second() - renew.0.as_second());
    elapsed > lease_duration
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use assert2::assert;
    use http::{Method, Request, Response};
    use http_body_util::BodyExt as _;
    use krabka_units::secs;

    use super::*;

    const NAMESPACE: &str = "krabka-system";
    const LEASE: &str = "krabka-operator-leader";
    const LEASE_PATH: &str =
        "/apis/coordination.k8s.io/v1/namespaces/krabka-system/leases/krabka-operator-leader";

    /// One request that the mock API server received: the method, the path,
    /// and the lease in the body, if the body holds one.
    type Observed = (Method, String, Option<Lease>);

    /// A Lease API whose server answers each request with the next of
    /// `responses`, as a status code and a JSON body. The second value
    /// collects the requests.
    fn scripted_api(
        responses: Vec<(u16, serde_json::Value)>,
    ) -> (Api<Lease>, Arc<Mutex<Vec<Observed>>>) {
        let responses = Arc::new(Mutex::new(responses.into_iter()));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let seen = observed.clone();
        let service = tower::service_fn(move |request: Request<kube::client::Body>| {
            let responses = responses.clone();
            let seen = seen.clone();
            async move {
                let (parts, body) = request.into_parts();
                let bytes = body.collect().await.unwrap().to_bytes();
                let lease = serde_json::from_slice::<Lease>(&bytes).ok();
                seen.lock()
                    .unwrap()
                    .push((parts.method, parts.uri.path().to_owned(), lease));
                let (status, body) = responses
                    .lock()
                    .unwrap()
                    .next()
                    .expect("the script has a response for each request");
                let response = Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(kube::client::Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap();
                Ok::<_, kube::Error>(response)
            }
        });
        let client = Client::new(service, NAMESPACE);
        (Api::namespaced(client, NAMESPACE), observed)
    }

    fn status_body(code: u16, reason: &str) -> serde_json::Value {
        serde_json::json!({
            "kind": "Status",
            "apiVersion": "v1",
            "status": "Failure",
            "code": code,
            "reason": reason,
            "message": reason,
        })
    }

    fn stored_lease(holder: Option<&str>, renew: jiff::Timestamp) -> Lease {
        Lease {
            metadata: ObjectMeta {
                name: Some(LEASE.into()),
                namespace: Some(NAMESPACE.into()),
                resource_version: Some("41".into()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: holder.map(Into::into),
                lease_duration_seconds: Some(15),
                acquire_time: Some(MicroTime(renew)),
                renew_time: Some(MicroTime(renew)),
                lease_transitions: Some(3),
                ..Default::default()
            }),
        }
    }

    #[tokio::test]
    async fn release_clears_the_holder_only_with_an_unchanged_lease() {
        struct Case {
            name: &'static str,
            responses: Vec<(u16, serde_json::Value)>,
            outcome: Result<Release, u16>,
            put: bool,
        }

        let renewed = jiff::Timestamp::from_second(1_800_000_000).unwrap();
        let now = jiff::Timestamp::from_second(1_800_000_005).unwrap();
        let ours = serde_json::to_value(stored_lease(Some("me"), renewed)).unwrap();
        let theirs = serde_json::to_value(stored_lease(Some("other"), renewed)).unwrap();
        let vacant = serde_json::to_value(stored_lease(None, renewed)).unwrap();
        // The update that a release sends: no holder, a 1 s duration, both
        // times at `now`, and the resourceVersion and transition count of the
        // read.
        let released = Lease {
            metadata: ObjectMeta {
                name: Some(LEASE.into()),
                namespace: Some(NAMESPACE.into()),
                resource_version: Some("41".into()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: None,
                lease_duration_seconds: Some(1),
                acquire_time: Some(MicroTime(now)),
                renew_time: Some(MicroTime(now)),
                lease_transitions: Some(3),
                ..Default::default()
            }),
        };

        let cases = [
            Case {
                name: "holds the lease and releases it",
                responses: vec![(200, ours.clone()), (200, ours.clone())],
                outcome: Ok(Release::Released),
                put: true,
            },
            Case {
                name: "another replica took the lease",
                responses: vec![(200, theirs)],
                outcome: Ok(Release::NotHeld),
                put: false,
            },
            Case {
                name: "the lease has no holder",
                responses: vec![(200, vacant)],
                outcome: Ok(Release::NotHeld),
                put: false,
            },
            Case {
                name: "the lease was deleted",
                responses: vec![(404, status_body(404, "NotFound"))],
                outcome: Ok(Release::NotHeld),
                put: false,
            },
            Case {
                name: "the lease changed between the read and the update",
                responses: vec![(200, ours.clone()), (409, status_body(409, "Conflict"))],
                outcome: Ok(Release::Conflict),
                put: true,
            },
            Case {
                name: "the update fails for another reason",
                responses: vec![(200, ours), (500, status_body(500, "InternalError"))],
                outcome: Err(500),
                put: true,
            },
        ];

        for case in cases {
            let (api, observed) = scripted_api(case.responses);
            let outcome =
                release_lease(&api, LEASE, "me", now)
                    .await
                    .map_err(|error| match error {
                        kube::Error::Api(status) => status.code,
                        other => panic!("{}: unexpected error {other}", case.name),
                    });
            let mut expected = vec![(Method::GET, LEASE_PATH.to_owned(), None)];
            if case.put {
                expected.push((Method::PUT, LEASE_PATH.to_owned(), Some(released.clone())));
            }
            let name = case.name;
            assert!(outcome == case.outcome, "{name}");
            assert!(*observed.lock().unwrap() == expected, "{name}");
        }
    }

    #[test]
    fn only_a_vacant_or_expired_lease_is_claimable() {
        let fresh = now();
        let stale = jiff::Timestamp::from_second(fresh.as_second() - 60).unwrap();
        let cases = [
            ("held and renewed", Some("other"), fresh, false),
            ("held and not renewed in time", Some("other"), stale, true),
            ("released, holder absent", None, fresh, true),
            ("released, holder empty", Some(""), fresh, true),
        ];
        for (name, holder, renew, claimable) in cases {
            let lease = stored_lease(holder, renew);
            assert!(is_claimable(&lease, secs(15)) == claimable, "{name}");
        }
    }

    fn lease_with(holder: &str, renew: jiff::Timestamp) -> Lease {
        Lease {
            metadata: ObjectMeta::default(),
            spec: Some(LeaseSpec {
                holder_identity: Some(holder.into()),
                lease_duration_seconds: Some(15),
                acquire_time: Some(MicroTime(renew)),
                renew_time: Some(MicroTime(renew)),
                lease_transitions: Some(1),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn held_by_us_matches_identity() {
        let l = lease_with("me", now());
        assert!(held_by_us(&l, "me"));
        assert!(!held_by_us(&l, "someone-else"));
    }

    #[test]
    fn expiry_uses_renew_time() {
        let stale = jiff::Timestamp::from_second(now().as_second() - 60).unwrap();
        let fresh = now();
        assert!(is_expired(&lease_with("x", stale), secs(15)));
        assert!(!is_expired(&lease_with("x", fresh), secs(15)));
    }

    #[test]
    fn expiry_uses_configured_fallback_when_lease_omits_duration() {
        let renew = jiff::Timestamp::from_second(now().as_second() - 20).unwrap();
        let mut lease = lease_with("x", renew);
        lease.spec.as_mut().unwrap().lease_duration_seconds = None;
        assert!(!is_expired(&lease, secs(30)));
        assert!(is_expired(&lease, secs(10)));
    }

    #[test]
    fn lease_duration_renders_as_whole_seconds() {
        // The k8s field is `Option<i32>` seconds; the extent narrows there and
        // nowhere else.
        assert!(lease_duration_seconds(secs(15)).unwrap() == 15);
        assert!(lease_duration_seconds(krabka_units::minutes(2)).unwrap() == 120);
        assert!(lease_duration_seconds(krabka_units::days(365 * 100)).is_err());
    }

    #[test]
    fn claim_updates_owner_timestamps_and_transition_count() {
        let old = jiff::Timestamp::from_second(now().as_second() - 60).unwrap();
        let mut lease = lease_with("old", old);
        claim(&mut lease, "new", 30);
        let spec = lease.spec.unwrap();
        assert!(spec.holder_identity.as_deref() == Some("new"));
        assert!(spec.lease_duration_seconds == Some(30));
        assert!(spec.lease_transitions == Some(2));
        assert!(spec.acquire_time == spec.renew_time);
        assert!(spec.renew_time.unwrap().0 > old);
    }
}
