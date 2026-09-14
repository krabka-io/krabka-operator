//! The health server: `/healthz`, `/readyz` and `/metrics`.
//!
//! Readiness follows the process [`Phase`]. It does not follow leadership. A
//! replica that waits for the leader-election lease is ready, because the
//! Deployment rolls with `maxUnavailable: 0`. The old pod stops only after the
//! new pod is Ready, and the new pod gets the lease only after the old pod
//! stops. The `krabka_operator_leader` gauge and the `/readyz` body show which
//! replica leads.

use std::{
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use axum::{Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use prometheus_client::{metrics::gauge::Gauge, registry::Registry};
use tokio::sync::Mutex;

use crate::telemetry::SharedRegistry;

/// The lifecycle phase of one operator process. A phase only moves forward.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    /// The process has not built its Kubernetes client yet.
    Starting,
    /// The process can serve. It waits for the leader-election lease and runs
    /// no controller.
    Standby,
    /// The process holds the leader-election lease and runs the controllers.
    Leading,
    /// The process stops. A shutdown signal arrived, a supervised task ended,
    /// or the process lost the lease.
    Stopping,
}

impl Phase {
    const fn to_raw(self) -> u8 {
        match self {
            Self::Starting => 0,
            Self::Standby => 1,
            Self::Leading => 2,
            Self::Stopping => 3,
        }
    }

    const fn from_raw(raw: u8) -> Self {
        match raw {
            0 => Self::Starting,
            1 => Self::Standby,
            2 => Self::Leading,
            _ => Self::Stopping,
        }
    }

    /// Whether `/readyz` answers 200 in this phase. A standby replica is
    /// ready, so that a rolling update can finish while the old replica holds
    /// the lease.
    #[must_use]
    pub const fn is_ready(self) -> bool {
        matches!(self, Self::Standby | Self::Leading)
    }

    /// Whether this phase holds the leader-election lease.
    #[must_use]
    pub const fn is_leader(self) -> bool {
        matches!(self, Self::Leading)
    }

    const fn name(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Standby => "standby",
            Self::Leading => "leading",
            Self::Stopping => "stopping",
        }
    }
}

#[derive(Clone)]
pub struct HealthState {
    /// Shared metrics registry. Controllers that must register metrics
    /// clone it, and the `/metrics` handler reads it.
    pub registry: SharedRegistry,
    phase: Arc<AtomicU8>,
    leader: Gauge,
}

impl HealthState {
    /// Register the `leader` gauge in `registry`, then share the registry.
    /// The state starts in [`Phase::Starting`].
    #[must_use]
    pub fn new(mut registry: Registry) -> Self {
        let leader = Gauge::default();
        registry.register(
            "leader",
            "1 while this replica holds the leader-election lease and runs the controllers, else 0",
            leader.clone(),
        );
        Self {
            registry: Arc::new(Mutex::new(registry)),
            phase: Arc::new(AtomicU8::new(Phase::Starting.to_raw())),
            leader,
        }
    }

    /// The current phase.
    #[must_use]
    pub fn phase(&self) -> Phase {
        Phase::from_raw(self.phase.load(Ordering::Acquire))
    }

    /// Move to `next`. A move to an earlier phase has no effect, so a late
    /// call cannot make a stopping process ready again.
    pub fn advance(&self, next: Phase) {
        self.phase.fetch_max(next.to_raw(), Ordering::AcqRel);
    }
}

pub fn router(state: HealthState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

/// Bind and serve forever. Returns only on a socket error or a shutdown signal.
///
/// # Errors
///
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub async fn serve(addr: SocketAddr, state: HealthState) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "health server listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

async fn healthz() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

async fn readyz(State(s): State<HealthState>) -> impl IntoResponse {
    let phase = s.phase();
    if phase.is_ready() {
        (StatusCode::OK, format!("ready: {}", phase.name()))
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            format!("not ready: {}", phase.name()),
        )
    }
}

async fn metrics(State(s): State<HealthState>) -> impl IntoResponse {
    // Write the gauge from the phase at scrape time, so that the two always
    // agree.
    s.leader.set(i64::from(s.phase().is_leader()));
    let mut buf = String::new();
    let r = s.registry.lock().await;
    if let Err(e) = prometheus_client::encoding::text::encode(&mut buf, &r) {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("encode: {e}")).into_response();
    }
    (
        StatusCode::OK,
        [(
            "content-type",
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        buf,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use axum::{body::Body, http::Request};
    use http::StatusCode as Code;
    use tower::ServiceExt as _;

    use super::*;

    fn fixture() -> HealthState {
        HealthState::new(crate::telemetry::new_registry())
    }

    async fn get(app: &Router, uri: &str) -> (Code, String) {
        let resp = app
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    #[tokio::test]
    async fn healthz_ok() {
        let (status, _) = get(&router(fixture()), "/healthz").await;
        assert!(status == Code::OK);
    }

    /// What the health server shows after a sequence of phase moves.
    #[derive(Debug, PartialEq, Eq)]
    struct Observed {
        phase: Phase,
        readyz_status: Code,
        readyz_body: String,
        leader_metric: String,
    }

    async fn observe(state: &HealthState) -> Observed {
        let app = router(state.clone());
        let (readyz_status, readyz_body) = get(&app, "/readyz").await;
        let (_, metrics) = get(&app, "/metrics").await;
        let leader_metric = metrics
            .lines()
            .find(|line| line.starts_with("krabka_operator_leader "))
            .unwrap_or("<missing>")
            .to_owned();
        Observed {
            phase: state.phase(),
            readyz_status,
            readyz_body,
            leader_metric,
        }
    }

    fn expected(phase: Phase, status: Code, body: &str, leader: &str) -> Observed {
        Observed {
            phase,
            readyz_status: status,
            readyz_body: body.to_owned(),
            leader_metric: format!("krabka_operator_leader {leader}"),
        }
    }

    /// A rolling update with `maxUnavailable: 0` stops the old pod only after
    /// the new pod is Ready. The old pod holds the lease until it stops. A
    /// standby replica must therefore be ready (issue #49).
    #[tokio::test]
    async fn readiness_follows_the_phase_and_not_the_lease() {
        use Phase::{Leading, Standby, Starting, Stopping};
        const OK: Code = Code::OK;
        const UNAVAILABLE: Code = Code::SERVICE_UNAVAILABLE;
        let cases: [(&str, &[Phase], Observed); 7] = [
            (
                "a new process is not ready",
                &[],
                expected(Starting, UNAVAILABLE, "not ready: starting", "0"),
            ),
            (
                "a standby replica is ready before it holds the lease",
                &[Standby],
                expected(Standby, OK, "ready: standby", "0"),
            ),
            (
                "a leader is ready",
                &[Standby, Leading],
                expected(Leading, OK, "ready: leading", "1"),
            ),
            (
                "a leader that stops is not ready",
                &[Standby, Leading, Stopping],
                expected(Stopping, UNAVAILABLE, "not ready: stopping", "0"),
            ),
            (
                "a standby replica that stops is not ready",
                &[Standby, Stopping],
                expected(Stopping, UNAVAILABLE, "not ready: stopping", "0"),
            ),
            (
                "a leader does not move back to standby",
                &[Standby, Leading, Standby],
                expected(Leading, OK, "ready: leading", "1"),
            ),
            (
                "a stopping process does not get ready again",
                &[Standby, Leading, Stopping, Leading],
                expected(Stopping, UNAVAILABLE, "not ready: stopping", "0"),
            ),
        ];
        for (name, moves, want) in cases {
            let state = fixture();
            for &next in moves {
                state.advance(next);
            }
            let got = observe(&state).await;
            assert!(got == want, "{name}");
        }
    }

    #[tokio::test]
    async fn metrics_returns_openmetrics() {
        let app = router(fixture());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(resp.status() == Code::OK);
        let ct = resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ct.starts_with("application/openmetrics-text"));

        // Body should be valid OpenMetrics text: encoder always emits `# EOF`
        // as the terminator. Catches a future regression where the route is
        // wired but the encoder isn't actually run.
        let body_bytes = axum::body::to_bytes(resp.into_body(), 64 * 1024)
            .await
            .unwrap();
        let body = std::str::from_utf8(&body_bytes).unwrap();
        assert!(body.contains("# EOF"), "metrics body missing # EOF: {body}");
    }
}
