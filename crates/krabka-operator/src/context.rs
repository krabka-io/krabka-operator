use std::{collections::HashMap, sync::Arc};

use k8s_openapi::api::core::v1::Secret;
use krabka_client_admin::{AdminClient, AdminClientLike};
use krabka_security::ListenerProtocol;
use kube::{Api, Client};
use tokio::sync::Mutex;

use crate::{
    config::OperatorConfig,
    rebalancer_client::{ConnectRebalancerClient, RebalancerClientLike},
    telemetry::{ControllerMetrics, SharedRegistry},
};

/// Boxed-dyn admin client handle. The optional TLS material shares the
/// handle's lifetime, so cache eviction cannot remove files still used by an
/// in-flight lazy connection.
#[derive(Clone)]
pub struct AdminClientHandle {
    inner: Arc<Mutex<dyn AdminClientLike + Send>>,
    _material: Option<Arc<tempfile::TempDir>>,
}

impl AdminClientHandle {
    pub(crate) async fn lock(&self) -> tokio::sync::MutexGuard<'_, dyn AdminClientLike + Send> {
        self.inner.lock().await
    }
}

/// Boxed-dyn rebalancer client handle.
///
/// Production wraps a [`ConnectRebalancerClient`]. Reconcile tests
/// substitute a fake. The handle needs no `Mutex`, because the methods of
/// the client take `&self` and the inner HTTP client is a connection pool
/// that many callers can share.
pub type RebalancerClientHandle = Arc<dyn RebalancerClientLike>;

/// Shared context for each reconciler.
///
/// A clone is cheap. Every field is an `Arc` or is shared with interior
/// mutability.
#[derive(Clone)]
pub struct Context {
    pub client: Client,
    pub config: Arc<OperatorConfig>,
    pub registry: SharedRegistry,
    /// Controller metrics for the whole operator: the reconcile counters,
    /// histograms, and gauges. A clone is cheap. The handles are
    /// registered against `registry`.
    pub metrics: ControllerMetrics,
    /// Per-cluster-and-endpoint admin-client cache.
    /// The cache replaces a broken connection at the next use.
    pub admin_clients: Arc<Mutex<HashMap<String, AdminClientHandle>>>,
    /// Per-endpoint rebalancer-client cache, keyed by the resolved Connect
    /// base URL. The cache drops an entry after a transport failure and
    /// builds it again at the next use.
    pub rebalancer_clients: Arc<Mutex<HashMap<String, RebalancerClientHandle>>>,
}

impl Context {
    #[must_use]
    pub fn new(
        client: Client,
        config: OperatorConfig,
        registry: SharedRegistry,
        metrics: ControllerMetrics,
    ) -> Self {
        let config = Arc::new(config);
        Self {
            client,
            config,
            registry,
            metrics,
            admin_clients: Arc::new(Mutex::new(HashMap::new())),
            rebalancer_clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Returns the cached admin client for `cluster` at `bootstrap`, connecting
    /// one on a cache miss.
    ///
    /// # Errors
    ///
    /// Returns an error if the configured dispatch-queue capacity or frame max
    /// is out of range, or if the connection to `bootstrap` fails.
    pub async fn admin_client_for(
        &self,
        namespace: &str,
        cluster: &str,
        bootstrap: &str,
    ) -> Result<AdminClientHandle, krabka_client_admin::AdminError> {
        if let Some(client) = self.admin_clients.lock().await.get(cluster).cloned() {
            return Ok(client);
        }
        let secret_name = crate::controller::user_tls::operator_secret_name(cluster);
        let secret = Api::<Secret>::namespaced(self.client.clone(), namespace)
            .get(&secret_name)
            .await
            .map_err(|error| {
                krabka_client_admin::AdminError::Protocol(format!(
                    "operator identity Secret {namespace}/{secret_name}: {error}"
                ))
            })?;
        let version = secret
            .metadata
            .resource_version
            .as_deref()
            .unwrap_or("unknown");
        let mut map = self.admin_clients.lock().await;
        let prefix = format!("{cluster}\0{namespace}\0{bootstrap}\0");
        let key = format!("{prefix}{version}");
        if let Some(client) = map.get(&key).or_else(|| map.get(cluster)) {
            return Ok(client.clone());
        }
        let data = secret.data.as_ref().ok_or_else(|| {
            krabka_client_admin::AdminError::Protocol(format!(
                "operator identity Secret {namespace}/{secret_name} has no data"
            ))
        })?;
        let value = |name: &str| {
            data.get(name)
                .map(|value| value.0.as_slice())
                .ok_or_else(|| {
                    krabka_client_admin::AdminError::Protocol(format!(
                        "operator identity Secret {namespace}/{secret_name} has no {name}"
                    ))
                })
        };
        let material = tempfile::tempdir().map_err(|error| {
            krabka_client_admin::AdminError::Protocol(format!("operator identity tempdir: {error}"))
        })?;
        let ca_path = material.path().join("ca.crt");
        let cert_path = material.path().join("user.crt");
        let key_path = material.path().join("user.key");
        for (path, bytes) in [
            (&ca_path, value("ca.crt")?),
            (&cert_path, value("user.crt")?),
            (&key_path, value("user.key")?),
        ] {
            std::fs::write(path, bytes).map_err(|error| {
                krabka_client_admin::AdminError::Protocol(format!(
                    "write operator identity {}: {error}",
                    path.display()
                ))
            })?;
        }
        let admin = AdminClient::connect_with_options(
            &[bootstrap.to_string()],
            krabka_client_core::ConnectionOptions {
                dispatch_queue_capacity: krabka_client_core::ConnectionDispatchQueueCapacity::new(
                    self.config.client_dispatch_queue_capacity,
                )
                .map_err(krabka_client_admin::AdminError::Protocol)?,
                frame_max: krabka_client_core::ClientFrameMax::try_from(
                    self.config.client_frame_max,
                )
                .map_err(krabka_client_admin::AdminError::Protocol)?,
                security: Some(Box::new(krabka_client_core::ClientSecurity {
                    protocol: ListenerProtocol::Ssl,
                    tls: Some(krabka_client_core::TlsConnectorConfig {
                        trust_roots_pem: Some(ca_path),
                        server_name: format!(
                            "{cluster}-broker-headless.{namespace}.svc.cluster.local"
                        ),
                        client_identity: Some((cert_path, key_path)),
                    }),
                    sasl: None,
                    sasl_host: None,
                })),
                ..krabka_client_core::ConnectionOptions::default()
            },
        )
        .await?;
        let entry = AdminClientHandle {
            inner: Arc::new(Mutex::new(admin)),
            _material: Some(Arc::new(material)),
        };
        map.retain(|cached, _| !cached.starts_with(&prefix));
        map.insert(key, entry.clone());
        Ok(entry)
    }

    /// Drops the cached admin client for `cluster`.
    ///
    /// Reconcile calls this when a Transport error shows that the
    /// connection died. The next call opens a new connection.
    pub async fn drop_admin_client(&self, cluster: &str) {
        let mut clients = self.admin_clients.lock().await;
        clients.retain(|key, _| key != cluster && !key.starts_with(&format!("{cluster}\0")));
    }

    /// Fills the admin-client cache with a handle from the caller. This is
    /// for tests only.
    ///
    /// The `AdminClientLike` trait covers both the real client and the
    /// fakes of each test, so reconcile tests can call the trait methods
    /// and open no TCP connection.
    ///
    /// There is no `cfg` gate on this function. It stays in the public
    /// API. In production it does no damage and nothing calls it. Without
    /// the gate, the build needs no parallel test-only profile.
    pub async fn insert_admin_client_for_test<T>(&self, cluster: &str, admin: Arc<Mutex<T>>)
    where
        T: AdminClientLike + Send + 'static,
    {
        self.admin_clients.lock().await.insert(
            cluster.to_string(),
            AdminClientHandle {
                inner: admin,
                _material: None,
            },
        );
    }

    /// Looks up a rebalancer client for `endpoint`, or builds one.
    ///
    /// `endpoint` is a Connect base URL such as `http://host:9300`. The
    /// construction cannot fail, because the client opens no connection
    /// before the first RPC. This method therefore returns the handle
    /// directly.
    pub async fn rebalancer_client_for(&self, endpoint: &str) -> RebalancerClientHandle {
        let mut map = self.rebalancer_clients.lock().await;
        if let Some(client) = map.get(endpoint) {
            return client.clone();
        }
        let client: RebalancerClientHandle = Arc::new(ConnectRebalancerClient::new(
            endpoint,
            self.config.rebalancer_request_timeout,
        ));
        map.insert(endpoint.to_string(), client.clone());
        client
    }

    /// Drops the cached rebalancer client for `endpoint`.
    ///
    /// Reconcile calls this after a transport error. The next call builds
    /// the client again.
    pub async fn drop_rebalancer_client(&self, endpoint: &str) {
        self.rebalancer_clients.lock().await.remove(endpoint);
    }

    /// Fills the rebalancer-client cache with a fake. This is for tests
    /// only. It follows [`Self::insert_admin_client_for_test`].
    pub async fn insert_rebalancer_client_for_test(
        &self,
        endpoint: &str,
        client: RebalancerClientHandle,
    ) {
        self.rebalancer_clients
            .lock()
            .await
            .insert(endpoint.to_string(), client);
    }
}
