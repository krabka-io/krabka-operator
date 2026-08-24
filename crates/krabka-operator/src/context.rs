use std::{collections::HashMap, sync::Arc};

use crabka_client_admin::{AdminClient, AdminClientLike};
use kube::Client;
use tokio::sync::Mutex;

use crate::{
    config::OperatorConfig,
    rebalancer_client::{ConnectRebalancerClient, RebalancerClientLike},
    telemetry::{ControllerMetrics, SharedRegistry},
};

/// Boxed-dyn admin client handle.
///
/// Tests substitute a fake here and open no TCP connection. Production
/// code wraps a real `AdminClient`.
pub type AdminClientHandle = Arc<Mutex<dyn AdminClientLike + Send>>;

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

    pub async fn admin_client_for(
        &self,
        cluster: &str,
        bootstrap: &str,
    ) -> Result<AdminClientHandle, crabka_client_admin::AdminError> {
        let mut map = self.admin_clients.lock().await;
        let key = format!("{cluster}\0{bootstrap}");
        if let Some(client) = map.get(&key).or_else(|| map.get(cluster)) {
            return Ok(client.clone());
        }
        let admin = AdminClient::connect_with_options(
            &[bootstrap.to_string()],
            crabka_client_core::ConnectionOptions {
                dispatch_queue_capacity: crabka_client_core::ConnectionDispatchQueueCapacity::new(
                    self.config.client_dispatch_queue_capacity,
                )
                .map_err(crabka_client_admin::AdminError::Protocol)?,
                frame_max: crabka_client_core::ClientFrameMax::try_from(
                    self.config.client_frame_max,
                )
                .map_err(crabka_client_admin::AdminError::Protocol)?,
                ..crabka_client_core::ConnectionOptions::default()
            },
        )
        .await?;
        let entry: AdminClientHandle = Arc::new(Mutex::new(admin));
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
    pub async fn insert_admin_client_for_test(&self, cluster: &str, admin: AdminClientHandle) {
        self.admin_clients
            .lock()
            .await
            .insert(cluster.to_string(), admin);
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
