//! `run` subcommand entry point.
//!
//! This module connects telemetry, the health and metrics server, leader
//! election, and the Kafka controller into one supervised task tree. It
//! returns when a supervised task finishes, when a supervised task fails,
//! or when a shutdown signal arrives.
//!
//! The process moves through the [`Phase`] values. It is ready in
//! [`Phase::Standby`], while it waits for the lease, and it starts the
//! controllers only in [`Phase::Leading`]. On a shutdown signal, or when a
//! controller or the health server ends, it stops the controllers and then
//! releases the lease, so that a standby replica takes over at once. A
//! replica that lost the lease does not release it.

use std::collections::HashMap;

use krabka_units::{Time, convert::TimeExt as _};
use kube::Client;
use tokio::task::JoinSet;

use crate::{
    config::OperatorConfig,
    context::Context,
    controller,
    health::{self, HealthState, Phase},
    leader_election::{self, Leadership, Release},
    telemetry,
};

/// Run the operator. See the module docs for the supervision shape.
///
/// # Errors
///
/// Returns an error if the Kubernetes client cannot be constructed, or if
/// leader election gives an unrecoverable API error. This function logs
/// per-task failures in the `tokio::select!` arms but does not propagate
/// them, because this function is supervisor glue and the e2e test is the
/// contract.
pub async fn run(config: OperatorConfig) -> anyhow::Result<()> {
    config.validate().map_err(anyhow::Error::msg)?;
    telemetry::init_tracing(&config.log_filter);
    let (registry, metrics) = telemetry::new_registry_with_metrics();
    let health_state = HealthState::new(registry);
    let registry = health_state.registry.clone();

    let health_addr = config.health_addr;
    let health_handle = tokio::spawn({
        let state = health_state.clone();
        async move { health::serve(health_addr, state).await }
    });

    let client = Client::try_default().await?;

    // Ready before the lease: a rolling update stops the old leader only
    // after this pod is Ready.
    health_state.advance(Phase::Standby);
    tracing::info!(
        lease = %config.lease_name,
        "ready in standby; waiting for the leader-election lease"
    );
    let mut leadership = leader_election::acquire(
        client.clone(),
        &config.operator_namespace,
        &config.lease_name,
        &config.pod_name,
        config.leader_lease_duration,
        config.leader_retry_interval,
    )
    .await?;

    let lease_duration = config.leader_lease_duration;
    let ctx = Context::new(client, config, registry, metrics);
    health_state.advance(Phase::Leading);
    tracing::info!("leading; starting the controllers");

    let mut controllers = Controllers::default();
    controllers.spawn("Kafka", controller::kafka::run(ctx.clone()));
    controllers.spawn(
        "KafkaNodePool",
        controller::kafka_node_pool::run(ctx.clone()),
    );
    controllers.spawn("KafkaTopic", controller::topic::run(ctx.clone()));
    controllers.spawn("KafkaUser", controller::user::run(ctx.clone()));
    controllers.spawn("KafkaRebalance", controller::rebalance::run(ctx.clone()));
    controllers.spawn(
        "KafkaGrpcGateway",
        controller::grpc_gateway::run(ctx.clone()),
    );
    controllers.spawn("KafkaConnector", controller::connector::run(ctx.clone()));
    controllers.spawn("SchemaRegistry", controller::schema_registry::run(ctx));

    tokio::select! {
        res = leadership.wait() => {
            health_state.advance(Phase::Stopping);
            return Err(res.err().unwrap_or_else(|| anyhow::anyhow!("leader-election renewal stopped")));
        },
        res = health_handle => match res {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::error!(error = %e, "health server exited with error"),
            Err(e) => tracing::error!(error = %e, "health task panicked"),
        },
        () = controllers.first_exit() => {}
        () = shutdown_signal() => tracing::info!("shutdown signal received"),
    }
    health_state.advance(Phase::Stopping);
    // Stop every controller before the lease goes back, so that this replica
    // and the next leader never reconcile at the same time.
    controllers.tasks.shutdown().await;
    release(leadership, lease_duration).await;
    Ok(())
}

/// The controller tasks, with the name of each task for the log.
#[derive(Default)]
struct Controllers {
    tasks: JoinSet<anyhow::Result<()>>,
    names: HashMap<tokio::task::Id, &'static str>,
}

impl Controllers {
    fn spawn(
        &mut self,
        name: &'static str,
        controller: impl Future<Output = anyhow::Result<()>> + Send + 'static,
    ) {
        let id = self.tasks.spawn(controller).id();
        self.names.insert(id, name);
    }

    /// Wait until one controller ends, and log how it ended.
    async fn first_exit(&mut self) {
        let Some(joined) = self.tasks.join_next_with_id().await else {
            return;
        };
        let (id, result) = match joined {
            Ok((id, result)) => (id, result.map_err(|error| error.to_string())),
            Err(error) => (error.id(), Err(format!("task panicked: {error}"))),
        };
        let controller = self.names.get(&id).copied().unwrap_or("unknown");
        match result {
            Ok(()) => tracing::info!(controller, "controller exited"),
            Err(error) => tracing::error!(controller, %error, "controller exited with error"),
        }
    }
}

/// Give the lease back after the controllers stop. The lease expires on its
/// own after `lease_duration`, so the process does not wait longer than that.
async fn release(leadership: Leadership, lease_duration: Time) {
    match tokio::time::timeout(lease_duration.to_std(), leadership.release()).await {
        Ok(Ok(Release::Released)) => tracing::info!("released the leader-election lease"),
        Ok(Ok(Release::NotHeld)) => {
            tracing::info!("leader-election lease is not held; nothing to release");
        }
        Ok(Ok(Release::Conflict)) => {
            tracing::warn!("leader-election lease changed during the release; left as it is");
        }
        Ok(Err(error)) => {
            tracing::warn!(%error, "could not release the leader-election lease; it expires");
        }
        Err(_) => tracing::warn!("leader-election lease release timed out; the lease expires"),
    }
}

/// Resolve when SIGINT arrives, or when SIGTERM arrives on Unix. Kubernetes
/// sends SIGTERM on pod shutdown. SIGINT covers `Ctrl+C` for local runs and
/// also works on Windows.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}
