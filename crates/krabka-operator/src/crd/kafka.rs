use krabka_units::{
    ByteSize, Ratio, Time,
    convert::{ByteSizeExt as _, RatioExt as _, TimeExt as _},
    fmt::Human as _,
};
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Krabka cluster spec. The spec carries only the version label. Sibling
/// `KafkaNodePool`s labeled `krabka.io/cluster=<this name>` describe the
/// broker pods.
#[derive(CustomResource, Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[kube(
    group = "krabka.io",
    version = "v1alpha1",
    kind = "Kafka",
    plural = "kafkas",
    singular = "kafka",
    shortname = "kk",
    namespaced,
    status = "KafkaStatus",
    derive = "PartialEq"
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaSpec {
    /// Krabka version label. The operator propagates it to all pool pods
    /// through the `app.kubernetes.io/version` label.
    pub kafka_version: String,
    /// `KRaft` metadata version, the runtime analog of
    /// `inter.broker.protocol.version`. When unset, it tracks the
    /// `major.minor` of `kafkaVersion`. When set, it pins the metadata version
    /// for a two-step upgrade or an online downgrade. The operator validates
    /// it against `kafkaVersion`; changes on an existing cluster are finalized
    /// through `UpdateFeatures`, with safe-downgrade semantics for a lower
    /// target. Rejections surface `KafkaVersionValid=False` and leave
    /// `status.metadataVersion` unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_version: Option<String>,
    /// Opaque broker properties, in `server.properties`-style key and value
    /// pairs. The operator passes them through to the broker's
    /// `[server_properties]` TOML table, and the broker treats them as inert
    /// today. Changes propagate through the config hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<std::collections::BTreeMap<String, String>>,
    /// Named listeners. An empty or absent list synthesizes one internal
    /// `PLAIN` listener on port 9092.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<crate::crd::Listener>,
    /// Name of the listener for inter-broker traffic. When `None`, the
    /// operator picks the first `internal` listener. When `listeners` is
    /// empty, the operator picks the synthesized default `"PLAIN"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_broker_listener_name: Option<String>,
    /// Prometheus scrape configuration. When `None`, the brokers do not bind
    /// `/metrics`, and the operator renders no `PodMonitor` and no
    /// `ServiceMonitor`. When `Some`, the broker `StatefulSet` gains a
    /// `metrics` container port on TCP 9404, and the operator SSA-applies the
    /// resources that `pod_monitor` and `service_monitor` request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics_config: Option<crate::crd::MetricsConfig>,
    /// Opt-in `NetworkPolicy` generation. When `None`, the operator generates
    /// no `NetworkPolicy`. When `Some`, even `{}`, the operator renders a
    /// cluster-level `NetworkPolicy` that gates ingress to the broker and
    /// controller pods.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network_policy: Option<crate::crd::NetworkPolicySpec>,
    /// Per-cluster CA for inter-broker mTLS and broker certs. When absent,
    /// the operator uses a fully-defaulted `CertificateAuthority`, which it
    /// generates itself with 365/30 days.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_ca: Option<crate::crd::CertificateAuthority>,
    /// Per-cluster CA that signs `KafkaUser` TLS certs. When absent, the
    /// operator uses a fully-defaulted `CertificateAuthority`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients_ca: Option<crate::crd::CertificateAuthority>,
    /// Broker log configuration. When `None`, the brokers use their built-in
    /// default `RUST_LOG` filter. When `Some`, the operator composes an inline
    /// `tracing` env-filter string, or reads an external one. The operator
    /// then renders it into the broker `ConfigMap` under the `rust.log` key,
    /// wires it into the `RUST_LOG` env of each broker pod, and rolls the
    /// cluster on a change through the config hash.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logging: Option<crate::crd::Logging>,
    /// Delegation-token master HMAC key source. When `None`, the broker
    /// rejects all KIP-48 delegation-token RPCs with err 61
    /// `DELEGATION_TOKEN_AUTH_DISABLED`. When `Some`, the operator injects
    /// `KRABKA_DELEGATION_TOKEN_SECRET_KEY` into each broker pod through a
    /// `valueFrom.secretKeyRef`. The key is then part of the rendered
    /// `StatefulSet`, so the SSA reconcile does not race with out-of-band
    /// `kubectl set env` patches.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delegation_token: Option<DelegationTokenConfig>,
    /// Cluster-level authorizer selection. When `None`, the broker uses the
    /// default `AllowAll` authorizer and makes no ACL checks. When `Some`, the
    /// operator renders the `[authorization]` TOML section, so the broker
    /// builds the matching `Arc<dyn Authorizer>`. That is
    /// `SimpleAclAuthorizer` for `type: simple`, and `OpaAuthorizer` for
    /// `type: opa`. With `simple` or `opa` selected, the operator's
    /// inter-broker principal MUST appear in `super_users`. There is no
    /// implicit `ANONYMOUS` allow, and operators must opt in explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authorization: Option<Authorization>,
    /// KIP-405: cluster-wide tiered storage. When `Some`, every broker pod
    /// boots with the local-tier RSM enabled, with an `emptyDir` mounted at
    /// `/var/lib/krabka/remote`, which is the broker's
    /// `remote_log_storage_dir`, and with `[remote_storage]` rendered in the
    /// broker TOML. The per-topic enablement does not change. It stays
    /// `KafkaTopic.spec.config["remote.storage.enable"] = "true"`.
    ///
    /// With the `emptyDir` default and `InmemoryRemoteLogMetadataManager` as
    /// the only RLMM, tier data does not survive pod restarts. PVC support
    /// pairs with the production RLMM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tiered_storage: Option<TieredStorage>,
    /// Inter-broker Kerberos initiate config. It is required when
    /// `interBrokerListenerName` resolves to a `type: gssapi` listener. It
    /// supplies the shared client principal and the KDC. The keytab comes from
    /// that listener's `keytabSecretRef`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inter_broker_kerberos: Option<InterBrokerKerberos>,
    /// Optional process-wide `krb5.conf`. The operator mounts it into the
    /// broker pods and points `KRB5_CONFIG` at it. It serves both the accept
    /// path and the initiate path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub krb5_conf_secret_ref: Option<Krb5ConfSecretRef>,
    /// Distributed-tracing wiring for the broker pods. When `Some`, the
    /// operator renders the matching `KRABKA_OTLP_*` env vars onto every
    /// broker pod. The broker's telemetry pipeline reads them with
    /// `TelemetryConfig::from_env` and installs the OTLP tracer at startup.
    /// When `None`, the operator emits no OTLP env vars, and the broker leaves
    /// tracing off. That is the default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracing: Option<Tracing>,
    /// Validated broker operational policy rendered into `[runtime]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub broker_tuning: Option<BrokerTuning>,
}

fn validate_nonnegative_tuning_time(field: &str, value: Time) -> Result<(), String> {
    if value.secs_f64().is_finite() && value >= Time::from_secs(0) {
        Ok(())
    } else {
        Err(BrokerTuning::invalid(
            field,
            "must be finite and nonnegative",
        ))
    }
}

fn validate_positive_tuning_time(field: &str, value: Time) -> Result<(), String> {
    validate_nonnegative_tuning_time(field, value)?;
    if value > Time::from_secs(0) {
        Ok(())
    } else {
        Err(BrokerTuning::invalid(field, "must be positive"))
    }
}

fn validate_bounded_tuning_time(field: &str, value: Time, max_ms: i32) -> Result<(), String> {
    validate_whole_millis_tuning_time(field, value)?;
    let millis = value.millis_i64();
    if millis <= i64::from(max_ms) {
        Ok(())
    } else {
        Err(BrokerTuning::invalid(
            field,
            format!("must be at most {max_ms}ms"),
        ))
    }
}

fn validate_whole_millis_tuning_time(field: &str, value: Time) -> Result<(), String> {
    validate_positive_tuning_time(field, value)?;
    if Time::from_millis(value.millis_i64()) == value {
        Ok(())
    } else {
        Err(BrokerTuning::invalid(
            field,
            "must be a whole number of milliseconds",
        ))
    }
}

fn validate_tuning_size(field: &str, value: ByteSize, max: u64) -> Result<(), String> {
    let bytes = value.bytes_u64();
    if !value.bytes_f64().is_finite()
        || value <= ByteSize::from_bytes(0)
        || ByteSize::from_bytes(bytes) != value
    {
        return Err(BrokerTuning::invalid(
            field,
            "must be a positive whole number of bytes",
        ));
    }
    if bytes <= max {
        Ok(())
    } else {
        Err(BrokerTuning::invalid(
            field,
            format!("must be at most {max} bytes"),
        ))
    }
}

fn validate_positive_tuning_ratio(field: &str, value: Ratio) -> Result<(), String> {
    if value.as_f64().is_finite() && value > krabka_units::fraction(0.0) {
        Ok(())
    } else {
        Err(BrokerTuning::invalid(field, "must be finite and positive"))
    }
}

fn validate_unit_interval_tuning_ratio(field: &str, value: Ratio) -> Result<(), String> {
    if value.as_f64().is_finite()
        && value >= krabka_units::fraction(0.0)
        && value <= krabka_units::fraction(1.0)
    {
        Ok(())
    } else {
        Err(BrokerTuning::invalid(field, "must be between 0% and 100%"))
    }
}

macro_rules! validate_tuning_field {
    (refined, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            <$rule>::new(value)
                .map_err(|error| BrokerTuning::invalid(stringify!($field), error))?;
        }
    };
    (plain, $owner:ident, $field:ident, $rule:ty) => {};
    (string, $owner:ident, $field:ident, $rule:ty) => {};
    (time, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_positive_tuning_time(stringify!($field), value)?;
        }
    };
    (time_nonnegative, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_nonnegative_tuning_time(stringify!($field), value)?;
        }
    };
    (time_voter, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_bounded_tuning_time(stringify!($field), value, i32::MAX)?;
        }
    };
    (time_transaction_max, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_bounded_tuning_time(stringify!($field), value, i32::MAX - 1)?;
        }
    };
    (time_i32, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_bounded_tuning_time(stringify!($field), value, i32::MAX)?;
        }
    };
    (time_i64, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_whole_millis_tuning_time(stringify!($field), value)?;
        }
    };
    (size_i32, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_tuning_size(
                stringify!($field),
                value,
                u64::try_from(i32::MAX).expect("i32::MAX fits u64"),
            )?;
        }
    };
    (size_u32, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_tuning_size(stringify!($field), value, u64::from(u32::MAX))?;
        }
    };
    (size_usize, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_tuning_size(
                stringify!($field),
                value,
                u64::try_from(usize::MAX).unwrap_or(u64::MAX),
            )?;
        }
    };
    (size_u64, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_tuning_size(stringify!($field), value, u64::MAX)?;
        }
    };
    (size_snapshot_fetch, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            krabka_kraft_core::snapshot_fetch::MetadataSnapshotFetchMax::new(value)
                .map_err(|error| BrokerTuning::invalid(stringify!($field), error))?;
        }
    };
    (ratio_positive, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_positive_tuning_ratio(stringify!($field), value)?;
        }
    };
    (ratio_unit, $owner:ident, $field:ident, $rule:ty) => {
        if let Some(value) = $owner.$field {
            validate_unit_interval_tuning_ratio(stringify!($field), value)?;
        }
    };
}

macro_rules! render_tuning_field {
    (refined, $owner:ident, $out:ident, $field:ident) => {
        if let Some(value) = $owner.$field {
            use std::fmt::Write as _;
            let _ = writeln!($out, "{} = {value}", stringify!($field));
        }
    };
    (plain, $owner:ident, $out:ident, $field:ident) => {
        if let Some(value) = $owner.$field {
            use std::fmt::Write as _;
            let _ = writeln!($out, "{} = {value}", stringify!($field));
        }
    };
    (string, $owner:ident, $out:ident, $field:ident) => {
        if let Some(value) = &$owner.$field {
            use std::fmt::Write as _;
            let _ = writeln!(
                $out,
                "{} = {}",
                stringify!($field),
                toml::Value::String(value.clone())
            );
        }
    };
    ($kind:ident, $owner:ident, $out:ident, $field:ident) => {
        if let Some(value) = $owner.$field {
            use std::fmt::Write as _;
            let _ = writeln!(
                $out,
                "{} = {}",
                stringify!($field),
                toml::Value::String(value.human().to_string())
            );
        }
    };
}

macro_rules! define_broker_tuning {
    ($(
        $kind:ident
        $(#[$meta:meta])*
        $field:ident: $ty:ty => $rule:ty;
    )*) => {
        /// Typed Kafka CRD surface for broker `[runtime]` policy.
        #[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
        #[serde(rename_all = "camelCase", deny_unknown_fields)]
        pub struct BrokerTuning {
            $(
                $(#[$meta])*
                #[serde(default, skip_serializing_if = "Option::is_none")]
                pub $field: Option<$ty>,
            )*
        }

        impl BrokerTuning {
            /// Validate scalar and relational runtime constraints.
            ///
            /// # Errors
            ///
            /// Returns the invalid camel-case CRD path.
            pub fn validate(&self) -> Result<(), String> {
                $(validate_tuning_field!($kind, self, $field, $rule);)*
                self.validate_strings()?;
                self.validate_relations()
            }

            pub(crate) fn render_runtime_toml(&self) -> String {
                let mut values = String::new();
                $(render_tuning_field!($kind, self, values, $field);)*
                if values.is_empty() {
                    String::new()
                } else {
                    format!("[runtime]\n{values}\n")
                }
            }
        }
    };
}

define_broker_tuning! {
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] startup_leader_wait_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] self_registration_backoff_min: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] self_registration_backoff_max: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] observer_poll_interval: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] audit_spool_replay_interval: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] audit_stats_poll_interval: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] audit_partition_wait_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] liveness_tick_interval: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] gauge_poll_interval: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] cleaner_interval: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] isr_scan_interval: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] future_log_move_retry_backoff: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] client_metrics_eviction_tick: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] client_metrics_stale_floor: Time => ();
    time_i32 #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] client_metrics_default_interval: Time => ();
    size_i32 #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] client_metrics_telemetry_max: ByteSize => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] client_metrics_prom_snapshot_ttl: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] rlmm_reconcile_tick: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] rlmm_bootstrap_backoff_initial: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] rlmm_bootstrap_backoff_max: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] connection_creation_throttle_max: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] opa_http_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] oauth_jwks_http_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] auto_join_retry_backoff: Time => ();
    time_voter #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] auto_join_voter_request_timeout: Time => ();
    size_i32 #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] replication_fetch_max: ByteSize => ();
    time_i32 #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] replication_fetch_max_wait: Time => ();
    size_i32 #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] replication_fetch_min: ByteSize => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] replication_throttle_exhausted_backoff: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] replication_send_error_backoff: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] replication_unknown_topic_retry_delay: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] replication_epoch_fence_backoff: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] replication_unexpected_error_backoff: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] replication_reconnect_initial_delay: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] replication_reconnect_delay_cap: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] coordinator_session_expiry_tick: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] coordinator_shutdown_ack_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] consumer_group_session_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] consumer_group_heartbeat_interval: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] consumer_group_min_session_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] consumer_group_max_session_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] consumer_group_min_heartbeat_interval: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] consumer_group_max_heartbeat_interval: Time => ();
    refined #[schemars(range(min = 1))] consumer_group_max_size: usize => refined_type::rule::GreaterUsize<0>;
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] classic_group_initial_rebalance_delay: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] sync_group_follower_wait: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] unclean_recovery_aggressive_deadline: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] unclean_recovery_balanced_deadline: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] operator_recovery_deadline: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] quota_throttle_max: Time => ();
    refined #[schemars(range(min = 1))] self_registration_max_attempts: u32 => refined_type::rule::GreaterU32<0>;
    size_u32 #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] observer_fetch_max: ByteSize => ();
    refined #[schemars(range(min = 1))] audit_event_queue_capacity: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] audit_tail_window_offsets: i64 => refined_type::rule::GreaterI64<0>;
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] audit_tail_read_max: ByteSize => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] offsets_topic_metadata_wait_timeout: Time => ();
    refined #[schemars(range(min = 1))] client_metrics_stale_push_intervals: u32 => refined_type::rule::GreaterU32<0>;
    refined #[schemars(range(min = 1))] client_metrics_otlp_queue_capacity: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] coordinator_actor_mailbox_capacity: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] diskless_wal_local_replica_count: usize => refined_type::rule::GreaterUsize<0>;
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] diskless_wal_flush_interval: Time => ();
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] diskless_wal_flush_max_size: ByteSize => ();
    refined #[schemars(range(min = 0))] diskless_wal_trim_safety_lag: i64 => refined_type::rule::GreaterEqualI64<0>;
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] diskless_wal_index_projection_timeout: Time => ();
    refined #[schemars(range(min = 1))] unclean_recovery_queue_capacity: usize => refined_type::rule::GreaterUsize<0>;
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] share_recovery_read_max: ByteSize => ();
    refined #[schemars(range(min = 1))] share_session_cache_max_when_unlimited: usize => refined_type::rule::GreaterUsize<0>;
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] log_read_buffer_cap: ByteSize => ();
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] log_timestamp_scan_window: ByteSize => ();
    size_u32 #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] socket_request_max: ByteSize => ();
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] sendfile_min: ByteSize => ();
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] socket_send_buffer: ByteSize => ();
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] socket_receive_buffer: ByteSize => ();
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] acl_max_principal: ByteSize => ();
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] acl_max_resource_name: ByteSize => ();
    ratio_positive #[serde(with = "krabka_units::serde_units::human::option_ratio")] #[schemars(with = "Option<String>")] telemetry_max_decompression_ratio: Ratio => ();
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] telemetry_decompressed_output_floor: ByteSize => ();
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] telemetry_decompressed_output_ceiling: ByteSize => ();
    ratio_positive #[serde(with = "krabka_units::serde_units::human::option_ratio")] #[schemars(with = "Option<String>")] record_decompression_max_ratio: Ratio => ();
    size_u64 #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] record_decompression_output_floor: ByteSize => ();
    size_u64 #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] record_decompression_output_ceiling: ByteSize => ();
    string #[schemars(length(min = 1))] inter_broker_server_name: String => ();
    time_i64 #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] producer_id_expiration: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] producer_id_expiration_scan_interval: Time => ();
    refined #[schemars(range(min = 1))] max_produce_group: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] partition_writer_queue_depth: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] default_min_insync_replicas: i32 => refined_type::rule::GreaterI32<0>;
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] future_log_move_read_chunk: ByteSize => ();
    refined #[schemars(range(min = 1))] share_state_num_partitions: i32 => refined_type::rule::GreaterI32<0>;
    refined #[schemars(range(min = 1))] share_state_replication_factor: i16 => refined_type::rule::GreaterI16<0>;
    refined #[schemars(range(min = 1))] transaction_state_num_partitions: i32 => refined_type::rule::GreaterI32<0>;
    size_usize #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] transaction_recovery_read_max: ByteSize => ();
    refined #[schemars(range(min = 1))] transaction_state_replication_factor: i16 => refined_type::rule::GreaterI16<0>;
    time_i32 #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] transaction_min_timeout: Time => ();
    time_transaction_max #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] transaction_max_timeout: Time => ();
    time_nonnegative #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] partition_disk_scan_interval: Time => ();
    plain observer_lag_bound: u64 => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] heartbeat_interval: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] heartbeat_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] replica_lag_time_max: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] controller_election_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] controller_heartbeat_interval: Time => ();
    refined #[schemars(range(min = 1))] controller_fetch_miss_limit: u32 => refined_type::rule::GreaterU32<0>;
    refined #[schemars(range(min = 1))] metadata_raft_command_queue_capacity: usize => refined_type::rule::GreaterUsize<0>;
    size_i32 #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] metadata_raft_fetch_max: ByteSize => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] controlled_shutdown_drain_timeout: Time => ();
    size_u64 #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] metadata_max_between_snapshots: ByteSize => ();
    time_nonnegative #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] metadata_max_snapshot_interval: Time => ();
    refined #[schemars(range(min = 1))] metadata_snapshot_interval_records: u64 => refined_type::rule::GreaterU64<0>;
    size_snapshot_fetch #[serde(with = "krabka_units::serde_units::human::option_byte_size")] #[schemars(with = "Option<String>")] metadata_snapshot_fetch_max: ByteSize => ();
    time_nonnegative #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] txn_abort_cleanup_interval: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] leader_imbalance_check_interval: Time => ();
    ratio_unit #[serde(with = "krabka_units::serde_units::human::option_ratio")] #[schemars(with = "Option<String>")] leader_imbalance_per_broker: Ratio => ();
    time_nonnegative #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] tls_reload_interval: Time => ();
    plain max_incremental_fetch_session_cache_slots: usize => ();
    plain max_connections: usize => ();
    plain max_connections_per_ip: usize => ();
    time_i64 #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] delegation_token_max_lifetime: Time => ();
    time_i64 #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] delegation_token_expiry_check_interval: Time => ();
    time_i64 #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] delegation_token_default_renew_period: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] remote_log_manager_interval: Time => ();
    plain share_group_enable: bool => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] share_group_session_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] share_group_heartbeat_interval: Time => ();
    refined #[schemars(range(min = 1))] share_group_max_size: usize => refined_type::rule::GreaterUsize<0>;
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] share_group_record_lock_duration: Time => ();
    refined #[schemars(range(min = 1))] share_group_max_delivery_attempts: i16 => refined_type::rule::GreaterI16<0>;
    refined #[schemars(range(min = 1))] share_group_max_inflight_records: i32 => refined_type::rule::GreaterI32<0>;
    string share_group_isolation_level: String => ();
    plain streams_group_enable: bool => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] streams_group_session_timeout: Time => ();
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] streams_group_heartbeat_interval: Time => ();
    refined #[schemars(range(min = 1))] streams_group_max_size: usize => refined_type::rule::GreaterUsize<0>;
    refined #[schemars(range(min = 1))] streams_internal_topic_replication_factor: i16 => refined_type::rule::GreaterI16<0>;
    refined #[schemars(range(min = 0))] streams_group_num_standby_replicas: i32 => refined_type::rule::GreaterEqualI32<0>;
    refined #[schemars(range(min = 0))] streams_group_num_warmup_replicas: i32 => refined_type::rule::GreaterEqualI32<0>;
    refined #[schemars(range(min = 0))] streams_group_acceptable_recovery_lag: i64 => refined_type::rule::GreaterEqualI64<0>;
    time #[serde(with = "krabka_units::serde_units::human::option_time")] #[schemars(with = "Option<String>")] streams_group_task_offset_interval: Time => ();
    string streams_group_assignor: String => ();
}

impl BrokerTuning {
    fn camel_case(field: &str) -> String {
        let mut parts = field.split('_');
        let mut result = parts.next().unwrap_or_default().to_owned();
        for part in parts {
            let mut chars = part.chars();
            if let Some(first) = chars.next() {
                result.push(first.to_ascii_uppercase());
                result.extend(chars);
            }
        }
        result
    }

    fn path(field: &str) -> String {
        format!("spec.brokerTuning.{}", Self::camel_case(field))
    }

    fn invalid(field: &str, error: impl std::fmt::Display) -> String {
        format!("{}: {error}", Self::path(field))
    }

    fn invalid_relation(left: &str, right: &str, message: &str) -> String {
        format!("{} and {}: {message}", Self::path(left), Self::path(right))
    }

    fn validate_strings(&self) -> Result<(), String> {
        if let Some(value) = &self.inter_broker_server_name {
            refined_type::rule::NonEmptyString::new(value.clone())
                .map_err(|error| Self::invalid("inter_broker_server_name", error))?;
        }
        if let Some(value) = &self.share_group_isolation_level
            && !matches!(value.as_str(), "read-uncommitted" | "read-committed")
        {
            return Err(Self::invalid(
                "share_group_isolation_level",
                "expected `read-uncommitted` or `read-committed`",
            ));
        }
        if let Some(value) = &self.streams_group_assignor
            && !matches!(value.as_str(), "auto" | "sticky" | "highly-available")
        {
            return Err(Self::invalid(
                "streams_group_assignor",
                "expected `auto`, `sticky`, or `highly-available`",
            ));
        }
        Ok(())
    }

    fn validate_relations(&self) -> Result<(), String> {
        macro_rules! ordered {
            ($left:ident, $left_default:expr, <=, $right:ident, $right_default:expr) => {
                if self.$left.unwrap_or($left_default) > self.$right.unwrap_or($right_default) {
                    return Err(Self::invalid_relation(
                        stringify!($left),
                        stringify!($right),
                        "minimum or initial value exceeds maximum",
                    ));
                }
            };
            ($left:ident, $left_default:expr, <, $right:ident, $right_default:expr) => {
                if self.$left.unwrap_or($left_default) >= self.$right.unwrap_or($right_default) {
                    return Err(Self::invalid_relation(
                        stringify!($left),
                        stringify!($right),
                        "left value must be below right value",
                    ));
                }
            };
        }
        macro_rules! bounded {
            (
                $value:ident, $value_default:expr,
                $min:ident, $min_default:expr,
                $max:ident, $max_default:expr
            ) => {{
                let value = self.$value.unwrap_or($value_default);
                let min = self.$min.unwrap_or($min_default);
                let max = self.$max.unwrap_or($max_default);
                if !(min..=max).contains(&value) {
                    return Err(format!(
                        "{} must be within {} and {}",
                        Self::path(stringify!($value)),
                        Self::path(stringify!($min)),
                        Self::path(stringify!($max))
                    ));
                }
            }};
        }

        ordered!(
            self_registration_backoff_min,
            Time::from_millis(100),
            <=,
            self_registration_backoff_max,
            Time::from_millis(5_000)
        );
        ordered!(
            rlmm_bootstrap_backoff_initial,
            Time::from_millis(250),
            <=,
            rlmm_bootstrap_backoff_max,
            Time::from_millis(10_000)
        );
        ordered!(
            replication_fetch_min,
            ByteSize::from_bytes(1),
            <=,
            replication_fetch_max,
            ByteSize::from_bytes(1_048_576)
        );
        ordered!(
            replication_reconnect_initial_delay,
            Time::from_millis(100),
            <=,
            replication_reconnect_delay_cap,
            Time::from_millis(5_000)
        );
        ordered!(
            heartbeat_interval,
            Time::from_millis(3_000),
            <,
            heartbeat_timeout,
            Time::from_millis(9_000)
        );
        ordered!(
            controller_heartbeat_interval,
            Time::from_millis(500),
            <,
            controller_election_timeout,
            Time::from_millis(5_000)
        );
        ordered!(
            delegation_token_default_renew_period,
            Time::from_millis(86_400_000),
            <=,
            delegation_token_max_lifetime,
            Time::from_millis(604_800_000)
        );
        ordered!(
            client_metrics_eviction_tick,
            Time::from_millis(60_000),
            <=,
            client_metrics_stale_floor,
            Time::from_millis(600_000)
        );
        ordered!(
            unclean_recovery_aggressive_deadline,
            Time::from_millis(2_000),
            <=,
            unclean_recovery_balanced_deadline,
            Time::from_millis(30_000)
        );
        ordered!(
            telemetry_decompressed_output_floor,
            ByteSize::from_bytes(16_777_216),
            <=,
            telemetry_decompressed_output_ceiling,
            ByteSize::from_bytes(1_073_741_824)
        );
        self.validate_record_decompression()?;
        ordered!(
            transaction_min_timeout,
            Time::from_millis(1_000),
            <,
            transaction_max_timeout,
            Time::from_millis(900_000)
        );

        ordered!(
            consumer_group_min_session_timeout,
            Time::from_millis(45_000),
            <=,
            consumer_group_max_session_timeout,
            Time::from_millis(60_000)
        );
        bounded!(
            consumer_group_session_timeout,
            Time::from_millis(45_000),
            consumer_group_min_session_timeout,
            Time::from_millis(45_000),
            consumer_group_max_session_timeout,
            Time::from_millis(60_000)
        );
        ordered!(
            consumer_group_min_heartbeat_interval,
            Time::from_millis(5_000),
            <=,
            consumer_group_max_heartbeat_interval,
            Time::from_millis(15_000)
        );
        bounded!(
            consumer_group_heartbeat_interval,
            Time::from_millis(5_000),
            consumer_group_min_heartbeat_interval,
            Time::from_millis(5_000),
            consumer_group_max_heartbeat_interval,
            Time::from_millis(15_000)
        );

        if !(Time::from_millis(45_000)..=Time::from_millis(60_000)).contains(
            &self
                .share_group_session_timeout
                .unwrap_or_else(|| Time::from_millis(45_000)),
        ) {
            return Err(Self::invalid(
                "share_group_session_timeout",
                "must be within 45000..=60000",
            ));
        }
        if !(Time::from_millis(5_000)..=Time::from_millis(15_000)).contains(
            &self
                .share_group_heartbeat_interval
                .unwrap_or_else(|| Time::from_millis(5_000)),
        ) {
            return Err(Self::invalid(
                "share_group_heartbeat_interval",
                "must be within 5000..=15000",
            ));
        }
        if !(Time::from_millis(45_000)..=Time::from_millis(60_000)).contains(
            &self
                .streams_group_session_timeout
                .unwrap_or_else(|| Time::from_millis(45_000)),
        ) {
            return Err(Self::invalid(
                "streams_group_session_timeout",
                "must be within 45000..=60000",
            ));
        }
        if !(Time::from_millis(5_000)..=Time::from_millis(15_000)).contains(
            &self
                .streams_group_heartbeat_interval
                .unwrap_or_else(|| Time::from_millis(5_000)),
        ) {
            return Err(Self::invalid(
                "streams_group_heartbeat_interval",
                "must be within 5000..=15000",
            ));
        }
        Ok(())
    }

    fn validate_record_decompression(&self) -> Result<(), String> {
        let defaults = krabka_compression::RecordDecompressionPolicy::default();
        krabka_compression::RecordDecompressionPolicy::new(
            self.record_decompression_max_ratio
                .unwrap_or(defaults.max_ratio()),
            self.record_decompression_output_floor
                .unwrap_or(defaults.output_floor()),
            self.record_decompression_output_ceiling
                .unwrap_or(defaults.output_ceiling()),
        )
        .map(|_| ())
        .map_err(|error| {
            format!(
                "{}, {}, and {}: {error}",
                Self::path("record_decompression_max_ratio"),
                Self::path("record_decompression_output_floor"),
                Self::path("record_decompression_output_ceiling"),
            )
        })
    }
}

/// Inter-broker GSSAPI initiate config. There is one shared client principal
/// cluster-wide, and there are no per-broker host-templated SPNs.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct InterBrokerKerberos {
    /// Principal that every broker authenticates as when it dials peers, for
    /// example `kafka@EXAMPLE.COM`. It must exist in the shared keytab.
    pub client_principal: String,
    /// Target SPN primary. Defaults to `kafka`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// KDC endpoint, for example `tcp://kdc:88`.
    pub kdc_url: String,
}

/// Reference to a Secret holding a `krb5.conf`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Krb5ConfSecretRef {
    /// Name of the Secret holding the krb5.conf.
    pub secret_name: String,
    /// Key within the Secret whose value is the krb5.conf contents.
    pub key: String,
}

/// KIP-405: cluster-wide tiered-storage configuration.
///
/// The `type` discriminator picks the backend. The per-backend tuning is in
/// the matching sibling field: `s3` for `Type = S3`, `gcs` for `Type = Gcs`,
/// and no extra field for `Local`. The operator reconciler rejects a
/// mis-pairing with a `TieredStorageInvalid` status condition. The
/// mis-pairings are `type = "S3"` without `spec.s3`, `type = "Gcs"` without
/// `spec.gcs`, and `type = "Local"` with `spec.s3` or `spec.gcs` set.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TieredStorage {
    /// Backend kind selector.
    #[serde(rename = "type")]
    pub kind: TieredStorageType,
    /// S3-backend tuning. It is required when `kind == S3`, and it must be
    /// absent in any other case. The struct has the same shape as
    /// `krabka_remote_storage::S3Config`. The operator renders the
    /// non-credential fields verbatim into the broker TOML's
    /// `[remote_storage.s3]` block. The credentials come from Kubernetes
    /// Secrets, and the operator injects them as the broker-pod env vars
    /// `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3: Option<S3StorageSpec>,
    /// GCS-backend tuning. It is required when `kind == Gcs`, and it must be
    /// absent in any other case. The struct has the same shape as
    /// `krabka_remote_storage::GcsConfig`. The operator renders the
    /// non-credential fields verbatim into the broker TOML's
    /// `[remote_storage.gcs]` block. S3 uses env-var credentials, but GCS does
    /// not. The operator mounts an explicit service-account JSON key as a FILE
    /// on the broker pod and gives it to the broker as `service_account_path`
    /// in the TOML. Unset credentials select keyless Workload Identity or
    /// ADC.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gcs: Option<GcsStorageSpec>,
    /// KIP-405: pick the `RemoteLogMetadataManager` that the broker pods run.
    /// When the field is absent, or when it is `type: Topic`, the broker
    /// activates the durable
    /// `krabka_remote_storage_topic::TopicBasedRemoteLogMetadataManager`
    /// against the internal `__remote_log_metadata` topic. Tier-segment
    /// metadata then survives pod restarts and is consistent across the
    /// brokers in the cluster. Only an explicit `type: InMemory` selects the
    /// in-memory fixture, which is for test and dev only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_manager: Option<MetadataManagerSpec>,
    /// KIP-405: durable storage for the local-tier directory. It is valid
    /// only with `type=Local`. When it is absent, which is the default, the
    /// operator renders an `emptyDir` for `tier-storage`. When `Some`, the
    /// operator renders a `volumeClaimTemplate` of the configured size and
    /// class, so tier data survives pod restarts. Together with the
    /// topic-backed RLMM, this closes the "tier data is lost on pod restart"
    /// caveat.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<TieredStoragePersistence>,
}

/// KIP-405: PVC-backed local-tier directory.
///
/// The field shapes are the same as
/// [`crate::crd::kafka_node_pool::PersistentClaimSpec`], so operators learn
/// one schema for both the data dir and the tier-cache dir. PVC retention
/// follows the parent `KafkaNodePool.spec.storage.deleteClaim` setting. The
/// `persistentVolumeClaimRetentionPolicy` of the `StatefulSet` is set-wide,
/// and Kubernetes has no per-template override.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TieredStoragePersistence {
    /// K8s `Quantity`, for example `"50Gi"` or `"500Mi"`. It must be
    /// non-empty. The Kubernetes API server validates the resource-quantity
    /// form at SSA time.
    pub size: String,
    /// Storage class name. `None` means the cluster default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<String>,
    /// `true` gives
    /// `persistentVolumeClaimRetentionPolicy.whenDeleted: Delete`. It must
    /// match the parent `KafkaNodePool.spec.storage.deleteClaim` when both
    /// PVCs are present, because K8s `StatefulSets` have one set-wide
    /// retention policy and no per-template override. The operator validates
    /// this at reconcile time, and a mismatch surfaces as
    /// `TieredStorageInvalid`.
    #[serde(default)]
    pub delete_claim: bool,
}

/// KIP-405: the set of RSM backends that the operator can render. To add a
/// backend, extend this enum AND the matching render path in
/// `crate::controller::listeners::render_broker_toml`.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum TieredStorageType {
    /// On-pod filesystem store through `LocalTieredStorage`, the reference
    /// RSM. The data is at `/var/lib/krabka/remote` on the broker pod.
    #[default]
    Local,
    /// S3-compatible object store through `S3RemoteStorage`, the production
    /// RSM. Pair it with a filled-in [`TieredStorage::s3`] for the bucket, the
    /// region, and the credentials.
    S3,
    /// Native Google Cloud Storage through the GCS backend of
    /// `S3RemoteStorage`. Pair it with a filled-in [`TieredStorage::gcs`] for
    /// the bucket, the prefix, and the credentials. An unset
    /// `gcs.credentials` selects GKE Workload Identity or Application Default
    /// Credentials, which is the keyless production path. The operator mounts
    /// an explicit service-account JSON key as a file.
    Gcs,
}

/// KIP-405: cluster-wide S3 backend configuration.
///
/// The operator renders the non-credential fields verbatim into the broker
/// config TOML's `[remote_storage.s3]` block, and the broker parses them back
/// into `krabka_remote_storage::S3Config`. The operator NEVER renders
/// credentials into TOML. When [`Self::credentials`] is set, the operator
/// wires the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` env vars onto the
/// broker pod through `valueFrom.secretKeyRef`, and the `AmazonS3Builder` of
/// `object_store` reads them through the standard AWS credential chain. When
/// the credentials are absent, the broker pod inherits the IAM, IRSA, or
/// instance-profile auth that the cluster provides.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct S3StorageSpec {
    /// S3 bucket name. Required.
    pub bucket: String,
    /// AWS region. It is required even for the non-AWS endpoints `MinIO` and
    /// R2, because the `AmazonS3Builder` of `object_store` rejects an empty
    /// region.
    pub region: String,
    /// Optional key prefix inside the bucket. It lets more than one Krabka
    /// cluster share a bucket without a collision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Optional custom endpoint URL, for example `http://minio:9000` for
    /// `MinIO`, or `https://<account>.r2.cloudflarestorage.com` for Cloudflare
    /// R2. When `None`, the broker uses the AWS S3 endpoint for the configured
    /// region.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Optional explicit credentials. When `None`, the broker falls back to
    /// the AWS credential chain, such as IRSA on EKS or an instance profile on
    /// EC2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<S3Credentials>,
    /// Allow plaintext HTTP. It is off by default. Turn it on for a `MinIO`
    /// that runs without TLS. AWS S3 never needs it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_http: bool,
    /// Override the single-PUT and multipart cutoff in bytes. When unset, the
    /// broker uses `krabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD`, which
    /// is 100 MiB. Lower it in tests to exercise the multipart path on small
    /// fixtures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_threshold: Option<u64>,
    /// Override the per-part size for multipart uploads in bytes. When unset,
    /// the broker uses
    /// `krabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE`, which is
    /// 16 MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_chunk_size: Option<u64>,
}

/// KIP-405: cluster-wide native GCS backend configuration.
///
/// The shape is the same as `krabka_remote_storage::GcsConfig`. The operator
/// renders the non-credential fields verbatim into the broker config TOML's
/// `[remote_storage.gcs]` block, and the broker parses them back into
/// `krabka_remote_storage::GcsConfig`.
///
/// The credentials are different from S3. GCS credentials are a JSON key FILE,
/// and the GCS builder of `object_store` reads the file path directly. It does
/// NOT read `GOOGLE_APPLICATION_CREDENTIALS`. So when [`Self::credentials`] is
/// set, the operator mounts the referenced Secret as a file on the broker pod
/// and renders its path into the TOML as `service_account_path`. When the
/// credentials are absent, the broker uses Workload Identity or ADC, which is
/// the keyless GKE path, and the operator wires no credential file and no
/// env.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GcsStorageSpec {
    /// GCS bucket name. Required.
    pub bucket: String,
    /// Optional key prefix inside the bucket. It lets more than one Krabka
    /// cluster share a bucket without a collision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Optional custom GCS API base URL, for example for emulators and
    /// fakes. When `None`, the broker uses the standard Google Cloud Storage
    /// endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// Optional explicit service-account credentials. When None, the broker
    /// uses Workload Identity or ADC, which is the keyless GKE path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credentials: Option<GcsCredentials>,
    /// Allow plaintext HTTP. It is off by default. Turn it on for GCS
    /// emulators that run without TLS. Real GCS never needs it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub allow_http: bool,
    /// Override the single-PUT and multipart cutoff in bytes. When unset, the
    /// broker uses `krabka_remote_storage::DEFAULT_MULTIPART_THRESHOLD`, which
    /// is 100 MiB. Lower it in tests to exercise the multipart path on small
    /// fixtures.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_threshold: Option<u64>,
    /// Override the per-part size for multipart uploads in bytes. When unset,
    /// the broker uses
    /// `krabka_remote_storage::DEFAULT_MULTIPART_CHUNK_SIZE`, which is
    /// 16 MiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multipart_chunk_size: Option<u64>,
}

/// KIP-405: GCS service-account credential.
///
/// A single [`SecretKeyRef`] to the Secret that holds the service-account
/// JSON key. When set, the operator mounts the Secret as a file on the broker
/// pod and renders `service_account_path` into the broker TOML. Omit it to use
/// keyless Workload Identity or ADC.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct GcsCredentials {
    /// Reference to the Secret holding the service-account JSON key.
    pub service_account_key: SecretKeyRef,
}

impl TieredStorage {
    /// KIP-405: shape-validate the tagged union. On a failure it returns the
    /// description of the offending field, and the reconciler wraps that
    /// description in [`crate::controller::common::ReconcileError::TieredStorageInvalid`].
    /// The function is pure and does no I/O, so a unit test can call it
    /// without a cluster.
    ///
    /// # Errors
    ///
    /// Fails when the discriminator and the sibling fields disagree, for
    /// example `type=S3` without `s3`, `type=Gcs` without `gcs`, or a backend
    /// set together with the wrong discriminator. Also fails when the selected
    /// spec has no value for a required field: `bucket` and `region` for S3,
    /// and `bucket` for GCS.
    pub fn validate(&self) -> Result<(), String> {
        match self.kind {
            TieredStorageType::Local => {
                if self.s3.is_some() {
                    return Err("type=Local must not set `s3`".into());
                }
                if self.gcs.is_some() {
                    return Err("type=Local must not set `gcs`".into());
                }
            }
            TieredStorageType::S3 => {
                if self.gcs.is_some() {
                    return Err("type=S3 must not set `gcs`".into());
                }
                let s3 = self
                    .s3
                    .as_ref()
                    .ok_or("type=S3 requires `s3` (bucket + region at minimum)")?;
                if s3.bucket.trim().is_empty() {
                    return Err("s3.bucket is required and must be non-empty".into());
                }
                if s3.region.trim().is_empty() {
                    return Err("s3.region is required and must be non-empty".into());
                }
            }
            TieredStorageType::Gcs => {
                if self.s3.is_some() {
                    return Err("type=Gcs must not set `s3`".into());
                }
                let gcs = self
                    .gcs
                    .as_ref()
                    .ok_or("type=Gcs requires `gcs` (bucket at minimum)")?;
                if gcs.bucket.trim().is_empty() {
                    return Err("gcs.bucket is required and must be non-empty".into());
                }
            }
        }
        if let Some(mm) = self.metadata_manager.as_ref() {
            mm.validate()?;
        }
        if let Some(p) = self.persistence.as_ref() {
            if self.kind != TieredStorageType::Local {
                return Err("persistence is only valid with type=Local".into());
            }
            if p.size.trim().is_empty() {
                return Err("persistence.size is required and must be non-empty".into());
            }
        }
        Ok(())
    }
}

/// KIP-405: which `RemoteLogMetadataManager` the broker pods use. When you
/// omit this field, the default is the topic-backed manager, `type: Topic`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct MetadataManagerSpec {
    /// Implementation selector.
    #[serde(rename = "type")]
    pub kind: MetadataManagerType,
    /// Topic-backed tuning. It is optional when `kind == Topic`, and the
    /// broker fills the defaults for the bootstrap and topic parameters. It
    /// must be absent in any other case.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<TopicMetadataManagerSpec>,
}

impl MetadataManagerSpec {
    /// Shape-validate. The function is pure, and [`TieredStorage::validate`]
    /// calls it.
    ///
    /// # Errors
    ///
    /// Fails when `type=InMemory` is paired with a `topic` sub-block. Also
    /// fails when a topic-backed configuration supplies a `topic` block with
    /// invalid fields, for example an empty `bootstrap` or a non-positive
    /// `numPartitions`. A bare `type=Topic` with no `topic` block is valid,
    /// and the broker fills all defaults.
    pub fn validate(&self) -> Result<(), String> {
        match (self.kind, &self.topic) {
            (MetadataManagerType::InMemory, Some(_)) => {
                Err("metadataManager.type=InMemory must not set `topic`".into())
            }
            (MetadataManagerType::Topic | MetadataManagerType::InMemory, None) => Ok(()),
            (MetadataManagerType::Topic, Some(topic)) => topic.validate(),
        }
    }
}

/// KIP-405: the RLMM implementations that the operator can render.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum MetadataManagerType {
    /// In-memory fixture from `krabka_remote_storage`. Tier-segment metadata
    /// does not survive pod restarts. Only an explicit `type: InMemory`
    /// selects it, and it is for test and dev.
    InMemory,
    /// Production topic-backed manager from `krabka_remote_storage_topic`.
    /// This is the default. An optional [`MetadataManagerSpec::topic`]
    /// sub-block tunes the bootstrap address and the topic-creation
    /// parameters. The broker fills the defaults when you omit that
    /// sub-block.
    #[default]
    Topic,
}

/// KIP-405: topic-backed RLMM tuning. The operator renders it into the broker
/// TOML's `[remote_storage.kafka_metadata]` block.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TopicMetadataManagerSpec {
    /// `host:port` that the broker pod dials to reach its own listener, so
    /// that it can publish and consume `__remote_log_metadata`. This is
    /// usually the pod's loopback inter-broker listener, for example
    /// `127.0.0.1:9094`.
    pub bootstrap: String,
    /// Partition count for `__remote_log_metadata` on the first creation.
    /// Defaults to 50, which is the Kafka
    /// `remote.log.metadata.topic.num.partitions`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_partitions: Option<i32>,
    /// Replication factor for `__remote_log_metadata` on the first creation.
    /// Defaults to 3, which is the Kafka
    /// `remote.log.metadata.topic.replication.factor`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replication: Option<i32>,
    /// Timeout for provisioning each internal metadata topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub topic_create_timeout: Option<Time>,
    /// Maximum wait for each per-partition metadata fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub fetch_max_wait: Option<Time>,
    /// Maximum bytes returned by each per-partition metadata fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "krabka_units::serde_units::human::option_byte_size")]
    #[schemars(with = "Option<String>")]
    pub fetch_max_bytes: Option<ByteSize>,
    /// Backoff after a failed metadata fetch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub fetch_retry_backoff: Option<Time>,
    /// Capacity of the shared metadata-event delivery queue.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub event_queue_capacity: Option<usize>,
    /// RLMM cache snapshot cadence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[serde(with = "krabka_units::serde_units::human::option_time")]
    #[schemars(with = "Option<String>")]
    pub snapshot_interval: Option<Time>,
}

impl TopicMetadataManagerSpec {
    /// Shape-validate. The function is pure, and
    /// [`MetadataManagerSpec::validate`] calls it.
    ///
    /// # Errors
    ///
    /// Fails when `bootstrap` is empty, or when `num_partitions` or
    /// `replication` is non-positive.
    pub fn validate(&self) -> Result<(), String> {
        if self.bootstrap.trim().is_empty() {
            return Err("metadataManager.topic.bootstrap is required and must be non-empty".into());
        }
        if let Some(p) = self.num_partitions
            && p <= 0
        {
            return Err(format!(
                "metadataManager.topic.numPartitions must be > 0 (got {p})"
            ));
        }
        if let Some(r) = self.replication
            && r <= 0
        {
            return Err(format!(
                "metadataManager.topic.replication must be > 0 (got {r})"
            ));
        }
        let defaults = krabka_broker::KafkaRlmmConfig::default();
        let mut policy = krabka_broker::KafkaRlmmConfig {
            bootstrap: self.bootstrap.clone(),
            num_partitions: self.num_partitions.unwrap_or(defaults.num_partitions),
            replication: self.replication.unwrap_or(defaults.replication),
            ..defaults
        };
        policy.topic_create_timeout = self
            .topic_create_timeout
            .unwrap_or(policy.topic_create_timeout);
        policy.fetch_max_wait = self.fetch_max_wait.unwrap_or(policy.fetch_max_wait);
        policy.fetch_max_bytes = self.fetch_max_bytes.unwrap_or(policy.fetch_max_bytes);
        policy.fetch_retry_backoff = self
            .fetch_retry_backoff
            .unwrap_or(policy.fetch_retry_backoff);
        if let Some(capacity) = self.event_queue_capacity {
            refined_type::rule::GreaterUsize::<0>::new(capacity)
                .map_err(|error| format!("metadataManager.topic.event_queue_capacity: {error}"))?;
        }
        policy.snapshot_interval = self.snapshot_interval.unwrap_or(policy.snapshot_interval);
        policy
            .validate()
            .map_err(|error| format!("metadataManager.topic: {error}"))
    }
}

/// Fleet-wide distributed-tracing configuration. `Kafka.spec.tracing` and
/// `Gres.spec.tracing` share it. It maps to the `KRABKA_OTLP_*` env-var
/// contract. The operator renders one env entry per filled-in field onto every
/// pod of the fleet, and the telemetry pipeline of that binary reads them from
/// the environment at startup.
///
/// The `type` discriminator is reserved for future tracing backends. Today
/// only `Otlp` is meaningful, and the matching `otlp` block is required when
/// `type = Otlp`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct Tracing {
    /// Tracing backend selector.
    #[serde(rename = "type")]
    pub kind: TracingType,
    /// OTLP-backend tuning. Required when `kind == Otlp`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otlp: Option<OtlpTracing>,
}

/// The tracing backends the operator knows how to render.
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum TracingType {
    /// OpenTelemetry OTLP exporter. Pair it with [`Tracing::otlp`] for the
    /// endpoint, the protocol, and the sampling.
    #[default]
    Otlp,
}

/// OTLP-specific tracing parameters. The operator renders each filled-in
/// field as a separate env var on every pod of the owning fleet.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OtlpTracing {
    /// Required. OTLP collector endpoint in the form `scheme://host:port`.
    /// The operator renders it as `KRABKA_OTLP_ENDPOINT`. A set field also
    /// sets `KRABKA_OTLP_ENABLED=true`.
    pub endpoint: String,
    /// Optional protocol. An unset field leaves the binary's own default of
    /// `Grpc`, which matches the OpenTelemetry SDK convention. The operator
    /// renders it as `KRABKA_OTLP_PROTOCOL`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol: Option<OtlpProtocol>,
    /// Optional sampling ratio in `[0.0, 1.0]`. The operator renders it as
    /// `KRABKA_OTLP_SAMPLE_RATIO`. An unset field leaves the binary's own
    /// default of `1.0`, which samples every trace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_ratio: Option<f64>,
    /// Optional `service.name` resource attribute. The operator renders it as
    /// `OTEL_SERVICE_NAME`. An unset field leaves the binary's own name, which
    /// is `"krabka-broker"` for `Kafka` and `"krabka-gres"` for `Gres`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_name: Option<String>,
    /// Optional export timeout. The operator renders it as
    /// `KRABKA_OTLP_TIMEOUT`. An unset field leaves the binary's own default
    /// of `10s`.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        with = "krabka_units::serde_units::human::option_time"
    )]
    #[schemars(with = "Option<String>")]
    pub timeout: Option<Time>,
}

/// OTLP wire protocol selector. It has the same shape as the broker's
/// internal `OtlpProtocol` enum and the `OTEL_EXPORTER_OTLP_PROTOCOL` spec
/// values.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtocol {
    /// gRPC over HTTP/2. This is the default, on port `:4317`.
    Grpc,
    /// HTTP/1 with a protobuf payload, on port `:4318`.
    HttpProtobuf,
}

impl OtlpProtocol {
    /// Render the env-var value that the broker's `OtlpProtocol::parse`
    /// expects.
    #[must_use]
    pub fn as_env_value(self) -> &'static str {
        match self {
            Self::Grpc => "grpc",
            Self::HttpProtobuf => "http/protobuf",
        }
    }
}

impl Tracing {
    /// Shape-validate the tagged union.
    ///
    /// # Errors
    ///
    /// Fails when `type=Otlp` is missing the `otlp` block, when
    /// `otlp.endpoint` is empty, when `sampleRatio` is outside
    /// `[0.0, 1.0]`, or when `timeout` is not positive.
    pub fn validate(&self) -> Result<(), String> {
        match (self.kind, &self.otlp) {
            (TracingType::Otlp, None) => {
                Err("type=Otlp requires `otlp` (endpoint at minimum)".into())
            }
            (TracingType::Otlp, Some(otlp)) => {
                if otlp.endpoint.trim().is_empty() {
                    return Err("otlp.endpoint is required and must be non-empty".into());
                }
                if let Some(r) = otlp.sample_ratio
                    && !(0.0..=1.0).contains(&r)
                {
                    return Err(format!("otlp.sampleRatio must be in [0.0, 1.0] (got {r})"));
                }
                if let Some(s) = otlp.service_name.as_deref()
                    && s.trim().is_empty()
                {
                    return Err("otlp.serviceName, when set, must be non-empty".into());
                }
                if let Some(timeout) = otlp.timeout
                    && (timeout.secs_f64() <= 0.0
                        || std::time::Duration::try_from_secs_f64(timeout.secs_f64()).is_err())
                {
                    return Err("otlp.timeout, when set, must be positive and representable".into());
                }
                Ok(())
            }
        }
    }
}

/// KIP-405: S3 access-key credential pair.
///
/// There are two [`SecretKeyRef`]s, one for each half of the AWS credential.
/// An operator can hold the secret-access-key in a separate Secret with
/// tighter permissions than the access-key-id. The common case of both keys in
/// one Secret also works, with different `key` values on the same `name`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct S3Credentials {
    /// Reference to the Secret holding the `AWS_ACCESS_KEY_ID` value.
    pub access_key_id: SecretKeyRef,
    /// Reference to the Secret holding the `AWS_SECRET_ACCESS_KEY` value.
    pub secret_access_key: SecretKeyRef,
}

/// Master-HMAC-key source for KIP-48 delegation tokens.
///
/// The operator wires the referenced Secret key as the
/// `KRABKA_DELEGATION_TOKEN_SECRET_KEY` env var of the broker pod. The env
/// value wins over the TOML value in the broker config layer. This field is
/// required for delegation-token `KafkaUser` support. If it is unset on the
/// parent `Kafka`, the broker rejects all delegation-token RPCs with err 61
/// `DELEGATION_TOKEN_AUTH_DISABLED`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct DelegationTokenConfig {
    /// Reference to a Kubernetes `Secret` in the same namespace as the
    /// `Kafka` CR. Its `data.<key>` value is the broker's master HMAC key for
    /// KIP-48 delegation tokens.
    pub secret_key_ref: SecretKeyRef,
}

/// Minimal namespaced Secret-key reference. It holds a name and an optional
/// data-map key, which defaults to `secret-key`.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SecretKeyRef {
    /// Secret name in the same namespace as the `Kafka` CR.
    pub name: String,
    /// Key within the Secret's `data`. Defaults to `secret-key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
}

/// Cluster-level authorizer selection on `Kafka.spec.authorization`.
///
/// The enum is tagged on `type` to pick the broker-side `Arc<dyn Authorizer>`
/// impl. `None` on the parent spec means `AllowAll`. The operator then renders
/// no `[authorization]` TOML section, and the broker uses
/// `AllowAllAuthorizer`. When the field is set, the operator's inter-broker
/// principal MUST be in `super_users`. There is no implicit ANONYMOUS allow.
///
/// The `schema_with` workaround avoids a kube-rs 3.x `StructuralSchemaRewriter`
/// panic when `oneOf` branches share a `type` discriminator with different
/// `enum` values. This is the same pattern as `Authentication` in `user.rs`
/// and `ListenerAuthentication` in `listener.rs`.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
#[schemars(schema_with = "authorization_schema")]
pub enum Authorization {
    #[serde(rename = "simple")]
    Simple(SimpleAuthorization),
    #[serde(rename = "opa")]
    Opa(OpaAuthorization),
}

/// `type: simple` config for `Kafka.spec.authorization`. It drives the
/// broker's `SimpleAclAuthorizer`. It is different from the per-user
/// `crate::crd::user::SimpleAuthorization`, which carries the ACL rules for
/// one `KafkaUser`. This struct is cluster-wide and carries only the
/// super-user bypass list. The `KafkaUser` CRs and `CreateAcls` own the ACLs
/// themselves.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct SimpleAuthorization {
    /// Principal strings that bypass the ACL checks, for example
    /// `"User:admin"` and `"ANONYMOUS"`. An empty list means no super-users.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,
}

/// `type: opa` config for `Kafka.spec.authorization`. It drives the broker's
/// `OpaAuthorizer`, an HTTP-backed authorizer with an LRU and TTL decision
/// cache. There is no `derive(Default)`, because `url` has no sensible
/// default.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct OpaAuthorization {
    /// OPA decision endpoint URL. It must include the data-API path, for
    /// example `http://opa:8181/v1/data/kafka/authz/allow`.
    pub url: String,
    /// Permit the operation on any OPA error, such as a timeout, a 5xx, or a
    /// parse failure. Default: false, which is fail-closed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_on_error: Option<bool>,
    /// Initial capacity of the broker's LRU decision cache. Broker
    /// default applies when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub initial_cache_capacity: Option<u32>,
    /// Hard upper bound on the LRU decision cache. Broker default
    /// applies when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub maximum_cache_size: Option<u32>,
    /// Per-entry TTL in ms. The broker default applies when unset. The
    /// minimum is 1000 ms, because a sub-second TTL defeats the cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1000))]
    pub expire_after_ms: Option<i64>,
    /// Principal strings that bypass OPA. The broker's internal calls, such
    /// as replication, use `ANONYMOUS` by default. `ANONYMOUS` MUST be a
    /// super-user for the inter-broker traffic to work when `type: opa` is
    /// selected. An empty list means no super-users, and OPA then decides
    /// every request.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub super_users: Vec<String>,
}

fn authorization_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "required": ["type"],
        "properties": {
            "type": {
                "type": "string",
                "enum": ["simple", "opa"],
            },
            "superUsers": {
                "type": "array",
                "items": { "type": "string" },
            },
            // OPA-only sibling properties.
            "url": { "type": "string" },
            "allowOnError": { "type": "boolean" },
            "initialCacheCapacity": { "type": "integer", "minimum": 0 },
            "maximumCacheSize": { "type": "integer", "minimum": 1 },
            "expireAfterMs": { "type": "integer", "minimum": 1000 },
        },
    })
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaStatus {
    /// Standard Kubernetes-style condition list. It shows `Ready`,
    /// `ListenersValid`, and `ListenersReady`.
    #[serde(default)]
    pub conditions: Vec<KafkaCondition>,
    /// The same value as `StatefulSet.status.replicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replicas: Option<i32>,
    /// The same value as `StatefulSet.status.readyReplicas`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ready_replicas: Option<i32>,
    /// Per-listener resolved addresses. The operator fills them in once
    /// `ListenersReady=True`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub listeners: Vec<crate::crd::ListenerStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_ca: Option<crate::crd::CertificateAuthorityStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clients_ca: Option<crate::crd::CertificateAuthorityStatus>,
    /// Echo of `spec.kafkaVersion`, for observability.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kafka_version: Option<String>,
    /// The operator-finalized metadata version. On an existing cluster it
    /// advances only after `UpdateFeatures` accepts the requested level.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_version: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaCondition {
    /// For example, `Ready`.
    #[serde(rename = "type")]
    pub type_: String,
    /// `True`, `False`, or `Unknown`.
    pub status: String,
    /// CamelCase machine reason.
    pub reason: String,
    /// Human-readable message.
    pub message: String,
    /// RFC3339 timestamp.
    pub last_transition_time: String,
}
