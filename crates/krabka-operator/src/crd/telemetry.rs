//! OpenTelemetry settings shared by the components the operator deploys
//! beside the brokers: the gRPC gateway and the Schema Registry.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// OpenTelemetry observability configuration.
#[derive(Debug, Clone, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct TelemetrySpec {
    /// OTLP exporter endpoint URL, for example
    /// `http://otel-collector.observability.svc:4317`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otlp_endpoint: Option<String>,

    /// OTLP exporter protocol. One of `grpc` or `http`. Default `grpc`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub otlp_protocol: Option<String>,

    /// Fraction of traces to sample, in the range `[0.0, 1.0]`. Default
    /// 1.0, which samples all traces.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0.0, max = 1.0))]
    pub sample_ratio: Option<f64>,

    /// Google Cloud project id for Cloud Logging trace correlation. The
    /// operator renders it as `KRABKA_GCP_PROJECT_ID`. When it is set and
    /// `otlpEndpoint` turns OTLP export on, each stdout log entry written
    /// inside a traced span carries `logging.googleapis.com/trace` as
    /// `projects/<project id>/traces/<trace id>`,
    /// `logging.googleapis.com/spanId` and
    /// `logging.googleapis.com/trace_sampled`, so Cloud Logging links the
    /// entry to its trace. Unset, log entries carry no trace keys.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gcp_project_id: Option<String>,
}
