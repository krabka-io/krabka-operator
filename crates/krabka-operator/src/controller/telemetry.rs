//! Rendering and validation of [`TelemetrySpec`] for the components that
//! export OTLP beside the brokers: the gRPC gateway and the Schema Registry.
//! Both binaries read the same `KRABKA_OTLP_*` and `KRABKA_GCP_PROJECT_ID`
//! environment through `krabka-telemetry`.

use serde_json::{Value, json};

use crate::crd::TelemetrySpec;

/// The container environment that configures OTLP export as `telemetry`
/// asks. An endpoint turns export on. A protocol of `http` becomes
/// `http/protobuf`, the spelling `krabka-telemetry` reads, and any other
/// protocol becomes `grpc`.
pub(crate) fn telemetry_env(telemetry: &TelemetrySpec) -> Vec<Value> {
    let mut env = Vec::new();
    if let Some(endpoint) = telemetry.otlp_endpoint.as_deref() {
        env.push(json!({ "name": "KRABKA_OTLP_ENABLED", "value": "true" }));
        env.push(json!({ "name": "KRABKA_OTLP_ENDPOINT", "value": endpoint }));
    }
    if let Some(project) = telemetry.gcp_project_id.as_deref() {
        env.push(json!({ "name": "KRABKA_GCP_PROJECT_ID", "value": project }));
    }
    if let Some(protocol) = telemetry.otlp_protocol.as_deref() {
        let protocol = if protocol == "http" {
            "http/protobuf"
        } else {
            "grpc"
        };
        env.push(json!({ "name": "KRABKA_OTLP_PROTOCOL", "value": protocol }));
    }
    if let Some(ratio) = telemetry.sample_ratio {
        env.push(json!({ "name": "KRABKA_OTLP_SAMPLE_RATIO", "value": ratio.to_string() }));
    }
    env
}

/// Checks `telemetry`, naming each field under `spec.telemetry` in the
/// error.
///
/// # Errors
///
/// Returns an error when `otlpEndpoint` or `gcpProjectId` is set and empty,
/// when `otlpProtocol` is neither `grpc` nor `http`, or when `sampleRatio`
/// is not a finite number in `[0, 1]`.
pub(crate) fn validate_telemetry(telemetry: &TelemetrySpec) -> Result<(), String> {
    for (value, path) in [
        (&telemetry.otlp_endpoint, "spec.telemetry.otlpEndpoint"),
        (&telemetry.gcp_project_id, "spec.telemetry.gcpProjectId"),
    ] {
        if let Some(value) = value {
            refined_type::rule::NonEmptyString::new(value.clone())
                .map_err(|error| format!("{path}: {error}"))?;
        }
    }
    if let Some(protocol) = telemetry.otlp_protocol.as_deref()
        && !matches!(protocol, "grpc" | "http")
    {
        return Err("spec.telemetry.otlpProtocol must be grpc or http".into());
    }
    if let Some(ratio) = telemetry.sample_ratio
        && (!ratio.is_finite() || !(0.0..=1.0).contains(&ratio))
    {
        return Err("spec.telemetry.sampleRatio must be finite and between 0 and 1".into());
    }
    Ok(())
}
