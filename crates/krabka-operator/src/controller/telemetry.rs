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

#[cfg(test)]
mod tests {
    use assert2::assert;
    use serde_json::json;

    use super::*;

    fn endpoint(otlp_endpoint: &str) -> TelemetrySpec {
        TelemetrySpec {
            otlp_endpoint: Some(otlp_endpoint.into()),
            ..TelemetrySpec::default()
        }
    }

    #[test]
    fn validation_accepts_valid_telemetry_and_names_each_invalid_field() {
        let ratio = |sample_ratio: f64| TelemetrySpec {
            sample_ratio: Some(sample_ratio),
            ..TelemetrySpec::default()
        };
        let protocol = |otlp_protocol: &str| TelemetrySpec {
            otlp_protocol: Some(otlp_protocol.into()),
            ..TelemetrySpec::default()
        };
        let ratio_error =
            || Err("spec.telemetry.sampleRatio must be finite and between 0 and 1".into());
        for (case, telemetry, expected) in [
            ("nothing set", TelemetrySpec::default(), Ok(())),
            (
                "every field set",
                TelemetrySpec {
                    otlp_endpoint: Some("http://otel:4317".into()),
                    otlp_protocol: Some("grpc".into()),
                    sample_ratio: Some(0.5),
                    gcp_project_id: Some("krabka-prod".into()),
                },
                Ok(()),
            ),
            ("http protocol", protocol("http"), Ok(())),
            ("ratio at zero", ratio(0.0), Ok(())),
            ("ratio at one", ratio(1.0), Ok(())),
            (
                "unknown protocol",
                protocol("udp"),
                Err("spec.telemetry.otlpProtocol must be grpc or http".into()),
            ),
            ("ratio above one", ratio(1.5), ratio_error()),
            ("negative ratio", ratio(-0.1), ratio_error()),
            ("NaN ratio", ratio(f64::NAN), ratio_error()),
            ("infinite ratio", ratio(f64::INFINITY), ratio_error()),
        ] {
            assert!(validate_telemetry(&telemetry) == expected, "{case}");
        }
    }

    #[test]
    fn validation_rejects_empty_strings_by_field() {
        for (case, telemetry, path) in [
            (
                "empty endpoint",
                endpoint(""),
                "spec.telemetry.otlpEndpoint",
            ),
            (
                "empty project",
                TelemetrySpec {
                    gcp_project_id: Some(String::new()),
                    ..TelemetrySpec::default()
                },
                "spec.telemetry.gcpProjectId",
            ),
        ] {
            let result = validate_telemetry(&telemetry);
            assert!(let Err(error) = result, "{case}");
            assert!(
                error.starts_with(&format!("{path}: ")),
                "{case}: got {error}"
            );
        }
    }

    #[test]
    fn env_maps_each_protocol_onto_krabka_telemetry_spelling() {
        let with_protocol = |otlp_protocol: &str| TelemetrySpec {
            otlp_protocol: Some(otlp_protocol.into()),
            ..TelemetrySpec::default()
        };
        for (case, telemetry, expected) in [
            ("nothing set", TelemetrySpec::default(), vec![]),
            (
                "endpoint turns export on",
                endpoint("http://otel:4317"),
                vec![
                    json!({ "name": "KRABKA_OTLP_ENABLED", "value": "true" }),
                    json!({ "name": "KRABKA_OTLP_ENDPOINT", "value": "http://otel:4317" }),
                ],
            ),
            (
                "http becomes http/protobuf",
                with_protocol("http"),
                vec![json!({ "name": "KRABKA_OTLP_PROTOCOL", "value": "http/protobuf" })],
            ),
            (
                "grpc stays grpc",
                with_protocol("grpc"),
                vec![json!({ "name": "KRABKA_OTLP_PROTOCOL", "value": "grpc" })],
            ),
        ] {
            assert!(telemetry_env(&telemetry) == expected, "{case}");
        }
    }
}
