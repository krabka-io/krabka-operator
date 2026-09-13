//! Built-in default images of the components that the operator runs.
//!
//! The operator uses one of these images when a resource sets no
//! `spec.image` and the operator has no `--default-*-image` flag for that
//! component.
//!
//! Each component repository builds and publishes its own image, and it
//! versions that image on its own. Each default therefore names the
//! repository that publishes the image, and a tag of that component. It does
//! not use the operator version. The tag has the format that the component
//! repository publishes:
//!
//! - krabka-io/krabka-connect promotes a tested image to `v<version>` when a
//!   `v*` git tag is pushed.
//! - krabka-io/krabka-schema-registry publishes `<version>`, with no `v`, from
//!   its main branch.
//! - krabka-io/krabka-gateway uses the `v<version>` release tag of the other
//!   krabka-io repositories.
//!
//! The version is the `[workspace.package] version` of the component
//! repository. Change a default here when the operator moves to a new
//! component release.

/// Worker image of a `KafkaConnector`, from krabka-io/krabka-connect.
pub const CONNECT_WORKER_IMAGE: &str = "ghcr.io/krabka-io/krabka-connect-worker:v0.4.0";

/// Image of a `KafkaGrpcGateway`, from krabka-io/krabka-gateway.
pub const GATEWAY_IMAGE: &str = "ghcr.io/krabka-io/krabka-gateway:v0.4.0";

/// Image of a `SchemaRegistry`, from krabka-io/krabka-schema-registry.
pub const SCHEMA_REGISTRY_IMAGE: &str = "ghcr.io/krabka-io/krabka-schema-registry:0.4.0";

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn defaults_name_the_publishing_repository_and_a_component_tag() {
        // A whole reference per component, so a changed name or tag fails
        // here. None of them may carry the operator version, because the
        // components release on their own.
        for (actual, expected) in [
            (
                CONNECT_WORKER_IMAGE,
                "ghcr.io/krabka-io/krabka-connect-worker:v0.4.0",
            ),
            (GATEWAY_IMAGE, "ghcr.io/krabka-io/krabka-gateway:v0.4.0"),
            (
                SCHEMA_REGISTRY_IMAGE,
                "ghcr.io/krabka-io/krabka-schema-registry:0.4.0",
            ),
        ] {
            assert!(actual == expected);
        }
    }
}
