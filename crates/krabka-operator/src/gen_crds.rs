use std::{fs, path::Path};

use crate::crd::{
    Kafka, KafkaConnector, KafkaGrpcGateway, KafkaNodePool, KafkaRebalance, KafkaTopic, KafkaUser,
    SchemaRegistry,
};

/// Writes every CRD that this operator owns into `out_dir` as
/// `<group>_<plural>.yaml`. This function overwrites an existing file.
/// # Errors
/// Returns an error when cluster state cannot be loaded, the proposed plan is invalid, or a broker, Kubernetes, or persistence operation fails.
pub fn write_all(out_dir: &Path) -> anyhow::Result<()> {
    fs::create_dir_all(out_dir)?;
    write_one::<Kafka>(out_dir)?;
    write_one::<KafkaNodePool>(out_dir)?;
    write_one::<KafkaTopic>(out_dir)?;
    write_one::<KafkaUser>(out_dir)?;
    write_one::<KafkaRebalance>(out_dir)?;
    write_one::<KafkaConnector>(out_dir)?;
    write_one::<KafkaGrpcGateway>(out_dir)?;
    write_one::<SchemaRegistry>(out_dir)?;
    Ok(())
}

fn write_one<K>(out_dir: &Path) -> anyhow::Result<()>
where
    K: kube::Resource<DynamicType = ()> + kube::CustomResourceExt,
{
    let crd = K::crd();
    let group = &crd.spec.group;
    let plural = &crd.spec.names.plural;
    let file = out_dir.join(format!("{group}_{plural}.yaml"));
    let yaml = serde_yaml::to_string(&crd)?;
    fs::write(&file, yaml)?;
    eprintln!("wrote {}", file.display());
    Ok(())
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn writes_kafka_pool_topic_and_user_crd_files() {
        let dir = tempdir().unwrap();
        write_all(dir.path()).unwrap();
        for (file, plural, short_name) in [
            ("krabka.io_kafkas.yaml", "plural: kafkas", None),
            (
                "krabka.io_kafkanodepools.yaml",
                "plural: kafkanodepools",
                Some("- knp"),
            ),
            (
                "krabka.io_kafkatopics.yaml",
                "plural: kafkatopics",
                Some("- kt"),
            ),
            (
                "krabka.io_kafkausers.yaml",
                "plural: kafkausers",
                Some("- ku"),
            ),
            (
                "krabka.io_kafkarebalances.yaml",
                "plural: kafkarebalances",
                Some("- kr"),
            ),
            (
                "krabka.io_kafkagrpcgateways.yaml",
                "plural: kafkagrpcgateways",
                Some("- kgg"),
            ),
            (
                "krabka.io_kafkaconnectors.yaml",
                "plural: kafkaconnectors",
                Some("- kc"),
            ),
            (
                "krabka.io_schemaregistries.yaml",
                "plural: schemaregistries",
                Some("- sr"),
            ),
        ] {
            let path = dir.path().join(file);
            assert!(path.exists(), "case {file:?}");
            let yaml = std::fs::read_to_string(&path).unwrap();
            assert!(yaml.contains(plural), "case {file:?}");
            if let Some(short) = short_name {
                assert!(yaml.contains(short), "case {file:?}");
            }
        }
    }
}
