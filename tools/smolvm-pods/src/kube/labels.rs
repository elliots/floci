//! What clients of the node service's API select pods by: a pod's labels and
//! the workload that controls it, as the pod watch reads them.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::pods::ObjectMeta;

/// The label that the Deployment controller puts on the pods of each of its
/// ReplicaSets, whose name is the Deployment's name, `-` and the label's
/// value.
const POD_TEMPLATE_HASH: &str = "pod-template-hash";

/// A pod's labels, sorted by key, in one shared allocation that holds the
/// keys and values with a new line between each, a character that
/// Kubernetes allows in neither. A clone shares the allocation. As JSON, an
/// object.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct Labels(Option<Arc<str>>);

impl Labels {
    pub fn is_empty(&self) -> bool {
        self.0.is_none()
    }

    /// The labels, sorted by key.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        let mut words = self.0.as_deref().unwrap_or_default().split('\n');
        std::iter::from_fn(move || Some((words.next()?, words.next()?)))
    }

    #[cfg(test)]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.iter().find(|&(k, _)| k == key).map(|(_, v)| v)
    }
}

impl From<&BTreeMap<String, String>> for Labels {
    /// The labels of a map. A label with an empty key or a new line, which
    /// Kubernetes does not allow, is left out.
    fn from(map: &BTreeMap<String, String>) -> Labels {
        let mut text = String::new();
        for (key, value) in map {
            if key.is_empty() || key.contains('\n') || value.contains('\n') {
                continue;
            }
            if !text.is_empty() {
                text.push('\n');
            }
            text.push_str(key);
            text.push('\n');
            text.push_str(value);
        }
        Labels((!text.is_empty()).then(|| Arc::from(text)))
    }
}

impl Serialize for Labels {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_map(self.iter())
    }
}

impl<'de> Deserialize<'de> for Labels {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Labels, D::Error> {
        BTreeMap::<String, String>::deserialize(deserializer).map(|map| Labels::from(&map))
    }
}

impl fmt::Debug for Labels {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

/// The workload that controls a pod, such as `{"kind": "Deployment", "name":
/// "orders"}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Controller {
    pub kind: String,
    pub name: String,
}

impl Controller {
    /// The controller of a pod: its owner with `controller: true`, and none
    /// for a bare pod. A ReplicaSet whose name is a name, `-` and the pod's
    /// `pod-template-hash` belongs to the Deployment of that name, which is
    /// the controller. Any other owner, such as a StatefulSet, a DaemonSet, a
    /// Job or a ReplicaSet of its own, is the controller as it is.
    pub fn of(meta: &ObjectMeta) -> Option<Controller> {
        let owner = meta
            .owner_references
            .iter()
            .find(|owner| owner.controller == Some(true))?;
        if owner.kind == "ReplicaSet" {
            let hash = meta
                .labels
                .get(POD_TEMPLATE_HASH)
                .filter(|hash| !hash.is_empty());
            let deployment = hash
                .and_then(|hash| owner.name.strip_suffix(hash.as_str()))
                .and_then(|name| name.strip_suffix('-'))
                .filter(|name| !name.is_empty());
            if let Some(name) = deployment {
                return Some(Controller {
                    kind: "Deployment".into(),
                    name: name.into(),
                });
            }
        }
        Some(Controller {
            kind: owner.kind.clone(),
            name: owner.name.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    fn meta(labels: serde_json::Value, owners: serde_json::Value) -> ObjectMeta {
        serde_json::from_value(json!({
            "name": "pod", "namespace": "default", "uid": "u-1",
            "labels": labels, "ownerReferences": owners,
        }))
        .unwrap()
    }

    fn owner(kind: &str, name: &str, controller: Option<bool>) -> serde_json::Value {
        let mut owner = json!({
            "apiVersion": "apps/v1", "kind": kind, "name": name, "uid": "o-1",
            "blockOwnerDeletion": true,
        });
        if let Some(controller) = controller {
            owner["controller"] = controller.into();
        }
        owner
    }

    fn controller(kind: &str, name: &str) -> Option<Controller> {
        Some(Controller {
            kind: kind.into(),
            name: name.into(),
        })
    }

    #[test]
    fn a_deployments_replicaset_maps_to_the_deployment() {
        let labels = json!({"app": "orders", "pod-template-hash": "7c9d8f6b5"});
        let pod = meta(
            labels.clone(),
            json!([owner("ReplicaSet", "orders-7c9d8f6b5", Some(true))]),
        );
        assert_eq!(Controller::of(&pod), controller("Deployment", "orders"));
        let pod = meta(
            labels,
            json!([owner("ReplicaSet", "orders-api-7c9d8f6b5", Some(true))]),
        );
        assert_eq!(Controller::of(&pod), controller("Deployment", "orders-api"));
    }

    #[test]
    fn a_replicaset_not_named_after_the_hash_is_the_controller() {
        let owners = json!([owner("ReplicaSet", "workers", Some(true))]);
        let pod = meta(json!({"app": "workers"}), owners.clone());
        assert_eq!(Controller::of(&pod), controller("ReplicaSet", "workers"));
        let pod = meta(json!({"pod-template-hash": "5d4f"}), owners);
        assert_eq!(Controller::of(&pod), controller("ReplicaSet", "workers"));
        for name in ["7c9d8f6b5", "-7c9d8f6b5", "orders7c9d8f6b5"] {
            let pod = meta(
                json!({"pod-template-hash": "7c9d8f6b5"}),
                json!([owner("ReplicaSet", name, Some(true))]),
            );
            assert_eq!(
                Controller::of(&pod),
                controller("ReplicaSet", name),
                "{name}"
            );
        }
        let pod = meta(
            json!({"pod-template-hash": ""}),
            json!([owner("ReplicaSet", "orders-", Some(true))]),
        );
        assert_eq!(Controller::of(&pod), controller("ReplicaSet", "orders-"));
    }

    #[test]
    fn statefulset_daemonset_and_job_owners_are_the_controller_as_they_are() {
        for (kind, name) in [
            ("StatefulSet", "db"),
            ("DaemonSet", "agent"),
            ("Job", "migrate-29071234"),
            ("Node", "floci-node"),
        ] {
            let pod = meta(
                json!({"controller-revision-hash": "db-5d4f"}),
                json!([owner(kind, name, Some(true))]),
            );
            assert_eq!(Controller::of(&pod), controller(kind, name), "{kind}");
        }
    }

    #[test]
    fn a_bare_pod_has_no_controller() {
        assert_eq!(
            Controller::of(&meta(json!({"app": "web"}), json!([]))),
            None
        );
        assert_eq!(Controller::of(&ObjectMeta::default()), None);
        for flag in [None, Some(false)] {
            let pod = meta(json!({}), json!([owner("ReplicaSet", "web-5d4f", flag)]));
            assert_eq!(Controller::of(&pod), None, "{flag:?}");
        }
        let owners = json!([
            owner("ConfigMap", "settings", None),
            owner("StatefulSet", "db", Some(true)),
        ]);
        assert_eq!(
            Controller::of(&meta(json!({}), owners)),
            controller("StatefulSet", "db")
        );
    }

    #[test]
    fn labels_keep_every_valid_label_in_key_order() {
        let map: BTreeMap<String, String> = serde_json::from_value(json!({
            "pod-template-hash": "7c9d8f6b5",
            "app.kubernetes.io/name": "orders",
            "app": "orders",
            "empty": "",
        }))
        .unwrap();
        let labels = Labels::from(&map);
        assert_eq!(
            labels.iter().collect::<Vec<_>>(),
            [
                ("app", "orders"),
                ("app.kubernetes.io/name", "orders"),
                ("empty", ""),
                ("pod-template-hash", "7c9d8f6b5"),
            ]
        );
        assert_eq!(labels.get("app.kubernetes.io/name"), Some("orders"));
        assert_eq!(labels.get("empty"), Some(""));
        assert_eq!(labels.get("missing"), None);
        assert!(!labels.is_empty());

        let mut bad = map.clone();
        bad.insert("".into(), "x".into());
        bad.insert("line\nbreak".into(), "x".into());
        bad.insert("note".into(), "two\nlines".into());
        assert_eq!(Labels::from(&bad), labels);

        let empty = Labels::from(&BTreeMap::new());
        assert!(empty.is_empty());
        assert_eq!(empty, Labels::default());
        assert_eq!(empty.iter().count(), 0);
        let only_empty = BTreeMap::from([("flag".to_string(), String::new())]);
        assert_eq!(
            Labels::from(&only_empty).iter().collect::<Vec<_>>(),
            [("flag", "")]
        );
    }

    #[test]
    fn labels_and_controllers_are_json_objects() {
        let map = BTreeMap::from([
            ("app".to_string(), "orders".to_string()),
            ("tier".to_string(), String::new()),
        ]);
        let labels = Labels::from(&map);
        assert_eq!(
            serde_json::to_value(&labels).unwrap(),
            json!({"app": "orders", "tier": ""})
        );
        assert_eq!(
            serde_json::from_value::<Labels>(json!({"tier": "", "app": "orders"})).unwrap(),
            labels
        );
        assert_eq!(serde_json::to_value(Labels::default()).unwrap(), json!({}));
        assert!(serde_json::from_value::<Labels>(json!({"app": 1})).is_err());
        assert_eq!(format!("{labels:?}"), r#"{"app": "orders", "tier": ""}"#);
        assert_eq!(
            serde_json::to_value(controller("Deployment", "orders")).unwrap(),
            json!({"kind": "Deployment", "name": "orders"})
        );
    }

    /// A clone shares the labels' one allocation, which holds the keys and
    /// values with one byte between each.
    #[test]
    fn labels_are_one_shared_allocation() {
        let map = BTreeMap::from([
            ("app".to_string(), "orders".to_string()),
            ("pod-template-hash".to_string(), "7c9d8f6b5".to_string()),
        ]);
        let labels = Labels::from(&map);
        let clone = labels.clone();
        let (Some(a), Some(b)) = (&labels.0, &clone.0) else {
            unreachable!()
        };
        assert!(Arc::ptr_eq(a, b));
        assert_eq!(a.len(), "app".len() + "orders".len() + 17 + 9 + 3);
        assert_eq!(std::mem::size_of::<Labels>(), 16);
    }
}
