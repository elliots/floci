//! A small client of the Kubernetes API server.
//!
//! `client.rs` is a client on hyper and rustls with the credentials of a
//! kubeconfig, and `pods.rs` the watch of the pods bound to this node
//! (`fieldSelector=spec.nodeName=NODE`), which reports them to a [`PodSink`]:
//! the node's [`crate::probe::NodeProbeBook`]. It reads only the pod fields
//! that [`Pod`] declares, so it needs neither `kube-rs` nor `k8s-openapi`.

mod client;
pub mod pods;

pub use pods::{Pod, PodSink, PodWatch};

/// The kubelet's kubeconfig under k3s's default data directory. The Node
/// authorizer lets it read the pods bound to its node.
pub const KUBELET_KUBECONFIG: &str = "/var/lib/rancher/k3s/agent/kubelet.kubeconfig";
