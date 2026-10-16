# Compute runtime design

This records the implemented boundaries and remaining work as of 2026-10-16.
Native providers cover RDS PostgreSQL/MySQL/Aurora and Kubernetes pods. Generic
application VM operations remain available to the shared adapter and tests.
EC2 remains at the baseline implementation; all new EC2 work is parked separately.
Lambda and full Kubernetes worker lifecycle retain their existing
providers. See [MIGRATION.md](MIGRATION.md) for configuration changes. A booted
microVM alone does not establish workload compatibility.

## Shared mechanics, separate lifecycle rules

Share the authenticated host-helper transport and generic VM operations: create,
inspect, execute, transfer files, stop, remove, checkpoint, and restore. Command
output is bounded; streaming logs remains future helper work.
Keep resource identity, generation fencing, and recovery journal conventions
consistent. Persist Floci-owned journals through `StorageFactory`.

Each service retains its own adapter and policy:

| Service | Lifecycle owner | Initial target |
| --- | --- | --- |
| RDS | `RdsBackendRuntime` and the database connection gate | Existing Aurora idle hibernation and ordinary database stop/start |
| Kubernetes pods | Shared native controller and `KubernetesWorkloadRuntime` | One application VM per Pod UID, with control-plane reconciliation available |

These are adapter boundaries, not a proposal to replace every Docker integration
with one universal lifecycle class. Keep AWS request handling and service policy
out of the generic host helper. Docker remains the default during qualification.

## Existing Docker workloads

EC2, ECS, Lambda and ElastiCache keep their existing execution backends. The
native execution scope is Kubernetes pods and Aurora. All new EC2 work, including
Docker fixes and new test fixtures, is parked in
`parked/microsandbox-ec2-20261017`. Baseline EC2 behavior and tests remain unchanged.

## Individual Kubernetes workers

The worker material below records earlier experiments, not the active implementation
plan. Current native execution targets individual Pods, not worker or EKS control-plane hibernation. Today's
`EksClusterManager` runs a Docker-backed k3s cluster with one shared node;
node-group metadata does not provision an independent worker VM per instance.
`ClusterNodeInstanceProvider` exposes synthesized cluster instances to EC2, and
EC2 currently rejects start/stop operations on those external instances.

The [native worker probe](README.md#observed-worker-qualification) now runs real
pods successfully on one VM. The default two-worker network fails cross-worker
Pod IP traffic; the optional `--relay-vxlan` now passes actual cross-worker Pod IP,
DNS and ClusterIP checks by forwarding UDP encapsulation through host loopback
ports. Docker-worker peers, NetworkPolicy, IPv6 and hibernation remain unqualified.
No existing AWS API or runtime default changed in this audit.

AWS EKS now exposes `warmPoolConfig`, including `poolState=HIBERNATED` and
`reuseOnScaleIn=true`. By default, scale-in terminates instances rather than
returning them to the pool. Floci currently stores that configuration as metadata;
it does not implement warm-pool lifecycle or `UpdateNodegroupConfig`. Implement
those AWS semantics, eligibility constraints, and SDK tests after worker
qualification. [EKS WarmPoolConfig](https://docs.aws.amazon.com/eks/latest/APIReference/API_WarmPoolConfig.html).

1. First prove a microsandbox guest can run containerd, kubelet, and real pods.
   Check kernel facilities, cgroups, mounts, CNI networking, DNS, service routing,
   storage, logs, and exec. Booting the k3s agent alone is insufficient.
2. Introduce a worker runtime boundary and one real worker per requested node,
   retaining the available control plane. Reconcile desired count, node identity,
   credentials, labels, taints, registration, readiness, and deletion. Integrate
   real EC2 instance records rather than extending synthesized placeholders.
3. Begin hibernated warm pools with initialized or drained workers. On scale-in,
   reuse only when configured; retain termination as the default. Cordon, evict workload
   pods while respecting disruption budgets, and verify completion before saving
   worker state. After restore, reconcile credentials and networking, require
   `Ready`, then uncordon. Draining terminates workload processes, so this saves
   worker startup state rather than application RAM. DaemonSets and local storage
   require explicit handling.
   [Kubernetes node draining](https://kubernetes.io/docs/tasks/administer-cluster/safely-drain-node/).
4. Treat sleeping workers that retain application pods as a separate experiment.
   Nodes send status and Lease heartbeats; an unreachable worker becomes unhealthy
   and may have its pods evicted. Cordon alone does not prevent that. Resuming old
   pods after replacement pods were scheduled can duplicate work. Do not fake
   health or disable normal reconciliation to make a snapshot appear successful.
   [Kubernetes node lifecycle](https://kubernetes.io/docs/concepts/architecture/nodes/).

Wake policy must stay outside sleeping workers. Pending schedulable workloads
can drive worker capacity; request-driven application scale-to-zero also needs
an available request gate and workload controller. A VM checkpoint does not
provide those mechanisms by itself. Test multiple workers, disruption budgets,
PVC handling, controller restart, and actual pod placement before claiming support.

## Shared pod controller

The application target is hundreds of mostly sleeping Kubernetes pods. A kubelet in every application VM costs memory and stops heartbeating
when its VM sleeps. Keep one shared controller running outside the application
VMs, with one VM per pod. A Virtual Kubelet provider is the qualification seam;
its node and pod controllers supply Kubernetes reconciliation, while a native
host provider owns actual execution and networking.
[Virtual Kubelet](https://github.com/virtual-kubelet/virtual-kubelet).

The external host process lives in this repository under
`tools/microsandbox-pods`. It is an experimental tool, not a published sidecar or
a new AWS endpoint. Floci's `KubernetesWorkloadRuntime` uses its authenticated
contract to discover Deployment ownership and hibernate or resume the existing
pods. Cluster registration and controller setup remain manual. Unsupported pod
features are rejected before guest creation.

The Kubernetes Pod UID owns one VM, with a separate monotonically increasing
lifecycle generation fencing stale operations. The provider reports actual task
state and exec readiness, serves logs and non-interactive exec, and removes only
owned resources. A live node lease represents the shared controller. In strict
availability mode, cold and hibernated pods remain unavailable until activation
and readiness succeed.
Cold registration does not allocate a VM; subsequent idle cycles preserve RAM.
An opt-in proxy availability policy instead reports the available shared wake
listener as Ready before the application starts, allowing lazy deployments to
finish. This is an explicit local readiness extension. Actual application state
and readiness remain separate, and the gate waits for them before forwarding.
The available Floci gateway and queue coordinator drive activation while the
controller bounds active guest RAM. See
[native Kubernetes configuration](../../docs/configuration/microsandbox-kubernetes.md).

The guest runs containerd/runc under its primary process supervisor, without a
kubelet. Current support includes one OCI container with image defaults,
ConfigMap/Secret environment and directory mounts, disk/memory `emptyDir`,
restart policies, logs, exec, common security contexts, and HTTP/TCP/exec
startup, readiness and liveness probes. Most-feature support still needs init
containers, sidecars, shared namespaces, persistent volumes, service-account
projection and gRPC probes. The historical Virtual Kubelet CRI sample
explicitly lacks working networking and persistent volumes and is not a production
shortcut. [CRI sample limitations](https://github.com/virtual-kubelet/cri).

An owned Linux bridge routes genuine Pod IPs through VXLAN relays. Qualification
checks Docker and native clients, source IP preservation, DNS, Services, two RAM
resumes, controller crash adoption and deletion. NetworkPolicy, IPv6 and arbitrary
CNI plugins remain unsupported. Proxy-mode Pod IPs belong to a shared Linux TCP
relay, with separate guest backend addresses; strict routing preserves direct
client source addresses. Floci-owned idle policy observes batched controller
state and proxy connections alongside queue work. Standalone proxy idle policy
is separate and cannot also manage a Floci-owned workload.
Continue measuring idle host memory, checkpoint
storage and bounded restore throughput before claiming capacity for 500 actual
applications.

## Archived Lambda execution design

The proposal below is retained as design history. Lambda keeps its existing
backend and is outside the native Kubernetes/Aurora implementation scope.

`LambdaRuntimeLauncher` already supplies `launch`, `stop`, and `isAlive`, with
Docker and Kubernetes implementations selected by `floci.services.lambda.executor`.
Each environment has a Floci-hosted Runtime API server. `WarmPool` leases one
environment per invocation, reuses warm environments, and destroys them after
300 seconds idle by default. Ephemeral and hot-reload modes destroy environments
after each invocation. Timeouts, crashes, and code updates also retire environments.

First add a microsandbox launcher using this seam, preserving code/layer staging,
image overrides, execution-role credentials, extensions, logs, and shutdown.
Remote helpers need artifact transfer or presigned downloads; Floci filesystem
paths cannot automatically be used as helper-host paths. The current bounded
text command transport also needs log streaming and process-exit monitoring.

Then add a separate suspension capability owned by the Lambda pool. AWS freezes
an environment only after the runtime and every extension have signalled completion
through their next-event calls and no events remain pending. Reuse is optional.
[AWS Lambda lifecycle](https://docs.aws.amazon.com/lambda/latest/dg/lambda-runtime-environment.html).

Before idle checkpointing is safe:

- Add a runtime/extension completion barrier. Today's `RuntimeApiServer` completes
  an invocation on the runtime response without awaiting extension post-response
  work, so `WarmPool.release()` alone is not a safe freeze point.
- Preserve the per-environment API endpoint and verify runtime and extension
  long-poll reconnects after restore, without losing or duplicating invocation
  delivery. Existing `quiesce()` sends shutdown and is not a suspension primitive.
- Exclude capture/restore from concurrent acquisition and recheck code-update and
  deletion generations. Capture only idle environments, never active invocations.
- Keep checkpoints per environment. Discard them on Floci restart initially:
  guest-held API addresses, extension identifiers, and IAM sessions need matching
  host state before recovery across restarts can be supported.
- Test two resume cycles, global variables and `/tmp`, delayed extensions,
  parallel invokes, crashes, logs, code updates, and deletion during capture.

This proposed idle reuse is distinct from AWS SnapStart, which snapshots an
initialized published version for creating new environments. Cloning an already
invoked environment would copy mutable state and credentials; saved connections
also require validation after restore.
[AWS SnapStart](https://docs.aws.amazon.com/lambda/latest/dg/snapstart.html).
