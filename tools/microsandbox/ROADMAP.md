# Native runtime completion plan

The target is a large local AWS application with roughly 500 mostly cold
Kubernetes workloads and Aurora databases. Docker remains the default, native execution is
opt-in, and AWS management protocols retain their existing shapes. Proxy
availability is an explicit local Kubernetes policy.

## Workload identity qualification

Native pods support default service-account automount, real Pod-bound TokenRequest
projections, IRSA annotation injection and EKS Pod Identity routing. New identity
checkpoints freeze application processes before capture and refresh projections
before thaw. The shared AWS SDK qualification exercises allowed and denied
operations, wrong audiences and subjects, explicit credential precedence and
identity refresh across sleep. Its optional ten-minute expiry wait checks renewal
after projected tokens expire. This uses the shared controller without a
credential daemon per Pod; see the dated validation record for completed runs.

Reuse Floci's existing OIDC/STS and Pod Identity services. Explicit application
credentials retain their AWS SDK precedence. Target-role chaining and downstream
session-tag ABAC remain limitations described in the EKS service guide.
The contracts are [EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-how-it-works.html)
and [SDK credential precedence](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html).

Multiple application containers and restartable init sidecars are implemented.
Local persistent volumes are implemented. Broader storage and networking
compatibility remain priorities for ordinary Kubernetes application manifests.

## Kubernetes compatibility and capacity

- Implemented: cold proxy availability, HTTP/queue-driven wake, RAM-preserving
  sleep, Pod ownership and stable-state controller recovery, standard probes,
  mounted ConfigMaps/Secrets, and disk/memory `emptyDir`.
- Implemented in the current tranche: independent virtual CPU/memory scheduling
  capacity, allocation sizes derived from Pod requests and limits, CPU shares
  and quotas, memory limits, kernel OOM attribution, and atomic admission against
  the active guest RAM budget. Guest allocations are persisted so recovery and
  RAM restore retain their original sizes. Scheduling requests remain unchanged.
- Application startup now uses OCI image ENTRYPOINT/CMD defaults, Kubernetes
  command/argument overrides and environment expansion. Common security settings
  include UID/GID, non-root checks, read-only root filesystems, privilege
  escalation controls, capabilities, and RuntimeDefault/Unconfined seccomp.
- The controller has a configurable shared Kubernetes API rate limiter for
  registration, status publication and recovery at larger Pod counts.
- Failure recovery preserves OOM/exit diagnostics, rejects uncertain application
  starts, and bounds missing-task detection so failed status publication cannot
  leave a dead application Ready. Memory reservations are released only when
  runtime inspection proves the guest is absent or stopped.
- Sequential init containers, Docker registry pull secrets, and `subPath` /
  `subPathExpr` mounts are implemented in the current compatibility tranche.
- Application lifecycle hooks and projected Downward API resource fields are
  implemented. Current/previous/follow/timestamp-filtered logs retain bounded
  output across sleep and controller recovery; one shared Event broadcaster
  reports lifecycle failures. See [application operations](../microsandbox-pods/OPERATIONS.md).
- Multiple regular containers, restartable init sidecars, projected service-account
  tokens, default automount, interactive exec/resize, localhost port-forward and
  gRPC probes and AWS identity admission are implemented. The shared SDK fixture
  covers identity on both worker backends. Preserve the shared controller and
  one VM per Pod seam instead of adding a kubelet to every guest. `fsGroup` and supplemental groups are implemented. Remaining security work includes
  privileged containers, custom seccomp, AppArmor and SELinux profiles.
- Container CPU and working-set metrics are implemented without waking guests;
  see the [metric semantics](../microsandbox-pods/METRICS.md). Resource compatibility
  still needs Pod-level resources, in-place resizing and resource types beyond
  CPU/memory. Runtime overhead is a
  separate local allocation reserve, not an application resource request.
- Storage: native root disk sizes are configurable and persisted. Shared disk
  admission accounts for image caches, guest disks, checkpoints and outstanding
  reservations, with an optional free-space reserve. It is admission control,
  not a filesystem quota. Preview-first cleanup can reclaim unused runtime RAM
  backing and opt-in old images while preserving guest/checkpoint ownership;
  durable checkpoints are not age-evicted. See [storage maintenance](../microsandbox-runtime/STORAGE.md). The opt-in
  [local persistent-volume provisioner](../microsandbox-pods/STORAGE.md) adds
  node-bound Filesystem RWO disks, guarded single-Pod attachments and Delete/Retain
  reclaim. CSI drivers, expansion and broader access modes remain unsupported.
- Networking: environment egress isolation and simulator routing are implemented.
  Reconcile policy changes, then broaden
  NetworkPolicy and networking compatibility. DNS overrides alone do not prevent
  direct-IP egress.

## Aurora and other AWS workloads

- The retained RDS provider runs PostgreSQL and MySQL, including Aurora Serverless v2
  scale-to-zero. Aurora zero-capacity engine/version eligibility and configurable
  native root disk sizing are implemented. Database recovery now checks durable
  ownership and immutable VM/checkpoint identities without eagerly waking the
  guest. AWS allocated-storage resizing and MySQL logical snapshots remain
  unsupported. See the [recovery migration](AURORA_RECOVERY_MIGRATION.md) and
  [engine-version migration](AURORA_VERSION_MIGRATION.md).
- Native execution work is limited to Kubernetes pods and Aurora. EC2, Lambda,
  ECS and ElastiCache retain their existing backends. All new EC2 work is parked
  in `parked/microsandbox-ec2-20261017`; baseline EC2 remains unchanged.

## Operating hundreds of applications

- Shared guest RAM admission now coordinates pod and host runtimes. Container
  working-set metrics are available without waking sleeping guests. Broaden
  policies for workloads waiting on dependencies; do not evict active
  applications merely because another wake is queued.
- Interrupted native commands now have durable terminal receipts. Startup clears
  a receipt only after read-only proof of its owner and runtime outcome; it does
  not replay mutations. Ambiguous outcomes continue to fail closed.
- Kubernetes reconciliation now has eight workers by default, configurable from
  one to 64 independently of VM-start admission. Virtual Kubelet can remove cold Pod API
  objects before provider cleanup finishes; deleting hundreds of pods queues
  relay and runtime ownership checks alongside new Pod starts. Zero active guest
  RAM does not mean that cleanup queue has drained. Qualification now waits for
  the owned Pod journals to disappear and records cleanup time.
- The setup helper provides repeatable owned-cluster installation, a restricted
  controller identity, an optional portable background supervisor, automatic
  certificate renewal, diagnostics and explicit shared disk/RAM reporting.
  Shared admission bounds new operations; unused cache cleanup is explicit and
  durable checkpoints remain until their workload owner releases them.
- Qualify Linux hosts as well as Apple Silicon, and test real application graphs,
  persistent client connections, concurrent wakes and failure recovery.

Native Linux qualification has a manual GitHub Actions entry point,
`Microsandbox Native Linux Qualification`, for a trusted self-hosted runner
labelled `linux` and `microsandbox-native`. The runner needs readable/writable
`/dev/kvm`, Docker, Python 3, OpenSSL, a C compiler and microsandbox 0.7.8 with its
matching native runtime. Set repository variable `MICROSANDBOX_EXECUTABLE` when
the CLI is not `/usr/local/bin/msb`, and optionally `MICROSANDBOX_NODE_IP` for an
explicit peer-reachable host address. It builds current helpers and runs the
existing Pod and Java wake qualification in a dedicated runtime home. It retains
diagnostic state and publishes the JSON result. This workflow is not a completed
Linux qualification; the current development host has no KVM device.

Before the SDK migration, the expanded Apple Silicon qualification completed
49 native checks, including 500 native-backed Pod identities with real scheduling requests, cold proxy
availability, deferred init execution, selective wake and controller recovery.
A separate post-rebase native fixture passed 73 Java gateway and contract tests.
Controller race tests and vet passed, and 192 controller unit tests passed on
Linux ARM64. The fixture uses one application shape and does not establish
native Linux runtime support or compatibility, storage use and performance for
500 different applications. See the measured evidence in the
[Kubernetes guide](../../docs/configuration/microsandbox-kubernetes.md) and project
changes in [MIGRATION.md](MIGRATION.md) as each step lands.

The SDK migration subsequently passed 53 native checks and 73 Java tests using
five proxy-ready pods and two cold declarations. The 29 shared RDS cases passed
on both Docker and microsandbox. EC2 qualification belongs to the parked branch. See the dated
[validation record](VALIDATION.md) for platform coverage and remaining limits.

The 17 October SDK-backed rerun passed 70 native checks with 500 proxy-ready
replicas, selective Service/PodIP HTTP wake, recovery and complete ownership
cleanup. A separate shared AWS SDK run passed both identity methods on Docker
and native workers, plus expired projected-token renewal after native sleep.
Generic explicit-host networking also passed a native smoke and Linux peer
kernel checks. The shared persistent-volume fixture passed replacement,
completed-Job writeback, Delete/Retain reclaim, native RAM restore and controller
recovery. These results still do not establish mixed 500-application graphs or
native Linux/KVM operation.
