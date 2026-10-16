# Native runtime completion plan

The target is a large local AWS application with roughly 500 mostly cold
Kubernetes workloads and Aurora databases. Docker remains the default, native execution is
opt-in, and AWS management protocols retain their existing shapes. Proxy
availability is an explicit local Kubernetes policy.

## Workload identity qualification

Native pods support default service-account automount, real Pod-bound TokenRequest
projections, IRSA annotation injection and EKS Pod Identity routing. New identity
checkpoints freeze application processes before capture and refresh projections
before thaw. Validate these paths with real AWS SDK credential providers, allowed
and denied operations, wrong audiences and subjects, and credentials expiring
while asleep. Keep the shared controller and avoid a credential daemon per Pod.

Reuse Floci's existing OIDC/STS and Pod Identity services. Explicit application
credentials retain their AWS SDK precedence. Target-role chaining and downstream
session-tag ABAC remain limitations described in the EKS service guide.
The contracts are [EKS Pod Identity](https://docs.aws.amazon.com/eks/latest/userguide/pod-id-how-it-works.html)
and [SDK credential precedence](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html).

Multiple application containers and restartable init sidecars are implemented.
Persistent storage and networking compatibility remain priorities for ordinary
Kubernetes application manifests.

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
- Multiple regular containers, restartable init sidecars, projected service-account
  tokens, default automount, interactive exec/resize, localhost port-forward and
  gRPC probes and AWS identity admission are implemented. Qualify the complete identity flow. Preserve
  the shared controller and one VM per Pod seam instead of adding a kubelet to
  every guest. `fsGroup` and supplemental groups are implemented. Remaining security work includes
  privileged containers, custom seccomp, AppArmor and SELinux profiles.
- Resource compatibility still needs Pod-level resources, in-place resizing,
  resource metrics, and resource types beyond CPU/memory. Runtime overhead is a
  separate local allocation reserve, not an application resource request.
- Storage: add persistent volumes and a defined local CSI story;
  then disk sizing, quotas and image/checkpoint storage accounting.
- Networking: environment egress isolation and simulator routing are implemented.
  Reconcile policy changes, then broaden
  NetworkPolicy and networking compatibility. DNS overrides alone do not prevent
  direct-IP egress.

## Aurora and other AWS workloads

- The retained RDS provider runs PostgreSQL and MySQL, including Aurora Serverless v2
  scale-to-zero. Remaining work includes engine/version eligibility checks for
  Aurora auto-pause, storage sizing and backup limitations.
- Native execution work is limited to Kubernetes pods and Aurora. EC2, Lambda,
  ECS and ElastiCache retain their existing backends. All new EC2 work is parked
  in `parked/microsandbox-ec2-20261017`; baseline EC2 remains unchanged.

## Operating hundreds of applications

- Shared guest RAM admission now coordinates pod and host runtimes. Add working-set
  metrics and policies for workloads waiting on dependencies; do not evict active
  applications merely because another wake is queued.
- Make interrupted native-command recovery automatic where ownership and runtime
  outcome can be proved. Ambiguous outcomes must continue to fail closed.
- Improve bulk Pod cleanup throughput. Virtual Kubelet can remove cold Pod API
  objects before provider cleanup finishes; deleting hundreds of pods queues
  relay and runtime ownership checks alongside new Pod starts. Zero active guest
  RAM does not mean that cleanup queue has drained.
- Provide repeatable installation, least-privilege cluster manifests, diagnostics
  and explicit disk/RAM reporting. Shared image caching and checkpoint retention
  policies are needed to bound disk use across hundreds of previously used apps.
- Qualify Linux hosts as well as Apple Silicon, and test real application graphs,
  persistent client connections, concurrent wakes and failure recovery.

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
