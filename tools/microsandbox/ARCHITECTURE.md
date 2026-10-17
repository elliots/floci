# Native runtime architecture and memory

The target is hundreds of deployed resources with a much smaller active working
set. It does not assume hundreds of simultaneously running application VMs fit
within the host memory budget. Docker remains the default; native execution is
opt-in for Kubernetes pods and Aurora. EC2 remains at the baseline implementation;
new EC2 work is parked separately.

## Shared infrastructure

Floci owns AWS management APIs, workload declarations and wake coordination.
The Go host helper controls native database VMs. A shared Virtual Kubelet
controller owns native pods. They use the same microsandbox SDK adapter and
runtime installation.

The Kubernetes control plane and network peer stay running. A shared relay owns
pod routes, wake proxies and health checks. There is no kubelet or Kubernetes
control plane in each pod VM. The peer is infrastructure per configured network,
not one sidecar per application.

DNS and simulator routing belong to shared infrastructure. Environment isolation
is enforced by Docker network rules or, for the native host helper, the SDK's
network policy. Native Pods currently reject environment isolation because their
shared peer's tunnel forwarding and DNS paths need a protected boundary too.
Guest DNS configuration is routing convenience, not the security boundary.
Native DNS setup runs once at boot or restore, installs kernel routing to the
configured shared resolvers, then exits and removes its temporary binary. There
is no resident DNS server or DNS sidecar per VM.

## Workload state

| State | Active guest RAM | Other retained resources |
|---|---|---|
| Cold declaration | None | Resource metadata, shared wake routes and any bound PVC storage |
| Running Kubernetes pod | One guest allocation for the whole pod | Guest kernel, containerd, application containers and declared init sidecars |
| Running database | Database guest allocation | Guest kernel, database processes and stable proxy |
| RAM-hibernated workload | None | Root disk, checkpoint, ownership metadata and wake routes |

Declared application sidecars remain application processes and count toward pod
resources. They share the pod VM. Floci does not create a separate VM for each
container or inject an observability/service-mesh sidecar into every pod.

Timestamped logs use one small capture process per active application container
inside its existing guest supervisor tree. The measured helper RSS was about
3.1 MiB in the qualification fixture. It sleeps with the VM and retains bounded
output on disk. Events, metrics collection, network listeners and optional
controller supervision are shared host processes, with no extra Pod containers.
Metric scrapes omit sleeping containers and never wake them.

Cold proxy availability lets a deployment become available through a working wake
route. It does not prove its application has booted successfully. Strict readiness
remains available when deployment must verify actual application health.

## Native capacity

The shared Go adapter now coordinates RAM and concurrent native starts across
Aurora and pod-controller processes using the same runtime home. A private
ledger and short filesystem locks reserve capacity before launch or restore.
Compound wakes reserve before removing the stopped source. Unknown runtime
outcomes retain their reservations across helper restarts. This adds no service
or guest process and does not change Docker admission. The Java and pod-specific
budgets remain additional limits. The shared limit covers configured guest RAM,
not Floci's JVM, Docker infrastructure, host page cache or checkpoint disk space.
See the [adapter configuration](../microsandbox-runtime/README.md#shared-native-capacity).

Pod scheduling capacity is separate from active guest RAM. Five hundred cold pods
can retain realistic Kubernetes resource requests without allocating five hundred
VMs. Active pods currently have a 768 MiB minimum guest allocation, including
256 MiB runtime headroom; their application requests and limits can require more.
Reported allocation is an admission reservation, not a measured resident working
set. Guest and host measurements are needed before lowering these defaults.

Full RAM checkpoints trade memory for disk use and restore latency. Image caching,
checkpoint retention, host page cache and filesystem allocation also matter.
Shared disk admission now accounts for cached images, guest disks, checkpoints
and outstanding reservations, with an optional free-space reserve. This is
conservative admission accounting, not a filesystem quota or an automatic
retention policy. Explicit storage maintenance can remove unused RAM backing and
old images using SDK ownership checks. Durable checkpoints remain until their
workload owner releases them; cold Pod base images are retained. Managed
controller logs and credential generations also have bounded retention.

The existing 500-pod fixture validates one synthetic application shape, cold
deployment, selective wake and recovery. It does not establish the footprint or
compatibility of 500 distinct enterprise applications. Small installed graphs
now cover nested HTTP dependencies and a shared Docker/native SQS-to-Aurora
application, including concurrent wakes, shared-budget refusal and zero active
guest allocation between cycles. Broader mixed graphs, retained checkpoint
storage and cleanup backlog still need measurements on Apple Silicon and Linux/KVM.

See [validation evidence](VALIDATION.md), [remaining work](ROADMAP.md) and
[project migration notes](MIGRATION.md).
