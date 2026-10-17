# Native Pod architecture

One shared Virtual Kubelet host process maintains genuine Kubernetes leases.
Each opted-in Pod UID owns a native microsandbox VM with containerd/runc and its
actual OCI application. There is no guest kubelet. The pinned k3s image supplies
runtime binaries. This experimental tool is separate from the published Quarkus
sidecars and their contract. Kubernetes retains Deployment, ReplicaSet and Pod
identity. Standard logs/exec use mutual TLS and Kubernetes authorization.

## Lifecycle and ownership

The annotation `microsandbox.dev/on-demand: "true"` creates a journal-only COLD
Pod: Pending, not Ready, with no VM, disk or RAM allocation. Its first resume boots
the application. Unannotated opted-in Pods start normally. Cold and sleeping
monitors wait on wake channels instead of polling guests.

The VM primary process supervises containerd and foreground `ctr run`. Short
runtime exec calls submit commands and inspect tasks. Long-lived managed exec
jobs prevent full snapshots, so application execution belongs to the primary
process tree. The original supervisor, logs and exit status survive restore.
Always/OnFailure restart observes actual exits. Completed terminal tasks stop
their VM and release active RAM.

Operations serialize with probes, logs, exec and deletion. Hibernate withdraws
actual application readiness, pauses the VM, captures and verifies an integrity checkpoint, journals
CHECKPOINTED, stops the VM, verifies Stopped, and journals SLEEPING. Only then is
memory admission released. Resume requires the exact owned stopped source and
recorded immutable snapshot ID. Source removal is a one-use replay fence. The
controller journals RESUMING before removal, restores RAM, verifies provenance,
then waits for real task state and readiness. Connections may need to reconnect.
The latest checkpoint remains; older checkpoints are deleted by immutable ID.
Two checkpoints can exist during a transition.

Microsandbox 0.7.8 drops labels and network rules on restore. Restored ownership
requires the recorded snapshot parent in both stored and active configuration;
surviving labels must match. Original sources require labels and no snapshot
parent. The controller reapplies network rules on each restore.

The private state directory contains a locked cluster/node/peer identity and
atomic fsynced Pod journals. Resolved environment values, including Secrets, are
stored in mode 0600 files inside a mode 0700 directory. Normal shutdown preserves
guests; `--cleanup-on-exit` is for disposable fixtures. Stable COLD, sleeping and
running states support restart adoption. Missing or ambiguous sources fail
closed; terminal and failed states cannot replay old checkpoints.

An interrupted initial launch or container restart is also ambiguous: the VM
may be running before its application was submitted. Recovery marks that Pod failed rather than
claiming an empty VM is a running application. Replace the Pod to recover it.
Completed container status, including its exit code and `OOMKilled` reason, is
preserved across controller restarts.

Application request and exit markers are published only after successful writes.
If a running task disappears without a completed exit marker, readiness is
withdrawn immediately. The controller permits a 30-second status handoff before
failing the Pod. Neither automatic nor explicit hibernation can hide that missing
task. Failed start reservations are released only when a fenced runtime inspection
proves the guest is absent or stopped; uncertain or still-running guests remain
charged until their lifecycle is resolved.

Mutating SDK operations finish under the lifecycle lock after client disconnect.
A durable `native-command.pending` marker precedes each mutation and is removed
only after successful completion. A controller crash during a command or uncertain
runtime failure leaves that marker. Startup, mutations and deletion then stop until
an operator reconciles the native outcome. Blind marker deletion can replay work
or miss a late-created VM. Automatic interrupted-command recovery remains future
work.

## Networking

An owned bridge in a Docker k3s worker represents the virtual node's /22 Pod CIDR.
Each VM has a unique IPv4 PodIP and VXLAN attachment. Outer UDP8472 travels through
per-VM host relays and exact-peer destination translation inside owned guests.
VXLAN learning is disabled because relays rewrite outer source addresses; the
bridge MAC stays stable. Existing worker routing supplies ClusterIP and DNS.
Host routes and unrelated containers are untouched.

Qualification verifies Pod/Service/DNS traffic and original source Pod IPs.
This is a specific k3s/IPv4 topology. NetworkPolicy is unsupported: create/resume
reject policies selecting the Pod, but policies added while it runs are not
dynamically enforced. Use a separate development cluster without NetworkPolicy
requirements. IPv6, arbitrary CNIs and additional interfaces remain unsupported.

## Optional shared proxy availability

Strict mode remains the default: readiness follows the guest application. A Pod
template with `microsandbox.dev/availability: "proxy"` instead registers COLD and
reports Ready only after real shared TCP listeners and their owned Linux routes
exist. The controller records actual guest readiness separately. Its lifecycle
API still reports COLD or SLEEPING until application activation; projected Pod
readiness does not fabricate a running VM.

The Linux `network-relay` process runs once in the owned Docker peer. It binds
public PodIP aliases and forwards declared TCP ports to shared host listeners.
This socket remains reachable after kube-proxy translates a ClusterIP to PodIP.
The host gate defaults to HTTP request processing, selected by
`microsandbox.dev/proxy-protocol: http`. Each HTTP/1 or prior-knowledge
cleartext HTTP/2/gRPC request acquires activity, cold-boots or restores the exact Pod UID and waits
for actual app readiness before forwarding. The request retains its lease through
the complete response body or upgraded stream. Idle frontend keepalive connections
do not acquire activity and can remain open while the VM sleeps; the next request
wakes it again. The relay and HTTP handling are shared, with no resident proxy
sidecar in each pod. The backend protocol matches the incoming request protocol;
there is no automatic HTTP/2-to-HTTP/1 translation.

Explicit `proxy-protocol: tcp` retains the previous raw TCP/TLS path. Its
connections hold activity until both directions finish, so idle connections
prevent hibernation. HTTPS needs TLS termination before the HTTP listener or this
TCP passthrough mode. The mode applies to all declared proxy ports. Admission
remains bounded and startup waits have a four-minute deadline.

Proxy guests use a separately reserved private /22, default `10.42.204.0/22`,
inside the cluster Pod CIDR. Their public PodIP remains on the shared relay.
This mode translates client source addresses and supports 1..16 declared TCP
ports. UDP, SCTP, transparent source preservation and arbitrary CNIs are not
supported. Direct strict-mode VXLAN traffic retains source Pod IPs. OrbStack's
peer-to-host loopback reachability is qualified; other Docker hosts must verify
that path before using proxy mode. Listeners require a Linux relay binary built
for the peer architecture, supplied through `--network-relay-binary`. Before
advertising availability, a bounded challenge verifies peer-to-host loopback
reachability without activating an application. Setup uses eight registration
slots, independently of the two VM boot/restore slots and active RAM admission.

`microsandbox.dev/idle-owner: "controller"` is the default for proxy Pods, with
`microsandbox.dev/idle-timeout-seconds: "120"`. The controller hibernates stable running
guests after inactivity, guarded atomically against `activeRequests` and
`activeConnections`. An app that remains unready also releases RAM after its failed
requests drain. External queue-aware coordinators must set `idle-owner: "external"`;
they own idle decisions and observe both counters plus actual lifecycle state.
The entry's `activitySequence` also advances on activity acquisition/release,
allowing an external poller to notice work completed between observations.
HTTP requests and response/upgrade streams use `activeRequests`; TCP-mode and
port-forward sessions use `activeConnections`. There must be one idle owner.
Direct HTTP health requests count as requests, and TCP-mode connections count
as connections; the proxy does not infer health URLs. Kubernetes probes do not
wake sleeping guests or renew normal idle time. This proxy does not invent health
responses. Deployment/Helm status checks can complete from proxy availability
without waking the application. Container waiting reasons still identify COLD or
hibernated guests rather than inventing a running application process.

A shared watcher verifies exact relay ownership, listener tuples and live accept
loops. Missing routes withdraw projected readiness even during a blocked lifecycle
operation. Its bounded idle jobs do not block health checks for other Pods.
Shutdown drains host sessions before taking lifecycle locks. Recovery accounts
every journal's RAM before enabling listeners, then replays current status after
Virtual Kubelet registers its notification callback. Stable sleeping Pods need
that replay because the asynchronous provider does not poll their status.

## Application probes

Startup success and pending probe-triggered termination are journaled separately
from projected availability. Startup gates readiness/liveness; successful startup
is not replayed after RAM restore. Fresh container starts reset every probe.
Readiness thresholds are based on actual results, never on proxy Ready status.
Cold/sleeping monitors execute no probes and internal probes do not acquire wake
leases or update activity timestamps.

Exec checks use the real container task. HTTP/HTTPS/TCP checks run the Linux
relay binary's stateless `probe` command through Docker exec, targeting the
actual guest address. They bypass both the public wake relay and native host
port forwards, whose TCP accept alone would not prove an application listener.
No extra guest listeners or per-Pod host ports are allocated. The shared relay
advertises probe support so an old installed binary is rejected.

Probe failure sends SIGTERM, honors termination grace, and escalates to SIGKILL.
Task exit follows the existing Pod restart/backoff policy. The failure reason is
persisted before signaling, and an unhealthy task awaiting termination cannot
hibernate. Network probe transport errors are logged without counting as application
failures. Checks are serial within the per-Pod lifecycle lock; slow checks can
delay other checks. Kubernetes probe Events and separate probe workers remain
future work.

The extended 2026-10-16 native fixture passed 25 checks, including HTTP readiness
on an undeclared port, named TCP startup probes, startup/liveness gating,
probe-triggered restart, `Never` termination, startup success across RAM restore
and controller crashes, and cached image restart with registry DNS unavailable.
Lazy proxy traffic still woke guests and allowed idle hibernation with probes
enabled. The 69 Floci Java gateway/client/coordinator tests and cleanup passed.
Go race tests cover HTTP status, TLS, redirects, timeouts, thresholds and image
pull policies. This extended run used two lazy replicas; it did not repeat the
500-replica scale experiment below.

## Feature inventory

| Area | Implemented | Remaining |
| --- | --- | --- |
| Lifecycle | Real UIDs, COLD, RAM sleep/resume, restart adoption, delete | Interrupted-command reconciliation, in-place updates |
| OCI | Image ENTRYPOINT/CMD/USER defaults, command/args overrides and expansion, working directory, env and ConfigMap/Secret env references, image pull policies | Registry credentials |
| Status | Real task state; exec/HTTP/HTTPS/TCP/gRPC readiness, startup and liveness; Always/OnFailure/Never | Probe host overrides, independent probe workers, hooks |
| Logs/exec | Actual logs, tail/byte limit, standard exec with TTY/resize, localhost port-forward | Follow, previous, timestamps, rotation, attach |
| Network | Strict PodIP/Service/DNS with source IP; opt-in HTTP wake proxy and explicit TCP compatibility mode | NetworkPolicy, IPv6, arbitrary CNIs |
| Containers | Multiple regular OCI containers with sequential init containers | Restartable init sidecars, ephemeral containers |
| Volumes | Disk/memory emptyDir, ConfigMap/Secret mounts, subPath/subPathExpr, atomic directory refresh | Projected tokens, PVC/CSI, host paths, disk quotas |
| Security | VM boundary, mTLS/RBAC, token control, ownership fences, UID/GID, non-root validation, read-only root, privilege escalation, capabilities and RuntimeDefault/Unconfined seccomp | fsGroup, supplemental groups, privileged containers, custom seccomp, SELinux and AppArmor profiles |
| Resources | Request/limit-derived profiles, CPU shares/quota, memory cgroups, kernel OOM attribution, bounded starts and active RAM budget | Metrics, eviction and shared host budgets |
| Wake | Generic UID API, cold boot, retained replicas, Service HTTP request wake and explicit TCP connection wake | Protocol-aware health responses, UDP |
| Scale | Configurable tracked count and RAM, bounded checkpoints | 500 checkpointed apps, disk admission, shared host budget |

Defaults: 512 tracked Pods and 4096 MiB active RAM. Guests have a 4 GiB flat root
disk and at least one vCPU/768 MiB. New sizes follow the greater of CPU/memory
requests and limits, with 256 MiB guest headroom and 2 MiB memory alignment.
Sleeping Pods retain snapshots and disk. Cold/sleeping Pods retain Kubernetes
resource requests; `--scheduling-cpu` and `--scheduling-memory` advertise virtual
capacity independently of active memory admission. Their defaults remain four
CPUs and the active RAM budget. This controller budget is separate from any emulator budget. A
500-COLD-identity or proxy-Ready test does not establish 500 running or checkpointed app capacity.

Single-application Pod journals use version 2, or version 3 with sequential init containers. Multiple applications use version 4 with separate per-container start, probe-failure, termination and backoff state. Older controllers reject version 4; do not downgrade while those Pods exist. All versions after version 1 require an allocation. Version 1 journals
migrate as their original one-vCPU/768-MiB size, regardless of current sizing
rules. Older controllers reject version 2 rather than undercounting larger
guests. Admission grants both a start slot and sufficient RAM together; a large
waiter cannot occupy a start slot while a smaller guest could fit.

Each fresh application process receives a unique parent cgroup. CPU/memory limits
apply to its child. A private cgroup namespace and read-only mount expose the
application's own limits. The parent retains hierarchical `memory.events` counters
after `ctr run --rm` deletes that child. The supervisor reads `oom_kill` before
atomically publishing the exit status. Positive kernel evidence reports
`OOMKilled`; exit 137 alone does not. Missing accounting fails explicitly. The
parent and counters survive RAM restore and are not reused for later restarts.

## Volume lifecycle

Volume backing directories live in the Pod VM outside the application's
containerd snapshot. Replacing an application container therefore preserves
disk and memory `emptyDir` contents; deleting the Pod removes its VM and volume
identity. Memory `emptyDir` is a tmpfs with its configured capacity, at most the
guest RAM allocation. Disk `emptyDir` shares the VM's 4 GiB disk and currently
rejects `sizeLimit` because there is no disk quota or eviction implementation.

ConfigMap and Secret sources are read from the Pod's namespace through the
controller's Kubernetes client. Required sources are checked before first VM
allocation. Strict pods wait Pending for missing sources; a proxy pod can retain
its COLD listener and retry activation on later traffic. Optional absent sources
or keys are omitted. File projection supports nested paths, binary bytes and
per-file modes, and application mounts are read-only. Secret backing uses tmpfs.
The pre-allocation `WAITINGVOLUMES` state is journaled separately from `STARTING`;
controller recovery continues waiting without replaying an uncertain VM creation.

An update builds a new private generation and atomically swaps the volume's
`..data` symlink. This preserves Kubernetes' reopen-to-refresh behavior without
rewriting files that an application already has open. Each volume is independent;
this is not an atomic transaction across several volumes or file opens. API
errors and missing required sources retain the last successful projection.

The controller refreshes about every 30 seconds while awake and attempts a
refresh after RAM restore before forwarding wake-proxy traffic. It does not
poll cold/sleeping pods. The VM's application processes have already resumed
before that post-restore refresh, and source failures preserve old contents.
RAM checkpoints include Secret tmpfs data and any credentials in process memory;
the controller's private state and runtime directories must remain protected.
Checkpoint encryption is not implemented.

Directory mounts update atomically; supported subpaths pin their selected content.
Combined projections support ConfigMap, Secret, DownwardAPI metadata and real
Pod-bound TokenRequest credentials. The shared controller caches tokens only in
memory and rotates at 80% TTL or 24 hours. Sleep does not poll the API. Wake issues
fresh identity before VM restore and writes it before forwarding traffic;
running-controller recovery requires identity refresh before opening controller proxy routes.
Background processes resume before the write, so they may briefly see an expired
snapshot token. Token subpaths remain pinned, as in Kubernetes.
Host paths, PVC/CSI, certificate projections, resource-field projections, mount
propagation, recursive read-only options and ownership changes are rejected.
See the [migration notes](../microsandbox/MIGRATION.md#kubernetes-mounted-configuration-and-temporary-storage)
for manifest changes.

## Qualification measurements

Measured on an Apple Silicon macOS/OrbStack development host with microsandbox
0.7.8 on 2026-10-16. One complete run passed 42 fixture checks, 71 Java tests and
cleanup. Its 500 lazy replicas retained requests totaling 500 CPUs and 375 GiB
while the node advertised 500 CPUs/384 GiB and admitted at most 4 GiB of active
guest RAM. These are fixture observations, not latency guarantees.

| Check | Observed result |
| --- | --- |
| One Deployment with 500 replicas of the same HTTP fixture | All proxy-Ready and COLD in 51.466 seconds, zero app VMs or allocated guest RAM |
| Shared processes at 500 cold replicas | 113.797 MiB controller RSS plus 12.770 MiB Linux relay RSS, 126.566 MiB combined |
| First Service request | One app booted in 23.740 seconds; the other 499 remained COLD |
| Controller recovery with 500 replicas | Lifecycle health endpoint available in 64.348 seconds |
| Service request restoring the sleeping proxy fixture | 2.075 seconds, with the original kernel boot identity and RAM marker |
| Two strict-mode RAM hibernations | 2.442 and 2.194 seconds |
| Two strict-mode resumes through actual application readiness | 3.713 and 4.648 seconds |
| Allocated snapshot disk bytes for those cycles | 616,456,192 and 619,798,528 bytes for a 768 MiB VM |

The 64.348-second recovery sample included 499 cold replicas, one sleeping
replica and a separate running native client Pod. It measures recovery until the
lifecycle health endpoint responds, not publication of every Kubernetes Ready
condition. The fixture verified restored proxy routing and RAM retention after
that recovery.

Checks covered image defaults and overrides, security settings, CPU/memory
limits, kernel OOM attribution, missing exit status, probes, mounted sources,
logs/exec, Pod source addresses, ClusterIP/DNS, listener loss, active-stream
protection, auto-idle and sleeping/restored adoption. The 71 Java tests included
direct proxy wake adoption and two gateway hibernate/resume cycles with unchanged
Deployment and Pod UIDs; there were no failures, errors or skips. Owned VMs,
checkpoints and the Docker fixture were removed successfully. Separately, 156
Linux unit tests, the macOS Go race suite and `go vet ./...` passed. Native Linux
runtime execution remains unqualified.

RSS excludes Kubernetes, Docker, Floci, host overhead and filesystem caches.
Timings include readiness and verification work. These results do not establish
compatibility or capacity for 500 distinct applications or 500 retained RAM
checkpoints; previously activated apps also require private disks and checkpoint
storage. Working sets, image downloads and concurrent wakes affect real workloads.

References: [Virtual Kubelet](https://github.com/virtual-kubelet/virtual-kubelet),
[provider contract](https://virtual-kubelet.io/docs/providers/),
[microsandbox snapshots](https://github.com/superradcompany/microsandbox/blob/v0.7.8/docs/sandboxes/snapshots.mdx),
[microsandbox networking](https://github.com/superradcompany/microsandbox/blob/v0.7.8/docs/networking/overview.mdx).
