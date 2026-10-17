# SDK and shared-runtime validation, 2026-10-16

The implementation through `4a7c6c8d5` was checked on Apple Silicon with
Docker/OrbStack, Java 25, Go 1.26 and native microsandbox 0.7.8. This records the
tested scope; it is not a full repository test run or a claim of feature parity.

This is a historical record for the retained database and Kubernetes work. All
new Docker EC2 work is parked in `parked/microsandbox-ec2-20261017`. The original
native EC2 implementation and its validation record remain in
`backup/microsandbox-before-ec2-removal-20261017`. Baseline EC2 is unchanged here.
Current native work targets Kubernetes pods and Aurora.

## Shared AWS and workload contracts

| Backend | Cases | Result |
| --- | ---: | --- |
| Docker | 29 database cases in 11 suites | Passed, no failures, errors or skips |
| Microsandbox | The same 29 database cases | Passed, no failures, errors or skips |

The final Docker run used automatic tag discovery in a clean worktree. The
retained database cases exercised PostgreSQL and MySQL, including repeated
pause/resume after ordinary stop/start. Cleanup left no owned database containers,
volumes or native VMs. The shared suites exposed and now cover MySQL remote root
password rotation. Focused unit and integration checks for that fix also passed.
The [test guide](../runtime-tests/README.md) lists the contracts and exclusions.

## SDK adapter and host helper

Race tests, `go vet` and builds passed for the shared SDK adapter, Go HTTP host
and pod controller on macOS ARM64 and Linux ARM64. The Linux relay and health
probe also built with CGO disabled. Platform-specific unit tests skip where
their required kernel facilities or Docker peer are absent; this is separate
from the zero-skip shared AWS matrix above.

The Linux checks loaded the matching SDK library but did not run native VMs on
Linux/KVM. Native VM behavior was qualified on Apple Silicon only.

The native on-demand integration case passed HTTP, SQS, RabbitMQ and IoT MQTT
wake with RAM preservation. The 42 focused Java tests for command reconciliation,
database lifecycle and the helper HTTP contract passed. Python probe tests,
Checkstyle, documentation checks and the partition-literal check also passed.

## Native Kubernetes qualification

The SDK-backed pod controller passed all 53 checks in `qualify.py`, followed by
73 Java gateway and contract tests with no failures, errors or skips. Coverage
included init containers, private image pull and secret rotation, volume subpaths,
probes, logs, exec, security settings, networking, cold proxy readiness, deferred
initialization, RAM resume and controller crash recovery. Cleanup passed, and the
dedicated runtime home contained no VMs or RAM snapshots afterwards.

This rerun used two cold declarations and five proxy-ready pods, with 8 CPUs and
8 GiB of advertised scheduling capacity. All five replicas became Ready without
allocating a native VM. It was not a fresh 500-pod scale qualification; the earlier
500-pod result predates the SDK migration.

For this small fixture, first Service wake took 26.356 seconds including deferred
init execution; Service RAM resume took 1.559 seconds. Two separate 768 MiB guest
cycles took 2.143 to 2.321 seconds to hibernate and 3.010 to 3.300 seconds to resume,
including the fixture's verification calls. These are observations from one local
run, not latency guarantees for other images or application graphs.

Build and configuration changes are recorded in [MIGRATION.md](MIGRATION.md).

## Isolated native networking: 17 October 2026

The explicit native adapter regression passed on Apple Silicon against SDK
0.7.8, including two full RAM restores and an ordinary cold restart. Controlled
DNS and allowed local HTTP worked; unrelated host HTTP, external direct-IP
HTTP and the SDK's default DNS path were blocked. Restore was tested with both
the captured policy and an explicit allow-all request clipped to the environment
boundary. Changing the network-file identity prevented adoption and execution.

The guest had no resident DNS process, and the temporary DNS installer and
configuration files were removed after each boot or restore. VM and checkpoint
cleanup passed, leaving the dedicated runtime home empty. A separate native
check verified public CA mounting and cancellation with an observed command exit.
An additional regression injected an unexecutable DNS installer during cold start
and fresh launch. Both failures stopped the exact owned VM and rejected subsequent
execution or restart through the otherwise valid client.

Guest nftables routing also passed Docker Linux kernel tests for TCP/UDP,
multiple resolvers, retargeting an existing UDP socket and preserving unrelated
tables. This is not native Linux/KVM VM qualification.

## Managed cluster setup: 17 October 2026

On Apple Silicon, the new setup helper built all five host/guest binaries and
created its owned k3s peer. Its restricted controller registered the virtual node,
published a cold Pod as Ready with zero active guest RAM, woke it on HTTP and
hibernated it back to zero RAM. After controller shutdown, repeated setup renewed
credentials without changing ownership. Restart adopted the sleeping Pod without
waking it; the next HTTP request returned the same in-memory UUID. Pod cleanup
drained and `destroy` removed the empty owned peer.

Real Kubernetes authorization checks denied Secret creation, Deployment creation,
RBAC binding creation and modification of another node, while permitting required
Pod reads. Seven Python tests cover file permissions, setup exclusion, ownership
and image validation, cleanup refusal and role boundaries. This is a local
development-cluster workflow, not a production Kubernetes installer.

## Capacity, database lifecycle and selective wake: 17 October 2026

After the shared disk-admission, immutable database recovery and Aurora version
checks landed, the same database matrix passed again on both Docker and native
microsandbox: 29 cases per backend with no failures, errors or skips. The matrix
includes PostgreSQL and MySQL. This is the shared workload matrix, not the full
repository test suite.

The packaged-Floci restart fixture also passed with persistent storage. After
the actual 300-second Aurora idle interval, ordinary JVM shutdown and restart
retained the stopped guest, immutable RAM checkpoint and AWS endpoint. Metadata
reads left it asleep; first SQL restored the same PostgreSQL process and its
committed row. This run measured 0.882 seconds for cold wake after JVM restart
and 0.630 seconds for RAM wake, with the artificial resume delay disabled. Its
6 GiB native root geometry survived recovery. Cleanup left no VMs or checkpoints.
These are fixture observations, not latency guarantees. The script also accepts
hybrid storage, but this run used persistent storage only.

The native disk-admission fixture denied launch, capture and restore before
mutation when the budget was exhausted. It preserved a full RAM checkpoint,
retained a prepared restore reservation across a lowered limit and verified
cleanup of VMs, checkpoints and permits. Measured runtime storage remained
accounted after cleanup. Admission uses conservative logical sizes, not assumed
copy-on-write savings, and does not impose a filesystem quota.

A separate two-replica native Deployment ran with capacity for only one guest.
Real Java HTTP and queue wake selected one replica, health polling did not retain
it awake, and two hibernate/resume cycles preserved its RAM. The other replica
remained cold with the same Pod UID. Cleanup passed. This run covers selected
replica activation; ordinary Kubernetes Services still choose endpoints using
their normal routing policy.

Race tests and vet passed for the shared runtime, host helper and pod controller.
A repeated cancellation regression found and fixed an input-stream ownership
leak when forwarding was canceled during dialing. The setup helper's ten Python
tests passed, including explicit host-IP selection and unavailable-capacity
reporting. Native VM evidence remains Apple Silicon only; Linux kernel networking
checks and cross-platform builds do not establish native Linux/KVM qualification.

## SDK-backed 500-pod qualification: 17 October 2026

The expanded fixture passed 70 native checks and cleanup with controller
`b375c17c0`, including the SDK adapter, bounded reconciliation workers and terminal
command recovery. It advertised 600 CPUs/512 GiB for scheduling while admitting
at most 4 GiB of active guest RAM. All 500 proxy-ready replicas retained their
requests for 500 CPUs/375 GiB without creating any VM.

| Observation | Result |
| --- | ---: |
| Register 500 cold, Ready replicas | 50.740 seconds |
| Controller and shared relay RSS | 183.6 MiB combined |
| First Service wake, including deferred init | 18.718 seconds |
| Service RAM resume | 5.556 seconds |
| Controller recovery with 500 declared replicas | 56.180 seconds |
| Deployment deletion through complete ownership-journal cleanup | 83.369 seconds |

The memory figure excludes Floci, k3s, Docker, runtime libraries, host page cache
and active guests. This is 500 replicas of one synthetic application, not 500
different applications or 500 checkpointed guests. The run also checked
multi-container Pods, sidecars, projected identity, private images, probes,
volumes, security settings, ordinary Service/PodIP HTTP wake, idle keep-alive
sleep and two strict-readiness RAM cycles. It did not run the Java suite.

The later reused-guest-IP network fix, generic host binding and persistent-volume
work have separate qualification; this scale result does not claim those later
changes were included in the 500-pod binary.

## Generic host networking: 17 October 2026

The controller uses the explicit node address and verifies peer reachability.
It can bind a specific local interface when host loopback is not reachable,
without exposing the SDK backend or controller control API. Owned UDP ingress
accepts only the peer address, the Pod's source port and its VXLAN identity.
Linux Docker namespace checks passed forwarding and rejection cases. A separate
kernel check covered legacy route migration, reused guest addresses and cleanup
that preserves other Pods' routes.

A real Apple Silicon smoke with controller `be184d03c` passed seven checks:
cold Ready without a VM, Service HTTP wake, full RAM sleep to zero active guest
memory, controller restart retaining sleep, and HTTP resume with the same process
nonce, start time and kernel boot ID. Guest, snapshot and volume inventories were
empty after deletion, and the owned peer was removed. The fixture stopped its
controller before Kubernetes finished deleting the Pod API object; after proving
its exact UID and empty native inventory, cleanup removed that remaining API
object. No native guest force cleanup was needed.

The generic-networking controller passed 303 top-level tests and 471 subcases
under race detection, plus vet. Eight host/platform/opt-in tests and nine subcases
were skipped. These Linux peer tests and Apple Silicon VM checks do not establish
native Linux/KVM qualification. No vendor-specific Docker hostname is required.

## Shared AWS SDK credential providers: 17 October 2026

One AWS Go SDK probe passed on both Docker k3s and native Pods for IRSA and EKS
Pod Identity. The same assertions checked the expected provider, assumed-role
identity, an allowed SSM parameter, denial of a different existing parameter and
explicit environment-credential precedence. Eight negative cases covered wrong
audiences and service-account subjects on both backends; network errors could
not satisfy those assertions.

Both native processes hibernated with zero active guest RAM while their actual
ten-minute projected tokens expired. After controller restart, the first HTTP
request resumed the same process nonce with a new projected token and fresh SDK
credentials. This proves successful refresh on the first request after resume;
the probe itself does not independently establish before-thaw ordering. The
probe shortens the SDK cache window rather than waiting for the one-hour AWS
session to expire. All native guests and the disposable cluster were cleaned.

The Java fixture passed with no failures, errors or skips. It served real Floci
OIDC/STS, Pod Identity and IAM enforcement using the disposable cluster's public
signing key. The test calls the real admission handler and applies its patch,
but does not qualify installation of the TLS webhook. Native controller
`8ba7973df` included the reused-guest-IP routing fix. This was Apple Silicon
qualification, not a native Linux/KVM run.

## Shared persistent-volume qualification: 17 October 2026

The shared Docker/native fixture passed all 13 checks and cleanup. Both backends
preserved data across Pod replacement and completed Jobs, including buffered
writes without an application-issued sync. Delete reclaimed storage; Retain
preserved the same PV for explicit rebinding to a new claim before final cleanup.

Native-only checks preserved both data and the application RAM marker through
hibernation and controller restart. Replacement retained data with a new process
marker. A sleeping Pod held its disk attachment and prevented a second writer
from starting a VM. The Retain case also deleted an already sleeping Pod before
replacement. The 128 MiB requests produced the documented 256 MiB native minimum,
with unchanged disk identity and geometry throughout. Final guest, snapshot and
volume inventories were empty, and the owned peer was removed.

The run exposed a writeback gap during Pod replacement, which was fixed before
this successful rerun. Hibernation freezes managed application writers and
flushes the attached filesystems before capture. Deletion and terminal completion
seal the external ext4 filesystems before guest removal. A failed or uncertain
seal preserves recovery state rather than replaying the operation blindly. The
one-shot helper exits after each operation; no storage sidecar is required.

This native run used the completed lifecycle implementation before its journal
format was raised from 6 to 7 for PVC Pods. Separate recovery regressions verified
the format-7 guard, including rejection of older PVC journals before orphan
cleanup. Linux guest-filesystem race tests and ARM64/AMD64 static builds passed.
The native VM run was on Apple Silicon; it does not establish Linux/KVM support.

After integration with generic host networking and the format-7 ownership guard,
the complete Pod-controller and shared-runtime Go race suites and vet passed.
The host-helper race suite and vet also passed, along with Java test compilation,
Checkstyle and 11 managed-setup Python tests. Platform-gated native tests remain
opt-in; these checks do not imply a run of every repository test.

## Native storage maintenance and resource metrics: 17 October 2026

The storage fixture passed on Apple Silicon with SDK/runtime 0.7.8. A 384 MiB
guest stored a marker in RAM and hibernated to a full checkpoint. Both preview
and applied cleanup preserved its stopped VM, checkpoint and image dependencies.
Restore returned the same marker. After exact owned VM/checkpoint cleanup, the
fixture's now-unused image was removed and no RAM reservation remained. The
fixture completed in 2.35 seconds; this is one small synthetic workload, not an
application latency guarantee. The dedicated VM inventory was empty afterward.

The complete shared-runtime and host-helper race suites and vet passed. The
Pod-controller race suite passed in 41.908 seconds, and vet passed. Focused
resource tests cover cgroup counter decoding, working-set calculation, incomplete
scrapes and no activity or guest execution for cold/sleeping Pods. Native metric
endpoint behavior is covered separately by the installed-path qualification.

## Shared lifecycle and log operations: 17 October 2026

The final format-8 controller passed all 12 checks in the shared Docker/native
operations fixture and completed ownership cleanup. Both worker types exercised
postStart side effects, HTTP preStop before SIGTERM, resource projections and
current/previous/follow/timestamp/time-filter/tail/byte-limit log behavior.
Native checks additionally read logs while asleep, recovered the sleeping Pod
after a controller crash without replaying hooks, and resumed for graceful
deletion. Controller recovery took 2.038 seconds in this fixture. The log capture
process used 3184 KiB RSS in the running guest. These are fixture observations.

The complete Pod race suite passed in 45.610 seconds and both Pod/runtime vet
checks passed. Guest, snapshot and owned peer cleanup completed. The installed
mixed graph later found an empty Prometheus-family encoding error when every
guest slept; the fix omits absent container families and adds a regression through
the actual Prometheus text encoder. This does not fabricate zero usage samples.

## Installed identity and mixed HTTP graph: 17 October 2026

The managed installer and background supervisor passed the real TLS identity
fixture's seven checks, followed by complete ownership cleanup. Kubernetes called
the installed TLS webhook to mutate untouched positive Pods for IRSA and Pod
Identity. The shared AWS Go SDK assertions passed on Docker and native workers:
allowed/denied role operations, explicit credential precedence and forged
audience/subject rejection. Native processes retained RAM and refreshed projected
tokens and SDK credentials after supervisor-led controller recovery. This run did
not wait for actual token expiry; that was qualified separately above. The Java
fixture passed with no failures, errors or skips.

A second installed-path fixture passed all 11 checks with the integrated metrics
and operations helpers. Three different application shapes deployed cold: a
ConfigMap-backed catalog, a frontend calling its ordinary Kubernetes Service,
and an initialized memory-emptyDir counter. Concurrent frontend requests woke
only its two dependencies. A third wake waited at the 1536 MiB guest budget and
completed when an idle workload slept, without evicting active guests.

TLS kubelet metrics reported active containers and omitted every sleeping
container without waking them. Complete credential rotation and supervisor crash
recovery preserved Pod identities and RAM markers. A replacement Deployment
completed its rollout while cold and served its new version on first use. All
owned VM, checkpoint and volume inventories were empty after both fixtures, and
their separate Kubernetes peers were removed. This is a small mixed HTTP graph,
not a 500-application graph or an Aurora/message integration result.

The current Docker host exposes no KVM device. These new native VM results are
Apple Silicon results; native Linux/KVM qualification still requires a suitable
Linux host. Linux unit tests and cross-builds do not replace that qualification.

## Installed Aurora and SQS application graph: 17 October 2026

The installed graph passed all ten checks, including ownership cleanup. The same
Go application and AWS SDK SQS consumer ran on ordinary Docker workers and native
Pods, using a real native Aurora PostgreSQL database. HTTP requests and queued
messages produced six committed SQL rows, verified through the Java AWS SDK/JDBC
fixture. The Java fixture completed with no failures, errors or skips.

Native application deployments became proxy-Ready without starting their VMs or
Aurora. HTTP subsequently woke the application and its database dependency. SQS
enqueue woke the worker, which consumed the message, committed its SQL write and
returned to idle sleep. Both application RAM markers and PostgreSQL's original
process start time survived sleep and controller recovery.

The Pod controller and Aurora host helper shared a 1536 MiB runtime budget. With
one 768 MiB Pod and Aurora active, two separate third-guest admission attempts
returned `409` with `operationStarted=false`; VM inventory and reservations stayed
unchanged. Shared active guest allocation returned to zero between cycles. All
owned VMs, snapshots, volumes, AWS resources and the Kubernetes peer were removed.

This run used macOS/Apple Silicon. It advanced Aurora's existing test idle clock
instead of waiting its minimum five-minute auto-pause interval; Pod idleness used
the real coordinator. It qualifies PostgreSQL/SQS and this application graph,
not MySQL in this graph, 500 different applications or native Linux/KVM.
