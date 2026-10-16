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
