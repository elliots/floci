# Microsandbox runtimes

This branch starts from `main`, independently of `feat/on-demand-runtime`.
It includes experimental PostgreSQL/MySQL/Aurora and Kubernetes pod runtimes,
with HTTP and queue-driven wake coordination. Generic application execution
remains available to the shared runtime and qualification tools. Docker remains the default.
The providers preserve the existing AWS APIs and Aurora Serverless v2 pause eligibility.
See [migration and setup](MIGRATION.md) before enabling it.
The [architecture and memory model](ARCHITECTURE.md) describes shared infrastructure,
active VMs and cold workloads. Native execution work is limited to Kubernetes
pods and Aurora. New EC2 work is parked in `parked/microsandbox-ec2-20261017`;
this branch retains baseline EC2 behavior. The [compute design](COMPUTE.md) also records earlier
options for other services, which now retain their existing backends.

## One checkout

All implementation sources now live on `feat/microsandbox` in the Floci checkout:

- `src/main` and `src/test`: Floci runtime adapters and integration tests.
- [`tools/microsandbox-host`](../microsandbox-host/API.md): the Go HTTP host helper.
- [`tools/microsandbox-pods`](../microsandbox-pods/README.md): the Go pod controller and Linux network relay.
- `tools/microsandbox-runtime`: the shared adapter for the official microsandbox Go SDK.
- [`tools/runtime-tests`](../runtime-tests/README.md): common AWS/workload tests for Docker and microsandbox.
- `tools/microsandbox`: qualification scripts, design notes and migration guidance.

The helper and pod controller have independent builds so their dependencies do
not enter Floci's native image. No companion repository checkout is required.
From the repository root, with Go 1.26+ and a C compiler installed:

```sh
(cd tools/microsandbox-runtime && go test -race ./... && go vet ./...)
(cd tools/microsandbox-host && go test -race ./... && go vet ./...)
(cd tools/microsandbox-pods && go test -race ./... && go vet ./...)
```

The host helper and pod controller were imported from the experimental
`feat/microsandbox-host-runtime` branch of `floci-io/floci-sidecars` at
`ce58b50`. The helper is now Go and shares the pinned SDK adapter with the pod
controller. Published container sidecars remain in their existing repository.

## Small integration seam

`RdsBackendRuntime` is the boundary between RDS service semantics and database
execution. `RdsContainerManager` implements it with its existing Docker code.
Only the dependency types in `RdsService` and `EmulatorLifecycle` change.
The service tests now mock the interface rather than the Docker implementation.
The wiring test checks that lifecycle operations and `RdsBackendGate` use the
same CDI provider, including the gate injected into proxies and the Data API.

Keep this extraction as a separate commit. Runtime implementation work can then
land in new files, while upstream fixes to Docker's manager continue to apply
in their original location. This intentionally avoids moving or renaming the
manager, proxy classes, persisted model fields, and existing method names.
`isDockerReachable` is a legacy method name for provider reachability at this
boundary. Renaming it can be a separate mechanical change.

The existing handle, `containerId`, and storage names form a compatibility
boundary, not a requirement to use Docker. A new provider must map these opaque
identities to its own active execution generation. It must preserve backend
host/port across resume because the proxy and Data API retain that address.
Docker host resolution still used by RDS describes where Floci itself publishes
its proxies; it is distinct from the provider's guest networking.

The interface covers startup, shutdown, liveness, cleanup ownership, durable
storage, SQL backup/restore, password rotation, and auto-pause. Extracting only
`AutoPauseController.Freezer` would not make Docker replaceable: the rest of
these paths still need a provider. In particular, a hibernated VM must not appear
missing to RDS's reconciliation and trigger creation of an empty replacement.

## Run the probe

Use native microsandbox **0.7.8**, with its matching runtime libraries. On macOS
run it on the host, where it can access the hypervisor. On Linux use a host with
working KVM. This harness does not install microsandbox or modify Docker.

```sh
python3 -m venv /tmp/floci-msb-python
/tmp/floci-msb-python/bin/pip install -r tools/microsandbox/requirements.txt
/tmp/floci-msb-python/bin/python tools/microsandbox/probe.py \
  --msb /path/to/msb --report /tmp/floci-msb-result.json

python3 -m unittest discover -s tools/microsandbox -v
```

The default image is `postgres:17.11-alpine`; use `--image postgres@sha256:...`
to pin the appropriate manifest for the test host. Allow several GB of disk
space for images, root disks, and full memory checkpoints. Each invocation
creates an isolated, short `MSB_HOME` under `/tmp`: macOS's normal temporary
directory can exceed microsandbox's Unix socket path limit.

The probe:

1. Starts a 512 MiB PostgreSQL guest with a loopback-only published TCP port.
2. Commits a row through a real host PostgreSQL connection and writes a tmpfs
   marker. It records the postmaster's start time.
3. Closes clients, pauses the guest, captures and verifies a full snapshot,
   then force-stops the source VM. It checks the source reports `Stopped` and
   that the host port is closed.
4. Restores into a new VM generation using the same host port. Through a new
   TCP connection it verifies the process start time, row count, and tmpfs marker.
5. Commits another row, then repeats from the new generation. A stale snapshot
   cannot satisfy the second cycle.
6. Removes the probe's VMs and snapshots, and writes timing and outcome JSON.

The image cache and diagnostic state remain in the printed directory. Delete
that directory after inspecting the results. On cleanup failure the probe
reports the owned resources and fails; it never cleans another `MSB_HOME`.
An interrupted process or host crash may require manual cleanup of that isolated
directory's sandboxes. The harness is a qualification tool, not a durable daemon.

Snapshot/verification failure resumes the original guest and never intentionally
terminates it. Stop or restore failure fails the run. The unit tests exercise
these failure paths and reject a cold restart or stale database state.

### Observed result

On 2026-10-16, the complete probe passed on macOS 27.0 arm64 with microsandbox
0.7.8 and PostgreSQL 17.11. Both cycles preserved the postmaster start time,
tmpfs marker, and committed rows, including the write after the first restore.
All probe VMs and indexed snapshots were removed successfully.

| Cycle | Capture, verify, and stop | Restore until host SQL ready |
| --- | --- | --- |
| 1 | 1.060 s | 0.315 s |
| 2 | 1.007 s | 0.324 s |

These are observations for one tiny, idle database with 512 MiB of guest RAM,
not performance guarantees. The source reported `Stopped` and its host port
closed before each restore. The probe does not measure system-wide free RAM,
which also includes filesystem caching, and it does not establish Linux results.

## Provider design

`MicrosandboxRdsRuntime` implements `RdsBackendRuntime`, selected as the same
provider for `RdsBackendGate`. Runtime selection is confined to a CDI producer;
`RdsService` does not branch on the provider. These constraints keep the seam
small enough to rebase while the Docker implementation continues to evolve.

- Keep the AWS resource identity, storage identity, and public proxy endpoint
  stable. Track VM generations and checkpoint IDs separately.
- Preserve the existing lease rules: TCP sessions, Data API transactions, and
  maintenance commands all prevent suspension. Serialize wake requests.
- Publish a verified checkpoint before destroying execution. After resume,
  never replay the old checkpoint over newer writes. Persist transition state
  and reconcile interrupted capture, stop, restore, and cleanup after restart.
- Keep durable data in guest-owned disks captured with memory. Host bind mounts
  and external services are not rolled back by restoring guest RAM.
- Distinguish resident pause, hibernation to disk, and ordinary stop/start. The
  latter must preserve storage but boot fresh processes.
- Keep logical AWS database snapshots separate from runtime memory checkpoints.
- Fail explicitly for unsupported engines or features. Switching providers must
  not silently abandon an existing Docker volume; data migration is separate.
- Put the native helper behind the repository's sidecar manager/client contract.
  A native Mac helper needs the externally supplied URL path; packaging it inside
  a Linux Docker container does not give it direct access to macOS's hypervisor.
  Publish and pin a helper release before adding a managed-image default.

The native helper keeps its operations generic: create, readiness, acquire/release activity, suspend, resume, stop,
inspect, and delete. RDS owns AWS pause eligibility and events; Kubernetes owns scheduling. Do not put those
policies into the helper. The current on-demand adapters invoke these operations
while HTTP and queue wake infrastructure stays available.

Freezing a Kubernetes worker while its control plane continues running also
requires handling health checks and eviction. The database experiment does not
establish Kubernetes or EC2 support, or cross-host/cross-architecture snapshot
portability. Linux qualification remains required separately from macOS.

## Historical individual-worker experiment

The current implementation runs one VM per Pod through the shared
[Pod controller](../microsandbox-pods/README.md). The earlier whole-worker
experiment below is retained for investigation; it is not required for Pod setup.

`worker_probe.py` uses only Python's standard library. It creates a disposable
Docker k3s control plane and native microsandbox workers, leaving existing
clusters alone. Docker must be running and microsandbox must run on the host.

```sh
python3 tools/microsandbox/worker_probe.py \
  --msb /path/to/msb --report /tmp/floci-worker-result.json
```

The default is one worker with 2 vCPU, 2 GiB RAM, and an 8 GiB ext4 root disk.
It verifies an actual kubelet lease and Ready condition, host-network pod
execution and logs, then ordinary pod networking, DNS, and a ClusterIP Service.
It uses pinned k3s and BusyBox image tags. Add `--workers 2` to test traffic
between workers. Native guests need the optional VXLAN relay for that test:

```sh
python3 tools/microsandbox/worker_probe.py \
  --msb /path/to/msb --msb-home /tmp/floci-worker-msb \
  --workers 2 --relay-vxlan --report /tmp/floci-worker-relay-result.json
```

The relay publishes each guest's UDP port 8472 on a separate host loopback port.
Guest-only iptables rules translate VXLAN traffic for the exact peer node IP to
that peer's published port through `host.microsandbox.internal`. The host's
routing table and existing containers are untouched. This qualifies IPv4 Flannel
VXLAN only; it is not a general VPC or arbitrary CNI implementation.
`--skip-cni` only checks registration and host-network pods.

The probe removes only its uniquely named, labelled VMs and Docker control plane,
and writes JSON and redacted diagnostic logs. It retains its runtime
home and image cache for inspection. Supply a dedicated short `--msb-home`
to reuse image downloads. An interrupted host or process can require manual
cleanup of the exact resource names printed by the probe.

This is a standalone qualification tool. It does not add a Floci worker provider,
change EKS APIs, establish EC2 networking/storage parity, or test hibernation.
See [COMPUTE.md](COMPUTE.md) for the remaining integration requirements and the
AWS-compatible worker warm-pool lifecycle.

### Observed worker qualification

On 2026-10-16, one native worker passed all checks on macOS 27.0 arm64 with
OrbStack, microsandbox 0.7.8, and `rancher/k3s:v1.35.0-k3s1`. The guest ran
Linux 6.12.111, containerd 2.1.5, and kubelet 1.35.0. It had its own Linux kernel,
registered a real kubelet lease, executed a host-network pod, and passed ordinary
pod DNS and ClusterIP traffic on that worker. The control plane stayed in Docker.

With two native workers and the default network, both kubelets became Ready and
executed host-network pods, but direct cross-worker Pod IP traffic timed out.
Adding `--relay-vxlan` passed the full two-worker test twice, including direct
Pod IP HTTP, DNS and ClusterIP traffic. The persisted probe's two workers reached
Ready with actual kubelet leases in approximately 14 seconds each, using cached
images. Translation changes the outer VXLAN destination only. Inner pod addresses
are expected to remain intact; the probe does not separately assert observed
source IPs.

These tests qualify native-worker-to-native-worker networking. They do not yet
qualify a Docker worker as an overlay peer, pod hibernation, NetworkPolicy, IPv6,
EC2 identity/storage, Linux/KVM hosts, or capacity for hundreds of applications.
All resources created by the runs were removed by the probe. A full kubelet per
application pod is not the planned scaling architecture: see
[shared pod controller](COMPUTE.md#shared-pod-controller) for that separate seam.

## Project migration impact

The default Docker configuration requires no application changes. Existing Java
callers that pass `RdsContainerManager` remain source-compatible because it
implements `RdsBackendRuntime`. Enabling microsandbox changes local runtime and
storage requirements; [MIGRATION.md](MIGRATION.md) lists those changes and the
current limits. It does not add an AWS hibernation API for ordinary RDS instances.

## Upstream references

- [Microsandbox v0.7.8](https://github.com/superradcompany/microsandbox/releases/tag/v0.7.8)
- [Full snapshots](https://docs.microsandbox.dev/sandboxes/snapshots)
- [Pause and lifecycle semantics](https://docs.microsandbox.dev/sandboxes/lifecycle)
- [Native host helper](../microsandbox-host/API.md)
- [Native Kubernetes pod controller](../microsandbox-pods/README.md)
