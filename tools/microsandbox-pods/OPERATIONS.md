# Application lifecycle and diagnostics

The native provider accepts `postStart` exec and HTTP handlers and `preStop`
exec, HTTP and sleep handlers on ordinary application containers and restartable
init sidecars. Ordinary sequential init containers cannot declare hooks, as in
Kubernetes. TCP lifecycle handlers, custom stop signals and HTTP host overrides
remain rejected explicitly. Use an HTTP `Host` header for virtual hosting.

The application entrypoint starts before the controller invokes `postStart`.
Startup and readiness checks wait for successful completion of that hook. A
failed hook stops the container and its restart policy determines the next
attempt. HTTP hooks use the shared network relay and reach the actual Pod backend,
without a wake request or an idle-activity update. As in kubelet, a received HTTP
response completes an HTTP lifecycle handler even if its status is an error;
HTTP readiness/liveness probes retain their usual status-code checks.

`preStop` runs before SIGTERM and consumes the existing termination grace period.
A timed-out running hook receives Kubernetes' one-off two-second extension before
forced termination. Restartable sidecars still stop after regular applications,
in reverse declaration order. RAM hibernation and restore do not invoke either
hook. Graceful deletion can resume a previously running sleeping Pod to execute
`preStop`, subject to the remaining deletion grace and shared VM admission budget.
A never-started cold Pod and a forced zero-grace deletion never boot for hooks.
Admission or identity failure is reported as a Warning Event; an uncertain native
restore retains ownership for recovery rather than deleting unknown state.

Hook intent and completion are persisted per container attempt. After a
controller crash, an incomplete invocation is failed rather than automatically
replayed. Kubernetes itself can deliver a hook more than once; applications
should continue to make their hooks idempotent. A completed hook is never replayed
when resuming its existing process from RAM.

A sleeping container retains its logical `Running` status because its execution
state still exists in a checkpoint. With strict availability its Ready condition
is false; proxy availability can keep it Ready when the wake path is healthy.
This distinction prevents virtual-kubelet from treating a sleeping application as
already terminated and bypassing graceful deletion.

## Downward API

Both standalone `downwardAPI` volumes and combined projected volumes accept
`resourceFieldRef` for CPU, memory and ephemeral-storage requests and limits.
A volume item names its source container explicitly. Divisors follow Kubernetes'
millicore/byte conversion and upward rounding. An absent CPU or memory limit
resolves against this virtual node's advertised allocatable capacity, not the
individual guest size or currently free native RAM. Ephemeral-storage projection
can report an unspecified value, but declaring ephemeral-storage requests/limits
still requires quota support and remains rejected. Resource resizing and
huge-page resources remain unsupported.

Metadata and resource files use the existing atomic projection refresh. A
mounted `subPath` remains pinned to its initial content, as for other projections.

## Logs and Events

`kubectl logs` supports current and previous attempts, `--follow`, `--timestamps`,
`--since`, `--since-time`, `--tail` including zero, and byte limits. Partial lines
and binary bytes are preserved. Follow reads end when the selected container
attempt ends; they do not silently switch to the next restart. Closing or
canceling a log reader releases its worker even when the client stops reading.

The existing `guestfs` executable provides timestamped capture at output receipt.
It wraps the container launch as one small process within the VM supervisor tree,
not an injected container or sidecar. That process is checkpointed with the VM and
drains stdout/stderr before the supervisor publishes container completion and
before persistent-volume sealing. Cold and sleeping workloads have no active
logging process. The fixture report records the observed helper RSS separately
from the application and VM overhead.

Each running container keeps two bounded 1 MiB timestamped segments and two
bounded 1 MiB raw segments in its guest root disk. The controller retains at most
3 MiB per current/previous attempt outside the guest; completed legacy init logs
also retain their existing 1 MiB bound. These are recent diagnostics, not an
unbounded log archive. Older rotated output is discarded. Deleting a Pod removes
its retained host logs. Reading retained logs from a sleeping or completed Pod
never starts a VM and never extends the workload's idle deadline. Following a
sleeping Pod waits for later output without waking it.

Old checkpoints without timestamp capture remain readable as raw logs. They
cannot supply authentic timestamps or time filters: those options return an
explicit error until the container starts using the updated helper. Following a
legacy raw capture also stops with an explicit error at its 1 MiB retention limit,
because it has no reliable offsets for rotated output. Managed setup
builds and installs the helper; manually launched controllers should provide the
current `--guest-fs-binary` and `--network-relay-binary`.

One controller-wide Kubernetes Event broadcaster publishes deferred start,
running/readiness, hibernation, termination and hook/runtime failures. It uses a
bounded aggregation cache and Kubernetes' rate limiting. It does not create a
watcher, sidecar or reporter per Pod.

## Qualification

`qualify.py --compat-operations-only` runs the same manifest and assertions against
the ordinary Docker worker and native provider: postStart side effects, resource
projection, HTTP preStop before SIGTERM, current/previous/streaming logs, timestamp
and time filters, tails and byte limits. Native additions verify retained logs
while asleep, controller recovery without wake, no hook replay on RAM restore,
and graceful deletion starting from a checkpoint. Supply the rebuilt controller,
network relay and guestfs binaries. The fixture owns its Kubernetes peer and
cleans only its own resources.

The 2026-10-17 macOS/Apple Silicon run passed all twelve shared/native checks and
cleanup using journal version 8. The log helper used 3,184 KiB RSS for the fixture
container. Controller recovery took 2.038 seconds; retained sleeping logs stayed
identical and no startup hook replayed. Linux/KVM requires separate qualification.
