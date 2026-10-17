# Shared microsandbox runtime adapter

This Go module is the native runtime seam used by the Floci host helper and
Kubernetes pod provider. It pins the upstream Go SDK to `v0.7.8` and checks the
loaded native SDK library's version before executing SDK operations. It requires
CGO and the host's existing microsandbox 0.7.8 installation. It does not download
or start a VM during construction.

`Client.Execute` accepts the existing Floci command argument arrays and returns
separate `stdout`, `stderr`, and `exitCode` fields. SDK operations implement VM
lifecycle, guest execution, paginated inventory, and full
snapshot creation, verification, restoration, listing, and removal. The parser
accepts only the options used by Floci. Unknown operations never fall through to
an arbitrary CLI command.

Four read operations use the configured executable: `--version`,
`doctor`, `inspect`, and `metrics`. The SDK exposes persisted `config_json`, while Floci's
ownership checks also examine the CLI's `active_config`. Keeping that inspection
preserves existing adoption and snapshot-lineage checks.
The metrics fallback preserves the CLI report's sample timestamp, CPU count,
and running/stalled/exited state, which the Go SDK's counter-only type omits.

Detached `run` also retains the CLI. The Go binding does not expose the runtime's
`background_command` launch intent. Creating an idle VM and starting its image
command with `ExecDetached` is not equivalent: active managed jobs prevent full
RAM snapshots. The narrow launch fallback preserves OCI entrypoints, image users,
working directories, logs and snapshot support without requiring a shell in the
image. The SDK version is checked before launch. A cold SDK `start` boots the
existing agent only, matching Floci's explicit database restart scripts. Full
snapshot restoration resumes the captured process state and never starts
another image command.

`ExecuteStreamWithOptions` adds SDK-backed TTY allocation, an initial terminal
size and live resize events. The original `ExecuteStream` remains available for
non-TTY callers. Disconnect cancellation kills only the attached exec session.
The shared `guesttask` package builds entrypoint-owned container supervision
plans for multi-container workloads without managed detached exec jobs.

Buffered output is limited to 64 MiB for each stream. `ExecuteStream` supports
live Kubernetes exec without a total-output cap. Guest stdin goes through the
SDK pipe, and is never placed in host process arguments. Attached streaming
sessions are killed and released on cancellation or transport/output failure.
For blocking stdin readers, the caller must provide an `io.ReadCloser` or arrange
for cancellation to unblock reads. The adapter closes supplied read-closers when
the session finishes.

Errors before dispatch wrap `ErrInvalidArgument`. Guest exit statuses are normal
results. SDK errors return an error without claiming that a mutation did not
happen; callers must keep and reconcile their existing operation journals.
Returned error text excludes native command arguments, credentials, and native
error messages. Original causes remain available to `errors.Is`/`errors.As` for
in-process classification.

The upstream SDK caches one backend, home, and executable per process. All
clients must use the same paths; the adapter rejects incompatible clients and
cloud backends. These settings are established lazily before the first SDK call.
Use separate processes for separate runtime homes.

Environment isolation requires the static Linux guestnet binary through
`FLOCI_MICROSANDBOX_GUEST_AGENT_PATH`. Its one-shot DNS setup installs kernel
routing to shared resolvers and exits; it does not add a DNS daemon to each VM.
The binary is streamed over private SDK stdin and removed after configuration.
The adapter retains effective network policy in private records tied to immutable
guest and checkpoint IDs. SDK 0.7.8 drops network/DNS settings on restore, so the
adapter reapplies policy before resuming RAM and blocks the SDK default resolver.
See [network configuration](../../docs/configuration/workload-network.md).

Lifecycle methods use the SDK's stable identity guards wherever exposed. The
0.7.8 Go binding still addresses resident `resume` and snapshot capture by name.
For those operations the adapter verifies the captured identity before and
after dispatch, and serializes local lifecycle changes to each name. This
detects replacement but cannot make these operations atomic against an external
process reusing a name. Floci's ownership checks and exclusive runtime names
remain required, as with the previous CLI implementation.

Run `go test -race ./...` and `go vet ./...` here. The host helper's native
qualification and the pod provider's qualification script exercise the real
SDK, databases, Kubernetes workloads, networking, snapshots and recovery.

The isolated network regression requires an explicit native run against a
dedicated runtime home, with a Linux guestnet binary for the host architecture:

```sh
FLOCI_NATIVE_TEST_MSB=/absolute/path/to/msb \
FLOCI_NATIVE_TEST_HOME=/absolute/path/to/dedicated/test-home \
FLOCI_MICROSANDBOX_GUEST_AGENT_PATH=/absolute/path/to/linux/guestnet \
go test -tags native -run '^TestNativeNetworkBoundary$' -count=1 -v .
```

It checks controlled DNS, denied external DNS and direct-IP egress, allowed local
HTTP, two RAM restores, cold restart, policy-change fencing and owned cleanup.
It also injects a broken DNS installer into a test-owned helper copy and verifies
that failed starts and launches stop the guest and invalidate its admission record.
Missing prerequisites fail this explicit test instead of silently skipping it.

Upstream implementation references at the pinned SDK release:

- [Go SDK](https://github.com/superradcompany/microsandbox/tree/sdk/go/v0.7.8/sdk/go)
- [Detached CLI launch](https://github.com/superradcompany/microsandbox/blob/8352bb67d4ed51f5eb086dc80b16c3184a22b3a6/crates/cli/lib/commands/run.rs)
- [SDK handle lifecycle](https://github.com/superradcompany/microsandbox/blob/8352bb67d4ed51f5eb086dc80b16c3184a22b3a6/sdk/go/sandbox.go)

## Image init options

Detached guest launches also accept `--init`, repeatable `--init-arg` and
`--init-env` for the SDK's PID 1 handoff. The image entrypoint remains separate
from its init system. Init paths must be absolute or `auto`, and arguments and
environment values are passed literally, including values that look like runtime
flags. Environment network-policy rewriting preserves these option/value pairs.

## Shared native capacity

Every adapter process using one runtime home shares `floci-capacity/state.json`.
Short filesystem locks serialize admission across the Go host helper and pod
controllers. No extra daemon is required. `FLOCI_MICROSANDBOX_MEMORY_LIMIT_MIB`
bounds configured guest RAM and `FLOCI_MICROSANDBOX_START_LIMIT` bounds in-flight
native launches, cold starts and restores. Zero means unlimited. Use the same
initial limits for every helper; conflicting nonzero settings fail closed.
The private ledger retains limits across helper restarts. The authenticated host
helper capacity API can update them while running, including lowering them below
current use. Lowering a limit preserves existing workloads and rejects new starts.

Live, paused and transitional guests count, together with pending reservations.
Confirmed stopped guests do not consume RAM capacity. The budget excludes Docker,
the JVM, host page cache and runtime overhead. Existing per-service Java and pod
limits remain optional, additional constraints. Direct use of the upstream CLI or
a different runtime home bypasses admission; do not share Floci-owned names with
external runtime mutators.

`PrepareAdmission` reserves before a compound restore deletes its stopped source.
Its opaque permit binds the target, command hash, source identity and checkpoint
identity, digest and captured memory size. `ExecuteAdmitted` consumes it once.
Restore revalidation opens the original checkpoint selector and verifies its
immutable ID, digest and memory size. A raw checkpoint ID can be ambiguous when
microsandbox retains multiple local copies.
New snapshot captures record geometry privately; legacy checkpoints without a
geometry record are rejected before admission. Recreate checkpoints after upgrading.
`ErrCapacityUnavailable` proves that this command was not dispatched. Callers
retain their cold workload and can retry. A disconnected or failed accepted
operation retains its reservation; neither elapsed time nor a restarted helper
proves completion. Capacity status lists retained tokens. Only an undispatched
reservation can be cancelled through the API. Unknown dispatched operations must
be reconciled with the runtime before the ledger can safely be repaired.

Tests cover two competing processes, symlink aliases, paused VM accounting,
checkpoint/source protection, changed checkpoint identity, lowered limits and
reservation retention after restart.

### Disk admission

`FLOCI_MICROSANDBOX_DISK_LIMIT_MIB` optionally limits conservative committed
native storage. `FLOCI_MICROSANDBOX_MIN_FREE_DISK_MIB` optionally reserves a host
filesystem free-space floor. Both default to zero and share the same private
ledger across Pod controllers and the Aurora host helper. The host capacity API
uses `diskMiB` and `minFreeDiskMiB`; its optional `disk` status reports observed,
writable, reserved and committed bytes plus currently available filesystem bytes.
With both limits disabled, operations do not query SDK storage or image inventory.

Observed storage comes from the SDK's `StorageUsage`: images, snapshots, sandbox
files, named volumes, branch memory and snapshot memory. Each category is charged
the larger of its reported logical or allocated bytes. Unknown logical sizes
refuse new work. Existing running/paused guests additionally reserve their full
writable root, twice configured RAM for memory backing/materialization, and
64 MiB of metadata headroom. Starts/restores reserve twice that allowance for
new files and subsequent writes. New flat-root launches also reserve a complete
shared flat base. Checkpoint capture reserves one additional allowance before
the Pod freezes or Aurora pauses. Reflink and sparse-file savings are deliberately
not credited; this is conservative admission accounting, not exclusive physical
usage. Pending reservations serialize under the same filesystem lock.

When enabled, launch requires a preloaded image with cached, converted layers,
an immutable manifest digest and explicit managed/flat root geometry. The adapter
does not pull an unmeasurable image inside the budget. Preload images before enabling
the budget. Saved checkpoints must include the new root-geometry record; recreate
older checkpoints before enabling disk admission. Named volumes already present
are counted, but the adapter does not expose volume creation or growth. Any future
volume mutation needs its own admission hook.

SDK full restores can omit root geometry from their sandbox configuration. The
adapter joins the restored sandbox's immutable `snapshot_parent` with its private
captured geometry record, requiring the captured memory allocation to match.
Subsequent captures retain that root size. Missing or inconsistent lineage remains
unknown and refuses disk admission; no default disk size is substituted.

The pinned SDK 0.7.8 leaves `Image.Inspect` layer EROFS size fields unset.
Preflight therefore also stats its known `cache/layers/<DiffID-safe>.erofs` and
`cache/fsmeta/<manifest-safe>.erofs` files using SDK-supplied SHA-256 identities.
Digest validation and directory-relative opens reject malformed paths, symlinks,
missing artifacts and non-regular files. This read-only fallback depends on the
[pinned cache layout](https://github.com/superradcompany/microsandbox/blob/8352bb67d4ed51f5eb086dc80b16c3184a22b3a6/crates/image/lib/cache/store.rs)
and must be requalified when upgrading the SDK. It does not download, convert or
prune files. `FLOCI_NATIVE_TEST_MSB`, `FLOCI_NATIVE_TEST_HOME` and a cached
`FLOCI_NATIVE_TEST_IMAGE` enable the read-only qualification:
`go test -tags native -run '^TestNativeDiskInventory$' -count=1 -v .`.
Against an empty, dedicated home with that image preloaded, the separate
`TestNativeDiskAdmissionProtectsCaptureAndRestore` qualification exercises denied
launch, denied capture before pause, denied restore with a retained checkpoint,
successful full RAM capture/restore and an already-prepared restore after a limit
reduction. It restores the previous limits and removes only its owned guest and
checkpoint. Serialize it with other native qualifications.

The adapter canonicalizes the native home before both SDK and CLI calls, including
the existing parent of a new home. This avoids duplicate snapshot index entries
through aliases such as macOS `/tmp` and `/private/tmp`. It does not create the
home during configuration. Previously created alias entries can still leave
unmeasurable indexed artifacts after deletion: admission deliberately refuses
those entries. Inspect SDK `StorageUsage` snapshot item paths, preserve a backup
and reconcile the old runtime index before enabling limits. A fresh dedicated
home with preloaded images is an alternative when that index cannot be repaired;
existing workload disks are not automatically migrated or deleted.

This is not a filesystem hard quota. Direct upstream CLI/SDK operations, writes
outside the SDK home, host logs and unrelated host processes can bypass or consume
the allowance. A prepared permit remains valid if limits are lowered, protecting
a restore whose stopped source was already removed. New work checks fresh SDK
usage and filesystem free space. No images, volumes or checkpoints are pruned
automatically, and reducing a limit never deletes workloads. Use dedicated host
filesystem quotas if a strict disk boundary is required.
