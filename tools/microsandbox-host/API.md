# Microsandbox native host helper (experimental)

This Go module is an external host helper for microsandbox 0.7.8. It shares
the official SDK adapter in `../microsandbox-runtime` with the pod controller.
It is **not a conforming
disposable container sidecar**: the host must supply its hypervisor, executable,
runtime libraries, and durable `MSB_HOME`. It is deliberately excluded from
managed-image defaults and publishing until that deployment contract is defined.
Existing container sidecars and contract v1 are unchanged.

The HTTP wrapper is stateless. Microsandbox owns the guest disks and checkpoints
in the supplied home directory. The caller owns workload lifecycle policy and
recovery journaling. Killing the helper does not intentionally stop guests.
Use a dedicated home for each emulator installation, with one owning helper and
controller. Do not point it at a developer's ordinary microsandbox home.

## Run on the host

Install microsandbox 0.7.8 and its matching native runtime libraries. Build with
Go 1.26+, CGO enabled and a C compiler. Java is no longer required for the helper.
Build for the host operating system and architecture; the host must provide a
working native hypervisor.
Linux binaries require glibc 2.28 or newer for the SDK's bundled native library.

```sh
(cd tools/microsandbox-host && go build -o /tmp/floci-microsandbox-host .)
MICROSANDBOX_EXECUTABLE=/path/to/msb \
MICROSANDBOX_HOME=/short/durable/path \
MICROSANDBOX_TOKEN=your-local-runtime-token \
PORT=8186 /tmp/floci-microsandbox-host
```

Keep the home path short: microsandbox derives Unix socket paths from it. On
macOS the total derived path must be shorter than 104 bytes. Do not use `/tmp`
for data that must survive host cleanup or reboot.

The helper defaults to loopback. Existing `QUARKUS_HTTP_HOST` and
`QUARKUS_HTTP_PORT` variables remain supported for migration; the latter takes
precedence over `PORT`. `QUARKUS_HTTP_HOST` can change the listener for
an emulator in Docker; provide an appropriate host firewall when doing so. All
functional endpoints require `Authorization: Bearer <MICROSANDBOX_TOKEN>`.
This credential grants control of the dedicated local runtime, including guest
execution. `GET /health` is unauthenticated and uses the shared contract with
`name: microsandbox`. A caller must also check the installed runtime version.

Set `FLOCI_NETWORK_CONFIG_FILE` to the same immutable file used by Floci when
enabling environment isolation. The helper validates existing active guests
before listening. `/health` adds `networkPolicy`, the configuration's SHA-256
fingerprint, or an empty string when isolation is disabled. Functional requests
with `X-Floci-Network-Policy` must match the loaded fingerprint; mismatch returns
409 before executing anything. See [workload networking](../../docs/configuration/workload-network.md)
for native destinations, controlled DNS, public CA files and checkpoint recovery.

## POST /v1/command

```json
{"arguments":["inspect","--format","json","guest-name"],"input":""}
```

The first argument must be one of `--version`, `doctor`, `run`, `start`, `stop`,
`pause`, `resume`, `inspect`, `ls`, `rm`, `exec`, `snap`, or `metrics`. The shared
adapter maps the supported argument forms to SDK calls. Unsupported options
fail explicitly. `run`, `inspect`, `metrics`, `doctor` and `--version` use the
configured CLI without a host shell. The Go binding lacks the image launch mode
compatible with full RAM capture, active configuration inspection, and complete
host diagnostic reports. Other lifecycle operations, snapshots and guest exec
use the SDK. `input`
is optional UTF-8 stdin, useful for database scripts executed inside a guest.

```json
{"exitCode":0,"stdout":"...","stderr":"..."}
```

Guest command exit codes are returned in a successful HTTP response, including
nonzero exits. Invalid requests return 400, unauthorized requests 401, and
transport/execution failures 500. An unconfigured helper returns 503. Commands
have a 180-second timeout; a timeout is an **unknown outcome**, not evidence that
the guest operation had no effect. The caller must inspect before retrying.
Output is limited to 64 MiB per stream; HTTP input is limited to 64 MiB.
Disconnecting an HTTP client does not cancel an accepted runtime operation.
SDK/transport errors retain an unknown outcome and do not invent a guest exit
code. Error responses and helper logs omit runtime arguments and credentials.

## POST /v1/port

Returns `{"port":12345}` for a currently free host TCP port. This is a candidate,
not a reservation; a concurrent binder can win the race and guest publication
must then fail explicitly.

## Guest container supervision

`POST /v1/guest-task/plan` accepts the generic
[guest task request](../microsandbox-runtime/guesttask/README.md) and returns
`{"arguments":["/bin/sh","-ec","..."],"stdin":"..."}`. Planning has no guest
side effects. Callers execute the returned plan through their existing operation
journal, passing stdin separately so environment values stay out of host command
arguments. The `bootstrap` action supplies a containerd supervisor script. It
must run as the VM's persistent entrypoint for full RAM checkpoint support.

`POST /v1/guest-task` accepts the same request plus `vm` and executes its plan,
returning the ordinary `exitCode`, `stdout`, `stderr` result. It has the same
timeout and uncertain-outcome rules as `/v1/command`. Task status distinguishes
`queued`, `running`, `exited` and `not_found`; submission is not readiness.
Both endpoints require authorization and reject unknown request fields.

## GET /v1/exec (WebSocket)

Authenticated callers can open a WebSocket for attached guest exec. The first
JSON frame is:

```json
{"type":"start","arguments":["exec","guest-name","--","sh"],"stdin":true,"tty":true,"rows":24,"cols":80}
```

Subsequent input frames are `{"type":"stdin","data":"base64 bytes"}`,
`{"type":"eof"}`, `{"type":"resize","rows":40,"cols":120}` or
`{"type":"cancel"}`. Resize requires TTY, and EOF closes stdin while allowing
output and resize to continue. Input is limited to 64 KiB per decoded chunk and
four queued chunks; a client exceeding the backlog terminates its exec session.

Output frames use `type: stdout` or `type: stderr` with base64 `data`. Normal
completion sends `{"type":"exit","exitCode":0}`, including nonzero guest
exit codes. Failures send a redacted `error` frame without an invented exit code.
TTY streams may merge stderr into stdout according to terminal behavior.
Output is streamed in chunks of at most 32 KiB with no total-output limit.

Disconnect, cancellation and the helper's 180-second command timeout terminate
the attached exec session. They do not stop the VM or its application containers.
Unlike lifecycle commands, an interactive session is owned by its connection.
The endpoint rejects cross-origin browser upgrades unless the Origin matches
the helper's host.

## Validation

Run `go test -race ./...` and `go vet ./...` from this directory. Transport tests
cover authentication, literal command arguments and stdin, guest exit codes,
timeouts, client disconnects, JSON errors and size limits. SDK adapter tests live
in `../microsandbox-runtime`. Workload lifecycle tests belong to Floci and the
pod controller; the helper does not implement database or cloud service policy.
Linux/KVM workload qualification remains separate from Apple Silicon testing.

## Native capacity admission

The following endpoints are private host-helper operations, protected by the same
bearer token and environment network-policy check as `/v1/command`:

| Method and path | Body or result |
| --- | --- |
| `GET /v1/capacity` | Limits, accounted guest RAM, pending start count, reservation tokens and optional conservative disk accounting |
| `POST /v1/capacity/limits` | `{"memoryMiB":16384,"starts":4,"diskMiB":131072,"minFreeDiskMiB":8192}`; zero disables that limit; omitted fields also become zero |
| `POST /v1/capacity/prepare` | `{"arguments":["snap","restore","source:sleep","--name","target"],"sourceName":"source"}`; returns `{"token":"..."}` |
| `POST /v1/capacity/cancel` | `{"token":"..."}`; only undispatched reservations can be cancelled |

Pass a prepared token as `admission` alongside `arguments` in `/v1/command`.
It must match the prepared command exactly. Ordinary launch/start/restore/capture
calls reserve automatically. Compound restores must prepare before consuming their
source. Checkpoint callers prepare before pausing or freezing a guest, using
`{"arguments":["snap","create","sleep","--sandbox","guest","--full","--integrity"]}`
without `sourceName`. Capture reservations do not consume RAM or start slots.
Cancel an undispatched capture reservation if preparation or pause fails. A
dispatched capture retains its reservation until completion can be reconciled.
Capacity refusal is HTTP 409 with `code: "CapacityUnavailable"` and the boolean
`operationStarted: false`. Only this complete response proves non-dispatch. A lost
response or any other runtime failure retains the usual uncertain-outcome semantics.
Do not expire reservations based on age or helper PID. See the
[shared adapter notes](../microsandbox-runtime/README.md#shared-native-capacity).
