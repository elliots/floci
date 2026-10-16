# Shared guest container supervision

This package supplies a small guest supervisor shared by native Kubernetes pods
and ECS tasks. It assumes the pinned `rancher/k3s:v1.35.0-k3s1` guest, including
`/bin/sh`, containerd, ctr and runc. It has no Kubernetes or AWS dependencies.

Run `BootstrapScript()` as the VM entrypoint. It starts containerd and accepts
independent container requests below `/tmp/floci-tasks`. Alternatively the
`bootstrap` plan installs that script at `BootstrapPath`; the VM entrypoint must
wait for that file and execute it. Installing the script does not start another
supervisor. There must be exactly one supervisor per VM.

`Plan(Request)` returns guest exec arguments and private stdin. The `start`
request includes an opaque safe task ID, a complete argv, optional environment
and working directory. The caller plans image pulls, OCI defaults, mounts,
resources, health checks, dependencies and restart policy. Launch payloads are
transported through stdin and stored in private guest files, never embedded in
host argv. They can contain secrets and are included in full RAM snapshots.

Start acknowledges `queued`; it does not prove the container started. `status`
returns JSON with `state` (`queued`, `running`, `exited`, `not_found`), the guest
launcher `pid`, optional `exitCode`, and UTC `startedAt`/`finishedAt`. A running
launcher does not prove application readiness. The caller must inspect the
containerd task and apply its own readiness policy. `stop` signals the containerd
task with that ID, so the planned ctr task ID must equal the request ID. It does
not wait for graceful termination. `logs` returns combined stdout/stderr, with
an optional positive line limit. Restart retains one previous log and status.

Each container is an ordinary child of the VM supervisor, rather than an SDK
managed detached execution. This permits full RAM snapshots once the caller's
attached preparation/health/exec operations have finished. A duplicate start
cannot replace a queued or active request. The caller must serialize its own
lifecycle and retain its ownership journal when command completion is uncertain.

`StartWithCompletion` additionally accepts a trusted completion accounting hook,
used by Kubernetes to preserve OOM attribution before publishing the exit code.
Accounting failures have their own marker and never replace the actual exit
code. The hook is not exposed in the JSON request contract.
