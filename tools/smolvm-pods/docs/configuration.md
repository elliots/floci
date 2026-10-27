# smolvm-pods configuration

Every setting of a smolvm-pods node, every pod annotation, the node service's API and
`smolvm-pods ctl`. How the parts behave is in [architecture.md](architecture.md).

- [Commands](#commands)
- [The node's settings](#the-nodes-settings)
- [Node service flags](#node-service-flags)
- [Pod annotations](#pod-annotations)
- [The API](#the-api)
- [The API over TCP](#the-api-over-tcp)
- [Docker and tunnels](#docker-and-tunnels)
- [Metrics](#metrics)
- [Kubernetes Events](#kubernetes-events)
- [The admin UI](#the-admin-ui)
- [smolvm-pods ctl](#smolvm-pods-ctl)
- [Files](#files)
- [Test settings](#test-settings)

## Commands

The binary is `containerd-shim-smolvm-pods-v2`; the node image also links it as `smolvm-pods`.

| Command | What it runs |
| --- | --- |
| `containerd-shim-smolvm-pods-v2 -namespace NS ... start\|delete`, and the daemon | The front shim, which containerd starts for the runtime type `io.containerd.smolvm-pods.v2`. Any invocation whose first argument is not one of the commands below is the front shim |
| `smolvm-pods node-service [FLAGS]` | The node service ([Node service flags](#node-service-flags)) |
| `smolvm-pods ctl COMMAND ARGS` | The API's client ([smolvm-pods ctl](#smolvm-pods-ctl)) |
| `smolvm-pods _boot-vm CONFIG` | A pod VM's process, which smolvm starts |
| `smolvm-pods _expand-templates` | Expands smolvm's disk templates under a lock; the node runs it at boot |
| `smolvm-pods _tso-canary` | Runs the TSO canary; the node runs it at boot. Exits 1 if it fails or cannot run |
| `smolvm-pods _network-boot` | Puts the node address on `floci0` and loads the table that holds until the node service has a network profile; the node runs it before k3s |
| `smolvm-pods _build-info` | Prints `smolvm COMMIT`, the smolvm commit the binary was built against |
| `smolvm-pods _netns-holder` | Unshares a network namespace, prints `ready`, and stays in it until its stdin closes; the node service runs it while a standalone container's network hooks run |
| `ip`, `tc` | The stand-ins that a pod VM's process finds first on its `PATH` |

## The node's settings

`/etc/floci-node/env` is the environment of k3s and of the node service; containerd, and through
it the front shim, inherit it from k3s. `floci-node` reads the whole file again each time it
starts k3s: after a change to the capacity settings, kill k3s, which `floci-node` starts again 5 s
later, or restart the node. A process that k3s starts after that, such as containerd or a new
front shim, gets the new `PATH`, `RUST_LOG` and `SMOLVM_*` values too. `floci-node` reads the node
service's settings, `FLOCI_NODE_API_LISTEN`, `FLOCI_NODE_IDLE_TIMEOUT`, `FLOCI_NODE_CPU_THRESHOLD`,
`FLOCI_NODE_CPU_WINDOW`, `FLOCI_NODE_MAX_AWAKE_PODS`, `FLOCI_NODE_MAX_AWAKE_MEMORY` and
`FLOCI_NODE_ADMISSION_WAIT`, each time it starts the node service: after a change, kill the node
service, which `floci-node` starts again 1 s later. The node service gets `PATH`, `RUST_LOG` and
the `SMOLVM_*` variables from the environment that `floci-node` read at boot, so a change to them
reaches the node service only when the node restarts.

On a node made with `node-vm.sh create --seed DIR`, `floci-node` reads the seed's `env` after
`/etc/floci-node/env`, each time it reads the file: lines `NAME=VALUE`, taken as they are, and `#`
comments. A setting with a value replaces the file's, and an empty one keeps it. The seed may set
`RUST_LOG` and the `FLOCI_NODE_*` settings except `FLOCI_NODE_ISOLATION`. `floci-node` checks the
seed's `env` every 5 s and, when it changes, restarts the node service, so a node service setting
there applies within about 6 s without a restart by hand
([node/README.md](../node/README.md#the-seed)).

| Variable | Default | Meaning |
| --- | --- | --- |
| `PATH` | the Debian paths, then `/opt/k3s/bin` and `/opt/k3s/bin/aux` | |
| `SMOLVM_DATA_DIR` | `/var/lib/smolvm` | smolvm's VM data, with the disk templates in `$SMOLVM_DATA_DIR/.smolvm`. Both must be on one mount, because a pause hard-links the templates |
| `SMOLVM_AGENT_ROOTFS` | `/var/lib/smolvm/runtime/agent-rootfs` | The patched guest agent's rootfs, which `floci-node` copies there at boot. It must contain `/usr/bin/rosetta-wrapper` |
| `SMOLVM_LIB_DIR` | `/var/lib/smolvm/runtime/lib` | The smolvm Linux release's libraries, with the patched libkrun and the pod kernel |
| `SMOLVM_VM_UID_DROP` | `off` | Keeps each pod VM's VMM root, so that a non-root process in a pod can write to its volumes and `/dev/shm` through the pod share, and a Docker container can own its volume's files with its own uids, as Postgres needs |
| `SMOLVM_DELTA_SAVES` | unset, on | `off` writes the whole RAM image at every pause and keeps no RAM base ([Pause](architecture.md#pause)) |
| `SMOL_NO_HOT_RELOAD` | `1` | Turns off smolvm's hot reload of VM shares |
| `RUST_LOG` | `info` | The log level of the front shim and the node service: `off`, `error`, `warn`, `info`, `debug` or `trace` |
| `FLOCI_NODE_MEMORY_GIB` | `256` | The memory, in GiB, that kubelet reports. 0, or a value below the node's own, reports the node's own. A whole number |
| `FLOCI_NODE_CPUS` | `128` | The CPUs that kubelet reports, the same way |
| `FLOCI_NODE_API_LISTEN` | `0.0.0.0:7070` | The node service's `--api-listen`; empty for no TCP listener |
| `FLOCI_NODE_IDLE_TIMEOUT` | `5m` | The node service's `--idle-timeout` |
| `FLOCI_NODE_CPU_THRESHOLD` | `0.5` | The node service's `--cpu-threshold`; `off` or `0` lets busy pods sleep |
| `FLOCI_NODE_CPU_WINDOW` | `60s` | The node service's `--cpu-window` |
| `FLOCI_NODE_MAX_AWAKE_PODS` | empty, no cap | The node service's `--max-awake-pods` |
| `FLOCI_NODE_MAX_AWAKE_MEMORY` | empty, no cap | The node service's `--max-awake-memory`, in MiB |
| `FLOCI_NODE_ADMISSION_WAIT` | empty, `10s` | The node service's `--admission-wait` |
| `FLOCI_NODE_NETWORK_ADDRESS` | `169.254.53.1` | The node address, on `floci0`, where pods reach DNS, Floci and routed hosts ([node/README.md](../node/README.md#network)) |
| `FLOCI_NODE_FLOCI_HOST` | empty, the default gateway | Where Floci listens from the node: the default route's gateway, or `loopback` for Floci on the node itself |
| `FLOCI_NODE_ISOLATION` | unset | `1` isolates the node's pods whatever the network profile says. Only `node-vm.sh create --isolate` sets it, in the machine's create-time environment; the seed's `env` cannot |

An empty node service setting leaves the node service's own default. The capacity settings bound
the sum of the requests of every pod on the node, awake or asleep, so size each to at least the
sum of the requests of the pods the node should hold; see
[node/README.md](../node/README.md#capacity). They report far more memory than the node has, so
only the admission caps keep awake pods within the node's real memory, and they are off by
default ([node/README.md](../node/README.md#wake-admission)).

The bearer token of the API over TCP is in `/etc/floci-node/api-token`, which `floci-node` creates
at boot when it is missing or empty: 32 random bytes in hex, owned by root, mode 0600. On a node
whose seed has an `api-token`, `floci-node` installs the seed's there instead.

## Node service flags

`smolvm-pods node-service` takes these flags, as `--flag VALUE` or `--flag=VALUE`; a flag given
twice takes its last value. Durations are Go durations ([below](#durations)) and must be more
than 0.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--idle-timeout` | `5m` | How long a pod may go with no traffic on connections to it before it sleeps, unless its annotation says otherwise |
| `--poll-interval` | `15s` | How often the node's conntrack table is read |
| `--cpu-threshold` | `0.5` | VM CPU, in vCPUs averaged over `--cpu-window`, above which a pod is busy and does not sleep. `off`, `0` or `0.0` turns the CPU rule off |
| `--cpu-window` | `60s` | The time over which VM CPU is averaged |
| `--max-awake-pods` | `0`, no cap | A wake is admitted only while fewer pods than this are creating, awake, pausing or waking. Creates are not refused, so they can take the count above the cap |
| `--max-awake-memory` | `0`, no cap | A wake is admitted only while the guest RAM of the pods that are creating, awake, pausing or waking, with the waking pod's, stays within this many MiB. Every pod VM counts as its own guest RAM, 1024 MiB when the pod has no memory limit ([the size of a pod VM](architecture.md#the-size-of-a-pod-vm)). Creates are not refused |
| `--admission-wait` | `10s` | How long a wake waits for room under a cap before it is refused |
| `--api-listen` | none | The API's TCP listener, `ADDR:PORT` ([The API over TCP](#the-api-over-tcp)) |
| `--api-token-file` | `/etc/floci-node/api-token` | The listener's bearer token |

An unknown flag, a missing value or a value it cannot read stops the node service with exit
status 2.

### Durations

A duration is `0`, or numbers with units, such as `30s`, `1.5h` or `1m30s`. The units are `ns`,
`us` (or `µs`), `ms`, `s`, `m` and `h`.

## Pod annotations

A pod sets these in `metadata.annotations`. containerd passes a pod's annotations to the shim, on
the sandbox's OCI spec and on each container's, only if the runtime's `pod_annotations` allow
them; the node image allows `smolvm-pods.floci.io/*` and `smolvm.io/*`. The shim reads them when
the sandbox is created. Values are trimmed and compared without case.

| Annotation | Values | Effect |
| --- | --- | --- |
| `smolvm-pods.floci.io/rosetta` | `"true"` or `"1"`; `"false"` or `"0"` | On an arm64 node every pod VM gets Rosetta and TSO when the node can give them. `"true"` requires them, and refuses the pod when the node cannot; `"false"` turns them off. Any other value refuses the pod. x86_64 nodes ignore it |
| `smolvm-pods.floci.io/hibernate` | `"false"` or `"0"`; for a standalone container, `"true"` or `"1"` | A pod from CRI sleeps unless it says `"false"`, and then never sleeps, by the idle rule or through the API; other values are ignored. A standalone container, such as Docker's, never sleeps unless it says `"true"` |
| `smolvm-pods.floci.io/wake-on-terminate` | `"true"` or `"1"` | SIGTERM to a sleeping pod wakes it and is delivered, instead of reporting exit 143 at once and killing the process at the next wake |
| `smolvm-pods.floci.io/idle-timeout` | A duration from `10s` to `24h` | The pod's idle timeout, in place of `--idle-timeout` |
| `smolvm-pods.floci.io/cpu-threshold` | vCPUs above 0, such as `"0.5"` or `"2"`, or `"off"` or `"0"` | The pod's CPU threshold, in place of `--cpu-threshold`; `"off"` lets the pod sleep however busy it is |
| `smolvm-pods.floci.io/cpu-window` | A duration from `5s` to `1h` | The pod's CPU window, in place of `--cpu-window` |
| `smolvm.io/memory-mib` | A whole number of MiB from 512 to 16384 | The guest RAM of the sandbox VM, in place of the pod's memory limit plus 256 MiB (1024 MiB without a limit). Another value is ignored, with a warning |
| `smolvm.io/gpu` | `"true"` or `"1"` | Gives the sandbox VM a virtio-gpu device (Venus and Vulkan). Other values leave it off |
| `smolvm.io/gpu-vram-mib` | A whole number of MiB above 0 | The size of the GPU's shared memory region, with `smolvm.io/gpu` only. Another value takes the default, with a warning |

The node service ignores an `idle-timeout`, `cpu-threshold` or `cpu-window` value it cannot read,
with a warning in its log, and the node's setting applies.

To run in a VM at all, a pod names the RuntimeClass `smolvm-pods`:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: web
  annotations:
    smolvm-pods.floci.io/idle-timeout: "2m"
spec:
  runtimeClassName: smolvm-pods
  containers:
    - name: web
      image: nginx:1.27
```

**Docker containers.** A Docker container runs in a pod VM only when it names the runtime
`io.containerd.smolvm-pods.v2` (`docker run --runtime`, or `HostConfig.Runtime`); dockerd's
default stays runc. It has no pod, so it sets the same annotations on its own OCI spec with
`docker run --annotation KEY=VALUE`, or `HostConfig.Annotations` (Docker 24 and later, which
write them into the spec; `--label` does not). The defaults differ in one place: such a
container stays awake unless it sets `smolvm-pods.floci.io/hibernate: "true"`. The others,
such as `idle-timeout`, `cpu-threshold`, `rosetta` and `wake-on-terminate`, work as for a pod:

```sh
docker run -d --runtime io.containerd.smolvm-pods.v2 \
  --annotation smolvm-pods.floci.io/hibernate=true \
  --annotation smolvm-pods.floci.io/idle-timeout=2m nginx:1.27
```

Its volumes and bind mounts (`-v NAME:/path`, `-v /node/dir:/path`, `--mount type=volume` and
`type=bind`, with `:ro` or `readonly`) are bound in place, as under runc: the VM writes to the
volume's directory under dockerd's data root, so `docker system df`, a runc container on the same
volume, and a container created again on it see the data. A `--tmpfs` is a tmpfs in the VM
([Docker containers](architecture.md#docker-containers)).

## The API

The node service serves HTTP and JSON on the Unix socket `/run/smolvm-pods/api.sock`, which only
root may use, and, with `--api-listen`, on TCP with a bearer token. Both serve the same routes.
`get`, `list` and `events` answer at once in every state, also during a pause or a wake.

| Request | Answer |
| --- | --- |
| `GET /v1/pods?ip=IP&uid=UID&namespace=NS&name=NAME&netns=PATH` | `{"pods": [POD, ...]}`: the pods that match every filter given; no filter lists every pod with a record. An unknown filter is `invalid` |
| `GET /v1/pods/ID` | `POD` |
| `POST /v1/pods/ID/wake` `{"reason", "source", "lease_ttl", "incarnation"}` | Once the pod is awake: `{"pod", "resumed", "cancelled_pause", "generation", "duration_ms", "steps_ms", "clock", "lease"}` |
| `POST /v1/pods/ID/hibernate` `{"reason", "source", "incarnation"}` | Once the pod is asleep, or at once if it already is: `{"pod", "already_asleep", "generation", "duration_ms", "steps_ms", "checkpoint_bytes", "save"}`, with `save` as in an EVENT below |
| `PUT /v1/pods/ID/leases/LEASE` `{"ttl_secs", "holder", "incarnation"}` | `{"pod", "lease"}`. Holds the lease, or renews it if it is held. It does not wake the pod |
| `PATCH /v1/pods/ID/leases/LEASE` `{"ttl_secs", "incarnation"}` | `{"pod", "lease"}`. Renews a lease that has not expired, and fails with 404 `lease_not_found` once it has, when the pod may already sleep again |
| `DELETE /v1/pods/ID/leases/LEASE` | `{"pod"}`. An unknown lease is not an error |
| `GET /v1/pods/ID/agent` | `{"pid", "threads", "fds"}`: the guest agent's process, counted in the guest. Fails for a pod that is not awake, and does not wake it |
| `GET /v1/events?cursor=SEQ&limit=N&pod=SANDBOX` | `{"events": [EVENT, ...], "cursor", "gap"}`: the events after SEQ (default 0), at most N (1 to 500, default 500), of one pod or all. `cursor` is the last sequence number read; `gap` is true when events after SEQ were rotated away |
| `GET /v1/network` | `{"generation", "effective_isolation", "node_isolation", "address", "cluster_cidrs", "routes", "trust", "error", "counters"}`: the network profile in force, by its generation, or null before one, and the attempts the sinkhole and relay counted since the node service started |
| `PUT /v1/network` `PROFILE` | Checks the profile, applies it, keeps it in `.node/network.json` and answers as `GET /v1/network`. A profile that fails a check is `invalid`, and one that nftables refuses is `apply_error`; the profile in force stays. `tests/fixtures/network-profile.json` is its contract with Floci |
| `GET /v1/network/events?cursor=SEQ&limit=N` | `{"events": [NETWORK_EVENT, ...], "cursor"}`: the network events after SEQ. With `follow=true`, each one as a line, as `/v1/events` follows |
| `GET /v1/events?follow=true&cursor=SEQ&pod=SANDBOX` | Every event after SEQ, one JSON object per line (`Content-Type: application/x-ndjson`, chunked), then each new event as it happens, until the client closes the connection. After 30 s without an event it sends a blank line, which keeps the connection open through port forwards |
| `GET /v1/logs/LOG` | Each line written to a node log from now on (`Content-Type: text/plain; charset=utf-8`, chunked), as it is written, until the client closes the connection. LOG is `k3s` for `/var/log/k3s.log` or `kube-apiserver-audit` for `/var/log/kube-apiserver-audit.log`, and any other name is `not_found`. See [Node logs](#node-logs) |
| `GET /v1/traffic` | `{"flows": [{"from", "to", "packets", "bytes", "last"}, ...]}`: the traffic on connections that the pod sandbox `from` opened to the pod sandbox `to`, either way, as the idle loop's last conntrack dump counted it since the node service started, and when it last grew. Empty without `nf_conntrack_acct` |
| `GET /metrics` | The [metrics](#metrics) in the Prometheus text format (`text/plain; version=0.0.4`) |
| `GET /ui` | The [admin UI](#the-admin-ui), one HTML page |
| `METHOD /docker/PATH` | dockerd's Engine API: the request goes to `/run/docker.sock` as `METHOD /PATH` ([Docker and tunnels](#docker-and-tunnels)) |
| `GET /v1/tunnel?to=IP:PORT`, with `Connection: Upgrade` and `Upgrade: tcp` | `101 Switching Protocols`, then a TCP connection to `IP:PORT`, an address on one of dockerd's networks ([Docker and tunnels](#docker-and-tunnels)) |

Every body field is optional, and an empty body is an empty object.

- **ID** is the CRI sandbox id, `uid:UID`, `ip:IP`, `netns:PATH` or `NAMESPACE/NAME`,
  percent-encoded as one path segment (`ns%2Fweb`). When kubelet recreates a pod's sandbox, the
  pod's uid and name can match the failed sandbox as well, so a lookup takes a pod that has not
  failed and is not being deleted, and then the newest.
- **reason** is a kind with no details, `api` (the default), `exec`, `container-create`,
  `signal`, `idle`, `error` or `recovery`, or a whole reason: `{"kind": "traffic", "from":
  "10.42.0.7", "port": 8080}`, with an optional `via`, or `{"kind": "aws", "queue": "URL"}`.
  **source** is free text that the events carry, such as the caller's name.
- **lease_ttl** and **ttl_secs** are seconds, above 0 and at most 60; `ttl_secs` defaults to 30.
  A wake with `lease_ttl` holds a new lease before the wake and returns it as `lease`. A lease id
  has 1 to 128 characters.
- **incarnation**, when given, must be the pod's current incarnation, or the call fails with
  `stale_incarnation`, so that a caller cannot act on a newer pod of the same name by mistake.
- **steps_ms** has the duration of each step that ran, in order, by name: `admit`, `slot`,
  `resume`, `early_release`, `tso`, `ping`, `clock`, `thaw`, `deferred_kills`, `before_release`
  and `release` for a wake, `early_release` being the time from the start of the resume to the
  release of the held traffic when that came before the agent answered
  ([Wake](architecture.md#wake));
  `slot`, `freeze`, `pausing`, `stop-pumps` and `pause` for a pause, then `stop-paused` when the VM process
  outlived a durable pause. A wake that cancelled a pause, and its `pausing` to `awake` event, have
  the pause's steps that ran, then `resume` if smolvm resumed the frozen VM in place, then the
  wake's steps from `tso`. **clock** is `{"skew_ns", "stepped"}`: the guest's realtime less the
  node's before the step, and whether the agent stepped it.
- **resumed** is true only for the call that ran the resume; a wake of an awake pod, or one merged
  into another's resume, returns false. **cancelled_pause** is true when the wake cancelled a
  pause.

A **POD**:

| Field | Meaning |
| --- | --- |
| `sandbox_id`, `namespace`, `name`, `uid` | The pod's identity |
| `labels` | The pod's labels, as an object, from the node service's watch of the API server: `{}` until the watch reports the pod after its sandbox was created |
| `controller` | The workload that controls the pod, `{"kind", "name"}`, such as `{"kind": "Deployment", "name": "orders"}`: the pod's owner with `controller: true`, where a ReplicaSet named after the pod's `pod-template-hash` label is given as its Deployment, and any other owner, such as a StatefulSet, DaemonSet or Job, as it is. Null for a pod with no controller, and until the watch reports the pod |
| `ip`, `mac` | The pod's IP and the guest's MAC, once the VM has booted |
| `netns`, `machine` | The pod's network namespace and its smolvm machine |
| `state` | `creating`, `awake`, `pausing`, `asleep`, `waking`, `failed` or `deleting` |
| `phase` | While `pausing`: `saving` until the checkpoint is durable, then `stopping`; else null |
| `generation` | Counts the pod's transitions |
| `incarnation` | A random id of this record |
| `rosetta`, `hibernate` | Whether the VM has Rosetta, and whether the pod may sleep |
| `standalone` | Whether the pod is a container that did not come from CRI, such as Docker's, whose `sandbox_id` is the container's id ([Docker containers](architecture.md#docker-containers)) |
| `vm_pid` | The VM process while one runs, else null |
| `leases` | Each `{"id", "kind": "api", "holder", "expires_at"}` held through the API, and `{"id", "kind"}` for the implicit leases: `exec` for a running or starting exec, `create` for a container being created, `waiter` for a wake waiter |
| `created`, `state_since` | Times, as `YYYY-MM-DDTHH:MM:SS.mmmZ` |
| `last_wake_ms`, `last_pause_ms` | The last wake's and pause's durations |
| `checkpoint_bytes` | The size of the checkpoint |
| `wake_failures`, `failure` | Failed resumes in a row, and why the pod failed |

The pod's record keeps `labels` and `controller`, and the watch updates them when the pod's
labels change. After a restart of the node service they are there before the watch lists the
pods again, also while the API server is down.

An **EVENT**, one per state change:

| Field | Meaning |
| --- | --- |
| `seq` | The sequence number, rising, also across restarts of the node service |
| `time` | As `YYYY-MM-DDTHH:MM:SS.mmmZ` |
| `pod`, `incarnation`, `generation` | The pod |
| `from`, `to` | The states; `from` is `none` for a new record and `to` is `deleted` for a removed one |
| `reason` | The reason's kind: `api`, `exec`, `container-create`, `signal`, `traffic`, `aws`, `idle`, `error`, or `recovery` for a change that the node service's recovery made |
| `reason_detail` | The whole reason for `traffic` (`from`, the caller's `NAMESPACE/NAME` or address; `via`, such as `route payments` when the relay of a routed origin carried the call; and `port`) and `aws` (`queue`) |
| `source` | The caller's `source` |
| `steps_ms`, `duration_ms` | The transition's steps and duration |
| `checkpoint_bytes`, `clock` | Of a pause, and of a wake |
| `save` | Of a pause that wrote the guest's RAM: `{"mode", "written_bytes"}`, where `mode` is `full` for the whole RAM image or `delta` for only the 4 KiB pages that changed since the pod's last pause, and `written_bytes` is the RAM data that the pause wrote |
| `error`, `warning` | Why a transition failed, was refused or did not happen (for example `not sleeping: REASON` or `wake refused: awake pods 2/2`), and a warning such as a pod that runs without Rosetta because the TSO canary failed, or `trust stale: ...` for a wake that gave containers Floci's new CA |
| `cancelled` | true for a pause that a wake cancelled (`pausing` to `awake`) |
| `capture_to_release_ms` | For a wake on traffic, the time from the first held frame to its release |
| `waiting` | On `asleep` to `asleep`: a wake waits for admission, and why, such as `awake pods 2/2`, or `earlier wakes wait for room` for one that fits but is behind others |
| `recovered` | true for a change that recovery made |

A wake is the pair `asleep` to `waking` and `waking` to `awake`. A pause that failed with the VM
still running is `pausing` to `awake` with `error` set. A wake that waits for admission first has
an event from `asleep` to `asleep` with `waiting`, once, with the wake's reason and source; one
that admission then refused is `asleep` to `asleep` with `error`.

**Errors** are `{"error": {"code", "message", "reasons"}}` with an HTTP status:

| Code | Status | When |
| --- | --- | --- |
| `invalid` | 400 | A bad body, query, reason or lease |
| `unauthorized` | 401 | TCP only: no token, or the wrong one |
| `not_found` | 404 | No such pod, or no such node log |
| `lease_not_found` | 404 | A renewal of a lease that expired or does not exist |
| `refused` | 409 | A hibernate or wake that cannot run, with `reasons`: `lease held`, `exec running`, `container being created`, `wake waiter`, `hibernation disabled`, `state STATE`, `wake refused` (admission, with its reason in the message), or the `pausing` hook's reason, such as `a packet after the idle decision` |
| `cancelled` | 409 | A hibernate whose pause a wake cancelled |
| `failed` | 409 | A wake, or another call that needs the VM, of a pod that failed |
| `stale_incarnation` | 409 | The call named an older incarnation |
| `gone` | 410 | The pod's lifecycle ended during the call |
| `vm_error`, `store_error` | 500 | A VM operation or the durable store failed |
| `apply_error` | 500 | nftables refused a network profile |
| `forbidden` | 403 | A tunnel to an address that is not a container's on dockerd's networks |
| `unreachable` | 502, or 504 after 30 s | A tunnel whose connection to the container failed |
| `unavailable` | 503 | A `/v1/network` route while the node's network is not running; a `/docker` route or a tunnel while dockerd cannot be reached |

### Node logs

`GET /v1/logs/LOG` follows one of two fixed files of the node, by name, never by path. Floci ships
the EKS control plane's logs to CloudWatch Logs from it.

| LOG | File |
| --- | --- |
| `k3s` | `/var/log/k3s.log`, which k3s writes itself and rotates at 50 MB |
| `kube-apiserver-audit` | `/var/log/kube-apiserver-audit.log`, kube-apiserver's audit log, at the `audit-log-path` that Floci passes |

- The follow starts at the file's end: it sends only the lines written after the request. A file
  that does not exist yet is waited for, and then read from its start.
- Each complete line is sent as it is written, ending in a newline. A partial last line waits for
  its newline. A line over 256 KiB is sent in parts of 256 KiB. Bytes that are not UTF-8 are
  replaced with U+FFFD.
- Blank lines of the file are not sent. After 30 s without a line the follow sends a blank line,
  as the follow of `/v1/events` does, so a client skips blank lines.
- It follows rotation. When the path names another file (a rename and a new file), it reads the
  old file to its end, sends its partial last line, and goes on with the new file from its start.
  When the file shrinks below the read offset (a truncate), it starts again at the file's start.
- It reads the file every 250 ms, and the stream ends when the client closes the connection.

## The API over TCP

With `--api-listen ADDR:PORT` the node service also serves the API on TCP, for Floci. Every
request but `GET /ui` needs `Authorization: Bearer TOKEN`, with the token in `--api-token-file`.
A request without it, or with another token, gets 401, `{"error": {"code": "unauthorized", ...}}`
and `WWW-Authenticate: Bearer`, before any route is matched. The admin UI's page holds no data
and asks for the token itself. There is no TLS.

The node service reads the token when it starts, and refuses to start if the file does not belong
to its own user, if group or others may read or write it, if it does not hold one token of at least
16 visible ASCII characters, or if the port is taken. The Unix socket needs no token.

The node image listens on `0.0.0.0:7070` (`FLOCI_NODE_API_LISTEN`), on every address of the node,
so pods can reach it too, but not without the token. A Mac reaches it through the port that
`node-vm.sh create --service-port` publishes, and `node-vm.sh token` prints the token
([node/README.md](../node/README.md#the-node-services-api-over-tcp)).

## Docker and tunnels

The API also serves the node's dockerd, so that Floci runs Docker containers in pod VMs
([Docker containers](#pod-annotations)) and reaches them with the API's token alone: nothing
else is published.

**The Docker proxy.** Every method on `/docker/PATH` goes to dockerd's socket,
`/run/docker.sock`, as `/PATH` with its query, on a connection of its own, so a Docker client
whose host is `tcp://NODE:PORT/docker` works through it, docker-java among them. The proxy drops
the `Authorization` header and the hop-by-hop headers, and streams both bodies as they come: an
image that `POST /docker/images/load` sends, followed logs, and the raw stream that dockerd sends
for an exec or an attach until it closes the connection. A request with `Connection: Upgrade` and
`Upgrade: tcp` that dockerd upgrades (`101 UPGRADED`, as `docker attach` and `docker exec -i`
ask) gets dockerd's 101, and the two connections are then joined until either side closes.
Errors are dockerd's own, except `unavailable` (503) when dockerd cannot be reached.

**Tunnels.** `GET /v1/tunnel?to=IP:PORT`, with `Connection: Upgrade` and `Upgrade: tcp`, opens a
TCP connection from the node to `IP:PORT` and then answers `101 Switching Protocols` with
`Connection: Upgrade` and `Upgrade: tcp`; from then on the connection carries the bytes of that
TCP connection both ways, and a half-close passes through. `to` may be percent-encoded, and an
IPv6 address is in brackets. Only an address on one of dockerd's networks, by the subnets of
`GET /networks`, is allowed, but not the network's own address, its broadcast address or its
gateway, which is the node; any other address is `forbidden`. The connection is made before the
101, so a refused connection is `unreachable` (502), and one with no answer in 30 s is
`unreachable` (504). The node service makes the connection on the node, so a connection to a
sleeping container wakes it like any other traffic, and the 101 comes once it is awake.

```sh
curl -s -H "Authorization: Bearer $TOKEN" http://127.0.0.1:17070/docker/v1.47/containers/json
curl -s -N -H "Authorization: Bearer $TOKEN" -H 'Connection: Upgrade' -H 'Upgrade: tcp' \
  'http://127.0.0.1:17070/v1/tunnel?to=172.17.0.2:5432'
```

## Metrics

`GET /metrics` serves these, on the Unix socket and, with the bearer token, on TCP. A Prometheus
scrape job sets the token with `authorization: {credentials_file: FILE}`. Every series is of the
node, by a label with few values; only the traffic series are by pod, one for each pair of pods
that talked. Counters start at 0 when the node service starts.

| Metric | Type | Labels | What it counts |
| --- | --- | --- | --- |
| `smolvm_pods_pods` | gauge | `state` | Pods with a record, by state: `creating`, `awake`, `pausing`, `asleep`, `waking`, `failed`, `deleting` |
| `smolvm_pods_checkpoint_bytes` | gauge | | The checkpoints of the sleeping pods |
| `smolvm_pods_wakes_total` | counter | `reason`, `outcome` | Wakes, by the reason's kind, and `awake`, `cancelled_pause` (the wake cancelled a pause), `refused` (admission) or `failed` (the resume failed and the pod sleeps on) |
| `smolvm_pods_wake_duration_seconds` | histogram | `reason` | From a wake's start, admission included, to the pod being awake |
| `smolvm_pods_sleeps_total` | counter | `reason`, `outcome` | Pauses, by the reason's kind, and `asleep`, `cancelled` (by a wake, counted by the wake's reason) or `failed` |
| `smolvm_pods_sleep_duration_seconds` | histogram | `reason` | From a pause's start to the pod being asleep |
| `smolvm_pods_checkpoint_written_bytes_total` | counter | `mode` | The guest RAM that the pauses that completed wrote: `full` or `delta` |
| `smolvm_pods_failures_total` | counter | | Pods that failed |
| `smolvm_pods_admission_waits_total` | counter | | Wakes that did not fit under the caps at once and waited |
| `smolvm_pods_admission_refusals_total` | counter | | Wakes refused after the admission wait |
| `smolvm_pods_admission_waiting` | gauge | | Wakes waiting for admission now |
| `smolvm_pods_admission_wait_seconds` | histogram | | How long each wake that waited waited, admitted or refused |
| `smolvm_pods_network_attempts_total` | counter | `kind` | `blocked` (the sinkhole took a connection or datagram), `relayed` and `relay_failed` (a routed connection); the counters of `GET /v1/network` |
| `smolvm_pods_kubernetes_events_total` | counter | `result` | [Kubernetes Events](#kubernetes-events): `sent`, `rate_limited`, `dropped` (the queue of 64 was full, or the reporter fell behind) or `failed` (the API server could not be reached or refused it) |
| `smolvm_pods_pod_traffic_packets_total`, `smolvm_pods_pod_traffic_bytes_total` | counter | `source_namespace`, `source_pod`, `destination_namespace`, `destination_pod` | The flows of `GET /v1/traffic`: packets and bytes, either way, on connections that the source pod opened to the destination pod |
| `process_resident_memory_bytes`, `process_virtual_memory_bytes`, `process_cpu_seconds_total`, `process_open_fds`, `process_start_time_seconds` | gauge, and counter for `_total` | | The node service's own process, from `/proc/self` |

The `reason` label is a reason's kind, as in the events: `api`, `exec`, `container-create`,
`signal`, `traffic`, `aws`, `idle`, `error` or `recovery`. The durations of changes that recovery
made are not observed.

## Kubernetes Events

The node service posts a core/v1 Event about the pod, from the component `smolvm-pods` on the
node, for these events of the log, with kubelet's credentials:

| Reason | Type | When | Message, for example |
| --- | --- | --- | --- |
| `Slept` | Normal | `pausing` to `asleep` | `slept in 1.8 s (idle), a checkpoint of 1024 MiB` |
| `Woke` | Normal | `waking` to `awake` | `woke in 230 ms (traffic from ns/web to port 8080)` |
| `WakeWaiting` | Normal | a wake waits for admission | `a wake (messages on QUEUE_URL, by floci) waits for admission: awake pods 2/2` |
| `SleepCancelled` | Normal | a wake cancelled a pause | `a wake (api, by floci) cancelled the sleep` |
| `WakeRefused` | Warning | admission refused a wake | `wake refused: awake pods 2/2 (traffic from ns/web to port 8080)` |
| `WakeFailed` | Warning | a resume failed and the pod sleeps on | `the wake failed: ERROR` |
| `SleepFailed` | Warning | a pause failed or was refused, and the pod stays awake | `the sleep failed: ERROR` |
| `VMFailed` | Warning | the pod failed | `the pod's VM failed: ERROR` |

`kubectl get events --field-selector involvedObject.name=POD` and `kubectl describe pod` show
them. Each pod may send 10 at once and one more every 30 s, and the node 25 at once and one more
every second. An Event over either budget is dropped, not queued, and counted in
`smolvm_pods_kubernetes_events_total`; the event log keeps every change.

## The admin UI

`GET /ui` serves one HTML page with its script and style inline, built into the binary: it
loads nothing else and needs no build step. Open it on the API's TCP listener, such as
`http://127.0.0.1:17070/ui` from a Mac ([node/README.md](../node/README.md#the-node-services-api-over-tcp)).
It asks for the bearer token once, keeps it in the tab's `sessionStorage`, and sends it on each
request. It shows:

- **Node**: cards from `GET /metrics` every 10 s: the pods by state, wakes, sleeps, the wake
  duration's p50 and p95 estimated from the histogram, refused wakes, admission waits, the
  checkpoints, blocked attempts, Kubernetes Events sent and the node service's resident memory,
  and every metric as text.
- **Pods and who woke whom**: each pod on a circle, in its state's color, with a dashed arrow
  from each wake's source to the pod it woke: a pod for `traffic` from a pod, else a box for the
  address, the queue of an `aws` wake or the `source` of a wake through the API. Solid arrows are
  `GET /v1/traffic`, every 15 s, from client to server, thicker for more bytes.
- **Pods**: a table from `GET /v1/pods`, read again after each event.
- **Timeline**: for the pod clicked, a strip of its states over the last hour and its events,
  newest first, with reasons, durations, errors and admission waits. `/ui#pod=SANDBOX` opens
  with that pod's timeline.

The page follows `GET /v1/events?follow=true` from the start of the log, keeps the last 300
events of each pod, and follows again 2 s after the stream ends.

## smolvm-pods ctl

`smolvm-pods ctl` calls the API on its Unix socket, as root:

```sh
smolvm-pods ctl list
smolvm-pods ctl get POD
smolvm-pods ctl wake POD [--reason REASON] [--source TEXT] [--lease-ttl SECONDS]
smolvm-pods ctl hibernate POD [--reason REASON] [--source TEXT]
smolvm-pods ctl hold POD --lease ID --ttl SECONDS [--holder TEXT]
smolvm-pods ctl release POD --lease ID
smolvm-pods ctl events [--cursor SEQ]
smolvm-pods ctl agent POD
```

- POD is the CRI sandbox id, `uid:UID`, `ip:IP`, `netns:PATH` or `NAMESPACE/NAME`.
- REASON is a reason kind with no details: `api`, `exec`, `container-create`, `signal`, `idle`,
  `error` or `recovery`.
- `events` prints every event after SEQ (default 0), one per line, a page at a time, and exits.
  Every other command prints the API's answer, one JSON document.
- The exit status is 0 for success; 1 when the API answered with an error, which is printed as
  `{"error": {"code", "message", "reasons"}}`; 2 for a wrong command line; and 3 when the API
  cannot be reached or the connection closes before the answer, as when the node service dies
  during the call.

## Files

| Path | Content |
| --- | --- |
| `/run/smolvm-pods/node.sock` | The front shim's connection to the node service |
| `/run/smolvm-pods/node.sock.lock` | The flock that the node service serving `node.sock` holds |
| `/run/smolvm-pods/api.sock` | The API, mode 0600 |
| `/run/smolvm-pods/cursors/SANDBOX/ID[@EXEC]` | Each output pump's cursor |
| `/run/smolvm-pods/netns/ID` | The network namespace of each standalone container, such as Docker's, bound there while its pod exists |
| `/var/lib/containerd-shim-smolvm-pods/ID/podshare` | Each sandbox's pod share, which its VM mounts: the containers' rootfs and mounts, in `CONTAINER/mounts/N`, a copy for a CRI container and a bind of the source for a standalone one. The node removes the pod shares at boot, after it detaches the binds under them |
| `/var/lib/containerd-shim-smolvm-pods/.node/pods/SANDBOX.json` | Each pod's record |
| `/var/lib/containerd-shim-smolvm-pods/.node/events.jsonl` | The event log, rotated to `events.jsonl.1` at 8 MiB |
| `/var/lib/containerd-shim-smolvm-pods/.node/network.json` | The network profile in force |
| `/var/lib/containerd-shim-smolvm-pods/.node/network-events.jsonl` | The network event log, rotated at 8 MiB |
| `/var/lib/containerd-shim-smolvm-pods/.node/node.lock` | The flock that one node service holds |
| `/var/lib/containerd-shim-smolvm-pods/.node/tombstones/ID` | A sandbox delete in progress |
| `/var/lib/containerd-shim-smolvm-pods/.node/faults-fired` | The kill fault points that already fired |
| `/var/lib/containerd-shim-smolvm-pods/.tso-canary` | The TSO canary's verdict, until the node restarts |
| `/var/lib/containerd-shim-smolvm-pods/.tso-canary.lock` | The flock under which the TSO canary runs |
| `/var/lib/containerd-shim-smolvm-pods/.templates.lock` | The flock under which smolvm's disk templates are expanded |
| `/run/containerd/s/HASH.lock` | Beside the front shim's socket: the flock that serializes containerd's `start` actions in a namespace until its front shim serves |
| `DIR/smolvm-pods-tso` | In a VM's directory: `PID STARTTIME` of the VM process that turned TSO on |
| `DIR/pause-*.smolcheckpoint` | In a VM's directory: the checkpoint of a sleeping pod |
| `/var/log/smolvm-pods/front-shim.log` | The front shim's log, which `floci-node` rotates to `front-shim.log.1` above 64 MiB |
| `/var/log/smolvm-pods/node-service.log` | The node service's stdout and stderr, which `floci-node` appends and rotates to `node-service.log.1` above 64 MiB, and a line from `floci-node` at each restart |
| `/run/floci-node/refused` | Why `floci-node` stops the node: kubelet's configuration evicts on memory or runs a CPU or memory manager. `floci-node run` logs it as it exits |
| `/etc/floci-node/seed` | The seed directory of `node-vm.sh create --seed DIR`, read-only ([node/README.md](../node/README.md#the-seed)) |
| `/var/lib/floci-node/seed/installed` | The paths of the files that the last seed installed, one per line |
| `/var/lib/floci-node/seed/image/PATH` | The image's file at PATH, while a seed file replaces it |
| `/etc/hosts` | Between `# floci-node seed: begin` and `# floci-node seed: end`, the seed's host names at the gateway of the node's default route |

## Test settings

These exist for tests and for trying things on a node.

**`/run/smolvm-pods/node-service.args`.** While this file exists, each start of the node service
adds its words, separated by spaces or new lines, to the flags from `/etc/floci-node/env`. A flag
given twice takes its last value, so the file can replace one:

```sh
scripts/node-vm.sh exec -- sh -c 'echo --idle-timeout 1m > /run/smolvm-pods/node-service.args'
scripts/node-vm.sh exec -- pkill -f "^smolvm-pods node-service"   # floci-node starts it again
```

**Fault points.** `SMOLVM_PODS_FAULT` takes a comma-separated list of points; `POINT@SANDBOX`
arms a point for one pod sandbox. When a point fires, the node service writes `fault point POINT
reached` to stderr, followed by ` for pod sandbox SANDBOX` where the pod is known.

| Point | Where | What it does |
| --- | --- | --- |
| `pause:saving` | The record says `pausing`, the VM is frozen and smolvm is saving it | Kills the node service with SIGKILL, once |
| `pause:durable` | The checkpoint is durable and smolvm's record says Paused, before the node service checks the VM process and writes `asleep` | Kills the node service, once |
| `wake:resumed` | The resume returned and the new VM process runs, before the TSO check, the ping and the clock step | Kills the node service, once |
| `wake:clock` | The agent answered `SetClock`, before the deferred kills and `awake` | Kills the node service, once |
| `idle:decided` | An idle pause, after the record says `pausing` and before the `pausing` hook holds the pod's traffic | Waits 5 s, once |
| `wake:resume-error` | Each resume, before smolvm restores the checkpoint | Fails the resume and leaves the checkpoint intact, every time, as a checkpoint that cannot be restored would: the third failure in a row fails the pod |

A kill point is recorded in `.node/faults-fired` before the process dies, and a node service
restarted with the same environment skips it; remove the file to arm it again. On a node,
`floci-node` takes `/run/smolvm-pods/fault` at each start of the node service, if it exists: it
removes the file and starts the node service with `SMOLVM_PODS_FAULT` set to the file's first line.
The next start runs without it.

**Other variables.**

| Variable | Read by | Effect |
| --- | --- | --- |
| `SMOLVM_PODS_API_SOCKET` | `ctl` | The API socket, in place of `/run/smolvm-pods/api.sock` |
| `SMOLVM_SHIM_MOCK=1` | The node service | Mock pods that boot no VM and do not sleep |
