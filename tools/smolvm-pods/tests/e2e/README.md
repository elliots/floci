# smolvm-pods end-to-end tests

The tests run on the Mac. They create a node VM with `scripts/node-vm.sh`, build the test image
with Docker, import it into the node's containerd without a registry, and run kubectl inside the
node. This package is a Cargo workspace of its own: it has no dependencies and builds with the
Mac's Rust toolchain.

| Suite | What it checks |
| --- | --- |
| [`pods`](#pods) | Pods run as nested VMs in the node VM, amd64 pods under Rosetta with TSO |
| [`network`](#network) | Pod networking: Services, NodePorts, DNS, the Mac, the internet and the delete of a pod's network |
| [`hibernation`](#hibernation) | Transparent hibernation through the node service's API |
| [`wake`](#wake-on-traffic-and-idle-sleep) | Wake on traffic and idle sleep |
| [`scale`](#scale) | Hundreds of pods on one node, through the capacity that kubelet reports |

## Run

From the floci repository, after building the shim, the shim's unit test binary, the node kernel,
the patched libkrun, the guest agent
([The guest agent](../../docs/getting-started.md#the-guest-agent)) and the node image.
[Run a node on a Mac](../../README.md#run-a-node-on-a-mac) lists what these builds need first:
the smolvm checkout, its `target/floci` and the Docker images.

```sh
tools/smolvm-pods/scripts/build.sh
tools/smolvm-pods/scripts/build.sh test --locked --profile release-small --no-run
#   prints the test binary: target/release-small/deps/containerd_shim_smolvm_pods_v2-<hash>
tools/smolvm-pods/node/kernel/build.sh
tools/smolvm-pods/node/libkrun/build.sh
tools/smolvm-pods/scripts/node-vm.sh build
SMOLVM_PODS_E2E=1 \
SMOLVM_PODS_E2E_NETNS_TEST=$PWD/tools/smolvm-pods/target/release-small/deps/containerd_shim_smolvm_pods_v2-<hash> \
cargo test --locked --manifest-path tools/smolvm-pods/tests/e2e/Cargo.toml -- --ignored --nocapture
```

This runs every suite, one after the other, each on a node of its own. To run one, name it with
`--test`; only `pods` needs `SMOLVM_PODS_E2E_NETNS_TEST`:

```sh
SMOLVM_PODS_E2E=1 \
cargo test --locked --manifest-path tools/smolvm-pods/tests/e2e/Cargo.toml --test network -- --ignored --nocapture
```

The suites are ignored unless `--ignored` is given, and then they fail unless
`SMOLVM_PODS_E2E=1`, so that a run cannot pass without a node. A missing prerequisite fails them
too: Docker, the pinned Go image in the local Docker cache (`golang@sha256:2a0ba12e...`, never
pulled), and what `node-vm.sh create` needs, such as the node image tar and `smolvm-floci`. The
network test also needs the Mac to reach `example.com` over HTTP.

- `SMOLVM_PODS_E2E_NODE=NAME` runs against that running node and leaves it running. By
  default, each test creates a node named `floci-SUITE-e2e-ID`, SUITE being its name above, with
  8 vCPUs (and 8 GiB for the hibernation and wake tests, 16 GiB for the scale test), its API and
  one NodePort on free ports, and deletes it at the end.
- `SMOLVM_PODS_E2E_PREFIX=PREFIX` names what the test creates `PREFIX-ID` instead: the node, the
  namespace, and the Docker images and containers on the Mac.
- `SMOLVM_PODS_E2E_NODE_PORT=PORT` is a NodePort that a running node publishes on the Mac, for
  the network and wake tests (default: 30080, the first that `node-vm.sh create` publishes).
- The node's shim must be the build under test: the one at `SMOLVM_PODS_SHIM` (default:
  `target/release-small/containerd-shim-smolvm-pods-v2`, as for `node-vm.sh build`), or the
  SHA-256 in `SMOLVM_PODS_E2E_SHIM_SHA256`.
- `SMOLVM_PODS_E2E_NETNS_TEST` is the shim's unit test binary, whose network test the pods
  test's bridge check runs in the node. It must be an absolute path: cargo runs the tests in
  `tests/e2e`.
- `SMOLVM_PODS_E2E_KEEP=1` keeps the node and the test's namespace for debugging.
- `node-vm.sh` reads its own variables, such as `SMOLVM_PODS_WORK_DIR` for the node image.

Every test asserts that the node's THP policy is `always`, and prints the node's THP collapse
counters and khugepaged settings before and after the test.

Teardown deletes what the test created: the node VM, with smolvm's copies of its image, or the
test's namespace and images in a running node, the local Docker images, and the scratch
directory. An interrupted run can leave a node behind; delete it with
`SMOLVM_PODS_NODE=floci-SUITE-e2e-ID tools/smolvm-pods/scripts/node-vm.sh delete`.

`cargo test --locked --manifest-path tools/smolvm-pods/tests/e2e/Cargo.toml` without `--ignored`
runs the harness's own tests, which need no node.

## Pods

`tests/pods.rs` runs three pods with the `smolvm-pods` RuntimeClass: `arm64` (app and sidecar)
with the annotation `smolvm-pods.floci.io/rosetta: "false"`, `amd64` with no annotation, so that
it gets Rosetta by default, and `arm64-tso` with `smolvm-pods.floci.io/rosetta: "true"`. Once
they are Ready, it waits for each app's first answer, then runs every check and reports each one
that fails:

- **Own VMs**: each pod is Running and Ready without restarts, its kernel command line and boot
  id differ from the node's and from each other's, the node runs one VM process for it, and that
  VM's directory is on the same mount as smolvm's disk templates.
- **Architectures**: the arm64 app reports ELF machine 183. The amd64 app reports ELF machine 62
  on the arm64 node, so it runs through Rosetta.
- **TSO**: the litmus test finds no memory-ordering violations in the amd64 pod or in
  `arm64-tso`, and finds some in `arm64`, without Rosetta, which shows that it can detect them. A
  run whose threads overlapped too little to tell is repeated, up to three times, as the TSO
  canary does.
- **Logs and exec**: `kubectl exec` returns stdout, stderr and the exit code; `kubectl logs`
  shows the app's stdout and stderr.
- **Sidecar**: the app and the sidecar reach each other on localhost and share one kernel.
- **Writable /etc files**: the arm64 app appends a line to `/etc/hosts`, `/etc/resolv.conf`
  and `/etc/hostname`, which kubelet and containerd mount into each container, and reads each
  one back. The app runs as root without a read-only root filesystem, so it may write all three,
  as under runc.
- **Pod network**: each app answers at its pod IP, from the node and from another pod, with its
  own boot id.
- **Bridge in the node**: the shim's network test (`net::netns_tests`) passes in the node,
  against its kernel. It runs in a mount namespace of its own: `ip netns add` would otherwise
  make `/run/netns` a mount of its own over the pods' namespaces, which containerd then cannot
  remove.
- **stdin refused**: `kubectl exec -i` is refused at once, with the reason: the guest agent
  passes no stdin.
- **amd64 with Rosetta off**: an amd64 pod with `rosetta: "false"` is refused, with a reason
  that names the annotation.
- **TSO canary**: the node's TSO canary passed in this boot. While its verdict is a failure: a
  pod with `rosetta: "true"` is refused with the canary's reason; an amd64 pod with no annotation
  gets a sandbox, and its container is refused with the canary's reason; and an arm64 pod with
  no annotation runs without Rosetta: its VM process has no TSO marker, the node service logs
  the warning with the canary's reason, and the litmus test finds violations.
- **No restarts**: after the other checks, each pod is still Running and Ready without restarts.
- **Delete**: deleting the pods ends their VM processes and removes their VM directories.

## Network

`tests/network.rs` checks pod networking. It runs two arm64 pods, `vm` with the `smolvm-pods`
RuntimeClass and `runc` with the node's default runtime, a ClusterIP Service for each, a NodePort
Service for `vm` on the node's published NodePort, and on the Mac a stand-in for Floci: an HTTP
server on the Mac's loopback whose answer carries a token and 64 KiB of padding. Once the pods
are Ready, it runs every check and reports each one that fails:

- **ClusterIP Service**: the runc pod reaches the VM pod's app through its ClusterIP Service,
  and the answer comes from the VM's kernel.
- **NodePort from the Mac**: the Mac reaches the VM pod's app through the NodePort, on the Mac's
  loopback.
- **DNS**: in the VM, DNS resolves the runc pod's Service name to its ClusterIP, and resolves
  `example.com`.
- **Service from the VM**: the VM pod reaches the runc pod through that Service's name.
- **Floci on the Mac**: the VM pod reaches the stand-in for Floci the way the node reaches the
  Mac: at the node's default gateway, smolvm's, which takes TCP to the Mac's loopback. The node
  is asked first, so that a failure tells the two paths apart. The padding needs full-size TCP
  segments.
- **External host**: the VM pod gets `http://example.com/`.
- **Pod IP and MTU**: in the VM, eth0 has the pod IP that Kubernetes reports, and the MTU of the
  pod's CNI interface and of the runc pod's eth0.
- **TCP streams**: a 100 MiB TCP stream from the runc pod to the VM pod, and one back, arrive
  whole: the receiver's SHA-256 matches the sender's. The test prints the throughput of each.
- **No restarts**: after the other checks, both pods are still Running and Ready without
  restarts.
- **Delete**: before the delete, the VM pod's network namespace has podbr0 and tap0. A process of
  the test then holds the namespace, as a process or socket left in it would, so that removing
  the namespace cannot hide what the shim left. Deleting the pod ends its VM process and the
  shim, whose last pod it was, and removes its pod share, its VM directory, smolvm's fork and
  pause locks beside that directory, its ready marker in the agent rootfs, its record in smolvm's
  uid registry, and podbr0 and tap0 from the held namespace, and neither the shim nor the node
  service logs a failure to remove the bridge. Once the test's process ends, no process, nsfs
  file descriptor or mount holds the namespace. Sockets that hold it are not looked for.

## Hibernation

`tests/hibernation.rs` checks transparent hibernation. It runs on a fresh node:

```sh
SMOLVM_PODS_E2E=1 \
cargo test --locked --manifest-path tools/smolvm-pods/tests/e2e/Cargo.toml --test hibernation -- --ignored --nocapture
```

The pods run `testapp hold` (see [Workloads](#workloads)) with the `smolvm-pods` RuntimeClass and
no probes: `arm64` and `amd64` with no annotation, so Rosetta is on by default, and `arm64-plain`
with `smolvm-pods.floci.io/rosetta: "false"`, each holding 256 MiB; `resume` and `resume-plain`,
the same two kinds holding 512 MiB, for the resume-through-the-shim check; and `runc`, a runc pod
from which that check reaches the others. Before the pods start, a `ctr events` recorder starts
in the node, in a session of its own, and runs until the end; it starts `ctr events` again
whenever it ends, as when containerd restarts. The test then waits for each app's first answer
and records what the checks compare with: the memory hash, the app's pid, start time and boot id,
the pod IP, the sandbox, the containers and the VM directory. It copies the arm64 test app into
the node, which makes the requests to the pods' IPs from the node and reads the node's clocks
around them.

The checks run in this order, each also after one fails:

- **20 cycles** of `arm64`, `amd64` and `arm64-plain`: hibernate each through the API; its VM
  process is gone and the node's `AnonPages` dropped by at least 75% of the VM process's
  anonymous memory; sleep 5 s (60 s in every fifth cycle) while crictl shows every sandbox ready
  and every container running and the API shows the pods asleep; wake each through the API, which
  names the one new VM process. After each wake, `/hash`, `/id`, the pod IP, the sandbox, the
  VM directory and every restart count are unchanged. The check also records the clock samples,
  the TSO markers and the guest agent's counts for the clock, TSO, and node settings and guest
  health checks.
- **Logs**: each pod's `tick N` lines in `kubectl logs` run from 1 to the app's latest counter
  (`/ticks`), with no gap and no repeat.
- **Clock**: after each wake, the guest's realtime was within 100 ms of the node's, and its
  monotonic clock advanced by the time the guest ran, within 50 ms: the node's time between the
  samples, less the time it was frozen, which lies between the end of the hibernate call and the
  start of the wake call at least, and between the start of the one and the end of the other at
  most (`checkers::check_monotonic`). Each `waking` to `awake` event of the cycles records the
  clock step.
- **TSO**: at setup, the VM processes of `arm64`, `amd64` and `resume` had TSO on (the marker
  in the [runtime contract](#runtime-contract)) and those of the `rosetta: "false"` pods had not;
  after each wake of the cycles the same held for the new VM process; after the last cycle, the
  litmus test finds no violation in `amd64`.
- **Exec wakes**: `kubectl exec` into the sleeping `arm64-plain` returns stdout, stderr and the
  exit code, and the pod is awake after one resume with the reason `exec`. While a 30 s exec
  runs, the pod has an `exec` lease and a hibernate is refused with `exec running`; while a hold
  is held, with `lease held`.
- **Merged wakes**: 12 concurrent wakes of the sleeping `arm64` all return it awake, with one
  resume event, and one of them says it resumed the pod.
- **Wake during a pause**: a wake 100 ms after `ctl get` first shows `arm64` pausing leaves it
  awake with the same VM process, a `pausing` to `awake` event with `cancelled`, and no resume.
  A resume after the pause fails the check.
- **Calls while asleep**: for each sleeping pod, `crictl statsp`, `crictl stats --pod` and
  `crictl stats` of each container answer within 1 s, `crictl inspectp` and `crictl inspect`
  show it ready and running, a hold and release work, and the pod stays asleep with no VM
  process.
- **Resume through the shim**, for `resume` and `resume-plain`, which shows what Rosetta by
  default costs: first the awake pod is measured, then 20 cycles of a hibernate, 2 s asleep and
  a wake. `testapp firstanswer`, in the runc pod's network namespace,
  runs `ctl wake` and polls the pod's IP from just before it. The poll's first SYN can start the
  resume as a traffic wake, which the wake call joins; each cycle must resume the pod once. Each
  cycle must answer within 0.5 s of the wake call, with the VM process's `AnonHugePages` at
  least 95% of its RSS at that answer, smolvm's IPv6 probe (replayed by `testapp ipv6probe` in
  the VM process's network namespace) at most 5 ms, and the memory hash unchanged. It prints the
  median, 95th percentile and range of those, of the time from the wake's return to the answer,
  and of each wake step.
- **No process per sleeping pod**: with every pod asleep, the runtime's only lasting processes
  are one front shim and one node service. Five more pods (`idle-1` to `idle-5`, 16 MiB) are
  created and hibernated; the front shim is the same process and its USS grew by less than
  50 KiB per pod. They are then deleted.
- **SIGKILL of the node service**: first with `arm64` and `amd64` asleep and `arm64-plain`
  awake; after the restart, the two are still asleep, `arm64-plain` is awake with the same VM
  process, the wakes work, nothing changed, and `arm64-plain`'s log goes on with no gap and at
  most one replayed frame. Then at each fault point that kills the node service, with `arm64`:
  the call that reaches it fails, the node service logs the point and restarts, and the pod ends
  as recovery decides: `pause:saving` awake with the same VM process, `pause:durable` asleep
  with none (and a wake then works), `wake:resumed` and `wake:clock` awake with the clock within
  100 ms and TSO on. Nothing about the pod changed.
- **containerd restart**: with the three pods asleep, k3s, its server and containerd get
  SIGKILL, and `floci-node` starts them again. The pods' tasks reload ready and running with
  the same ids, the pods stay asleep, the recorder saw no exit or delete of their tasks, and
  after the next wake nothing changed and their logs go on without a gap. Events that
  containerd publishes while it loads, before the recorder reconnects, are not seen, so the
  check also compares the tasks' state.
- **Delete while asleep**: deleting the sleeping `arm64-plain` wakes nothing, and removes its
  VM directory with its checkpoints, smolvm's locks and ready marker, its pod share and its
  record; `ctl get` then answers `not_found`. A process of the test holds the pod's network
  namespace, as in the network test, and podbr0 and tap0 are gone from it. The disk use of the
  smolvm volume goes back to at most 32 MiB over what it was with the pod awake.
- **No false exits**: the recorder runs and saw every container of the test's pods start, and
  saw no `/tasks/exit` or `/tasks/delete` of them except the delete of `arm64-plain`, and
  kubelet logged no `SandboxChanged` or `Killing` for them (for `arm64-plain`, before its
  delete). The exit of an exec, whose event names the exec in its `id`, is not one. kubelet's
  events last an hour.
- **Node settings and guest health**: khugepaged's `scan_sleep_millisecs` and
  `alloc_sleep_millisecs` are 86400000, the node's boot id has not changed, and the guest agent
  of each cycled pod had the same thread and descriptor counts after cycle 20 as after cycle 1.
  Each count is the settled reading of five samples 200 ms apart: the value seen most often, if
  it is seen at least twice and no other value is seen as often. A connection that the agent
  serves for a moment, such as kubelet's stats poll every 10 s, changes one sample at most,
  while a leak changes them all.
- **SIGKILL of the front shim**, last, because kubelet then recreates every pod: with `arm64`
  and `resume` asleep and `amd64` and `resume-plain` awake, kubelet gives each pod a new sandbox,
  nothing of the old ones is left (VM process, VM directory, checkpoints, locks, pod share,
  record, network namespace), the node service has no record of them, and each new pod's app
  answers from a new kernel.

A check that judges what the cycles did (logs, clock, TSO, no false exits, and node settings and
guest health) fails unless the cycle check completed all 20. A check whose call to the runtime
fails, fails; nothing is skipped. If `smolvm-pods ctl list` does not answer when the test
starts, every later call fails at once with that reason.

The checkers that need no node, `checkers::Ticks`, `checkers::skew`,
`checkers::check_monotonic` and `checkers::settled`, the `ctr events` parser and the parsing of
every ctl output of the [runtime contract](#runtime-contract) in `src/runtime.rs` have unit tests
(`cargo test` without `--ignored`).

## Wake on traffic and idle sleep

`tests/wake.rs` checks wake on traffic and idle sleep. It runs on a fresh node of 8 GiB:

```sh
SMOLVM_PODS_E2E=1 \
cargo test --locked --manifest-path tools/smolvm-pods/tests/e2e/Cargo.toml --test wake -- --ignored --nocapture
```

Every VM pod runs `testapp hold` (see [Workloads](#workloads)) with all its servers (HTTP on
8080, HTTPS on 8443, gRPC health on 9090, UDP echo on 8081) and the annotation
`smolvm-pods.floci.io/idle-timeout: "10s"`:

- `web` (64 MiB), with a ClusterIP Service, for most checks;
- `caller` and `callee`, each with a ClusterIP Service, for the pod-to-pod check;
- `probes-http`, `probes-tcp`, `probes-grpc` and `probes-https`, with readiness and liveness
  probes of that kind every 2 s, and `probes-exec`, an amd64 image whose exec probe
  (`/testapp get http://127.0.0.1:8080/health`, every 10 s) runs under Rosetta. A NodePort
  Service on the node's published NodePort reaches `probes-http`'s probe port;
- `unhealthy`, whose readiness fails at the first failed probe and whose liveness restarts it
  only after 15 failures (30 s), longer than it needs to sleep; `doomed`, with HTTP probes; and
  `unready`, whose `/health` answers 500 from the start (`-unready`), with a readiness probe only;
- `follower`, for the outbound check, and `cpu-off`, with the annotation
  `smolvm-pods.floci.io/cpu-threshold: "off"`, for the CPU check;
- `client`, a runc pod. The test's requests run the arm64 test app from the node in the client
  pod's network namespace (`nsenter -n`), so they come from the client's pod IP without
  kubectl exec (`src/traffic.rs`). Its app also serves `/stream`, which the outbound check
  follows.

The test waits until every pod but `unready` is Ready and every app answers, and records each
app's `/id` and `web`'s `/hash`. A pod "sleeps by itself" when its pause starts within the idle
timeout, one poll interval (`--poll-interval`, 15 s) and 2 s for the test's polling, 27 s in all,
after its last activity, and it is asleep within 30 s more with no VM process; the last sleep
event since the activity must have the reason `idle`. Where a check needs a pod asleep or awake
only as its starting point, it hibernates or wakes the pod through the API, so that the check
fails at its own first step.

The checks run in this order, each also after one fails:

- **The test app's endpoints**, against awake pods: `/remote`, `/health` switched to failing
  and back (with the gRPC health service following it), HTTPS, gRPC health, `/stream`, `/ws` with
  pings, `/call` from one pod to another, `/busy`, UDP echo, ICMP echo, a kept connection, a TCP
  connect, the unready app's 500, and every probe pod Ready under kubelet's probes. It needs no
  sleep or wake.
- **Probes leave pods asleep**: the five probe pods sleep by themselves, then stay asleep for
  3 minutes: every 10 s each is asleep with no VM process and Ready; at the end none woke, the
  Ready condition did not change, no container restarted and kubelet recorded no `Unhealthy`
  event. Needs idle sleep, the probe answers for TCP, HTTP, HTTPS and gRPC, and the exec cache.
- **Exec probes under Rosetta**: `probes-exec` slept through the probe check; a
  `kubectl exec` with the probe's exact command, no TTY and no stdin, gets the cached answer
  (`ok`, exit 0) without a wake; an exec of another command wakes the pod once with the reason
  `exec`.
- **NodePort to a probe port**: from the Mac, `GET /id` through the NodePort to the sleeping
  `probes-http` wakes it once with the reason `traffic` and gets the app's own answer.
- **SIGKILL of the node service** with `probes-http`, `probes-tcp` and `probes-grpc` asleep.
  First the test's own probes from the node (HTTP with `User-Agent: kube-probe/1.35`, gRPC
  health and a TCP connect) must be answered at once. Right after the kill a request to
  `probes-tcp` starts; after the restart the test's probes are answered within 5 s, the request
  connects after a SYN retransmit (at least 0.9 s) and is answered. A probe whose SYN and its
  first retransmit fall in the outage times out, and kubelet records an `Unhealthy` event for
  it: the outage is floci-node's 1 s restart delay and the dying node service's exit, which waits
  11 to 50 ms for RCU for each packet socket it closes. Once the node service is back and the
  probes' timeout (2 s) has passed, the three are Ready with no restart and no new `Unhealthy`
  event for 20 s, and `probes-http` and `probes-grpc` did not wake.
- **20 automatic cycles through a Service**: `web` sleeps by itself, then `GET /id` through
  its Service wakes it; the answer is the recorded `/id`, `/hash` follows with the recorded hash,
  and the pod IP, sandbox and restart count are unchanged. There is one resume per cycle with the
  reason `traffic`, and over the whole check there are 20 idle sleeps and 20 traffic wakes.
- **First-request latency**, from the 20 wakes of the cycles: the client's time from the start
  of the request to the first byte has a median under 0.3 s and a 95th percentile under 0.6 s,
  and every `waking` to `awake` event has `steps_ms` and `capture_to_release_ms`. The test
  prints the summaries of both and of each step.
- **10 concurrent requests** to the sleeping `web`: one resume with the reason `traffic`, all
  answered with the recorded `/id`, each connected within 1 s, so no SYN was retransmitted.
- **Client IP preserved**: `/remote` shows the client's pod IP, awake and on a wake, at the
  pod IP and through the Service.
- **Pod-to-pod calls wake the callee**: `caller`'s `/call` reaches the sleeping `callee`
  through its Service and at its pod IP; the callee sees the caller's pod IP and its wake names
  `NAMESPACE/caller` and port 8080. A call through `caller`'s Service while both sleep wakes the
  caller from `NAMESPACE/client` and then the callee from `NAMESPACE/caller`.
- **Streams and WebSockets**: a `/stream` with a chunk every second for 30 s, and a WebSocket
  whose server pings every 5 s held for 30 s, each keep `web` awake (no pause starts), and it
  sleeps by itself within 27 s after each ends. Then a WebSocket with no frames lets `web` sleep,
  and the next message on the same connection (the server numbers its connections) is answered
  within 5 s by a traffic wake from `NAMESPACE/client`.
- **Keepalive and quiet connections**: a client keeps an idle connection with Go's default
  TCP keepalive (probes after 15 s idle, every 15 s); `web` is hibernated, and 40 s later it did
  not wake, the connection has unanswered keepalive probes (`tcpi_probes` at least 1), and the
  next request on the same connection is answered by a traffic wake.
- **UDP and ICMP**: with `web` asleep, three UDP datagrams get no echo and wake nothing,
  `ping` gets three replies, and the node service logs no `ERROR` line; after a TCP wake the UDP
  echo works.
- **Wake during a pause**: a request 100 ms after `ctl get` first shows `web` pausing (from an
  API hibernate) connects and is answered within 1 s, and the pause ends as `pausing` to `awake`
  with `cancelled` and no resume, with the same VM process.
- **The recheck**: fault point `idle:decided` for `web`; as soon as the node service logs
  that it reached the point, a request is answered, and the pause ends as `pausing` to `awake`
  with `cancelled` or `error` before any sleep, with the same VM process. The node service is
  restarted afterwards to disarm the point.
- **CPU keeps a pod awake**: `/busy` keeps a vCPU of `web` busy for 45 s, longer than an idle
  sleep takes; no pause starts in that time, and the pod sleeps by itself within 87 s after (the
  60 s CPU window and 27 s). `cpu-off`, made as busy, sleeps by itself within 27 s all the same.
- **Outbound connections**: `follower` follows `/stream` of the client pod (`/follow`), which
  pushes a line every 3 s on the connection that `follower` opened. `follower` sleeps by itself
  anyway, and 20 s of pushes do not wake it; a request to it then wakes it once, from
  `NAMESPACE/client`. The test reports what became of the stream 7 s later (`/followed`): it went
  on, it stalled, or the app saw it end.
- **A failing app still fails**: `unhealthy` sleeps by itself while healthy; then
  `/health/fail` wakes it, kubelet restarts it, and no pause started in between. `unready` runs,
  never became Ready, never started a pause, and an event says why it does not sleep. `doomed`
  sleeps by itself; with `wake:resume-error` armed for its sandbox, a request starts a wake;
  there are at least three resumes and a `failed` event, the node service logged the point,
  kubelet recorded new `Unhealthy` events, and kubelet recreates the pod in a new sandbox whose
  container's last exit was 255 and whose app has a new boot id. The node service is restarted
  afterwards.
- **TCP API**: the node service starts with `--api-listen 127.0.0.1:17070` and a token file of
  mode 0600. No token and a wrong token get 401; `GET /v1/pods?ip=IP` and `GET /v1/pods/ip:IP`
  find `web`; a follower of `GET /v1/events?follow=true&cursor=SEQ` receives `web`'s next
  resume; `POST /v1/pods/ID/wake` with `"lease_ttl": 40` wakes the sleeping `web`, which stays
  awake for the 40 s, past its idle timeout, and sleeps by itself within 27 s after the lease
  ends. The node service then starts again without the test's arguments.
- **Admission**: `web` and `callee` awake, `caller` asleep, and every awake pod of the node
  held with a lease, so that none makes room by sleeping; the node service starts with
  `--max-awake-pods N --admission-wait 10s`, N the number of awake pods. A request to `caller`
  waits: 3 s later it has no answer and no VM process. `web`'s lease is released and `web`
  hibernated, and the request is answered by a traffic wake. Then a request to the sleeping
  `web` gets "connection refused" (or reset) within 11 s, an event says the wake was refused, and
  `web` is still asleep. The node service then starts again without the cap, and the leases are
  released.
- **Small scale**: 40 pods (`scale-01` to `scale-40`, 16 MiB, HTTP probes every 2 s), made 10
  at a time, each sleeping by itself; then 10 minutes in which all stay asleep, the node service
  uses under 10% of one core (user and system time from `/proc/PID/stat`), and its USS grows by
  under 200 KiB per pod from before the pods were made, with the same node service process; then
  one request to each of 10 of them at once is answered, with 10 resumes. The pods are deleted at
  the end.

First-request latency judges the records of the automatic cycles, and exec probes under Rosetta
those of the probe check; each fails unless that check recorded them. A check whose call to the
runtime fails, fails; nothing is skipped. A step names in its error the feature that it needs,
such as "(idle sleep)".

The checkers that need no node have unit tests (`cargo test` without `--ignored`): the parsing
of `testapp fetch` and the other clients, quantiles, the event classes, traffic sources,
refusals, fault point lines, node service arguments, the sleep watch, curl output, CPU shares and
log levels, and in `tests/wake.rs` the pod and Service manifests, the kubectl rows, the
`Unhealthy` counts and the timing limits.

## Runtime contract

The tests make every ctl call through `src/runtime.rs`, which also holds the fault points and
the node service's files, and reach idle sleep, wake on traffic, the node service's settings and
the TCP API through `src/wake.rs`. This is what they expect of the runtime.

**Processes.** Every process of the runtime runs `/usr/local/bin/containerd-shim-smolvm-pods-v2`
(`/proc/PID/exe`), and `/usr/local/bin/smolvm-pods` links to it. The tests tell them apart by
their arguments:

| Process | Arguments | How many |
| --- | --- | --- |
| Front shim | `containerd-shim-smolvm-pods-v2 -namespace k8s.io -id ID -address ADDRESS -socket SOCKET [-debug]`: argv[0] ends with the binary's name, `-id` is there, and the last argument is not `start` or `delete` | one while any task exists |
| Node service | `smolvm-pods node-service ...`: argv[1] is `node-service` | one; `floci-node run` starts it, and again 1 s after it exits |
| VM process | `EXE _boot-vm DIR/boot-config.json`, with smolvm's machine name, which contains the sandbox id, in `DIR/name` | one per awake pod |
| Short-lived | `ctl`, the shim's `start` and `delete` actions, and the `ip` and `tc` stand-ins | any |

USS is `Private_Clean` plus `Private_Dirty` from `/proc/PID/smaps_rollup`.

**Files.**

- `/var/log/smolvm-pods/node-service.log`: the node service's stdout and stderr, which `floci-node`
  appends, and a line from `floci-node` at each restart.
- `/var/lib/containerd-shim-smolvm-pods/.node/pods/SANDBOX.json`: the pod's record. The tests
  only check that it exists while the pod lives and is gone after a delete.
- `DIR/pause-*.smolcheckpoint`: smolvm's local checkpoints in the VM directory, which exist while
  the pod sleeps.
- `DIR/smolvm-pods-tso`: `PID STARTTIME` of the VM process that turned TSO on, written by the VM
  entry point on every boot and resume. STARTTIME is field 22 of `/proc/PID/stat`.
- `/run/smolvm-pods/fault` and `/run/smolvm-pods/node-service.args`: see fault points and node
  service settings.
- khugepaged's `scan_sleep_millisecs` and `alloc_sleep_millisecs` are 86400000.

**ctl.** `smolvm-pods ctl COMMAND ARGS`, on the API socket `/run/smolvm-pods/api.sock`.

- POD is the CRI sandbox id. The tests use no other lookup.
- stdout is one JSON document, except for `events`, which prints one per line. stderr is free.
- Exit status: 0 for success; 1 when the API answered with an error, with
  `{"error": {"code": CODE, "message": TEXT, "reasons": [TEXT, ...]}}` on stdout; 2 for a wrong
  command line; 3 when the API cannot be reached, or the connection closed before the answer, as
  when the node service dies during the call.
- Error codes the tests read: `not_found` (no such pod), `refused` (with `reasons`), `cancelled`
  (a hibernate whose pause a wake cancelled). Others, such as `failed` or `invalid`, are free.
- An answer can have more fields than the table below shows.

| Command | Returns |
| --- | --- |
| `ctl get POD` | the pod |
| `ctl list` | `{"pods": [POD, ...]}`: every pod with a record |
| `ctl wake POD [--reason REASON] [--source TEXT] [--lease-ttl SECONDS]` | when the pod is awake: `{"pod": POD, "resumed": BOOL, "duration_ms": N, "steps_ms": {...}, "clock": {...}}` |
| `ctl hibernate POD [--reason REASON]` | when the pod is asleep: `{"pod": POD, "duration_ms": N, "checkpoint_bytes": N}`; on a pod already asleep, at once |
| `ctl hold POD --lease ID --ttl SECONDS [--holder TEXT]` | `{"pod": POD, "lease": {...}}`; holding the same ID again renews it; it does not wake the pod |
| `ctl release POD --lease ID` | `{"pod": POD}`; an unknown ID is not an error |
| `ctl events [--cursor SEQ]` | every retained event with a `seq` above SEQ (default 0), in order, one per line, then exits |
| `ctl agent POD` | `{"pid": N, "threads": N, "fds": N}`: the guest agent's process, counted in the guest, of an awake pod; it does not wake the pod and fails on one that is not awake |

The tests call `wake` with `--reason api --source e2e`, `hibernate` with `--reason api`, and
`hold` with `--holder e2e`.

- `resumed` is true only for the call that ran the resume; a wake of an awake pod, or one merged
  into another's resume, returns false.
- `steps_ms` has one duration per wake step that ran, for example `admit`, `resume`, `tso`,
  `ping`, `clock`, `deferred_kills`, `release`; the tests print whatever names they find.
- `clock` is `{"skew_ns": N, "stepped": BOOL}`: the guest's realtime minus the node's before the
  step, and whether the agent stepped it.
- A hibernate is refused with code `refused` and the reasons that apply, of which the tests read
  `exec running` and `lease held`. The others: `container being created`, `wake waiter`,
  `hibernation disabled` (the `hibernate: "false"` annotation) and `state STATE` for a pod that
  is creating, waking, failed or deleting.

A POD object:

```json
{"sandbox_id": "5f3c...", "namespace": "ns", "name": "arm64", "uid": "...",
 "ip": "10.42.0.9", "netns": "/var/run/netns/cni-...", "machine": "...",
 "state": "awake", "generation": 41, "incarnation": "...", "rosetta": true,
 "vm_pid": 4242,
 "leases": [{"id": "test", "kind": "api", "holder": "e2e",
             "expires_at": "2026-10-19T12:00:30.000Z"}],
 "state_since": "2026-10-19T12:00:00.000Z", "last_wake_ms": 183.2, "last_pause_ms": 950.2}
```

The tests read `sandbox_id`, `state` (one of `creating`, `awake`, `pausing`, `asleep`, `waking`,
`failed`, `deleting`), `generation`, `vm_pid` (the VM process while one runs, else `null`),
`rosetta`, and the `id` and `kind` of each lease. A lease's kind is `api` for a hold, or `exec`,
`create` or `waiter` for an implicit one.

**Idle timeout.** The pod annotation `smolvm-pods.floci.io/idle-timeout` is a Go duration from
10 s to 24 h; the wake test sets `"10s"`. The tests allow a pause to start up to the idle timeout
plus 15 s (the node service's `--poll-interval` default) after the last activity, and up to 30 s
more for the pause itself. Activity is traffic on connections opened to the pod; traffic on the
pod's own connections is not. The VM pods of `tests/pods.rs`, `tests/network.rs`,
`tests/hibernation.rs` and `tests/scale.rs` set `"24h"`: their checks expect a pod to stay awake
until the test hibernates it (`smolvm_pods_e2e::NO_IDLE_SLEEP`).

**Events.** An event, as `ctl events` prints it:

```json
{"seq": 812, "time": "2026-10-19T12:00:00.300Z", "pod": "5f3c...", "from": "waking", "to": "awake",
 "reason": "api", "source": "e2e", "steps_ms": {"resume": 140.2},
 "clock": {"skew_ns": -61000000000, "stepped": true}}
```

- One event per state change, with `seq` rising, also across node service restarts.
- `reason` is the reason of the call that caused it, `recovery` for a change that the node
  service's recovery makes.
- A wake is the pair `asleep` to `waking` and `waking` to `awake`. The tests count resumes as
  events whose `to` is `waking`, and read `clock` on `waking` to `awake`.
- A pause cancelled by a wake or a request is `pausing` to `awake` with `"cancelled": true`; one
  that failed with the VM still running, or that the recheck refused, is `pausing` to `awake`
  with `error` set. The recheck check accepts either.
- An idle sleep: `awake` to `pausing` and `pausing` to `asleep`, both with the reason `idle`.
- A traffic wake: `asleep` to `waking` and `waking` to `awake` with the reason `traffic` and
  `"reason_detail": {"kind": "traffic", "from": FROM, "port": PORT}`, as the API prints any
  reason with details. FROM is the source pod's `NAMESPACE/NAME` when the node service knows the
  IP, the conntrack entry's original source for a source on the node (such as the Mac behind a
  NodePort), or else the IP. PORT is the pod's port that the first held frame was sent to.
- On the `waking` to `awake` event of a traffic wake: `steps_ms` with every wake step, and
  `"capture_to_release_ms": N`, the node service's time from capturing the first held frame to
  the release.
- A pod that is idle but does not sleep because of its probes or readiness: an event from
  `awake` to `awake` with the reason `idle` and the reason in `error`, for example
  `not sleeping: the pod is not Ready`, once per change of the reason, not at every idle
  decision.
- A wake that admission refuses: an event from `asleep` to `asleep` with the wake's reason and
  an `error` that starts with `wake refused`, for example `wake refused: awake pods 2/2`.
- A failed restore: `waking` to `asleep` with `error`, and after three in a row `waking` to
  `failed`. The tests count resumes (`to` is `waking`) and look for `to` `failed`.

**Traffic while asleep.** Any TCP connection to the pod IP wakes the pod, and the held SYN
reaches the app with the client's own address. TCP segments without payload (keepalive probes)
are dropped without a wake and unanswered; the next data segment on the same connection wakes
the pod and reaches the app. Data on a connection that the pod opened does not wake it; it is
held, and reaches the app at the next wake. UDP is dropped without a wake. ICMP echo is
answered. A refused wake answers the held SYN with a RST within the admission wait plus 1 s.

**Probes while not awake.** The node service answers TCP, HTTP, HTTPS and gRPC probes from the
node's addresses, with no NAT, on probe ports, without a wake; the tests' own probe-like
requests from the node's root namespace (HTTP with `User-Agent: kube-probe/1.35`, gRPC
`grpc.health.v1.Health/Check` over h2c, a TCP connect and close) must be answered like
kubelet's. Traffic through a NodePort to a probe port is traffic: it wakes the pod. An exec whose
argv equals an exec probe's, with no TTY and no stdin, gets the cached output and exit code
without a wake.

**Fault points.** `SMOLVM_PODS_FAULT=POINT` in the node service's environment arms POINT. It
takes a comma-separated list, and `POINT@SANDBOX` arms a point for that pod sandbox only. Each
time the node service reaches an armed point, it writes `fault point POINT reached` to stderr,
followed by ` for pod sandbox SANDBOX` when the point fires during a pod's transition, and then
acts:

| POINT | Where | What it does |
| --- | --- | --- |
| `pause:saving` | the record says `pausing`, the VM is frozen and smolvm is saving it; the checkpoint is not durable | kills the node service; after the restart the pod is `awake`, with the same VM process |
| `pause:durable` | the pause returned, its checkpoint durable and smolvm's record Paused, before the node service checks the VM process and writes `asleep` | kills the node service; after the restart the pod is `asleep`, with no VM process |
| `wake:resumed` | the resume returned and the new VM process runs, before the TSO check, the ping and the clock step | kills the node service; after the restart the pod is `awake`, with the steps done again |
| `wake:clock` | the agent answered SetClock, before the deferred kills and `awake` | kills the node service; after the restart the pod is `awake` |
| `idle:decided` | a pause for being idle, after the idle decision and after the record says `pausing`, before the `pausing` hook holds the pod's traffic | waits 5 s, once, then the pause goes on |
| `wake:resume-error` | each resume of the pod, before smolvm restores the checkpoint | fails the resume, every time until the node service restarts, and leaves the checkpoint intact, as a checkpoint that cannot be restored makes it: the pod goes back to `asleep`, and the third failure in a row fails it with exit 255 |

A killing point kills the node service with SIGKILL when it first reaches the point. A node
service that does not know a point logs `unknown fault point` and exits, and the tests fail on
that line.

The tests cannot set the environment of the node service that `floci-node` starts, so each time
`floci-node` starts the node service, if `/run/smolvm-pods/fault` exists, it reads it, removes
it, and starts the node service with `SMOLVM_PODS_FAULT` set to its first line. The tests write
the file, kill the node service with SIGKILL, wait until a new one answers `ctl list`, check that
the file is gone, and then make the call that reaches the point.

**Node service settings.** Each time floci-node starts the node service while
`/run/smolvm-pods/node-service.args` exists, it adds the file's words, separated by spaces or new
lines, to `smolvm-pods node-service`. Unlike the fault file, floci-node leaves it in place. The
tests write it, kill the node service, wait for the next one, and check its arguments in
`/proc/PID/cmdline`; they remove the file and restart the node service at the end of the check. The
tests use the flags `--max-awake-pods N`, `--admission-wait 10s`, `--api-listen ADDR:PORT` and
`--api-token-file PATH`, with Go durations, as the runtime's
[`src/node/settings.rs`](../../src/node/settings.rs) parses them. Before the file's words,
floci-node passes `--api-listen`, `--idle-timeout`, `--cpu-threshold`, `--cpu-window`,
`--max-awake-pods`, `--max-awake-memory` and `--admission-wait` with their settings in
`/etc/floci-node/env` (`0.0.0.0:7070`, `5m`, `0.5`, `60s`, and empty for the last three), each left
out when its setting is empty; a flag given twice takes its last value, so the file's
`--api-listen` replaces it.

**TCP API.** The listener serves the Unix socket's routes. A request without
`Authorization: Bearer TOKEN`, or with another token, gets 401. The tests read:

- `GET /v1/pods?ip=IP`: `{"pods": [POD]}` with the one pod of that IP;
- `GET /v1/pods/ip:IP`: the POD;
- `GET /v1/events?follow=true&cursor=SEQ`: the events after SEQ, one JSON object per line in
  the shape of `ctl events`, then each new one as it happens, over chunked HTTP; after 30 s
  without an event a blank line, which the tests skip;
- `POST /v1/pods/ID/wake` with `{"reason": "api", "source": "e2e", "lease_ttl": 40}`: status
  200 and `{"pod": POD, ...}` once the pod is awake; the lease keeps it awake until it ends.

**Other expectations.** `get`, `list` and `events` answer at once in every state, also while a
pause or a wake runs: the hibernation test's wake during a pause polls `get` every 5 ms during a
pause. A pod with no Rosetta annotation on an arm64 node whose TSO canary passed gets Rosetta
and TSO. `kubectl exec` into a sleeping pod wakes it with the reason `exec`. Hibernation keeps
the pod's tasks RUNNING for containerd, its sandbox ready, and its fifos open, so that
`kubectl logs` shows each output line once. A wake through the API also works on a pod with
probes and an idle timeout, and a hibernate through the API also works on a pod with no probes.

## Scale

`tests/scale.rs` checks that one node runs hundreds of pods whose requests add up to far more
than it has, through the capacity that kubelet reports
([Capacity](../../node/README.md#capacity)):

```sh
SMOLVM_PODS_E2E=1 \
cargo test --locked --manifest-path tools/smolvm-pods/tests/e2e/Cargo.toml --test scale -- --ignored --nocapture
```

On a node with 8 vCPUs and 16 GiB, it creates a Deployment of 500 runc pause pods (160m and
320Mi each) and 5 VM pods (`vm-1` to `vm-5`, 500m and 1Gi each, running `testapp hold -mib 64`),
which request about 10 times the node's online CPUs and memory. Once all are Running, it runs
every check and reports each one that fails:

- **Node settings**: kubelet reports the capacity of `/etc/floci-node/env`, 600 pods and a /22
  pod CIDR; the inotify and neighbour table limits are set; kubelet's running configuration
  (`configz`) has no memory eviction threshold and the CPU and memory managers `none` and
  `None`; and k3s's mount namespace has the generated `MemTotal`, CPU directories and the real
  online CPUs.
- **Cgroups**: the node service and every VM process are in `/smolvm-pods`; k3s, containerd and
  the front shim are not; the runc pods are in kubepods. Both cgroups have the `cpu.weight`
  that kubelet gives kubepods for the reported CPUs, and kubepods' cpuset is the online CPUs.
- **Requests and pods**: all 505 pods are Running with no restart, and request at least 10
  times the node's online CPUs and memory.
- **Memory**: the node has more than 1 GiB available with every pod running. It prints the
  node's memory use per runc pod.
- **Hibernate and wake under load**: three cycles of each VM pod through `smolvm-pods ctl`:
  hibernate, its VM process gone, wake, and its memory hash unchanged. It prints the timings.
- **CPU share under contention**: the VM pods burn their 2 vCPUs each and a runc pod 8
  threads (`testapp burn`) for 40 s. Over 20 s of it, the VM pods must get at least 35% of the
  CPU that they and the runc pods use together; equal weights give half.
- **k3s restart**: after SIGTERM to k3s, which `floci-node` starts again: every pod runs on with
  the same uid, sandbox and container and no restart, the node settings check passes again, each
  VM pod keeps its memory hash and hibernates and wakes, and 5 more runc pods and a VM pod `vm-6`
  start.
- **Warnings**: it prints kubelet's and k3s's warnings and errors from `k3s.log`, grouped, the
  warning events, the node service's warnings, and the node's memory at each step. It fails on
  an event that a full node would cause (`FailedScheduling`, `OutOfmemory`, `OutOfcpu`,
  `OutOfpods`, `UnexpectedAdmissionError`, `FailedNodeAllocatableEnforcement`, `Evicted`,
  `EvictionThresholdMet`) and on a log line about too many open files, a full neighbour table
  or a full disk.

## Workloads

`workloads/` holds the test app and the litmus test, in Go so that they cross-compile to static
amd64 and arm64 binaries offline; the pinned Rust image has only the arm64 target. The test
image is `FROM scratch` with both binaries, so the test app also does what the checks would
otherwise need tools in the image for: it sends a stream to another app's sink, resolves names,
describes a network interface and appends a line to a file (see `workloads/testapp/main.go`).

`testapp hold [-mib N] NAME ADDR` is the app of the VM pods in the hibernation, wake and scale
tests (`workloads/testapp/hold.go`). It fills N MiB (default 256) of anonymous memory with
pseudo-random words from a random seed, then serves `/hash` (the region's SHA-256), `/id` (its
pid, its start time in clock ticks after boot, and the boot id), `/clock` (`CLOCK_REALTIME`,
`CLOCK_MONOTONIC` and `CLOCK_BOOTTIME` in ns), `/ticks` (the last counter printed) and `/ping`,
and prints `tick N` to stdout every 100 ms. The arm64 test app also runs in the node for the
hibernation test: `get -times` adds the node's clocks before and after a request, `now` prints
them, `firstanswer` runs a command and polls a URL from just before it, with a new connection
every 5 ms so that a lost SYN costs nothing, and `ipv6probe` replays smolvm's IPv6 probe
(`host_has_ipv6_route`) and times it. `testapp burn THREADS SECONDS` keeps threads busy for the
scale test. The node image also has the arm64 litmus test, for the shim's TSO canary. The litmus
test: a writer stores `x` and then `y`, a reader loads `y` and then `x`, each access a single
plain load or store in assembly, and a read of `x < y` is a violation. A run counts only if the
reader saw `y` change between its reads at least 10,000 times, which shows that both threads ran
at once.

For the wake test, `hold` also takes `-https ADDR`, `-grpc ADDR`, `-udp ADDR` and `-unready`, and
serves (`workloads/testapp/wake.go`): `/health` (`ok`, or 500 after `/health/fail` until
`/health/ok`, or from the start with `-unready`), `/remote` (the client address it saw),
`/stream?seconds=N&every=S` (a chunk every S seconds for N seconds), `/ws?ping=S` (a WebSocket
echo that answers `conn=ID;n=N;text=TEXT` and pings every S seconds), `/call?url=URL` (a GET of
URL from this pod, its answer prefixed `callee.`), `/busy?seconds=N` (one CPU busy), and
`/follow?url=URL` and `/followed` (a stream that another pod pushes to this one, and what it
received). With `-https` it serves the same over TLS with a self-signed certificate, with
`-grpc` a gRPC health service over h2c built on the standard library (SERVING while `/health` is
ok), and with `-udp` a UDP echo. Its servers turn TCP keepalive off on accepted connections, so
that an idle connection sends no packets from the pod. The clients
(`workloads/testapp/clients.go`) print `key=value` lines and exit with 0 when they could run:
`fetch` makes N GETs of each URL at once on new connections without keepalive, with each one's
connect and first-byte times, status, body and error; `ws` holds a WebSocket and sends a second
message once a file exists; `pool` keeps a connection with Go's default TCP keepalive and reports
its unanswered keepalive probes (`tcpi_probes`) before a second request; `udp`, `ping` (a raw
ICMP socket), `grpchealth` and `dial` act as a UDP client, `ping`, kubelet's gRPC probe and its
TCP probe.
