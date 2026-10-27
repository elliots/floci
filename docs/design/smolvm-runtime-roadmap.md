# smolvm runtime roadmap

The smolvm runtime, `tools/smolvm-pods`, runs each Kubernetes pod in its own smolvm microVM, puts
idle pods to sleep and wakes them on incoming connections. `tools/smolvm-pods/README.md` and
`tools/smolvm-pods/docs/architecture.md` describe how it works, and
`tools/smolvm-pods/docs/getting-started.md` goes step by step from the builds to a running app.
This page lists the work that is left, the decisions, the open issues and risks, and the tests
that the whole system must pass.

## Goal

Run 500 or more applications on one machine by putting idle Kubernetes pods to sleep. Each pod,
with its init containers and sidecars, runs in its own microVM. A sleeping pod uses no RAM, while
Kubernetes still sees it Running and Ready with the same IP. Incoming traffic, or a message that
Floci delivers, wakes it with its memory intact. Apps and manifests need no change; a pod opts in
with the RuntimeClass `smolvm-pods`.

The environment can be cut off from the outside world, with no egress except to Floci, while
chosen third-party hostnames reach local simulators over HTTPS that apps trust. Every sleep and
wake is observable, with its reason. It must work on a Mac as well as possible, and also run in the
cloud as fast, cheap customer test environments, not production.

## Work left, in order

Each step ends with the [runtime's acceptance tests](#runtime) passing, each suite on a fresh node.

1. **A manual trial of a real application.** Let Floci's EKS service create the node, or start
   one by hand as `tools/smolvm-pods/docs/getting-started.md` describes, with Floci on the Mac and
   a runtime file (`docs/configuration/smolvm-runtime.md`) that names the application's message
   consumers in `workloads` and its simulators in `network.routes`. Start Floci with
   `FLOCI_HOSTNAME=localhost.floci.io` and `FLOCI_BASE_URL=http://localhost.floci.io:4566`, so
   that the URLs it returns, such as SQS `QueueUrl` and pre-signed URLs, work from pods. Deploy
   the application with the RuntimeClass `smolvm-pods`. Check that its HTTP services sleep when
   idle and wake on calls, including calls from one app to another through a Service, that its
   consumers wake for their messages, and that its simulators answer over trusted HTTPS with
   `network.isolation.enabled`. Each pod VM has 2 vCPUs and 1 GiB, each container has its own copy
   of its volumes, and containers with stdin or a TTY are refused (see "Known limits" in
   `tools/smolvm-pods/docs/architecture.md`). Fix what blocks the trial, and record what is
   missing.
2. **Pod shapes and volumes**, in the order the trial needs them. Run each
   [required workload](#required-workloads) through kubelet. Refresh ConfigMap, Secret and
   projected token volumes at every wake, in the `before_release` step, after the clock and before
   traffic, keeping their owners and modes ("Known limits" in
   `tools/smolvm-pods/docs/architecture.md`). Account for and clean up emptyDir storage.
3. **Standard libraries for protocol code.** Replace hand-written protocol code where a standard
   crate fits. The candidates are `h2` and `tonic-health` for the gRPC health answer and the
   pre-sleep gRPC check, `httparse` for the HTTP/1.1 that the probe responder and `ctl` parse,
   `kube-rs` for the Kubernetes client and the pod watch, and netlink crates for the raw netlink
   code. A research pass comes first: for each candidate, whether it fits where the code runs (the
   probe responder runs on smoltcp in one epoll thread, not on tokio), and what it adds to the
   binary and to `Cargo.lock`, which is smolvm's lock reduced to this crate's dependencies.
4. **Save only changed memory.** Keep the last checkpoint as a base and write only the 2 MiB
   blocks that changed since, found by hashing at pause or by userfaultfd write-protect set once
   before the vCPUs start. It is done when the bytes written per sleep and wake cycle after a short
   wake drop, with resume time and memory integrity unchanged.
5. **Scale, observability, Linux hosts and the final tests**, in this order:
   - **Scale with real applications.** Run 500 VM pods of real applications, including public
     Java and Node apps, several using about 1 GB, pinned by digest. Size each pod VM from its
     pod's resources, and admit wakes within real RAM by that size. Account for checkpoint disk
     and handle a full disk. Keep checkpoint I/O out of the page cache. Give k3s a CPU share of
     its own. Measure huge pages on a full node, interference between nested VMs, more resume
     slots, and resume from a cold SSD. Shrink checkpoints only if needed, and restore lazily in
     2 MiB blocks only if cold storage makes the eager read slow. Make the smolvm and libkrun
     patches upstreamable, with a setting for eager or lazy restore, an opt-in local pause with an
     export that builds a portable checkpoint on demand, and integration tests. Publish the smolvm
     patch series so that anyone can build the runtime.
   - **Events, metrics and an admin UI.** The event log, its live stream through the API, the
     reason of each event, an event for a wake that waits on admission, metrics on
     `GET /metrics` in the Prometheus text format, the traffic between pods from conntrack's
     accounting, rate-limited Kubernetes Events for sleeps and wakes, and an admin UI in the
     binary at `GET /ui` with a live graph of pods, edges from wake reasons and from traffic
     between pods, and each pod's timeline exist (`tools/smolvm-pods/docs/configuration.md`).
     Left: events and metrics over OTLP. No OTLP crate fits the binary's lock, which is smolvm's
     reduced to this crate: `opentelemetry-otlp` needs `opentelemetry`, `opentelemetry_sdk`,
     `opentelemetry-proto` and a newer `prost` than the 0.8 that `ttrpc` brings, none of which is
     there. Until then an OpenTelemetry Collector scrapes `GET /metrics` with its `prometheus`
     receiver, and a small client of the `follow` stream forwards the events as logs. Doing it in
     the binary means OTLP/HTTP with JSON bodies on the hyper client it already has.
   - **Linux hosts.** Floci's EKS service uses the host itself as the node. Run the runtime's
     suites on a Linux host with KVM, with amd64 pods running natively, TLS on the API, a systemd
     unit for the node service, and egress denied with nftables on the host and in the pod
     datapath.
   - **The [final tests](#final-tests)**, on a Mac and then on Linux.

## Decisions

- **One Linux implementation.** The node is a Linux machine with a fixed amount of RAM that awake
  pods share: on a Mac a smolvm VM started with `--nested`, with nested pod VMs; in the cloud the
  instance itself. Returning the node VM's memory to macOS is optional.
- **Our own Rust binary on smolvm as a pinned library**, never the smolvm CLI. smolvm and libkrun
  changes stay small and upstreamable.
- **No extra process per sleeping pod.** Shared node services do the work.
- **A front shim and a node service.** A small front shim for each containerd namespace is the
  only process containerd talks to; one node service per node runs the VMs and can restart unseen.
  A crash of the front shim recreates every pod, which is accepted.
- **Only TCP wakes a pod, and only traffic on connections opened to a pod, TCP or UDP, keeps it
  awake.** No protocol parsing and no HTTP gateway. The first SYN is held, never dropped or
  proxied, and the client's IP is kept. Traffic on connections that the pod opened neither wakes
  it nor keeps it awake, and waits for the next wake.
- **Floci owns AWS and broker triggers, from events.** It wakes consumers of SQS queues and of
  RabbitMQ queues on its Amazon MQ brokers through the node service's API with leases, and adds
  no HTTP proxies for waking. Floci runs SQS, so SQS reports every change to a watched queue: a
  message that can be received wakes its consumer at once, a delayed one when it becomes visible,
  and every send, receive, delete and visibility change restarts the linger. Floci reads a queue
  only once at startup, after a reset or when a workload is added, and at the end of a linger,
  so a consumer that handles a steady stream of messages stays awake. It learns of pods from the
  node's pod events. For RabbitMQ queues, Floci turns each broker's firehose on and follows it
  as its own broker user: every publish routed to a watched queue and every delivery from one is
  an event. The broker has no event for an acknowledgement, so the read at the end of a linger
  also keeps the leases while messages are unacknowledged, and reads again one linger later.
  SNS, EventBridge, IoT rules and S3 reach consumers through SQS, and MQTT through an IoT rule
  into SQS.
- **One runtime file describes the application.** An external tool writes it, and Floci reads it
  from `FLOCI_RUNTIME_CONFIG_FILE`, with `version: 1` and sections for the nodes Floci did not
  create, the network and the workloads (`docs/configuration/smolvm-runtime.md`). Floci reads it
  again when it changes. A workload names its SQS and RabbitMQ queues and its pods: a Floci EKS
  cluster or a declared node, a namespace, and a Deployment, StatefulSet or label selector, which
  Floci matches against the labels and controller in the node's pod records.
  `idle-timeout-seconds` is the linger: how long Floci keeps the pods' leases after the last
  event of the workload's queues, 120 s by default. The pod's idle timeout for incoming traffic
  stays its own annotation. `startup-timeout-seconds` is the limit of one wake call. The format
  may change freely, with no backwards compatibility. Finding consumers automatically can come
  later.
- **The CPU rule** keeps a busy pod awake, and is configurable per node and per pod, including
  off.
- **A pod sleeps only when every probe passes** right before the freeze. Probes are answered from
  those results while it sleeps, and failures stay visible.
- **SIGTERM does not wake a sleeping pod** unless the pod opts in.
- **The guest clock is stepped after every resume**, using E2B's thresholds: when the guest is
  more than 50 ms behind or 5 s ahead.
- **On Mac nodes, khugepaged sleeps a day between scans**, and nothing calls `MADV_COLLAPSE`.
- **Rosetta and TSO are on by default on arm64 nodes**, guarded by the TSO canary.
- **The node reports a configured, larger capacity to kubelet**, so that sleeping pods' requests
  do not fill it; real RAM is protected by the runtime's own wake admission once its caps are
  set. Memory eviction and the CPU and memory managers stay off, and the runtime has a cgroup of
  its own.
- **Restores read the checkpoint eagerly into anonymous huge-page memory.**
- **Saving only changed memory** tracks memory in 2 MiB blocks, and the tracking must keep stage-2
  huge mappings. KVM dirty logging on arm64 makes every new stage-2 mapping 4 KiB while it is on
  and never collapses them back. With it on, a cold first touch of 1 GiB in the nested node took
  7.1 s even with huge pages, 13 times as long as the same VM took with it off.
- **Checkpoint compression is not needed for now.**
- **Admission caps are off by default.** A refused wake waits up to 10 s, then held connections get
  a RST.
- **Traffic metrics between pods only if they come almost free**, from conntrack accounting, and
  never at the cost of the packet path.
- **Observability uses OpenTelemetry.**
- **The API is served over TCP with a bearer token**; TLS comes with Linux hosts.
- **Floci's Java changes** go in new classes and are off by default.

## Open issues and risks

- A node service killed with SIGKILL, or by the OOM killer, closes its packet sockets one after
  another as the process ends, each after an RCU grace period. At normal grace periods of 11 to
  50 ms that is up to 25 s for 500 sleeping pods, close to kubelet's 30 s probe budget;
  `floci-node` expedites them, and their length in the nested node is not measured yet. At a
  SIGTERM the node service closes them at once.
- A pod with probes that is put to sleep through the API, rather than by the idle rule, has no
  fresh probe results, so kubelet's probes can wake it.
- About one resume in 225 took 1.15 s inside the VM process (its first ping to the guest agent);
  the cause is unknown.
- A crash right after a resume can leave the used checkpoint on disk until the pod is deleted.
- k3s and containerd stay in the root cgroup, so long full CPU contention could starve the control
  plane.
- Checkpoint page cache is charged to the runtime's cgroup; wake admission within real RAM must
  account for it.
- Each k3s restart leaves an old mount namespace alive while old shims run.
- Every pause writes all guest RAM, uncompressed: about 1.3 GB for a pod using 1 GiB. That costs
  disk, and SSD wear of about 2 TB a day for 500 pods sleeping 10 times a day. Saving only changed
  memory addresses it.
- A crash of the front shim recreates every pod (new IPs, memory lost); it is kept small to make
  this unlikely.
- Hypervisor.framework crashed the nested node twice when huge pages were collapsed
  (`MADV_COLLAPSE`); khugepaged is slowed on Mac nodes to avoid it. Rosetta and TSO in nested VMs
  are not documented by Apple and could break with a macOS update, and Apple limits general
  Rosetta after macOS 27.
- Upgrading smolvm, libkrun or the node kernel may make saved checkpoints unloadable; sleeping pods
  would need waking and saving first.
- While a pod sleeps, the far ends of its outbound connections may close them (broker heartbeats,
  database idle timeouts, Kubernetes watches); the app reconnects after it wakes. Work driven by the
  pod's own timers pauses while it sleeps.
- The node VM's memory is not returned to macOS.
- Huge pages on a full, fragmented node, and slowdowns between nested VMs, are not measured yet.
- The smolvm and libkrun patches are carried locally and are not upstream yet. The smolvm patch
  series is the branch `floci/runtime`, whose head is the pinned commit. The patched guest agent
  is built from it, so building the runtime needs a checkout of that branch.
- An upstream note, not a problem for Floci: with the patched guest agent, smolvm's own
  `smolvm-shim` loses interactive pods. The shim asks for a TTY and sends input on the connection
  that starts a pod process; the agent refuses the TTY, and input there ends the stream.
  Interactive pods need the agent to take a TTY and input for a pod session, which it does not
  yet, and the shim to send them there.
- Every pod VM has 2 vCPUs and 1 GiB, whatever the pod requests.
- The `smolvm.io/gpu` annotations, which come from smolvm's shim, are untested, and whether a VM
  with a GPU can sleep is unknown.
- Whether kube-proxy clears stale UDP conntrack entries on the node kernel is not verified.
- The network suite's delete check does not look for packet sockets that hold a deleted pod's
  network namespace.
- A Linux bridge holds at most 1,024 ports, so `cni0` limits a node to about 1,000 pods.
- At most 2 pauses and 4 resumes run at once on a node, so 50 pods woken at once take about 2.5 s
  for the last.

## Acceptance tests

### Runtime

The runtime's unit tests, clippy and rustfmt (`scripts/build.sh` in `tools/smolvm-pods`), and its
end-to-end suites in `tools/smolvm-pods/tests/e2e`, each on a fresh node:

- **Pods:** arm64 and amd64 pods, one with a sidecar, run in their own VMs, with Rosetta and TSO
  checked by a litmus test; exec and logs work; refusals are clean; a delete leaves nothing.
- **Network:** ClusterIP, NodePort from the Mac, DNS, Floci on the Mac, external hosts, the
  pod's IP and MTU, 100 MiB streams both ways, and a clean delete.
- **Hibernation:** 20 sleep and wake cycles per pod with memory, pids, IP and restart counts
  unchanged; logs with no gap; the clock and TSO after every wake; exec and merged wakes; a wake
  that cancels a pause; crashes of the node service at every fault point that kills it, of
  containerd and of the front shim; delete while asleep.
- **Wake on traffic and idle sleep:** pods with every kind of probe sleep by themselves and stay
  Ready; traffic through a Service wakes them with the client's IP; first-request latency; ten
  concurrent requests cause one resume; streams and WebSockets keep a pod awake; keepalive probes
  to a sleeping pod, outbound connections and UDP do not wake it; a request during the recheck
  before a sleep cancels it; failing apps still fail, and so does a pod whose resume keeps
  failing; the API over TCP; admission; 40 sleeping pods for 10 minutes.
- **Scale:** 500 runc pods and 5 VM pods on an 8 vCPU, 16 GiB node, requesting 10 times the node,
  with sleep and wake under load, a fair CPU share for VM pods, and a k3s restart.
- **Messages:** with Floci on the Mac and the node in its runtime file, Node.js consumers on the
  AWS SDK for JavaScript v3 and on amqplib sleep while their queues are empty, and Floci wakes
  them for SQS messages, delayed messages, a dead-letter queue, SNS, MQTT through an IoT rule, a
  queue shared by three replicas and a RabbitMQ queue; the linger keeps a consumer awake through
  work after its acknowledgement and a burst; a long message keeps it awake; a message that a
  sleeping consumer's long poll claims, a refused wake, and restarts of Floci and the node service
  lose nothing; a workload added by reload wakes its consumer.

### Required workloads

| Requirement | Acceptance |
| --- | --- |
| App plus zero to three sidecars | Shared localhost before and after a sleep |
| Ordinary init containers | Run in order and do not run again after a sleep |
| Native sidecar | `restartPolicy: Always`, including independent restarts |
| Disk emptyDir | Data survives a sleep; a shared `/tmp` with a 512 MiB limit and storage accounting |
| Memory emptyDir | One guest tmpfs shared by the app and a sidecar, contents kept |
| ConfigMap and Secret volumes | Read-only live projections, including atomic updates while asleep |
| Projected service account token, IRSA | Correct identity and audience, token rotation, and SDK credential refresh after expiry and a sleep |

Pods never share mounts, and deleting a pod deletes its emptyDir contents. HostPath, CSI and
general PVCs, privileged and ephemeral containers, NetworkPolicy, RWX and block volumes are not
required. An emptyDir size limit is not a strict write quota.

### Final tests

End-to-end tests against a real Floci whose EKS service creates the node, on a Mac and then on
Linux:

1. **HTTP wake.** Twelve simultaneous requests to a sleeping app cause one restore. Bodies and
   headers arrive intact, and a value held in memory is unchanged.
2. **Idle sleep.** Without traffic, the pod sleeps after its idle timeout and its VM process exits.
   Health probes neither wake it nor keep it awake.
3. **Long requests.** A streaming request or a WebSocket keeps the pod awake past the idle timeout.
4. **SQS wake.** A message wakes the sleeping consumer, which receives, processes and deletes it,
   then sleeps again. A delayed message wakes it only when it becomes visible, and in-flight
   messages keep it awake.
5. **Other triggers.** SNS to SQS and an IoT rule to SQS wake consumers, and a call from another
   pod through a Service wakes a sleeping pod.
6. **Identity.** After a wake, an IRSA web identity call through Floci's STS works, also after the
   token rotates.
7. **Restarts.** Restart Floci and the node services while pods sleep. The pods still wake
   correctly, and no container is reported as exited.
8. **Density.** Deploy as many small apps as the test limits allow and let them sleep. Measure RAM
   and disk per sleeping pod, estimate the cost of 500 apps, and wake a random sample.
9. **No egress, simulators instead.** A pod cannot reach the internet by name, by IP, over UDP or
   DNS, or over IPv6, and the attempts are logged. A configured vendor hostname reaches its
   simulator over HTTPS with a trusted certificate and no app change, and a sleeping simulator pod
   wakes on that call.
10. **Observability.** The event feed shows every sleep and wake with its reason, including which
    pod's call woke it. The admin UI's graph shows which pods call which, and each pod's history.

### Limits for tests

Tests stay within 8 GiB of active guest RAM and 20 GiB of checkpoint disk, including temporary
checkpoint space and concurrent wakes. Scale tests may use a node VM of up to 24 GiB and up to
200 GiB of checkpoint disk. Tests create uniquely named resources and remove only their own, never
touch other Kubernetes clusters or containers on the machine, and keep credentials out of Git.
