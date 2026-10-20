# smolvm-pods architecture

How smolvm-pods runs each Kubernetes pod in its own smolvm microVM, puts idle pods to sleep and
wakes them, and why it works this way. Settings, annotations, the API and `smolvm-pods ctl` are in
[configuration.md](configuration.md). How the node image is built and laid out is in
[node/README.md](../node/README.md).

- [The front shim and the node service](#the-front-shim-and-the-node-service)
- [A pod's lifecycle](#a-pods-lifecycle)
- [Sleep and wake](#sleep-and-wake)
- [Waking on incoming connections](#waking-on-incoming-connections)
- [The idle rule](#the-idle-rule)
- [Health probes while a pod is not awake](#health-probes-while-a-pod-is-not-awake)
- [amd64 pods](#amd64-pods)
- [Networking](#networking)
- [The node](#the-node)
- [Known limits](#known-limits)

## The front shim and the node service

Each node runs two long-lived kinds of smolvm-pods process, both the same binary: a small **front
shim** for each containerd namespace, the only process that containerd talks to, and one **node
service**, which owns the VMs. They are split because of how containerd treats a shim:

- containerd opens one ttrpc connection per task, even when every task shares one shim process.
- When the process that serves a task dies, containerd runs the shim binary's `delete` action for
  each task and publishes TaskExit. One shim process with 1,500 tasks was killed in a test, and
  every task ended within 15 s.
- CRI learns a container's exit from a long-lived Wait, and a failed Wait counts as exit 255.
  kubelet then recreates the pod: a new sandbox, a new IP, and the memory of the old one lost.

So whatever containerd talks to must not die, and the VM work, which is large and changes often,
must be able to crash and restart without containerd noticing. One front shim serves every pod
of a containerd namespace, so a sleeping pod costs no process: the front shim measured 9.95 MiB
USS with 1,500 tasks, about 5.3 KiB a task, and grows about 23 KiB a pod.

The price is that a crash of the front shim recreates every pod on the node. It is kept small to
make that unlikely.

### The front shim

The front shim (`src/front`) is the daemon that containerd starts. One serves every task of a
containerd namespace: its `start` action gives every task the same address, and starts the
daemon under a lock when none answers there, so that tasks that start together start one daemon.

- It keeps the task table, publishes the task events, and forwards to the node service what needs
  a VM. It never calls smolvm.
- It runs on a current-thread tokio runtime with no blocking calls, with `oom_score_adj` -998,
  and logs to `/var/log/smolvm-pods/front-shim.log`.
- It exits 1 s after a Shutdown finds its table empty.
- It never forwards State, Connect, Pids or Wait. containerd's Pids at reload has no timeout, so a
  blocked Pids would stall containerd's start; CRI's recovery reads State with a 2 s limit.

What containerd sees:

| Call | Answered |
| --- | --- |
| State, Connect, Wait, Shutdown | From the table, never forwarded |
| Update | Accepted and ignored: the guest keeps the resources of its create |
| Pids | From the table: the task's pid and the guest's pids as last reported, never empty. containerd deletes a non-sandbox task with an empty Pids list at reload as leaked. A Pids also asks the node service for the guest's pids in the background, for 500 ms at most |
| Stats | Forwarded, for 500 ms at most; then the last sample, or Unavailable if there is none. CRI's ListContainerStats calls Stats for each task in turn with no timeout, so one slow answer would stall the whole listing. containerd leaves out a task whose Stats fails, while CRI fails its whole listing for a container without stats. A CPU total never goes back |
| Create, Start, Exec, Kill, Delete, CloseIO, ResizePty | Forwarded. While the node service is unreachable, they wait until shortly before their deadline, or 30 s without one, then answer Unavailable |

The table holds one descriptor, opened for reading and writing without blocking, on each stdout
and stderr fifo of a process until it exits:

- It is a writer, so CRI never reads the end of a fifo while the node service, whose pumps write
  the fifos, restarts. When the last writer of a fifo closes, CRI stops logging that container for
  good.
- It is a reader, so a pump never gets EPIPE while containerd restarts. Output then fills the pipe
  buffer, the pump blocks, and the guest agent's journal holds the rest.
- It closes at the exit, so that CRI's log of the container ends once the pump closes its end.

### The node service

The node service (`smolvm-pods node-service`, `src/node`) owns the VMs. It runs each pod's
lifecycle, hosts the engine backend for each pod, runs the output pumps, serves the API, watches
and reaps the VM processes, and runs the parts that put idle pods to sleep and wake them on
traffic.

`floci-node run` starts it before k3s, in the cgroup `smolvm-pods` that the VMs inherit, starts it
again 1 s after it exits, and appends its output to `/var/log/smolvm-pods/node-service.log`. k3s
neither waits for it nor stops it, so a restart of either leaves the other running (see
[the node image](../node/README.md#the-node-service-and-k3s)).

The VMs outlive the node service: the node service registers no boot binary, so smolvm arms no
parent-death watch on the VM processes. The node service reaps only the VM processes, with
smolvm's own reaper. containerd-shim's default reaper would take the exit status of every child,
and the waits of the shim's own short commands would then fail with "No child processes".

### The internal protocol

The front shim and the node service talk over one connection on `/run/smolvm-pods/node.sock`
(`src/proto.rs`), in frames of a 4-byte big-endian length and a JSON body.

- The front shim opens it with `Hello { version, namespace, tasks }`, its whole task table, and
  the node service answers `Welcome` or `Refused`. It accepts protocol version 1.
- Requests carry an id. Mutating requests are idempotent by sandbox, task, exec and operation:
  the node service joins a request to an identical one still running, and answers a repeated
  create or start with the first one's pid.
- Exits are notifications, which the node service sends to every open session of the namespace
  and repeats at every Hello until a front shim acknowledges them.
- A request on a connection of its own (`Call`) serves the `delete` action.
- The front shim reconnects with a backoff from 50 ms to 1 s, and sends again every request that
  has no answer.

### A restart of the node service

The node service takes its pods from its durable records (see [Durable
state](#durable-state-and-recovery)), not from the front shim's table. Recovery classifies each
pod against smolvm's view of its VM, adopts the VMs that still run with a pidfd watch, and fails
the pods whose VM is gone, each of whose processes then exits with 255.

A pump attaches to each running process at the frame of its cursor file,
`/run/smolvm-pods/cursors/SANDBOX/ID[@EXEC]`, which a pump writes after each frame it writes to a
fifo, once the pod is awake. A pump repeats at most the frame it was writing, and an exit during
the outage comes from the guest agent's journal.

The node service accepts the front shim only after recovery. At the Hello it repeats the exits
that the front shim lacks. A pod of the namespace that the table does not have is abandoned, and
deleted after 5 minutes. A task of a pod with no record exits with 255. The pids in the records
and in the table stay reserved.

### The delete action

containerd runs the shim's `delete` action whenever a task's connection closes, for containers
too. It deletes a VM only for a sandbox task, by asking the node service (`DropSandbox`). The node
service writes a tombstone, `/var/lib/containerd-shim-smolvm-pods/.node/tombstones/ID`, answers,
deletes the VM, and removes the tombstone. If the node service does not answer within 2.5 s, the
action writes the tombstone itself, and the node service deletes the sandbox of every tombstone it
finds when it starts. After a crash of the front shim, containerd runs the action for every task,
which deletes every VM.

### Panics

The binary is built with `panic = "unwind"`. Each request handler of the front shim and of the
node service catches a panic and fails only that request, with `INTERNAL`; the front shim's
connection to the node service starts again after a panic. The VM process (`_boot-vm`) aborts at
a panic, because it must not run on with a thread gone.

## A pod's lifecycle

The lifecycle (`src/lifecycle`) runs each pod's transitions one at a time, in a small actor per
pod, over the VM operations of `src/node/vm.rs`.

| State | Meaning | VM process |
| --- | --- | --- |
| `creating` | The sandbox VM boots and the guest is set up | starting |
| `awake` | The agent answers and the steps after a resume are done | running |
| `pausing` | A pause runs: `saving` until the checkpoint is durable, then `stopping` | running, then frozen |
| `asleep` | The checkpoint is durable and the VM process is verified gone | none |
| `waking` | A resume and the steps after it run | starting |
| `failed` | The VM is lost: it crashed, a restore failed three times, or the checkpoint is missing. Every task has been reported as exited with 255 | none |
| `deleting` | The sandbox is being deleted; the record goes at the end | stopping |

| From | Trigger | To |
| --- | --- | --- |
| `creating` | boot and guest setup succeed | `awake` |
| `creating` | an error | record removed; the create fails and kubelet retries |
| `awake` | a hibernate with nothing blocking it | `pausing` |
| `pausing` | the pause is durable, the VM process is gone (pid and start time) and smolvm's record says Paused | `asleep` |
| `pausing` | a wake before the pause is durable | `awake`: smolvm resumes the frozen VM in place, with no restore |
| `pausing` | a wake after the pause is durable | `waking`, as soon as the VM process stops |
| `pausing` | the pause fails and the VM still runs | `awake`, with the error in the event |
| `pausing` | the VM process dies before the pause is durable | `failed` |
| `asleep` | a wake: the API, an exec, a container create, a signal, or traffic | `waking` |
| `waking` | the resume and the steps after it succeed | `awake` |
| `waking` | the resume fails before the VM starts and the checkpoint is intact | `asleep`; the third failure in a row: `failed` |
| `waking` | the VM started and then died, its agent did not answer within 30 s, or TSO is missing | `failed` |
| `awake` | the VM process exits outside a pause, for example killed by the node's OOM killer | `failed` |
| any | a sandbox delete, or the sandbox is abandoned | `deleting`, without a wake |

A VM process's exit is a hibernation only after a durable pause. The node service watches each VM
process with a pidfd. Any other exit fails the pod, and every task reports exit 255, so that
kubelet recreates the pod. Containers and execs in a VM that is gone fail at once.

**What blocks a pause.** A hibernate is refused while a lease is held, an exec runs or is being
created, a container is being created, a wake has waiters (until 5 s after the wake completes),
or the pod has `smolvm-pods.floci.io/hibernate: "false"`. The refusal lists the reasons.

**Leases.** A lease keeps a pod awake for work that the node cannot see, such as a message that
Floci delivered. It lasts at most 60 s (30 s by default) and is renewed by holding it again.
Leases are durable, and expired ones are dropped when a pause is considered. Taking a lease on a
sleeping pod does not wake it, and only blocks a pause; a wake can take a lease in the same call.
Running execs, execs and containers being created, and wake waiters count as implicit leases that
never expire on a timer.

**Merged wakes.** A wake of an awake pod returns at once, a wake of a waking pod waits for the
same resume, and only the first wake of a sleeping pod starts one. A wake during a pause cancels
it if the checkpoint is not durable yet: smolvm stops writing it and resumes the frozen VM in
place. After the checkpoint is durable, the wake resumes the pod once the VM process stops.

**Incarnations.** Each record has a random incarnation. An API call may name it, and a call that
names an older incarnation of a recreated pod is refused.

### Durable state and recovery

The records are in `/var/lib/containerd-shim-smolvm-pods/.node/`, which the leading dot keeps out
of the node's boot sweep:

- `pods/SANDBOX.json`: one record per pod, with its identity, state, tasks and execs with their
  synthetic pids and stdio paths, leases, deferred kills, probes and timings, written atomically
  before and after every transition;
- `events.jsonl`: one line per state change, synced, and rotated to `events.jsonl.1` at 8 MiB;
  the sequence numbers keep counting across rotations and restarts;
- `node.lock`: an flock, so that one node service runs at a time.

smolvm's own record of each VM (Running, Pausing or Paused, the pid and start time, and the
checkpoint) is the second view. When the node service starts, it classifies each pod by the pair
(`src/lifecycle/recovery.rs`), then accepts the front shim:

| Record says | smolvm says | Action |
| --- | --- | --- |
| `creating` | anything | Stop and delete the machine and drop the record. The pending create fails, and kubelet retries |
| `awake` | Running, process alive | Adopt the VM with a pidfd and reattach the pumps |
| `awake` | Running, process gone | `failed` |
| `pausing` | Running, process alive | The freeze never started or was undone: run the steps after a resume, then `awake` |
| `pausing` | Pausing, process alive (frozen) | Resume the VM in place, then `awake` |
| `pausing` | Paused, process alive | Stop the paused VM process, then `asleep` |
| `pausing` | Paused, process gone | `asleep` if the checkpoint is there, else `failed` |
| `pausing` | Pausing or Running, process gone | `failed` |
| `asleep` | Paused, process gone, checkpoint present | `asleep` |
| `asleep` | checkpoint missing | `failed` |
| `waking` | Paused, process gone, checkpoint present | Run the wake again: an exec or create may still wait for it |
| `waking` | Running, process alive | Run the steps after a resume again, which are idempotent, then `awake` |
| `waking` | Running, process gone | `failed` |
| `failed` | anything | Stays `failed` |
| `deleting` | anything | Finish the delete |

Any other pair, which only a VM changed outside the node service can make, follows smolvm's view.
The same table settles a pause or a resume whose outcome is uncertain, such as one that returned
an error. Records of an earlier boot of the node are dropped with their machines, and so is every
smolvm pod machine that no record names. The node service does not call smolvm's
`reconcile_runtime_machines`.

What survives a crash:

| What crashes | What happens |
| --- | --- |
| The node service | Nothing that containerd sees. Pods keep their state; awake VMs keep running; mutating calls wait for the restart |
| containerd or k3s | Tasks reload as RUNNING with no exits, sleeping pods stay asleep, and logs continue after the next wake |
| The front shim | containerd ends every task, its `delete` action deletes every VM, and kubelet recreates every pod with new IPs and empty memory |
| A VM process | Its pod fails, and kubelet recreates it |
| The node VM | Every pod, asleep or awake, is lost, and kubelet recreates them |

## Sleep and wake

A pod sleeps to a local checkpoint on the node's disk: its VM's RAM, devices and vCPU state, after
which the VM process exits and the pod uses no memory. Its network namespace, IP, containers and
log fifos stay, and Kubernetes keeps seeing it Running and Ready.

### Pause

The pause steps are `slot`, `pausing`, `stop-pumps` and `pause`, then `stop-paused` when the VM
process outlives a durable pause:

1. Wait for one of the node's 2 pause slots.
2. The `pausing` hook holds the pod's traffic (see [Waking on incoming
   connections](#waking-on-incoming-connections)). For an idle pause it then checks conntrack
   again, and a packet since the idle decision refuses the pause.
3. The output pumps finish the frame they are reading, record their cursors and close their agent
   connections, so the checkpoint holds no host connection.
4. smolvm's `pause_machine_local_with` freezes the VM and writes the checkpoint into the VM's
   directory (`pause-*.smolcheckpoint`), unpacked, with a SHA-256 of the RAM image bound to its
   inode. A resume on the same node then needs no extraction or verification, which would cost 0.6
   to 0.7 s. The pause can be cancelled until the checkpoint is durable.
5. Once smolvm's record says Paused and the VM process is verified gone, the pod is asleep. A VM
   process that is still there after a durable pause is stopped first.

A pause took a median of 560 ms on an idle node, and 421 to 462 ms with 505 pods on the node.
Each pause writes all of the guest's RAM, uncompressed: 1.27 GB for a pod using 1 GiB, and 1.30 to
1.37 GB for a Java app (PetClinic) whose RAM image is 0.75 GB.

### Wake

The wake steps are `admit`, `slot`, `resume`, `tso`, `ping`, `clock`, `deferred_kills`,
`before_release` and `release`:

1. Admission (see [Admission](#admission)), then the wait for one of the node's 4 resume slots.
2. smolvm's `resume_machine` starts the VM process as `/proc/self/exe _boot-vm`, this binary's
   entry point, so TSO and the pod's network namespace are set as at a cold boot, with the same
   tap, MAC and IP.
3. For a Rosetta pod, the new VM process must have recorded that it turned TSO on, or the pod
   fails.
4. The guest agent is pinged with a 1 s timeout, retried for up to 30 s.
5. The guest clock is stepped (see [Guest clock](#guest-clock)).
6. Deferred kills are sent (`deferred_kills`).
7. `before_release` deletes in the guest the containers and execs that were deleted while the pod
   slept, then the `release` hook writes the traffic held while the pod slept into the VM, in
   arrival order. Then the pod is awake and its pumps attach again.

A wake that cancelled a pause resumes the frozen VM in place if the freeze had begun (the step
`resume`, with no restore), then runs the steps from step 3.

**Memory after a resume.** The node's libkrun is patched to read the checkpoint into new anonymous
memory, 2 MiB aligned and advised for huge pages, with parallel reads before any vCPU runs. With
the release's libkrun, a resumed VM's RAM is a shared mapping of the checkpoint file on the node's
ext4. That mapping gets almost no huge pages, the first write to a file huge page splits it, and
each writeback unmaps the written page from the VM again, so resumed memory stays slow for minutes
in a nested VM. The cost of the patch is that a resumed pod's RAM is resident at once. A second
patch lets libkrun take the RAM image read-only, and smolvm then skips its private copy and fsync
of it, which would cost 0.3 to 0.7 s per resume. [node/libkrun](../node/libkrun/README.md) has
both patches and their measurements.

**Timings.** Through the shim and pod networking, the first answer at the pod's IP came a median
of 160 to 169 ms after the wake call (134 to 203 ms), with 98% of the guest's RAM in huge pages;
with 505 pods on the node, wakes took a median of 147 to 156 ms. Through smolvm alone, in the
[resume check](../README.md#tests), the first answer came 0.15 s after the start of the resume for
a pod holding 1 GiB and 0.21 s for PetClinic, whose warm requests were then as fast as after a
cold boot (median 4.0 ms against 7.5 ms). About 0.15 s of a resume is spent inside the VM process.
The first read of 1 GiB after a resume still takes 385 ms, against 52 ms warm.

### Guest clock

A resumed guest's wall clock is behind by the time it slept: arm64 KVM freezes the guest's
counter, smolvm does not restamp the wall clock on a resume from a local pause, and libkrun's
vsock time sync is built only for macOS and Windows. A guest was 192 s behind after 3.2 minutes
asleep.

So every wake sends the guest agent `SetClock` with the node's time, before the deferred kills
and before any held traffic is released. The agent steps `CLOCK_REALTIME` only if the guest is
more than 50 ms behind or more than 5 s ahead, the rule E2B uses, and answers with the skew it
found. The wake's event records it as `clock`. After every wake the guest clock is within 100 ms
of the node's. Monotonic time advances only by the time the pod was awake.

### Output and logs

While a pod is awake, each process's output pump (`src/pump.rs`) is a thread that owns its
streaming agent connection. It waits for output in 50 ms steps, looks at its commands between
them, and writes to the fifos without blocking. The guest agent journals up to 8 MiB of each
process's output in guest memory, so the journal is part of the checkpoint.

- **Pause.** Before a VM pauses, the node service sends each pump of the pod `Pause` and waits for
  every acknowledgement. A pump finishes the frame it is reading (up to 1 s for a frame that
  stalls), writes it, records its cursor, closes its connection and acknowledges, normally within
  50 ms. A frame that a full fifo has not taken stays in the pump, which writes it while paused,
  so a pause never waits for containerd. A pump that has ended holds no connection either.
- **While paused,** a pump holds no connection, never counts the missing VM as a failure, reports
  no exit, and keeps containerd's fifos open. It keeps the last resize for the wake. Once it has
  written all its output, it parks: its thread ends, and the node service keeps only its state. A
  sleeping pod's pumps use no thread.
- **Resume.** After the wake, `Resume` starts a thread for a parked pump and makes the pump attach
  at its cursor with `PodAttach`, so no frame is lost or repeated. A process that exited just
  before the freeze reports its exit now, from the journal.
- **After a restart of the node service,** a pump of a sleeping pod starts paused, parks, and
  attaches at its cursor file at the first `Resume`. If the node service drops a paused pump, the
  pump ends without an exit.
- **A broken stream.** smolvm's client shuts a connection's read half when a frame stalls part way
  for as long as the read timeout, which a busy guest can cause. The pump then reattaches with
  `PodAttach` and goes on from the last frame received. If it cannot, it kills the process before
  it reports exit 255, so that the process does not run on unseen.
- **Gaps.** When a cursor is older than the journal, at an attach or on a stream that fell behind,
  the agent answers `CURSOR_EXPIRED` with the oldest cursor it holds. The pump then writes one line
  on stderr (on stdout for a terminal), on a line of its own, records the new cursor once the line
  is written, and goes on at the oldest frame. A gap never ends the process or reports an exit:

  ```text
  smolvm-pods: output lost: 2 frames (3 to 4) fell out of the guest's journal before they reached the log
  ```

A pump publishes an exit only when none is recorded yet, so a later real exit never replaces one
that the node service recorded for a sleeping pod. `kubectl logs` reads CRI's log files and works
while a pod sleeps.

### What containerd sees while a pod sleeps

The pod's tasks stay RUNNING, Wait stays blocked, and the front shim answers State, Connect and
Pids from its table, so restart counts do not change.

| Call | While the pod sleeps |
| --- | --- |
| Stats | Answered at once: memory 0, and the CPU total of the last sample |
| Exec, container Create and Start | Wake the pod first, with the reason `exec` or `container-create`; an exec blocks a pause until it exits. CRI's ExecSync timeout starts after Start, so a wake only delays it |
| Kill SIGKILL, and SIGTERM unless the pod has `smolvm-pods.floci.io/wake-on-terminate: "true"` | The exit, 137 or 143, is reported at once, and the next wake sends SIGKILL to the guest process before it releases anything |
| Kill, other signals | Wake the pod and deliver the signal |
| Delete of a container or exec | Removed at once; the guest deletes it at the next wake |
| Delete of the sandbox | Deletes the VM and its checkpoint without a wake |

SIGTERM and SIGKILL do not wake a sleeping pod by default because deleting a namespace would
otherwise wake all its pods at once and need all their RAM.

Every task has a synthetic pid, the sandbox's too, because the VM process changes at every wake. A
real pid would go stale at the first pause and could later name another process. Stats answer
memory 0 also while a pod pauses or wakes.

## Waking on incoming connections

While a pod pauses, sleeps or wakes, the node service holds every frame sent to it and answers for
it, and the first connection to the pod wakes it. Nothing is proxied: the held frames go to the
guest unchanged, so the app sees the client's own address. This needs no smolvm or kernel change.

### The catcher

A catcher (`src/sleepnet`) is the node service's state for one pod that is not awake. One netloop
thread serves every pod's catcher with epoll, so a sleeping pod costs no thread. The thread runs
in the node's network namespace, and work in a pod's namespace runs on a short thread that enters
it.

- **A packet socket on the pod's `eth0`.** A packet socket on a bridge port sees each received
  frame before the bridge, so it sees the frames that the hold stops. It is opened with protocol 0
  and then bound, which takes 0.03 ms; opening it bound to `ETH_P_ALL` takes 36 ms and closing it
  11 to 50 ms in the nested node, both waiting for RCU. It uses `PACKET_VNET_HDR`, because frames
  that a local veth delivers carry a partial checksum: replayed without their virtio-net header the
  guest drops them, and the client waits for its 1 s SYN retransmit. It also uses
  `PACKET_IGNORE_OUTGOING`, `SO_TIMESTAMPNS`, and a classic BPF filter that passes only ARP
  requests for the pod's IP and IPv4 frames to it.
- **The hold.** `eth0`'s port on the pod's bridge `podbr0` is set to `listening` over route netlink,
  as `ip link set dev eth0 type bridge_slave state 1` does. A port in that state forwards nothing in
  or out, and keeps the state when a new `tap0` joins the bridge at a resume. `disabled` and
  `blocking` would not do: with STP off they flip back to `forwarding` at once. A carrier flap of
  the port resets it to `forwarding`, so a route netlink socket joined to the namespace's link
  events makes the netloop check the hold, and set it again if it was lost.
- **ARP.** Nothing in the pod's namespace answers ARP for the pod while it sleeps, so the catcher
  answers requests for the pod's IP with the guest's MAC. It also holds a copy of each request for
  the guest: at the release the guest learns each sender's current MAC from it, before the
  sender's SYN. Without it, a client whose MAC changed while the pod slept, such as a new pod with
  a reused IP, would get the guest's SYN-ACKs at its old MAC for more than 5 s.

The catcher's calls, each idempotent, run one at a time for one pod and in parallel for different
pods:

- **`catch`** opens the catcher and sets the hold, in 0.3 to 0.6 ms. On an open catcher it only
  sets the phase (`pausing`, `asleep` or `waking`) and checks the hold. It fails if the namespace
  or `eth0` is gone, or `eth0` is not a port of `podbr0`.
- **`release`** lets the probe responder's connections finish (at most 100 ms; new SYNs are held
  for the guest meanwhile), sets the port to `forwarding`, reads the socket until it is empty,
  keeps the frames received up to the flip, writes every held frame into the new `tap0` in arrival
  order with its virtio-net header, and closes the sockets on a background thread. The time since
  the first held frame was captured is the wake event's `capture_to_release_ms`.
- **`refuse`** answers each held SYN and data segment with a RST, one per segment, and drops them
  and held UDP. Held client RSTs and ARP requests stay for the guest.
- **`close`** refuses the held connections the same way, sets the port to `forwarding` and closes
  the catcher. Without a catcher, or for a namespace that is gone, it only ends a hold that the
  kernel kept.

Stopping the netloop closes the sockets but leaves the holds, as a restart does. After a restart,
before any hook runs, each pod that is `pausing`, `asleep` or `waking` is caught again, and a hold
that a crash left on an awake or failed pod is ended.

### Frame rules

| Frame to the pod | Action | Wakes |
| --- | --- | --- |
| ARP request for the pod's IP | Answered with the guest's MAC; a copy is held for the guest | no |
| TCP SYN of a kubelet probe, or a segment of a responder connection | To the probe responder | no |
| Other TCP SYN | Held | yes |
| TCP segment with data or FIN, on a connection opened to the pod | Held | yes |
| TCP segment with data or FIN, on a connection the pod opened | Held for the next wake | no |
| TCP RST | Held, at most 64 | no |
| TCP with no data and no SYN, FIN or RST: an ACK, a keepalive probe, a window update | Dropped | no |
| UDP | Held while the pod wakes, else dropped | no |
| ICMP echo request | Answered | no |
| Anything else, IPv6 included | Not passed by the filter | no |

The queue holds at most 1,024 frames or 4 MiB, drops exact repeats, and past the limit drops the
newest frame, which the client retransmits. Frames received before the hold was set passed the
bridge and are not held. Segmented frames never go to the responder.

Keepalive probes to a sleeping pod are dropped, so a client eventually resets an idle pooled
connection; the RST is held for the guest, and the pool's next connection wakes the pod.

**Traffic wakes.** For a frame that may wake the pod, the netloop asks the traffic waker, never
while the pod wakes. The waker looks the frame's tuple up in conntrack by its reply tuple: an
entry whose original source is the pod is a connection the pod opened, and its frame does not wake
the pod. Otherwise the waker starts a wake and returns at once, one at a time for each pod; frames
that come meanwhile are held, and are released or refused with the first. After a wake that the
waker started, traffic asks again at most once a second, so that a declined frame never delays a
wake. The wake's reason is `traffic`, whose `from` is the source pod's `NAMESPACE/NAME`; for a
source on the node, the conntrack entry's original source, such as the Mac behind a NodePort;
otherwise the IP. Its `port` is the pod's port. The name comes from the record of a live sandbox
at that IP, then from the pod watch, which knows runc pods too, then from the record of a failed
or deleting sandbox: a failed sandbox, like a finished pod, keeps its IP after the network gives
the IP to another pod. Of several pods at one IP, the watch names one that has not finished, then
the newest.

**Measured** on a node with 4 vCPUs: 35 client connections to a sleeping HTTP pod, 1 or 10 at a
time, all connected with no SYN retransmit, 96 to 144 ms after the connect (median 115 ms), and
the app saw each client's address. The wake took 78 to 143 ms of that and the release 0.13 to
0.27 ms; the first SYN asked for the wake 0.1 to 0.4 ms after the connect. Each sleeping pod's
catcher costs 2 descriptors, 9 to 12 KiB of kernel memory (about 3 KiB for the netlink socket, the
rest for the packet socket, its filter and epoll), about 0.5 KiB of the node service's memory, and
no thread. The node service's whole cost for each sleeping pod is in
[Measured on a node](#measured-on-a-node).

## The idle rule

A pod sleeps by itself when, for its idle timeout (5 minutes by default):

- no TCP or UDP packet passed, in either direction, on a connection opened to it, other than a
  kubelet probe;
- it was awake the whole time (a wake starts the clock again);
- its VM used no more than the CPU threshold (see [The CPU rule](#the-cpu-rule));

and, when the idle loop gets to it, no lease, exec or container create blocks the pause, it does
not have `hibernate: "false"`, and it passes the [pre-sleep probe check](#the-pre-sleep-check).

Quiet open connections do not keep a pod awake, because any later packet on a connection to the
pod wakes it. A wrong sleep costs a wake, about 0.2 s, and a pause; it never loses a request.

### Only connections opened to the pod count

A conntrack entry counts for the pod whose IP is its reply source, unless that is also its
original source. That covers connections to the pod's IP, and those through a Service or a
NodePort after DNAT. Traffic on the pod's own connections, such as its DNS lookups, a database
pool, a broker's pushes or a server's heartbeats, neither keeps it awake nor wakes it; while it
sleeps, data on those connections is held for the next wake.

The reason: a pod is idle when nobody calls it, whatever it keeps open toward others. If outbound
traffic counted, a heartbeat, a watch or a pool with keepalives toward another service would keep a
pod awake for good. The costs:

- The far ends of a sleeping pod's outbound connections see silence and may close them (broker
  heartbeats, database idle timeouts, Kubernetes watches). The app reconnects after it wakes.
- Messages pushed to a sleeping consumer wait until something calls it. Floci wakes consumers of
  AWS and broker messages through the API, with a lease.
- Work driven by the pod's own timers, such as a poll loop or a retry with a sleep, pauses while
  it sleeps.

### Reading conntrack

Every `--poll-interval` (15 s) the activity monitor (`src/activity`) dumps the node's conntrack
table once, over ctnetlink, on a raw `NETLINK_NETFILTER` socket (IPv4 TCP and UDP entries), and
reads each awake pod's VM CPU. Pod-to-pod traffic on `cni0` passes conntrack because
`bridge-nf-call-iptables` is 1 on the node.

- **The last packet.** An entry's age is the shortest timeout that its state may run on, from the
  node's `nf_conntrack_*timeout*` sysctls, less `CTA_TIMEOUT`, less a second of rounding. Between
  dumps, an entry moved if its packet counters changed or, without counters, if its TCP state
  changed or its timeout did not count down. Each choice errs toward awake. A pod's last activity
  grows only with a packet.
- **Connections that ended between dumps** are still seen: a closed connection stays in
  `TIME_WAIT` for 120 s. kube-proxy sets the established timeout to 86,400 s.
- **Counters.** `floci-node` sets `net.netfilter.nf_conntrack_acct=1` before k3s starts, and only
  connections that start after it count bytes. Without it, activity still works from the
  timeouts. The monitor also keeps the packets and bytes that each pod sent and received, and
  those of each pair of client and server addresses (at most 4,096 pairs), with connection counts;
  the API does not serve them.
- **Cost** (kernel 6.18.55, accounting on): a dump of 2,287 entries took 1.3 ms, of 7,287 about
  3 ms, and of 29,287 11 to 14 ms; a lookup by reply tuple 16 to 25 µs. Lookups use a socket of
  their own, so they never wait for a dump.

Kubelet probes do not count: TCP from one of the node's addresses to one of the pod's probe ports,
with no NAT. That also leaves out the node service's own pre-sleep checks. NodePort traffic also
reaches a pod from `cni0`'s address, but NATed, so it counts. A process on the node that connects
straight to a probe port is taken for kubelet.

### The CPU rule

A pod whose VM used more than the CPU threshold does not sleep, even with no traffic. The default
is 0.5 vCPU over a 60 s window, settable for the node and for each pod, and `off` lets busy pods
sleep. An exec probe under Rosetta takes about 2.4 s, about 24% of a vCPU at a 10 s period, while
real computation usually fills a core.

VM CPU is `utime` plus `stime` in `/proc/PID/stat` for every thread of the VM process. libkrun
names a thread `libkrun VM`, with a space, so the fields are counted from the last `)`. The
average runs from the newest reading at least a window old, or from the process's start if it
started within the window, as after every wake. The larger of that and the average since the
newest reading at least 5 s old counts, so that a computation that has just begun keeps the pod
awake before it fills the window. After a restart of the node service, a VM process older than the
window needs a second reading, so its pod waits one poll.

### The idle loop

The idle loop starts once recovery has settled. It polls the monitor every `--poll-interval`, and
at a busy pod's due time when that comes sooner, at least 1 s apart. For each idle candidate, one
at a time for each pod: a pod with `hibernate: "false"` does not sleep, and one with leases, execs
or creates waits for the next poll. Otherwise the pre-sleep check runs, then a hibernate with the
reason `idle`.

When the check fails, or a lease or the annotation refuses the pause, an event from `awake` to
`awake` with the reason `idle` says why, as the error `not sleeping: REASON`, once per change of
the reason (its numbers left out) until the pod sleeps. The monitor itself keeps a pod awake for
recent traffic, a recent wake, CPU above the threshold, CPU not measured yet, conntrack that could
not be read, or no recorded IP.

**Hold, recheck, freeze.** The `pausing` hook sets the hold before the freeze, then reads
conntrack again, and any packet newer than the idle decision refuses the pause. Without the
recheck, a request could reach the guest between the decision and the freeze; the guest would
acknowledge it and freeze before it answered, and the client would wait with nothing to
retransmit. With the hold set first, every later frame reaches the catcher and wakes the pod, and
every earlier one is in conntrack.

### Admission

The caps on awake pods and on their guest RAM are off by default; `FLOCI_NODE_MAX_AWAKE_PODS` and
`FLOCI_NODE_MAX_AWAKE_MEMORY` in `/etc/floci-node/env` set them
([node/README.md](../node/README.md#wake-admission)). A pod holds room from its create, or from
the admission of its wake, until it is asleep, failed or deleted: every pod that is creating,
awake, pausing or waking, with 1024 MiB of guest RAM, the size of every pod VM. A wake that does
not fit waits, first come first served, for at most the admission wait (10 s), and is then
refused. Its event goes from `asleep` to `asleep` with the error `wake refused: awake pods 2/2`
(or `memory 3/2 GiB`), the held connections get a RST so that clients see a refusal rather than a
hang, and waiting execs and creates fail with the reason. Probes keep their recorded answers:
restarting the container would only ask for another wake.

## Health probes while a pod is not awake

kubelet keeps probing a sleeping pod. A frozen app cannot change its answer, so the node service
answers each probe with the result the app gave just before the freeze, and a pod sleeps only when
every probe has just passed. Failures stay visible: a pod that is starting, failing or not ready
never sleeps, so kubelet keeps seeing the real app.

### Probe definitions

CRI carries no probes, so the node service watches the pods bound to its node on the API server
(`src/kube`), with the kubelet's kubeconfig, `/var/lib/rancher/k3s/agent/kubelet.kubeconfig`. The
node's name is the client certificate's common name, `system:node:NAME`, or the host name without
one. The watch lists the pods with `fieldSelector=spec.nodeName=NAME`, which the Node authorizer
requires, then watches from the list's resourceVersion with bookmarks, again from the last version
when a watch ends after 5 minutes, and lists again when the version expires. After an error it
waits 1 s, doubling up to 30 s, and reads the kubeconfig again, because the node service starts
before k3s writes it and k3s renews its certificates. It is a small client on hyper and rustls
that reads about ten fields of a pod.

The probe book (`src/probe/spec.rs`) keeps, for each app container and sidecar (an init container
with `restartPolicy: Always`), the startup, liveness and readiness probes as kubelet runs them:

- ports: a number, a name of the container's own ports, or a number in a string;
- exec commands with `$(VAR)` expanded from the container's `env` values, as kubelet does: the
  last of a name wins, a variable from `valueFrom` is empty, `$$` is `$`, and an unknown reference
  stays;
- the HTTP scheme, the probe's own headers, and the request target as kubelet sends it (no
  fragment, the query kept, the path escaped as Go escapes it);
- the gRPC port and service, and the period and timeout (default 10 s and 1 s).

A pod maps to its sandboxes by uid, or by namespace and name when the sandbox has no uid or the
pod is a static pod's mirror. The book writes each sandbox's probes, their last results, and
whether the pod is ready into its record, so a restart while the API server is down still has
them. The responder reads them from memory, then from the record, and never waits on the API
server.

A pod is ready only when it runs, is `Ready`, is not terminating, and every probed container is
started and ready. A probe that kubelet cannot run either (a port name the container does not
have, a scheme other than HTTP or HTTPS, an invalid port), or that probes another host, leaves the
pod with no probes, and the pre-sleep check says why.

### The pre-sleep check

Before an idle pause, the node service runs every TCP, HTTP(S) and gRPC liveness and readiness
probe against the pod's IP at once, each within its own timeout, from the node's root namespace,
and records the results for the responder. Startup probes are skipped: kubelet stops them once a
container started. A sleep is refused unless every probe passes:

| Probe | Passes when |
| --- | --- |
| TCP | It connects |
| HTTP | `GET` with `User-Agent: kube-probe/1.35` and `Accept: */*` unless the probe sets its own, the probe's headers, and `Connection: close`, answers 200 to 399. Redirects to the same host are followed, at most 10 requests, with each `Location` resolved as Go's client resolves it; one to another host passes, as in kubelet. The probe's `Host` is sent until a redirect to a URL with a scheme |
| HTTPS | The same over TLS with any certificate, as kubelet does, offering only `http/1.1` |
| gRPC | `grpc.health.v1.Health/Check` over h2c answers status OK and `SERVING` |
| Exec | Its last real result, which a real probe exec records with at most 10 KiB of output, passed within twice its period, or twice its timeout if that is longer |

The check also refuses a pod whose probes are not recorded yet, or that is not ready, with the
reason.

### The probe responder

The catcher gives a pod's frames to its probe responder (`src/probe/responder.rs`), a smoltcp
interface with the pod's IP and the guest's MAC, made on the pod's first probe SYN or echo request.
Its answers go out of `eth0` with full checksums.

A SYN is a kubelet probe when its port is a TCP, HTTP or gRPC probe port, its source is one of the
node's addresses, and conntrack's entry for its reply tuple shows no NAT. The NAT check matters:
NodePort traffic from the Mac also arrives from `cni0`'s address, `10.42.0.1`, and only its
conntrack entry shows the NAT. If the lookup fails, the SYN is traffic and wakes the pod. A TCP or
gRPC probe with no recorded result is not taken, so it wakes the pod too.

| Probe | Answer |
| --- | --- |
| TCP | Accepted |
| HTTP, HTTPS | The recorded status with `Content-Length: 0` and `Connection: close`, once the request's head is in, or after 4 KiB or 1 s. HTTPS uses a self-signed certificate made once for the node, and offers only `http/1.1` |
| gRPC | `SETTINGS` as soon as the connection is open (grpc-go waits for it), then the `SETTINGS` acknowledgement, an acknowledgement for each `PING`, and `SERVING`: 87 bytes with the `SETTINGS` |
| An HTTP request whose `User-Agent` is neither `kube-probe/...` nor the probe's own (from its `httpHeaders`), or to a path with no recorded result | 503 with `Retry-After: 1`, and a wake |
| Any probe after a failed restore, and a failed result with no status | A RST |
| ICMP echo request | Answered |

Responder connections end within 5 s, and the catcher stops offering SYNs while it waits for them
before a release.

### Exec probes

An exec whose argv is exactly the command of an exec probe of its container (by the container's
name in its bundle's CRI annotation), with no terminal and no stdin, is a probe exec.

- While the pod is awake, it runs in the guest. Its pump keeps the first 10 KiB of its output, and
  its exit records the result.
- While the pod pauses, sleeps or wakes, it gets the last result without a wake: its start writes
  the output to its stdout fifo, and its exit follows at once, with a pid of its own and no process
  in the guest. After a failed restore it gets exit 1 and `smolvm-pods: the pod could not wake:
  REASON`.
- Without a result, it wakes the pod, as any exec does.

A user's `kubectl exec` with exactly a probe's argv, no terminal and no stdin, also gets the cached
answer while the pod sleeps.

### Failures stay visible

| Situation | What kubelet sees |
| --- | --- |
| A container that never becomes healthy | The real app: the pod never sleeps |
| An app that starts failing while awake | The real app: the pre-sleep check fails, so it does not sleep |
| A restore that fails | Every probe fails, so readiness drops and liveness restarts the container. The restart is a container create, which wakes the pod. The third failure in a row fails the pod with exit 255 |
| A wake that admission refuses | The recorded answers |
| The node service down | Probes time out; kubelet tolerates 30 s by default (period 10 s, failure threshold 3) |

## amd64 pods

On an Apple silicon node, every pod's VM gets Rosetta by default, so that it can run linux/amd64
images, and pods need no annotation. The VM gets the node's `/mnt/rosetta` as a read-only share at
`/mnt/rosetta`, the guest registers Rosetta with binfmt_misc, and each container mounts the share.
smolvm's own Rosetta option is not used, because smolvm refuses to pause a VM that has it.

Rosetta needs the vCPUs to run with Apple's TSO memory ordering. The VM process turns it on with
`prctl(PR_SET_MEM_MODEL)` before any vCPU thread starts, on every boot and every resume, and writes
`PID STARTTIME` to `smolvm-pods-tso` in the VM's directory. It must do this itself:
`PR_SET_MEM_MODEL` does not survive `execve` on the node kernel. If it fails, the VM does not
start, and the shim refuses a Rosetta VM whose process did not record it, such as one that the
smolvm CLI started.

The annotation `smolvm-pods.floci.io/rosetta` changes the default (`src/rosetta.rs`):

| Annotation | The node has `/mnt/rosetta` and its TSO canary passed | Otherwise |
| --- | --- | --- |
| none | Rosetta and TSO | No Rosetta, with a warning in the node service's log |
| `"true"` | Rosetta and TSO | The pod is refused, with the reason |
| `"false"` | No Rosetta | No Rosetta |

The warning is `sandbox ID: the pod runs without Rosetta, because REASON`. x86_64 nodes run amd64
natively and ignore the annotation. A container whose entrypoint is an x86-64 binary, in a VM
without Rosetta, is refused when it is created, with the reason the VM has none; without the check
it would fail in the guest with an exec format error.

**The TSO canary.** The kernel confirms TSO for the VM process either way, but whether the guest's
vCPUs run with it in a nested VM depends on undocumented macOS behaviour, and a failure would be
silent: amd64 programs would see memory-ordering violations. So the node runs a canary at boot
(`smolvm-pods _tso-canary`, `src/canary.rs`). It runs the litmus test of the end-to-end tests
(`/opt/smolvm-pods/litmus/litmus` in the node) in a small VM with the Rosetta share, where it must
find no violations, and in one without it, where it must find some, which shows that the test can
see them. A run whose threads overlapped too little (fewer than 10,000 racing reads) is repeated,
up to three times. With TSO the test finds no violations, and without it tens of millions. The
verdict is in `/var/lib/containerd-shim-smolvm-pods/.tso-canary` and holds until the node
restarts. If there is no verdict yet, the first sandbox that could get Rosetta runs the canary,
and the others wait for it.

amd64 execs under Rosetta take about 2.4 s each, so an exec probe with a short timeout can fail in
a pod's first minute.

## Networking

A pod VM's network card is on the pod's CNI network, with the pod's IP and MAC. containerd runs
CNI in the pod's network namespace first, which leaves `eth0` there. smolvm's boot reads `eth0`
with `ip`, opens `tap0` in the namespace, copies frames between `tap0` and the VM's network card,
and joins `eth0` and `tap0` with tc `matchall` and `mirred` redirects. The node kernel has no tc,
so a bridge in the pod's namespace, `podbr0`, joins `eth0` and `tap0` (`src/net.rs`):

- smolvm runs `tc` and `ip` through `nsenter` from `PATH`. A pod VM's process (`_boot-vm`) puts a
  directory first on its `PATH` in which both are this binary. The `tc` stand-in accepts only
  smolvm's redirect calls, and makes the bridge on the second one.
- The bridge is a two-port wire: no port learns addresses. `cni0`'s ports are in hairpin mode, so
  the guest's own broadcasts come back in through `eth0`, and a learning bridge would then place
  the guest behind `eth0` and drop the replies to it.
- The guest takes `eth0`'s MAC, which the node and other pods already have for the pod's IP. A
  bridge port cannot share the MAC of a host behind it, so `eth0` gets the guest's MAC with the
  locally administered bit flipped. When smolvm reads `eth0` on a later boot, the `ip` stand-in
  flips it back, so the guest always gets the same MAC.
- `eth0` keeps the pod's IP, which CNI needs to delete the pod and smolvm needs on a later boot.
  The namespace answers no ARP for it (`arp_ignore` 8), sends no gratuitous ARP, and has IPv6 off,
  so only the guest answers. Otherwise the node would send the pod's traffic to the bridge.
- The VM process stays in the pod's namespace, as Kata's does. smolvm then sees no IPv6 route, so
  the guest gets no IPv6 address from smolvm's NAT, and smolvm's IPv6 probe fails at once instead
  of trying the internet (under 1 ms).
- smolvm leaves the guest's MTU at 1500, while the pod network's is lower (1450 with flannel's
  VXLAN). The bridge would drop the guest's full-size frames, and the node would drop TCP segments
  that peers off the pod network size for the guest's MTU. So after the first boot the shim sets
  the pod network's MTU in the guest through the agent, and checks that the guest has the pod's
  address. Both are guest state and survive a sleep.
- smolvm gives the namespace only to a VM's first boot. The VM process records it in the VM's data
  directory and puts it back into the boot config of later boots, such as a resume.
- `podbr0` has no forward delay. With one, the bridge would move `eth0` out of the `listening`
  state that holds a sleeping pod's traffic.

The node service opens only packet sockets in a pod's namespace: an IPv4 socket there would make
the namespace send ARP with `eth0`'s flipped MAC, and peers would then send the pod's traffic to
`podbr0`.

**Deleting a pod.** containerd deletes the sandbox task before it deletes `eth0` with CNI and
removes the namespace. The shim's delete stops and deletes the VM, removes the pod share, and
removes `podbr0`, and `tap0` if the VM process left it, from the namespace (`net::detach`), so that
nothing of the VM stays there if the namespace outlives the pod. smolvm's library delete removes
the VM's record and directory, but not the files that its CLI removes after a delete: the fork lock
`.NAME.fork-operation.lock` beside the VM's directory, which every stop and delete takes, and the
ready marker in the agent rootfs after an unclean stop. So the shim runs smolvm's sweeps of those
after each delete (`src/cleanup.rs`), and its own of the pause lock
`.NAME.fork-operation.pause-operation.lock`, which every pause and resume takes. The sweeps cover
every VM that is gone. The delete action that containerd runs for a sandbox whose shim is gone does
the same.

A pod without a network namespace of its own (`hostNetwork`) is refused. A task that is not a pod
(`ctr run`) gets a VM without networking.

**Egress on a Mac.** Everything the node sends goes through smolvm's gateway, whose egress policy
(`--allow-cidr` and the other `smolvm machine create` flags) nothing in the node can bypass. The
gateway answers every UDP datagram to port 53 itself, whatever its destination, by asking its
upstream resolver, so CIDR rules leave DNS open; only `--allow-host` or `--allow-host-pattern`
filter names. TCP to the gateway's address reaches the Mac's loopback, but UDP to it on any other
port is lost. The node kernel has nf_tables with `reject`, `ct` and counters, but no `log`, NFLOG
or NFQUEUE.

## The node

The Kubernetes node is a Linux machine with a fixed amount of RAM that awake pods share. On a Mac
it is a smolvm VM started with `--nested`, and pod VMs are nested KVM VMs inside it; on Linux it
is the host itself, with KVM. [node/README.md](../node/README.md) describes the image, its inputs
and its layout.

### The node kernel

The node VM boots libkrunfw's Linux 6.18.55 with two options added (see
[node/kernel](../node/kernel/README.md)):

- `CONFIG_TRANSPARENT_HUGEPAGE=y` with the policy `always`. In a pod VM nested in the node VM on a
  Mac, each first touch of a 4 KiB page faults through macOS and then the node's KVM, about 140 µs
  each. On a kernel without huge pages, the first touch of 1 GiB took 32 to 43 s, against 0.57 s
  in a VM directly on macOS. With huge pages the node's KVM maps pod VM RAM in 2 MiB blocks: the
  first touch of 1 GiB took 0.92 s, 97% of the VM process's memory was in huge pages, and a Java
  app (PetClinic) started in 6.1 s (5.1 s on macOS) and answered warm requests in 9.5 ms (10.4 ms
  on macOS). KVM maps a 2 MiB block only where the node itself maps the memory with a huge page.
- `CONFIG_NF_CT_NETLINK=y`: the node service reads conntrack over ctnetlink, and kube-proxy needs
  it to clear stale UDP entries.

The policy is `always`, not `madvise`, because libkrun aligns guest RAM to 2 MiB but deliberately
does not advise it for huge pages: with the default `defrag=madvise`, an advised fault compacts
memory synchronously, which made a 3 GiB first touch 4 to 7 times slower. Under `madvise`, pod VMs
would get no huge pages. With `always` and the default `defrag`, a fault takes a huge page only if
one is free, without compaction. On a node whose page cache filled its memory, 6 to 17% of
huge-page faults fell back to 4 KiB pages and the first touch of 1 GiB took 0.9 to 2.0 s.

The node keeps 4 KiB pages: libkrun's Linux checkpoint code assumes them, and on a 16 KiB kernel
every pause failed reading the guest RAM's page map. Pod VMs keep the smolvm release's guest
kernel, which has no huge pages.

The node kernel has no tc (`NET_SCHED`), NFQUEUE, TPROXY, BPF JIT, conntrack events, `INET_DIAG` or
nftables logging, and at most 16 CPUs (`CONFIG_NR_CPUS`).

### khugepaged on a Mac

On a Mac node, `floci-node` makes khugepaged sleep a day between scans. `MADV_COLLAPSE` in the
nested node coincided twice with a Hypervisor.framework assertion
(`VcpuStateManager::set_pstate` from `handle_exception_exit`) that killed the whole node VM, and
khugepaged runs the same collapse code. With khugepaged sleeping, resumes saw no collapses and the
same 98% of guest RAM in huge pages, because faults get huge pages directly under `always`. The
cost is that memory faulted in 4 KiB pages stays that way; at its default rate khugepaged takes
about 10 minutes per GiB anyway. Nothing in smolvm-pods calls `MADV_COLLAPSE`.

A node is a Mac node when `/proc/cpuinfo` reports CPU implementer `0x61` (Apple):
Hypervisor.framework gives every vCPU `MIDR_EL1` `0x610f0000`, while KVM on Linux reports the
host's vendor, such as `0x41` for Arm or `0xc0` for Ampere. Linux on Apple hardware (Asahi) also
counts as a Mac node.

### The capacity kubelet sees

Kubernetes counts every pod's requests, awake or asleep, so a node that reports its own memory and
CPUs refuses pods long before 500. kubelet reads its capacity once at start, from `MemTotal` in
`/proc/meminfo` and, with no NUMA, from the number of `cpuN` directories under
`/sys/devices/system/cpu`; nothing else sets it, and a patched node status is rewritten at every
sync. So `floci-node` starts k3s in a mount namespace of its own, with a generated meminfo and a
sparse CPU tree that report a configured, larger capacity: 256 GiB and 128 CPUs by default, which
hold 500 pods requesting 512 MiB and 250m each. Real RAM is protected by the runtime's own wake
admission once its caps are set; they are off by default. The CPU tree keeps the real `online` file
and topology, so kubelet still gives kubepods the real CPUs as its cpuset; a fully fake tree makes
kubelet write a cpuset of CPUs that do not exist and fail every minute.
[node/README.md](../node/README.md#capacity) has the details.

With that capacity, kubelet's `memory.available` is the reported memory less the real use, so a
memory eviction threshold could never fire, and real memory rests on wake admission, when its caps
are set, and the kernel's OOM killer. CPU and memory managers would assign CPUs and memory that the
node does not have. So `floci-node` stops the node if kubelet would evict on memory or run either
manager.

On a Mac the node VM cannot simply be larger: Hypervisor.framework did not start a 64 GiB node VM
(60 GiB booted), the guest kernel reserves 19.7 MiB per GiB of guest RAM, and the node kernel has
at most 16 CPUs.

**Room for 500 pods.** `max-pods=600` and a /22 of pod IPs (1,021 addresses); inotify limits of
8192 instances (each container shim holds one, and the default of 128 stopped pods at about 105)
and 1048576 watches; neighbour table thresholds of 1024, 4096 and 8192. A Linux bridge holds at
most 1,024 ports, so `cni0` limits a node to about 1,000 pods.

### The runtime's cgroup

The node service and the pod VMs run in `/sys/fs/cgroup/smolvm-pods`, beside kubepods, with the
`cpu.weight` that kubelet gives kubepods for the reported CPUs (5000 for 128). In the root cgroup
each VM thread would weigh 100 against kubepods' 5000, and five busy VMs with 2 vCPUs each would
get about a sixth of the CPU. Under full contention, VM pods then got 3.87 to 3.95 CPUs and runc
pods 3.77 to 3.83, a share of 0.50 to 0.51. Kata Containers keeps its VMM outside kubepods in the
same way. k3s, kubelet and containerd stay in the root cgroup.

### Measured on a node

On an Apple M4 Max:

- A node is Ready about 10 s after it starts.
- A lone pod VM boots in 2.5 to 2.9 s and its pod is Ready in about 5 s; three at once boot in
  6.8 to 8.5 s. In the pods test, arm64 pods were Ready in 19 to 27 s and amd64 pods under Rosetta
  in 42 to 45 s.
- A pod VM process uses about 100 to 120 MiB on the node; an idle awake pod with 1 GiB of guest
  RAM about 105 MiB.
- The node service's USS grows 136 to 153 KiB for each pod that sleeps (40 pods in the wake test,
  which allows 200 KiB).
- 100 MiB TCP streams ran at 654 to 820 MiB/s into a VM pod and 322 MiB/s out.
- On a node VM with 8 vCPUs and 16 GiB, 500 runc pods and 5 VM pods requesting 82.5 CPUs and
  161 GiB (10.3 times the node) were Running 28 to 33 s after they were applied, with 9.6 of
  15.6 GiB available. Each runc pod cost about 11 MiB. The node VM's memory on the Mac was 8.6 GiB,
  and it does not shrink after pods stop.
- After SIGTERM to k3s, the API answered in 10 to 11 s, and containerd listed the same 505
  sandboxes in 15 s, with no pod restarted.

## Known limits

- A pod VM always has 2 vCPUs and 1024 MiB of RAM; the pod's requests and limits do not size it,
  and admission counts every pod as 1024 MiB.
- A container resource update, such as an in-place resize, succeeds and changes nothing.
- IPv4 only, with the CNI interface `eth0`; smolvm reads only its first IPv4 address. UDP never
  wakes a pod.
- stdin and terminals are refused when a process is created: `kubectl exec -i` and `-t`,
  `kubectl attach`, `kubectl cp` into a pod, and containers with `stdin: true` or
  `tty: true`. The guest agent ends a process's stream when the host sends any input on it.
  `kubectl port-forward` does not work for VM pods.
- Images must be imported, since the node pulls none. For a multi-platform image, containerd picks
  the arm64 variant on an arm64 node, so to run the amd64 variant, import it as a single-platform
  amd64 image.
- Each container gets its own copy of its volumes, taken when it is created. So an emptyDir is
  shared neither with init containers nor with sidecars; ConfigMap, Secret and service account
  token volumes do not update; and writes to a volume stay in the container's copy, which kubelet
  never sees, so a termination message is lost. A restarted container gets a new copy, so an
  emptyDir's contents do not survive a container restart either.
- Every file and directory of a volume copy is made readable by all users (`chmod a+rX`), so a
  Secret's or ConfigMap's `defaultMode` or `items` mode is not kept. Programs that refuse loosely
  permissioned keys, such as PostgreSQL's `ssl_key_file` or OpenSSH, fail.
- Each container's image root file system is copied into the pod share when the container is
  created. On a Mac node's ext4 storage disk, which has no reflinks, that is a full copy: it stays
  on the storage disk beside the image's own snapshot for as long as the container exists, and
  the containers of a larger image take longer to create. Give a node with many or large images a
  larger storage disk with `node-vm.sh create --storage GIB`.
- A restart of the node VM loses every pod, awake or asleep.
- A pod put to sleep through the API, not by the idle rule, has no fresh pre-sleep results: it is
  answered from its last idle pause's results, and one that never slept by itself has none, so
  kubelet's probes wake it.
- A refused wake also resets held data on the pod's own outbound connections.
- A probe exec answered from the cache is not recorded, so one that a restart of the node service
  interrupts between its create and its start never exits, until kubelet's timeout.
- Other clients on the node that connect straight to a pod's probe port are taken for kubelet.
- TCP keepalives and connection pools keep an awake pod awake, because conntrack counts packets,
  not payload.
- A process that exits just before a freeze reports its exit only at the next wake.
- Guest deletes of containers and execs deleted while their pod sleeps are kept in memory only, and
  are lost if the node service restarts.
- A crash right after a resume can leave the used checkpoint on disk until the pod is deleted.
- A killed node service waits 11 to 50 ms for RCU as it closes each sleeping pod's packet socket,
  so it takes up to about 25 s to exit at 500 sleeping pods, close to kubelet's 30 s probe budget.
- At most 2 pauses and 4 resumes run at once on a node, so 50 pods woken together take about 2.5 s
  for the last.
- Every pause writes all of the guest's RAM, uncompressed.
- The logs of the node service and of the front shim (`/var/log/smolvm-pods`) are not rotated.
- The node VM's memory is not returned to macOS.
