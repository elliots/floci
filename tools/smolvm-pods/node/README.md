# smolvm-pods node image

The Kubernetes node that smolvm-pods runs on: Debian 12 with k3s, the smolvm
v1.30.1 Linux release, the patched guest agent and libkrun, and the smolvm-pods
shim. On a Mac it runs as a nested smolvm VM on the node kernel with transparent
huge pages (`kernel/`); `scripts/node-vm.sh` builds the image and manages the VM:

```sh
node/kernel/build.sh                   # the node kernel, in the smolvm checkout's target/floci/kernel
node/libkrun/build.sh                  # the patched libkrun, in target/libkrun
scripts/node-vm.sh build --shim PATH   # writes WORK/node-image.tar
scripts/node-vm.sh create              # 4 vCPUs, 6 GiB, Rosetta, API on 127.0.0.1:6443
scripts/node-vm.sh start               # waits until the node and the RuntimeClass are ready
scripts/node-vm.sh kubectl get pods -A
```

Every command but `build` acts on the node VM `smolvm-pods-node`, or on the one
that `--name` or `SMOLVM_PODS_NODE` names.

`node-vm.sh delete` also removes smolvm's copies of the image on the Mac when no
other machine uses them: the image archive, and the seed disk that the node's
storage disk was cloned from. It removes the seed under smolvm's own locks, and
leaves smolvm's lock files in `image-seeds`, which must stay. smolvm takes no
lock on the archive, so do not delete a node while a node is being created from
the same image tar.

## Inputs

Every input is pinned, and a mismatch fails the build. The build never pulls an
image: they must be in the local Docker image store. It removes the image from
Docker once it has saved the tar, unless `--tag` names a tag to keep.

| Input | Pin |
| --- | --- |
| Base image `ghcr.io/superradcompany/debian-systemd:12` | Digest in `Dockerfile` |
| Debian packages | Snapshot `20260623T000000Z`, its InRelease files (`apt/InRelease.sha256`) and package versions |
| k3s v1.35.0+k3s1, `/bin` of `rancher/k3s` | Image digest in `node-vm.sh`; `.sha256sums` and `k3s` checksums in `Dockerfile` |
| pause 3.6 and CoreDNS 1.13.1 images | Index digests in `node-vm.sh`, which also checks every blob |
| smolvm v1.30.1 Linux arm64 release | Checksums of the archive and of `checksums.sha256` in `Dockerfile` |
| Patched guest agent | Checksum in `Dockerfile` |
| Patched libkrun, from `libkrun/build.sh`, in place of the release's `lib/libkrun.so` | Checksum in `Dockerfile`; the build is reproducible (see [libkrun](libkrun/README.md)) |
| Shim | A local build, from the smolvm commit that `scripts/build.sh` pins. Its checksum and that commit are in `/etc/floci-node/build-info` |
| Litmus test of the TSO canary | A local build of `tests/e2e/workloads/litmus` with the Go image the end-to-end tests pin. Its checksum is in `/etc/floci-node/build-info` |

## Layout in the node

| Path | Content |
| --- | --- |
| `/usr/local/bin/containerd-shim-smolvm-pods-v2` | The shim, also linked as `smolvm-pods` |
| `/usr/local/bin/floci-node` | The VM's workload: `floci-node run` prepares the node and runs the node service and k3s; `floci-node exec CMD` runs a command with the node's environment |
| `/etc/floci-node/env` | Environment of k3s, containerd, the shim and the node service: `PATH`, `RUST_LOG`, the `SMOLVM_*` paths, the capacity that kubelet reports ([Capacity](#capacity)), the idle sleep settings ([Idle sleep](#idle-sleep)) and the API's address ([The node service's API over TCP](#the-node-services-api-over-tcp)). Every variable is in [the node's settings](../docs/configuration.md#the-nodes-settings) |
| `/etc/floci-node/api-token` | The bearer token of that API, mode 0600, made at the first boot |
| `/opt/k3s/bin` | k3s and its binaries, on `PATH` after the Debian ones |
| `/opt/smolvm` | The smolvm release, with the patched agent in `agent-rootfs` |
| `/opt/smolvm-pods/litmus/litmus` | The litmus test that the shim's TSO canary runs in small VMs |
| `/etc/rancher/k3s/config.yaml` | k3s settings, with room for 600 pods; `config.yaml.d/*.yaml` can override them |
| `/run/floci-node/capacity` | The meminfo and CPU tree that k3s sees ([Capacity](#capacity)) |
| `/etc/rancher/k3s/registries.yaml` | Sends every registry to an endpoint that refuses connections, so that the node pulls no image |
| `/etc/floci-node/build-info` | The checksums of the pinned inputs and the local builds, the SHA-256 of the node kernel's Image that `node-vm.sh` boots it with (`node-kernel`), and the shim's smolvm commit |

The node VM has six disk volumes on its ext4 storage disk: `/var/lib/rancher`,
`/var/lib/kubelet`, `/var/lib/smolvm`, `/var/lib/containerd-shim-smolvm-pods`,
`/var/log` and `/var/lib/cni`. Pod logs and CNI's IP leases are then on the
disk whose free space kubelet watches, not on the 8 GiB overlay. The volumes
start empty and hide what the image has at those paths, so on every boot
`floci-node run` installs:

- the containerd template `config-v3.toml.tmpl`, which registers the runtime
  handler `smolvm-pods` (runtime type `io.containerd.smolvm-pods.v2`) and passes
  pod annotations under `smolvm.io/` and `smolvm-pods.floci.io/` to the shim;
- the `smolvm-pods` RuntimeClass, with overhead 0, as a k3s manifest;
- the pause and CoreDNS images, which k3s imports, so the node needs no registry;
- the agent rootfs and libkrun in `/var/lib/smolvm/runtime`, and the disk
  templates in `/var/lib/smolvm/.smolvm`.

smolvm's data must be under `SMOLVM_DATA_DIR=/var/lib/smolvm`: a pause
hard-links the disk templates that a VM's disks are layered on, and a hard link
cannot cross mounts. smolvm finds the templates when `HOME` is
`SMOLVM_DATA_DIR`, which `smolvm::process::apply_system_data_root` arranges.
The templates are `.zst` archives of sparse files of 20 and 10 GiB, and
`smolvm-pods _expand-templates` expands them while k3s starts. It ends about 18
seconds after boot, and a pod VM that starts earlier waits for it. Then
`smolvm-pods _tso-canary` checks TSO for Rosetta pods in two small VMs, and a
Rosetta pod that starts earlier waits for it (see
[amd64 pods](../docs/architecture.md#amd64-pods)). Both write to the node VM's log.

Before it starts k3s, `floci-node run` also removes the pod shares of the last
boot from `/var/lib/containerd-shim-smolvm-pods`: no pod VM survives a node
restart, and containerd, whose state is in `/run`, never deletes those pods. k3s
writes `/var/log/k3s.log` itself and rotates it at 50 MB, keeping three old
files; it rotates `containerd.log` the same way.

## The node service and k3s

`floci-node run` starts the node service, `smolvm-pods node-service`, before
k3s, and runs each until the node VM stops. It starts the node service again
1 second after it exits, and k3s 5 seconds after it exits. Neither waits for
the other or stops it: the front shim that containerd starts waits for the node
service, and the pod VMs outlive both. The node service's stdout and stderr
are appended to `/var/log/smolvm-pods/node-service.log`, which also gets a line
from `floci-node` at each restart. The log is not rotated, and neither is the
front shim's, `/var/log/smolvm-pods/front-shim.log`. The node service
and the pod VMs run in a cgroup of their own ([The runtime's
cgroup](#the-runtimes-cgroup)), and k3s in a mount namespace of its own
([Capacity](#capacity)).

At each start of the node service, `floci-node` takes the file
`/run/smolvm-pods/fault` if it exists: it removes it and starts the node
service with `SMOLVM_PODS_FAULT` set to its first line. This is how
[the end-to-end tests](../tests/e2e/README.md#runtime-contract) arm a fault point; the next
start runs without one.

Each start also builds the node service's arguments: `--api-listen` with
`FLOCI_NODE_API_LISTEN` (see [the API over TCP](#the-node-services-api-over-tcp)),
`--idle-timeout`, `--cpu-threshold` and `--cpu-window` with the settings of
[idle sleep](#idle-sleep), and `--max-awake-pods`, `--max-awake-memory` and
`--admission-wait` with the settings of [wake admission](#wake-admission),
each read again from `/etc/floci-node/env` and left out when empty, then the
words of `/run/smolvm-pods/node-service.args`, separated by spaces or new
lines, if the file exists. Unlike the fault file, the file stays, so every
later start uses it too, until it is removed. A flag given twice takes its
last value, so the file can replace `--api-listen`. This is how the end-to-end
tests set
`--max-awake-pods` and the API's listener, and how to try a setting such as
`--idle-timeout` on a node:

```sh
scripts/node-vm.sh exec -- sh -c 'echo --idle-timeout 1m > /run/smolvm-pods/node-service.args'
scripts/node-vm.sh exec -- pkill -f "^smolvm-pods node-service"   # floci-node starts it again
```

The node service may open 65536 files (`RLIMIT_NOFILE`): it keeps a packet
socket for each sleeping pod, and each pod VM inherits the limit. Before k3s
starts, `floci-node` also turns on conntrack accounting
(`net.netfilter.nf_conntrack_acct=1`), so that each connection's conntrack
entry counts its packets and bytes, which idle detection reads. It counts
only connections that start after it.

## Idle sleep

A VM pod that nobody calls goes to sleep by itself, and its next connection
wakes it. Three settings in `/etc/floci-node/env` decide when, for every pod
of the node: `FLOCI_NODE_IDLE_TIMEOUT`, `FLOCI_NODE_CPU_THRESHOLD` and
`FLOCI_NODE_CPU_WINDOW` ([the node's settings](../docs/configuration.md#the-nodes-settings)).
`floci-node` passes them to the node service each time it starts it, so after
a change, kill the node service and it starts again with them. A pod can set
its own with annotations ([pod annotations](../docs/configuration.md#pod-annotations)).

## Wake admission

kubelet sees far more memory than the node has ([Capacity](#capacity)), so
nothing in Kubernetes keeps awake pods within the node's real memory. The node
service's wake admission does, once a cap is set; its caps are off by default.
Three settings in `/etc/floci-node/env` set them: `FLOCI_NODE_MAX_AWAKE_PODS`,
`FLOCI_NODE_MAX_AWAKE_MEMORY`, in MiB of guest RAM with every pod VM counted
as 1024 MiB, and `FLOCI_NODE_ADMISSION_WAIT`, how long a wake waits for room
before it is refused ([the node's settings](../docs/configuration.md#the-nodes-settings)).
`floci-node` passes them to the node service each time it starts it, so after
a change, kill the node service and it starts again with them. Creates are
not refused, so pods that start at the same time can take the count above a
cap.

Each awake pod VM can take about 1.1 GiB of the node's memory: up to 1 GiB of
guest RAM and about 100 MiB for its process. A pod that wakes has all the
guest RAM it used before it slept resident at once. Without a cap, more awake
pods than the node's memory holds start the kernel's OOM killer, which kills a
pod VM's process: the pod fails, and kubelet recreates it with its memory
lost. A node VM with 6 GiB, the default, has about 5.2 GiB free for pods,
room for about four awake pods that use all of their RAM. To cap it at four:

```sh
scripts/node-vm.sh exec -- sed -i 's/^FLOCI_NODE_MAX_AWAKE_PODS=.*/FLOCI_NODE_MAX_AWAKE_PODS=4/' /etc/floci-node/env
scripts/node-vm.sh exec -- pkill -f "^smolvm-pods node-service"   # floci-node starts it again
```

The file is on the node's overlay root, so the setting survives restarts of
the node.

## The node service's API over TCP

The node service also serves its API over TCP, with a bearer token, for
programs outside the node such as Floci ([the API over
TCP](../docs/configuration.md#the-api-over-tcp)). `FLOCI_NODE_API_LISTEN` in
`/etc/floci-node/env` is the listener's address, `0.0.0.0:7070` by default;
empty turns the listener off. The token is in `/etc/floci-node/api-token`. At
each boot, `floci-node` makes that file if it is missing or empty, with 32
random bytes in hex, and makes it readable by root only (mode 0600). It
survives restarts of the node, on the overlay root. The node service reads the
token and the address when it starts, so after changing either, kill the node
service and `floci-node` starts it again with them.

**From the Mac.** The node VM publishes only the ports that `node-vm.sh
create` maps, so map a Mac port to 7070 with the arguments that
`node-vm.sh create` passes on to `smolvm machine create`. smolvm listens on
that port of the Mac's loopback and forwards each connection to the node:

```sh
scripts/node-vm.sh create -- --port 17070:7070
token=$(scripts/node-vm.sh exec -- cat /etc/floci-node/api-token)
curl -H "Authorization: Bearer $token" http://127.0.0.1:17070/v1/pods
```

Pick a Mac port that no other node uses, as with `--api-port`. A program that
creates its own node does the same: it maps a free Mac port to the node's 7070,
then reads the token from the node once it has booted, or writes its own and
restarts the node service.

## Capacity

Kubernetes counts every pod's requests, awake or asleep, so a node that
reports its own memory and CPUs refuses pods long before 500. So kubelet
reports a configured, larger capacity, the capacity view, and the runtime's
[wake admission](#wake-admission) protects real memory once its caps are set;
they are off by default. With `FLOCI_NODE_MEMORY_GIB` and `FLOCI_NODE_CPUS` at
their defaults, a node VM with 16 GiB and 8 vCPUs runs 500 runc pods and 5 VM
pods whose requests add up to at least ten times its real memory and CPUs, as
[the scale test](../tests/e2e/README.md#scale) checks.

**What kubelet sees.** `floci-node run` starts k3s in a mount namespace of its
own (`unshare --mount`), where `floci-node _k3s` binds two generated files
before it starts `k3s server`:

- over `/proc/meminfo`, the real file with `MemTotal` set to
  `FLOCI_NODE_MEMORY_GIB`, and `MemFree` and `MemAvailable` raised by as much,
  so that the memory in use stays the real figure;
- over `/sys/devices/system/cpu`, a copy of the real `online`, `possible`,
  `present`, `offline` and `kernel_max` files and of each CPU's topology, with
  empty `cpuN` directories up to `FLOCI_NODE_CPUS`. cAdvisor counts the
  directories, and kubelet still gives kubepods the real online CPUs as its
  cpuset, because the empty directories have no topology.

kubelet reads both once, when it starts. containerd, the runc shims and the
front shim, which k3s starts, see them too. runc containers, and the node
service, which runs outside k3s, see the real values. Before k3s starts,
`floci-node run` makes the node's mounts shared (`mount --make-rshared /`),
and `_k3s` makes `/proc` and `/sys` slaves in k3s's namespace, so that the two
binds stay there while kubelet's volume mounts and containerd's rootfs and
network namespace mounts reach the node service. Each start of k3s, a restart
too, makes a new namespace and reads the settings again. floci-node logs what
k3s sees and, once kubelet answers, what it reports, in the node VM's log
(`smolvm machine logs --name NODE`):

```text
floci-node: k3s sees 268435456 KiB of memory and 128 CPUs; the node has 16376332 KiB and 16 CPUs, 0-7 online
floci-node: kubelet reports cpu 128, memory 256Gi, pods 600; eviction imagefs.available<5%,nodefs.available<5%, CPU manager none, memory manager None
```

**Settings,** in `/etc/floci-node/env`:

| Variable | Default | What kubelet reports |
| --- | --- | --- |
| `FLOCI_NODE_MEMORY_GIB` | `256` | Memory, in GiB |
| `FLOCI_NODE_CPUS` | `128` | CPUs |

0, or a value below the node's own, reports the node's own. The two values
bound the sum of the requests of every pod on the node, awake or asleep, so
size each to at least the sum of the requests of the pods the node should
hold, with room for the system pods. The defaults hold 500 pods that request
512 MiB and 250m each. A larger value costs nothing: `max-pods` bounds the
number of pods, and wake admission bounds real memory once its caps are set.
Above 256 CPUs, kubepods' `cpu.weight` stays at its maximum, 10000. kubelet
admits a pod that requests more memory than the node has; whether it can wake
is the runtime's decision.

The side effects:

- kubepods' `memory.max` is the reported memory, and its `cpu.weight` the one
  for the reported CPUs: 5000 for 128, against 625 for the 16 CPUs that kubelet
  counts in an 8 vCPU node VM, whose `cpu8` to `cpu15` exist but are offline.
- The `oom_score_adj` of Burstable pods shifts slightly (999 instead of 980).
- At each start, kubelet logs `Cannot read physical package id` once for each
  CPU directory that has no topology: 120 warnings with 128 CPUs on a node
  with 8 online.

**What floci-node refuses.** kubelet must not evict on memory: its
`memory.available` is the reported memory less the real use, so a memory
threshold could never fire, and real memory rests on the runtime's wake
admission, when its caps are set, and the kernel's OOM killer. Its CPU and
memory managers must stay `none` and `None`, or they would assign CPUs and
memory that the node does not have. Before each start of k3s, floci-node reads
the `kubelet-arg` flags in `/etc/rancher/k3s/config.yaml` and
`config.yaml.d/*.yaml`, and the files in kubelet's drop-in directory
`/var/lib/rancher/k3s/agent/etc/kubelet.conf.d`. Once kubelet answers, it
reads kubelet's running configuration (`configz`), which also has kubelet
config files that a flag names. If either has a memory threshold in
`evictionHard` or `evictionSoft` (`eviction-hard` or `eviction-soft`), or a
CPU or memory manager, floci-node stops the node service and k3s and exits 1,
with the reason in the node VM's log:

```text
floci-node: error: kubelet runs with evictionSoft memory.available, which the capacity view does not allow (node/README.md, "Capacity"); stopping the node
```

Pod VMs that run keep running, as whenever the node service stops.

**Room for 500 pods.** `config.yaml` sets `max-pods=600` and
`node-cidr-mask-size=22`, 1,021 pod IPs, which takes effect only when the
node first registers. floci-node raises `fs.inotify.max_user_instances` to
8192 (each container shim holds one, and the default of 128 let only about
105 pods start) and `max_user_watches` to 1048576, and the neighbour table's
`gc_thresh1`, `gc_thresh2` and `gc_thresh3` to 1024, 4096 and 8192.

## The runtime's cgroup

The node service, its supervisor in floci-node, and the boot's template
expansion and TSO canary run in `/sys/fs/cgroup/smolvm-pods`, beside kubelet's
kubepods. The pod VMs inherit it: the node service starts each VM process, and
nothing moves it. smolvm would place each VM in a cgroup of its own only with
`SMOLVM_CGROUP_ROOT`, which the node does not set.

```text
/sys/fs/cgroup     smolvm's init, floci-node, k3s with kubelet and containerd,
 |                 containerd's shims and the front shim
 +- kubepods       runc pods; kubelet's cgroups of VM pods stay empty
 +- smolvm-pods    the node service and every pod VM
```

Its `cpu.weight` is the one that kubelet gives kubepods for the reported CPUs,
1024 shares a CPU converted as runc does: 5000 for 128 CPUs. Under contention
the pod VMs together then get as much CPU as the runc pods together. In the
root cgroup each VM thread would weigh as a process at nice 0, 100, against
kubepods' 5000: five busy VMs with 2 vCPUs each would get about a sixth of the
CPU. floci-node enables the `cpu`, `memory` and `pids` controllers for it,
sets the weight again at each start of k3s, and sets no limits. Its
`memory.current` is what the node service and the pod VMs use. Kata Containers
keeps its VMM and I/O threads outside kubepods in the same way, in
`/kata_overhead`, unless `sandbox_cgroup_only` is set.

## khugepaged on a Mac

On a Mac node, `floci-node run` writes `86400000`, a day in milliseconds, to
khugepaged's `scan_sleep_millisecs` and `alloc_sleep_millisecs` in
`/sys/kernel/mm/transparent_hugepage/khugepaged` before k3s starts. khugepaged
runs the same collapse code as `MADV_COLLAPSE`, which crashed the nested node
twice under Hypervisor.framework. With khugepaged asleep, a resumed pod VM
still has 98% of its RAM in huge pages, all from faults, but memory that pod
VMs fault in 4 KiB pages stays that way. On other hosts the kernel's settings
stay.

A node is a Mac node when `/proc/cpuinfo` reports Apple, `0x61`, as the CPU
implementer. Hypervisor.framework gives every vCPU an Apple `MIDR_EL1`: the node
VM's is `0x610f0000`, implementer `0x61` and part 0. A node VM under KVM on a
Linux host reports the vendor of the host's CPU, such as `0x41` for Arm's own
cores or `0xc0` for Ampere, and x86_64 has no CPU implementer. Linux on Apple
silicon hardware (Asahi) also reports Apple, so a node there counts as a Mac
node too, which costs only fewer huge pages. `floci-node` logs which kind of
node it found.

## Pod networking tools

The node kernel has no traffic control (tc), so the shim joins each pod VM to
its pod network with a bridge in the pod's network namespace (see
[Networking](../docs/architecture.md#networking)). It runs the image's `ip` (iproute2) and
`nsenter` (util-linux) for this, and stands in for the `ip` and `tc` that
smolvm runs.
