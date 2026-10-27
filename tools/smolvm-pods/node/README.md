# smolvm-pods node image

The Kubernetes node that smolvm-pods runs on: Debian 12 with k3s, the smolvm
v1.32.1 Linux release, the patched guest agent, libkrun and pod kernel, Docker, and the smolvm-pods
shim. On a Mac it runs as a nested smolvm VM on the node kernel with transparent
huge pages (`kernel/`); `scripts/node-vm.sh` builds the image and manages the VM:

```sh
node/kernel/build.sh                   # the node kernel, in the smolvm checkout's target/floci/kernel
node/libkrun/build.sh                  # the patched libkrun, in target/libkrun
node/podkernel/build.sh                # the pod VMs' kernel, in target/podkernel
scripts/node-vm.sh build --shim PATH   # writes WORK/node-image.tar
scripts/node-vm.sh create              # 4 vCPUs, 6 GiB, Rosetta, API on 127.0.0.1:6443
scripts/node-vm.sh start               # waits until the node and the RuntimeClass are ready
scripts/node-vm.sh kubectl get pods -A
```

Every command but `build` and `status` acts on the node VM `smolvm-pods-node`,
or on the one that `--name` or `SMOLVM_PODS_NODE` names. `node-vm.sh status`
lists every node VM that `create` made, with its state and its labels, which
record its Mac ports (`api-port`, `service-port`, `node-ports`) and whether it
is isolated (`isolated=true`); `status --json` prints them as a JSON array.

`node-vm.sh delete` also removes smolvm's copies of the image on the Mac when no
other machine uses them: the image archive, and the seed disk that the node's
storage disk was cloned from. It removes the seed disk under smolvm's own locks,
and leaves smolvm's lock files in `image-seeds`, which must stay. smolvm takes
no lock on the archive, so `create` and this cleanup hold a lock of their own,
`~/Library/Caches/smolvm-pods/node-vm.lock`, and one waits for the other.

## Inputs

Every input is pinned, and a mismatch fails the build. The build never pulls an
image: they must be in the local Docker image store. It removes the image from
Docker once it has saved the tar, unless `--tag` names a tag to keep.

| Input | Pin |
| --- | --- |
| Base image `ghcr.io/superradcompany/debian-systemd:12` | Digest in `Dockerfile` |
| Debian packages | Snapshot `20260623T000000Z`, its InRelease files (`apt/InRelease.sha256`) and package versions |
| k3s v1.35.0+k3s1, `/bin` of `rancher/k3s` | Image digest in `node-vm.sh`; `.sha256sums` and `k3s` checksums in `Dockerfile` |
| pause 3.6, CoreDNS 1.13.1 and metrics-server v0.8.0 images | Index digests in `node-vm.sh`, which also checks every blob |
| smolvm v1.32.1 Linux arm64 release | Checksums of the archive and of `checksums.sha256` in `Dockerfile` |
| Docker 27.5.1 static release for aarch64, `docker-27.5.1.tgz` from download.docker.com, which `node-vm.sh build` downloads into its release directory when missing | Checksum of the archive in `Dockerfile` (`DOCKER_SHA256`); in `/etc/floci-node/build-info` (`docker`) |
| Patched guest agent | Checksum in `Dockerfile` |
| Patched libkrun, from `libkrun/build.sh`, in place of the release's `lib/libkrun.so` | Checksum in `Dockerfile`; the build is reproducible (see [libkrun](libkrun/README.md)) |
| Pod kernel, from `podkernel/build.sh`, in place of the release's `lib/libkrunfw.so.5.5.0` | Checksum from `node-vm.sh build`; in `/etc/floci-node/build-info` (`pod-kernel`) |
| Shim | A local build, from the smolvm commit that `scripts/build.sh` pins. Its checksum and that commit are in `/etc/floci-node/build-info` |
| Litmus test of the TSO canary | A local build of `tests/e2e/workloads/litmus` with the Go image the end-to-end tests pin. Its checksum is in `/etc/floci-node/build-info` |

## Layout in the node

| Path | Content |
| --- | --- |
| `/usr/local/bin/containerd-shim-smolvm-pods-v2` | The shim, also linked as `smolvm-pods` |
| `/usr/local/bin/floci-node` | The VM's workload: `floci-node run` prepares the node and runs the node service, k3s and dockerd; `floci-node exec CMD` runs a command with the node's environment |
| `/etc/floci-node/env` | Environment of k3s, containerd, the shim and the node service: `PATH`, `RUST_LOG`, the `SMOLVM_*` paths and settings, `SMOL_NO_HOT_RELOAD`, the capacity that kubelet reports ([Capacity](#capacity)), the idle sleep settings ([Idle sleep](#idle-sleep)) and the API's address ([The node service's API over TCP](#the-node-services-api-over-tcp)). Every variable is in [the node's settings](../docs/configuration.md#the-nodes-settings). A seed's `env` can replace the node's settings ([The seed](#the-seed)) |
| `/etc/floci-node/api-token` | The bearer token of that API, mode 0600, from the seed or made at the first boot. `node-vm.sh token` prints it |
| `/etc/floci-node/seed` | The Mac directory of `node-vm.sh create --seed DIR`, read-only ([The seed](#the-seed)) |
| `/var/lib/floci-node/seed` | `installed`, the paths of the files that the last seed installed, and in `image` copies of the image's files that seed files replace |
| `/opt/k3s/bin` | k3s and its binaries, on `PATH` after the Debian ones |
| `/usr/local/bin/dockerd`, `docker`, `docker-proxy`, `docker-init` | Docker's daemon, CLI and helpers ([Docker](#docker)) |
| `/var/lib/smolvm/docker`, `/run/docker.sock`, `/var/log/dockerd.log` | dockerd's data on the storage disk, its socket and its log |
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
- the pause, CoreDNS and metrics-server images, which k3s imports, so the node needs no registry;
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
from `floci-node` at each restart. Every minute `floci-node` rotates it, and
the front shim's `/var/log/smolvm-pods/front-shim.log`, once it is larger than
64 MiB: it copies the log to `LOG.1`, over the older copy, and empties it.
Every writer of the two logs appends, so it goes on at the new end; the lines
written between the copy and the truncate are lost. The node service
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
each read again from `/etc/floci-node/env`, and from the seed's `env` on a node
with a [seed](#the-seed), and left out when empty, then the
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

## Docker

The node also runs Docker. `floci-node run` starts dockerd once k3s's containerd
socket is there, and starts it again 5 seconds after it exits:

```sh
dockerd --containerd=/run/k3s/containerd/containerd.sock --data-root /var/lib/smolvm/docker \
    --bip 172.17.0.1/16 --host unix:///run/docker.sock
```

Containers on the default bridge get the network of pods ([Network](#network)):
the node's DNS, Floci's ports and routed hosts at the node address, and
isolation.

dockerd keeps its containers in containerd's namespace `moby`, its images and
volumes on the storage disk, and its log in `/var/log/dockerd.log`. The image
has Docker's `dockerd`, `docker`, `docker-proxy` and `docker-init`, but not the
release's containerd, `containerd-shim-runc-v2`, `ctr` or `runc`, so that dockerd
uses k3s's. `floci-node exec` sets `DOCKER_HOST`, so `docker` works through it:

```sh
docker save IMAGE | scripts/node-vm.sh exec -i -- docker load
scripts/node-vm.sh exec -- docker run -d --name web IMAGE                       # runc
scripts/node-vm.sh exec -- docker run -d --name vm --runtime io.containerd.smolvm-pods.v2 IMAGE
scripts/node-vm.sh exec -- docker run -d --name sleepy --runtime io.containerd.smolvm-pods.v2 \
    --annotation smolvm-pods.floci.io/hibernate=true --annotation smolvm-pods.floci.io/idle-timeout=2m IMAGE
```

dockerd's default runtime stays runc, so a container is a plain container
unless it names the runtime `io.containerd.smolvm-pods.v2`, with `--runtime` or
`HostConfig.Runtime` in the API. dockerd needs no configuration for it: it
finds the shim on `PATH` by the runtime's name. Such a container runs in a pod
VM of its own, at its Docker IP, and stays awake unless it opts in to sleep
with the annotation `smolvm-pods.floci.io/hibernate: "true"`. `docker run
--annotation KEY=VALUE` and `HostConfig.Annotations` put annotations into the
container's OCI spec, where the shim reads them, as it reads a pod's;
`docker run --label` does not. [Docker containers](../docs/architecture.md#docker-containers)
has how it works and what Docker features work.

**From the Mac.** On a node made with `--service-port`, the Docker CLI on the
Mac reaches dockerd through the node service's API, which serves dockerd's
Engine API at `/docker` behind its token ([Docker and
tunnels](../docs/configuration.md#docker-and-tunnels)):

```sh
eval "$(scripts/node-vm.sh docker-env)"
docker ps
docker logs -f vm
```

`docker-env` writes a Docker CLI config of the node's own,
`~/.config/smolvm-pods/docker/NODE/config.json` (directory mode 0700, file
mode 0600), whose `HttpHeaders` send the token to this node only, and prints
the exports of `DOCKER_HOST=tcp://127.0.0.1:PORT/docker` and of
`DOCKER_CONFIG`. Run it again after the token changes. `docker exec -i` and
`-t` into a container in a pod VM are refused, as for a pod.

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
random bytes in hex, and makes it readable by root only (mode 0600). On a node
whose [seed](#the-seed) has an `api-token`, the file is the seed's. It
survives restarts of the node, on the overlay root. The node service reads the
token and the address when it starts, so after changing either, kill the node
service and `floci-node` starts it again with them.

**From the Mac.** The node VM publishes only the ports that `node-vm.sh
create` maps. `--service-port PORT` maps the Mac's PORT to the node's 7070, and
records it in the machine's `service-port` label. smolvm listens on that port
of the Mac's loopback and forwards each connection to the node:

```sh
scripts/node-vm.sh create --service-port 17070
scripts/node-vm.sh start
token=$(scripts/node-vm.sh token)
curl -H "Authorization: Bearer $token" http://127.0.0.1:17070/v1/pods
```

Pick a Mac port that no other node uses, as with `--api-port`. A program that
creates its own node does the same: it maps a free Mac port to the node's 7070,
then reads the token with `node-vm.sh token` once the node has booted.

The same port serves the node service's admin UI at
`http://127.0.0.1:17070/ui`, which asks for the token once
([the admin UI](../docs/configuration.md#the-admin-ui)).

## An isolated node

`node-vm.sh create --isolate` limits what the node VM reaches with smolvm's
egress policy, which nothing in the node can bypass
([Networking](../docs/architecture.md#networking)). The node reaches:

- smolvm's gateway over TCP at its IPv4 address, which reaches every port of
  the Mac's loopback, such as Floci's. The address is `100.96.0.1`, or the
  first address of the `--guest-subnet` passed to `smolvm machine create`
  after `--`. `create --isolate` refuses smolvm's `--network`, since a machine
  on a named network gets its gateway only when it starts;
- each `--allow-cidr CIDR` range.

Every other destination is refused, and the gateway, which answers every DNS
query, resolves no name: the policy allows only names under
`closed.floci.invalid`, which has none. Connections from the Mac to the
node's published ports are not egress, so the Kubernetes API, the node
service's API and the NodePorts work as on any node. `create --isolate` also
sets `FLOCI_NODE_ISOLATION=1` in the node's environment and labels the
machine `isolated=true`.

The node stays isolated for its life. `node-vm.sh update` changes its ranges,
at once on a running node:

```sh
scripts/node-vm.sh update --allow-cidr 203.0.113.10/32
scripts/node-vm.sh update --remove-allow-cidr 203.0.113.10/32
```

`update` refuses a node without the `isolated=true` label, since an allow list
would restrict an open node to that list.

## Network

Each node owns an address that its pods reach through their default route,
`169.254.53.1` (`FLOCI_NODE_NETWORK_ADDRESS`), on a dummy interface
`floci0`. Before k3s starts, `floci-node` runs `smolvm-pods _network-boot`,
which adds the address and loads the table `inet floci`, and points both
k3s's `resolv-conf` (`/etc/floci-node/pod-resolv.conf`, which CoreDNS
forwards to) and the node's own `/etc/resolv.conf` at the address. It keeps
the node's first resolver in `/etc/floci-node/upstream-resolv.conf`, and
passes the k3s cluster and Service CIDRs and the cluster DNS address, from
k3s's configuration, to the node service as `FLOCI_NODE_CLUSTER_CIDRS` and
`FLOCI_NODE_CLUSTER_DNS`.

Until Floci publishes a network profile (`PUT /v1/network`), the node
service's DNS forwarder answers from that first resolver, and the table only
sends DNS at the address to the forwarder; on an isolated node, the forwarder
refuses every name and pods reach only the cluster, while node processes are
held only by smolvm's boundary, so that k3s reaches Floci's webhooks. With
a profile, pods reach Floci's ports, the profile's endpoints and routed hosts
at the address, Floci answers their DNS, and under isolation every other
connection goes to the sinkhole. The node service keeps the profile and
applies it again when it starts.
[docs/architecture.md](../docs/architecture.md#the-node-address) has the
table and what serves each port.

## The seed

A node that Floci creates gets its settings and files from a seed directory
that Floci writes on the Mac. `node-vm.sh create --seed DIR` mounts the
directory read-only at `/etc/floci-node/seed`. On a node with that mount,
`floci-node run` applies the seed at every boot, before it installs the k3s
files and starts the node service and k3s:

1. It waits until the seed's `version` file reads `1`. Floci writes `version`
   last, so the seed is complete by then. Until then `floci-node` logs why it
   waits and checks again, after 2 s, then twice as long each time up to 30 s;
   neither the node service nor k3s starts.
2. It installs each seed file of the table below, owned by root with its mode,
   and removes each one that the last seed installed and this one lacks. Before
   a seed file replaces one of the image's, such as `registries.yaml`, a copy of
   the image's goes to `/var/lib/floci-node/seed/image`, and it comes back when
   the seed's goes. A boot that ends during this step leaves every file for the
   next boot to finish.
3. It writes each name of the seed's `hosts` file, one per line, in
   `/etc/hosts` at the gateway of the node's default route, between the lines
   `# floci-node seed: begin` and `# floci-node seed: end`. The gateway reaches
   the Mac's loopback, so k3s and containerd reach Floci by such a name, such
   as `host.docker.internal`. Without a `hosts` file, the marked lines go.

| Seed file | In the node | Mode |
| --- | --- | --- |
| `version` | Not installed: the seed is complete once it reads `1` | |
| `env` | Read where it is: the node's settings, below | |
| `k3s.yaml` | `/etc/rancher/k3s/config.yaml.d/50-floci.yaml`: k3s settings such as the node's name, kubelet flags and API server flags | 0644 |
| `token-webhook.yaml` | `/etc/rancher/k3s/floci/token-webhook.yaml` | 0644 |
| `tokens.csv` | `/etc/rancher/k3s/floci/tokens.csv` | 0600 |
| `sa-signing-key.pem`, `sa-public-key.pem` | `/etc/rancher/k3s/floci/` | 0600 |
| `registries.yaml` | `/etc/rancher/k3s/registries.yaml`, in place of the image's | 0644 |
| `manifests/floci-admission.yaml` | `/var/lib/rancher/k3s/server/manifests/floci-admission.yaml`, which k3s applies | 0644 |
| `api-token` | `/etc/floci-node/api-token`, the node service's bearer token | 0600 |
| `hosts` | Lines in `/etc/hosts` | |

In the node, the mount's files belong to the Mac user that wrote them, and the
node service refuses a token file that is not its own, so `floci-node` copies
the files rather than using them where they are.

**The node's settings.** `floci-node` reads `/etc/floci-node/env`, then the
seed's `env`, whose lines are `NAME=VALUE`, taken as they are, without quotes
or expansion, and `#` comments. A setting with a value replaces the one in
`/etc/floci-node/env`; an empty one keeps it. The seed may set `RUST_LOG` and
the `FLOCI_NODE_*` settings, but not `FLOCI_NODE_ISOLATION`, which only the
machine's environment sets ([An isolated node](#an-isolated-node));
`floci-node` logs and ignores any other line. Everything that reads the node's
settings reads both files this way, so kubelet reports the seed's capacity from
the first start of k3s, and the node service gets the seed's flags.

Every 5 s `floci-node` compares the SHA-256 of the seed's `env` with the last
one. On a change it logs `the seed's env changed: restarting the node service`
in the node service's log and stops the node service, which it starts again
1 second later with the new settings; k3s and the pods keep running. The
capacity settings and `RUST_LOG` apply as after a change to
`/etc/floci-node/env`: at the next start of k3s, and of the node for the node
service's `RUST_LOG` ([the node's
settings](../docs/configuration.md#the-nodes-settings)).

`floci-node` checks the seed's `k3s.yaml` with the rest of k3s's
configuration: a memory eviction threshold, or a CPU or memory manager, stops
the node ([Capacity](#capacity)).

A node without the mount reads only `/etc/floci-node/env` and installs nothing
of a seed.

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

## Expedited RCU

floci-node writes 1 to `/sys/kernel/rcu_expedited`, so that
`synchronize_rcu` runs as `synchronize_rcu_expedited`. Closing a packet socket
waits for an RCU grace period, 11 to 50 ms at the nested node's normal pace,
and the node service holds one packet socket for each sleeping pod. At a
SIGTERM it closes them all at once, but the kernel closes those of a node
service killed with SIGKILL, or by the OOM killer, one after another: up to
25 s for 500 sleeping pods, close to kubelet's 30 s probe budget. An expedited
grace period interrupts the busy CPUs instead of waiting for each to pass a
quiescent state on its own. OrbStack's Linux VM on a Mac runs with it, and
there 500 packet sockets close one after another in about 10 ms.

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
