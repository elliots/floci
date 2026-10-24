# smolvm-pods

smolvm-pods runs each Kubernetes pod in its own [smolvm](https://github.com/smol-machines/smolvm)
microVM, puts idle pods to sleep and wakes them when a connection comes in. A sleeping pod's VM
is saved to a checkpoint on the node's disk and its process exits, so it uses no memory, while
Kubernetes keeps seeing the pod Running and Ready, with the same IP. Apps and manifests need no
change: a pod opts in with the RuntimeClass `smolvm-pods`. The goal is hundreds of applications on
one machine, on a Mac as a nested VM, or on a Linux host with KVM.

New to smolvm-pods? [docs/getting-started.md](docs/getting-started.md) goes step by step from what
a Mac needs to a pod that sleeps and wakes, with the tests and the common problems.

It is one binary, `containerd-shim-smolvm-pods-v2`: the containerd shim for the runtime type
`io.containerd.smolvm-pods.v2`, the node service that owns the pod VMs, the `ctl` client of that
service's API, and the entry point of each pod's VM process. It uses smolvm as a library
(`smolvm::embedded` and `smolvm::internal_boot`) and never runs the smolvm CLI. Part of the code
is ported from smolvm's shim ([Code from smolvm-shim](#code-from-smolvm-shim)).

## How it fits together

The node is a k3s node on Linux. On a Mac it is a smolvm VM started with `--nested`, and each pod
VM is a KVM VM nested inside it. The node image ([node/README.md](node/README.md)) carries k3s,
the smolvm Linux release with a patched guest agent and a patched libkrun, and this binary. It
boots a kernel with transparent huge pages ([node/kernel](node/kernel/README.md)), without which
memory in a nested VM is many times slower.

containerd starts the **front shim**, a small process that serves every pod's tasks from a table
and forwards whatever needs a VM to the **node service**, over a Unix socket. The node service
boots each pod sandbox as a smolvm machine, whose process runs this binary as `_boot-vm`, and runs
the pod's containers in it through smolvm's guest agent. It streams their output to containerd's
fifos and keeps each pod's state in a durable record. The two are separate so that the node
service can crash and restart without containerd noticing, because containerd ends every task of
a shim process that dies.

A pod VM keeps the pod's CNI network: a two-port bridge in the pod's network namespace joins the
pod's `eth0` to the VM's tap. To put a pod to sleep, the node service holds its traffic at that
bridge and pauses the VM to a local checkpoint. While the pod sleeps, a packet socket on `eth0`
answers ARP and kubelet's probes for it, and the first connection to the pod wakes it: the VM
resumes in about 0.2 s, the guest clock is stepped, and the held packets go to the guest
unchanged. A pod sleeps by itself when no connection to it has seen traffic for its idle timeout
(5 minutes by default), its VM is not busy, and its probes pass. Floci, and anything else with the
token, can wake a pod and keep it awake through the node service's API.

The node also runs dockerd on the same containerd, with runc as its default runtime. A Docker
container that names the runtime, `docker run --runtime io.containerd.smolvm-pods.v2`, runs in a
pod VM of its own at its Docker IP, and sleeps and wakes as a pod does when it opts in with the
annotation `smolvm-pods.floci.io/hibernate: "true"`
([Docker containers](docs/architecture.md#docker-containers)).

[docs/architecture.md](docs/architecture.md) explains each part and why it works this way.

## Run a node on a Mac

The node needs an Apple silicon Mac with nested virtualization (M3 or later), Docker, and a smolvm
checkout next to the floci repository (`../smolvm` from the floci repository root) at the commit
that `scripts/build.sh` pins ([Build](#build) says where it is). `SMOLVM_SRC` names another
checkout; `scripts/build.sh`, `node/kernel/build.sh`, `node/podkernel/build.sh`,
`node/libkrun/build.sh` and `scripts/node-vm.sh` all use the same one. Before the first build:

- Docker must already have the images that `node-vm.sh build` uses, because it never pulls one:
  the base image of `node/Dockerfile`, `rancher/k3s`, pause, CoreDNS and metrics-server, at the digests that
  `node/Dockerfile` and `scripts/node-vm.sh` pin ([node/README.md](node/README.md#inputs)), and
  the Go image of `tests/e2e/workloads/go-image`, and the Rust image of `scripts/rust.env`, which
  `node/libkrun/build.sh` never pulls either.
- The checkout's `target/floci` must hold smolvm-floci, the smolvm CLI that runs the node VM:
  `release/`, smolvm v1.32.1's `smolvm-1.32.1-darwin-arm64.tar.gz` unpacked without its top
  directory, so that `checksums.txt` and `lib/` are directly in `release/`; `bin/smolvm`, the
  smolvm CLI built on the Mac from the checkout and signed with `smolvm.entitlements` (`cargo make
  dev` in the checkout builds and signs `target/release/smolvm`), with the release's disk
  templates beside it or in `~/.smolvm`; and `agent-rootfs/`, the release's agent rootfs with the
  patched guest agent ([Build](#build)) at `usr/local/bin/smolvm-agent`. `node/kernel/build.sh`
  stops without them, and `node-vm.sh build` takes that agent unless `--agent` names another.
  `SMOLVM_PODS_KERNEL_DIR` and `SMOLVM` put the node kernel and the CLI elsewhere. In the
  checkout, the release is unpacked with:

  ```sh
  mkdir -p target/floci/release
  tar -xzf smolvm-1.32.1-darwin-arm64.tar.gz -C target/floci/release --strip-components 1
  ```

From `tools/smolvm-pods`:

```sh
node/kernel/build.sh                    # the node kernel with transparent huge pages
node/podkernel/build.sh                 # the pod VMs' kernel, with the netfilter options of sidecars
node/libkrun/build.sh                   # the patched libkrun
scripts/build.sh                        # this binary, target/release-small/containerd-shim-smolvm-pods-v2
scripts/node-vm.sh build                # the node image tar
scripts/node-vm.sh create               # 4 vCPUs, 6 GiB, Rosetta, Kubernetes API on 127.0.0.1:6443
scripts/node-vm.sh start                # waits until the node and the RuntimeClass are ready
scripts/node-vm.sh kubectl get pods -A
```

A pod names the RuntimeClass to run in a VM:

```yaml
apiVersion: v1
kind: Pod
metadata:
  name: web
spec:
  runtimeClassName: smolvm-pods
  containers:
    - name: web
      image: nginx:1.27
```

The node pulls no image from a registry: import images into its containerd, for example:

```sh
docker save IMAGE | scripts/node-vm.sh exec -i -- ctr -n k8s.io images import --platform linux/arm64 -
```

A pod that names an image by digest also needs a digest name in the node
([Import the image](docs/getting-started.md#import-the-image)). On an arm64 node, amd64 images run
under Rosetta: import a single-platform amd64 image with `--platform linux/amd64`
([Known limits](docs/architecture.md#known-limits)).

`scripts/node-vm.sh exec -- smolvm-pods ctl list` shows each pod's state, and `ctl hibernate` and
`ctl wake` put a pod to sleep and wake it. The node's settings, the pod annotations, the API and
`ctl` are in [docs/configuration.md](docs/configuration.md).

## Run on another node

Any Linux node with KVM and containerd 2.x can run it, with:

- the binary on containerd's `PATH`, also linked as `smolvm-pods`, the name that the commands
  below use, and containerd configured with:

  ```toml
  [plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.smolvm-pods]
    runtime_type = 'io.containerd.smolvm-pods.v2'
    sandboxer = 'podsandbox'
    pod_annotations = ['smolvm.io/*', 'smolvm-pods.floci.io/*']
    container_annotations = ['smolvm.io/*', 'smolvm-pods.floci.io/*']
  ```

- a RuntimeClass named `smolvm-pods` with the handler `smolvm-pods`;
- the kubelet's kubeconfig at `/var/lib/rancher/k3s/agent/kubelet.kubeconfig` (k3s's path), which
  the node service uses to watch this node's pods and their probes; without it no pod sleeps by
  itself;
- in containerd's environment: `SMOLVM_DATA_DIR`, `SMOLVM_LIB_DIR` (the smolvm Linux release's
  libraries, with the patched libkrun of `node/libkrun` and the pod kernel of `node/podkernel` as
  `libkrunfw.so.5.5.0`), `SMOLVM_AGENT_ROOTFS` (the patched agent rootfs, which must contain
  `/usr/bin/rosetta-wrapper`), and `SMOLVM_VM_UID_DROP=off` and `SMOL_NO_HOT_RELOAD=1`, without
  which non-root processes in a pod cannot write their volumes;
- a kernel with conntrack netlink (`CONFIG_NF_CT_NETLINK` or the `nf_conntrack_netlink` module)
  and `br_netfilter`, with `net.bridge.bridge-nf-call-iptables=1`, `/dev/kvm`, and iproute2's
  `ip` and util-linux's `nsenter` on containerd's `PATH`;
- `/var/lib/containerd-shim-smolvm-pods` and the smolvm data directory on ext4 or xfs, not on
  overlayfs, with smolvm's disk templates (or their `.zst` archives) in `$SMOLVM_DATA_DIR/.smolvm`;
- on an arm64 node, the static arm64 litmus test of `tests/e2e/workloads` at
  `/opt/smolvm-pods/litmus/litmus`, for the TSO canary;
- at boot, before k3s starts: remove the pod shares of the last boot (every entry of
  `/var/lib/containerd-shim-smolvm-pods` but its dot files), which containerd never deletes; turn
  on conntrack accounting (`net.netfilter.nf_conntrack_acct=1`); on a Mac, make khugepaged sleep a
  day between scans; and start `smolvm-pods node-service` with the same environment as
  containerd, 65536 open files, and again 1 s after it exits. Then run
  `smolvm-pods _expand-templates` and `smolvm-pods _tso-canary`, which may run while k3s starts;
- the node service and the pod VMs in a cgroup of their own beside kubepods, with kubepods'
  `cpu.weight`;
- for hundreds of pods: kubelet reporting a larger memory and CPU capacity than the node has,
  never with memory eviction or a CPU or memory manager, `max-pods=600`, a /22 of pod IPs, and
  higher inotify and neighbour table limits;
- for Floci: the node service's `--api-listen` and a token file of mode 0600 owned by root.

`floci-node` in the node image does each of these; [node/README.md](node/README.md) describes how.

## Build

The binary links glibc, because smolvm loads the release's `libkrun.so` with `dlopen`, which a
static musl binary cannot do. `scripts/build.sh` builds it for linux/arm64 in the
`rust:1.92-bookworm` image (Debian 12 and glibc 2.36, like the node) with Rust 1.99.1, which rustup
installs into a cache in the smolvm checkout on first use. `scripts/rust.env` pins both. With no
arguments it runs `cargo build --locked --profile release-small`; other arguments go to cargo. The
result is `target/release-small/containerd-shim-smolvm-pods-v2`.

smolvm is a path dependency (`../../../smolvm`, or `SMOLVM_SRC`), pinned by commit in
`scripts/build.sh`. The pinned commit is smolvm v1.32.1 with the patches that smolvm-pods needs:
`SetClock`; the guest agent's pod processes that outlive host connections and journal their
output, with `PodAttach` and a cursor error that names the oldest journal entry; a local pause
and resume that keep the checkpoint unpacked; deletion of a used checkpoint off the resume path;
no private RAM copy with the patched libkrun; a cancellable local pause; the release of a save
that an interrupted pause left in the VMM; stopping and inspecting paused machines; and CRI mounts
of a pod container bound in place, read-only when the mount says `ro`, with each container's own
writable copy of `/etc/hosts`, `/etc/resolv.conf` and `/etc/hostname`. The patch series, with
the guest agent's sources, is the smolvm branch `floci/runtime`, whose head is the pinned commit.
A build needs a checkout of it: [The checkouts](docs/getting-started.md#the-checkouts) says how
to make one.

`scripts/build.sh` refuses a smolvm checkout at another commit or with uncommitted changes, and
passes the commit to the build, which `smolvm-pods _build-info` prints and the node image records
in `/etc/floci-node/build-info`. The guest agent that `node/Dockerfile` pins comes from the same
commit. `cargo build --locked --profile release-small -p smolvm-agent` in the smolvm checkout,
with Rust 1.90.0 in `rust:1.91.1-alpine3.22`
(`rust@sha256:8efbfb788786eeb127adc581394349c5fb567712156e0f8c2e499acadbc23756`), gives the
binary of `AGENT_SHA256`. The image's digest fixes its Alpine packages, `musl-dev` among them, so
the build installs none. `node-vm.sh build` takes the agent with `--agent`, and
[The guest agent](docs/getting-started.md#the-guest-agent) has the command. To move to another
smolvm commit, change `smolvm_commit` in `scripts/build.sh` and, if the agent changed,
`AGENT_SHA256` in `node/Dockerfile`.

`Cargo.lock` is smolvm's lock reduced to this crate's dependencies, so the versions match
smolvm's, and the build adds no crate. To refresh it:

```sh
cp "${SMOLVM_SRC:-../../../smolvm}"/Cargo.lock . && scripts/build.sh update --workspace --offline
```

## Tests

```sh
scripts/build.sh test --locked
scripts/build.sh clippy --locked --all-targets -- -D warnings
scripts/build.sh fmt --check
```

The unit tests boot no VM. They drive the task lifecycle through the front shim and a node
service with mock pods, in one process: standalone containers as pods of their own and the
delete action of one whose id a new task took, every call while the node service is down, a restart of
the node service while pods run, a lost VM, an abandoned pod, the fifo keep-alive, the `delete`
action's tombstone, a panic in a handler through a real ttrpc server, and a fixed random sequence
of calls with restarts. The lifecycle runs over a mock VM layer, with recovery after a crash at
every step, leases, merged wakes, cancelled pauses and incarnations. They also check the
protocol's wire format, the Rosetta default and the amd64 entrypoint check, the TSO step and its
marker, the canary's verdict, the network stand-ins, what a delete removes, the sweep of smolvm's
locks, the VM watcher, the output pumps against a fake agent whose frames stall or fall out of its
journal, the API and `ctl`, conntrack parsing, the CPU meter, the catcher's frame rules, the probe
responder's answers, the pre-sleep checks, admission, and a standalone container's network
check and OCI hooks.

**Live tests.** Some tests need root, network namespaces and `/dev/net/tun`, or a node, and run
only with `--ignored`. Build the test binary first:

```sh
scripts/build.sh test --locked --no-run   # prints target/debug/deps/containerd_shim_smolvm_pods_v2-<hash>
```

- `net::netns_tests::smolvm_attaches_a_pod_through_the_bridge` runs smolvm's own pod network
  attach through the stand-ins in real network namespaces: `cni0` in hairpin mode, the pod's
  `eth0`, and a tap in a third namespace as the guest. It checks traffic both ways after every
  neighbour flush, that the node finds the guest's MAC, that while the pod sleeps a packet socket
  on `eth0` that answers ARP with the guest's MAC sees the SYN of a client with an empty
  neighbour table and the client waits instead of being refused, that a second attach gives the
  guest the same MAC, and that a delete removes `podbr0` and `tap0` and leaves `eth0`. It fails
  if a bridge port learns addresses or the namespace answers ARP. It needs `ip` and `nsenter`, and
  runs in a privileged container from the node image, or in the node itself, where the pods test
  of `tests/e2e` runs it against the node's kernel. `node-vm.sh build` removes the image from
  Docker unless `--tag` keeps it:

  ```sh
  scripts/build.sh build --locked
  scripts/node-vm.sh build --tag smolvm-pods-node:test
  docker run --rm --privileged --network none -v "$PWD/target/debug:/t:ro" \
    -e SMOLVM_PODS_BIN=/t/containerd-shim-smolvm-pods-v2 \
    --entrypoint /t/deps/containerd_shim_smolvm_pods_v2-<hash> smolvm-pods-node:test \
    --ignored --exact net::netns_tests::smolvm_attaches_a_pod_through_the_bridge
  ```

- `probe::responder::tests::tap` puts the probe responder behind a tap with checksum offload, so
  that the kernel's own clients connect to it with partial checksums, as through a veth. Run it
  under `docker run --cap-add NET_ADMIN --device /dev/net/tun`.
- `sleepnet::netloop::netns_tests` run the catchers against a node, a pod, a client and a guest
  made of namespaces and taps: a wake, a refusal, a lost hold, a close and a restart, a stop of
  many catchers at once (`SLEEPNET_STOP_PODS`, default 100), and the cost of a catcher. `sleepnet::netloop::live_tests` need a node, a pod and a client namespace on
  `cni0`:

  ```sh
  unshare -m <test binary> --ignored --test-threads 1 sleepnet::netloop::netns_tests
  SLEEPNET_LIVE_POD=SANDBOX SLEEPNET_LIVE_NETNS=/var/run/netns/cni-... \
  SLEEPNET_LIVE_CLIENT=/var/run/netns/CLIENT SLEEPNET_LIVE_PORT=8080 SLEEPNET_LIVE_CLIENTS=10 \
  <test binary> --ignored --nocapture sleepnet::netloop::live_tests
  ```

- `activity::cpu::tests::live_a_busy_thread_reads_as_one_vcpu` reads its own process while a
  thread spins for a second. It runs on any Linux host that is otherwise idle.
- These run in a node as root, with the test binary shared into it:
  `activity::conntrack::tests::live_dump_and_get` opens `CONNTRACK_LIVE_TCP` loopback connections
  (200), sends to `CONNTRACK_LIVE_UDP` closed UDP ports (0) for more entries, times five dumps,
  gets one entry by its reply tuple (`CONNTRACK_CAPTURE=FILE` records the raw reply), and finds it
  by either tuple, named as the original or the reply;
  `activity::idle::tests::live_watch_a_pod` dumps `LIVE_POLLS` times (3), `LIVE_INTERVAL` seconds
  apart (15), for `LIVE_POD_IP` with the probe ports `LIVE_PROBE_PORTS`, and prints what it
  counts; `activity::cpu::tests::live_reads_a_vm_process` reads the CPU of `LIVE_VM_PID` twice,
  `LIVE_INTERVAL` seconds apart (5);
  `kube::pods::tests::live_the_kubelets_credentials_list_this_nodes_pods` lists the node's pods
  with the kubelet's kubeconfig; and
  `probe::spec::tests::live_the_probes_of_this_nodes_pods_run_from_the_node` runs every TCP, HTTP
  and gRPC liveness and readiness probe of the node's pods from the node. The node needs Mac
  ports that no running node uses:

  ```sh
  scripts/node-vm.sh create --name NODE --api-port 16443 --node-ports 31080-31080 \
    --volume "$PWD/target/debug:/t:ro"
  scripts/node-vm.sh start --name NODE
  scripts/node-vm.sh exec --name NODE -- env CONNTRACK_LIVE_TCP=2000 CONNTRACK_LIVE_UDP=5000 \
    /t/deps/containerd_shim_smolvm_pods_v2-<hash> --ignored --exact --nocapture \
    activity::conntrack::tests::live_dump_and_get
  ```

The end-to-end tests, which create node VMs and run pods through kubelet, are in
[tests/e2e](tests/e2e/README.md), with the runtime contract that they rely on.

**The resume check.** `bench/resume/check.sh` checks smolvm's own pause and resume in a fresh node
VM, without the runtime. In the node, `resume-harness` drives two VMs with smolvm as a library:
memhold, which holds 1 GiB of pseudo-random memory, and PetClinic on Tomcat. Each runs three
repetitions of a cold start and two cycles of a local pause and a resume. `summarize.py` fails a
resume whose first answer comes 0.5 s or more after the resume started, whose memory hash
changed, whose VM process has less than 90% of its RSS in huge pages at the first answer, or
whose warm requests are more than 1.5 times as slow as after the cold start: PetClinic's median
of 50 requests, and memhold's second read of 1 GiB and its random reads. It also fails unless the
node runs the libkrun that its build-info names, with THP `always`, and the harness is built from
the binary's smolvm commit. It needs the harness, PetClinic 6.1.2's exploded WAR, the Go image
of `tests/e2e/workloads/go-image` in the local Docker image store, on the Mac `jq`, `python3` and
GNU `timeout` (for example from Homebrew's coreutils), and the network, because smolvm in the
node pulls the busybox and Tomcat images by digest from public.ecr.aws:

```sh
scripts/build.sh build --locked --release --manifest-path bench/resume/harness/Cargo.toml
bench/resume/check.sh --petclinic DIR   # check.sh help lists the options
```

**The save benchmark.** `check.sh --delta on|off` compares delta saves, which write only the 4 KiB
pages of guest RAM that changed, with full saves (`SMOLVM_DELTA_SAVES=off`). Per workload, a new
pod VM gets a cold start and its first pause, then `--cycles` cycles (default 6) of a resume, an
optional dirtying request, an idle period (`--idle`, default 2 s) and a pause, then a last resume.
Each pause records smolvm's save mode, bytes written and time, and memhold checks its own SHA-256
of the memory it uses after every resume. A run takes about 10 minutes. `--settle S` also reads
the node storage disk's sectors written around each pause, after a sync and S seconds of
writeback, less the node's background writes; it makes a run several times longer, and smolvm's
bytes matched the disk's within 20 MB in every pause measured. The default workloads are memhold
with no dirtying (`idle`), with 0.05% of its pages rewritten at random places (`scattered:0.05`)
and with all its memory rewritten (`all`). `--workloads` also takes `petclinic`, whose first answer
after a resume is one request, and `scattered`, `blocks` (whole 2 MiB blocks), `zero`, `free`,
`fresh` and `mixed` with a percentage, such as `scattered:1`. `compare.py` prints both runs side
by side, with the medians and p95 of the bytes, pause and resume times, the memory checks, the
daily writes of 500 pods that each sleep 10 times a day, and the design's pass bars as checks
(`python3 -m unittest test_compare` in `bench/resume` tests it):

```sh
bench/resume/check.sh --delta on    # results in target/resume-check/saves-on
bench/resume/check.sh --delta off   # results in target/resume-check/saves-off
bench/resume/compare.py saves-on saves-off
```

## Where the code lives

| Path | What |
| --- | --- |
| `src/main.rs` | The binary's modes |
| `src/front` | The front shim: the task table that containerd talks to, and its connection to the node service |
| `src/proto.rs` | The protocol between the front shim and the node service |
| `src/node` | The node service: sessions with the front shim, the VM operations over smolvm, the hooks for sleep and wake, admission and its flags |
| `src/lifecycle` | Each pod's states and transitions, leases, durable records, events, recovery and fault points |
| `src/backend.rs`, `src/engine.rs`, `src/bundle.rs` | A pod's VM: boot, containers and execs through the guest agent, and OCI bundles |
| `src/standalone.rs` | Containers without CRI annotations, such as Docker's, as pods of their own: their network namespace and OCI hooks |
| `src/pump.rs` | The output pumps |
| `src/api.rs` | The API and `ctl` |
| `src/sleepnet` | The catcher: a sleeping pod's packet socket, the hold, the frame rules and the release |
| `src/activity` | Conntrack, each pod's last activity and idle timer, and VM CPU |
| `src/probe` | Probe definitions and results, the pre-sleep check, the probe responder and exec probes |
| `src/kube` | The watch of this node's pods on the API server, and its Kubernetes Events |
| `src/network`, `src/trust.rs` | The node address, its nftables table, the DNS forwarder, the relay, the sinkhole and Floci's network profile, and Floci's CA in each container |
| `src/metrics.rs` | The node service's metrics |
| `src/net.rs` | Pod networking: the `ip` and `tc` stand-ins and the bridge |
| `src/boot.rs`, `src/rosetta.rs`, `src/canary.rs` | The VM process's entry point and TSO, Rosetta, and the TSO canary |
| `src/templates.rs`, `src/cleanup.rs`, `src/tombstone.rs` | Disk template expansion, the sweeps after a delete, and delete tombstones |
| `src/logging.rs`, `src/panics.rs` | Logging and panic isolation |
| `src/files.rs`, `src/sync.rs`, `src/netlink.rs`, `src/http1.rs` | Shared helpers: file locks, durable writes and the boot id, lock helpers that tolerate poisoning, raw netlink, and an HTTP/1.1 client over TCP or TLS |
| `src/test_netns.rs` | Network namespaces and taps for the tests that need root |
| `node/` | The node image, its kernel and the libkrun patches |
| `scripts/` | `build.sh` and `node-vm.sh` |
| `tests/e2e` | The end-to-end tests and their workloads |
| `bench/resume` | The resume check and the save benchmark |

## Code from smolvm-shim

These files are derived from smolvm's `crates/smolvm-shim` (Apache-2.0) of smolvm v1.32.1, the
base of the pinned commit ([Build](#build)).

| File | Upstream file |
| --- | --- |
| `src/main.rs` | `main.rs` |
| `src/front/task.rs` | `task.rs` |
| `src/front/service.rs` | `service.rs` |
| `src/backend.rs` | `backend.rs` |
| `src/bundle.rs` | `bundle.rs` |
| `src/engine.rs` | `engine.rs` |
| `src/pump.rs` | The output pump of `engine.rs` |

Besides the split into the front shim and the node service, they differ from upstream in these
ways:

- No task Pause or Resume.
- A task is stopped before Wait returns its exit, so a Delete right after Wait finds it stopped,
  and a SIGKILL or SIGTERM to a task that has not started stops it with status 128 plus the
  signal.
- Pod shares are under `/var/lib/containerd-shim-smolvm-pods`, and smolvm's state is under
  `SMOLVM_DATA_DIR`.
- Rosetta and TSO for amd64 pods, with the TSO canary.
- Pod networking on a node kernel without tc.
- The node service reaps only the VM processes, with smolvm's own reaper.
- smolvm's disk templates are expanded under a lock.
- The output pump has its own agent connection, writes the fifos without blocking, reattaches
  after a broken stream, pauses around each sleep without holding a thread, and marks output lost
  from the guest's journal.
- A process with stdin or a terminal is refused when it is created.
- A pod fails when its VM process exits outside a pause or a delete.
- A failed container create removes its copies from the pod share.
- The `delete` action deletes a VM only for a sandbox task, through the node service.

To port an upstream fix, diff the upstream file against v1.32.1 and apply the change to the
matching file here.

## Documentation

- [docs/getting-started.md](docs/getting-started.md): a first node and a first app, step by step.
- [docs/architecture.md](docs/architecture.md): how each part works and why.
- [docs/configuration.md](docs/configuration.md): settings, annotations, the API and `ctl`.
- [node/README.md](node/README.md): the node image, its layout, Docker, idle sleep settings, the
  API over TCP, capacity, cgroups and khugepaged.
- [node/kernel/README.md](node/kernel/README.md): the node kernel.
- [node/libkrun/README.md](node/libkrun/README.md): the node's libkrun build; the patches and
  what they do are in `patches/libkrun` of the smolvm checkout.
- [tests/e2e/README.md](tests/e2e/README.md): the end-to-end tests.
- [docs/design/smolvm-runtime-roadmap.md](../../docs/design/smolvm-runtime-roadmap.md): the work
  that is left, open issues and risks.
