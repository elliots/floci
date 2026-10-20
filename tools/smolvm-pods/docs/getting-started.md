# Getting started with smolvm-pods

This guide takes a Mac from nothing to a node VM that runs a pod in its own microVM, puts the pod
to sleep when nobody calls it, and wakes it on the next request. It then covers the node service's
API, the tests, the common problems and how to remove a node. How each part works is in
[architecture.md](architecture.md), and every setting, annotation and API route is in
[configuration.md](configuration.md).

Commands run from `tools/smolvm-pods` in the floci repository unless a step says otherwise.

- [What you need](#what-you-need)
- [Build](#build)
- [Start a node](#start-a-node)
- [Run a first app](#run-a-first-app)
- [Reach the node service's API from the Mac](#reach-the-node-services-api-from-the-mac)
- [Run the tests](#run-the-tests)
- [Common problems](#common-problems)
- [Where the logs are](#where-the-logs-are)
- [Stop and delete a node](#stop-and-delete-a-node)

## What you need

### A Mac that can nest VMs

The node is a smolvm VM started with `--nested`, and each pod VM is a KVM VM inside it.

- **Apple silicon, M3 or later.** smolvm's `--nested` needs an M3 or newer chip and macOS 15.
- **macOS 15.2 or later.** The smolvm release's `libkrunfw.5.dylib`, and the node kernel's that
  `node/kernel/build.sh` builds, are built for macOS 15.2.
- **Rosetta 2,** for amd64 images: `softwareupdate --install-rosetta --agree-to-license`.
  `node-vm.sh create` shares it with the node. On a Mac without it, smolvm starts the node without
  Rosetta and says nothing.
- **Memory.** The node VM takes 6 GiB by default, and the Mac gets it back only when the node
  stops. The end-to-end suites create nodes with 8 vCPUs and up to 16 GiB. Docker needs memory
  of its own for the builds (below).

### Tools

- **Docker, with the containerd image store.** Every build runs in a container. `node-vm.sh build`
  saves the pause and CoreDNS images and checks that each archive names its pinned multi-platform
  index digest, so Docker must keep image indexes: `docker info` must show the driver type
  `io.containerd.snapshotter.v1`. If it shows another, turn the store on: in Docker Desktop,
  Settings > General > Use containerd for pulling and storing images; for dockerd or OrbStack,
  `"features": {"containerd-snapshotter": true}` in the daemon config
  (`~/.orbstack/config/docker.json` for OrbStack), then restart Docker.
- **Docker's CPUs and memory.** The largest build containers ask for `--memory 12g`
  (`node/libkrun/build.sh`) and `--memory 6g` (`node/kernel/build.sh`); `scripts/build.sh` asks
  for `--memory 3g --cpus 4`, so Docker needs at least 4 CPUs. `node/kernel/build.sh` and
  `node/libkrun/build.sh` ask for 8 CPUs, which Docker refuses when its VM has fewer: pass
  `--jobs N` to the kernel build and `--cpus N` to the libkrun build.
- **git and Git LFS.** smolvm keeps its macOS libraries in `lib/` as Git LFS objects. Run
  `git lfs install` once after installing Git LFS. Without it, git sees the libraries that
  `git lfs pull` fetches as changed, and `scripts/build.sh` refuses the checkout.
- **The Xcode Command Line Tools,** for `cc` and `make`: `node/kernel/build.sh` compiles the
  kernel into a dylib on the Mac. It checks for `cc`, `curl`, `docker`, `git`, `make` and
  `shasum`.
- **Rust on the Mac** (rustup), to build the smolvm CLI and to run the end-to-end tests, and
  [cargo-make](https://github.com/sagiegurari/cargo-make) for smolvm's `cargo make dev`.
  `docs/DEVELOPMENT.md` in the smolvm checkout lists what smolvm's own build needs.
- **jq** on the Mac, which `node-vm.sh` uses. Recent macOS has `/usr/bin/jq`.
- **kubectl** on the Mac, if you want to use the node from the Mac directly. `node-vm.sh kubectl`
  runs the node's own kubectl without it.
- **The network.** The builds download crates, Rust, Debian packages, the kernel source, libkrun,
  libkrunfw, the smolvm releases and Docker images.

### Disk space

The builds and Docker's images below take about 15 GB. Each node's disks come on top: they can
grow to 48 GiB by default, a 40 GiB storage disk and an 8 GiB overlay. Plan for 60 GB or more free
for a trial with several apps.

- the release build of the binary in `target/release-small` (about 2 GB), the unit test builds in
  `target/debug` (several GB more), the libkrun build in `target/libkrun` (about 1 GB), and the
  node kernel, the Rust toolchain and the cargo registry in the smolvm checkout's `target/floci`
  (about 1.5 GB);
- the smolvm checkout's own builds: the CLI in `target/release` (about 1 GB) and the guest agent
  in `target/release-small` (about 1.5 GB);
- Docker's images and build cache: the Rust, Debian, Go, k3s and node base images;
- the node image tar in `target/node` (about 0.2 GB), the image's inputs and the smolvm Linux
  release beside it in `target/node/inputs` and `target/node/release` (about 0.3 GB), and
  smolvm's copy of the tar in `~/Library/Caches`;
- each node VM's disks. They are sparse files that grow as the node uses them, up to 40 GiB for
  the storage disk and 8 GiB for the overlay by default. Each sleeping pod has a checkpoint on
  the storage disk: up to about 1.3 GB for a pod that uses all of its 1 GiB of RAM. Every
  container also has a full copy of its image's root file system in its pod share, on the storage
  disk, for as long as it exists. For many or large images, give the node a larger storage disk
  with `node-vm.sh create --storage GIB`. The end-to-end suites create nodes of their own.

### The checkouts

You need two checkouts side by side: the floci repository, with this directory, and smolvm.

```text
src/
  floci/     the floci repository
  smolvm/    smolvm at the pinned commit
```

The scripts find smolvm at `../smolvm` from the floci repository root (`../../../smolvm` from
`tools/smolvm-pods`). To keep it elsewhere, export `SMOLVM_SRC=/path/to/smolvm` in every shell
that runs the scripts: `scripts/build.sh`, `node/libkrun/build.sh`, `node/kernel/build.sh` and
`scripts/node-vm.sh` all read it.

**The pinned commit.** `smolvm_commit` in [scripts/build.sh](../scripts/build.sh) pins smolvm to
`7d9e020c1edf088e513937a4d050521f66fa8e60`: smolvm v1.30.1 with the patches that smolvm-pods
needs ([Build](../README.md#build) lists them). The patch series is the smolvm branch
`floci/runtime-clean`, whose head is the pinned commit, and it is not published yet: ask the
maintainers for a bundle or a repository with that branch. Then, in the directory that holds the
floci checkout:

```sh
git clone https://github.com/smol-machines/smolvm.git smolvm
cd smolvm
git fetch SOURCE floci/runtime-clean    # SOURCE: the repository or bundle you were given
git switch --detach 7d9e020c1edf088e513937a4d050521f66fa8e60
git lfs pull
```

`scripts/build.sh` refuses a smolvm checkout at another commit, or with uncommitted changes to
tracked files.

**smolvm-floci.** The node VM runs under smolvm-floci: the smolvm CLI built from the checkout,
with the libraries of the smolvm v1.30.1 macOS release and the node kernel.
`node/kernel/build.sh` writes it, and first needs `release/`, `bin/smolvm` and `agent-rootfs/` in
the checkout's `target/floci`. In the smolvm checkout:

```sh
mkdir -p target/floci/release target/floci/bin
curl -fL -o target/floci/smolvm-1.30.1-darwin-arm64.tar.gz \
    https://github.com/smol-machines/smolvm/releases/download/v1.30.1/smolvm-1.30.1-darwin-arm64.tar.gz
tar -xzf target/floci/smolvm-1.30.1-darwin-arm64.tar.gz -C target/floci/release --strip-components 1
cargo make dev                          # builds and signs target/release/smolvm
cp target/release/smolvm target/floci/bin/smolvm
git restore lib/libkrun.dylib
ln -s ../release/storage-template.ext4.zst ../release/overlay-template.ext4.zst target/floci/bin/
cp -a target/floci/release/agent-rootfs target/floci/agent-rootfs
```

- The release is unpacked without its top directory, so that `checksums.txt` and `lib/` are
  directly in `release/`. `node/kernel/build.sh` checks `checksums.txt` against the SHA-256 that
  it pins.
- Without cargo-make, `cargo make dev` is these two commands:

  ```sh
  LIBKRUN_BUNDLE=$PWD/lib cargo build --release
  codesign --force --sign - --entitlements smolvm.entitlements target/release/smolvm
  ```

- The build rewrites the install name of `lib/libkrun.dylib`, and `scripts/build.sh` needs a
  clean checkout, so `git restore` puts the file back. smolvm-floci loads libkrun from
  `target/floci/kernel/lib`, not from `lib/`.
- The links put the release's compressed disk templates beside `bin/smolvm`, where the CLI finds
  them and expands them on first use.
- `agent-rootfs/` starts as the release's agent rootfs. [The guest agent](#the-guest-agent)
  replaces its agent with the patched one.

## Build

There are five builds. Each pins its inputs and stops on a mismatch.

| Build | Command | Result |
| --- | --- | --- |
| The runtime binary | `scripts/build.sh` | `target/release-small/containerd-shim-smolvm-pods-v2` |
| The patched libkrun | `node/libkrun/build.sh` | `target/libkrun/libkrun.so` |
| The node kernel | `node/kernel/build.sh` | `target/floci/kernel` in the smolvm checkout, with `bin/smolvm-floci` |
| The guest agent | `cargo build` in a `docker run`, in the smolvm checkout | `target/floci/agent-rootfs/usr/local/bin/smolvm-agent` in the smolvm checkout |
| The node image | `scripts/node-vm.sh build` | `target/node/node-image.tar` |

### The runtime binary

```sh
scripts/build.sh
```

It runs `cargo build --locked --profile release-small` for linux/arm64 in the
`rust:1.92-bookworm` image that `scripts/rust.env` pins, which Docker pulls on the first run. On
first use it installs Rust 1.99.1 into `target/floci/rustup` of the smolvm checkout, and keeps
the cargo registry in `target/floci/cargo-registry` there. Other arguments go to cargo instead.
The result is `target/release-small/containerd-shim-smolvm-pods-v2`.

### The patched libkrun

```sh
node/libkrun/build.sh
```

It fetches libkrun at the commit of the smolvm v1.30.1 Linux release, applies the two patches in
`node/libkrun`, and builds `libkrun.so` in the same Rust image. It never pulls that image, so
build the binary first. It takes about 3 minutes on an M4 Max, plus the first download of the
crates. It writes `target/libkrun/libkrun.so`, with `build-info` and `build.log` beside it. The
build is reproducible: its SHA-256 must be `LIBKRUN_SHA256` in `node/Dockerfile`. `--test` also
runs the patches' unit tests. [node/libkrun](../node/libkrun/README.md) explains the patches.

### The node kernel

```sh
node/kernel/build.sh
```

It builds smolvm's guest kernel, Linux 6.18.55, with transparent huge pages and conntrack netlink,
in a Debian container, and compiles it into `libkrunfw.5.dylib` on the Mac. It takes 3 to 5
minutes on an M4 Max, plus the first download of the 148 MiB kernel tarball. It installs into
`target/floci/kernel` of the smolvm checkout (`SMOLVM_PODS_KERNEL_DIR` moves it):

- `lib/`, smolvm's macOS libraries with this kernel;
- `bin/smolvm-floci`, which runs `target/floci/bin/smolvm` with `target/floci/agent-rootfs` and
  this `lib/`. `node-vm.sh` creates and starts the node with it;
- `build/`, with `build-info`, the kernel `Image` and `build.log`.

Without the node kernel, memory in the nested pod VMs is many times slower
([node/kernel](../node/kernel/README.md)).

### The guest agent

The pod VMs run smolvm's guest agent patched for smolvm-pods, built from the pinned commit.
`node/Dockerfile` pins it by checksum, `AGENT_SHA256`, and `node-vm.sh build` and smolvm-floci
both take it from `target/floci/agent-rootfs` of the smolvm checkout. Compare the one there with
the pin:

```sh
grep '^ARG AGENT_SHA256=' node/Dockerfile
shasum -a 256 "${SMOLVM_SRC:-../../../smolvm}"/target/floci/agent-rootfs/usr/local/bin/smolvm-agent
```

If they match, skip this step; the release's agent never does. No script builds the agent:
`cargo build --locked --profile release-small -p smolvm-agent` in the smolvm checkout, run in
`rust:1.90-alpine` (`rust@sha256:b4b54b176a74db7e5c68fdfe6029be39a02ccbcfe72b6e5a3e18e2c61b57ae26`)
with Rust 1.90.0 and `musl-dev`, gives the binary of `AGENT_SHA256`. It takes about a minute. In
the smolvm checkout:

```sh
docker run --rm -v "$PWD:/work" -w /work -e RUSTUP_TOOLCHAIN=1.90.0 \
    rust@sha256:b4b54b176a74db7e5c68fdfe6029be39a02ccbcfe72b6e5a3e18e2c61b57ae26 \
    sh -ec 'apk add musl-dev && cargo build --locked --profile release-small -p smolvm-agent'
shasum -a 256 target/release-small/smolvm-agent
install -m 0755 target/release-small/smolvm-agent target/floci/agent-rootfs/usr/local/bin/smolvm-agent
```

Mount the checkout at `/work`: the binary records its source paths, so another path gives
another checksum. `RUSTUP_TOOLCHAIN` keeps the image's Rust 1.90.0, which the checkout's
`rust-toolchain.toml` would otherwise replace with the latest stable Rust.
`node-vm.sh build --agent PATH` takes an agent from another path for the image.

### The node image

`node-vm.sh build` never pulls an image: Docker must already have the base image of
`node/Dockerfile`, `rancher/k3s`, pause, CoreDNS and the Go image of the end-to-end tests, at the
digests that `node/Dockerfile`, `scripts/node-vm.sh` and `tests/e2e/workloads/go-image` pin
([node/README.md](../node/README.md#inputs)). Pull them once:

```sh
docker pull "$(sed -n 's/^FROM //p' node/Dockerfile)"
docker pull "$(sed -n 's/^k3s_image=//p' scripts/node-vm.sh)"
docker pull rancher/mirrored-pause:3.6
docker pull rancher/mirrored-coredns-coredns:1.13.1
docker pull "$(cat tests/e2e/workloads/go-image)"
```

Then build the image:

```sh
scripts/node-vm.sh build
```

It takes the binary, the agent and libkrun from their default paths (`--shim`, `--agent` and
`--libkrun` name others), and records the node kernel's checksum, so it needs every build above.
On the first run it downloads the smolvm v1.30.1 Linux release into `target/node/release`. It
builds the litmus test of the TSO canary with the Go image, builds the image with Docker, saves
it as `target/node/node-image.tar` (`--out`) and removes it from Docker, unless `--tag TAG` keeps
it. `node/Dockerfile` checks every input against its checksum. `/etc/floci-node/build-info` in
the node lists them, with the binary's smolvm commit.

## Start a node

```sh
scripts/node-vm.sh create -- --port 17070:7070
scripts/node-vm.sh start
```

`create` makes the node VM `smolvm-pods-node` from `target/node/node-image.tar` with 4 vCPUs,
6 GiB of memory, a 40 GiB storage disk, an 8 GiB overlay disk and Rosetta. It publishes the
Kubernetes API on the Mac's `127.0.0.1:6443` and NodePorts 30080 to 30099 on the same Mac ports.
The arguments after `--` go to `smolvm machine create`: `--port 17070:7070` also publishes the
node service's API, port 7070 in the node, on the Mac's port 17070, for
[the API from the Mac](#reach-the-node-services-api-from-the-mac), which also shows how to add the
port to a node made without it. `scripts/node-vm.sh help` lists the options: `--image`, `--cpus`,
`--mem` (MiB), `--storage`, `--overlay` (GiB), `--api-port`, `--node-ports`, `--volume`,
`--no-rosetta` and `--name`.

`start` boots the node and waits, for up to 300 seconds (`--timeout`), until the Kubernetes API
answers, the node is Ready and the `smolvm-pods` RuntimeClass exists. On an M4 Max a node is Ready
about 10 seconds after it starts. `start` ends with:

```text
==> Ready: the API is at https://127.0.0.1:6443 (see node-vm.sh kubeconfig)
```

If the node does not boot the node kernel, `start` warns that the node kernel has no transparent
huge pages with the policy always.

`node-vm.sh exec -- COMMAND` runs a command in the node, as root, with the node's environment,
which sets up kubectl, `crictl` and `ctr` for k3s. `node-vm.sh kubectl ARGS` runs the node's
kubectl. Both take `-i` to pass stdin. Check the node:

```sh
scripts/node-vm.sh kubectl get nodes
scripts/node-vm.sh kubectl get runtimeclass smolvm-pods
scripts/node-vm.sh exec -- smolvm-pods ctl list        # {"pods":[]} until a VM pod runs
scripts/node-vm.sh exec -- cat /etc/floci-node/build-info
```

**kubectl on the Mac.** `node-vm.sh kubeconfig` prints a kubeconfig for the published API port:

```sh
mkdir -p ~/.kube
scripts/node-vm.sh kubeconfig >~/.kube/smolvm-pods-node.yaml
export KUBECONFIG=~/.kube/smolvm-pods-node.yaml
kubectl get pods -A
```

**More than one node.** Give each node its own `--name` and its own Mac ports with `--api-port`,
`--node-ports` and the port mapped to 7070. Every command but `build` then takes `--name NAME`,
or reads `SMOLVM_PODS_NODE`.

## Run a first app

This runs nginx in a VM pod that sleeps after 30 seconds without requests, with a NodePort that
the Mac reaches on port 30080.

### Import the image

The node pulls no image from a registry: its `registries.yaml` sends every registry to an
endpoint that refuses connections. Pull the image on the Mac and import it into the node's
containerd:

```sh
docker pull --platform linux/arm64 nginx:1.27
docker save nginx:1.27 |
    scripts/node-vm.sh exec -i -- ctr -n k8s.io images import --platform linux/arm64 -
scripts/node-vm.sh exec -- crictl images
```

Give the pod a tag other than `latest`: for `latest`, or no tag, kubelet's default pull policy is
`Always`, and the pull fails.

The import names the image only by its tag, `docker.io/library/nginx:1.27`. A pod that names an
image by digest, as `nginx@sha256:...` or `nginx:1.27@sha256:...` (Helm charts and pinned
manifests often do), needs that digest name in the node too, or kubelet tries to pull the image
and fails. `docker save` without `--platform` keeps the registry's image index, whose digest is the
one that such references use; with the containerd image store, `docker image inspect` prints it as
the image's id. Add the name:

```sh
digest=$(docker image inspect --format '{{.Id}}' nginx:1.27)
scripts/node-vm.sh exec -- ctr -n k8s.io images tag docker.io/library/nginx:1.27 \
    "docker.io/library/nginx@$digest"
```

On this arm64 node, amd64 images run under Rosetta. Import one as a single-platform amd64 image,
`docker save --platform linux/amd64 IMAGE`, imported with `--platform linux/amd64`, and name it by
its tag ([Known limits](architecture.md#known-limits)).

### Create the pod

```sh
scripts/node-vm.sh kubectl -i -- apply -f - <<'EOF'
apiVersion: v1
kind: Pod
metadata:
  name: web
  labels:
    app: web
  annotations:
    smolvm-pods.floci.io/idle-timeout: "30s"
spec:
  runtimeClassName: smolvm-pods
  containers:
    - name: web
      image: nginx:1.27
      ports:
        - containerPort: 80
      readinessProbe:
        httpGet:
          path: /
          port: 80
---
apiVersion: v1
kind: Service
metadata:
  name: web
spec:
  type: NodePort
  selector:
    app: web
  ports:
    - port: 80
      nodePort: 30080
EOF
scripts/node-vm.sh kubectl wait --for=condition=Ready pod/web --timeout=120s
scripts/node-vm.sh exec -- smolvm-pods ctl get default/web
```

- `runtimeClassName: smolvm-pods` runs the pod in its own VM. Without it, the pod runs in runc,
  the node's default runtime, and never sleeps.
- `smolvm-pods.floci.io/idle-timeout` sets this pod's idle timeout, from `10s` to `24h`. The
  node's default is 5 minutes. [Pod annotations](configuration.md#pod-annotations) lists the
  others, such as `smolvm-pods.floci.io/hibernate: "false"`, which keeps a pod from sleeping.
- The readiness probe shows that kubelet's probes do not keep a pod awake: while it sleeps, the
  node service answers them with the app's last result.
- Every pod VM has 2 vCPUs and 1024 MiB of RAM, whatever the pod requests. On an Apple silicon
  node every pod VM gets Rosetta by default (`"rosetta": true`), once the node's TSO canary has
  passed ([amd64 pods](architecture.md#amd64-pods)).
- The first VM pod after a node starts waits for the node to expand smolvm's disk templates,
  about 18 seconds after boot, and to run its TSO canary.

`ctl get` prints the pod's record as JSON: `state` is `awake` and `vm_pid` is the VM process. A
pod is `NAMESPACE/NAME`, its CRI sandbox id, `uid:UID`, `ip:IP` or `netns:PATH`
([smolvm-pods ctl](configuration.md#smolvm-pods-ctl)).

### Watch it fall asleep

A pod sleeps by itself when no packet has passed on a connection opened to it for its idle
timeout, kubelet's probes aside, its VM used no more than 0.5 vCPU over the last minute, it is
Ready and its probes pass ([The idle rule](architecture.md#the-idle-rule)). The node service reads
conntrack every 15 seconds, so with a 30 s timeout the pause starts 30 to 45 seconds after the
last request, and takes about half a second. Watch it, and stop with Ctrl-C:

```sh
while sleep 5; do
    scripts/node-vm.sh exec -- smolvm-pods ctl get default/web | jq -r '"\(.state) vm_pid=\(.vm_pid)"'
done
```

The state goes from `awake` to `pausing` to `asleep`, and `vm_pid` becomes `null`: the VM's memory
is in a checkpoint on the node's disk and its process has exited. Kubernetes sees no change:

```sh
scripts/node-vm.sh kubectl get pod web -o wide         # Running, 1/1 Ready, the same IP
```

The events record every state change:

```sh
scripts/node-vm.sh exec -- smolvm-pods ctl events |
    jq -c '{seq, time, from, to, reason, reason_detail, duration_ms, error}'
```

An idle sleep is `awake` to `pausing` and `pausing` to `asleep`, both with the reason `idle`. A
pod that is idle but may not sleep has an event from `awake` to `awake` with the reason `idle` and
the error `not sleeping: REASON`.

### Wake it with a request

From the Mac, through the NodePort:

```sh
curl -s -o /dev/null -w '%{http_code} %{time_total}s\n' http://127.0.0.1:30080/
```

It answers 200. The first connection woke the pod: its packets were held while the VM resumed,
in about 0.2 seconds, then reached nginx unchanged. The events show `asleep` to `waking` and
`waking` to `awake` with the reason `traffic` and a `reason_detail` with the caller and port 80.
`ctl get` shows the pod `awake` with a new `vm_pid`; its IP, containers and memory are the same.
After another 30 to 45 seconds without requests, it sleeps again.

### Call it from another pod

A client pod in runc, with busybox for `wget`:

```sh
docker pull --platform linux/arm64 busybox:1.36
docker save busybox:1.36 |
    scripts/node-vm.sh exec -i -- ctr -n k8s.io images import --platform linux/arm64 -
scripts/node-vm.sh kubectl run client --image=busybox:1.36 --restart=Never -- sleep 86400
scripts/node-vm.sh kubectl wait --for=condition=Ready pod/client --timeout=60s
```

Once `ctl get default/web` says `asleep`, call it through its Service:

```sh
scripts/node-vm.sh kubectl exec client -- wget -qO- http://web
```

The answer is nginx's page, and the wake's `reason_detail` names `default/client` and port 80. A
client in a VM pod works the same way. Only connections opened to a pod wake it or keep it awake;
the pod's own connections to others do neither, and what arrives on them while it sleeps waits
for its next wake ([Only connections opened to the pod count](architecture.md#only-connections-opened-to-the-pod-count)).

### Sleep and wake it by hand

```sh
scripts/node-vm.sh exec -- smolvm-pods ctl hibernate default/web
scripts/node-vm.sh exec -- smolvm-pods ctl wake default/web
scripts/node-vm.sh exec -- smolvm-pods ctl hold default/web --lease demo --ttl 60
scripts/node-vm.sh exec -- smolvm-pods ctl release default/web --lease demo
```

- `hibernate` returns once the pod is asleep, with the pause's `duration_ms` and
  `checkpoint_bytes`, or fails with the code `refused` and its `reasons`, such as `lease held`.
- `wake` returns once the pod is awake. `resumed` is true when this call ran the resume.
- `hold` takes a lease of at most 60 seconds, which keeps the pod from sleeping but does not
  wake it; holding the same lease again renews it. `release` drops it.
- A pod with probes that never slept by itself has no probe results to answer kubelet with, so
  after a `hibernate` kubelet's next probe wakes it. Let a pod sleep by itself once first.

`kubectl exec web -- nginx -v` wakes a sleeping pod, with the reason `exec`. `kubectl logs web`
works while it sleeps.

### Your own app

- Add `runtimeClassName: smolvm-pods` to the pod spec, and the annotations to the pod's metadata.
  In a Deployment, both go in the pod template, `spec.template`.
- Import every image the pods use, as above, and add a digest name for each image that a
  manifest names by digest.
- Reach pods through Services, or from the Mac through a NodePort Service whose `nodePort` is
  one of 30080 to 30099, the ports that `node-vm.sh create` publishes (`--node-ports` changes
  them). `kubectl port-forward` does not work for VM pods.
- A pod reaches a server on the Mac's loopback, such as Floci, at the node's default gateway,
  smolvm's gateway (100.96.0.1 by default; `scripts/node-vm.sh exec -- ip -4 route show default`
  shows it), over TCP only: for example `http://100.96.0.1:4566`
  ([Networking](architecture.md#networking)). Start Floci with
  `FLOCI_BASE_URL=http://100.96.0.1:4566`, so that the URLs it returns, such as SQS `QueueUrl`
  and pre-signed URLs, work from pods. Clients on the Mac itself then get URLs they cannot reach,
  so give those clients an explicit endpoint.
- The node has no default StorageClass, no Ingress controller, no LoadBalancer and no
  metrics-server: `node/rootfs/etc/rancher/k3s/config.yaml` turns off k3s's local-storage,
  Traefik, ServiceLB and metrics-server. A PersistentVolumeClaim stays Pending, a LoadBalancer
  Service gets no external address, an Ingress does nothing and `kubectl top` has no metrics.
  Reach apps through NodePort Services.
- A pod that only consumes from SQS, SNS or a message broker gets no incoming connection: its
  polls and broker connections are its own. So it falls asleep, and no message wakes it. Give it
  `smolvm-pods.floci.io/hibernate: "false"`, or wake it, with a lease, through
  [the API](#reach-the-node-services-api-from-the-mac) when work is waiting.
- UDP never wakes a pod, and pod networking is IPv4 only.
- Each container gets its own copy of its volumes when it is created: containers do not share an
  emptyDir, an emptyDir starts empty again when its container restarts, and ConfigMap, Secret and
  service account token volumes do not update. Every file of a copy is readable by all users, so
  a Secret's `defaultMode` is not kept, and programs that refuse a loosely permissioned key, such
  as PostgreSQL's `ssl_key_file` or OpenSSH, fail ([Known limits](architecture.md#known-limits)).
- Size the node for the pods that are awake at the same time. An awake pod can take about 1.1 GiB
  of the node's memory, up to 1 GiB of guest RAM and about 100 MiB for its VM process, and a pod
  that wakes has all the memory it used resident at once. The default node has 6 GiB, about
  5.2 GiB of it free, so more than four memory-heavy pods awake at once start the node's OOM
  killer, which kills a pod's VM: the pod fails, and kubelet recreates it with its memory lost.
  Give the node more memory with `node-vm.sh create --mem`, and cap the awake pods, which nothing
  does by default: set `FLOCI_NODE_MAX_AWAKE_PODS` or `FLOCI_NODE_MAX_AWAKE_MEMORY`, such as
  `FLOCI_NODE_MAX_AWAKE_PODS=4` on the default node
  ([Wake admission](../node/README.md#wake-admission)). A wake over a cap waits up to 10 s for
  room, then is refused.
- `kubectl exec -i` and `-t`, `kubectl attach`, `kubectl cp` into a pod, and containers with
  `stdin: true` or `tty: true` are refused. So are pods with `hostNetwork`.
- A sleeping pod's peers may close its outbound connections, such as database pools and broker
  connections, and its own timers stop until it wakes. The app should reconnect after a wake.
- Connection pools and TCP keepalives to an awake pod keep it awake.
- A restart of the node VM loses every pod's memory and processes, awake or asleep, and kubelet
  then starts the pods again from scratch.

[Known limits](architecture.md#known-limits) has the full list.

## Reach the node service's API from the Mac

The node service serves its API on a Unix socket in the node, which `smolvm-pods ctl` uses, and on
TCP port 7070 of every node address, with a bearer token in `/etc/floci-node/api-token`. The TCP
API is for programs outside the node, such as Floci, to wake pods and keep them awake. The
`create` above published it on the Mac's port 17070:

```sh
token=$(scripts/node-vm.sh exec -- cat /etc/floci-node/api-token)
api=http://127.0.0.1:17070
curl -s -H "Authorization: Bearer $token" $api/v1/pods | jq -r '.pods[] | "\(.namespace)/\(.name) \(.state)"'
curl -s -H "Authorization: Bearer $token" $api/v1/pods/default%2Fweb | jq .state
curl -s -X POST -H "Authorization: Bearer $token" -d '{"source": "mac", "lease_ttl": 60}' \
    $api/v1/pods/default%2Fweb/wake | jq '{resumed, duration_ms, lease}'
curl -sN -H "Authorization: Bearer $token" "$api/v1/events?follow=true&cursor=0"
```

- A pod id is percent-encoded as one path segment: `default/web` is `default%2Fweb`.
- A wake with `lease_ttl` also holds a new lease for that many seconds, at most 60, and returns
  it. Renew it with `PATCH /v1/pods/ID/leases/LEASE` and `{"ttl_secs": 60}` before it expires.
- `follow=true` streams every event after the cursor, one JSON object per line, then each new one,
  until you stop curl.
- A request without the token, or with another, gets 401. There is no TLS.

The node publishes only the ports in its machine record. To add the mapping to a node made without
`-- --port 17070:7070`:

```sh
"${SMOLVM_SRC:-../../../smolvm}"/target/floci/kernel/bin/smolvm-floci \
    machine update --name "${SMOLVM_PODS_NODE:-smolvm-pods-node}" --port 17070:7070
```

On a running node, it adds the port at once and keeps the pods; on a stopped node, the port is
published from its next start.

[The API](configuration.md#the-api) lists every route, and
[node/README.md](../node/README.md#the-node-services-api-over-tcp) the token and the listener.

## Run the tests

### Unit tests

```sh
scripts/build.sh test --locked
scripts/build.sh clippy --locked --all-targets -- -D warnings
scripts/build.sh fmt --check
```

They run in the Linux build container and boot no VM. Tests that need root, network namespaces or
a node are ignored by default; [Tests](../README.md#tests) shows how to run them.

### End-to-end tests

The end-to-end tests run on the Mac, with the Mac's Rust. Their harness has tests of its own that
need no node:

```sh
cargo test --locked --manifest-path tests/e2e/Cargo.toml
```

The suites (`pods`, `network`, `hibernation`, `wake` and `scale`) create node VMs and run pods
through kubelet. The node image must hold the binary under test, and the `pods` suite needs the
binary's unit test build:

```sh
scripts/build.sh
scripts/build.sh test --locked --profile release-small --no-run
#   prints the test binary: target/release-small/deps/containerd_shim_smolvm_pods_v2-<hash>
scripts/node-vm.sh build
SMOLVM_PODS_E2E=1 \
SMOLVM_PODS_E2E_NETNS_TEST=$PWD/target/release-small/deps/containerd_shim_smolvm_pods_v2-<hash> \
cargo test --locked --manifest-path tests/e2e/Cargo.toml -- --ignored --nocapture
```

This runs every suite, one after the other. Each creates a node named `floci-SUITE-e2e-ID` with
8 vCPUs (8 GiB for `hibernation` and `wake`, 16 GiB for `scale`), its API and a NodePort on free
ports, and deletes it at the end. To run one suite, name it with `--test`:

```sh
SMOLVM_PODS_E2E=1 \
cargo test --locked --manifest-path tests/e2e/Cargo.toml --test wake -- --ignored --nocapture
```

`SMOLVM_PODS_E2E_KEEP=1` keeps the node for debugging. An interrupted run can leave a node behind:
delete it with `SMOLVM_PODS_NODE=floci-SUITE-e2e-ID scripts/node-vm.sh delete`.
[tests/e2e](../tests/e2e/README.md) describes every suite and setting.

### The resume check

`bench/resume/check.sh` checks smolvm's own pause and resume in a fresh node VM, without the
runtime. [The resume check](../README.md#tests) lists what it needs.

## Common problems

### Builds

| Message or symptom | What to do |
| --- | --- |
| `build.sh: DIR is at COMMIT, not at the pinned smolvm commit ...` | Check out the pinned commit in the smolvm checkout, or point `SMOLVM_SRC` at one that has it |
| `build.sh: DIR has uncommitted changes` | `git -C DIR status`, then commit or restore the changes. A changed `lib/*.dylib` comes from a missing `git lfs install`, or from the smolvm build ([The checkouts](#the-checkouts)) |
| `docker run` refuses `--cpus 8` (kernel or libkrun build) because only N CPUs are available | Docker's VM has fewer than 8 CPUs: `node/kernel/build.sh --jobs N` or `node/libkrun/build.sh --cpus N` |
| `rust@sha256:... is not in the local Docker image store, and this script never pulls` (libkrun) | Run `scripts/build.sh` first, or `docker pull "$(sed -n 's/^rust_image=//p' scripts/rust.env)"` |
| `build.sh: PATH is missing; smolvm-floci must be built first` (kernel), for `checksums.txt`, `bin/smolvm` or `agent-rootfs` | Prepare `target/floci` in the smolvm checkout ([The checkouts](#the-checkouts)) |
| `build.sh: .../checksums.txt is not the pinned smolvm v1.30.1 release's` | Unpack the smolvm v1.30.1 darwin-arm64 release again, without its top directory |
| A build container dies of memory pressure: exit status 137, or cargo reports `SIGKILL` | Give Docker more memory, or wait for other builds to finish, and run it again. `scripts/build.sh build --locked --profile release-small -j 2` builds with fewer jobs |
| `node-vm.sh: IMAGE is not in the local Docker image store, and this script never pulls` | `docker pull` that image ([The node image](#the-node-image)) |
| `node-vm.sh: the cached IMAGE is ID, expected DIGEST` | The cached tag is another image: `docker image rm IMAGE`, then pull it again |
| `node-vm.sh: ARCHIVE does not hold DIGEST intact` | Docker saved the image without its index: use the containerd image store |
| `node-vm.sh: shim not found`, `guest agent not found` or `libkrun not found` | Run that build, or name the file with `--shim`, `--agent` or `--libkrun` |
| `node-vm.sh: no node kernel in DIR/build/build-info: run node/kernel/build.sh` | Build the node kernel |
| The image build stops at a `sha256sum --check` with `FAILED` | An input differs from its pin in `node/Dockerfile`, usually the guest agent (`AGENT_SHA256`) or libkrun (`LIBKRUN_SHA256`) |

### The node

| Message or symptom | What to do |
| --- | --- |
| `node-vm.sh: smolvm CLI not found: PATH` | Build the node kernel, which writes `bin/smolvm-floci`, or set `SMOLVM` |
| `node-vm.sh: image tar not found: PATH (run node-vm.sh build)` | Build the node image |
| `node-vm.sh: machine NAME already exists` | `scripts/node-vm.sh delete`, or create another with `--name` |
| `node-vm.sh: machine NAME does not exist (run node-vm.sh create)` | Create it, or name an existing node with `--name` or `SMOLVM_PODS_NODE` |
| `create` fails because a Mac port is taken, such as 6443 by another Kubernetes | Choose free ports with `--api-port`, `--node-ports` and the mapping for 7070 |
| `node-vm.sh: timed out waiting for ...` | Read `/var/log/k3s.log` in the node and floci-node's log ([Where the logs are](#where-the-logs-are)) |
| `start` warns that the node kernel has no transparent huge pages | The node runs the stock kernel: build the node kernel, and check `SMOLVM` and `SMOLVM_PODS_KERNEL_DIR` |
| The node stops by itself, with `... which the capacity view does not allow ...` in floci-node's log | A k3s setting gives kubelet a memory eviction threshold, or a CPU or memory manager, which floci-node refuses ([Capacity](../node/README.md#capacity)) |
| `smolvm-pods ctl` exits with status 3 | The node service is not answering. floci-node starts it again 1 second after it exits; read its log |

### Pods

| Message or symptom | What to do |
| --- | --- |
| `ErrImagePull`, `ImagePullBackOff` or `ErrImageNeverPull` | The image is not imported, its tag is `latest`, or the pod names it by a digest that the node has no name for. Import it, use a tag other than `latest` or `imagePullPolicy: IfNotPresent`, and for a digest add the digest name ([Import the image](#import-the-image)) |
| The pod is refused when it is created | `kubectl describe pod` shows why: `hostNetwork`, `stdin` or `tty`, an amd64 image with `smolvm-pods.floci.io/rosetta: "false"`, or `rosetta: "true"` on a node whose TSO canary failed |
| amd64 pods are refused, or the node service warns that a pod runs without Rosetta | Install Rosetta 2 on the Mac, then stop and start the node: smolvm checks for Rosetta each time the node starts |
| `kubectl exec -it` fails | The guest agent passes no stdin: run `kubectl exec` without `-i` and `-t` |
| A pod that does not name the RuntimeClass never sleeps | It runs in runc: add `runtimeClassName: smolvm-pods` |

### Sleep and wake

| Message or symptom | What to do |
| --- | --- |
| The pod never sleeps, and `ctl events` has `not sleeping: REASON` | The reason says what to fix: the pod is not Ready, its probes are not recorded yet, or a probe fails. A lease, an exec or `hibernate: "false"` also keeps it awake |
| The pod never sleeps, with no such event | It still has activity: traffic on a connection to it, such as a keepalive or a connection pool, or VM CPU above 0.5 vCPU averaged over 60 seconds. `smolvm-pods.floci.io/cpu-threshold: "off"` lets a busy pod sleep |
| A probe wakes the pod right after `ctl hibernate` | It never slept by itself, so it has no probe results: let it sleep by itself once |
| A request to a sleeping pod fails | `ctl events` says why: `asleep` to `asleep` with `wake refused` when admission refused the wake (only with a cap, `FLOCI_NODE_MAX_AWAKE_PODS` or `FLOCI_NODE_MAX_AWAKE_MEMORY`), or `waking` to `asleep` with an error when the restore failed. The third failed restore in a row fails the pod (`waking` to `failed`), and kubelet recreates it |

## Where the logs are

In the node, read them with `scripts/node-vm.sh exec -- tail -n 100 FILE`:

| Path in the node | What |
| --- | --- |
| `/var/log/smolvm-pods/node-service.log` | The node service: pod VMs, sleeps, wakes, the idle loop and warnings, and a line from floci-node at each restart. Not rotated |
| `/var/log/smolvm-pods/front-shim.log` | The front shim, which containerd starts. Not rotated |
| `/var/log/k3s.log` | k3s and kubelet, rotated at 50 MB |
| `/var/lib/rancher/k3s/agent/containerd/containerd.log` | containerd |
| `/var/lib/containerd-shim-smolvm-pods/.node/events.jsonl` | The events that `ctl events` prints, rotated at 8 MiB |
| `/etc/floci-node/build-info` | The checksums of what the node runs, and the binary's smolvm commit |

`RUST_LOG` in `/etc/floci-node/env` sets the log level of the node service and the front shim
(`info` by default); the node service takes a change when the node restarts.

floci-node, which prepares the node and runs k3s and the node service, writes to the node VM's
own log, on the Mac. It reports the capacity that kubelet sees, khugepaged, whether it created the
API token, and why it stopped the node. Read it with smolvm-floci, the default `SMOLVM`:

```sh
"${SMOLVM_SRC:-../../../smolvm}"/target/floci/kernel/bin/smolvm-floci \
    machine logs --name smolvm-pods-node
```

`machine logs` takes `-f` to follow and `--tail N`. The builds keep their own logs:
`target/libkrun/build.log`, and `target/floci/kernel/build/build.log` in the smolvm checkout.

## Stop and delete a node

```sh
scripts/node-vm.sh stop
scripts/node-vm.sh start
scripts/node-vm.sh delete
```

- `stop` stops the node VM, which loses every pod's memory and processes, awake or asleep. `start`
  boots it again, and kubelet then starts the pods again from scratch.
- `delete` stops the node if it runs and deletes it with its disks. It also removes smolvm's copy
  of the image and the seed disk that the node's storage disk was cloned from, when no other node
  uses them. Do not run it while a node is being created from the same image tar.
- A node keeps the image it was created from. To run a new build, build the binary and the image
  again, then delete the node, create it again and import your images again: they are on the
  node's storage disk, which `delete` removes.

This lists every node VM, including any that an interrupted end-to-end run left behind:

```sh
"${SMOLVM_SRC:-../../../smolvm}"/target/floci/kernel/bin/smolvm-floci machine ls
```
