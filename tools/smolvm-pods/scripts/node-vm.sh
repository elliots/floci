#!/usr/bin/env bash
# Builds the smolvm-pods node image and runs it as a nested smolvm VM on a Mac.
# Run "node-vm.sh help" for the commands and their options.
set -euo pipefail

scripts_dir=$(cd "$(dirname "$0")" && pwd)
pods_dir=$(dirname "$scripts_dir")
node_dir=$pods_dir/node
smolvm_src=${SMOLVM_SRC:-$(cd "$pods_dir/../../.." && pwd)/smolvm}
# The node VM boots the kernel in the lib dir of the smolvm CLI that starts it:
# by default the node kernel with transparent huge pages (node/kernel/build.sh).
kernel_dir=${SMOLVM_PODS_KERNEL_DIR:-$smolvm_src/target/floci/kernel}
smolvm=${SMOLVM:-$kernel_dir/bin/smolvm-floci}
work_dir=${SMOLVM_PODS_WORK_DIR:-$pods_dir/target/node}
name=${SMOLVM_PODS_NODE:-smolvm-pods-node}
default_shim=${SMOLVM_PODS_SHIM:-$pods_dir/target/release-small/containerd-shim-smolvm-pods-v2}
default_agent=$smolvm_src/target/floci/agent-rootfs/usr/local/bin/smolvm-agent
default_libkrun=$pods_dir/target/libkrun/libkrun.so
default_pod_kernel=${SMOLVM_PODS_POD_KERNEL_DIR:-$pods_dir/target/podkernel}/libkrunfw.so.5.5.0

# The build takes files from these images and never pulls them.
k3s_image=rancher/k3s@sha256:10464930d9bad0c06aef9830e84cd4019c24ed44d5eab594efb7416119097248
# Builds the litmus test of the shim's TSO canary, as the end-to-end tests do.
workloads_dir=$pods_dir/tests/e2e/workloads
go_image=$(cat "$workloads_dir/go-image")
# The images k3s runs at startup. The node imports them, so it needs no registry.
system_images=(
    rancher/mirrored-pause:3.6@sha256:74c4244427b7312c5b901fe0f67cbc53683d06f4f24c6faee65d4182bf0fa893
    rancher/mirrored-coredns-coredns:1.13.1@sha256:9b9128672209474da07c91439bf15ed704ae05ad918dd6454e5b6ae14e35fee6
    rancher/mirrored-metrics-server:v0.8.0@sha256:89258156d0e9af60403eafd44da9676fd66f600c7934d468ccc17e42b199aee2
)
# The smolvm release that node/Dockerfile pins, smolvm-VERSION-linux-arm64.
release_name=$(sed -n 's/^ARG SMOLVM_RELEASE=//p' "$node_dir/Dockerfile")
release_version=${release_name#smolvm-}
release_url=https://github.com/smol-machines/smolvm/releases/download/v${release_version%-linux-arm64}
# Docker's static release that node/Dockerfile pins and checks.
docker_version=$(sed -n 's/^ARG DOCKER_VERSION=//p' "$node_dir/Dockerfile")
docker_url=https://download.docker.com/linux/static/stable/aarch64/docker-$docker_version.tgz

# Node paths kept on the node's ext4 storage disk instead of its overlay root:
# k3s and containerd, kubelet, smolvm's data, the shim's pod shares, the logs
# (pod logs too, which kubelet's eviction then sees), and CNI's IP leases.
volumes=(/var/lib/rancher /var/lib/kubelet /var/lib/smolvm /var/lib/containerd-shim-smolvm-pods
    /var/log /var/lib/cni)

# smolvm's caches on the Mac, which a machine delete leaves.
image_archives=$HOME/Library/Caches/smolvm-image-archives
vms_dir=$HOME/Library/Caches/smolvm/vms
image_seeds=$HOME/Library/Caches/smolvm/image-seeds
# Every create, and the cache cleanup of every delete, holds this lock.
lock_file=$HOME/Library/Caches/smolvm-pods/node-vm.lock

# The node's mount of a seed directory, and the label key of every node VM.
seed_mount=/etc/floci-node/seed
node_label='smolvm-pods-node'
# An isolated node may resolve only names under this domain, which has none.
closed_names='*.closed.floci.invalid'

usage() {
    cat <<EOF
Usage: node-vm.sh COMMAND [OPTIONS]

Builds the smolvm-pods node image and runs it as a nested smolvm VM.

Commands:
  build        Build the node image and save it as a tar.
  create       Create the node VM from the image tar.
  start        Start the node VM and wait until Kubernetes is ready.
  stop         Stop the node VM.
  delete       Delete the node VM and its disks, and smolvm's cached copies of
               its image when no other machine uses them.
  update       Change the allow list of an isolated node VM, also while it runs.
  exec         Run a command in the node: exec [-i] [-t] [--stream] [--] COMMAND [ARG...]
  kubectl      Run kubectl in the node: kubectl [-i] [-t] [--stream] [--] [ARG...]
               --stream prints the output as it arrives. Without it a command's output comes
               when it ends, so one that does not end (kubectl logs -f) prints nothing.
  kubeconfig   Print a kubeconfig that reaches the node's API from the Mac.
  token        Print the bearer token of the node service's API.
  docker-env   Write a Docker CLI config that sends that token, and print the exports that
               point docker at the node's dockerd through the node service's API:
               eval "$(node-vm.sh docker-env)". Needs a node made with --service-port.
  status       List every node VM that create made, with its state and labels.

Every command except build and status takes --name NAME (default: $name).

build options:
  --shim PATH          The smolvm-pods shim, a Linux arm64 binary
                       (default: $default_shim)
  --agent PATH         The patched guest agent (default: $default_agent)
  --libkrun PATH       The patched libkrun.so of node/libkrun/build.sh
                       (default: $default_libkrun)
  --pod-kernel PATH    The pod VMs' libkrunfw.so of node/podkernel/build.sh
                       (default: $default_pod_kernel)
  --release-dir DIR    Directory with $release_name.tar.gz, checksums.sha256
                       and Docker's docker-$docker_version.tgz, downloaded into it
                       when missing (default: WORK/release)
  --tag TAG            Keep the image in Docker with this tag (default: remove
                       the image once it is saved)
  --out PATH           Image tar to write (default: WORK/node-image.tar)

create options:
  --image PATH         Image tar (default: WORK/node-image.tar)
  --cpus N             vCPUs (default: 4)
  --mem MIB            Memory in MiB (default: 6144)
  --storage GIB        Disk for the image and the node's volumes (default: 40)
  --overlay GIB        Disk for changes to the node's root file system (default: 8)
  --api-port PORT      Mac port for the Kubernetes API (default: 6443)
  --service-port PORT  Mac port for the node service's API, port 7070 in the node
                       (default: not published)
  --node-ports RANGE   NodePorts published on the same Mac ports: FIRST-LAST,
                       PORT, or none (default: 32080-32099)
  --seed DIR           Mount DIR read-only at $seed_mount
  --isolate            Limit the node's egress to smolvm's gateway, whose TCP
                       reaches the Mac's loopback, and to the --allow-cidr ranges,
                       and make smolvm's DNS answer no name. Only a new node can
                       change this. Not with smolvm's --network
  --allow-cidr CIDR    Also allow egress to CIDR (repeatable, with --isolate)
  --volume HOST:GUEST[:ro]
                       Share a Mac directory with the node (repeatable)
  --no-rosetta         Do not share Rosetta with the node, so amd64 pods cannot run
  -- ARG...            Pass the remaining arguments to smolvm machine create

The node VM's labels record its ports: api-port, service-port and node-ports,
and isolated=true for an isolated node.

start options:
  --timeout SECONDS    How long to wait for Kubernetes (default: 300, 0: no wait)

update options, for a node made with --isolate:
  --allow-cidr CIDR         Allow egress to CIDR (repeatable)
  --remove-allow-cidr CIDR  Remove CIDR from the allow list (repeatable)

status options:
  --json               Print a JSON array of {name, state, labels}

Environment:
  SMOLVM_PODS_WORK_DIR   WORK above (default: $pods_dir/target/node)
  SMOLVM_PODS_NODE       Default node VM name
  SMOLVM_PODS_SHIM       Default shim for build
  SMOLVM_SRC             smolvm checkout (default: $smolvm_src)
  SMOLVM_PODS_KERNEL_DIR The node kernel's install dir from node/kernel/build.sh
                         (default: SMOLVM_SRC/target/floci/kernel). build records
                         its kernel in the image's build-info
  SMOLVM                 smolvm CLI, whose lib dir has the kernel the node boots
                         (default: SMOLVM_PODS_KERNEL_DIR/bin/smolvm-floci)
EOF
}

die() {
    printf 'node-vm.sh: %s\n' "$*" >&2
    exit 1
}

step() {
    printf '==> %s\n' "$*" >&2
}

warn() {
    printf 'node-vm.sh: warning: %s\n' "$*" >&2
}

# need OPTION [VALUE...]: fail unless OPTION is followed by a value.
need() {
    [ $# -ge 2 ] || die "$1 needs a value"
}

# check_port OPTION VALUE: fail unless VALUE is a TCP port number.
check_port() {
    [[ $2 =~ ^[1-9][0-9]{0,4}$ ]] && [ "$2" -le 65535 ] || die "$1: not a port: $2"
}

# check_port_range OPTION VALUE: fail unless VALUE is PORT or FIRST-LAST.
check_port_range() {
    [[ $2 =~ ^([0-9]+)(-([0-9]+))?$ ]] || die "$1: not a port range: $2"
    local first=${BASH_REMATCH[1]} last=${BASH_REMATCH[3]:-${BASH_REMATCH[1]}}
    check_port "$1" "$first"
    check_port "$1" "$last"
    [ "$first" -le "$last" ] || die "$1: the range ends before it starts: $2"
}

# Hold lock_file on file descriptor 9 until the script exits or calls unlock.
# Commands that may outlive the script must not inherit it: run them with 9>&-.
lock() {
    mkdir -p "$(dirname "$lock_file")"
    exec 9>>"$lock_file"
    if ! flock_fd9 LOCK_NB; then
        step "Waiting for another node-vm.sh create or delete"
        flock_fd9 || die "cannot lock $lock_file"
    fi
}

unlock() {
    exec 9>&-
}

# flock_fd9 [LOCK_NB]: lock file descriptor 9. The lock stays with the shell's
# descriptor when perl exits.
flock_fd9() {
    perl -MFcntl=:flock -e '
        open(my $fd, ">&=", 9) or die "$!\n";
        flock($fd, @ARGV ? LOCK_EX | LOCK_NB : LOCK_EX) or exit 1;
    ' "$@"
}

# guest_gateway [SMOLVM-ARG...]: smolvm's gateway for the node, the first
# address of the --guest-subnet in the arguments, or of 100.96.0.0/30. A
# machine on a named network gets its gateway only when it starts.
guest_gateway() {
    local subnet=100.96.0.0/30
    while [ $# -gt 0 ]; do
        case $1 in
            --guest-subnet) subnet=${2:-}; shift ;;
            --guest-subnet=*) subnet=${1#*=} ;;
            --network | --network=*)
                die "create: --isolate does not work with smolvm's --network: a machine on a named" \
                    "network gets its gateway when it starts, so the allow list cannot name it" ;;
        esac
        shift
    done
    [[ $subnet =~ ^([0-9]+\.[0-9]+\.[0-9]+\.)([0-9]+)/[0-9]+$ ]] ||
        die "--guest-subnet: not an IPv4 CIDR: $subnet"
    printf '%s%s\n' "${BASH_REMATCH[1]}" "$((BASH_REMATCH[2] + 1))"
}

sha256() {
    shasum -a 256 "$1" | cut -d ' ' -f 1
}

extract_container=
cleanup() {
    if [ -n "$extract_container" ]; then
        docker rm --force "$extract_container" >/dev/null 2>&1 || true
    fi
}
trap cleanup EXIT

# require_cached REF [DIGEST]: fail unless the image is in the local store, as
# DIGEST when one is given.
require_cached() {
    local found
    found=$(docker image inspect --format '{{.Id}} {{join .RepoDigests " "}}' "$1" 2>/dev/null) ||
        die "$1 is not in the local Docker image store, and this script never pulls"
    case " $found " in
        *"${2:-}"*) ;;
        *) die "the cached $1 is ${found%% *}, expected $2" ;;
    esac
}

# check_image_archive TAR DIGEST: the archive's index names DIGEST, it holds
# the linux/arm64 image's manifest, config and layers, which a Docker store can
# leave out, and every blob in it matches its digest.
check_image_archive() {
    local dir ok=1 blobs manifest blob
    dir=$(mktemp -d)
    tar -xf "$1" -C "$dir"
    blobs=$dir/blobs/sha256
    [ "$(jq -r '.manifests[0].digest' "$dir/index.json")" = "$2" ] || ok=0
    manifest=$2
    if jq -e '.manifests' "$blobs/${2#sha256:}" >/dev/null 2>&1; then
        manifest=$(jq -r '[.manifests[] | select(.platform.os == "linux" and
            .platform.architecture == "arm64")][0].digest // empty' "$blobs/${2#sha256:}")
    fi
    for blob in "$manifest" $(jq -r '.config.digest, .layers[].digest' "$blobs/${manifest#sha256:}" 2>/dev/null); do
        [ -f "$blobs/${blob#sha256:}" ] || ok=0
    done
    (cd "$blobs" && for blob in *; do printf '%s  %s\n' "$blob" "$blob"; done |
        shasum -a 256 -c --quiet) || ok=0
    rm -rf "$dir"
    [ "$ok" = 1 ] || die "$1 does not hold $2 intact"
}

# check_linux_arm64 PATH WHAT: fail unless PATH is a 64-bit arm64 ELF binary.
check_linux_arm64() {
    [ -f "$1" ] || die "$2 not found: $1"
    file -b "$1" | grep -q '^ELF 64-bit LSB .*ARM aarch64' || die "$2 is not a Linux arm64 binary: $1"
}

# The state of machine $name, or nothing when it does not exist.
machine_state() {
    "$smolvm" machine ls --json | jq -r --arg name "$name" '.[] | select(.name == $name) | .state'
}

machine_label() {
    "$smolvm" machine ls --json |
        jq -r --arg name "$name" --arg key "$1" '.[] | select(.name == $name) | .labels[$key] // empty'
}

require_machine() {
    [ -x "$smolvm" ] || die "smolvm CLI not found: $smolvm"
    [ -n "$(machine_state)" ] || die "machine $name does not exist (run node-vm.sh create)"
}

node_exec() {
    "$smolvm" machine exec --name "$name" -- /usr/local/bin/floci-node exec "$@"
}

# Succeed once kubelet has reported the node Ready since the node VM booted.
# After a stop and a start, the node keeps the Ready status of its previous
# boot until kubelet posts its first status, which carries the new boot id.
node_ready() {
    local boot nodes
    boot=$(node_exec cat /proc/sys/kernel/random/boot_id) || return 1
    nodes=$(node_exec kubectl get nodes --output json) || return 1
    jq -e --arg boot "$boot" 'any(.items[] | select(.status.nodeInfo.bootID == $boot)
        | .status.conditions[]; .type == "Ready" and .status == "True")' <<<"$nodes"
}

# wait_until WHAT DEADLINE COMMAND...: run COMMAND every 2 seconds until it
# succeeds, or fail at DEADLINE (in $SECONDS).
wait_until() {
    local what=$1 deadline=$2
    shift 2
    step "Waiting for $what"
    until "$@" >/dev/null 2>&1; do
        [ "$SECONDS" -lt "$deadline" ] ||
            die "timed out waiting for $what; the k3s log is /var/log/k3s.log in the node, and" \
                "floci-node's log is in: $smolvm machine logs --name $name"
        sleep 2
    done
}

cmd_build() {
    local shim=$default_shim agent=$default_agent libkrun=$default_libkrun pod_kernel=$default_pod_kernel
    local release_dir=$work_dir/release
    local tag=smolvm-pods-node:latest keep_tag=0 out=$work_dir/node-image.tar
    while [ $# -gt 0 ]; do
        case $1 in
            --shim) need "$@"; shim=$2; shift 2 ;;
            --agent) need "$@"; agent=$2; shift 2 ;;
            --libkrun) need "$@"; libkrun=$2; shift 2 ;;
            --pod-kernel) need "$@"; pod_kernel=$2; shift 2 ;;
            --release-dir) need "$@"; release_dir=$2; shift 2 ;;
            --tag) need "$@"; tag=$2; keep_tag=1; shift 2 ;;
            --out) need "$@"; out=$2; shift 2 ;;
            *) die "build: unknown option $1" ;;
        esac
    done
    check_linux_arm64 "$shim" shim
    check_linux_arm64 "$agent" "guest agent"
    check_linux_arm64 "$libkrun" libkrun
    [ -f "$pod_kernel" ] || die "no pod kernel at $pod_kernel: run node/podkernel/build.sh"
    check_linux_arm64 "$pod_kernel" "pod kernel"
    local kernel_sha
    kernel_sha=$(awk '$1 == "sha256" && $3 == "Image" { print $2 }' "$kernel_dir/build/build-info" 2>/dev/null) ||
        true
    [ -n "$kernel_sha" ] ||
        die "no node kernel in $kernel_dir/build/build-info: run node/kernel/build.sh"
    if file -b "$shim" | grep -q 'statically linked'; then
        warn "the shim is statically linked, so it cannot load the release's libkrun.so and pod VMs will not boot"
    fi
    local base
    base=$(sed -n 's/^FROM //p' "$node_dir/Dockerfile")
    require_cached "$base"
    require_cached "$k3s_image"
    require_cached "$go_image"

    local inputs=$work_dir/inputs
    rm -rf "$inputs"
    mkdir -p "$inputs/release" "$inputs/images"

    step "Copying /bin out of $k3s_image"
    extract_container=floci-node-k3s-bin-$$
    docker create --name "$extract_container" "$k3s_image" >/dev/null
    docker cp "$extract_container:/bin" - >"$inputs/k3s-bin.tar"
    docker rm "$extract_container" >/dev/null
    extract_container=

    local ref image digest archive
    for ref in "${system_images[@]}"; do
        image=${ref%@*}
        digest=${ref#*@}
        require_cached "$image" "$digest"
        archive=${image##*/}
        archive=$inputs/images/${archive/:/-}.tar
        step "Saving $image"
        docker save "$image" -o "$archive"
        check_image_archive "$archive" "$digest"
    done

    step "Staging the smolvm release, Docker, the guest agent, libkrun, the pod kernel and the shim"
    local release_file
    for release_file in "$release_name.tar.gz" checksums.sha256; do
        # A checksums file kept from another release does not list this one.
        if [ ! -f "$release_dir/$release_file" ] || { [ "$release_file" = checksums.sha256 ] \
            && ! grep -q "  $release_name.tar.gz\$" "$release_dir/$release_file"; }; then
            step "Downloading $release_file"
            mkdir -p "$release_dir"
            curl --fail --location --silent --show-error --retry 3 \
                --output "$release_dir/$release_file.part" "$release_url/$release_file"
            mv "$release_dir/$release_file.part" "$release_dir/$release_file"
        fi
        cp "$release_dir/$release_file" "$inputs/release/$release_file"
    done
    local docker_tgz=$release_dir/docker-$docker_version.tgz
    if [ ! -f "$docker_tgz" ]; then
        step "Downloading ${docker_tgz##*/}"
        mkdir -p "$release_dir"
        curl --fail --location --silent --show-error --retry 3 \
            --output "$docker_tgz.part" "$docker_url"
        mv "$docker_tgz.part" "$docker_tgz"
    fi
    cp "$docker_tgz" "$inputs/docker.tgz"
    cp "$agent" "$inputs/smolvm-agent"
    cp "$libkrun" "$inputs/libkrun.so"
    cp "$pod_kernel" "$inputs/libkrunfw.so"
    cp "$shim" "$inputs/shim"

    step "Building the litmus test"
    extract_container=floci-node-litmus-$$
    docker run --rm --pull=never --network=none --name "$extract_container" \
        -v "$workloads_dir:/src:ro" -v "$inputs:/out" -w /src \
        -e CGO_ENABLED=0 -e GOOS=linux -e GOARCH=arm64 \
        "$go_image" go build -trimpath -ldflags=-s -o /out/litmus ./litmus
    extract_container=
    check_linux_arm64 "$inputs/litmus" "litmus test"

    step "Building $tag"
    docker build --pull=false --platform linux/arm64 --provenance=false --sbom=false \
        --build-context "inputs=$inputs" \
        --build-arg "SHIM_SHA256=$(sha256 "$inputs/shim")" \
        --build-arg "LITMUS_SHA256=$(sha256 "$inputs/litmus")" \
        --build-arg "POD_KERNEL_SHA256=$(sha256 "$inputs/libkrunfw.so")" \
        --build-arg "NODE_KERNEL_SHA256=$kernel_sha" \
        --tag "$tag" "$node_dir"

    step "Saving $tag to $out"
    mkdir -p "$(dirname "$out")"
    docker save "$tag" -o "$out.part"
    mv "$out.part" "$out"
    if [ "$keep_tag" = 0 ]; then
        docker image rm "$tag" >/dev/null
    fi
    printf '%s  %s\n' "$(sha256 "$out")" "$out"
}

cmd_create() {
    local image=$work_dir/node-image.tar cpus=4 mem=6144 storage=40 overlay=8
    local api_port=6443 service_port='' node_ports=32080-32099 seed='' isolate=0 rosetta=1
    local args=() allow=() extra=() volume cidr gateway
    while [ $# -gt 0 ]; do
        case $1 in
            --name) need "$@"; name=$2; shift 2 ;;
            --image) need "$@"; image=$2; shift 2 ;;
            --cpus) need "$@"; cpus=$2; shift 2 ;;
            --mem) need "$@"; mem=$2; shift 2 ;;
            --storage) need "$@"; storage=$2; shift 2 ;;
            --overlay) need "$@"; overlay=$2; shift 2 ;;
            --api-port) need "$@"; api_port=$2; shift 2 ;;
            --service-port) need "$@"; service_port=$2; shift 2 ;;
            --node-ports) need "$@"; node_ports=$2; shift 2 ;;
            --seed) need "$@"; seed=$2; shift 2 ;;
            --isolate) isolate=1; shift ;;
            --allow-cidr) need "$@"; allow+=("$2"); shift 2 ;;
            --volume) need "$@"; args+=(--volume "$2"); shift 2 ;;
            --no-rosetta) rosetta=0; shift ;;
            --) shift; extra=("$@"); break ;;
            *) die "create: unknown option $1" ;;
        esac
    done
    [ -x "$smolvm" ] || die "smolvm CLI not found: $smolvm"
    [ -f "$image" ] || die "image tar not found: $image (run node-vm.sh build)"
    image=$(cd "$(dirname "$image")" && pwd)/$(basename "$image")
    check_port --api-port "$api_port"
    args+=(--port "$api_port:6443" --label "api-port=$api_port")
    if [ -n "$service_port" ]; then
        check_port --service-port "$service_port"
        args+=(--port "$service_port:7070" --label "service-port=$service_port")
    fi
    if [ "$node_ports" != none ]; then
        check_port_range --node-ports "$node_ports"
        args+=(--port "$node_ports:$node_ports")
    fi
    args+=(--label "node-ports=$node_ports")
    if [ -n "$seed" ]; then
        [ -d "$seed" ] || die "seed directory not found: $seed"
        args+=(--volume "$(cd "$seed" && pwd):$seed_mount:ro")
    fi
    if [ "$isolate" = 1 ]; then
        gateway=$(guest_gateway ${extra[@]+"${extra[@]}"})
        args+=(--allow-cidr "$gateway/32")
        for cidr in ${allow[@]+"${allow[@]}"}; do
            args+=(--allow-cidr "$cidr")
        done
        args+=(--allow-host-pattern "$closed_names" --env FLOCI_NODE_ISOLATION=1 --label isolated=true)
    elif [ ${#allow[@]} -gt 0 ]; then
        die "create: --allow-cidr needs --isolate"
    fi
    for volume in "${volumes[@]}"; do
        args+=(--volume "$volume")
    done
    if [ "$rosetta" = 1 ]; then
        args+=(--rosetta)
    fi
    # smolvm keeps a copy of the image under this checksum (see cmd_delete).
    args+=(--label "image-sha256=$(sha256 "$image")")
    # A delete removes smolvm's copy of an image that no machine names (see
    # cmd_delete), so the copy and the machine that names it appear together.
    lock
    [ -z "$(machine_state)" ] || die "machine $name already exists"
    step "Creating $name from $image"
    "$smolvm" machine create --name "$name" --image "$image" \
        --nested --net --net-backend virtio-net \
        --cpus "$cpus" --mem "$mem" --storage "$storage" --overlay "$overlay" \
        --stop-timeout 30 --label "$node_label=true" \
        "${args[@]}" ${extra[@]+"${extra[@]}"} 9>&-
    unlock
}

cmd_start() {
    local timeout=300 deadline
    while [ $# -gt 0 ]; do
        case $1 in
            --name) need "$@"; name=$2; shift 2 ;;
            --timeout) need "$@"; timeout=$2; shift 2 ;;
            *) die "start: unknown option $1" ;;
        esac
    done
    require_machine
    if [ "$(machine_state)" != running ]; then
        step "Starting $name"
        "$smolvm" machine start --name "$name"
    fi
    if [ "$timeout" = 0 ]; then
        return
    fi
    deadline=$((SECONDS + timeout))
    wait_until "the Kubernetes API" "$deadline" node_exec kubectl get --raw /readyz
    wait_until "the node to be Ready" "$deadline" node_ready
    wait_until "the smolvm-pods RuntimeClass" "$deadline" \
        node_exec kubectl get runtimeclass smolvm-pods
    local thp
    thp=$(node_exec cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null) || true
    case $thp in
        *'[always]'*) ;;
        *) warn "the node kernel has no transparent huge pages with the policy always, so nested pod VMs are slow (see node/kernel/README.md)" ;;
    esac
    step "Ready: the API is at https://127.0.0.1:$(machine_label api-port) (see node-vm.sh kubeconfig)"
    local service_port
    service_port=$(machine_label service-port)
    if [ -n "$service_port" ]; then
        step "The node service's API is at http://127.0.0.1:$service_port (see node-vm.sh token and docker-env)"
    fi
}

# Parse --name and nothing else, for the commands that take only that.
name_only() {
    local command=$1
    shift
    while [ $# -gt 0 ]; do
        case $1 in
            --name) need "$@"; name=$2; shift 2 ;;
            *) die "$command: unknown option $1" ;;
        esac
    done
}

cmd_stop() {
    name_only stop "$@"
    require_machine
    "$smolvm" machine stop --name "$name"
}

cmd_delete() {
    name_only delete "$@"
    require_machine
    # smolvm keeps the image archive under its checksum, and the seed disk
    # that the machine's storage disk was cloned from. Neither goes with the
    # machine, and each new node image adds one of each.
    local image_sha seed
    image_sha=$(machine_label image-sha256)
    seed=$(cat "$vms_dir/$(printf %s "$name" | shasum -a 256 | cut -c 1-16)/storage.seed" 2>/dev/null) || true
    if [ "$(machine_state)" = running ]; then
        "$smolvm" machine stop --name "$name"
    fi
    "$smolvm" machine delete --name "$name" --force
    lock
    remove_unused_image_copies "$image_sha" "$seed"
    unlock
}

# remove_unused_image_copies SHA256 SEED: remove the image archive SHA256 and
# the seed disk SEED unless another machine uses them. smolvm takes no lock on
# the archive, so the caller holds node-vm.sh's lock, which every create holds
# until its machine names the archive.
remove_unused_image_copies() {
    local image_sha=$1 seed=$2 machines
    if [[ $image_sha =~ ^[0-9a-f]{64}$ ]] && [ -d "$image_archives/$image_sha" ]; then
        # Read the whole listing: a pipe to a reader that stops at the first
        # match fails smolvm with a broken pipe. A failed listing stops here.
        machines=$("$smolvm" machine ls --json)
        if [[ $machines != *"$image_sha"* ]]; then
            step "Removing smolvm's copy of image $image_sha"
            rm -rf "${image_archives:?}/$image_sha"
        fi
    fi
    if [[ $seed == "$image_seeds"/*/storage.raw ]] && [ -d "$(dirname "$seed")" ]; then
        remove_seed "$seed"
    fi
}

# remove_seed SEED: remove the seed disk SEED unless a machine's storage disk
# was cloned from it. As smolvm's own eviction does (prune in its
# src/image_seed.rs), it first takes the cache lock, which a create holds while
# it clones a seed and records it in storage.seed, then the seed's own lock,
# which its build holds. The lock files stay, as smolvm needs.
remove_seed() {
    perl -MFcntl=:DEFAULT,:flock -MFile::Glob=bsd_glob -MFile::Path=remove_tree -e '
        my ($seed, $seeds, $vms) = @ARGV;
        my ($dir, $key) = $seed =~ m{^(.*/([^/]+))/[^/]+$} or die "no seed directory in $seed\n";
        my @held;
        for (["$seeds/.cache.lock", LOCK_EX], ["$seeds/$key.lock", LOCK_EX | LOCK_NB]) {
            sysopen(my $lock, $_->[0], O_WRONLY | O_CREAT | O_NOFOLLOW, 0600) or die "open $_->[0]: $!\n";
            flock($lock, $_->[1]) or do { print STDERR "==> Keeping $dir: in use\n"; exit 0 };
            push @held, $lock;
        }
        for (bsd_glob("$vms/*/storage.seed")) {
            open(my $in, "<", $_) or next;
            exit 0 if do { local $/; <$in> } eq $seed;
        }
        print STDERR "==> Removing smolvm'\''s seed disk $dir\n";
        remove_tree($dir);
    ' "$1" "$image_seeds" "$vms_dir"
}

# run_in_node PREFIX [OPTIONS] [--] [ARG...]: run PREFIX and ARG in the node with
# the node's environment. An empty PREFIX runs ARG itself.
run_in_node() {
    local prefix=$1 flags=()
    shift
    while [ $# -gt 0 ]; do
        case $1 in
            --name) need "$@"; name=$2; shift 2 ;;
            -i | --interactive) flags+=(--interactive); shift ;;
            -t | --tty) flags+=(--tty); shift ;;
            --stream) flags+=(--stream); shift ;;
            --) shift; break ;;
            *) break ;;
        esac
    done
    if [ -n "$prefix" ]; then
        set -- "$prefix" "$@"
    fi
    [ $# -gt 0 ] || die "exec: no command given"
    require_machine
    "$smolvm" machine exec --name "$name" ${flags[@]+"${flags[@]}"} -- \
        /usr/local/bin/floci-node exec "$@"
}

cmd_exec() {
    run_in_node '' "$@"
}

cmd_kubectl() {
    run_in_node kubectl "$@"
}

cmd_kubeconfig() {
    name_only kubeconfig "$@"
    require_machine
    local port
    port=$(machine_label api-port)
    [ -n "$port" ] || die "machine $name has no api-port label; was it made by node-vm.sh create?"
    node_exec cat /etc/rancher/k3s/k3s.yaml | sed "s#https://127.0.0.1:6443#https://127.0.0.1:$port#"
}

# smolvm changes the allow list of a running machine at once. An allow list
# turns an open machine into a restricted one, so only an isolated node takes it.
cmd_update() {
    local args=()
    while [ $# -gt 0 ]; do
        case $1 in
            --name) need "$@"; name=$2; shift 2 ;;
            --allow-cidr | --remove-allow-cidr) need "$@"; args+=("$1" "$2"); shift 2 ;;
            *) die "update: unknown option $1" ;;
        esac
    done
    [ ${#args[@]} -gt 0 ] || die "update: give --allow-cidr or --remove-allow-cidr"
    require_machine
    [ "$(machine_label isolated)" = true ] ||
        die "machine $name is not isolated: only a node made with create --isolate has an allow list"
    "$smolvm" machine update --name "$name" "${args[@]}"
}

cmd_token() {
    name_only token "$@"
    require_machine
    [ "$(machine_state)" = running ] || die "machine $name is not running (run node-vm.sh start)"
    node_exec cat /etc/floci-node/api-token
}

# A config directory of the Docker CLI's own for this node, so that the token
# goes only to this node's dockerd, at the /docker path of the node service's API.
cmd_docker_env() {
    name_only docker-env "$@"
    require_machine
    local port
    port=$(machine_label service-port)
    [ -n "$port" ] || die "machine $name publishes no node service API: make it again with" \
        "node-vm.sh create --service-port PORT"
    [ "$(machine_state)" = running ] || die "machine $name is not running (run node-vm.sh start)"
    local token dir=${XDG_CONFIG_HOME:-$HOME/.config}/smolvm-pods/docker/$name
    token=$(node_exec cat /etc/floci-node/api-token)
    [ -n "$token" ] || die "machine $name has no node service token"
    (
        umask 077
        mkdir -p "$dir"
        chmod 0700 "$dir"
        jq -n --arg auth "Bearer $token" '{HttpHeaders: {Authorization: $auth}}' >"$dir/config.json.part"
        mv "$dir/config.json.part" "$dir/config.json"
    )
    printf 'export DOCKER_HOST=%q\n' "tcp://127.0.0.1:$port/docker"
    printf 'export DOCKER_CONFIG=%q\n' "$dir"
}

cmd_status() {
    local json=0
    while [ $# -gt 0 ]; do
        case $1 in
            --json) json=1; shift ;;
            *) die "status: unknown option $1" ;;
        esac
    done
    [ -x "$smolvm" ] || die "smolvm CLI not found: $smolvm"
    local nodes
    nodes=$("$smolvm" machine ls --json | jq --arg key "$node_label" \
        '[.[] | select((.labels // {})[$key] == "true") | {name, state, labels}]')
    if [ "$json" = 1 ]; then
        printf '%s\n' "$nodes"
        return
    fi
    jq -r '["NAME", "STATE", "API", "SERVICE", "NODEPORTS", "ISOLATED"],
        (.[] | [.name, .state, .labels["api-port"] // "-", .labels["service-port"] // "-",
            .labels["node-ports"] // "-", .labels.isolated // "false"]) | @tsv' <<<"$nodes" |
        column -t -s "$(printf '\t')"
}

case ${1:-help} in
    build | create | start | stop | delete | update | exec | kubectl | kubeconfig | token | docker-env | status)
        command=${1//-/_}
        shift
        "cmd_$command" "$@"
        ;;
    help | -h | --help)
        usage
        ;;
    *)
        usage >&2
        exit 2
        ;;
esac
