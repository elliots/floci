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

# The build takes files from these images and never pulls them.
k3s_image=rancher/k3s@sha256:10464930d9bad0c06aef9830e84cd4019c24ed44d5eab594efb7416119097248
# Builds the litmus test of the shim's TSO canary, as the end-to-end tests do.
workloads_dir=$pods_dir/tests/e2e/workloads
go_image=$(cat "$workloads_dir/go-image")
# The images k3s runs at startup. The node imports them, so it needs no registry.
system_images=(
    rancher/mirrored-pause:3.6@sha256:74c4244427b7312c5b901fe0f67cbc53683d06f4f24c6faee65d4182bf0fa893
    rancher/mirrored-coredns-coredns:1.13.1@sha256:9b9128672209474da07c91439bf15ed704ae05ad918dd6454e5b6ae14e35fee6
)
# The smolvm release that node/Dockerfile pins, smolvm-VERSION-linux-arm64.
release_name=$(sed -n 's/^ARG SMOLVM_RELEASE=//p' "$node_dir/Dockerfile")
release_version=${release_name#smolvm-}
release_url=https://github.com/smol-machines/smolvm/releases/download/v${release_version%-linux-arm64}

# Node paths kept on the node's ext4 storage disk instead of its overlay root:
# k3s and containerd, kubelet, smolvm's data, the shim's pod shares, the logs
# (pod logs too, which kubelet's eviction then sees), and CNI's IP leases.
volumes=(/var/lib/rancher /var/lib/kubelet /var/lib/smolvm /var/lib/containerd-shim-smolvm-pods
    /var/log /var/lib/cni)

# smolvm's caches on the Mac, which a machine delete leaves.
image_archives=$HOME/Library/Caches/smolvm-image-archives
vms_dir=$HOME/Library/Caches/smolvm/vms
image_seeds=$HOME/Library/Caches/smolvm/image-seeds

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
               its image when no other machine uses them. Do not run it while
               a create from the same image tar runs.
  exec         Run a command in the node: exec [-i] [-t] [--] COMMAND [ARG...]
  kubectl      Run kubectl in the node: kubectl [-i] [-t] [--] [ARG...]
  kubeconfig   Print a kubeconfig that reaches the node's API from the Mac.

Every command except build takes --name NAME (default: $name).

build options:
  --shim PATH          The smolvm-pods shim, a Linux arm64 binary
                       (default: $default_shim)
  --agent PATH         The patched guest agent (default: $default_agent)
  --libkrun PATH       The patched libkrun.so of node/libkrun/build.sh
                       (default: $default_libkrun)
  --release-dir DIR    Directory with $release_name.tar.gz and checksums.sha256,
                       downloaded into it when missing (default: WORK/release)
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
  --node-ports RANGE   NodePorts published on the same Mac ports (default: 30080-30099)
  --volume HOST:GUEST[:ro]
                       Share a Mac directory with the node (repeatable)
  --no-rosetta         Do not share Rosetta with the node, so amd64 pods cannot run
  -- ARG...            Pass the remaining arguments to smolvm machine create, for
                       example: -- --allow-cidr 100.96.0.1/32, which allows TCP and
                       UDP only to smolvm's gateway, whose TCP reaches the Mac's
                       loopback. It leaves DNS open: the gateway answers every DNS
                       query itself (see docs/architecture.md, "Networking")

start options:
  --timeout SECONDS    How long to wait for Kubernetes (default: 300, 0: no wait)

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

# check_image_archive TAR DIGEST: the archive's index names DIGEST and every
# blob in it matches its digest.
check_image_archive() {
    local dir ok=1
    dir=$(mktemp -d)
    tar -xf "$1" -C "$dir"
    [ "$(jq -r '.manifests[0].digest' "$dir/index.json")" = "$2" ] || ok=0
    (cd "$dir/blobs/sha256" && for blob in *; do printf '%s  %s\n' "$blob" "$blob"; done |
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
    local shim=$default_shim agent=$default_agent libkrun=$default_libkrun
    local release_dir=$work_dir/release
    local tag=smolvm-pods-node:latest keep_tag=0 out=$work_dir/node-image.tar
    while [ $# -gt 0 ]; do
        case $1 in
            --shim) need "$@"; shim=$2; shift 2 ;;
            --agent) need "$@"; agent=$2; shift 2 ;;
            --libkrun) need "$@"; libkrun=$2; shift 2 ;;
            --release-dir) need "$@"; release_dir=$2; shift 2 ;;
            --tag) need "$@"; tag=$2; keep_tag=1; shift 2 ;;
            --out) need "$@"; out=$2; shift 2 ;;
            *) die "build: unknown option $1" ;;
        esac
    done
    check_linux_arm64 "$shim" shim
    check_linux_arm64 "$agent" "guest agent"
    check_linux_arm64 "$libkrun" libkrun
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

    step "Staging the smolvm release, the guest agent, libkrun and the shim"
    local release_file
    for release_file in "$release_name.tar.gz" checksums.sha256; do
        if [ ! -f "$release_dir/$release_file" ]; then
            step "Downloading $release_file"
            mkdir -p "$release_dir"
            curl --fail --location --silent --show-error --retry 3 \
                --output "$release_dir/$release_file.part" "$release_url/$release_file"
            mv "$release_dir/$release_file.part" "$release_dir/$release_file"
        fi
        cp "$release_dir/$release_file" "$inputs/release/$release_file"
    done
    cp "$agent" "$inputs/smolvm-agent"
    cp "$libkrun" "$inputs/libkrun.so"
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
    local api_port=6443 node_ports=30080-30099 rosetta=1 volume
    local args=() extra=()
    while [ $# -gt 0 ]; do
        case $1 in
            --name) need "$@"; name=$2; shift 2 ;;
            --image) need "$@"; image=$2; shift 2 ;;
            --cpus) need "$@"; cpus=$2; shift 2 ;;
            --mem) need "$@"; mem=$2; shift 2 ;;
            --storage) need "$@"; storage=$2; shift 2 ;;
            --overlay) need "$@"; overlay=$2; shift 2 ;;
            --api-port) need "$@"; api_port=$2; shift 2 ;;
            --node-ports) need "$@"; node_ports=$2; shift 2 ;;
            --volume) need "$@"; args+=(--volume "$2"); shift 2 ;;
            --no-rosetta) rosetta=0; shift ;;
            --) shift; extra=("$@"); break ;;
            *) die "create: unknown option $1" ;;
        esac
    done
    [ -x "$smolvm" ] || die "smolvm CLI not found: $smolvm"
    [ -f "$image" ] || die "image tar not found: $image (run node-vm.sh build)"
    image=$(cd "$(dirname "$image")" && pwd)/$(basename "$image")
    [ -z "$(machine_state)" ] || die "machine $name already exists"
    # smolvm keeps a copy of the image under this checksum (see cmd_delete).
    args+=(--label "image-sha256=$(sha256 "$image")")
    for volume in "${volumes[@]}"; do
        args+=(--volume "$volume")
    done
    if [ "$rosetta" = 1 ]; then
        args+=(--rosetta)
    fi
    step "Creating $name from $image"
    "$smolvm" machine create --name "$name" --image "$image" \
        --nested --net --net-backend virtio-net \
        --cpus "$cpus" --mem "$mem" --storage "$storage" --overlay "$overlay" \
        --port "$api_port:6443" --port "$node_ports:$node_ports" \
        --stop-timeout 30 \
        --label smolvm-pods-node=true --label "api-port=$api_port" \
        "${args[@]}" ${extra[@]+"${extra[@]}"}
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
    wait_until "the node to be Ready" "$deadline" \
        node_exec kubectl wait --for=condition=Ready node --all --timeout=10s
    wait_until "the smolvm-pods RuntimeClass" "$deadline" \
        node_exec kubectl get runtimeclass smolvm-pods
    local thp
    thp=$(node_exec cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null) || true
    case $thp in
        *'[always]'*) ;;
        *) warn "the node kernel has no transparent huge pages with the policy always, so nested pod VMs are slow (see node/kernel/README.md)" ;;
    esac
    step "Ready: the API is at https://127.0.0.1:$(machine_label api-port) (see node-vm.sh kubeconfig)"
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
    remove_unused_image_copies "$image_sha" "$seed"
}

# remove_unused_image_copies SHA256 SEED: remove the image archive SHA256 and
# the seed disk SEED unless another machine uses them. smolvm takes no lock on
# the archive, so a delete must not run while a create from the same image does.
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

case ${1:-help} in
    build | create | start | stop | delete | exec | kubectl | kubeconfig)
        command=$1
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
