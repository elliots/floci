#!/usr/bin/env bash
# The resume check. On a fresh node VM from the node image, it runs memhold and
# PetClinic in pod VMs that resume-harness drives with smolvm as a library: per
# workload, repetitions of a cold start and two cycles of a local pause and a
# resume. summarize.py applies the pass criteria. Run "check.sh help" for the
# options.
set -euo pipefail

resume_dir=$(cd "$(dirname "$0")" && pwd)
pods_dir=$(cd "$resume_dir/../.." && pwd)
node_vm=$pods_dir/scripts/node-vm.sh
# The Go image of the end-to-end tests, which never pull it.
go_image=$(cat "$pods_dir/tests/e2e/workloads/go-image")
# spring-framework-petclinic 6.1.2's exploded WAR: the SHA-256 of
# "find . -type f | LC_ALL=C sort | xargs shasum -a 256" in it.
petclinic_sha256=965a36587b7cbc8b13537af57824a2722c0acdcd227d6061b1d5a9b0b1b4a929

label=check
name=floci-resume-check
reps=3
keep=0
khugepaged_sleep=
harness=${SMOLVM_PODS_RESUME_HARNESS:-$resume_dir/harness/target/release/resume-harness}
petclinic=${SMOLVM_PODS_PETCLINIC:-}

usage() {
    cat <<EOF
Usage: check.sh [OPTIONS]

Creates node VM NAME from the node image (node-vm.sh create, 8 vCPUs, 8 GiB),
runs the resume check in it, deletes it, and prints the summary. Exits 1 if a
check failed. The results stay in $pods_dir/target/resume-check/LABEL.

Options:
  --label LABEL       Name of this run (default: $label)
  --name NAME         Node VM name (default: $name)
  --reps N            Repetitions per workload (default: $reps)
  --harness PATH      resume-harness, built with
                      scripts/build.sh build --locked --release \\
                        --manifest-path bench/resume/harness/Cargo.toml
                      (default: $harness)
  --petclinic DIR     PetClinic's exploded WAR (default: \$SMOLVM_PODS_PETCLINIC)
  --khugepaged-sleep MS
                      Before the workloads, make khugepaged sleep MS ms between
                      scans and between allocations (default: floci-node's
                      86400000, a day, on a Mac node)
  --keep              Keep the node VM

node-vm.sh reads its own variables, such as SMOLVM_PODS_WORK_DIR for the node
image and SMOLVM for the smolvm CLI and kernel.
EOF
}

die() {
    printf 'check.sh: %s\n' "$*" >&2
    exit 1
}

step() {
    printf '==> %s %s\n' "$(date +%H:%M:%S)" "$*" >&2
}

need() {
    [ $# -ge 2 ] || die "$1 needs a value"
}

while [ $# -gt 0 ]; do
    case $1 in
        --label) need "$@"; label=$2; shift 2 ;;
        --name) need "$@"; name=$2; shift 2 ;;
        --reps) need "$@"; reps=$2; shift 2 ;;
        --harness) need "$@"; harness=$2; shift 2 ;;
        --petclinic) need "$@"; petclinic=$2; shift 2 ;;
        --khugepaged-sleep) need "$@"; khugepaged_sleep=$2; shift 2 ;;
        --keep) keep=1; shift ;;
        help | -h | --help) usage; exit 0 ;;
        *) die "unknown option $1, see check.sh help" ;;
    esac
done

for tool in docker jq python3 shasum timeout; do
    command -v "$tool" >/dev/null || die "$tool is not installed"
done
docker image inspect "$go_image" >/dev/null 2>&1 ||
    die "$go_image must be in the local Docker image store, and this script never pulls"
file -b "$harness" 2>/dev/null | grep -q '^ELF 64-bit LSB .*ARM aarch64' ||
    die "no Linux arm64 resume-harness at $harness (see check.sh help)"
[ -n "$petclinic" ] && [ -d "$petclinic" ] || die "set --petclinic or SMOLVM_PODS_PETCLINIC to PetClinic's exploded WAR"
tree=$(cd "$petclinic" && find . -type f | LC_ALL=C sort | xargs shasum -a 256 | shasum -a 256 | cut -d ' ' -f 1)
[ "$tree" = "$petclinic_sha256" ] || die "$petclinic is not the pinned PetClinic build (tree $tree)"

out=$pods_dir/target/resume-check/$label
share=$out/share
step "Staging the programs in $share"
rm -rf "$out"
mkdir -p "$share/petclinic"
docker run --rm --pull=never --network=none --name "floci-resume-go-$$" \
    -v "$pods_dir/bench:/src:ro" -v "$share:/out" \
    -e CGO_ENABLED=0 -e GOOS=linux -e GOARCH=arm64 -e GOCACHE=/tmp/go-cache -e GOFLAGS=-buildvcs=false \
    "$go_image" sh -ec '
        cd /src/resume/driver && go build -trimpath -ldflags="-s -w" -o /out/driver .
        cd /src/resume/memhold && go build -trimpath -ldflags="-s -w" -o /out/memhold .'
cp "$harness" "$share/resume-harness"
cp "$resume_dir/run-pod.sh" "$share/"
cp -R "$petclinic" "$share/petclinic/ROOT"

# A port in the range that nothing on this host listens on.
free_port() {
    python3 -I -c '
import random, socket, sys
low, high = int(sys.argv[1]), int(sys.argv[2])
for port in random.sample(range(low, high + 1), high - low + 1):
    try:
        for ip in ("127.0.0.1", "0.0.0.0"):
            with socket.socket() as s:
                s.bind((ip, port))
    except OSError:
        continue
    print(port)
    break
' "$1" "$2"
}

cleanup() {
    if [ "$keep" = 1 ]; then
        step "Kept node $name; delete it with: $node_vm delete --name $name"
    else
        step "Deleting node $name"
        timeout 300 "$node_vm" delete --name "$name" || printf 'check.sh: could not delete node %s\n' "$name" >&2
    fi
}

# create refuses a node that exists, and the trap deletes only the node it made.
node_port=$(free_port 30000 32767)
step "Creating node $name"
timeout 900 "$node_vm" create --name "$name" --cpus 8 --mem 8192 \
    --api-port "$(free_port 40000 48999)" --node-ports "$node_port-$node_port" \
    --volume "$share:/mnt/resume"
trap cleanup EXIT
timeout 600 "$node_vm" start --name "$name"

# run LIMIT ARG...: one run-pod.sh step in the node, limited to LIMIT seconds.
run() {
    local limit=$1
    shift
    timeout --kill-after 30 "$limit" "$node_vm" exec --name "$name" -- /mnt/resume/run-pod.sh "$@"
}
run 300 setup "$label" ${khugepaged_sleep:+"$khugepaged_sleep"}
# A failed step is reported by summarize.py, which finds its records missing.
for workload in memhold petclinic; do
    step "Creating the $workload pod VM"
    if ! run 900 create "$label" "$workload"; then
        step "Creating the $workload pod VM failed"
        continue
    fi
    for n in $(seq 1 "$reps"); do
        step "Repetition $n of $workload"
        run 2400 rep "$label" "$workload" "$n" >/dev/null || step "Repetition $n of $workload failed"
    done
    run 300 delete "$label" "$workload" || step "Deleting the $workload pod VM failed"
done
run 60 info "$label" node-info-end.txt

[ -d "$share/results-$label" ] || die "the node wrote no results"
mv "$share/results-$label" "$out/results"
status=0
python3 -I "$resume_dir/summarize.py" "$out/results" "$reps" 2 | tee "$out/summary.md" || status=$?
exit "$status"
