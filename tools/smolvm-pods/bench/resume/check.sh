#!/usr/bin/env bash
# The resume check. On a fresh node VM from the node image, it runs memhold and
# PetClinic in pod VMs that resume-harness drives with smolvm as a library: per
# workload, repetitions of a cold start and two cycles of a local pause and a
# resume. summarize.py applies the pass criteria. With --delta, it runs the save
# benchmark instead: per workload, a series of pauses with delta saves on or
# off, which compare.py summarizes. Run "check.sh help" for the options.
set -euo pipefail

resume_dir=$(cd "$(dirname "$0")" && pwd)
pods_dir=$(cd "$resume_dir/../.." && pwd)
node_vm=$pods_dir/scripts/node-vm.sh
# The Go image of the end-to-end tests, which never pull it.
go_image=$(cat "$pods_dir/tests/e2e/workloads/go-image")
# spring-framework-petclinic 6.1.2's exploded WAR: the SHA-256 of
# "find . -type f | LC_ALL=C sort | xargs shasum -a 256" in it.
petclinic_sha256=965a36587b7cbc8b13537af57824a2722c0acdcd227d6061b1d5a9b0b1b4a929

label=
name=floci-resume-check
reps=3
keep=0
khugepaged_sleep=
delta=
cycles=6
idle=2
settle=0
workloads=idle,scattered:0.05,all
harness=${SMOLVM_PODS_RESUME_HARNESS:-$resume_dir/harness/target/release/resume-harness}
petclinic=${SMOLVM_PODS_PETCLINIC:-}

usage() {
    cat <<EOF
Usage: check.sh [OPTIONS]

Creates node VM NAME from the node image (node-vm.sh create, 8 vCPUs, 8 GiB),
runs the resume check in it, deletes it, and prints the summary. Exits 1 if a
check failed. The results stay in $pods_dir/target/resume-check/LABEL.

Options:
  --label LABEL       Name of this run (default: check, or saves-on or
                      saves-off with --delta)
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

The save benchmark:
  --delta on|off      Run the save benchmark instead of the resume check, with
                      smolvm's delta saves on or off (SMOLVM_DELTA_SAVES). Per
                      workload, a new pod VM gets a cold start and its first
                      pause, then cycles of a resume, a memory check (memhold),
                      a dirtying request, an idle period and a pause, then a
                      last resume and memory check. Each pause records smolvm's
                      save mode and bytes written, and the node storage disk's
                      sectors written around it, after a sync and a wait for
                      writeback, and the node's background writes in a quiet
                      window just before it. compare.py prints the run, or two
                      runs side by side: compare.py saves-on saves-off
  --cycles N          Cycles after the first pause (default: $cycles)
  --idle S            Idle seconds before each pause (default: $idle)
  --settle S          Seconds to wait after a sync before reading the disk,
                      before and after each pause; 0 reads no disk and counts
                      smolvm's bytes only, which matched the disk's within
                      20 MB in every pause measured (default: $settle)
  --workloads LIST    Comma-separated (default: $workloads):
                        idle         memhold, no dirtying request
                        petclinic    PetClinic; its first answer is one request
                        all          memhold rewrites all its memory
                        PATTERN:P    memhold's /dirty with P percent of 1 GiB:
                                     scattered (4 KiB pages at random), blocks
                                     (whole 2 MiB blocks), zero (zeros over
                                     blocks that held data), free (freed, then
                                     written again), fresh (memory never used,
                                     from a 512 MiB reserve), mixed (each of
                                     these in turn)

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
        --delta) need "$@"; delta=$2; shift 2 ;;
        --cycles) need "$@"; cycles=$2; shift 2 ;;
        --idle) need "$@"; idle=$2; shift 2 ;;
        --settle) need "$@"; settle=$2; shift 2 ;;
        --workloads) need "$@"; workloads=$2; shift 2 ;;
        help | -h | --help) usage; exit 0 ;;
        *) die "unknown option $1, see check.sh help" ;;
    esac
done

case $delta in
    '' | on | off) ;;
    *) die "--delta must be on or off" ;;
esac
for n in "$cycles" "$idle" "$settle"; do
    [[ $n =~ ^[0-9]+$ ]] || die "--cycles, --idle and --settle take whole numbers"
done
[ -n "$label" ] || label=${delta:+saves-$delta}
label=${label:-check}

# bench_workload SPEC: sets id, the save benchmark workload's pod VM, and
# dirty, memhold's dirtying pattern.
bench_workload() {
    dirty=
    case $1 in
        petclinic) id=petclinic ;;
        idle) id=memhold-idle ;;
        all) id=memhold-all; dirty=all ;;
        scattered:* | blocks:* | zero:* | free:* | fresh:* | mixed:*)
            [[ ${1#*:} =~ ^[0-9]+(\.[0-9]+)?$ ]] || die "bad workload $1: P is a percentage"
            dirty=$1
            id=memhold-${1/:/-}
            id=${id//./p}
            ;;
        *) die "unknown workload $1, see check.sh help" ;;
    esac
}

need_petclinic=1
if [ -n "$delta" ]; then
    IFS=, read -r -a specs <<<"$workloads"
    [[ ",$workloads," == *,petclinic,* ]] || need_petclinic=0
    for spec in "${specs[@]}"; do
        bench_workload "$spec"
    done
fi

for tool in docker jq python3 shasum timeout; do
    command -v "$tool" >/dev/null || die "$tool is not installed"
done
docker image inspect "$go_image" >/dev/null 2>&1 ||
    die "$go_image must be in the local Docker image store, and this script never pulls"
file -b "$harness" 2>/dev/null | grep -q '^ELF 64-bit LSB .*ARM aarch64' ||
    die "no Linux arm64 resume-harness at $harness (see check.sh help)"
if [ "$need_petclinic" = 1 ]; then
    [ -n "$petclinic" ] && [ -d "$petclinic" ] || die "set --petclinic or SMOLVM_PODS_PETCLINIC to PetClinic's exploded WAR"
    tree=$(cd "$petclinic" && find . -type f | LC_ALL=C sort | xargs shasum -a 256 | shasum -a 256 | cut -d ' ' -f 1)
    [ "$tree" = "$petclinic_sha256" ] || die "$petclinic is not the pinned PetClinic build (tree $tree)"
fi

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
[ "$need_petclinic" = 0 ] || cp -R "$petclinic" "$share/petclinic/ROOT"

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
# A failed step is reported by summarize.py or compare.py, which find its
# records missing.
if [ -n "$delta" ]; then
    for spec in "${specs[@]}"; do
        bench_workload "$spec"
        step "Creating the $id pod VM"
        if ! run 900 create "$label" "$id"; then
            step "Creating the $id pod VM failed"
            continue
        fi
        step "$cycles cycles of $id, delta saves $delta"
        run $((600 + cycles * 300)) series "$label" "$id" "$cycles" "$idle" "$settle" "$delta" ${dirty:+"$dirty"} \
            >/dev/null || step "The cycles of $id failed"
        run 300 delete "$label" "$id" || step "Deleting the $id pod VM failed"
    done
else
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
fi
run 60 info "$label" node-info-end.txt

[ -d "$share/results-$label" ] || die "the node wrote no results"
mv "$share/results-$label" "$out/results"
status=0
if [ -n "$delta" ]; then
    python3 -I "$resume_dir/compare.py" --cycles "$cycles" --workloads "$workloads" "$out/results" |
        tee "$out/summary.md" || status=$?
else
    python3 -I "$resume_dir/summarize.py" "$out/results" "$reps" 2 | tee "$out/summary.md" || status=$?
fi
exit "$status"
