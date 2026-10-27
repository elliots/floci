#!/usr/bin/env bash
# Runs in the node, one step of the resume check at a time: the workloads in
# pod VMs that resume-harness drives with smolvm as a library, with a local
# pause. check.sh runs it with the check's share at /mnt/resume.
#
#   run-pod.sh setup LABEL [SLEEP_MS]     copy the programs, record the node;
#                                         with SLEEP_MS, khugepaged sleeps that
#                                         long between scans and allocations
#   run-pod.sh create LABEL WORKLOAD      create the pod VM and start it once
#   run-pod.sh rep LABEL WORKLOAD N       repetition N: a cold start, 2 pause and
#                                         resume cycles, a stop
#   run-pod.sh series LABEL WORKLOAD N IDLE_S SETTLE_S DELTA [DIRTY]
#                                         a series of pauses for the save
#                                         benchmark: a cold start and a pause,
#                                         N cycles of a resume, a DIRTY request
#                                         to memhold, IDLE_S s idle and a pause,
#                                         with delta saves DELTA (on or off)
#   run-pod.sh delete LABEL WORKLOAD      delete the pod VM
#   run-pod.sh info LABEL FILE            record the node's state in FILE
#
# WORKLOAD is memhold, petclinic, or memhold-NAME for another memhold pod VM.
# Records and logs go to /mnt/resume/results-LABEL.
set -euo pipefail
command=$1
label=$2
share=/mnt/resume
# Pod VMs share the programs from the node's own disk, not from the Mac.
programs=/opt/resume-check
out=$share/results-$label
harness=$programs/resume-harness
busybox=public.ecr.aws/docker/library/busybox@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e
tomcat=public.ecr.aws/docker/library/tomcat@sha256:043a4c4ba1d2d0de665cabed064b1b159d4d8756ec100ca0d461b8c711bc147c
thp=/sys/kernel/mm/transparent_hugepage
mkdir -p "$out"
# The VM process's stderr, which the driver saves after each resume, gets
# smolvm's boot timing and libkrun's RAM read.
export RUST_LOG=${RUST_LOG:-info}
export SMOLVM_BOOT_DEBUG=1
export LIBKRUN_TIMING=1
export NO_COLOR=1

node_info() {
    uname -a
    nproc
    getconf PAGESIZE
    grep -E 'MemTotal|MemFree|AnonHugePages|FilePmdMapped|Hugepagesize' /proc/meminfo
    local f
    for f in enabled defrag shmem_enabled hpage_pmd_size khugepaged/defrag khugepaged/pages_to_scan \
        khugepaged/scan_sleep_millisecs khugepaged/alloc_sleep_millisecs khugepaged/max_ptes_none \
        khugepaged/max_ptes_swap khugepaged/max_ptes_shared khugepaged/full_scans \
        khugepaged/pages_collapsed; do
        if [ -e "$thp/$f" ]; then
            echo "thp $f: $(cat "$thp/$f")"
        else
            echo "thp $f: absent"
        fi
    done
    grep -E '^thp_' /proc/vmstat || true
    sha256sum "$SMOLVM_LIB_DIR/libkrun.so" "$harness" 2>/dev/null || true
    "$harness" _build-info 2>/dev/null | sed 's/^/harness /' || true
    sed 's/^/build-info /' /etc/floci-node/build-info
}

# kind_of WORKLOAD: the program a workload's pod VM runs.
kind_of() {
    case $1 in
        memhold | memhold-*) echo memhold ;;
        petclinic) echo petclinic ;;
        *) echo "unknown workload $1" >&2; exit 2 ;;
    esac
}

port_of() {
    case $(kind_of "$1") in
        memhold) echo 18090 ;;
        petclinic) echo 18091 ;;
        *) echo "unknown workload $1" >&2; exit 2 ;;
    esac
}

image_of() {
    case $(kind_of "$1") in
        memhold) echo "$busybox" ;;
        petclinic) echo "$tomcat" ;;
    esac
}

# driver WORKLOAD ARGS...: the driver for machine floci-resume-LABEL-WORKLOAD.
driver() {
    local workload=$1 kind port
    shift
    kind=$(kind_of "$workload")
    port=$(port_of "$workload")
    "$programs/driver" -lib "$harness" -liblog "$out/$workload.smolvm.log" \
        -vmmlog "$out/$workload.vmm.log" -name "floci-resume-$label-$workload" \
        -workload "$kind" -port "$port" "$@"
}

case $command in
    setup)
        mkdir -p "$programs"
        # install replaces each file with a new one, so a binary still running
        # from an earlier step does not make the copy fail.
        install -m 0755 "$share/memhold" "$share/driver" "$share/resume-harness" "$programs/"
        rm -rf "$programs/petclinic"
        cp -a "$share/petclinic" "$programs/"
        chmod -R a+rX "$programs"
        if [ -n "${3:-}" ]; then
            echo "$3" >"$thp/khugepaged/scan_sleep_millisecs"
            echo "$3" >"$thp/khugepaged/alloc_sleep_millisecs"
        fi
        node_info >"$out/node-info.txt"
        ;;
    create)
        driver "$3" -create "$(image_of "$3")" -share "$programs" -prepare 2>>"$out/$3.log"
        ;;
    rep)
        driver "$3" -node -reps 1 -first "${4:-1}" -cycles 2 -label "$label" \
            >>"$out/$3.jsonl" 2>>"$out/$3.log"
        tail -n 1 "$out/$3.jsonl"
        ;;
    series)
        case $7 in
            on | off) export SMOLVM_DELTA_SAVES=$7 ;;
            *) echo "DELTA must be on or off, not $7" >&2; exit 2 ;;
        esac
        disk=/var/lib/smolvm
        [ "$6" = 0 ] && disk=
        driver "$3" -node -series "$4" -idle "${5}s" -settle "${6}s" -disk "$disk" -dirty "${8:-}" -label "$label" \
            >>"$out/$3.series.jsonl" 2>>"$out/$3.log"
        tail -n 1 "$out/$3.series.jsonl"
        ;;
    delete)
        driver "$3" -delete 2>>"$out/$3.log"
        ;;
    info)
        node_info >"$out/$3"
        ;;
    *)
        echo "unknown command $command" >&2
        exit 2
        ;;
esac
