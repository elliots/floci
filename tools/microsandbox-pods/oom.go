package main

import (
	"crypto/rand"
	"fmt"
	"path"
	"strings"
)

type applicationOOMTracking struct {
	Name     string
	Args     []string
	Setup    string
	Complete string
}

// The owned parent retains its hierarchical oom_kill counter after ctr removes
// the application child. The kernel increments ancestors before killing a
// victim, so even an immediate exit needs no userspace event-delivery delay.
func newApplicationOOMTracking(generation uint64) applicationOOMTracking {
	name := fmt.Sprintf("floci-application-g%d-%s", generation, strings.ToLower(rand.Text()))
	return buildApplicationOOMTracking(name, "/sys/fs/cgroup", "/tmp", "/proc/mounts")
}

func buildApplicationOOMTracking(name, cgroupRoot, stateRoot, mounts string) applicationOOMTracking {
	parent := path.Join(cgroupRoot, name)
	used := path.Join(stateRoot, "application-oom-starts")
	marker := path.Join(stateRoot, "application.oom")
	failure := path.Join(stateRoot, "application.oom-error")
	setup := `set -eu
awk -v root=` + quote(cgroupRoot) + ` '
    $2 == root && $3 == "cgroup2" {
        found = 1
        if (index("," $4 ",", ",memory_localevents,")) invalid = 1
    }
    END { exit !found || invalid }
' ` + quote(mounts) + ` || { echo 'OOM tracking requires hierarchical cgroup-v2 memory events' >&2; exit 1; }
for controller in cpu memory; do
    awk -v required="$controller" '{ for (i = 1; i <= NF; i++) if ($i == required) found = 1 } END { exit !found }' ` + quote(path.Join(cgroupRoot, "cgroup.controllers")) + ` || { echo "missing cgroup controller: $controller" >&2; exit 1; }
done
mkdir -p ` + quote(used) + `
mkdir ` + quote(path.Join(used, name)) + `
printf '%s\n' '+cpu +memory' >` + quote(path.Join(cgroupRoot, "cgroup.subtree_control")) + `
mkdir ` + quote(parent) + `
printf '%s\n' '+cpu +memory' >` + quote(path.Join(parent, "cgroup.subtree_control")) + `
awk '$1 == "oom_kill" { seen++; if (NF != 2 || $2 !~ /^0+$/) invalid = 1 } END { exit seen != 1 || invalid }' ` + quote(path.Join(parent, "memory.events")) + `
rm -f ` + quote(marker) + ` ` + quote(marker+".pending") + ` ` + quote(failure) + ` ` + quote(failure+".pending") + `
`
	complete := `
if oom_count=$(awk '
    $1 == "oom_kill" {
        seen++
        if (NF != 2 || $2 !~ /^[0-9]+$/) invalid = 1
        value = $2
    }
    END { if (seen != 1 || invalid) exit 1; print value }
' ` + quote(path.Join(parent, "memory.events")) + `); then
    case "$oom_count" in
        *[1-9]*)
            printf '%s %s\n' ` + quote(name) + ` "$oom_count" >` + quote(marker+".pending") + ` && mv ` + quote(marker+".pending") + ` ` + quote(marker) + ` || exit 125
            ;;
    esac
    if ! rmdir ` + quote(parent) + `; then
        echo ` + quote("could not remove completed application cgroup "+name) + ` >&2
    fi
else
    printf '%s\n' ` + quote("cannot read reliable OOM counter for "+name) + ` >` + quote(failure+".pending") + ` && mv ` + quote(failure+".pending") + ` ` + quote(failure) + ` || exit 125
fi
`
	return applicationOOMTracking{
		Name: name,
		Args: []string{
			"--cgroup", "/" + name + "/application",
			// ctr omits the cgroup namespace and mount that CRI normally adds.
			// An empty namespace path creates a new namespace rooted at this
			// container, exposing its own limits without the guest hierarchy.
			"--with-ns", "cgroup:",
			"--mount", "type=cgroup,src=cgroup,dst=/sys/fs/cgroup,options=nosuid:noexec:nodev:relatime:ro",
		},
		Setup: setup, Complete: complete,
	}
}
