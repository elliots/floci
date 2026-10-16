package main

import (
	"path"
	"strings"
	"time"
)

const applicationMissingGrace = 30 * time.Second

func applicationRequestScript(script string) string {
	return "printf '%s' " + quote(script) + " >/tmp/application.pending && mv /tmp/application.pending /tmp/application.request"
}

func applicationProcessScript(args []string, completeAccounting string) string {
	return containerProcessScript(args, completeAccounting, "/tmp")
}

func containerProcessScript(args []string, completeAccounting, root string) string {
	log := quote(path.Join(root, "application.log"))
	failure := quote(path.Join(root, "application.oom-error"))
	failurePending := quote(path.Join(root, "application.oom-error.pending"))
	exit := quote(path.Join(root, "application.exit"))
	exitPending := quote(path.Join(root, "application.exit.pending"))
	return "set +e; " + joinCommand(args) + " >" + log + " 2>&1; code=$?; (" + completeAccounting +
		"); if test $? -ne 0; then printf '%s\\n' 'cannot publish container memory accounting' >" + failurePending + " && " +
		"mv " + failurePending + " " + failure + " || exit 125; fi; " +
		"printf '%s\\n' \"$code\" >" + exitPending + " && mv " + exitPending + " " + exit + " || exit 125; exit \"$code\""
}

func applicationTaskRunning(output string) bool {
	return containerTaskRunning(output, "application")
}

func containerTaskRunning(output, task string) bool {
	for line := range strings.SplitSeq(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == task && fields[2] == "RUNNING" {
			return true
		}
	}
	return false
}

// Missing tasks can briefly precede a supervisor's exit marker. Give that
// handoff time to finish without leaving a lost application Ready indefinitely.
func (e *podEntry) observeApplicationTask(output string, now time.Time) (running, expired bool) {
	if applicationTaskRunning(output) {
		e.applicationMissingSince = time.Time{}
		return true, false
	}
	if e.applicationMissingSince.IsZero() {
		e.applicationMissingSince = now
	}
	return false, now.Sub(e.applicationMissingSince) >= applicationMissingGrace
}
