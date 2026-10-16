package main

import (
	"strings"
	"time"
)

const applicationMissingGrace = 30 * time.Second

func applicationRequestScript(script string) string {
	return "printf '%s' " + quote(script) + " >/tmp/application.pending && mv /tmp/application.pending /tmp/application.request"
}

func applicationProcessScript(args []string, completeAccounting string) string {
	return "set +e; " + joinCommand(args) + " >/tmp/application.log 2>&1; code=$?; (" + completeAccounting +
		"); if test $? -ne 0; then printf '%s\\n' 'cannot publish application memory accounting' >/tmp/application.oom-error.pending && " +
		"mv /tmp/application.oom-error.pending /tmp/application.oom-error || exit 125; fi; " +
		"printf '%s\\n' \"$code\" >/tmp/application.exit.pending && mv /tmp/application.exit.pending /tmp/application.exit || exit 125; exit \"$code\""
}

func applicationTaskRunning(output string) bool {
	for line := range strings.SplitSeq(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "application" && fields[2] == "RUNNING" {
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
