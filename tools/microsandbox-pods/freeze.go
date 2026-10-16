package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// Only application cgroups are frozen. The guest agent, containerd and root
// supervisor remain available to refresh projections after a RAM restore.
type applicationFreeze struct {
	Version   int          `json:"version"`
	Phase     string       `json:"phase"`
	Operation string       `json:"operation"`
	Tasks     []frozenTask `json:"tasks"`
}

type frozenTask struct {
	Task       string `json:"task"`
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"startTicks"`
}

func renewableIdentity(e *podEntry) bool {
	for _, volume := range mountedVolumes(e.pod) {
		if projectedToken(volume) {
			return true
		}
	}
	return false
}

func freezeTaskNames(e *podEntry) []string {
	var tasks []string
	for container := range managedContainers(e.pod) {
		name := container.container(e).Name
		statuses := e.pod.Status.ContainerStatuses
		if container.init {
			statuses = e.pod.Status.InitContainerStatuses
		}
		terminated := slices.ContainsFunc(statuses, func(status corev1.ContainerStatus) bool {
			return status.Name == name && status.State.Terminated != nil
		})
		if !terminated {
			tasks = append(tasks, container.task(e))
		}
	}
	return tasks
}

func validateFreeze(e *podEntry) error {
	freeze := e.freeze
	if freeze == nil {
		return nil
	}
	if freeze.Version != 1 || !slices.Contains([]string{"FREEZING", "FROZEN", "THAWING"}, freeze.Phase) || len(freeze.Operation) < 20 || len(freeze.Tasks) == 0 {
		return errors.New("invalid application freeze journal")
	}
	for _, c := range freeze.Operation {
		if c < 'A' || c > 'Z' {
			if c < '2' || c > '7' {
				return errors.New("invalid application freeze operation")
			}
		}
	}
	wanted := freezeTaskNames(e)
	if len(wanted) != len(freeze.Tasks) {
		return errors.New("application freeze does not match managed containers")
	}
	for i, task := range freeze.Tasks {
		if task.Task != wanted[i] || task.PID <= 0 || task.StartTicks == 0 {
			return errors.New("application freeze has a different task identity")
		}
	}
	return nil
}

func (freeze *applicationFreeze) marker(action string) string {
	return "/run/floci-freeze/" + freeze.Operation + "." + action
}

func inspectFreezeTask(task string) string {
	// /proc's comm field may contain spaces and closing parentheses. Strip up
	// through its final delimiter before selecting field 22 (starttime).
	return "row=$(" + ctr + " tasks list | awk -v task=" + quote(task) + " '$1 == task {print $2, $3}'); " +
		"set -- $row; test \"$#\" -eq 2; pid=$1; state=$2; case $pid in ''|*[!0-9]*) exit 125;; esac; " +
		"ticks=$(sed 's/.*) //' /proc/$pid/stat | awk '{print $20}'); test -n \"$ticks\"; "
}

func freezeObservationScript(tasks []string) string {
	var script strings.Builder
	for _, task := range tasks {
		script.WriteString(inspectFreezeTask(task))
		script.WriteString("test \"$state\" = RUNNING; printf '%s %s %s\\n' " + quote(task) + " \"$pid\" \"$ticks\"; ")
	}
	return script.String()
}

func parseFrozenTasks(output string, wanted []string) ([]frozenTask, error) {
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != len(wanted) || len(wanted) == 0 {
		return nil, errors.New("application task inventory changed before freeze")
	}
	tasks := make([]frozenTask, len(wanted))
	for i, line := range lines {
		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != wanted[i] {
			return nil, errors.New("application task inventory changed before freeze")
		}
		pid, pidErr := strconv.Atoi(fields[1])
		ticks, ticksErr := strconv.ParseUint(fields[2], 10, 64)
		if pidErr != nil || ticksErr != nil || pid <= 0 || ticks == 0 {
			return nil, errors.New("invalid application process identity")
		}
		tasks[i] = frozenTask{Task: fields[0], PID: pid, StartTicks: ticks}
	}
	return tasks, nil
}

func freezeMutationScript(freeze *applicationFreeze, action string) string {
	marker := quote(freeze.marker(action))
	completion := "code=$?; trap - EXIT; printf '%s\\n' \"$code\" >" + marker + ".pending && mv " + marker + ".pending " + marker + "; exit \"$code\""
	script := "umask 077; mkdir -p /run/floci-freeze; test ! -e " + marker + "; trap " + quote(completion) + " EXIT; "
	for _, task := range freeze.Tasks {
		script += inspectFreezeTask(task.Task)
		script += fmt.Sprintf("test \"$pid\" = %d; test \"$ticks\" = %d; ", task.PID, task.StartTicks)
		if action == "pause" {
			script += "test \"$state\" = RUNNING; " + ctr + " tasks pause " + quote(task.Task) + "; "
		} else {
			script += "case $state in PAUSED) " + ctr + " tasks resume " + quote(task.Task) + ";; RUNNING) ;; *) exit 125;; esac; "
		}
		script += inspectFreezeTask(task.Task)
		wanted := "PAUSED"
		if action == "resume" {
			wanted = "RUNNING"
		}
		script += fmt.Sprintf("test \"$pid\" = %d; test \"$ticks\" = %d; test \"$state\" = %s; ", task.PID, task.StartTicks, wanted)
	}
	return script
}

func (p *provider) freezeCompletion(ctx context.Context, e *podEntry, action string) (int, error) {
	output, err := p.runner.guest(ctx, e.vm, "cat "+quote(e.freeze.marker(action)))
	if err != nil {
		return -1, errors.New("application freeze command has no completion proof")
	}
	code, err := strconv.Atoi(strings.TrimSpace(output))
	if err != nil || code < 0 || code > 255 {
		return -1, errors.New("invalid application freeze completion proof")
	}
	return code, nil
}

func (p *provider) freezeApplications(ctx context.Context, e *podEntry) error {
	if !renewableIdentity(e) {
		return nil
	}
	if e.freeze != nil {
		return errors.New("an earlier application freeze has not been reconciled")
	}
	wanted := freezeTaskNames(e)
	output, err := p.runner.guest(ctx, e.vm, freezeObservationScript(wanted))
	if err != nil {
		return fmt.Errorf("cannot freeze applications which are starting or exiting: %w", err)
	}
	tasks, err := parseFrozenTasks(output, wanted)
	if err != nil {
		return err
	}
	e.freeze = &applicationFreeze{Version: 1, Phase: "FREEZING", Operation: rand.Text(), Tasks: tasks}
	if err := p.journal(e); err != nil {
		return err
	}
	// Do not abandon accepted guest mutations at an HTTP timeout. The durable
	// guest completion marker is also required when recovering after a crash.
	if _, err := p.runner.guest(context.WithoutCancel(ctx), e.vm, freezeMutationScript(e.freeze, "pause")); err != nil {
		return err
	}
	code, err := p.freezeCompletion(ctx, e, "pause")
	if err != nil || code != 0 {
		return errors.Join(err, errors.New("application freeze did not complete successfully"))
	}
	e.freeze.Phase = "FROZEN"
	return p.journal(e)
}

func requireFrozenSnapshot(e *podEntry) error {
	if !renewableIdentity(e) {
		return nil
	}
	if err := validateFreeze(e); err != nil {
		return err
	}
	if e.freeze == nil || e.freeze.Phase != "FROZEN" {
		return errors.New("identity checkpoint has no application freeze proof; recreate this Pod before hibernating it again")
	}
	return nil
}

func (p *provider) thawApplications(ctx context.Context, e *podEntry) error {
	if e.freeze == nil {
		return nil
	}
	if err := validateFreeze(e); err != nil {
		return err
	}
	action := "pause"
	if e.freeze.Phase == "THAWING" {
		action = "resume"
	}
	code, err := p.freezeCompletion(ctx, e, action)
	if err != nil {
		return err
	}
	if e.freeze.Phase == "FROZEN" && code != 0 {
		return errors.New("checkpoint application freeze was not completed")
	}
	if e.freeze.Phase == "THAWING" {
		// Never race a lost resume command or replay a partially completed one.
		if code != 0 {
			return errors.New("application thaw requires explicit recovery")
		}
		e.freeze = nil
		return p.journal(e)
	}
	e.freeze.Phase = "THAWING"
	if err := p.journal(e); err != nil {
		return err
	}
	if _, err := p.runner.guest(context.WithoutCancel(ctx), e.vm, freezeMutationScript(e.freeze, "resume")); err != nil {
		return err
	}
	code, err = p.freezeCompletion(ctx, e, "resume")
	if err != nil || code != 0 {
		return errors.Join(err, errors.New("application thaw did not complete successfully"))
	}
	e.freeze = nil
	return p.journal(e)
}

func (p *provider) refreshAndThaw(ctx context.Context, e *podEntry) error {
	if e.freeze == nil {
		return nil
	}
	if err := p.preflightWakeTokens(ctx, e); err != nil {
		return err
	}
	e.volumeDigests = nil
	if err := p.refreshVolumeProjections(ctx, e, false, true); err != nil {
		return err
	}
	return p.thawApplications(ctx, e)
}

func (p *provider) rollbackFreeze(e *podEntry, cause error) error {
	ctx := context.Background()
	if err := p.requireVMStatus(ctx, e, "Running"); err != nil {
		return p.transitionFailed(e, errors.Join(cause, err))
	}
	if err := p.refreshAndThaw(ctx, e); err != nil {
		return p.transitionFailed(e, errors.Join(cause, err))
	}
	e.state = "RUNNING"
	return errors.Join(cause, p.journal(e))
}
