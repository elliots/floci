package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/floci-io/floci/tools/microsandbox-pods/internal/healthprobe"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

type probeProgress struct {
	at                  time.Time
	successes, failures int32
	passed              bool
}

func validateProbes(container corev1.Container) error {
	for kind, probe := range map[string]*corev1.Probe{"readiness": container.ReadinessProbe, "startup": container.StartupProbe, "liveness": container.LivenessProbe} {
		if probe == nil {
			continue
		}
		if probe.InitialDelaySeconds < 0 || probe.PeriodSeconds < 0 || probe.TimeoutSeconds < 0 || probe.SuccessThreshold < 0 || probe.FailureThreshold < 0 ||
			(kind != "readiness" && probe.SuccessThreshold > 1) || (probe.TerminationGracePeriodSeconds != nil && (kind == "readiness" || *probe.TerminationGracePeriodSeconds < 1)) {
			return fmt.Errorf("invalid %s probe timing or thresholds", kind)
		}
		handlers := 0
		if probe.Exec != nil {
			handlers++
			if len(probe.Exec.Command) == 0 {
				return errors.New("exec probes require a command")
			}
		}
		if probe.HTTPGet != nil {
			handlers++
			action := probe.HTTPGet
			if action.Host != "" || (action.Scheme != "" && action.Scheme != corev1.URISchemeHTTP && action.Scheme != corev1.URISchemeHTTPS) {
				return errors.New("HTTP probes require the Pod address and HTTP/HTTPS; use an HTTP Host header for virtual hosts")
			}
			if _, err := probePort(action.Port, container); err != nil {
				return err
			}
		}
		if probe.TCPSocket != nil {
			handlers++
			if probe.TCPSocket.Host != "" {
				return errors.New("TCP probe host overrides are not supported")
			}
			if _, err := probePort(probe.TCPSocket.Port, container); err != nil {
				return err
			}
		}
		if handlers != 1 || probe.GRPC != nil {
			return fmt.Errorf("%s probe requires exactly one exec, HTTP or TCP handler; gRPC is not supported", kind)
		}
	}
	return nil
}

func probePort(port intstr.IntOrString, container corev1.Container) (int, error) {
	value := int(port.IntVal)
	if port.Type == intstr.String {
		value = 0
		for _, declared := range container.Ports {
			if declared.Name == port.StrVal && (declared.Protocol == "" || declared.Protocol == corev1.ProtocolTCP) {
				value = int(declared.ContainerPort)
				break
			}
		}
	}
	if value < 1 || value > 65535 {
		return 0, fmt.Errorf("probe port %s does not identify a TCP port", port.String())
	}
	return value, nil
}

func networkProbes(container corev1.Container) bool {
	for _, probe := range []*corev1.Probe{container.ReadinessProbe, container.StartupProbe, container.LivenessProbe} {
		if probe != nil && (probe.HTTPGet != nil || probe.TCPSocket != nil) {
			return true
		}
	}
	return false
}

func (p *provider) executeProbe(ctx context.Context, e *podEntry, probe *corev1.Probe) (bool, error) {
	return p.executeContainerProbe(ctx, e, e.pod.Spec.Containers[0], "application", probe)
}

func (p *provider) executeContainerProbe(ctx context.Context, e *podEntry, container corev1.Container, task string, probe *corev1.Probe) (bool, error) {
	timeout := max(probe.TimeoutSeconds, 1)
	check, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	if probe.Exec != nil {
		_, err := p.runner.guest(check, e.vm, ctr+" tasks exec --exec-id probe-"+strconv.FormatInt(time.Now().UnixNano(), 10)+" "+quote(task)+" "+joinCommand(probe.Exec.Command))
		return err == nil, nil
	}
	address := e.ip
	if e.proxyAvailability {
		address = e.backendIP
	}
	spec := healthprobe.Request{Address: address, TimeoutSeconds: timeout}
	var err error
	if action := probe.HTTPGet; action != nil {
		spec.Scheme, spec.Path = string(cmp.Or(action.Scheme, corev1.URISchemeHTTP)), action.Path
		spec.Port, err = probePort(action.Port, container)
		spec.Headers = make(http.Header)
		for _, header := range action.HTTPHeaders {
			spec.Headers.Add(header.Name, header.Value)
		}
	} else if action := probe.TCPSocket; action != nil {
		spec.Scheme = "TCP"
		spec.Port, err = probePort(action.Port, container)
	} else {
		return false, errors.New("missing supported probe handler")
	}
	if err != nil {
		return false, err
	}
	// Transport setup is independent of the application timeout. The actual check
	// uses its own deadline inside the peer and never goes through a wake proxy.
	relay, err := p.ensureRelay(ctx)
	if err != nil {
		return false, err
	}
	data, err := json.Marshal(spec)
	if err != nil {
		return false, err
	}
	transport, stop := context.WithTimeout(ctx, time.Duration(timeout)*time.Second+5*time.Second)
	defer stop()
	command := p.runner.command(transport, "docker", "exec", "-i", p.runner.cfg.peer, relay.directory+"/relay", "probe")
	command.Stdin = strings.NewReader(string(data))
	output, err := command.Output()
	if err != nil {
		return false, fmt.Errorf("execute network probe: %w", err)
	}
	var result healthprobe.Result
	if err := json.Unmarshal(output, &result); err != nil {
		return false, err
	}
	if result.Warning != "" {
		slog.Warn("application probe warning", "pod", e.pod.Name, "warning", result.Warning)
	}
	return result.Success, nil
}

func (s *probeProgress) observe(success bool, probe *corev1.Probe) {
	if success {
		s.failures = 0
		s.successes = min(s.successes+1, max(probe.SuccessThreshold, 1))
		if s.successes >= max(probe.SuccessThreshold, 1) {
			s.passed = true
		}
	} else {
		s.successes = 0
		s.failures = min(s.failures+1, cmp.Or(probe.FailureThreshold, 3))
		if s.failures >= cmp.Or(probe.FailureThreshold, 3) {
			s.passed = false
		}
	}
}

func (p *provider) evaluateProbe(ctx context.Context, e *podEntry, probe *corev1.Probe, progress *probeProgress) {
	p.evaluateContainerProbe(ctx, e, e.pod.Spec.Containers[0], "application", e.started, probe, progress)
}

func (p *provider) evaluateContainerProbe(ctx context.Context, e *podEntry, container corev1.Container, task string, started time.Time, probe *corev1.Probe, progress *probeProgress) {
	if time.Since(started) < time.Duration(probe.InitialDelaySeconds)*time.Second || time.Since(progress.at) < time.Duration(cmp.Or(probe.PeriodSeconds, 10))*time.Second {
		return
	}
	progress.at = time.Now()
	success, err := p.executeContainerProbe(ctx, e, container, task, probe)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		// A broken control transport is not evidence that the app failed its probe.
		progress.successes, progress.failures = 0, 0
		slog.Warn("cannot execute application probe", "pod", e.pod.Name, "error", err)
		return
	}
	progress.observe(success, probe)
}

func (p *provider) probe(ctx context.Context, e *podEntry) bool {
	if e.state != "RUNNING" && e.state != "READY" {
		return false
	}
	probe := e.pod.Spec.Containers[0].ReadinessProbe
	if probe == nil {
		return true
	}
	p.evaluateProbe(ctx, e, probe, &e.readiness)
	return e.readiness.passed
}

func (p *provider) healthChecks(ctx context.Context, e *podEntry) (*corev1.Probe, string, error) {
	return p.containerHealthChecks(ctx, e, e.pod.Spec.Containers[0], "application", &e.applicationProgress)
}

func (p *provider) containerHealthChecks(ctx context.Context, e *podEntry, container corev1.Container, task string, progress *applicationProgress) (*corev1.Probe, string, error) {
	if progress.probeFailure != "" {
		if progress.probeFailure == "StartupProbeFailed" {
			return container.StartupProbe, progress.probeFailure, nil
		}
		return container.LivenessProbe, progress.probeFailure, nil
	}
	if !progress.startupComplete {
		if probe := container.StartupProbe; probe != nil {
			p.evaluateContainerProbe(ctx, e, container, task, progress.started, probe, &progress.startup)
			if progress.startup.failures >= cmp.Or(probe.FailureThreshold, 3) {
				return probe, "StartupProbeFailed", nil
			}
			if !progress.startup.passed {
				return nil, "", nil
			}
		}
		progress.startupComplete = true
		if err := p.journal(e); err != nil {
			return nil, "", err
		}
	}
	if probe := container.LivenessProbe; probe != nil {
		p.evaluateContainerProbe(ctx, e, container, task, progress.started, probe, &progress.liveness)
		if progress.liveness.failures >= cmp.Or(probe.FailureThreshold, 3) {
			return probe, "LivenessProbeFailed", nil
		}
	}
	return nil, "", nil
}

func (p *provider) stopUnhealthy(ctx context.Context, e *podEntry, probe *corev1.Probe, reason string) error {
	e.probeFailure = reason
	if err := p.journal(e); err != nil {
		return err
	}
	p.unavailable(e, reason)
	grace := int64(corev1.DefaultTerminationGracePeriodSeconds)
	if value := e.pod.Spec.TerminationGracePeriodSeconds; value != nil {
		grace = *value
	}
	if probe != nil && probe.TerminationGracePeriodSeconds != nil {
		grace = *probe.TerminationGracePeriodSeconds
	}
	signal := func(name string) error {
		check, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		_, err := p.runner.guest(check, e.vm, "test -f /tmp/application.exit || "+ctr+" tasks kill --signal "+name+" application || test -f /tmp/application.exit")
		return err
	}
	if err := signal("SIGTERM"); err != nil {
		return err
	}
	deadline := time.Now().Add(time.Duration(grace) * time.Second)
	for {
		check, cancel := context.WithTimeout(ctx, 5*time.Second)
		output, err := p.runner.guest(check, e.vm, "if test -f /tmp/application.exit; then echo EXIT; fi")
		cancel()
		if err != nil || output == "EXIT" {
			return err
		}
		if time.Now().After(deadline) {
			return signal("SIGKILL")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func resetProbeProgress(e *podEntry) {
	e.readiness, e.startup, e.liveness = probeProgress{}, probeProgress{}, probeProgress{}
}
