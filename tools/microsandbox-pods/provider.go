package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/floci-io/floci/tools/microsandbox-runtime/guesttask"
	dto "github.com/prometheus/client_model/go"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
)

type provider struct {
	registrations   chan struct{}
	idleJobs        chan struct{}
	stop            context.CancelFunc
	proxy           *wakeProxy
	relayMu         sync.Mutex
	relay           *networkRelay
	stateLock       *os.File
	client          kubernetes.Interface
	budgetMu        sync.Mutex
	activeMemoryMiB int
	ctx             context.Context
	runner          commandRunner
	network         *network
	mu              sync.Mutex
	pods            map[string]*podEntry
	notify          func(*corev1.Pod)
	starts          chan struct{}
	jobs            sync.WaitGroup
}

type podEntry struct {
	applicationProgress
	applications                                     map[string]*applicationProgress
	idleScheduled                                    bool
	proxyAvailability, proxyRoutesReady, actualReady bool
	proxyRouteFailed                                 atomic.Bool
	publication                                      sync.Mutex
	proxyPorts                                       []proxyPortBinding
	backendIP, idleOwner                             string
	idleTimeout                                      time.Duration
	lastActivity                                     time.Time
	activeConnections                                int
	publishedView                                    atomic.Pointer[lifecycleView]
	wake                                             chan struct{}
	snapshotIDs                                      map[string]string
	restoredParent                                   string
	allocated                                        bool
	allocation                                       podAllocation
	op                                               sync.Mutex
	lifetime                                         context.Context
	state                                            string
	generation                                       uint64
	snapshots                                        []string
	httpPort, appPort                                int
	deleted                                          bool
	mu                                               sync.Mutex
	pod                                              *corev1.Pod
	declaredSpec                                     *corev1.PodSpec
	vm, link, ip, guestIP                            string
	slot, port                                       int
	cancel                                           context.CancelFunc
	done                                             chan struct{}
	created                                          bool
	applicationMissingSince                          time.Time
	volumeDigests                                    map[string][32]byte
	volumesChecked                                   time.Time
	initialVolumes                                   map[string]map[string]volumeFile
}

func newProvider(ctx context.Context, cfg config, client kubernetes.Interface) (*provider, error) {
	stateLock, reusing, err := openState(ctx, cfg, client)
	if err != nil {
		return nil, err
	}
	succeeded := false
	defer func() {
		if !succeeded {
			stateLock.Close()
		}
	}()
	runner, err := newCommandRunner(cfg)
	if err != nil {
		return nil, err
	}
	if err := runner.checkPending(); err != nil {
		return nil, err
	}
	if err := runner.verifyNetworkBoundary(ctx); err != nil {
		return nil, fmt.Errorf("verify native environment network boundary: %w", err)
	}
	version, err := runner.vm(ctx, "--version")
	if err != nil {
		return nil, err
	}
	if version != "msb 0.7.8" {
		return nil, fmt.Errorf("requires microsandbox 0.7.8, got %s", version)
	}
	if _, err := runner.vm(ctx, "doctor"); err != nil {
		return nil, err
	}
	network, err := newNetwork(ctx, runner, reusing)
	if err != nil {
		return nil, err
	}
	lifetime, stop := context.WithCancel(ctx)
	defer func() {
		if !succeeded {
			stop()
		}
	}()
	p := &provider{registrations: make(chan struct{}, 8), idleJobs: make(chan struct{}, 2), stop: stop, stateLock: stateLock, client: client, ctx: lifetime, runner: runner, network: network, pods: make(map[string]*podEntry), starts: make(chan struct{}, 2)}
	p.proxy, err = newWakeProxy(lifetime, 1024, 4*time.Minute)
	if err != nil {
		return nil, err
	}
	if err := p.recover(ctx); err != nil {
		return nil, err
	}
	p.jobs.Go(func() { p.watchAvailability(lifetime) })
	succeeded = true
	return p, nil
}

func validatePod(pod *corev1.Pod) error {
	return validatePodWithCA(pod, "")
}

func validatePodWithCA(pod *corev1.Pod, caPath string) error {
	spec := pod.Spec
	if len(spec.Containers) == 0 || len(spec.EphemeralContainers) != 0 {
		return errdefs.InvalidInput("at least one regular container is required; ephemeral containers are not yet supported")
	}

	if spec.HostNetwork || spec.HostPID || spec.HostIPC ||
		spec.ShareProcessNamespace != nil {
		return errdefs.InvalidInput("host namespaces and shared process namespaces are not qualified")
	}
	if err := validateSecurityContexts(pod); err != nil {
		return errdefs.InvalidInput(err.Error())
	}
	if spec.DNSPolicy != corev1.DNSClusterFirst || spec.DNSConfig != nil {
		return errdefs.InvalidInput("qualification supports ClusterFirst DNS without overrides")
	}
	if _, _, _, err := proxySettings(pod); err != nil {
		return err
	}
	if _, err := derivePodAllocation(pod); err != nil {
		return errdefs.InvalidInput(err.Error())
	}
	for _, container := range spec.Containers {
		if len(container.EnvFrom) != 0 {
			return errdefs.InvalidInput("unresolved envFrom source")
		}
		if container.RestartPolicy != nil || len(container.RestartPolicyRules) != 0 ||
			len(container.VolumeDevices) != 0 || container.Lifecycle != nil || container.Stdin || container.TTY {
			return errdefs.InvalidInput("container restart overrides, volume devices, hooks and TTY are not qualified")
		}
		if err := validateProbes(container); err != nil {
			return errdefs.InvalidInput(err.Error())
		}
	}
	if err := validateVolumesWithCA(pod, caPath); err != nil {
		return errdefs.InvalidInput(err.Error())
	}
	if err := validateInitContainers(pod); err != nil {
		return errdefs.InvalidInput(err.Error())
	}
	return nil
}

func (p *provider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	if err := validatePodWithCA(pod, p.runner.guestCAPath()); err != nil {
		return err
	}
	allocation, err := derivePodAllocation(pod)
	if err != nil {
		return errdefs.InvalidInput(err.Error())
	}
	if podNetworkProbes(pod) && p.runner.cfg.relayBinary == "" {
		return errdefs.InvalidInput("HTTP/TCP/gRPC probes require --network-relay-binary")
	}
	if p.client != nil {
		if err := p.checkNetworkPolicy(ctx, pod); err != nil {
			return err
		}
	}
	declared, err := p.readDeclaredPod(ctx, pod)
	if err != nil {
		return err
	}
	key := pod.Namespace + "/" + pod.Name
	p.mu.Lock()
	if existing := p.pods[key]; existing != nil {
		p.mu.Unlock()
		if existing.pod.UID == pod.UID {
			return nil
		}
		return errdefs.InvalidInput("a previous Pod UID still owns this name")
	}
	if allocation.MemoryMiB > p.runner.cfg.maxActiveMemoryMiB {
		p.mu.Unlock()
		return errdefs.InvalidInput(fmt.Sprintf("pod requires %d MiB of guest RAM, exceeding --max-active-memory-mib %d", allocation.MemoryMiB, p.runner.cfg.maxActiveMemoryMiB))
	}
	used := make(map[int]bool)
	ports := make(map[int]bool)
	for _, existing := range p.pods {
		used[existing.slot] = true
		ports[existing.port] = true
		ports[existing.httpPort] = true
		for _, binding := range existing.proxyPorts {
			ports[binding.BackendPort] = true
			ports[binding.ProxyPort] = true
		}
	}
	slot := 0
	for used[slot] {
		slot++
	}
	if slot >= p.runner.cfg.maxPods {
		p.mu.Unlock()
		return errdefs.InvalidInput("configured maximum pod count reached")
	}
	port, err := uniquePort(ports, availableUDPPort)
	if err != nil {
		p.mu.Unlock()
		return err
	}
	uid := strings.ReplaceAll(string(pod.UID), "-", "")
	if len(uid) < 20 {
		p.mu.Unlock()
		return errdefs.InvalidInput("Pod UID is missing or malformed")
	}
	httpPort := 0
	appPort := int32(0)
	for _, port := range applicationPorts(pod) {
		if port.Protocol == "" || port.Protocol == corev1.ProtocolTCP {
			appPort = port.ContainerPort
			break
		}
	}
	if appPort != 0 {
		httpPort, err = uniquePort(ports, availableTCPPort)
		if err != nil {
			p.mu.Unlock()
			return err
		}
	}
	lifetime, cancel := context.WithCancel(p.ctx)
	e := &podEntry{wake: make(chan struct{}, 1), snapshotIDs: make(map[string]string), lifetime: lifetime, state: "WAITINGVOLUMES", httpPort: httpPort, appPort: int(appPort), pod: pod.DeepCopy(), vm: "fvp-" + uid[:20], link: "pv" + uid[:10], ip: p.network.address(slot), slot: slot, port: port, cancel: cancel, done: make(chan struct{})}
	e.allocation = allocation
	e.declaredSpec = declared.Spec.DeepCopy()
	e.pod.Status = corev1.PodStatus{Phase: corev1.PodPending, Reason: "NativeVMStarting", StartTime: new(metav1.Now())}
	for _, container := range pod.Spec.Containers {
		e.pod.Status.ContainerStatuses = append(e.pod.Status.ContainerStatuses, corev1.ContainerStatus{Name: container.Name, Image: container.Image,
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}})
	}
	initializeInitStatuses(e.pod)
	e.pod.Status.Conditions = podConditionsFor(e.pod, false)
	e.proxyAvailability, e.idleOwner, e.idleTimeout, _ = proxySettings(pod)
	if e.proxyAvailability {
		e.backendIP = addressAt(p.network.backendPrefix, slot)
		for _, declared := range applicationPorts(pod) {
			backend := httpPort
			if int(declared.ContainerPort) != e.appPort {
				backend, err = uniquePort(ports, availableTCPPort)
				if err != nil {
					cancel()
					p.mu.Unlock()
					return err
				}
			}
			proxyPort, err := uniquePort(ports, availableTCPPort)
			if err != nil {
				cancel()
				p.mu.Unlock()
				return err
			}
			e.proxyPorts = append(e.proxyPorts, proxyPortBinding{ContainerPort: int(declared.ContainerPort), BackendPort: backend, ProxyPort: proxyPort})
		}
	}
	cold := e.proxyAvailability || pod.Annotations["microsandbox.dev/on-demand"] == "true"
	if cold {
		e.state = "COLD"
		e.pod.Status.Reason = "NativeVMCold"
		for i := range e.pod.Status.ContainerStatuses {
			e.pod.Status.ContainerStatuses[i].State.Waiting.Reason = "NativeVMCold"
		}
	}
	if err := p.journal(e); err != nil {
		cancel()
		p.mu.Unlock()
		return err
	}
	p.pods[key] = e
	p.mu.Unlock()
	p.publish(e)
	p.jobs.Go(func() {
		defer close(e.done)
		if e.proxyAvailability {
			e.op.Lock()
			err := p.prepareProxy(lifetime, e)
			if err != nil && lifetime.Err() == nil {
				p.fail(e, err)
			}
			e.op.Unlock()
			if err != nil {
				return
			}
		}
		if cold {
			p.monitor(lifetime, e)
			return
		}
		if err := p.startStrictPod(e); err != nil {
			return
		}
		p.monitor(lifetime, e)
	})
	return nil
}

func (p *provider) startAdmitted(ctx context.Context, e *podEntry) error {
	if podNetworkProbes(e.pod) {
		if _, err := p.ensureRelay(ctx); err != nil {
			return err
		}
	}
	if _, exists, err := p.inspectOwned(ctx, e); err != nil {
		return err
	} else if exists {
		return errors.New("owned VM already exists without an adopted lifecycle journal")
	}
	if err := p.reserveMemory(ctx, e); err != nil {
		return err
	}
	if err := p.journal(e); err != nil {
		return err
	}
	args := []string{"run", "--detach", "--entrypoint", "/bin/sh", "--name", e.vm, "--label", ownerLabel + "=" + p.runner.cfg.peerOwner,
		"--label", "kubernetes.io/pod-uid=" + string(e.pod.UID), "--cpus", strconv.Itoa(e.allocation.VCPUs), "--memory", strconv.Itoa(e.allocation.MemoryMiB) + "M", "--root-disk", "flat:4G", "--net", "host,private,public",
		"--port", fmt.Sprintf("127.0.0.1:%d:8472/udp", e.port)}
	if e.proxyAvailability {
		for _, binding := range e.proxyPorts {
			args = append(args, "--port", fmt.Sprintf("127.0.0.1:%d:%d", binding.BackendPort, binding.ContainerPort))
		}
	} else if e.httpPort != 0 {
		args = append(args, "--port", fmt.Sprintf("127.0.0.1:%d:%d", e.httpPort, e.appPort))
	}
	bootstrap := "containerd --log-level=error & while true; do if test -f /tmp/application.request; then mv /tmp/application.request /tmp/application.active && sh /tmp/application.active || sleep 0.2; else sleep 0.2; fi; done"
	if concurrentApplications(e.pod) {
		// The legacy queue still runs ordinary sequential init containers. The
		// shared supervisor owns independent regular application processes.
		bootstrap = "(" + bootstrap + ") &\n" + guesttask.SupervisorScript()
	}
	args = append(args, guestImage, "--", "-ec", bootstrap)
	if _, err := p.runner.vm(ctx, args...); err != nil {
		return fmt.Errorf("create native pod VM: %w", err)
	}
	e.created = true

	if _, err := p.runner.guest(ctx, e.vm, "i=0; until "+ctr+" version >/dev/null 2>&1; do i=$((i+1)); test $i -lt 30; sleep 1; done"); err != nil {
		return fmt.Errorf("wait for guest containerd: %w", err)
	}
	if err := p.network.attach(ctx, e); err != nil {
		return fmt.Errorf("attach native pod network: %w", err)
	}
	if len(e.pod.Spec.InitContainers) > 0 {
		e.state = "INITIALIZING"
		return p.advanceInit(ctx, e)
	}
	if err := p.startApplication(ctx, e); err != nil {
		return err
	}
	if concurrentApplications(e.pod) {
		return nil
	}
	for {
		output, err := p.runner.guest(ctx, e.vm, "if test -f /tmp/application.exit; then echo EXIT; else "+ctr+" tasks list; fi")
		if err != nil {
			return err
		}
		if applicationTaskRunning(output) || output == "EXIT" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func (p *provider) startApplication(ctx context.Context, e *podEntry) error {
	if concurrentApplications(e.pod) {
		return p.startApplications(ctx, e)
	}
	container := e.pod.Spec.Containers[0]
	args, oom, err := p.containerLaunch(ctx, e, container, "application", "/tmp")
	if err != nil {
		return err
	}
	script := applicationProcessScript(args, oom.Complete)
	if _, err := p.runner.guest(ctx, e.vm, applicationRequestScript(script)); err != nil {
		return fmt.Errorf("submit application process to guest supervisor: %w", err)
	}
	e.started = time.Now()
	e.startupComplete = false
	e.probeFailure = ""
	e.terminationReason = ""
	e.applicationMissingSince = time.Time{}
	resetProbeProgress(e)
	e.state = "RUNNING"
	return p.journal(e)
}

func (p *provider) containerLaunch(ctx context.Context, e *podEntry, container corev1.Container, task, stateRoot string) ([]string, applicationOOMTracking, error) {
	var empty applicationOOMTracking
	image := normalizeImage(container.Image)
	if err := p.ensureApplicationImage(ctx, e, container); err != nil {
		return nil, empty, err
	}
	launch, err := p.applicationLaunch(ctx, e, container)
	if err != nil {
		return nil, empty, fmt.Errorf("resolve container image configuration: %w", err)
	}
	security, err := securityArgs(e.pod, container, launch.ImageUser)
	if err != nil {
		return nil, empty, fmt.Errorf("configure container security: %w", err)
	}
	resources, err := containerResourceArgs(container)
	if err != nil {
		return nil, empty, fmt.Errorf("configure container resources: %w", err)
	}
	if err := p.prepareVolumes(ctx, e); err != nil {
		return nil, empty, err
	}
	if err := p.prepareContainerSubpaths(ctx, e, container); err != nil {
		return nil, empty, err
	}
	if _, err := p.runner.guest(ctx, e.vm, "cp /etc/pod-resolv.conf /etc/resolv.conf"); err != nil {
		return nil, empty, fmt.Errorf("configure container DNS: %w", err)
	}
	oom := newApplicationOOMTracking(e.generation)
	if stateRoot != "/tmp" {
		oom = buildApplicationOOMTracking(oom.Name, "/sys/fs/cgroup", stateRoot, "/proc/mounts")
	}
	if _, err := p.runner.guest(ctx, e.vm, oom.Setup); err != nil {
		return nil, empty, fmt.Errorf("initialize container resource accounting: %w", err)
	}
	args := []string{"ctr", "--address", "/run/containerd/containerd.sock", "run", "--rm", "--net-host", "--platform", launch.Platform, "--mount", "type=bind,src=/etc/pod-resolv.conf,dst=/etc/resolv.conf,options=rbind:ro"}
	args = append(args, volumeMountArgsForContainer(e.pod, container)...)
	args = append(args, security...)
	args = append(args, resources...)
	args = append(args, oom.Args...)
	if launch.WorkingDir != "" {
		args = append(args, "--cwd", launch.WorkingDir)
	}
	for _, env := range container.Env {
		args = append(args, "--env", env.Name+"="+env.Value)
	}
	args = append(args, image, task)
	args = append(args, launch.Args...)
	return args, oom, nil
}

func normalizeImage(image string) string {
	first, _, _ := strings.Cut(image, "/")
	if !strings.Contains(image, "/") {
		image = "docker.io/library/" + image
	} else if !strings.ContainsAny(first, ".:") && first != "localhost" {
		image = "docker.io/" + image
	}
	if !strings.Contains(image, "@") && !strings.Contains(image[strings.LastIndex(image, "/")+1:], ":") {
		image += ":latest"
	}
	return image
}

func (p *provider) monitor(ctx context.Context, e *podEntry) {
	for {
		e.op.Lock()
		if e.state == "TERMINATED" {
			e.op.Unlock()
			return
		}
		if e.state == "COLD" || e.state == "SLEEPING" || e.state == "FAILED" || e.state == "CHECKPOINTED" || e.state == "RESUMING" || e.deleted {
			e.applicationMissingSince = time.Time{}
			e.op.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-e.wake:
				continue
			}
		}
		if e.state == "INITIALIZING" {
			check, cancel := boundedContext(ctx)
			err := p.monitorInit(check, e)
			cancel()
			if err != nil {
				if ctx.Err() == nil {
					p.fail(e, err)
				}
				e.op.Unlock()
				return
			}
			if e.state == "TERMINATED" {
				e.op.Unlock()
				return
			}
			e.op.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}
		if concurrentApplications(e.pod) {
			err := p.monitorApplications(ctx, e)
			if err != nil && ctx.Err() == nil {
				p.fail(e, err)
			}
			terminal := e.state == "TERMINATED"
			e.op.Unlock()
			if err != nil || terminal {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}
		check, cancel := context.WithTimeout(ctx, 15*time.Second)
		output, err := p.runner.guest(check, e.vm, "if test -f /tmp/application.exit; then if test -f /tmp/application.oom-error; then echo OOMERROR; else printf 'EXIT '; cat /tmp/application.exit; if test -f /tmp/application.oom; then echo OOMKilled; fi; fi; else "+ctr+" tasks list; fi")
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				p.fail(e, err)
			}
			e.op.Unlock()
			return
		}
		if output == "OOMERROR" {
			p.fail(e, errors.New("application terminated but its memory accounting could not be verified"))
			e.op.Unlock()
			return
		}
		if codeText, exited := strings.CutPrefix(output, "EXIT "); exited {
			e.terminationReason = ""
			if trimmed, oom := strings.CutSuffix(codeText, "\nOOMKilled"); oom {
				codeText = trimmed
				e.terminationReason = "OOMKilled"
			}
			code, err := strconv.Atoi(strings.TrimSpace(codeText))
			if err != nil {
				p.fail(e, err)
				e.op.Unlock()
				return
			}
			if e.pod.Spec.RestartPolicy == corev1.RestartPolicyAlways || e.pod.Spec.RestartPolicy == corev1.RestartPolicyOnFailure && (code != 0 || e.probeFailure != "") {
				if err := p.restart(ctx, e, int32(code)); err != nil {
					if ctx.Err() == nil {
						p.fail(e, err)
					}
					e.op.Unlock()
					return
				}
				e.op.Unlock()
				continue
			}
			p.terminated(e, int32(code))
			e.op.Unlock()
			return
		}
		running, missingExpired := e.observeApplicationTask(output, time.Now())
		if missingExpired {
			p.fail(e, errors.New("application task is absent or stopped without a published exit status for 30 seconds"))
			e.op.Unlock()
			return
		}
		if !running {
			e.state = "RUNNING"
			p.unavailable(e, "ApplicationTaskMissing")
		} else {
			if time.Since(e.volumesChecked) >= volumeRefreshInterval {
				check, cancel := context.WithTimeout(ctx, 15*time.Second)
				_ = p.refreshVolumes(check, e, false)
				cancel()
			}
			failed, reason, err := p.healthChecks(ctx, e)
			if err == nil && reason != "" {
				err = p.stopUnhealthy(ctx, e, failed, reason)
			}
			if err != nil {
				if ctx.Err() == nil {
					p.fail(e, err)
				}
				e.op.Unlock()
				return
			}
			if reason != "" {
				e.op.Unlock()
				continue
			}
			ready := e.startupComplete && p.probe(ctx, e)
			e.actualReady = ready
			e.state = "RUNNING"
			if ready {
				e.state = "READY"
			}
			e.mu.Lock()
			e.pod.Status.Phase, e.pod.Status.Reason, e.pod.Status.Message, e.pod.Status.PodIP = corev1.PodRunning, "", "", e.ip
			e.pod.Status.PodIPs = []corev1.PodIP{{IP: e.ip}}
			e.pod.Status.HostIP = p.runner.cfg.nodeIP
			status := &e.pod.Status.ContainerStatuses[0]
			status.ContainerID = "containerd://" + e.vm + "/application"
			status.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(e.started)}}
			status.Started, status.Ready = new(e.startupComplete), ready
			e.pod.Status.Conditions = podConditionsFor(e.pod, ready)
			e.mu.Unlock()
			p.publish(e)
		}
		e.op.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}

func podConditions(ready bool) []corev1.PodCondition {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return []corev1.PodCondition{
		{Type: corev1.PodInitialized, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
		{Type: corev1.ContainersReady, Status: status, LastTransitionTime: metav1.Now()},
		{Type: corev1.PodReady, Status: status, LastTransitionTime: metav1.Now()},
	}
}

func (p *provider) terminated(e *podEntry, code int32) {
	e.state = "TERMINATED"
	e.mu.Lock()
	e.pod.Status.Phase = corev1.PodSucceeded
	if code != 0 || e.probeFailure != "" {
		e.pod.Status.Phase = corev1.PodFailed
	}
	e.actualReady = false
	for i := range e.pod.Status.ContainerStatuses {
		e.pod.Status.ContainerStatuses[i].Ready = false
	}
	e.pod.Status.ContainerStatuses[0].Started = new(false)
	e.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		ExitCode: code, Reason: cmp.Or(e.terminationReason, e.probeFailure, string(e.pod.Status.Phase)), StartedAt: metav1.NewTime(e.started), FinishedAt: metav1.Now()}}
	e.pod.Status.Conditions = podConditionsFor(e.pod, false)
	e.mu.Unlock()
	if err := p.journal(e); err != nil {
		slog.Error("cannot persist terminal pod", "pod", e.pod.Name, "error", err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.stopTerminal(ctx, e); err != nil {
		slog.Error("cannot stop terminal pod VM", "pod", e.pod.Name, "error", err)
	}
	p.publish(e)
}

func (p *provider) stopTerminal(ctx context.Context, e *podEntry) error {
	status, exists, err := p.inspectOwned(ctx, e)
	if err != nil {
		return err
	}
	if exists && status != "Stopped" {
		if _, err := p.runner.vm(ctx, "stop", "--force", e.vm); err != nil {
			return err
		}
		if err := p.requireVMStatus(ctx, e, "Stopped"); err != nil {
			return err
		}
	}
	p.releaseMemory(e)
	return nil
}

func (p *provider) fail(e *podEntry, err error) {
	e.state = "FAILED"
	e.actualReady = false
	e.mu.Lock()
	e.pod.Status.Phase, e.pod.Status.Reason, e.pod.Status.Message = corev1.PodFailed, "NativeRuntimeFailed", err.Error()
	for i := range e.pod.Status.ContainerStatuses {
		e.pod.Status.ContainerStatuses[i].Ready = false
	}
	e.pod.Status.Conditions = podConditionsFor(e.pod, false)
	e.mu.Unlock()
	if journalErr := p.journal(e); journalErr != nil {
		slog.Error("cannot persist failed pod", "pod", e.pod.Name, "error", journalErr)
	}
	if releaseErr := p.releaseInactiveMemory(context.Background(), e); releaseErr != nil {
		slog.Error("cannot reconcile failed pod memory", "pod", e.pod.Name, "error", releaseErr)
	}
	slog.Error("pod runtime failed", "pod", e.pod.Namespace+"/"+e.pod.Name, "error", err)
	p.publish(e)
}

func (p *provider) publish(e *podEntry) {
	e.publication.Lock()
	defer e.publication.Unlock()
	p.projectProxyAvailability(e)
	refreshView(e)
	e.mu.Lock()
	pod := e.pod.DeepCopy()
	e.mu.Unlock()
	p.notifySnapshot(pod)
}

func (p *provider) notifySnapshot(pod *corev1.Pod) {
	p.mu.Lock()
	notify := p.notify
	p.mu.Unlock()
	if notify != nil {
		notify(pod)
	}
}
func (p *provider) NotifyPods(ctx context.Context, notify func(*corev1.Pod)) {
	p.mu.Lock()
	p.notify = notify
	entries := make([]*podEntry, 0, len(p.pods))
	for _, entry := range p.pods {
		entries = append(entries, entry)
	}
	p.mu.Unlock()
	p.jobs.Go(func() {
		for _, entry := range entries {
			if ctx.Err() != nil {
				return
			}
			entry.publication.Lock()
			entry.mu.Lock()
			pod := entry.pod.DeepCopy()
			entry.mu.Unlock()
			p.notifySnapshot(pod)
			entry.publication.Unlock()
		}
	})
}

func (p *provider) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	key := pod.Namespace + "/" + pod.Name
	p.mu.Lock()
	e := p.pods[key]
	p.mu.Unlock()
	if e == nil || e.pod.UID != pod.UID {
		return nil
	}
	e.cancel()
	if e.proxyAvailability {
		if err := p.proxy.closePod(ctx, string(e.pod.UID)); err != nil {
			return err
		}
	}
	select {
	case <-e.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	e.op.Lock()
	defer e.op.Unlock()
	if err := p.runner.checkPending(); err != nil {
		return err
	}
	e.deleted = true
	var errs []error
	if err := p.removeProxy(ctx, e); err != nil {
		errs = append(errs, err)
	}
	if _, exists, err := p.inspectOwned(ctx, e); err != nil {
		errs = append(errs, err)
	} else if exists {
		if _, err := p.runner.vm(ctx, "rm", "--force", e.vm); err != nil {
			errs = append(errs, err)
		}
	}
	if err := p.network.detach(ctx, e); err != nil {
		errs = append(errs, err)
	}
	for _, snapshot := range e.snapshots {
		if err := p.deleteSnapshot(ctx, e, snapshot); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	if _, exists, err := p.inspectOwned(ctx, e); err != nil || exists {
		return errors.Join(err, errors.New("owned VM deletion did not complete"))
	}
	if err := os.RemoveAll(p.initLogDirectory(e)); err != nil {
		return err
	}
	if err := os.Remove(filepath.Join(p.runner.cfg.state, string(e.pod.UID)+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	directory, err := os.Open(p.runner.cfg.state)
	if err != nil {
		return err
	}
	err = directory.Sync()
	directory.Close()
	if err != nil {
		return err
	}
	p.releaseMemory(e)
	p.mu.Lock()
	delete(p.pods, key)
	p.mu.Unlock()
	return nil
}

func (p *provider) entry(namespace, name string) (*podEntry, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.pods[namespace+"/"+name]
	if e == nil {
		return nil, errdefs.NotFound("pod is not owned by this provider")
	}
	return e, nil
}
func (p *provider) GetPod(_ context.Context, namespace, name string) (*corev1.Pod, error) {
	e, err := p.entry(namespace, name)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pod.DeepCopy(), nil
}
func (p *provider) GetPodStatus(ctx context.Context, namespace, name string) (*corev1.PodStatus, error) {
	pod, err := p.GetPod(ctx, namespace, name)
	if err != nil {
		return nil, err
	}
	return &pod.Status, nil
}
func (p *provider) GetPods(context.Context) ([]*corev1.Pod, error) {
	p.mu.Lock()
	entries := make([]*podEntry, 0, len(p.pods))
	for _, e := range p.pods {
		entries = append(entries, e)
	}
	p.mu.Unlock()
	pods := make([]*corev1.Pod, 0, len(entries))
	for _, e := range entries {
		e.mu.Lock()
		pods = append(pods, e.pod.DeepCopy())
		e.mu.Unlock()
	}
	return pods, nil
}
func (p *provider) GetContainerLogs(ctx context.Context, namespace, name, container string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	e, err := p.entry(namespace, name)
	if err != nil {
		return nil, err
	}
	e.op.Lock()
	defer e.op.Unlock()
	if opts.Follow || opts.Previous || opts.Timestamps || opts.SinceSeconds != 0 || !opts.SinceTime.IsZero() {
		return nil, errdefs.InvalidInput("follow, previous, timestamp and time-filtered logs are not qualified")
	}
	for index, init := range e.pod.Spec.InitContainers {
		if container == init.Name {
			data, err := p.readInitLogs(ctx, e, index, opts)
			if err != nil {
				return nil, err
			}
			return io.NopCloser(bytes.NewReader(data)), nil
		}
	}
	if e.state != "READY" && e.state != "RUNNING" {
		return nil, errdefs.InvalidInput("pod is not executing")
	}
	index := -1
	for i, declared := range e.pod.Spec.Containers {
		if container == declared.Name {
			index = i
			break
		}
	}
	if index < 0 {
		return nil, errdefs.NotFound("container not found")
	}
	log := quote(applicationRoot(e.pod, index) + "/application.log")
	script := "cat " + log
	if opts.Tail > 0 {
		script = fmt.Sprintf("tail -n %d %s", opts.Tail, log)
	}
	data, err := p.runner.nativeOutput(ctx, []string{"exec", "--no-stdin", e.vm, "--", "sh", "-ec", script}, nil)
	if err != nil {
		return nil, err
	}
	if opts.LimitBytes > 0 && len(data) > opts.LimitBytes {
		data = data[:opts.LimitBytes]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
func (p *provider) AttachToContainer(context.Context, string, string, string, api.AttachIO) error {
	return errdefs.InvalidInput("attach is not qualified")
}
func (p *provider) GetStatsSummary(context.Context) (*stats.Summary, error) {
	return nil, errdefs.InvalidInput("metrics are not qualified")
}
func (p *provider) GetMetricsResource(context.Context) ([]*dto.MetricFamily, error) {
	return nil, errdefs.InvalidInput("metrics are not qualified")
}
func (p *provider) Ping(ctx context.Context) error {
	check, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := p.runner.vm(check, "ls", "--format", "json"); err != nil {
		return err
	}
	_, err := p.runner.peer(check, "ip link show "+bridgeName)
	return err
}
func (p *provider) NotifyNodeStatus(context.Context, func(*corev1.Node)) {}
func (p *provider) close(ctx context.Context) error {
	defer p.stateLock.Close()
	if p.stop != nil {
		p.stop()
	}
	if p.proxy != nil {
		if err := p.proxy.close(ctx); err != nil {
			return err
		}
	}
	if !p.runner.cfg.cleanupOnExit {
		p.mu.Lock()
		entries := make([]*podEntry, 0, len(p.pods))
		for _, e := range p.pods {
			entries = append(entries, e)
			e.cancel()
		}
		p.mu.Unlock()
		p.jobs.Wait()
		var errs []error
		for _, e := range entries {
			e.op.Lock()
			if err := p.journal(e); err != nil {
				errs = append(errs, err)
			}
			e.op.Unlock()
		}
		return errors.Join(errs...)
	}
	pods, _ := p.GetPods(ctx)
	var errs []error
	for _, pod := range pods {
		if err := p.DeletePod(ctx, pod); err != nil {
			errs = append(errs, err)
		}
	}
	p.jobs.Wait()
	p.relayMu.Lock()
	relay := p.relay
	p.relayMu.Unlock()
	if relay != nil {
		if _, err := relay.request(ctx, "POST", "/shutdown", nil); err != nil {
			errs = append(errs, err)
		}
	}
	if err := p.network.close(ctx); err != nil {
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
