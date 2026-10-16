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
	started                                          time.Time
	readiness, startup, liveness                     probeProgress
	startupComplete                                  bool
	probeFailure                                     string
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
	runner := commandRunner{cfg: cfg}
	if err := runner.checkPending(); err != nil {
		return nil, err
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
	spec := pod.Spec
	if len(spec.Containers) != 1 || len(spec.InitContainers) != 0 || len(spec.EphemeralContainers) != 0 {
		return errdefs.InvalidInput("qualification supports exactly one regular container and no init or ephemeral containers")
	}

	if spec.HostNetwork || spec.HostPID || spec.HostIPC || len(spec.ImagePullSecrets) != 0 ||
		spec.ShareProcessNamespace != nil {
		return errdefs.InvalidInput("host namespaces, shared process namespaces and image pull credentials are not qualified")
	}
	if err := validateSecurityContexts(pod); err != nil {
		return errdefs.InvalidInput(err.Error())
	}
	if spec.DNSPolicy != corev1.DNSClusterFirst || spec.DNSConfig != nil {
		return errdefs.InvalidInput("qualification supports ClusterFirst DNS without overrides")
	}
	container := spec.Containers[0]
	if _, _, _, err := proxySettings(pod); err != nil {
		return err
	}
	if len(container.EnvFrom) != 0 {
		return errdefs.InvalidInput("unresolved envFrom source")
	}
	if container.Resources.Requests.Memory().Value() > vmMemoryMiB*1024*1024 || container.Resources.Requests.Cpu().MilliValue() > 1000 {
		return errdefs.InvalidInput("pod requests exceed the fixed one-vCPU, 768 MiB guest")
	}
	if container.RestartPolicy != nil || len(container.RestartPolicyRules) != 0 ||
		len(container.VolumeDevices) != 0 || len(container.Resources.Limits) != 0 || container.Lifecycle != nil || container.Stdin || container.TTY {
		return errdefs.InvalidInput("container restart overrides, volume devices, limits, hooks and TTY are not qualified")
	}
	if err := validateVolumes(pod); err != nil {
		return errdefs.InvalidInput(err.Error())
	}
	if err := validateProbes(container); err != nil {
		return errdefs.InvalidInput(err.Error())
	}
	for _, port := range container.Ports {
		if port.HostPort != 0 || port.HostIP != "" {
			return errdefs.InvalidInput("host ports are not qualified")
		}
	}
	for _, env := range container.Env {
		if env.ValueFrom != nil {
			return errdefs.InvalidInput("unresolved environment reference")
		}
	}
	return nil
}

func (p *provider) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	if err := validatePod(pod); err != nil {
		return err
	}
	if networkProbes(pod.Spec.Containers[0]) && p.runner.cfg.relayBinary == "" {
		return errdefs.InvalidInput("HTTP/TCP probes require --network-relay-binary")
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
	for _, port := range pod.Spec.Containers[0].Ports {
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
	e.declaredSpec = declared.Spec.DeepCopy()
	e.pod.Status = corev1.PodStatus{Phase: corev1.PodPending, Reason: "NativeVMStarting", StartTime: new(metav1.Now()),
		ContainerStatuses: []corev1.ContainerStatus{{Name: pod.Spec.Containers[0].Name, Image: pod.Spec.Containers[0].Image,
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}}}
	e.proxyAvailability, e.idleOwner, e.idleTimeout, _ = proxySettings(pod)
	if e.proxyAvailability {
		e.backendIP = addressAt(p.network.backendPrefix, slot)
		for _, declared := range pod.Spec.Containers[0].Ports {
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
		e.pod.Status.ContainerStatuses[0].State.Waiting.Reason = "NativeVMCold"
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
	if networkProbes(e.pod.Spec.Containers[0]) {
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
		"--label", "kubernetes.io/pod-uid=" + string(e.pod.UID), "--cpus", "1", "--memory", "768M", "--root-disk", "flat:4G", "--net", "host,private,public",
		"--port", fmt.Sprintf("127.0.0.1:%d:8472/udp", e.port)}
	if e.proxyAvailability {
		for _, binding := range e.proxyPorts {
			args = append(args, "--port", fmt.Sprintf("127.0.0.1:%d:%d", binding.BackendPort, binding.ContainerPort))
		}
	} else if e.httpPort != 0 {
		args = append(args, "--port", fmt.Sprintf("127.0.0.1:%d:%d", e.httpPort, e.appPort))
	}
	args = append(args, guestImage, "--", "-ec", "containerd --log-level=error & while true; do if test -f /tmp/application.request; then mv /tmp/application.request /tmp/application.active; sh /tmp/application.active || true; else sleep 0.2; fi; done")
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
	if err := p.startApplication(ctx, e); err != nil {
		return err
	}
	for {
		output, err := p.runner.guest(ctx, e.vm, "if test -f /tmp/application.exit; then echo EXIT; else "+ctr+" tasks list; fi")
		if err != nil {
			return err
		}
		if strings.Contains(output, "RUNNING") || strings.Contains(output, "EXIT") {
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
	container := e.pod.Spec.Containers[0]
	image := normalizeImage(container.Image)
	if err := p.ensureApplicationImage(ctx, e, container); err != nil {
		return err
	}
	launch, err := p.applicationLaunch(ctx, e, container)
	if err != nil {
		return fmt.Errorf("resolve application image configuration: %w", err)
	}
	security, err := securityArgs(e.pod, container, launch.ImageUser)
	if err != nil {
		return fmt.Errorf("configure application security: %w", err)
	}
	if err := p.prepareVolumes(ctx, e); err != nil {
		return err
	}
	if _, err := p.runner.guest(ctx, e.vm, "cp /etc/pod-resolv.conf /etc/resolv.conf"); err != nil {
		return fmt.Errorf("configure application DNS: %w", err)
	}
	args := []string{"ctr", "--address", "/run/containerd/containerd.sock", "run", "--rm", "--net-host", "--platform", launch.Platform, "--mount", "type=bind,src=/etc/pod-resolv.conf,dst=/etc/resolv.conf,options=rbind:ro"}
	args = append(args, volumeMountArgs(e.pod)...)
	args = append(args, security...)
	if launch.WorkingDir != "" {
		args = append(args, "--cwd", launch.WorkingDir)
	}
	for _, env := range container.Env {
		args = append(args, "--env", env.Name+"="+env.Value)
	}
	args = append(args, image, "application")
	args = append(args, launch.Args...)
	script := "set +e; " + joinCommand(args) + " >/tmp/application.log 2>&1; code=$?; printf '%s\\n' \"$code\" >/tmp/application.exit; exit \"$code\""
	if _, err := p.runner.guest(ctx, e.vm, "printf '%s' "+quote(script)+" >/tmp/application.pending; mv /tmp/application.pending /tmp/application.request"); err != nil {
		return fmt.Errorf("submit application process to guest supervisor: %w", err)
	}
	e.started = time.Now()
	e.startupComplete = false
	e.probeFailure = ""
	resetProbeProgress(e)
	e.state = "RUNNING"
	return p.journal(e)
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
		if e.state == "COLD" || e.state == "SLEEPING" || e.state == "FAILED" || e.state == "CHECKPOINTED" || e.state == "RESUMING" || e.deleted {
			e.op.Unlock()
			select {
			case <-ctx.Done():
				return
			case <-e.wake:
				continue
			}
		}
		check, cancel := context.WithTimeout(ctx, 15*time.Second)
		output, err := p.runner.guest(check, e.vm, "if test -f /tmp/application.exit; then printf 'EXIT '; cat /tmp/application.exit; else "+ctr+" tasks list; fi")
		cancel()
		if err != nil {
			if ctx.Err() == nil {
				p.fail(e, err)
			}
			e.op.Unlock()
			return
		}
		if codeText, exited := strings.CutPrefix(output, "EXIT "); exited {
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
		if strings.Contains(output, "RUNNING") {
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
			e.pod.Status.Conditions = podConditions(ready)
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
	e.pod.Status.ContainerStatuses[0].Ready = false
	e.pod.Status.ContainerStatuses[0].Started = new(false)
	e.pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
		ExitCode: code, Reason: cmp.Or(e.probeFailure, string(e.pod.Status.Phase)), StartedAt: metav1.NewTime(e.started), FinishedAt: metav1.Now()}}
	e.pod.Status.Conditions = podConditions(false)
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
	e.mu.Lock()
	e.pod.Status.Phase, e.pod.Status.Reason, e.pod.Status.Message = corev1.PodFailed, "NativeRuntimeFailed", err.Error()
	e.pod.Status.ContainerStatuses[0].Ready = false
	e.pod.Status.Conditions = podConditions(false)
	e.mu.Unlock()
	if journalErr := p.journal(e); journalErr != nil {
		slog.Error("cannot persist failed pod", "pod", e.pod.Name, "error", journalErr)
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
	if e.state != "READY" && e.state != "RUNNING" {
		return nil, errdefs.InvalidInput("pod is not executing")
	}
	if container != e.pod.Spec.Containers[0].Name {
		return nil, errdefs.NotFound("container not found")
	}
	if opts.Follow || opts.Previous || opts.Timestamps || opts.SinceSeconds != 0 || !opts.SinceTime.IsZero() {
		return nil, errdefs.InvalidInput("follow, previous, timestamp and time-filtered logs are not qualified")
	}
	script := "cat /tmp/application.log"
	if opts.Tail > 0 {
		script = fmt.Sprintf("tail -n %d /tmp/application.log", opts.Tail)
	}
	output, err := p.runner.guest(ctx, e.vm, script)
	if err != nil {
		return nil, err
	}
	data := []byte(output + "\n")
	if opts.LimitBytes > 0 && len(data) > opts.LimitBytes {
		data = data[:opts.LimitBytes]
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}
func (p *provider) RunInContainer(ctx context.Context, namespace, name, container string, args []string, attach api.AttachIO) error {
	e, err := p.entry(namespace, name)
	if err != nil {
		return err
	}
	e.op.Lock()
	defer e.op.Unlock()
	if e.state != "READY" && e.state != "RUNNING" {
		return errdefs.InvalidInput("pod is not executing")
	}
	if container != e.pod.Spec.Containers[0].Name {
		return errdefs.NotFound("container not found")
	}
	if attach.TTY() {
		return errdefs.InvalidInput("interactive TTY is not qualified")
	}
	command := []string{"exec", "--stream", e.vm, "--", "ctr", "--address", "/run/containerd/containerd.sock", "tasks", "exec", "--exec-id", "exec-" + strconv.FormatInt(time.Now().UnixNano(), 10), "application"}
	command = append(command, args...)
	cmd := p.runner.command(ctx, p.runner.cfg.msb, command...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = attach.Stdin(), attach.Stdout(), attach.Stderr()
	return cmd.Run()
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
func (p *provider) PortForward(context.Context, string, string, int32, io.ReadWriteCloser) error {
	return errdefs.InvalidInput("port-forward is not qualified")
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
