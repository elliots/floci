package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node"
	"github.com/virtual-kubelet/virtual-kubelet/node/nodeutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
)

func main() {
	if err := run(); err != nil {
		slog.Error("controller stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config{}
	flag.StringVar(&cfg.kubeconfig, "kubeconfig", "", "Kubernetes kubeconfig")
	flag.Float64Var(&cfg.kubeAPIQPS, "kube-api-qps", defaultKubeAPIQPS, "Sustained Kubernetes API request rate shared by the controller")
	flag.IntVar(&cfg.kubeAPIBurst, "kube-api-burst", defaultKubeAPIBurst, "Maximum burst of Kubernetes API requests shared by the controller")
	flag.StringVar(&cfg.nodeName, "node", "native-pods", "Owned virtual node name")
	flag.StringVar(&cfg.msb, "msb", "msb", "Native microsandbox 0.7.8 executable")
	flag.StringVar(&cfg.home, "msb-home", "", "Native runtime home")
	flag.StringVar(&cfg.state, "state", "", "Private persistent controller state directory")
	flag.StringVar(&cfg.peer, "network-peer", "", "Owned Docker k3s worker container")
	flag.StringVar(&cfg.peerOwner, "peer-owner", "", "Expected io.floci.pod-probe label value")
	flag.StringVar(&cfg.podCIDR, "pod-cidr", "10.42.200.0/22", "Unused IPv4 Pod CIDR within cluster CIDR")
	flag.StringVar(&cfg.clusterCIDR, "cluster-cidr", "10.42.0.0/16", "Cluster Pod CIDR")
	flag.StringVar(&cfg.serviceCIDR, "service-cidr", "10.43.0.0/16", "Cluster Service CIDR")
	flag.StringVar(&cfg.clusterDNS, "cluster-dns", "10.43.0.10", "Cluster DNS service address")
	flag.StringVar(&cfg.proxyBackendCIDR, "proxy-backend-cidr", "10.42.204.0/22", "Reserved private guest CIDR for proxy availability")
	flag.StringVar(&cfg.relayBinary, "network-relay-binary", "", "Linux network-relay executable matching the owned Docker peer architecture")
	flag.StringVar(&cfg.guestFSBinary, "guest-fs-binary", "", "Linux guestfs executable for fsGroup and supplemental group policies")
	flag.StringVar(&cfg.listen, "listen", "127.0.0.1:10250", "TLS kubelet listen address")
	flag.StringVar(&cfg.nodeIP, "node-ip", "", "Host IPv4 address reachable from the API server")
	flag.StringVar(&cfg.cert, "tls-cert", "", "Kubelet TLS serving certificate")
	flag.StringVar(&cfg.key, "tls-key", "", "Kubelet TLS private key")
	flag.StringVar(&cfg.ca, "client-ca", "", "Kubernetes kubelet-client certificate authority")
	flag.StringVar(&cfg.controlListen, "control-listen", "127.0.0.1:18187", "Loopback authenticated lifecycle listener")
	flag.StringVar(&cfg.tokenFile, "control-token-file", "", "File containing a bearer token of at least 32 bytes")
	flag.StringVar(&cfg.clusterName, "cluster-name", "", "Cluster identity advertised by lifecycle discovery")
	flag.IntVar(&cfg.maxPods, "max-pods", 512, "Maximum tracked pods including sleeping pods")
	flag.IntVar(&cfg.maxActiveMemoryMiB, "max-active-memory-mib", 4096, "Memory admission budget for active pod VMs only")
	flag.StringVar(&cfg.schedulingCPU, "scheduling-cpu", "", "Virtual node CPU capacity for scheduling, as a Kubernetes quantity (default 4)")
	flag.StringVar(&cfg.schedulingMemory, "scheduling-memory", "", "Virtual node memory capacity for scheduling, as a Kubernetes quantity (default active memory budget)")
	flag.BoolVar(&cfg.cleanupOnExit, "cleanup-on-exit", false, "Delete owned guests on exit, for disposable qualification only")
	flag.Parse()
	if err := cfg.validate(); err != nil {
		return err
	}
	capacity, err := cfg.nodeCapacity()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	client, err := newKubernetesClient(cfg)
	if err != nil {
		return err
	}
	cert, err := tls.LoadX509KeyPair(cfg.cert, cfg.key)
	if err != nil {
		return err
	}
	caPEM, err := os.ReadFile(cfg.ca)
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return errors.New("client CA contains no certificates")
	}
	ca, err := dynamiccertificates.NewStaticCAContent("kubelet-client-ca", caPEM)
	if err != nil {
		return err
	}
	auth, err := nodeutil.WebhookAuth(client, cfg.nodeName, func(c *nodeutil.WebhookAuthConfig) error {
		c.AuthnConfig.ClientCertificateCAContentProvider = ca
		return nil
	})
	if err != nil {
		return err
	}
	provider, err := newProvider(ctx, cfg, client)
	if err != nil {
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := provider.close(cleanupCtx); err != nil {
			slog.Error("owned resource cleanup failed", "error", err)
		}
	}()
	control, err := provider.serveControl(cfg.controlListen, cfg.tokenFile)
	if err != nil {
		return err
	}
	defer control.Close()
	mux := http.NewServeMux()
	_, portText, _ := net.SplitHostPort(cfg.listen)
	port, err := net.LookupPort("tcp", portText)
	if err != nil {
		return err
	}
	now := metav1.Now()
	spec := corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: cfg.nodeName, Labels: map[string]string{
			"kubernetes.io/hostname": cfg.nodeName, "kubernetes.io/os": "linux",
			"kubernetes.io/arch": runtime.GOARCH, "microsandbox.dev/runtime": "native",
		}},
		Spec: corev1.NodeSpec{PodCIDR: cfg.podCIDR, PodCIDRs: []string{cfg.podCIDR}, Taints: []corev1.Taint{{
			Key: "microsandbox.dev/runtime", Value: "native", Effect: corev1.TaintEffectNoSchedule,
		}}},
		Status: corev1.NodeStatus{
			Features: &corev1.NodeFeatures{SupplementalGroupsPolicy: new(cfg.guestFSBinary != "")},
			Capacity: capacity, Allocatable: capacity.DeepCopy(), Phase: corev1.NodeRunning,
			Addresses:       []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: cfg.nodeIP}},
			DaemonEndpoints: corev1.NodeDaemonEndpoints{KubeletEndpoint: corev1.DaemonEndpoint{Port: int32(port)}},
			NodeInfo: corev1.NodeSystemInfo{OperatingSystem: "linux", Architecture: runtime.GOARCH,
				KubeletVersion: "v1.35.0", ContainerRuntimeVersion: "microsandbox://0.7.8"},
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue,
				Reason: "NativeControllerReady", Message: "Native runtime and owned network peer passed startup checks",
				LastHeartbeatTime: now, LastTransitionTime: now}},
		},
	}
	vk, err := nodeutil.NewNode(cfg.nodeName, func(nodeutil.ProviderConfig) (nodeutil.Provider, node.NodeProvider, error) {
		return provider, provider, nil
	}, nodeutil.WithNodeConfig(nodeutil.NodeConfig{
		Client: client, NodeSpec: spec, InformerResyncPeriod: time.Minute,
		HTTPListenAddr: cfg.listen, Handler: nodeutil.WithAuth(auth, mux), NumWorkers: 2,
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert},
			ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool},
	}), nodeutil.AttachProviderRoutes(mux))
	if err != nil {
		return err
	}
	slog.Info("starting experimental native pod controller", "node", cfg.nodeName, "listen", cfg.listen,
		"schedulingCPU", capacity.Cpu().String(), "schedulingMemory", capacity.Memory().String(),
		"maxActiveMemoryMiB", cfg.maxActiveMemoryMiB)
	err = vk.Run(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (c config) validate() error {
	if c.guestFSBinary != "" {
		if _, err := readGuestFS(c.guestFSBinary); err != nil {
			return err
		}
	}
	if err := c.validateKubeAPIThrottle(); err != nil {
		return err
	}
	if c.maxPods < 1 || c.maxPods > 1021 || c.maxActiveMemoryMiB < vmMemoryMiB {
		return errors.New("max-pods must be 1..1021 and active memory must fit at least one 768 MiB guest")
	}
	if c.kubeconfig == "" || c.home == "" || c.state == "" || c.peer == "" || c.peerOwner == "" ||
		c.cert == "" || c.key == "" || c.ca == "" || net.ParseIP(c.nodeIP) == nil {
		return fmt.Errorf("kubeconfig, msb-home, state, network-peer, peer-owner, TLS files and node-ip are required")
	}
	return nil
}
