package main

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

func writeClientKubeconfig(t *testing.T) string {
	t.Helper()
	kubeconfig := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{"fixture": {
			Server: "https://127.0.0.1:6443", InsecureSkipTLSVerify: true,
		}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"fixture": {Token: "fixture-token"}},
		Contexts:       map[string]*clientcmdapi.Context{"fixture": {Cluster: "fixture", AuthInfo: "fixture", Namespace: "apps"}},
		CurrentContext: "fixture",
	}
	path := filepath.Join(t.TempDir(), "config")
	if err := clientcmd.WriteToFile(kubeconfig, path); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestKubeClientConfigLoadsExplicitFileWithSharedControllerDefaults(t *testing.T) {
	cfg := config{kubeconfig: writeClientKubeconfig(t), kubeAPIQPS: defaultKubeAPIQPS, kubeAPIBurst: defaultKubeAPIBurst}
	clientConfig, err := cfg.kubeClientConfig(func() (*rest.Config, error) {
		t.Fatal("existing kubeconfig unexpectedly selected in-cluster authentication")
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if clientConfig.Host != "https://127.0.0.1:6443" || !clientConfig.Insecure || clientConfig.BearerToken != "fixture-token" {
		t.Fatal("loading throttle settings changed the kubeconfig connection or authentication")
	}
	if clientConfig.QPS != 50 || clientConfig.Burst != 100 {
		t.Fatalf("unexpected controller throttle defaults: %v/%d", clientConfig.QPS, clientConfig.Burst)
	}
}

func TestKubeClientConfigFallsBackForEmptyOrMissingPath(t *testing.T) {
	for _, path := range []string{"", filepath.Join(t.TempDir(), "missing")} {
		original := &rest.Config{Host: "https://kubernetes.default.svc", BearerToken: "service-account-token",
			QPS: 5, Burst: 10, Timeout: 23 * time.Second}
		calls := 0
		cfg := config{kubeconfig: path, kubeAPIQPS: 23.5, kubeAPIBurst: 42}
		clientConfig, err := cfg.kubeClientConfig(func() (*rest.Config, error) {
			calls++
			return original, nil
		})
		if err != nil || calls != 1 {
			t.Fatalf("expected in-cluster fallback for %q: calls=%d, error=%v", path, calls, err)
		}
		if clientConfig.QPS != 23.5 || clientConfig.Burst != 42 || clientConfig.Timeout != original.Timeout ||
			clientConfig.Host != original.Host || clientConfig.BearerToken != original.BearerToken {
			t.Fatal("throttle override lost in-cluster connection settings")
		}
		if original.QPS != 5 || original.Burst != 10 {
			t.Fatal("throttle configuration mutated its input REST config")
		}
	}
}

func TestKubeClientConfigPreservesLoadErrorsWithoutFallback(t *testing.T) {
	directory := t.TempDir()
	malformed := filepath.Join(directory, "malformed")
	if err := os.WriteFile(malformed, []byte("clusters: [broken"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{malformed, filepath.Join(malformed, "not-a-directory")} {
		cfg := config{kubeconfig: path, kubeAPIQPS: 50, kubeAPIBurst: 100}
		_, err := cfg.kubeClientConfig(func() (*rest.Config, error) {
			t.Fatal("invalid explicit kubeconfig silently selected in-cluster authentication")
			return nil, nil
		})
		if err == nil || !strings.Contains(err.Error(), "load Kubernetes client configuration") {
			t.Fatalf("expected kubeconfig load error, got %v", err)
		}
	}
}

func TestKubeClientConfigPreservesInClusterErrors(t *testing.T) {
	failure := errors.New("fixture in-cluster configuration failure")
	cfg := config{kubeAPIQPS: 50, kubeAPIBurst: 100}
	_, err := cfg.kubeClientConfig(func() (*rest.Config, error) { return nil, failure })
	if !errors.Is(err, failure) {
		t.Fatalf("expected original in-cluster failure, got %v", err)
	}
}

func TestKubeAPIThrottleRejectsInvalidAndUnlimitedSettings(t *testing.T) {
	for _, qps := range []float64{0, -1, math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64, math.SmallestNonzeroFloat64} {
		cfg := config{kubeAPIQPS: qps, kubeAPIBurst: 100}
		if err := cfg.validateKubeAPIThrottle(); err == nil || !strings.Contains(err.Error(), "kube-api-qps") {
			t.Errorf("invalid QPS %v accepted: %v", qps, err)
		}
	}
	for _, burst := range []int{0, -1, math.MaxInt32 + 1} {
		cfg := config{kubeAPIQPS: 50, kubeAPIBurst: burst}
		if err := cfg.validateKubeAPIThrottle(); err == nil || !strings.Contains(err.Error(), "kube-api-burst") {
			t.Errorf("invalid burst %d accepted: %v", burst, err)
		}
	}
	for _, cfg := range []config{{kubeAPIQPS: 0.25, kubeAPIBurst: 1}, {kubeAPIQPS: math.MaxFloat32, kubeAPIBurst: math.MaxInt32}} {
		if err := cfg.validateKubeAPIThrottle(); err != nil {
			t.Fatalf("valid throttle settings rejected: %v", err)
		}
	}
}

func TestKubernetesClientSharesItsLimiterAcrossAPIResources(t *testing.T) {
	cfg := config{kubeconfig: writeClientKubeconfig(t), kubeAPIQPS: 0.001, kubeAPIBurst: 2}
	client, err := newKubernetesClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	coreLimiter := client.CoreV1().RESTClient().GetRateLimiter()
	appsLimiter := client.AppsV1().RESTClient().GetRateLimiter()
	if coreLimiter == nil || appsLimiter != coreLimiter || coreLimiter.QPS() != 0.001 {
		t.Fatal("API groups did not share the configured clientset rate limiter")
	}
	if !coreLimiter.TryAccept() || !appsLimiter.TryAccept() || coreLimiter.TryAccept() {
		t.Fatal("API groups did not consume the same finite burst allowance")
	}
}
