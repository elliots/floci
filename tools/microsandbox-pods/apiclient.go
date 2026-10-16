package main

import (
	"fmt"
	"math"
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultKubeAPIQPS   = 50
	defaultKubeAPIBurst = 100
)

func newKubernetesClient(cfg config) (*kubernetes.Clientset, error) {
	clientConfig, err := cfg.kubeClientConfig(rest.InClusterConfig)
	if err != nil {
		return nil, err
	}
	// Client-go creates one token bucket shared by this clientset's API groups.
	// The provider and Virtual Kubelet must both receive this same clientset.
	return kubernetes.NewForConfig(clientConfig)
}

func (c config) validateKubeAPIThrottle() error {
	if math.IsNaN(c.kubeAPIQPS) || math.IsInf(c.kubeAPIQPS, 0) || c.kubeAPIQPS > math.MaxFloat32 || float32(c.kubeAPIQPS) <= 0 {
		return fmt.Errorf("kube-api-qps must be finite, positive and representable as float32")
	}
	if c.kubeAPIBurst <= 0 || c.kubeAPIBurst > math.MaxInt32 {
		return fmt.Errorf("kube-api-burst must be between 1 and %d", math.MaxInt32)
	}
	return nil
}

func (c config) kubeClientConfig(inCluster func() (*rest.Config, error)) (*rest.Config, error) {
	if err := c.validateKubeAPIThrottle(); err != nil {
		return nil, err
	}
	var clientConfig *rest.Config
	var err error
	useInCluster := c.kubeconfig == ""
	if !useInCluster {
		_, err = os.Stat(c.kubeconfig)
		if os.IsNotExist(err) {
			useInCluster = true
		} else if err != nil {
			return nil, fmt.Errorf("load Kubernetes client configuration: %w", err)
		}
	}
	if useInCluster {
		clientConfig, err = inCluster()
	} else {
		clientConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			&clientcmd.ClientConfigLoadingRules{ExplicitPath: c.kubeconfig}, &clientcmd.ConfigOverrides{},
		).ClientConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("load Kubernetes client configuration: %w", err)
	}
	clientConfig = rest.CopyConfig(clientConfig)
	clientConfig.QPS = float32(c.kubeAPIQPS)
	clientConfig.Burst = c.kubeAPIBurst
	return clientConfig, nil
}
