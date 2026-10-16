package main

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

func effectivePullPolicy(container corev1.Container) corev1.PullPolicy {
	if container.ImagePullPolicy != "" {
		return container.ImagePullPolicy
	}
	image := normalizeImage(container.Image)
	if !strings.Contains(image, "@") && strings.HasSuffix(image, ":latest") {
		return corev1.PullAlways
	}
	return corev1.PullIfNotPresent
}

func (p *provider) ensureApplicationImage(ctx context.Context, e *podEntry, container corev1.Container) error {
	image := normalizeImage(container.Image)
	policy := effectivePullPolicy(container)
	if policy != corev1.PullAlways && policy != corev1.PullIfNotPresent && policy != corev1.PullNever {
		return fmt.Errorf("unsupported image pull policy %s", policy)
	}
	if policy != corev1.PullAlways {
		// A reference alone may point at an incomplete download. ctr check only
		// reports images with all content present and an unpacked root filesystem.
		output, err := p.runner.guest(ctx, e.vm, ctr+" images check --quiet")
		if err != nil {
			return fmt.Errorf("inspect cached application image: %w", err)
		}
		for cached := range strings.FieldsSeq(output) {
			if cached == image {
				return nil
			}
		}
		if policy == corev1.PullNever {
			return fmt.Errorf("image %s is not fully cached in this guest and imagePullPolicy is Never", image)
		}
	}
	credentials, err := p.imageCredentials(ctx, e.pod, image)
	if err != nil {
		return err
	}
	if len(credentials) != 0 {
		return p.pullWithCredentials(ctx, e.vm, image, credentials)
	}
	if _, err := p.runner.guest(ctx, e.vm, ctr+" images pull "+quote(image)+" >/tmp/image-pull.log 2>&1"); err != nil {
		return fmt.Errorf("pull application image: %w", err)
	}
	return nil
}
