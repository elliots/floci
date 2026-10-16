package main

import (
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

type applicationLaunchSpec struct {
	Args       []string
	WorkingDir string
	Platform   string
	ImageUser  string
}

type applicationImageConfig struct {
	Architecture string `json:"architecture"`
	OS           string `json:"os"`
	Config       struct {
		Entrypoint []string
		Cmd        []string
		WorkingDir string
		User       string
	} `json:"config"`
}

type imageDescriptor struct {
	Digest    string `json:"digest"`
	MediaType string `json:"mediaType"`
	Platform  *struct {
		Architecture string `json:"architecture"`
		OS           string `json:"os"`
		Variant      string `json:"variant"`
	} `json:"platform,omitempty"`
}

type imageManifest struct {
	SchemaVersion int               `json:"schemaVersion"`
	MediaType     string            `json:"mediaType"`
	Config        imageDescriptor   `json:"config"`
	Manifests     []imageDescriptor `json:"manifests"`
}

// applicationLaunch uses the same local content that ctr will execute. In
// particular, imagePullPolicy Never and cached IfNotPresent never contact a registry.
func (p *provider) applicationLaunch(ctx context.Context, e *podEntry, container corev1.Container) (applicationLaunchSpec, error) {
	if runtime.GOARCH != "arm64" && runtime.GOARCH != "amd64" {
		return applicationLaunchSpec{}, fmt.Errorf("unsupported native image architecture %s", runtime.GOARCH)
	}
	image := normalizeImage(container.Image)
	output, err := p.runner.guest(ctx, e.vm, ctr+" images list "+quote("name=="+strconv.Quote(image)))
	if err != nil {
		return applicationLaunchSpec{}, fmt.Errorf("inspect cached application image: %w", err)
	}
	target, err := cachedImageTarget(output, image)
	if err != nil {
		return applicationLaunchSpec{}, err
	}
	read := func(digest string) ([]byte, error) {
		if !validImageDigest(digest) {
			return nil, fmt.Errorf("cached application image has an invalid content digest")
		}
		output, err := p.runner.guest(ctx, e.vm, ctr+" content get "+quote(digest))
		if err != nil {
			return nil, fmt.Errorf("read cached application image content: %w", err)
		}
		if len(output) > 4<<20 {
			return nil, fmt.Errorf("application image metadata exceeds 4 MiB")
		}
		return []byte(output), nil
	}
	imageConfig, err := readApplicationImageConfig(target, runtime.GOARCH, read, 0)
	if err != nil {
		return applicationLaunchSpec{}, err
	}
	return resolveApplicationLaunch(container, imageConfig, runtime.GOARCH)
}

func cachedImageTarget(output, image string) (imageDescriptor, error) {
	for line := range strings.SplitSeq(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == image {
			if !validImageDigest(fields[2]) {
				return imageDescriptor{}, fmt.Errorf("cached application image has an invalid target digest")
			}
			return imageDescriptor{Digest: fields[2], MediaType: fields[1]}, nil
		}
	}
	return imageDescriptor{}, fmt.Errorf("application image %s is not present in the guest content store", image)
}

func validImageDigest(digest string) bool {
	algorithm, encoded, ok := strings.Cut(digest, ":")
	if !ok || (algorithm != "sha256" || len(encoded) != 64) && (algorithm != "sha512" || len(encoded) != 128) {
		return false
	}
	_, err := hex.DecodeString(encoded)
	return err == nil && encoded == strings.ToLower(encoded)
}

func readApplicationImageConfig(descriptor imageDescriptor, architecture string, read func(string) ([]byte, error), depth int) (applicationImageConfig, error) {
	if depth > 8 {
		return applicationImageConfig{}, fmt.Errorf("application image index nesting exceeds eight levels")
	}
	content, err := read(descriptor.Digest)
	if err != nil {
		return applicationImageConfig{}, err
	}
	var manifest imageManifest
	if err := json.Unmarshal(content, &manifest); err != nil || manifest.SchemaVersion != 2 {
		return applicationImageConfig{}, fmt.Errorf("cached application image has an invalid manifest")
	}
	// OCI makes mediaType optional inside the JSON. The containing descriptor
	// remains authoritative, including for manifests created by image import.
	mediaType := cmp.Or(descriptor.MediaType, manifest.MediaType)
	kind := imageMediaTypeKind(mediaType)
	if bodyKind := imageMediaTypeKind(manifest.MediaType); bodyKind != "" && bodyKind != kind {
		return applicationImageConfig{}, fmt.Errorf("application image manifest media type disagrees with its descriptor")
	}
	switch kind {
	case "index":
		if manifest.Config.Digest != "" {
			return applicationImageConfig{}, fmt.Errorf("application image index unexpectedly contains a configuration descriptor")
		}
		var unspecified *imageDescriptor
		for _, candidate := range manifest.Manifests {
			// containerd orders matching explicit platforms before unspecified
			// descriptors, then selects exactly one candidate in that order.
			if candidate.Platform == nil {
				if unspecified == nil {
					unspecified = new(candidate)
				}
				continue
			}
			if candidate.Platform.OS != "linux" || candidate.Platform.Architecture != architecture ||
				candidate.Platform.Variant != "" && !(architecture == "arm64" && candidate.Platform.Variant == "v8") &&
					!(architecture == "amd64" && candidate.Platform.Variant == "v1") {
				continue
			}
			return readApplicationImageConfig(candidate, architecture, read, depth+1)
		}
		if unspecified != nil {
			return readApplicationImageConfig(*unspecified, architecture, read, depth+1)
		}
		return applicationImageConfig{}, fmt.Errorf("cached application image has no linux/%s manifest", architecture)
	case "manifest":
		if len(manifest.Manifests) != 0 {
			return applicationImageConfig{}, fmt.Errorf("application image manifest unexpectedly contains an index")
		}
		content, err := read(manifest.Config.Digest)
		if err != nil {
			return applicationImageConfig{}, err
		}
		var config applicationImageConfig
		if err := json.Unmarshal(content, &config); err != nil {
			return applicationImageConfig{}, fmt.Errorf("cached application image has invalid process configuration")
		}
		if config.OS != "linux" || config.Architecture != architecture {
			return applicationImageConfig{}, fmt.Errorf("application image configuration does not match linux/%s", architecture)
		}
		return config, nil
	default:
		return applicationImageConfig{}, fmt.Errorf("cached application image has an unsupported manifest media type")
	}
}

func imageMediaTypeKind(mediaType string) string {
	switch mediaType {
	case "application/vnd.oci.image.index.v1+json", "application/vnd.docker.distribution.manifest.list.v2+json":
		return "index"
	case "application/vnd.oci.image.manifest.v1+json", "application/vnd.docker.distribution.manifest.v2+json":
		return "manifest"
	default:
		return ""
	}
}

func resolveApplicationLaunch(container corev1.Container, image applicationImageConfig, architecture string) (applicationLaunchSpec, error) {
	environment := make(map[string]string, len(container.Env))
	for _, variable := range container.Env {
		environment[variable.Name] = variable.Value
	}
	command := expandApplicationArguments(container.Command, environment)
	args := expandApplicationArguments(container.Args, environment)
	// Match containerd's CRI WithProcessArgs: specifying a command suppresses
	// image CMD; specifying only args retains image ENTRYPOINT. Image defaults
	// stay literal because kubelet expands only fields provided in the Pod.
	if len(command) == 0 {
		command = slices.Clone(image.Config.Entrypoint)
		if len(command) == 1 && command[0] == "" {
			command = nil
		}
		if len(args) == 0 {
			args = slices.Clone(image.Config.Cmd)
		}
	}
	command = append(command, args...)
	if len(command) == 0 || command[0] == "" {
		return applicationLaunchSpec{}, fmt.Errorf("application image and Pod do not specify an executable command")
	}
	return applicationLaunchSpec{
		Args: command, WorkingDir: cmp.Or(container.WorkingDir, image.Config.WorkingDir, "/"),
		Platform:  "linux/" + architecture,
		ImageUser: image.Config.User,
	}, nil
}

func expandApplicationArguments(arguments []string, environment map[string]string) []string {
	result := make([]string, len(arguments))
	for index, argument := range arguments {
		result[index] = expandApplicationArgument(argument, environment)
	}
	return result
}

func expandApplicationArgument(argument string, environment map[string]string) string {
	var expanded strings.Builder
	for index := 0; index < len(argument); index++ {
		if argument[index] == '$' && index+1 < len(argument) {
			switch argument[index+1] {
			case '$':
				expanded.WriteByte('$')
				index++
				continue
			case '(':
				if end := strings.IndexByte(argument[index+2:], ')'); end >= 0 {
					end += index + 2
					value, found := environment[argument[index+2:end]]
					if found {
						expanded.WriteString(value)
					} else {
						expanded.WriteString(argument[index : end+1])
					}
					index = end
					continue
				}
			}
		}
		expanded.WriteByte(argument[index])
	}
	return expanded.String()
}
