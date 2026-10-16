package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestApplicationLaunchMatchesKubernetesImageOverrides(t *testing.T) {
	var image applicationImageConfig
	if err := json.Unmarshal([]byte(`{"config":{"Entrypoint":["/entrypoint"],"Cmd":["image-argument","$(POD_VALUE)"],"WorkingDir":"/image-directory","User":"1000:2000","Env":["IMAGE_VALUE=image-value"]}}`), &image); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		command []string
		args    []string
		want    []string
	}{
		{"image defaults", nil, nil, []string{"/entrypoint", "image-argument", "$(POD_VALUE)"}},
		{"empty slices use image defaults", []string{}, []string{}, []string{"/entrypoint", "image-argument", "$(POD_VALUE)"}},
		{"command suppresses image cmd", []string{"/override"}, nil, []string{"/override"}},
		{"args retain image entrypoint", nil, []string{"$(POD_VALUE)"}, []string{"/entrypoint", "pod-value"}},
		{"both override image", []string{"$(EXECUTABLE)"}, []string{"$(POD_VALUE)", "$(IMAGE_VALUE)"}, []string{"/bin/echo", "pod-value", "$(IMAGE_VALUE)"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			container := corev1.Container{Command: test.command, Args: test.args, Env: []corev1.EnvVar{
				{Name: "EXECUTABLE", Value: "/bin/echo"}, {Name: "POD_VALUE", Value: "pod-value"},
			}}
			launch, err := resolveApplicationLaunch(container, image, "arm64")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(launch.Args, test.want) || launch.WorkingDir != "/image-directory" || launch.Platform != "linux/arm64" || launch.ImageUser != "1000:2000" {
				t.Fatalf("unexpected application launch: %#v", launch)
			}
			container.WorkingDir = "/pod-directory"
			launch, err = resolveApplicationLaunch(container, image, "arm64")
			if err != nil || launch.WorkingDir != container.WorkingDir {
				t.Fatalf("Pod working directory did not override image: %#v, %v", launch, err)
			}
		})
	}
	if !reflect.DeepEqual(image.Config.Entrypoint, []string{"/entrypoint"}) || !reflect.DeepEqual(image.Config.Cmd, []string{"image-argument", "$(POD_VALUE)"}) {
		t.Fatal("launch resolution changed cached image configuration")
	}
}

func TestApplicationLaunchHandlesCmdOnlyImagesAndEmptyEntrypoint(t *testing.T) {
	var image applicationImageConfig
	image.Config.Cmd = []string{"/bin/sh"}
	for _, entrypoint := range [][]string{nil, {""}} {
		image.Config.Entrypoint = entrypoint
		launch, err := resolveApplicationLaunch(corev1.Container{}, image, "amd64")
		if err != nil || !reflect.DeepEqual(launch.Args, []string{"/bin/sh"}) || launch.WorkingDir != "/" {
			t.Fatalf("CMD-only image failed: %#v, %v", launch, err)
		}
		launch, err = resolveApplicationLaunch(corev1.Container{Args: []string{"/bin/echo", "hello"}}, image, "amd64")
		if err != nil || !reflect.DeepEqual(launch.Args, []string{"/bin/echo", "hello"}) {
			t.Fatalf("args-only launch failed: %#v, %v", launch, err)
		}
	}
	image.Config.Cmd = nil
	if _, err := resolveApplicationLaunch(corev1.Container{}, image, "amd64"); err == nil {
		t.Fatal("image without a command was accepted")
	}
	if _, err := resolveApplicationLaunch(corev1.Container{Command: []string{""}}, image, "amd64"); err == nil {
		t.Fatal("empty executable was accepted")
	}
}

func TestApplicationArgumentExpansionMatchesKubernetes(t *testing.T) {
	environment := map[string]string{"VALUE": "hello world", "EMPTY": "", "NESTED": "$(VALUE)", "UNICODE": "你好"}
	for input, expected := range map[string]string{
		"$(VALUE)":                "hello world",
		"prefix$(VALUE)suffix":    "prefixhello worldsuffix",
		"$(MISSING)":              "$(MISSING)",
		"$$(VALUE)":               "$(VALUE)",
		"$$$(VALUE)":              "$hello world",
		"$(EMPTY)":                "",
		"$(NESTED)":               "$(VALUE)",
		"$(UNICODE)-é":            "你好-é",
		"$(VALUE)$(VALUE)":        "hello worldhello world",
		"${VALUE} $VALUE $":       "${VALUE} $VALUE $",
		"$(VALUE":                 "$(VALUE",
		"$(MISSING$(VALUE))":      "$(MISSING$(VALUE))",
		"'$(VALUE)' \"$(VALUE)\"": "'hello world' \"hello world\"",
		"\\$(VALUE)":              "\\hello world",
	} {
		if actual := expandApplicationArgument(input, environment); actual != expected {
			t.Errorf("%q expanded to %q, want %q", input, actual, expected)
		}
	}
}

func TestCachedImageTargetUsesExactReference(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	image := "docker.io/library/example:1"
	output := "REF TYPE DIGEST SIZE PLATFORMS LABELS\n" + image + "-other type sha256:bad 0 - -\n" + image + " type " + digest + " 1.0 MiB linux/arm64 -\n"
	if actual, err := cachedImageTarget(output, image); err != nil || actual.Digest != digest {
		t.Fatalf("cached target: %#v, %v", actual, err)
	}
	for _, malformed := range []string{"", "sha256:bad", "sha256:" + strings.Repeat("A", 64), "sha256:" + strings.Repeat("0", 64) + ";echo", "../other"} {
		if _, err := cachedImageTarget(image+" type "+malformed, image); err == nil {
			t.Fatalf("invalid digest %q accepted", malformed)
		}
	}
	if _, err := cachedImageTarget(output, "another-image"); err == nil {
		t.Fatal("missing image accepted")
	}
}

func TestImageConfigSelectsGuestPlatformFromCachedIndex(t *testing.T) {
	for _, mediaTypes := range [][2]string{
		{"application/vnd.oci.image.index.v1+json", "application/vnd.oci.image.manifest.v1+json"},
		{"application/vnd.docker.distribution.manifest.list.v2+json", "application/vnd.docker.distribution.manifest.v2+json"},
	} {
		content := map[string]string{
			"index": `{"schemaVersion":2,"mediaType":"` + mediaTypes[0] + `","manifests":[` +
				`{"digest":"unspecified"},` +
				`{"digest":"attestation","platform":{"os":"unknown","architecture":"unknown"}},` +
				`{"digest":"windows","platform":{"os":"windows","architecture":"arm64"}},` +
				`{"digest":"amd64","platform":{"os":"linux","architecture":"amd64"}},` +
				`{"digest":"arm64v9","platform":{"os":"linux","architecture":"arm64","variant":"v9"}},` +
				`{"digest":"arm64","platform":{"os":"linux","architecture":"arm64","variant":"v8"}}]}`,
			"arm64":  `{"schemaVersion":2,"mediaType":"` + mediaTypes[1] + `","config":{"digest":"config"}}`,
			"config": `{"os":"linux","architecture":"arm64","config":{"Entrypoint":["/entrypoint"],"Cmd":["serve"]}}`,
		}
		var reads []string
		config, err := readApplicationImageConfig(imageDescriptor{Digest: "index"}, "arm64", func(digest string) ([]byte, error) {
			reads = append(reads, digest)
			value, exists := content[digest]
			if !exists {
				return nil, errors.New("content is not cached")
			}
			return []byte(value), nil
		}, 0)
		if err != nil || !reflect.DeepEqual(config.Config.Entrypoint, []string{"/entrypoint"}) || !reflect.DeepEqual(reads, []string{"index", "arm64", "config"}) {
			t.Fatalf("wrong cached platform selected: %#v, reads %v, %v", config, reads, err)
		}
	}
}

func TestImageConfigAcceptsOCIOptionalMediaTypeAndNestedIndex(t *testing.T) {
	content := map[string]string{
		"index":    `{"schemaVersion":2,"manifests":[{"digest":"nested","mediaType":"application/vnd.oci.image.index.v1+json"}]}`,
		"nested":   `{"schemaVersion":2,"manifests":[{"digest":"manifest","mediaType":"application/vnd.oci.image.manifest.v1+json"}]}`,
		"manifest": `{"schemaVersion":2,"config":{"digest":"config"}}`,
		"config":   `{"os":"linux","architecture":"arm64","config":{"Cmd":["/bin/sh"]}}`,
	}
	config, err := readApplicationImageConfig(imageDescriptor{Digest: "index", MediaType: "application/vnd.oci.image.index.v1+json"}, "arm64", func(digest string) ([]byte, error) {
		return []byte(content[digest]), nil
	}, 0)
	if err != nil || !reflect.DeepEqual(config.Config.Cmd, []string{"/bin/sh"}) {
		t.Fatalf("OCI media type inheritance failed: %#v, %v", config, err)
	}
	content["index"] = `{"schemaVersion":2,"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[{"digest":"nested","mediaType":"application/vnd.oci.image.index.v1+json"}]}`
	if _, err := readApplicationImageConfig(imageDescriptor{Digest: "index", MediaType: "application/vnd.oci.image.index.v1+json"}, "arm64", func(digest string) ([]byte, error) {
		return []byte(content[digest]), nil
	}, 0); err != nil {
		t.Fatalf("compatible Docker and OCI index media types were rejected: %v", err)
	}
	content["index"] = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"digest":"config"}}`
	if _, err := readApplicationImageConfig(imageDescriptor{Digest: "index", MediaType: "application/vnd.oci.image.index.v1+json"}, "arm64", func(digest string) ([]byte, error) {
		return []byte(content[digest]), nil
	}, 0); err == nil {
		t.Fatal("conflicting OCI media type was accepted")
	}
}

func TestImageConfigRejectsInvalidOrUnavailableMetadata(t *testing.T) {
	for name, content := range map[string]map[string]string{
		"missing content": {},
		"invalid JSON":    {"target": "{"},
		"schema one":      {"target": `{"schemaVersion":1}`},
		"unknown format":  {"target": `{"schemaVersion":2,"mediaType":"not-an-image"}`},
		"missing platform": {"target": `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[` +
			`{"digest":"windows","platform":{"os":"windows","architecture":"arm64"}}]}`},
		"recursive index": {"target": `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.index.v1+json","manifests":[{"digest":"target"}]}`},
		"mismatched config": {
			"target": `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"digest":"config"}}`,
			"config": `{"os":"linux","architecture":"amd64"}`,
		},
		"invalid config": {
			"target": `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"digest":"config"}}`,
			"config": `{"os":"linux","architecture":"arm64","config":{"Cmd":"not-an-array"}}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := readApplicationImageConfig(imageDescriptor{Digest: "target"}, "arm64", func(digest string) ([]byte, error) {
				value, exists := content[digest]
				if !exists {
					return nil, errors.New("content is not cached")
				}
				return []byte(value), nil
			}, 0)
			if err == nil {
				t.Fatal("invalid image metadata accepted")
			}
		})
	}
}

func TestApplicationLaunchReadsOnlyGuestCachedMetadata(t *testing.T) {
	p, e := probeFixture(t)
	container := e.pod.Spec.Containers[0]
	container.Command = nil
	container.Args = []string{"--serve"}
	image := normalizeImage(container.Image)
	manifestDigest := "sha256:" + strings.Repeat("1", 64)
	configDigest := "sha256:" + strings.Repeat("2", 64)
	log := filepath.Join(p.runner.cfg.state, "calls")
	script := "#!/bin/sh\nprintf '%s\\n' \"$*\" >>" + quote(log) + "\ncase \"$*\" in\n" +
		"*'images list'*) printf '%s\\n' " + quote(image+" application/vnd.oci.image.manifest.v1+json "+manifestDigest+" 1 MiB linux/"+runtime.GOARCH+" -") + ";;\n" +
		"*'content get'*" + manifestDigest + "*) printf '%s\\n' " + quote(`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"digest":"`+configDigest+`"}}`) + ";;\n" +
		"*'content get'*" + configDigest + "*) printf '%s\\n' " + quote(`{"os":"linux","architecture":"`+runtime.GOARCH+`","config":{"Entrypoint":["/entrypoint"],"Cmd":["original"]}}`) + ";;\n" +
		"*) exit 2;;\nesac\n"
	if err := os.WriteFile(p.runner.cfg.msb, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	launch, err := p.applicationLaunch(t.Context(), e, container)
	if err != nil || !reflect.DeepEqual(launch.Args, []string{"/entrypoint", "--serve"}) {
		t.Fatalf("resolve cached image launch: %#v, %v", launch, err)
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "pull") || len(strings.Split(strings.TrimSpace(string(data)), "\n")) != 3 {
		t.Fatalf("launch resolution accessed more than guest metadata: %s", data)
	}
}
