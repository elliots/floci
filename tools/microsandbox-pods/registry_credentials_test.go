package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func registrySecret(t *testing.T, namespace, name string, auths map[string]dockerAuthEntry) *corev1.Secret {
	t.Helper()
	data, err := json.Marshal(map[string]any{"auths": auths})
	if err != nil {
		t.Fatal(err)
	}
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}, Type: corev1.SecretTypeDockerConfigJson,
		Data: map[string][]byte{corev1.DockerConfigJsonKey: data}}
}

func TestRegistryCredentialMatchingUsesKubernetesHostnameAndPathRules(t *testing.T) {
	for _, test := range []struct {
		location, image string
		matches         bool
	}{
		{"registry.test", "registry.test/repo:v1", true},
		{"registry.test", "registry.test.evil/repo:v1", false},
		{"registry.test:443", "registry.test/repo:v1", false},
		{"registry.test:443", "registry.test:443/repo:v1", true},
		{"https://registry.test/v2/team/", "registry.test/team/app:v1", true},
		{"http://registry.test/v1/", "registry.test/app:v1", true},
		{"registry.test/team", "registry.test/team-extra/app:v1", true},
		{"registry.test/team/", "registry.test/team-extra/app:v1", false},
		{"*.registry.test/team", "a.registry.test/team/app:v1", true},
		{"*.registry.test", "a.b.registry.test/repo:v1", false},
		{"*.registry.test", "registry.test/repo:v1", false},
		{"prefix*.registry.test", "prefix-two.registry.test/repo@sha256:abc", true},
		{"prefix*.registry.test", "different.registry.test/repo:v1", false},
		{"*.*.test", "one.two.test/repo:v1", true},
		{"[::1]:5000", "[::1]:5000/repo:v1", true},
	} {
		t.Run(test.location+"/"+test.image, func(t *testing.T) {
			key, err := registryCredentialKey(test.location)
			if err != nil {
				t.Fatal(err)
			}
			if actual := registryCredentialMatches(key, test.image); actual != test.matches {
				t.Fatalf("credential match was %t, expected %t", actual, test.matches)
			}
		})
	}
}

func TestImageCredentialsOrderAndRotateWithoutLeavingPodNamespace(t *testing.T) {
	pod := validPod()
	pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull"}, {Name: "overlap"}, {Name: "pull"}}
	secret := registrySecret(t, pod.Namespace, "pull", map[string]dockerAuthEntry{
		"registry.test": {Username: "broad", Password: "old"}, "registry.test/team": {Username: "specific", Password: "old"},
	})
	other := registrySecret(t, pod.Namespace, "overlap", map[string]dockerAuthEntry{
		"registry.test/team": {Auth: base64.RawStdEncoding.EncodeToString([]byte("overlap:token:with:colons"))},
	})
	foreign := registrySecret(t, "foreign", "pull", map[string]dockerAuthEntry{"registry.test": {Username: "foreign", Password: "wrong"}})
	client := fake.NewClientset(secret, other, foreign)
	p := &provider{client: client}
	credentials, err := p.imageCredentials(t.Context(), pod, "registry.test/team/app:v1")
	if err != nil {
		t.Fatal(err)
	}
	want := []registryCredential{{username: "specific", password: "old"}, {username: "overlap", password: "token:with:colons"}, {username: "broad", password: "old"}}
	if !slices.Equal(credentials, want) {
		t.Fatal("credentials were not ordered by most specific match and Secret order")
	}
	if len(client.Actions()) != 2 {
		t.Fatal("duplicate Secret reference caused an extra lookup")
	}
	for _, action := range client.Actions() {
		if action.GetNamespace() != pod.Namespace {
			t.Fatal("looked for imagePullSecrets outside the Pod namespace")
		}
	}
	updated := registrySecret(t, pod.Namespace, "pull", map[string]dockerAuthEntry{"registry.test/team": {Username: "rotated", Password: "new"}})
	if _, err := client.CoreV1().Secrets(pod.Namespace).Update(t.Context(), updated, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	credentials, err = p.imageCredentials(t.Context(), pod, "registry.test/team/app:v1")
	if err != nil || len(credentials) != 2 || credentials[0].username != "rotated" || credentials[0].password != "new" {
		t.Fatalf("next pull did not observe rotation: %v", err)
	}
}

func TestDockerHubCredentialsUseLegacyFallbackAfterSpecificMatches(t *testing.T) {
	pod := validPod()
	pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull"}}
	secret := registrySecret(t, pod.Namespace, "pull", map[string]dockerAuthEntry{
		"https://index.docker.io/v1/": {Username: "legacy", Password: "token"}, "docker.io/team": {Username: "specific", Password: "token"},
	})
	p := &provider{client: fake.NewClientset(secret)}
	for image, username := range map[string]string{"docker.io/library/busybox:1": "legacy", "docker.io/team/app:1": "specific", "index.docker.io/team/app:1": "legacy"} {
		credentials, err := p.imageCredentials(t.Context(), pod, image)
		if err != nil || len(credentials) != 1 || credentials[0].username != username {
			t.Fatalf("Docker Hub credential lookup failed for %s: %v", image, err)
		}
	}
	credentials, err := p.imageCredentials(t.Context(), pod, "unrelated.test/team/app:1")
	if err != nil || len(credentials) != 0 {
		t.Fatalf("credentials escaped their registry: %v", err)
	}
}

func TestDockerSecretTypesAndCredentialValidation(t *testing.T) {
	validAuth := base64.StdEncoding.EncodeToString([]byte("user:secret:token"))
	for _, test := range []struct {
		name, data   string
		typeName     corev1.SecretType
		key, failure string
	}{
		{"legacy", `{"registry.test":{"auth":"` + validAuth + `"}}`, corev1.SecretTypeDockercfg, corev1.DockerConfigKey, ""},
		{"modern", `{"auths":{"registry.test":{"username":"user","password":"secret:token"}}}`, corev1.SecretTypeDockerConfigJson, corev1.DockerConfigJsonKey, ""},
		{"auth wins", `{"auths":{"registry.test":{"username":"ignored","password":"ignored","auth":"` + validAuth + `"}}}`, corev1.SecretTypeDockerConfigJson, corev1.DockerConfigJsonKey, ""},
		{"wrong type", `secret-canary`, corev1.SecretTypeOpaque, corev1.DockerConfigJsonKey, "Secret type"},
		{"invalid JSON", `secret-canary`, corev1.SecretTypeDockerConfigJson, corev1.DockerConfigJsonKey, "invalid .dockerconfigjson"},
		{"missing data", `{}`, corev1.SecretTypeDockerConfigJson, "wrong-key", "invalid .dockerconfigjson"},
		{"missing auths", `{}`, corev1.SecretTypeDockerConfigJson, corev1.DockerConfigJsonKey, "auths object"},
		{"helper", `{"auths":{},"credsStore":"secret-canary"}`, corev1.SecretTypeDockerConfigJson, corev1.DockerConfigJsonKey, "credential helper"},
		{"invalid auth", `{"auths":{"registry.test":{"auth":"secret-canary"}}}`, corev1.SecretTypeDockerConfigJson, corev1.DockerConfigJsonKey, "invalid base64"},
		{"empty password", `{"auths":{"registry.test":{"username":"secret-canary"}}}`, corev1.SecretTypeDockerConfigJson, corev1.DockerConfigJsonKey, "nonempty username and password"},
		{"token only", `{"auths":{"registry.test":{"identitytoken":"secret-canary"}}}`, corev1.SecretTypeDockerConfigJson, corev1.DockerConfigJsonKey, "token fields"},
		{"userinfo", `{"auths":{"user:secret-canary@registry.test":{"username":"user","password":"token"}}}`, corev1.SecretTypeDockerConfigJson, corev1.DockerConfigJsonKey, "invalid registry address"},
		{"invalid glob", `{"auths":{"reg[.test":{"username":"user","password":"secret-canary"}}}`, corev1.SecretTypeDockerConfigJson, corev1.DockerConfigJsonKey, "invalid registry address"},
		{"NUL", `{"auths":{"registry.test":{"username":"user","password":"secret-canary\u0000"}}}`, corev1.SecretTypeDockerConfigJson, corev1.DockerConfigJsonKey, "NUL"},
	} {
		t.Run(test.name, func(t *testing.T) {
			pod := validPod()
			pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull"}}
			secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: pod.Namespace, Name: "pull"}, Type: test.typeName, Data: map[string][]byte{test.key: []byte(test.data)}}
			p := &provider{client: fake.NewClientset(secret)}
			credentials, err := p.imageCredentials(t.Context(), pod, "registry.test/repo:1")
			if test.failure == "" {
				if err != nil || len(credentials) != 1 || credentials[0].username != "user" || credentials[0].password != "secret:token" {
					t.Fatalf("valid credentials failed: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.failure) || strings.Contains(err.Error(), "secret-canary") {
				t.Fatalf("expected sanitized error containing %q", test.failure)
			}
		})
	}
}

func TestMissingPullSecretAndAPIErrorsDoNotExposeResponseBodies(t *testing.T) {
	p, e := probeFixture(t)
	e.pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "missing"}}
	client := fake.NewClientset()
	p.client = client
	if _, err := p.imageCredentials(t.Context(), e.pod, "registry.test/repo:1"); err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatal("missing Secret was not diagnosed")
	}
	client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("secret-canary") })
	if _, err := p.imageCredentials(t.Context(), e.pod, "registry.test/repo:1"); err == nil || strings.Contains(err.Error(), "secret-canary") {
		t.Fatal("Secret API response was exposed")
	}
}

func TestCachedImagesDoNotReadPullSecrets(t *testing.T) {
	for _, policy := range []corev1.PullPolicy{corev1.PullIfNotPresent, corev1.PullNever} {
		t.Run(string(policy), func(t *testing.T) {
			p, e := probeFixture(t)
			e.pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "missing"}}
			container := e.pod.Spec.Containers[0]
			container.ImagePullPolicy = policy
			client := fake.NewClientset()
			p.client = client
			script := "#!/bin/sh\nprintf '%s\\n' " + quote(normalizeImage(container.Image)) + "\n"
			if err := os.WriteFile(p.runner.cfg.msb, []byte(script), 0700); err != nil {
				t.Fatal(err)
			}
			if err := p.ensureApplicationImage(t.Context(), e, container); err != nil {
				t.Fatal(err)
			}
			if len(client.Actions()) != 0 {
				t.Fatal("cached image consulted registry credentials")
			}
		})
	}
}

func TestAuthenticatedPullUsesOnlyGuestStdinAndRetriesMatchingCredentials(t *testing.T) {
	p, e := probeFixture(t)
	directory := p.runner.cfg.state
	hostArgs := filepath.Join(directory, "host-args")
	guestArgs := filepath.Join(directory, "guest-args")
	injectionMarker := filepath.Join(directory, "injected")
	secret := "secret-'\"-$(touch " + injectionMarker + ")-`touch " + injectionMarker + "`-\\-\ntrailing\n"
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" >> " + quote(hostArgs) + "\nwhile test \"$1\" != --; do shift; done\nshift\nexec \"$@\"\n"
	if err := os.WriteFile(p.runner.cfg.msb, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	ctrScript := "#!/bin/sh\nprintf '%s\\000' \"$@\" >> " + quote(guestArgs) + "\ncase \"$*\" in *wrong*) echo secret-canary >&2; exit 1;; esac\n"
	if err := os.WriteFile(filepath.Join(directory, "ctr"), []byte(ctrScript), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", directory+string(os.PathListSeparator)+os.Getenv("PATH"))
	credentials := []registryCredential{{username: "first", password: "wrong"}, {username: "second", password: secret}}
	if err := p.pullWithCredentials(t.Context(), e.vm, "registry.test/repo:v1", credentials); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(hostArgs)
	if err != nil {
		t.Fatal(err)
	}
	for _, sensitive := range []string{secret, "first:wrong", "second:", base64.StdEncoding.EncodeToString([]byte("second:" + secret))} {
		if strings.Contains(string(data), sensitive) {
			t.Fatal("credentials appeared in host process arguments")
		}
	}
	data, err = os.ReadFile(guestArgs)
	if err != nil {
		t.Fatal(err)
	}
	args := strings.Split(string(data), "\x00")
	if !slices.Contains(args, "first:wrong") || !slices.Contains(args, "second:"+secret) {
		t.Fatal("guest transport altered credentials or did not retry")
	}
	if _, err := os.Stat(injectionMarker); !os.IsNotExist(err) {
		t.Fatal("credential bytes were interpreted as shell code")
	}
	if err := p.pullWithCredentials(t.Context(), e.vm, "registry.test/repo:v1", credentials[:1]); err == nil || strings.Contains(err.Error(), "secret-canary") || strings.Contains(err.Error(), "wrong") {
		t.Fatal("failed authenticated pull exposed credentials")
	}
}

func TestImagePullSecretAdmission(t *testing.T) {
	pod := validPod()
	pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull"}}
	if err := validatePod(pod); err != nil {
		t.Fatal(err)
	}
}

func TestUnmatchedPullCredentialsLeavePublicImagesAnonymous(t *testing.T) {
	p, e := probeFixture(t)
	e.pod.Spec.ImagePullSecrets = []corev1.LocalObjectReference{{Name: "pull"}}
	p.client = fake.NewClientset(registrySecret(t, e.pod.Namespace, "pull", map[string]dockerAuthEntry{
		"unrelated.test": {Username: "secret-user", Password: "secret-password"},
	}))
	log := filepath.Join(p.runner.cfg.state, "calls")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" >> " + quote(log) + "\n"
	if err := os.WriteFile(p.runner.cfg.msb, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	container := e.pod.Spec.Containers[0]
	container.ImagePullPolicy = corev1.PullAlways
	if err := p.ensureApplicationImage(t.Context(), e, container); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "images pull") || strings.Contains(string(data), "--user") || strings.Contains(string(data), "secret-") {
		t.Fatal("unrelated credentials changed the anonymous pull")
	}
}
