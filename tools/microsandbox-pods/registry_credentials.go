package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"path"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type registryCredential struct {
	username string
	password string
}

type dockerAuthEntry struct {
	Username      string `json:"username"`
	Password      string `json:"password"`
	Auth          string `json:"auth"`
	IdentityToken string `json:"identitytoken"`
	RegistryToken string `json:"registrytoken"`
}

// Credentials are resolved only for an actual pull. They are never retained on
// podEntry or in its journal, so the next pull observes Secret rotation.
func (p *provider) imageCredentials(ctx context.Context, pod *corev1.Pod, image string) ([]registryCredential, error) {
	if len(pod.Spec.ImagePullSecrets) == 0 {
		return nil, nil
	}
	if p.client == nil {
		return nil, errors.New("imagePullSecrets require a Kubernetes client")
	}
	entries := make(map[string][]registryCredential)
	seen := make(map[string]bool)
	for _, reference := range pod.Spec.ImagePullSecrets {
		if reference.Name == "" {
			return nil, errors.New("imagePullSecrets contains an empty Secret name")
		}
		if seen[reference.Name] {
			continue
		}
		seen[reference.Name] = true
		secret, err := p.client.CoreV1().Secrets(pod.Namespace).Get(ctx, reference.Name, metav1.GetOptions{})
		if err != nil {
			// API errors can contain arbitrary response bodies. Keep Secret data
			// and server-provided text out of the persisted Pod failure message.
			return nil, fmt.Errorf("cannot read imagePullSecret %s/%s", pod.Namespace, reference.Name)
		}
		auths, err := dockerSecretAuths(secret)
		if err != nil {
			return nil, fmt.Errorf("imagePullSecret %s/%s: %w", pod.Namespace, reference.Name, err)
		}
		for _, location := range slices.Sorted(maps.Keys(auths)) {
			key, err := registryCredentialKey(location)
			if err != nil {
				return nil, fmt.Errorf("imagePullSecret %s/%s contains an invalid registry address", pod.Namespace, reference.Name)
			}
			credential, err := auths[location].credential()
			if err != nil {
				return nil, fmt.Errorf("imagePullSecret %s/%s: %w", pod.Namespace, reference.Name, err)
			}
			entries[key] = append(entries[key], credential)
		}
	}
	keys := slices.Sorted(maps.Keys(entries))
	slices.Reverse(keys)
	var credentials []registryCredential
	for _, key := range keys {
		if registryCredentialMatches(key, image) {
			credentials = append(credentials, entries[key]...)
		}
	}
	// Docker's historic config key is index.docker.io/v1/, whereas normalized
	// image references normally use docker.io. Kubernetes uses this fallback
	// only when there was no more specific credential match.
	if len(credentials) == 0 {
		host, _, _ := strings.Cut(image, "/")
		if host == "docker.io" || host == "index.docker.io" {
			credentials = append(credentials, entries["index.docker.io"]...)
		}
	}
	return credentials, nil
}

func dockerSecretAuths(secret *corev1.Secret) (map[string]dockerAuthEntry, error) {
	var auths map[string]dockerAuthEntry
	switch secret.Type {
	case corev1.SecretTypeDockerConfigJson:
		var config struct {
			Auths       map[string]dockerAuthEntry `json:"auths"`
			CredsStore  string                     `json:"credsStore"`
			CredHelpers map[string]string          `json:"credHelpers"`
		}
		if err := json.Unmarshal(secret.Data[corev1.DockerConfigJsonKey], &config); err != nil {
			return nil, errors.New("invalid .dockerconfigjson data")
		}
		if config.CredsStore != "" || len(config.CredHelpers) != 0 {
			return nil, errors.New("credential helper references are not supported; supply inline registry credentials")
		}
		auths = config.Auths
	case corev1.SecretTypeDockercfg:
		if err := json.Unmarshal(secret.Data[corev1.DockerConfigKey], &auths); err != nil {
			return nil, errors.New("invalid .dockercfg data")
		}
	default:
		return nil, errors.New("expected kubernetes.io/dockerconfigjson or kubernetes.io/dockercfg Secret type")
	}
	if auths == nil {
		return nil, errors.New("Docker credentials must contain an auths object")
	}
	return auths, nil
}

func (entry dockerAuthEntry) credential() (registryCredential, error) {
	if entry.IdentityToken != "" || entry.RegistryToken != "" {
		return registryCredential{}, errors.New("registry token fields are not supported; supply username/password or auth")
	}
	username, password := entry.Username, entry.Password
	if entry.Auth != "" {
		encoding := base64.RawStdEncoding
		if strings.HasSuffix(strings.TrimSpace(entry.Auth), "=") {
			encoding = base64.StdEncoding
		}
		decoded, err := encoding.DecodeString(entry.Auth)
		if err != nil {
			return registryCredential{}, errors.New("invalid base64 registry auth")
		}
		var found bool
		username, password, found = strings.Cut(string(decoded), ":")
		if !found {
			return registryCredential{}, errors.New("registry auth must encode username:password")
		}
	}
	// ctr interprets an empty password as an interactive terminal prompt.
	if username == "" || password == "" || strings.ContainsAny(username, ":\x00\r\n") || strings.ContainsRune(password, '\x00') {
		return registryCredential{}, errors.New("registry credentials require a nonempty username and password without NUL bytes or username separators")
	}
	return registryCredential{username: username, password: password}, nil
}

func registryCredentialKey(location string) (string, error) {
	if !strings.HasPrefix(location, "http://") && !strings.HasPrefix(location, "https://") {
		if strings.Contains(location, "://") {
			return "", errors.New("invalid registry scheme")
		}
		location = "https://" + location
	}
	parsed, err := url.Parse(location)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", errors.New("invalid registry address")
	}
	for component := range strings.SplitSeq(parsed.Hostname(), ".") {
		if _, err := path.Match(component, ""); err != nil {
			return "", errors.New("invalid registry hostname pattern")
		}
	}
	registryPath := parsed.Path
	if strings.HasPrefix(registryPath, "/v1/") || strings.HasPrefix(registryPath, "/v2/") {
		registryPath = registryPath[3:]
	}
	if registryPath == "/" {
		registryPath = ""
	}
	return parsed.Host + registryPath, nil
}

// Kubernetes permits hostname globs with one pattern per DNS component and
// literal path prefixes. In particular, *.example.test does not match the
// registry example.test or two levels such as a.b.example.test.
func registryCredentialMatches(key, image string) bool {
	pattern, err := url.Parse("https://" + key)
	if err != nil {
		return false
	}
	target, err := url.Parse("https://" + image)
	if err != nil || pattern.Port() != target.Port() || !strings.HasPrefix(target.Path, pattern.Path) {
		return false
	}
	patternHost := strings.Split(pattern.Hostname(), ".")
	targetHost := strings.Split(target.Hostname(), ".")
	if len(patternHost) != len(targetHost) {
		return false
	}
	for index, component := range patternHost {
		matches, err := path.Match(component, targetHost[index])
		if err != nil || !matches {
			return false
		}
	}
	return true
}

func authenticatedPullScript(image, caPath string) string {
	// Secret bytes travel only on msb's stdin. The terminal-free ctr interface
	// still puts credentials in its guest process argv during the pull. Pod
	// containers have a separate PID namespace and cannot inspect this process.
	// Do not write a credential file, include input in shell source, or retain
	// registry output: even a registry error response could echo credentials.
	// Resolve in this short-lived ctr process, not the daemon transfer service,
	// so credentials and current CA roots belong to this pull only.
	return "set +x\n" +
		"IFS= read -r pull_encoded\n" +
		"pull_credential=$(printf '%s' \"$pull_encoded\" | base64 -d; printf '.')\n" +
		"pull_credential=${pull_credential%.}\n" +
		"unset pull_encoded\n" +
		"trap 'unset pull_credential' EXIT HUP INT TERM\n" +
		imagePullCommand(caPath, true) + " --user \"$pull_credential\" " + quote(image) + " </dev/null >/dev/null 2>&1\n"
}

func (p *provider) pullWithCredentials(ctx context.Context, vm, image string, credentials []registryCredential) error {
	for _, credential := range credentials {
		input := []byte(base64.StdEncoding.EncodeToString([]byte(credential.username+":"+credential.password)) + "\n")
		err := p.runner.guestInput(ctx, vm, authenticatedPullScript(image, p.runner.guestCAPath()), input)
		clear(input)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return fmt.Errorf("authenticated image pull canceled: %w", ctx.Err())
		}
	}
	return fmt.Errorf("pull image %s failed with all %d matching registry credentials", image, len(credentials))
}
