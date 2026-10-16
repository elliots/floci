package main

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

type tokenProjectionKey struct {
	podUID     types.UID
	account    string
	audience   string
	expiration int64
}

type tokenProjectionEntry struct {
	token     string
	refreshAt time.Time
}

// Credentials live only in controller memory and the guest's projected tmpfs.
// They are intentionally absent from lifecycle journals and command arguments.
// The mutex also covers pre-admission reads, before the Pod monitor starts.
type serviceAccountTokens struct {
	mu      sync.Mutex
	entries map[tokenProjectionKey]tokenProjectionEntry
	now     func() time.Time
}

func (cache *serviceAccountTokens) clear() {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.entries = nil
}

func validateTokenProjection(source *corev1.ServiceAccountTokenProjection) error {
	if err := validateProjectionPath(source.Path); err != nil {
		return err
	}
	if source.User != nil {
		return errors.New("serviceAccountToken ownership overrides are not qualified")
	}
	if source.ExpirationSeconds != nil && *source.ExpirationSeconds < 600 {
		return errors.New("serviceAccountToken expirationSeconds must be at least 600")
	}
	return nil
}

func (cache *serviceAccountTokens) token(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod, source *corev1.ServiceAccountTokenProjection) (string, error) {
	if err := validateTokenProjection(source); err != nil {
		return "", err
	}
	if client == nil || pod.UID == "" || pod.Namespace == "" || pod.Name == "" {
		return "", errors.New("serviceAccountToken requires a Kubernetes client and complete Pod identity")
	}
	expiration := int64(3600)
	if source.ExpirationSeconds != nil {
		expiration = *source.ExpirationSeconds
	}
	key := tokenProjectionKey{podUID: pod.UID, account: cmp.Or(pod.Spec.ServiceAccountName, "default"), audience: source.Audience, expiration: expiration}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	now := time.Now()
	if cache.now != nil {
		now = cache.now()
	}
	if previous, found := cache.entries[key]; found && now.Before(previous.refreshAt) {
		return previous.token, nil
	}
	account, err := client.CoreV1().ServiceAccounts(pod.Namespace).Get(ctx, key.account, metav1.GetOptions{})
	if err != nil {
		return "", tokenAPIError("read ServiceAccount", err)
	}
	if account.UID == "" || account.Namespace != pod.Namespace || account.Name != key.account {
		return "", errors.New("ServiceAccount response does not match the requested identity")
	}
	request := &authenticationv1.TokenRequest{ObjectMeta: metav1.ObjectMeta{Name: account.Name, Namespace: pod.Namespace, UID: account.UID},
		Spec: authenticationv1.TokenRequestSpec{ExpirationSeconds: new(expiration),
			BoundObjectRef: &authenticationv1.BoundObjectReference{APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID}}}
	if source.Audience != "" {
		request.Spec.Audiences = []string{source.Audience}
	}
	response, err := client.CoreV1().ServiceAccounts(pod.Namespace).CreateToken(ctx, account.Name, request, metav1.CreateOptions{})
	if err != nil {
		return "", tokenAPIError("request Pod-bound ServiceAccount token", err)
	}
	if err := validateTokenResponse(response, pod, account, source.Audience, now); err != nil {
		return "", err
	}
	ttl := response.Status.ExpirationTimestamp.Sub(now)
	refreshAt := now.Add(min(ttl/5*4, 24*time.Hour))
	if cache.entries == nil {
		cache.entries = make(map[tokenProjectionKey]tokenProjectionEntry)
	}
	cache.entries[key] = tokenProjectionEntry{token: response.Status.Token, refreshAt: refreshAt}
	return response.Status.Token, nil
}

type tokenStatusError interface {
	error
	apierrors.APIStatus
}

func tokenAPIError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: %w", operation, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", operation, context.DeadlineExceeded)
	}
	if status, ok := errors.AsType[tokenStatusError](err); ok {
		return fmt.Errorf("%s: Kubernetes API status %d", operation, status.Status().Code)
	}
	// API response diagnostics can contain credential material. Preserve the
	// operation and status, never an untrusted response body in Pod events.
	return fmt.Errorf("%s: Kubernetes API request failed", operation)
}

func validateTokenResponse(response *authenticationv1.TokenRequest, pod *corev1.Pod, account *corev1.ServiceAccount, audience string, now time.Time) error {
	invalid := errors.New("Kubernetes returned an invalid Pod-bound ServiceAccount token")
	if response == nil || response.UID != "" && response.UID != account.UID || response.Name != account.Name || response.Namespace != pod.Namespace ||
		!response.Status.ExpirationTimestamp.After(now.Add(30*time.Second)) || len(response.Spec.Audiences) == 0 {
		return invalid
	}
	bound := response.Spec.BoundObjectRef
	if bound == nil || bound.Kind != "Pod" || bound.APIVersion != "v1" || bound.Name != pod.Name || bound.UID != pod.UID {
		return invalid
	}
	if audience != "" && (len(response.Spec.Audiences) != 1 || response.Spec.Audiences[0] != audience) {
		return invalid
	}
	parts := strings.Split(response.Status.Token, ".")
	if len(parts) != 3 || len(response.Status.Token) > 1<<20 || parts[2] == "" {
		return invalid
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return invalid
	}
	var header struct {
		Algorithm string `json:"alg"`
	}
	if json.Unmarshal(headerBytes, &header) != nil || header.Algorithm == "" || strings.EqualFold(header.Algorithm, "none") {
		return invalid
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return invalid
	}
	var claims struct {
		Issuer     string          `json:"iss"`
		Subject    string          `json:"sub"`
		Audience   json.RawMessage `json:"aud"`
		Expires    int64           `json:"exp"`
		NotBefore  int64           `json:"nbf"`
		Kubernetes struct {
			Namespace string `json:"namespace"`
			Pod       struct {
				Name string    `json:"name"`
				UID  types.UID `json:"uid"`
			} `json:"pod"`
			Account struct {
				Name string    `json:"name"`
				UID  types.UID `json:"uid"`
			} `json:"serviceaccount"`
		} `json:"kubernetes.io"`
	}
	// The authenticated Kubernetes API supplies the token. These checks verify
	// response identity and lifetime; recipients still verify its signature.
	if json.Unmarshal(payload, &claims) != nil || claims.Issuer == "" || claims.Expires <= now.Unix() || claims.NotBefore > now.Add(30*time.Second).Unix() ||
		claims.Subject != "system:serviceaccount:"+pod.Namespace+":"+account.Name || claims.Kubernetes.Namespace != pod.Namespace ||
		claims.Kubernetes.Pod.Name != pod.Name || claims.Kubernetes.Pod.UID != pod.UID ||
		claims.Kubernetes.Account.Name != account.Name || claims.Kubernetes.Account.UID != account.UID ||
		response.Status.ExpirationTimestamp.After(time.Unix(claims.Expires, 0).Add(time.Second)) {
		return invalid
	}
	var audiences []string
	if json.Unmarshal(claims.Audience, &audiences) != nil {
		var single string
		if json.Unmarshal(claims.Audience, &single) != nil {
			return invalid
		}
		audiences = []string{single}
	}
	if len(audiences) != len(response.Spec.Audiences) {
		return invalid
	}
	for _, expected := range response.Spec.Audiences {
		if expected == "" || !slices.Contains(audiences, expected) {
			return invalid
		}
	}
	return nil
}
