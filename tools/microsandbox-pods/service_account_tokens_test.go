package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
)

type tokenFixture struct {
	client   *fake.Clientset
	pod      *corev1.Pod
	account  *corev1.ServiceAccount
	cache    serviceAccountTokens
	now      time.Time
	requests []*authenticationv1.TokenRequest
	failure  error
	mutate   func(*authenticationv1.TokenRequest)
}

func newTokenFixture(t *testing.T) *tokenFixture {
	t.Helper()
	f := &tokenFixture{now: time.Now().Truncate(time.Second)}
	f.pod = validPod()
	f.pod.Spec.ServiceAccountName = "application"
	f.account = &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "application", Namespace: f.pod.Namespace, UID: "account-uid"}}
	f.client = fake.NewClientset(f.account, f.pod)
	f.cache.now = func() time.Time { return f.now }
	f.client.PrependReactor("create", "serviceaccounts", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "token" {
			return false, nil, nil
		}
		request := action.(ktesting.CreateAction).GetObject().(*authenticationv1.TokenRequest).DeepCopy()
		f.requests = append(f.requests, request.DeepCopy())
		if action.GetNamespace() != f.pod.Namespace || request.Spec.BoundObjectRef.UID != f.pod.UID {
			t.Error("token request escaped exact namespaced Pod identity")
		}
		if f.failure != nil {
			return true, nil, f.failure
		}
		if len(request.Spec.Audiences) == 0 {
			request.Spec.Audiences = []string{"https://kubernetes.default.svc"}
		}
		request.Status.ExpirationTimestamp = metav1.NewTime(f.now.Add(time.Duration(*request.Spec.ExpirationSeconds) * time.Second))
		claims := map[string]any{"iss": "https://kubernetes.default.svc", "sub": "system:serviceaccount:" + f.pod.Namespace + ":" + f.account.Name,
			"aud": request.Spec.Audiences, "exp": request.Status.ExpirationTimestamp.Unix(), "nbf": f.now.Unix(), "jti": fmt.Sprint(len(f.requests)),
			"kubernetes.io": map[string]any{"namespace": f.pod.Namespace, "pod": map[string]any{"name": f.pod.Name, "uid": f.pod.UID},
				"serviceaccount": map[string]any{"name": f.account.Name, "uid": f.account.UID}}}
		request.Status.Token = fixtureToken(t, claims)
		if f.mutate != nil {
			f.mutate(request)
		}
		return true, request, nil
	})
	return f
}
func fixtureToken(t *testing.T, claims map[string]any) string {
	t.Helper()
	payload, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256"}`)) + "." + base64.RawURLEncoding.EncodeToString(payload) + ".c2lnbmF0dXJl"
}
func mutateTokenClaims(t *testing.T, response *authenticationv1.TokenRequest, mutate func(map[string]any)) {
	t.Helper()
	payload, err := base64.RawURLEncoding.DecodeString(strings.Split(response.Status.Token, ".")[1])
	if err != nil {
		t.Fatal(err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		t.Fatal(err)
	}
	mutate(claims)
	response.Status.Token = fixtureToken(t, claims)
}
func TestProjectedTokenUsesDefaultAudienceAndBoundPodWithRotation(t *testing.T) {
	f := newTokenFixture(t)
	source := &corev1.ServiceAccountTokenProjection{Path: "token"}
	initial, err := f.cache.token(t.Context(), f.client, f.pod, source)
	if err != nil {
		t.Fatal(err)
	}
	request := f.requests[0]
	if len(request.Spec.Audiences) != 0 || *request.Spec.ExpirationSeconds != 3600 || request.Spec.BoundObjectRef.Kind != "Pod" || request.Spec.BoundObjectRef.UID != f.pod.UID {
		t.Fatal("default audience/lifetime or Pod binding changed")
	}
	f.now = f.now.Add(47 * time.Minute)
	cached, err := f.cache.token(t.Context(), f.client, f.pod, source)
	if err != nil || cached != initial || len(f.requests) != 1 {
		t.Fatal("unexpired cache unnecessarily reissued")
	}
	f.now = f.now.Add(2 * time.Minute)
	rotated, err := f.cache.token(t.Context(), f.client, f.pod, source)
	if err != nil || rotated == initial || len(f.requests) != 2 {
		t.Fatal("token not rotated after 80% of lifetime")
	}
	f.cache.clear()
	if _, err := f.cache.token(t.Context(), f.client, f.pod, source); err != nil || len(f.requests) != 3 {
		t.Fatal("wake/recovery reused cached token")
	}
}
func TestProjectedTokenAudienceAndPodIdentitySeparateCaches(t *testing.T) {
	f := newTokenFixture(t)
	for _, audience := range []string{"sts.amazonaws.com", "pods.eks.amazonaws.com"} {
		if _, err := f.cache.token(t.Context(), f.client, f.pod, &corev1.ServiceAccountTokenProjection{Path: "token", Audience: audience}); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.requests) != 2 || f.requests[0].Spec.Audiences[0] == f.requests[1].Spec.Audiences[0] {
		t.Fatal("identity audiences shared token")
	}
	f.pod.UID = "replacement-pod"
	if _, err := f.cache.token(t.Context(), f.client, f.pod, &corev1.ServiceAccountTokenProjection{Path: "token", Audience: "sts.amazonaws.com"}); err != nil || len(f.requests) != 3 {
		t.Fatal("replacement Pod reused old UID token")
	}
}
func TestProjectedTokenRotatesAtTwentyFourHoursForLongTTL(t *testing.T) {
	f := newTokenFixture(t)
	source := &corev1.ServiceAccountTokenProjection{Path: "token", ExpirationSeconds: new(int64(172800))}
	if _, err := f.cache.token(t.Context(), f.client, f.pod, source); err != nil {
		t.Fatal(err)
	}
	f.now = f.now.Add(24 * time.Hour)
	if _, err := f.cache.token(t.Context(), f.client, f.pod, source); err != nil || len(f.requests) != 2 {
		t.Fatal("long-lived token not rotated after 24h")
	}
}
func TestProjectedTokenRejectsBadResponseWithoutCredentialLeak(t *testing.T) {
	for name, mutate := range map[string]func(*testing.T, *authenticationv1.TokenRequest){
		"missing audience": func(t *testing.T, r *authenticationv1.TokenRequest) { r.Spec.Audiences = nil },
		"wrong audience":   func(t *testing.T, r *authenticationv1.TokenRequest) { r.Spec.Audiences = []string{"other"} },
		"missing expiry":   func(t *testing.T, r *authenticationv1.TokenRequest) { r.Status.ExpirationTimestamp = metav1.Time{} },
		"expired": func(t *testing.T, r *authenticationv1.TokenRequest) {
			r.Status.ExpirationTimestamp = metav1.NewTime(time.Now().Add(-time.Second))
		},
		"wrong binding":   func(t *testing.T, r *authenticationv1.TokenRequest) { r.Spec.BoundObjectRef.UID = "other" },
		"missing binding": func(t *testing.T, r *authenticationv1.TokenRequest) { r.Spec.BoundObjectRef = nil },
		"wrong claim uid": func(t *testing.T, r *authenticationv1.TokenRequest) {
			mutateTokenClaims(t, r, func(c map[string]any) { c["kubernetes.io"].(map[string]any)["pod"].(map[string]any)["uid"] = "other" })
		},
		"missing claim audience": func(t *testing.T, r *authenticationv1.TokenRequest) {
			mutateTokenClaims(t, r, func(c map[string]any) { delete(c, "aud") })
		},
		"wrong claim subject": func(t *testing.T, r *authenticationv1.TokenRequest) {
			mutateTokenClaims(t, r, func(c map[string]any) { c["sub"] = "system:serviceaccount:other:account" })
		},
		"wrong account uid": func(t *testing.T, r *authenticationv1.TokenRequest) {
			mutateTokenClaims(t, r, func(c map[string]any) {
				c["kubernetes.io"].(map[string]any)["serviceaccount"].(map[string]any)["uid"] = "other"
			})
		},
		"malformed": func(t *testing.T, r *authenticationv1.TokenRequest) { r.Status.Token = "private-token-material" },
	} {
		t.Run(name, func(t *testing.T) {
			f := newTokenFixture(t)
			secret := ""
			f.mutate = func(r *authenticationv1.TokenRequest) { mutate(t, r); secret = r.Status.Token }
			_, err := f.cache.token(t.Context(), f.client, f.pod, &corev1.ServiceAccountTokenProjection{Path: "token", Audience: "sts.amazonaws.com"})
			if err == nil || strings.Contains(err.Error(), secret) || len(f.cache.entries) != 0 {
				t.Fatal("bad identity response accepted, cached or leaked")
			}
		})
	}
}
func TestProjectedTokenAcceptsLegacyResponseWithoutMetadataUID(t *testing.T) {
	f := newTokenFixture(t)
	f.mutate = func(r *authenticationv1.TokenRequest) { r.UID = "" }
	if _, err := f.cache.token(t.Context(), f.client, f.pod, &corev1.ServiceAccountTokenProjection{Path: "token"}); err != nil {
		t.Fatal(err)
	}
}
func TestProjectedTokenAPIFailureDoesNotExposeResponseBody(t *testing.T) {
	for _, failure := range []error{errors.New("private-token-material"),
		fmt.Errorf("wrapped private-token-material: %w", &apierrors.StatusError{ErrStatus: metav1.Status{Code: 403, Message: "private-token-material"}})} {
		f := newTokenFixture(t)
		f.failure = failure
		_, err := f.cache.token(t.Context(), f.client, f.pod, &corev1.ServiceAccountTokenProjection{Path: "token"})
		if err == nil || strings.Contains(err.Error(), "private-token-material") {
			t.Fatal("API diagnostics exposed credential material")
		}
	}
}
