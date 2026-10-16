// Built only by the native qualification fixture. Credentials never leave this process.
package main

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const accountPath = "/var/run/secrets/kubernetes.io/serviceaccount/"

func main() {
	marker := rand.Text()
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		result, err := identity(r, marker)
		if err != nil {
			http.Error(w, "identity qualification failed: "+err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(result)
	})
	server := &http.Server{Addr: ":8080", Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	if err := server.ListenAndServe(); err != nil {
		panic(err)
	}
}

func identity(r *http.Request, marker string) (map[string]any, error) {
	token, err := os.ReadFile(accountPath + "token")
	if err != nil {
		return nil, fmt.Errorf("read token file")
	}
	namespace, err := os.ReadFile(accountPath + "namespace")
	if err != nil {
		return nil, fmt.Errorf("read namespace file")
	}
	ca, err := os.ReadFile(accountPath + "ca.crt")
	if err != nil {
		return nil, fmt.Errorf("read CA file")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("invalid CA certificate")
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	request := func(path string, bearer []byte) (int, []byte, error) {
		request, err := http.NewRequestWithContext(r.Context(), http.MethodGet, "https://kubernetes.default.svc"+path, nil)
		if err != nil {
			return 0, nil, fmt.Errorf("construct Kubernetes request")
		}
		request.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(bearer)))
		response, err := client.Do(request)
		if err != nil {
			return 0, nil, fmt.Errorf("Kubernetes TLS request failed")
		}
		defer response.Body.Close()
		data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		return response.StatusCode, data, err
	}
	base := "/api/v1/namespaces/" + strings.TrimSpace(string(namespace))
	code, data, err := request(base+"/configmaps/native-identity-config", token)
	if err != nil || code != http.StatusOK {
		return nil, fmt.Errorf("allowed ConfigMap request: HTTP %d", code)
	}
	var config struct {
		Data map[string]string `json:"data"`
	}
	if json.Unmarshal(data, &config) != nil || config.Data["value"] != "authorized" {
		return nil, fmt.Errorf("unexpected ConfigMap data")
	}
	code, data, err = request(base+"/pods/native-identity", token)
	if err != nil || code != http.StatusOK {
		return nil, fmt.Errorf("allowed Pod request: HTTP %d", code)
	}
	var pod struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if json.Unmarshal(data, &pod) != nil || pod.Metadata.UID == "" {
		return nil, fmt.Errorf("invalid Pod identity")
	}
	denied, _, err := request(base+"/secrets/not-authorized", token)
	if err != nil || denied != http.StatusForbidden {
		return nil, fmt.Errorf("denied Secret request: HTTP %d", denied)
	}
	other, err := os.ReadFile("/audience/token")
	if err != nil {
		return nil, fmt.Errorf("read audience projection")
	}
	rejected, _, err := request(base+"/configmaps/native-identity-config", other)
	if err != nil || rejected != http.StatusUnauthorized {
		return nil, fmt.Errorf("wrong audience request: HTTP %d", rejected)
	}
	label, err := os.ReadFile("/metadata/label")
	if err != nil {
		return nil, fmt.Errorf("read metadata projection")
	}
	digest := sha256.Sum256(token)
	return map[string]any{"uid": pod.Metadata.UID, "marker": marker, "tokenHash": hex.EncodeToString(digest[:]),
		"label": string(label), "authorized": code, "denied": denied, "wrongAudience": rejected}, nil
}
