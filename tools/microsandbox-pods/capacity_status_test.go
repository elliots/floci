package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	msbruntime "github.com/floci-io/floci/tools/microsandbox-runtime"
)

type capacityProbe struct {
	nativeRuntime
	err   error
	calls int
}

func (p *capacityProbe) Capacity(context.Context) (msbruntime.CapacityStatus, error) {
	p.calls++
	return msbruntime.CapacityStatus{Limits: msbruntime.CapacityLimits{MemoryMiB: 2048, DiskMiB: 8192},
		MemoryMiB: 768, Reservations: []string{"pending-owned-wake"},
		Disk: &msbruntime.DiskCapacityStatus{ReservedBytes: 1024}}, p.err
}

func TestCapacityDiagnosticsRequireAuthenticationAndDoNotHideUnknownUsage(t *testing.T) {
	token := strings.Repeat("a", 32)
	file := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(file, []byte(token), 0600); err != nil {
		t.Fatal(err)
	}
	port, err := availableTCPPort()
	if err != nil {
		t.Fatal(err)
	}
	runtime := &capacityProbe{}
	p := &provider{runner: commandRunner{runtime: runtime}}
	server, err := p.serveControl(fmt.Sprintf("127.0.0.1:%d", port), file)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { server.Close() })
	for _, scenario := range []struct {
		auth, method  string
		failure       error
		status, calls int
	}{
		{"", http.MethodGet, nil, 401, 0},
		{"Bearer " + token, http.MethodPost, nil, 405, 0},
		{"Bearer " + token, http.MethodGet, nil, 200, 1},
		{"Bearer " + token, http.MethodGet, errors.New("private-runtime-path"), 503, 2},
	} {
		runtime.err = scenario.failure
		request, err := http.NewRequestWithContext(t.Context(), scenario.method, fmt.Sprintf("http://127.0.0.1:%d/v1/capacity", port), nil)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", scenario.auth)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != scenario.status || runtime.calls != scenario.calls {
			t.Fatalf("status=%d calls=%d, want %d/%d: %s", response.StatusCode, runtime.calls, scenario.status, scenario.calls, data)
		}
		if strings.Contains(string(data), "private-runtime-path") {
			t.Fatal("private runtime error escaped")
		}
		if response.StatusCode == http.StatusOK {
			var status msbruntime.CapacityStatus
			if err := json.Unmarshal(data, &status); err != nil {
				t.Fatal(err)
			}
			if status.MemoryMiB != 768 || status.Limits.DiskMiB != 8192 || status.Disk.ReservedBytes != 1024 || len(status.Reservations) != 1 {
				t.Fatalf("shared accounting lost: %+v", status)
			}
		}
	}
}
