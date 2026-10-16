package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func privateDirectory(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("", "fvp-relay-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(directory) })
	return directory
}

func TestUnixControlClientAndPrivateSocket(t *testing.T) {
	directory := privateDirectory(t)
	socket := filepath.Join(directory, "control.sock")
	tokenFile := filepath.Join(directory, "token")
	if err := os.WriteFile(tokenFile, []byte(testToken), 0600); err != nil {
		t.Fatal(err)
	}
	listener, err := controlListener(socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	info, err := os.Stat(socket)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("socket must be private: %v %v", info, err)
	}
	other, err := controlListener(socket)
	if err == nil {
		other.Close()
		t.Fatal("must not replace an existing socket")
	}
	r := fixture(t, 2, 2)
	server := &http.Server{Handler: r.handler()}
	t.Cleanup(func() { server.Close() })
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	call := func(method, path, body string) (string, error) {
		var output bytes.Buffer
		err := run([]string{"client", "--socket", socket, "--token-file", tokenFile,
			"--method", method, "--path", path}, strings.NewReader(body), &output)
		return output.String(), err
	}
	health, err := call("GET", "/health", "")
	if err != nil || !strings.Contains(health, `"owner":"owned-relay"`) {
		t.Fatalf("client health: %s %v", health, err)
	}
	target := tcpListener(t)
	body, err := json.Marshal(binding{availableAddress(t), target.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	if response, err := call("PUT", "/listeners/pod:8080", string(body)); err != nil || !strings.Contains(response, `"id":"pod:8080"`) {
		t.Fatalf("client PUT: %s %v", response, err)
	}
	if response, err := call("GET", "/listeners", ""); err != nil || !strings.Contains(response, `"id":"pod:8080"`) {
		t.Fatalf("client GET: %s %v", response, err)
	}
	if response, err := call("DELETE", "/listeners/pod:8080", ""); err != nil || !strings.Contains(response, `"deleted":true`) {
		t.Fatalf("client DELETE: %s %v", response, err)
	}
	if err := os.WriteFile(tokenFile, []byte(strings.Repeat("wrong", 10)), 0600); err != nil {
		t.Fatal(err)
	}
	if response, err := call("GET", "/health", ""); err == nil || !strings.Contains(response, "unauthorized") {
		t.Fatalf("unauthorized client must fail: %s %v", response, err)
	}
	server.Close()
	if err := <-finished; !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

func TestRejectsPublicTokenFilesAndSymlinks(t *testing.T) {
	directory := privateDirectory(t)
	file := filepath.Join(directory, "token")
	if err := os.WriteFile(file, []byte(testToken), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := readToken(file); err == nil {
		t.Fatal("public token file accepted")
	}
	if err := os.Chmod(file, 0600); err != nil {
		t.Fatal(err)
	}
	if actual, err := readToken(file); err != nil || actual != testToken {
		t.Fatalf("private token: %q %v", actual, err)
	}
	symlink := filepath.Join(directory, "token-link")
	if err := os.Symlink(file, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := readToken(symlink); err == nil {
		t.Fatal("symlink token file accepted")
	}
	if err := os.Chmod(directory, 0755); err != nil {
		t.Fatal(err)
	}
	if listener, err := controlListener(filepath.Join(directory, "control.sock")); err == nil {
		listener.Close()
		t.Fatal("public socket directory accepted")
	}
}

func TestInvalidCLIArgumentsFailBeforeStarting(t *testing.T) {
	for _, args := range [][]string{
		nil, {"unknown"}, {"serve"}, {"serve", "--socket", "/tmp/test", "--owner", "valid", "--listen-cidr", "0.0.0.0/0"},
		{"client", "--socket", "relative"}, {"client", "--socket", "/tmp/test", "--method", "PATCH"},
		{"client", "--socket", "/tmp/test", "--path", "/health?token=value"},
	} {
		if err := run(args, strings.NewReader(""), &bytes.Buffer{}); err == nil {
			t.Errorf("accepted arguments: %v", args)
		}
	}
}

func TestShutdownClosesSessionsAndExitsDaemon(t *testing.T) {
	directory := privateDirectory(t)
	socket := filepath.Join(directory, "control.sock")
	tokenFile := filepath.Join(directory, "token")
	if err := os.WriteFile(tokenFile, []byte(testToken), 0600); err != nil {
		t.Fatal(err)
	}
	args := []string{"serve", "--socket", socket, "--token-file", tokenFile, "--owner", "owned-process",
		"--listen-cidr", "127.0.0.0/8", "--target-host", "127.0.0.1"}
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestRelayDaemonProcess$")
	command.Env = append(os.Environ(), "FLOCI_RELAY_DAEMON_TEST="+string(encoded))
	var diagnostics bytes.Buffer
	command.Stdout = &diagnostics
	command.Stderr = &diagnostics
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- command.Wait() }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		default:
			command.Process.Kill()
		}
	})
	call := func(method, path, body string) (string, error) {
		var output bytes.Buffer
		err := run([]string{"client", "--socket", socket, "--token-file", tokenFile,
			"--method", method, "--path", path}, strings.NewReader(body), &output)
		return output.String(), err
	}
	for deadline := time.Now().Add(5 * time.Second); ; {
		if _, err := call("GET", "/health", ""); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("daemon did not become healthy")
		}
		time.Sleep(10 * time.Millisecond)
	}
	target := tcpListener(t)
	address := availableAddress(t)
	body, err := json.Marshal(binding{address, target.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	if response, err := call("PUT", "/listeners/pod:8080", string(body)); err != nil {
		t.Fatalf("PUT: %s %v", response, err)
	}
	client := connect(t, address)
	target.SetDeadline(time.Now().Add(3 * time.Second))
	upstream, err := target.AcceptTCP()
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	upstream.SetDeadline(time.Now().Add(3 * time.Second))
	upstream.Write([]byte("ready"))
	var ready [5]byte
	if _, err := io.ReadFull(client, ready[:]); err != nil {
		t.Fatal(err)
	}
	if response, err := call("POST", "/shutdown", ""); err != nil || !strings.Contains(response, `"shutdown":true`) {
		t.Fatalf("shutdown: %s %v", response, err)
	}
	if count, err := client.Read(ready[:]); count != 0 || err == nil {
		t.Fatalf("shutdown left client open: %d %v", count, err)
	}
	if count, err := upstream.Read(ready[:]); count != 0 || err == nil {
		t.Fatalf("shutdown left upstream open: %d %v", count, err)
	}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatalf("daemon exit: %v: %s", err, &diagnostics)
		}
	case <-ctx.Done():
		t.Fatal("daemon did not exit after shutdown")
	}
	if _, err := os.Lstat(socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shutdown left control socket behind: %v", err)
	}
}

func TestRelayDaemonProcess(t *testing.T) {
	encoded := os.Getenv("FLOCI_RELAY_DAEMON_TEST")
	if encoded == "" {
		return
	}
	var args []string
	if err := json.Unmarshal([]byte(encoded), &args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := run(args, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}
