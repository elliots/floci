package microsandboxruntime

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func publicCAFixture(t *testing.T) (*nativeNetworkBoundary, []byte, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1)}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	source := filepath.Join(t.TempDir(), "public-ca.pem")
	if err := os.WriteFile(source, certificate, 0600); err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(t.TempDir(), "network.yaml")
	document := "network:\n  isolation: {enabled: true}\n  native:\n    dns-nameservers: ['127.0.0.1:5300']\n    ca-file: '" + source + "'\n"
	if err := os.WriteFile(config, []byte(document), 0600); err != nil {
		t.Fatal(err)
	}
	boundary, err := readNetworkBoundary(config)
	if err != nil {
		t.Fatal(err)
	}
	return boundary, certificate, source
}

func TestBoundaryLaunchCopiesFrozenPublicCAAfterSourceReplacement(t *testing.T) {
	boundary, certificate, source := publicCAFixture(t)
	if err := os.WriteFile(source, []byte("PRIVATE KEY MUST NEVER REACH THE GUEST"), 0600); err != nil {
		t.Fatal(err)
	}
	home := t.TempDir()
	client := &Client{config: Config{Home: home}, boundary: boundary, core: fakeBackend(func(context.Context, command, io.Reader, io.Writer, io.Writer) (int, error) {
		t.Fatal("launch must use the CLI fallback")
		return 0, nil
	})}
	launches := 0
	client.cli = func(_ context.Context, arguments []string, _, _ io.Writer) (int, error) {
		launches++
		for index, value := range arguments {
			if value != "--copy-file" {
				continue
			}
			copied, ok := strings.CutSuffix(arguments[index+1], ":"+networkCAPath)
			if !ok || copied == source || filepath.Dir(copied) != filepath.Join(home, "floci-network-cas") {
				t.Fatalf("unsafe CA source %q", arguments[index+1])
			}
			content, err := os.ReadFile(copied)
			if err != nil || !bytes.Equal(content, certificate) {
				t.Fatalf("copy was not validated public data: %v", err)
			}
			info, err := os.Stat(copied)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatal("CA copy is not private")
			}
			return 0, nil
		}
		t.Fatal("public CA copy missing")
		return 0, nil
	}
	for range 2 {
		if _, err := client.Execute(t.Context(), []string{"run", "-d", "--name", "owned", "alpine"}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if launches != 2 {
		t.Fatalf("launch count %d", launches)
	}
	if err := os.Remove(source); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Execute(t.Context(), []string{"run", "-d", "--name", "owned", "alpine"}, nil); err != nil {
		t.Fatal(err)
	}
}

func TestBoundaryPublicCACacheRejectsTamperingAndSymlinks(t *testing.T) {
	for _, change := range []string{"content", "permissions", "symlink"} {
		t.Run(change, func(t *testing.T) {
			boundary, _, source := publicCAFixture(t)
			home := t.TempDir()
			path, err := boundary.freezeCA(home)
			if err != nil {
				t.Fatal(err)
			}
			switch change {
			case "content":
				err = os.WriteFile(path, []byte("different"), 0600)
			case "permissions":
				err = os.Chmod(path, 0644)
			case "symlink":
				if err = os.Remove(path); err == nil {
					err = os.Symlink(source, path)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := boundary.freezeCA(home); err == nil {
				t.Fatalf("accepted %s change", change)
			}
		})
	}
}

func TestBoundaryPublicCACacheRejectsSharedDirectory(t *testing.T) {
	boundary, _, _ := publicCAFixture(t)
	for _, symlink := range []bool{false, true} {
		home := t.TempDir()
		path := filepath.Join(home, "floci-network-cas")
		var err error
		if symlink {
			err = os.Symlink(t.TempDir(), path)
		} else {
			err = os.Mkdir(path, 0755)
		}
		if err != nil {
			t.Fatal(err)
		}
		if _, err := boundary.freezeCA(home); err == nil {
			t.Fatal("accepted untrusted CA directory")
		}
	}
}

func TestBoundaryPublicCACachePublishesAtomicallyAcrossLaunches(t *testing.T) {
	boundary, certificate, _ := publicCAFixture(t)
	home := t.TempDir()
	failures := make(chan error, 12)
	var workers sync.WaitGroup
	for range 12 {
		workers.Go(func() {
			path, err := boundary.freezeCA(home)
			if err == nil {
				var content []byte
				content, err = os.ReadFile(path)
				if err == nil && !bytes.Equal(content, certificate) {
					t.Error("partially published public CA")
				}
			}
			failures <- err
		})
	}
	workers.Wait()
	close(failures)
	for err := range failures {
		if err != nil {
			t.Error(err)
		}
	}
}

func TestBoundaryRejectsReservedCAEnvironmentBeforeLaunch(t *testing.T) {
	for _, flag := range []string{"--env", "-e"} {
		for _, value := range []string{"FLOCI_CA_BUNDLE=/override", "FLOCI_CA_BUNDLE=" + networkCAPath} {
			t.Run(flag+"/"+value, func(t *testing.T) {
				boundary, _, _ := publicCAFixture(t)
				client := &Client{config: Config{Home: t.TempDir()}, boundary: boundary}
				client.cli = func(context.Context, []string, io.Writer, io.Writer) (int, error) {
					t.Fatal("reserved CA variable reached launch")
					return 0, nil
				}
				_, err := client.Execute(t.Context(), []string{"run", "-d", "--name", "owned", flag, value, "alpine"}, nil)
				if !errors.Is(err, ErrInvalidArgument) {
					t.Fatalf("expected reserved variable rejection, got %v", err)
				}
			})
		}
	}
}

func TestBoundaryPreservesCAEnvironmentWithoutConfiguredCAAndLiteralGuestArguments(t *testing.T) {
	for _, configuredCA := range []bool{false, true} {
		boundary := boundaryFixture()
		arguments := []string{"run", "-d", "--name", "owned", "-e", "FLOCI_CA_BUNDLE=/application-ca", "alpine"}
		if configuredCA {
			boundary, _, _ = publicCAFixture(t)
			arguments = []string{"run", "-d", "--name", "owned", "--env", "APPLICATION_CA=/application-ca", "alpine", "--", "echo", "--env", "FLOCI_CA_BUNDLE=literal"}
		}
		client := &Client{config: Config{Home: t.TempDir()}, boundary: boundary, core: fakeBackend(func(context.Context, command, io.Reader, io.Writer, io.Writer) (int, error) {
			t.Fatal("launch must use the CLI fallback")
			return 0, nil
		})}
		launched := false
		client.cli = func(_ context.Context, args []string, _, _ io.Writer) (int, error) {
			launched = true
			if !strings.Contains(strings.Join(args, " "), arguments[len(arguments)-1]) {
				t.Fatal("guest literal was rewritten")
			}
			return 0, nil
		}
		if _, err := client.Execute(t.Context(), arguments, nil); err != nil {
			t.Fatal(err)
		}
		if !launched {
			t.Fatal("valid launch was rejected")
		}
	}
}
