package microsandboxruntime

import (
	"context"
	"io"
	"reflect"
	"slices"
	"testing"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

func TestInitHandoffPreservesArgumentsAndEnvironment(t *testing.T) {
	args := []string{"run", "-d", "--name", "owned", "--init", "/bin/sh", "--init-arg", "-c",
		"--init-arg", "until test -f /ready; do sleep 0.1; done; exec /sbin/init",
		"--init-env", "container=microsandbox", "--init-env", "LITERAL=value=two", "image:1", "--", "sleep", "infinity"}
	request, err := parse(args)
	want := &msb.InitConfig{Cmd: "/bin/sh", Args: []string{"-c", "until test -f /ready; do sleep 0.1; done; exec /sbin/init"},
		Env: map[string]string{"container": "microsandbox", "LITERAL": "value=two"}}
	if err != nil || !reflect.DeepEqual(request.config.Init, want) {
		t.Fatalf("init config: %#v, %v", request.config.Init, err)
	}
	client := &Client{core: fakeBackend(func(context.Context, command, io.Reader, io.Writer, io.Writer) (int, error) {
		t.Fatal("detached init guest must retain snapshot-compatible launch")
		return 0, nil
	})}
	client.cli = func(_ context.Context, got []string, _, _ io.Writer) (int, error) {
		if !slices.Equal(args, got) {
			t.Fatalf("init arguments changed: %v", got)
		}
		return 0, nil
	}
	if _, err := client.Execute(t.Context(), args, nil); err != nil {
		t.Fatal(err)
	}
}

func TestInitRejectsInvalidPathsAndIncompleteConfiguration(t *testing.T) {
	for _, options := range [][]string{
		{"--init", "relative"}, {"--init", "/bin/../sbin/init"}, {"--init", "/sbin/init", "--init", "auto"},
		{"--init-arg", "-c"}, {"--init-env", "KEY=value"}, {"--init", "auto", "--init-env", "=value"},
		{"--init", "auto", "--init-env", "KEY"}, {"--init", "auto", "--init-env", "BAD KEY=value"},
	} {
		args := append([]string{"run", "-d", "--name", "owned"}, options...)
		args = append(args, "image:1")
		if _, err := parse(args); err == nil {
			t.Fatalf("invalid init accepted: %v", options)
		}
	}
	if _, err := parse([]string{"run", "-d", "--name", "owned", "--init", "auto", "image:1"}); err != nil {
		t.Fatal(err)
	}
}

func TestNetworkBoundaryPreservesFlagLikeInitArguments(t *testing.T) {
	args := []string{"run", "-d", "--name", "owned", "--init", "/bin/sh", "--init-arg", "--net",
		"--init-arg", "--env", "--init-arg", "--", "--init-env", "KEY=--net-default", "--net", "all", "image:1", "--", "tail", "-f", "/dev/null"}
	request, err := parse(args)
	if err != nil {
		t.Fatal(err)
	}
	boundary := boundaryFixture()
	network, err := boundary.restrict(request.config.Network)
	if err != nil {
		t.Fatal(err)
	}
	got, err := boundary.launchArguments(args, network, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	index := slices.Index(got, "--init")
	want := []string{"--init", "/bin/sh", "--init-arg", "--net", "--init-arg", "--env", "--init-arg", "--", "--init-env", "KEY=--net-default", "image:1", "--", "tail", "-f", "/dev/null"}
	if index < 0 || !slices.Equal(got[index:], want) {
		t.Fatalf("network boundary changed init payload: %v", got)
	}
}
