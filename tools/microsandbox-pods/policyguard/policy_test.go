package policyguard

import "testing"

func TestApplicationPathCannotEscapeOwnedCgroups(t *testing.T) {
	for _, name := range []string{"", ".", "..", "/sys/fs/cgroup", "microsandbox-workload", "floci-application-g1-../node", "floci-application-g1-a/application", "floci-application-g1-a\x00"} {
		if validateApplication(name) == nil {
			t.Fatalf("accepted foreign cgroup %q", name)
		}
	}
	if err := validateApplication("floci-application-g12-abc123"); err != nil {
		t.Fatal(err)
	}
}
