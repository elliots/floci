package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestPeerHostIdentityPinsDirectControllerRestarts(t *testing.T) {
	n := &network{runner: commandRunner{cfg: config{state: t.TempDir()}}, hostIP: "192.0.2.1"}
	if err := n.pinHostIdentity(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := n.pinHostIdentity(t.Context()); err != nil {
		t.Fatal("same host was not reusable", err)
	}
	pod := validPod()
	record := journalRecord{Version: 6, Pod: pod, View: lifecycleView{UID: string(pod.UID), VM: "fvp-" + stringsWithoutHyphens(string(pod.UID))[:20], UDPPort: 40001}}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	journal := filepath.Join(n.runner.cfg.state, string(pod.UID)+".json")
	if err := os.WriteFile(journal, data, 0600); err != nil {
		t.Fatal(err)
	}
	n.hostIP = "192.0.2.2"
	if err := n.pinHostIdentity(t.Context()); err == nil {
		t.Fatal("changed host adopted existing Pod state")
	}
	if err := os.Remove(journal); err != nil {
		t.Fatal(err)
	}
	if err := n.pinHostIdentity(t.Context()); err != nil {
		t.Fatal("empty state could not move host", err)
	}
}

func TestLegacyPeerHostIdentityRequiresOwnedDestinationAndTuple(t *testing.T) {
	record := journalRecord{GuestIP: "172.16.0.6", Slot: 3, View: lifecycleView{UID: "owned", UDPPort: 40001}}
	legacy := "-A OUTPUT -d 172.16.0.6/32 -p udp -m udp --dport 8472 -j DNAT --to-destination 192.0.2.1:40001"
	current := "-A OUTPUT -s 192.0.2.2/32 -d 172.16.0.6/32 -p udp -m udp --sport 20003 --dport 8472 -j DNAT --to-destination 192.0.2.1:40001"
	for _, rule := range []string{legacy, current, legacy + "\n" + current} {
		if err := proveLegacyPeerHost(rule, "192.0.2.1", "192.0.2.2", record); err != nil {
			t.Fatal(err)
		}
	}
	for _, rules := range []string{"", strings.Replace(legacy, "40001", "40002", 1), strings.Replace(current, "20003", "20004", 1), strings.Replace(current, "192.0.2.2", "192.0.2.3", 1), strings.Replace(current, "-s ", "! -s ", 1), legacy + "\n" + strings.Replace(legacy, "192.0.2.1", "192.0.2.99", 1)} {
		if err := proveLegacyPeerHost(rules, "192.0.2.1", "192.0.2.2", record); err == nil {
			t.Fatal("unproven legacy destination accepted", rules)
		}
	}
}

func TestPeerHostIdentityRejectsSymlinksAndAdoptsOnlyNetworklessColdState(t *testing.T) {
	n := &network{runner: commandRunner{cfg: config{state: t.TempDir()}}, hostIP: "192.0.2.1"}
	path := filepath.Join(n.runner.cfg.state, "network-host")
	other := filepath.Join(t.TempDir(), "unowned")
	if err := os.WriteFile(other, []byte(`{"version":1,"nodeIP":"192.0.2.1"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(other, path); err != nil {
		t.Fatal(err)
	}
	if err := n.pinHostIdentity(t.Context()); err == nil {
		t.Fatal("symlink host identity accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	pod := validPod()
	record := journalRecord{Version: 5, Pod: pod, View: lifecycleView{UID: string(pod.UID), VM: "fvp-" + stringsWithoutHyphens(string(pod.UID))[:20], UDPPort: 40001}}
	data, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(n.runner.cfg.state, string(pod.UID)+".json"), data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := n.pinHostIdentity(t.Context()); err != nil {
		t.Fatal("cold state without any guest route failed adoption", err)
	}
}

func TestPersistentPodJournalHostAdoptionStillRequiresItsOwnedRoute(t *testing.T) {
	binaries := t.TempDir()
	script := "#!/bin/sh\n[ \"$1\" = exec ] && [ \"$2\" = owned-peer ] || exit 1\nprintf '%s\\n' \"$FLOCI_TEST_NETWORK_RULES\"\n"
	if err := os.WriteFile(filepath.Join(binaries, "docker"), []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binaries+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, scenario := range []struct {
		name        string
		version     int
		destination string
		accepted    bool
	}{
		{"persistentJournal", 7, "192.0.2.1", true},
		{"lifecycleJournal", 8, "192.0.2.1", true},
		{"policyJournal", 9, "192.0.2.1", true},
		{"changedHost", 7, "192.0.2.99", false},
		{"futureJournal", 10, "192.0.2.1", false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			n := &network{runner: commandRunner{cfg: config{state: t.TempDir(), peer: "owned-peer"}}, hostIP: "192.0.2.1", peerIP: "192.0.2.2"}
			pod := validPod()
			pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data"}}}}
			record := journalRecord{Version: scenario.version, Pod: pod, GuestIP: "172.16.0.6", Slot: 3,
				View: lifecycleView{UID: string(pod.UID), VM: "fvp-" + stringsWithoutHyphens(string(pod.UID))[:20], UDPPort: 40001}}
			data, err := json.Marshal(record)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(n.runner.cfg.state, string(pod.UID)+".json"), data, 0600); err != nil {
				t.Fatal(err)
			}
			t.Setenv("FLOCI_TEST_NETWORK_RULES", "-A OUTPUT -s 192.0.2.2/32 -d 172.16.0.6/32 -p udp -m udp --sport 20003 --dport 8472 -j DNAT --to-destination "+scenario.destination+":40001")
			err = n.pinHostIdentity(t.Context())
			if (err == nil) != scenario.accepted {
				t.Fatalf("host adoption accepted=%t: %v", scenario.accepted, err)
			}
			_, statErr := os.Stat(filepath.Join(n.runner.cfg.state, "network-host"))
			if scenario.accepted && statErr != nil || !scenario.accepted && !os.IsNotExist(statErr) {
				t.Fatalf("host identity must only be persisted after successful proof: %v", statErr)
			}
		})
	}
}
