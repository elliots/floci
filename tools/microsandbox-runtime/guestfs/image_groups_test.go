package guestfs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestImageMembershipAndExecUseTheSameResolvedIdentity(t *testing.T) {
	for _, strict := range []bool{false, true} {
		bundle := t.TempDir()
		etc := filepath.Join(bundle, "rootfs", "etc")
		if err := os.MkdirAll(etc, 0700); err != nil {
			t.Fatal(err)
		}
		for name, data := range map[string]string{"passwd": "app:x:1000:1000:app:/home/app:/bin/sh\n", "group": "app:x:1000:\nimplicit:x:50000:other,app\nunused:x:50001:other\n"} {
			if err := os.WriteFile(filepath.Join(etc, name), []byte(data), 0600); err != nil {
				t.Fatal(err)
			}
		}
		config := filepath.Join(bundle, "config.json")
		process := filepath.Join(bundle, "exec.json")
		if err := os.WriteFile(config, []byte(`{"root":{"path":"rootfs"},"process":{"user":{"uid":1000,"gid":2000}}}`), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(process, []byte(`{"user":{"uid":1000,"gid":2000}}`), 0600); err != nil {
			t.Fatal(err)
		}
		groups := Groups{IDs: []uint32{3000, 4000}, Strict: strict, StateFile: filepath.Join(bundle, "identity.json")}
		if err := groups.PrepareRunc([]string{"create", "--bundle", bundle, "task"}); err != nil {
			t.Fatal(err)
		}
		// Membership is frozen with create; application edits cannot grant exec
		// a new supplementary identity or remove its declared volume access.
		if err := os.WriteFile(filepath.Join(etc, "group"), []byte("changed:x:50002:app\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if err := groups.PrepareRunc([]string{"exec", "-p", process, "task"}); err != nil {
			t.Fatal(err)
		}
		want := []uint32{2000, 3000, 4000}
		if !strict {
			want = append(want, 50000)
		}
		for _, name := range []string{config, process} {
			data, _ := os.ReadFile(name)
			var doc map[string]json.RawMessage
			if err := json.Unmarshal(data, &doc); err != nil {
				t.Fatal(err)
			}
			if name == config {
				if err := json.Unmarshal(doc["process"], &doc); err != nil {
					t.Fatal(err)
				}
			}
			var user processUser
			if err := json.Unmarshal(doc["user"], &user); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(user.Groups, want) {
				t.Fatalf("strict=%v %s: groups %v want %v", strict, name, user.Groups, want)
			}
		}
	}
}

func TestImageGroupLookupDoesNotFollowEscapingSymlink(t *testing.T) {
	bundle := t.TempDir()
	root := filepath.Join(bundle, "rootfs")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "etc")); err != nil {
		t.Fatal(err)
	}
	if _, err := (Groups{}).withImageGroups([]byte(`{"root":{"path":"rootfs"},"process":{"user":{"uid":1000,"gid":2000}}}`), bundle); err == nil {
		t.Fatal("image database escaped rootfs")
	}
}
