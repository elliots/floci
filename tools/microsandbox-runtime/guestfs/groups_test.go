package guestfs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestGroupsApplyToCreateAndExecWithoutChangingOCIFields(t *testing.T) {
	for _, strict := range []bool{false, true} {
		for _, bundle := range []bool{false, true} {
			process := map[string]any{"args": []any{"/bin/app", "literal"}, "user": map[string]any{"uid": float64(1000), "gid": float64(2000), "additionalGids": []any{float64(50000), float64(2000)}}, "unknown": "preserved"}
			document := process
			if bundle {
				document = map[string]any{"process": process, "root": map[string]any{"path": "rootfs"}, "annotations": map[string]any{"other": "value"}}
			}
			input, _ := json.Marshal(document)
			output, err := (Groups{IDs: []uint32{3000, 4000, 3000}, Strict: strict}).Rewrite(input, bundle)
			if err != nil {
				t.Fatal(err)
			}
			var actual map[string]any
			if err := json.Unmarshal(output, &actual); err != nil {
				t.Fatal(err)
			}
			groups := []any{float64(2000), float64(3000), float64(4000)}
			if !strict {
				groups = append(groups, float64(50000))
			}
			process["user"].(map[string]any)["additionalGids"] = groups
			if !reflect.DeepEqual(actual, document) {
				t.Fatalf("strict=%v bundle=%v\ngot %s\nwant %#v", strict, bundle, output, document)
			}
		}
	}
}

func TestRuncPatchesBothInputFilesAndPassesLifecycleCommands(t *testing.T) {
	root := t.TempDir()
	config := filepath.Join(root, "config.json")
	process := filepath.Join(root, "exec.json")
	if err := os.WriteFile(config, []byte(`{"process":{"user":{"uid":1000,"gid":2000}}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(process, []byte(`{"user":{"uid":1000,"gid":2000}}`), 0600); err != nil {
		t.Fatal(err)
	}
	groups := Groups{IDs: []uint32{3000}, Strict: true}
	for _, args := range [][]string{
		{"--root", "/run/containerd/runc", "--log", "create", "--log-format", "json", "create", "--bundle", root, "task"},
		{"--root=/run/containerd/runc", "exec", "--detach", "--process=" + process, "task"},
		{"--rootless=false", "state", "task"},
		{"kill", "task", "SIGTERM"},
	} {
		if err := groups.PrepareRunc(args); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	for _, name := range []string{config, process} {
		data, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var doc map[string]any
		if err := json.Unmarshal(data, &doc); err != nil {
			t.Fatal(err)
		}
		if name == config {
			doc = doc["process"].(map[string]any)
		}
		if !reflect.DeepEqual(doc["user"].(map[string]any)["additionalGids"], []any{float64(2000), float64(3000)}) {
			t.Fatalf("groups missing: %s", data)
		}
	}
	if err := groups.PrepareRunc([]string{"exec", "task", "id"}); err == nil {
		t.Fatal("uncontrolled exec accepted")
	}
	if err := groups.PrepareRunc([]string{"--unknown", "create"}); err == nil {
		t.Fatal("unknown global argument accepted")
	}
}

func TestInvalidGroupsAndOCIUserFailClosed(t *testing.T) {
	for _, value := range []string{"-1", "2147483648", "1,,2", "root"} {
		if _, err := ParseGroups(value, "Merge"); err == nil {
			t.Fatalf("accepted %q", value)
		}
	}
	if _, err := ParseGroups("1", "Unknown"); err == nil {
		t.Fatal("unknown policy accepted")
	}
	for _, document := range []string{`{}`, `{"user":null}`, `{"user":{"gid":-1}}`, `{"user":{"gid":0,"additionalGids":["root"]}}`} {
		if _, err := (Groups{}).Rewrite([]byte(document), false); err == nil {
			t.Fatalf("accepted %s", document)
		}
	}
}
