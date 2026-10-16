//go:build linux

package guestfs

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestOwnershipMasksPreserveBitsAndDoNotFollowSymlinks(t *testing.T) {
	for _, readonly := range []bool{false, true} {
		root := t.TempDir()
		external := filepath.Join(t.TempDir(), "outside")
		file := filepath.Join(root, "file")
		sub := filepath.Join(root, "sub")
		if err := os.WriteFile(file, []byte("data"), 0401); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(external, []byte("outside"), 0400); err != nil {
			t.Fatal(err)
		}
		if err := os.Mkdir(sub, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(external, filepath.Join(sub, "link")); err != nil {
			t.Fatal(err)
		}
		if err := (Ownership{Group: int64(os.Getgid()), ReadOnly: readonly}).Apply(root); err != nil {
			t.Fatal(err)
		}
		fileMode, dirMode := os.FileMode(0661), os.FileMode(0770)|os.ModeSetgid
		if readonly {
			fileMode, dirMode = 0441, 0750|os.ModeSetgid
		}
		for name, want := range map[string]os.FileMode{file: fileMode, sub: dirMode, external: 0400} {
			info, err := os.Stat(name)
			if err != nil {
				t.Fatal(err)
			}
			if mode := info.Mode() & (os.ModePerm | os.ModeSetgid); mode != want {
				t.Errorf("%s got %v want %v", name, mode, want)
			}
			if name != external && info.Sys().(*syscall.Stat_t).Gid != uint32(os.Getgid()) {
				t.Fatal("gid mismatch")
			}
		}
	}
}

func TestOnRootMismatchSkipsOnlyMatchingRoot(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "data")
	if err := os.WriteFile(file, []byte("data"), 0400); err != nil {
		t.Fatal(err)
	}
	ownership := Ownership{Group: int64(os.Getgid()), OnRootMismatch: true}
	if err := ownership.Apply(root); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(file, 0400); err != nil {
		t.Fatal(err)
	}
	if err := ownership.Apply(root); err != nil {
		t.Fatal(err)
	}
	info, _ := os.Stat(file)
	if info.Mode().Perm() != 0400 {
		t.Fatal("matching root did not skip recursion")
	}
	if err := os.Chmod(root, 0750); err != nil {
		t.Fatal(err)
	}
	if err := ownership.Apply(root); err != nil {
		t.Fatal(err)
	}
	info, _ = os.Stat(file)
	if info.Mode().Perm() != 0660 {
		t.Fatal("mismatched root did not recurse")
	}
}
