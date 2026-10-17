//go:build linux

package guestfs

import (
	"errors"
	"slices"
	"testing"

	"golang.org/x/sys/unix"
)

func TestWritebackRejectsRootSubdirectoriesAliasesAndWrongFilesystem(t *testing.T) {
	mounts, err := parseWritebackMounts("20 0 8:1 / / rw - ext4 /dev/vda rw\n30 20 8:2 / /persistent rw - ext4 /dev/vdb rw\n40 20 8:2 / /alias rw - ext4 /dev/vdb rw\n50 20 8:3 / /tmpfs rw - tmpfs tmpfs rw\n60 20 8:4 /sub /subtree rw - ext4 /dev/vdc rw\n")
	if err != nil {
		t.Fatal(err)
	}
	if err := validateWritebackMount(mounts, "/persistent", "8:2"); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/", "/persistent/subdir", "/alias", "/tmpfs", "/subtree"} {
		if validateWritebackMount(mounts, path, map[string]string{"/": "8:1", "/alias": "8:2", "/tmpfs": "8:3", "/subtree": "8:4"}[path]) == nil {
			t.Fatal("unsafe filesystem accepted", path)
		}
	}
	if validateWritebackMount(mounts, "/persistent", "8:99") == nil {
		t.Fatal("replaced fd accepted")
	}
}

func TestWritebackErrorsNeverAuthorizeSeal(t *testing.T) {
	var sealed []int
	err := writebackFiles([]int{1, 2}, true, func(fd int) error {
		if fd == 2 {
			return unix.EIO
		}
		return nil
	}, func(fd int) error { sealed = append(sealed, fd); return nil })
	if !errors.Is(err, unix.EIO) || len(sealed) != 0 {
		t.Fatal("writeback failure proceeded to seal", err, sealed)
	}
	err = writebackFiles([]int{1, 2}, true, func(int) error { return nil }, func(fd int) error {
		sealed = append(sealed, fd)
		if fd == 1 {
			return unix.EBUSY
		}
		return nil
	})
	if !errors.Is(err, unix.EBUSY) || !slices.Equal(sealed, []int{1}) {
		t.Fatal("uncertain prior freeze treated as success", err, sealed)
	}
	sealed = nil
	if err := writebackFiles([]int{1, 2}, false, func(int) error { return nil }, func(fd int) error { sealed = append(sealed, fd); return nil }); err != nil || len(sealed) != 0 {
		t.Fatal("checkpoint sync froze filesystem", err)
	}
}
