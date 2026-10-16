// Package guestfs implements one-shot filesystem operations inside a Linux guest.
package guestfs

import (
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"syscall"
)

// Ownership follows Kubernetes fsGroup permission masks. Callers must keep the
// volume unavailable to workloads until this operation completes.
type Ownership struct {
	Group          int64
	ReadOnly       bool
	OnRootMismatch bool
	RootOnly       bool
}

func (o Ownership) Apply(root string) error {
	if o.Group < 0 || o.Group > math.MaxInt32 {
		return fmt.Errorf("filesystem group must be between 0 and %d", math.MaxInt32)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return fmt.Errorf("volume root must be a directory")
	}
	if o.OnRootMismatch {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if ok && int64(stat.Gid) == o.Group && info.Mode()&o.mask(true) == o.mask(true) {
			return nil
		}
	}
	return o.walk(root, info)
}

func (o Ownership) mask(directory bool) fs.FileMode {
	mask := fs.FileMode(0660)
	if o.ReadOnly {
		mask = 0440
	}
	if directory {
		mask |= 0110 | fs.ModeSetgid
	}
	return mask
}

func (o Ownership) walk(name string, info fs.FileInfo) error {
	// Process directories last, matching kubelet: a completed root proves that
	// every descendant has been visited for OnRootMismatch on the next mount.
	if info.IsDir() && !o.RootOnly {
		entries, err := os.ReadDir(name)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			child := filepath.Join(name, entry.Name())
			childInfo, err := os.Lstat(child)
			if err != nil {
				return err
			}
			if err := o.walk(child, childInfo); err != nil {
				return err
			}
		}
	}
	if err := os.Lchown(name, -1, int(o.Group)); err != nil {
		return err
	}
	if info.Mode()&fs.ModeSymlink != 0 {
		return nil
	}
	return os.Chmod(name, info.Mode()|o.mask(info.IsDir()))
}
