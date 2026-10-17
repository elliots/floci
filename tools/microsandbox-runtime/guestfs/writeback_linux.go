//go:build linux

package guestfs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// FIFREEZE is _IOWR('X', 119, int), identical on the supported Linux arches.
const freezeFilesystem = 0xc0045877

type writebackMount struct {
	id                       uint64
	device, root, path, kind string
}

func parseWritebackMounts(data string) ([]writebackMount, error) {
	var mounts []writebackMount
	for line := range strings.SplitSeq(data, "\n") {
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 10 {
			return nil, errors.New("invalid mount inventory")
		}
		id, err := strconv.ParseUint(fields[0], 10, 64)
		separator := -1
		for i := 6; i < len(fields); i++ {
			if fields[i] == "-" {
				separator = i
				break
			}
		}
		if err != nil || id == 0 || separator < 0 || separator+3 >= len(fields) {
			return nil, errors.New("invalid mount inventory")
		}
		decode := strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`)
		mounts = append(mounts, writebackMount{id: id, device: fields[2], root: decode.Replace(fields[3]), path: decode.Replace(fields[4]), kind: fields[separator+1]})
	}
	return mounts, nil
}

func validateWritebackMount(mounts []writebackMount, path, device string) error {
	var selected *writebackMount
	rootDevice := ""
	for i, mount := range mounts {
		if mount.path == "/" {
			rootDevice = mount.device
		}
		if mount.path == path {
			if selected != nil {
				return errors.New("ambiguous persistent mount")
			}
			selected = &mounts[i]
		}
	}
	if selected == nil || rootDevice == "" || selected.device != device || device == rootDevice || selected.root != "/" || selected.kind != "ext4" {
		return errors.New("writeback requires a distinct ext4 filesystem mount")
	}
	for _, mount := range mounts {
		if mount.device == device && mount.id < selected.id {
			return errors.New("writeback refuses a bind-mount alias")
		}
	}
	return nil
}

// Writeback flushes only the explicitly mounted ext4 filesystems. Seal leaves
// those filesystems frozen until this guest is destroyed; its agent/root stay
// writable so completion can be recorded even if an application keeps writing.
func Writeback(paths []string, seal bool) error {
	if len(paths) == 0 {
		return errors.New("writeback requires at least one filesystem")
	}
	inventory, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	mounts, err := parseWritebackMounts(string(inventory))
	if err != nil {
		return err
	}
	files := make([]*os.File, 0, len(paths))
	defer func() {
		for _, file := range files {
			file.Close()
		}
	}()
	devices := map[uint64]bool{}
	for _, path := range paths {
		canonical, err := filepath.EvalSymlinks(path)
		if err != nil || !filepath.IsAbs(path) || canonical != path || filepath.Clean(path) != path || path == "/" {
			return errors.New("writeback requires canonical filesystem roots")
		}
		fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		if err != nil {
			return err
		}
		file := os.NewFile(uintptr(fd), path)
		files = append(files, file)
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			return err
		}
		var fs unix.Statfs_t
		if err := unix.Fstatfs(fd, &fs); err != nil {
			return err
		}
		if fs.Type != unix.EXT4_SUPER_MAGIC || devices[stat.Dev] {
			return errors.New("writeback requires distinct ext4 filesystems")
		}
		if err := validateWritebackMount(mounts, path, fmt.Sprintf("%d:%d", unix.Major(stat.Dev), unix.Minor(stat.Dev))); err != nil {
			return err
		}
		devices[stat.Dev] = true
	}
	fds := make([]int, len(files))
	for i, file := range files {
		fds[i] = int(file.Fd())
	}
	return writebackFiles(fds, seal, unix.Syncfs, func(fd int) error { return unix.IoctlSetInt(fd, freezeFilesystem, 0) })
}

func writebackFiles(fds []int, seal bool, syncfs, freeze func(int) error) error {
	// Surface writeback errors before freezing any mount. No ioctl failure,
	// including EBUSY, proves that an earlier freeze completed successfully.
	for _, fd := range fds {
		if err := syncfs(fd); err != nil {
			return fmt.Errorf("persistent filesystem writeback failed: %w", err)
		}
	}
	if seal {
		for _, fd := range fds {
			if err := freeze(fd); err != nil {
				return fmt.Errorf("persistent filesystem seal failed: %w", err)
			}
		}
	}
	return nil
}
