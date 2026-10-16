package guestfs

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type processUser struct {
	UID    uint32   `json:"uid"`
	GID    uint32   `json:"gid"`
	Groups []uint32 `json:"additionalGids"`
}

func (g Groups) withImageGroups(data []byte, bundle string) (Groups, error) {
	if g.Strict {
		return g, nil
	}
	var spec struct {
		Root struct {
			Path string `json:"path"`
		} `json:"root"`
		Process struct {
			User processUser `json:"user"`
		} `json:"process"`
	}
	if err := json.Unmarshal(data, &spec); err != nil {
		return g, err
	}
	if spec.Root.Path == "" {
		return g, errors.New("OCI rootfs path is missing")
	}
	rootPath := spec.Root.Path
	if !filepath.IsAbs(rootPath) {
		rootPath = filepath.Join(bundle, rootPath)
	}
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return g, err
	}
	defer root.Close()
	username := ""
	if err := readDatabase(root, "etc/passwd", func(fields []string) {
		if len(fields) == 7 {
			if uid, err := strconv.ParseUint(fields[2], 10, 32); err == nil && uint32(uid) == spec.Process.User.UID && username == "" {
				username = fields[0]
			}
		}
	}); err != nil {
		return g, err
	}
	if username == "" {
		return g, nil
	}
	err = readDatabase(root, "etc/group", func(fields []string) {
		if len(fields) != 4 || fields[0] == username {
			return
		}
		gid, err := strconv.ParseUint(fields[2], 10, 32)
		if err != nil {
			return
		}
		for _, member := range strings.Split(fields[3], ",") {
			if member == username {
				g.IDs = append(g.IDs, uint32(gid))
				break
			}
		}
	})
	return g, err
}

func readDatabase(root *os.Root, name string, visit func([]string)) error {
	file, err := root.Open(name)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(io.LimitReader(file, 1<<20+1))
	read := 0
	for scanner.Scan() {
		line := scanner.Text()
		read += len(line) + 1
		if read > 1<<20 {
			return fmt.Errorf("image %s exceeds 1 MiB", name)
		}
		if strings.HasPrefix(line, "#") {
			continue
		}
		visit(strings.Split(line, ":"))
	}
	return scanner.Err()
}

func (g Groups) recordGroups(data []byte) error {
	var spec struct {
		Process struct {
			User processUser `json:"user"`
		} `json:"process"`
	}
	if err := json.Unmarshal(data, &spec); err != nil {
		return err
	}
	record, err := json.Marshal(spec.Process.User)
	if err != nil {
		return err
	}
	if err := os.WriteFile(g.StateFile+".pending", record, 0600); err != nil {
		return err
	}
	return os.Rename(g.StateFile+".pending", g.StateFile)
}

func (g Groups) withCreatedGroups(data []byte) (Groups, error) {
	state, err := os.ReadFile(g.StateFile)
	if err != nil {
		return g, fmt.Errorf("read container group identity: %w", err)
	}
	var original processUser
	if err := json.Unmarshal(state, &original); err != nil {
		return g, err
	}
	var process struct {
		User processUser `json:"user"`
	}
	if err := json.Unmarshal(data, &process); err != nil {
		return g, err
	}
	if process.User.UID != original.UID || process.User.GID != original.GID {
		return g, errors.New("exec user differs from the container group identity")
	}
	g.Strict = true
	g.IDs = original.Groups
	return g, nil
}
