package guestfs

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

type Groups struct {
	IDs       []uint32
	Strict    bool
	StateFile string
}

func ParseGroups(value, policy string) (Groups, error) {
	g := Groups{Strict: policy == "Strict"}
	if policy != "Merge" && policy != "Strict" {
		return g, errors.New("group policy must be Merge or Strict")
	}
	if value == "" {
		return g, nil
	}
	for _, part := range strings.Split(value, ",") {
		id, err := strconv.ParseUint(part, 10, 31)
		if err != nil {
			return g, fmt.Errorf("invalid supplemental group %q", part)
		}
		g.IDs = append(g.IDs, uint32(id))
	}
	return g, nil
}

// Rewrite changes only process.user.additionalGids and preserves unknown OCI
// members. The same operation is used for initial create and later exec/probes.
func (g Groups) Rewrite(data []byte, bundle bool) ([]byte, error) {
	var document map[string]json.RawMessage
	if err := json.Unmarshal(data, &document); err != nil {
		return nil, err
	}
	process := document
	if bundle {
		process = nil
		if err := json.Unmarshal(document["process"], &process); err != nil {
			return nil, fmt.Errorf("OCI process: %w", err)
		}
	}
	var user map[string]json.RawMessage
	if err := json.Unmarshal(process["user"], &user); err != nil {
		return nil, fmt.Errorf("OCI user: %w", err)
	}
	if user == nil {
		return nil, errors.New("OCI user is missing")
	}
	var primary uint32
	if err := json.Unmarshal(user["gid"], &primary); err != nil {
		return nil, fmt.Errorf("OCI primary gid: %w", err)
	}
	groups := []uint32{primary}
	if !g.Strict && len(user["additionalGids"]) != 0 {
		var inherited []uint32
		if err := json.Unmarshal(user["additionalGids"], &inherited); err != nil {
			return nil, err
		}
		groups = append(groups, inherited...)
	}
	for _, id := range g.IDs {
		groups = append(groups, id)
	}
	slices.Sort(groups)
	groups = slices.Compact(groups)
	user["additionalGids"], _ = json.Marshal(groups)
	process["user"], _ = json.Marshal(user)
	if bundle {
		document["process"], _ = json.Marshal(process)
	}
	return json.Marshal(document)
}

// PrepareRunc rewrites the OCI JSON consumed by runc, without changing ctr's
// image, snapshotter, lifecycle or cgroup setup. All other runc commands pass
// through unchanged. Arguments use the pinned runc/containerd invocation forms.
func (g Groups) PrepareRunc(args []string) error {
	command, rest, err := runcCommand(args)
	if err != nil {
		return err
	}
	if command != "create" && command != "exec" {
		return nil
	}
	option, short, file := "--bundle", "-b", "config.json"
	if command == "exec" {
		option, short, file = "--process", "-p", ""
	}
	for i := 0; i < len(rest); i++ {
		arg := rest[i]
		if value, ok := strings.CutPrefix(arg, option+"="); ok {
			file = value
			break
		}
		if arg == option || arg == short {
			if i+1 == len(rest) {
				return fmt.Errorf("missing %s argument", option)
			}
			file = rest[i+1]
			break
		}
	}
	if file == "" {
		return errors.New("runc exec requires an explicit process file for supplemental groups")
	}
	if command == "create" && file != "config.json" {
		file = filepath.Join(file, "config.json")
	}
	info, err := os.Lstat(file)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 4<<20 {
		return errors.New("OCI input must be a regular file no larger than 4 MiB")
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	if g.StateFile != "" {
		if command == "create" {
			g, err = g.withImageGroups(data, filepath.Dir(file))
		} else {
			g, err = g.withCreatedGroups(data)
		}
		if err != nil {
			return err
		}
	}
	data, err = g.Rewrite(data, command == "create")
	if err != nil {
		return err
	}
	if command == "create" && g.StateFile != "" {
		if err := g.recordGroups(data); err != nil {
			return err
		}
	}
	return os.WriteFile(file, data, info.Mode().Perm())
}

func runcCommand(args []string) (string, []string, error) {
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if !strings.HasPrefix(arg, "-") {
			return arg, args[i+1:], nil
		}
		name, _, inline := strings.Cut(arg, "=")
		switch name {
		case "--root", "--log", "--log-format", "--rootless", "--criu":
			if !inline {
				i++
				if i == len(args) {
					return "", nil, fmt.Errorf("missing %s argument", name)
				}
			}
		case "--debug", "--systemd-cgroup", "--help", "-h", "--version", "-v":
		default:
			return "", nil, fmt.Errorf("unrecognized runc global option %q", name)
		}
	}
	return "", nil, nil
}
