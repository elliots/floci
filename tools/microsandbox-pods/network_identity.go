package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type networkHostIdentity struct {
	Version int    `json:"version"`
	NodeIP  string `json:"nodeIP"`
}

func readNetworkIdentityFile(path string) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 8<<20 {
		return nil, errors.New("invalid private network identity file")
	}
	return io.ReadAll(io.LimitReader(file, 8<<20))
}

func (n *network) pinHostIdentity(ctx context.Context) error {
	path := filepath.Join(n.runner.cfg.state, "network-host")
	data, err := readNetworkIdentityFile(path)
	missing := errors.Is(err, os.ErrNotExist)
	if err != nil && !missing {
		return err
	}
	if !missing {
		var identity networkHostIdentity
		if json.Unmarshal(data, &identity) != nil || identity.Version != 1 || identity.NodeIP == "" {
			return errors.New("invalid stored peer host identity")
		}
		if identity.NodeIP == n.hostIP {
			return nil
		}
	}
	files, err := filepath.Glob(filepath.Join(n.runner.cfg.state, "*.json"))
	if err != nil {
		return err
	}
	if !missing && len(files) != 0 {
		return errors.New("node-ip changed while owned Pod journals remain")
	}
	var attached []journalRecord
	for _, file := range files {
		data, err := readNetworkIdentityFile(file)
		if err != nil {
			return err
		}
		var record journalRecord
		if json.Unmarshal(data, &record) != nil || record.Version < 1 || record.Version > 9 || record.Pod == nil || record.View.UID != string(record.Pod.UID) || filepath.Base(file) != record.View.UID+".json" {
			return errors.New("invalid Pod ownership while adopting peer host identity")
		}
		uid := stringsWithoutHyphens(record.View.UID)
		if len(uid) < 20 || record.View.VM != "fvp-"+uid[:20] || record.Slot < 0 || record.Slot > 1020 || record.View.UDPPort < 1 || record.View.UDPPort > 65535 {
			return errors.New("invalid Pod network allocation while adopting peer host identity")
		}
		if record.GuestIP != "" {
			attached = append(attached, record)
		} else if len(record.Snapshots) != 0 || record.RestoredParent != "" {
			return errors.New("checkpoint has no provable peer host identity")
		}
	}
	if len(attached) > 0 {
		rules, err := n.runner.peer(ctx, "iptables -t nat -S OUTPUT")
		if err != nil {
			return err
		}
		for _, record := range attached {
			if err := proveLegacyPeerHost(rules, n.hostIP, n.peerIP, record); err != nil {
				return err
			}
		}
	}
	file, err := os.CreateTemp(n.runner.cfg.state, ".network-host-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := json.NewEncoder(file).Encode(networkHostIdentity{Version: 1, NodeIP: n.hostIP}); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), path); err != nil {
		return err
	}
	return syncDirectory(n.runner.cfg.state)
}

func proveLegacyPeerHost(rules, hostIP, peerIP string, record journalRecord) error {
	found := false
	for line := range strings.SplitSeq(rules, "\n") {
		fields := strings.Fields(line)
		values := map[string]string{}
		for i := 0; i+1 < len(fields); i++ {
			if strings.HasPrefix(fields[i], "-") {
				values[fields[i]] = fields[i+1]
			}
		}
		if values["-A"] != "OUTPUT" || values["-p"] != "udp" || strings.TrimSuffix(values["-d"], "/32") != record.GuestIP || values["--dport"] != "8472" || values["-j"] != "DNAT" {
			continue
		}
		target, port, err := net.SplitHostPort(values["--to-destination"])
		if err != nil || port != strconv.Itoa(record.View.UDPPort) {
			continue
		}
		if slices.Contains(fields, "!") {
			return errors.New("negated legacy Pod route cannot prove host ownership")
		}
		if target != hostIP {
			return errors.New("legacy Pod route belongs to a different host address")
		}
		if source := values["-s"]; source != "" && strings.TrimSuffix(source, "/32") != peerIP {
			return errors.New("legacy Pod route peer ownership differs")
		}
		if source := values["--sport"]; source != "" && source != strconv.Itoa(20000+record.Slot) {
			return errors.New("legacy Pod route source ownership differs")
		}
		found = true
	}
	if !found {
		return fmt.Errorf("Pod %s has no owned route proving its original host address", record.View.UID)
	}
	return nil
}
