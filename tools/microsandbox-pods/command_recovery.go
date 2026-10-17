package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// A terminal record closes only the crash window after a successful native
// response and before fence removal. An intent is never a completion proof.
type nativeCommandRecord struct {
	Version      int                 `json:"version"`
	Nonce        string              `json:"nonce"`
	Phase        string              `json:"phase"`
	ArgumentHash string              `json:"argumentHash"`
	Operation    string              `json:"operation"`
	Target       string              `json:"target,omitempty"`
	Selector     string              `json:"selector,omitempty"`
	OwnerHash    string              `json:"ownerHash,omitempty"`
	UID          string              `json:"uid,omitempty"`
	Generation   uint64              `json:"generation"`
	Before       *nativeCommandProof `json:"before,omitempty"`
	After        *nativeCommandProof `json:"after,omitempty"`
}

type nativeCommandProof struct {
	VMID           string `json:"vmID,omitempty"`
	Status         string `json:"status,omitempty"`
	Parent         string `json:"parent,omitempty"`
	SnapshotID     string `json:"snapshotID,omitempty"`
	SnapshotDigest string `json:"snapshotDigest,omitempty"`
}

func commandHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (r commandRunner) commandIntent(ctx context.Context, args []string) nativeCommandRecord {
	encoded, _ := json.Marshal(args)
	op, target, selector := commandTarget(args)
	record := nativeCommandRecord{Version: 1, Nonce: rand.Text(), Phase: "intent", ArgumentHash: commandHash(encoded), Operation: op, Target: target, Selector: selector}
	owner, err := readPrivateCommandFile(filepath.Join(r.cfg.state, "owner"))
	if err != nil {
		return record
	}
	journal, err := r.commandJournal(target)
	if err != nil {
		return record
	}
	record.OwnerHash, record.UID, record.Generation = commandHash(owner), journal.View.UID, journal.View.Generation
	proof, err := r.commandProof(ctx, record, journal)
	if err == nil && beforeCommandMatches(record, proof, journal) {
		record.Before = &proof
	}
	return record
}

func beforeCommandMatches(record nativeCommandRecord, proof nativeCommandProof, journal journalRecord) bool {
	switch record.Operation {
	case "run":
		return proof.VMID == ""
	case "snap restore":
		return proof.VMID == "" && proof.SnapshotID != "" && proof.SnapshotID == journal.SnapshotIDs[record.Selector] && journal.RestoredParent == proof.SnapshotID
	case "snap create":
		return proof.VMID != "" && proof.SnapshotID == ""
	case "snap rm":
		return proof.SnapshotID != "" && proof.SnapshotID == journal.SnapshotIDs[record.Selector]
	case "start", "stop", "pause", "resume", "rm":
		return proof.VMID != ""
	}
	return false
}

func (r commandRunner) completeCommand(ctx context.Context, record *nativeCommandRecord, notStarted bool) error {
	if record.Before == nil {
		return nil
	}
	journal, err := r.commandJournal(record.Target)
	if err != nil || journal.View.UID != record.UID || journal.View.Generation != record.Generation {
		return nil
	}
	proof, err := r.commandProof(ctx, *record, journal)
	if err != nil {
		return nil
	}
	if notStarted {
		if proof != *record.Before {
			return nil
		}
		record.Phase = "not-started"
	} else {
		if !afterCommandMatches(*record, proof) {
			return nil
		}
		record.Phase = "completed"
	}
	record.After = &proof
	return r.writeCommandRecord(*record, false)
}

func afterCommandMatches(record nativeCommandRecord, proof nativeCommandProof) bool {
	before := record.Before
	if before == nil {
		return false
	}
	switch record.Operation {
	case "run":
		return before.VMID == "" && proof.VMID != "" && proof.Status == "Running"
	case "snap restore":
		return before.VMID == "" && proof.VMID != "" && proof.Status == "Running" && proof.Parent == before.SnapshotID && proof.SnapshotID == before.SnapshotID && proof.SnapshotDigest == before.SnapshotDigest
	case "rm":
		return before.VMID != "" && proof.VMID == ""
	case "snap rm":
		return before.SnapshotID != "" && proof.SnapshotID == "" && proof.VMID == before.VMID
	case "snap create":
		return proof.VMID == before.VMID && proof.Status == before.Status && proof.SnapshotID != "" && proof.SnapshotDigest != ""
	case "start", "resume", "pause", "stop":
		wanted := map[string]string{"start": "Running", "resume": "Running", "pause": "Paused", "stop": "Stopped"}[record.Operation]
		return proof.VMID == before.VMID && proof.Status == wanted && proof.Parent == before.Parent
	}
	return false
}

func (r commandRunner) recoverCommand(ctx context.Context) error {
	marker := filepath.Join(r.cfg.state, "native-command.pending")
	data, err := readPrivateCommandFile(marker)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var record nativeCommandRecord
	if json.Unmarshal(data, &record) != nil || record.Version != 1 || record.Nonce == "" || len(record.ArgumentHash) != 64 || record.Before == nil || record.After == nil || record.OwnerHash == "" || record.Phase != "completed" && record.Phase != "not-started" {
		return errors.New("native command has no durable terminal outcome; manual reconciliation is required")
	}
	owner, err := readPrivateCommandFile(filepath.Join(r.cfg.state, "owner"))
	if err != nil || commandHash(owner) != record.OwnerHash {
		return errors.New("native command controller ownership changed")
	}
	journal, err := r.commandJournal(record.Target)
	if err != nil || journal.View.UID != record.UID || journal.View.Generation != record.Generation {
		return errors.New("native command Pod identity or generation changed")
	}
	proof, err := r.commandProof(ctx, record, journal)
	if err != nil || proof != *record.After {
		return errors.New("native command terminal identity no longer matches the runtime")
	}
	if record.Phase == "completed" && !afterCommandMatches(record, proof) || record.Phase == "not-started" && (proof != *record.Before || !beforeCommandMatches(record, proof, journal)) {
		return errors.New("native command terminal outcome is inconsistent")
	}
	if err := os.Remove(marker); err != nil {
		return err
	}
	return syncDirectory(r.cfg.state)
}

func readPrivateCommandFile(path string) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 8<<20 {
		return nil, errors.New("native command ownership file is invalid")
	}
	return io.ReadAll(io.LimitReader(file, 8<<20))
}

func (r commandRunner) commandJournal(target string) (journalRecord, error) {
	var found journalRecord
	prefix, ok := strings.CutPrefix(target, "fvp-")
	if !ok || len(prefix) != 20 {
		return found, errors.New("native target has no Pod identity")
	}
	if _, err := hex.DecodeString(prefix); err != nil {
		return found, err
	}
	files, err := filepath.Glob(filepath.Join(r.cfg.state, prefix[:8]+"*.json"))
	if err != nil {
		return found, err
	}
	for _, path := range files {
		data, err := readPrivateCommandFile(path)
		if err != nil {
			return found, err
		}
		var journal journalRecord
		if json.Unmarshal(data, &journal) != nil || journal.View.VM != target {
			continue
		}
		if found.Pod != nil || journal.Pod == nil || journal.View.UID != string(journal.Pod.UID) || filepath.Base(path) != journal.View.UID+".json" || !strings.HasPrefix(stringsWithoutHyphens(journal.View.UID), prefix) {
			return found, errors.New("native target has ambiguous Pod ownership")
		}
		found = journal
	}
	if found.Pod == nil {
		return found, errors.New("native target has no Pod journal")
	}
	return found, nil
}

func (r commandRunner) commandProof(ctx context.Context, record nativeCommandRecord, journal journalRecord) (nativeCommandProof, error) {
	var proof nativeCommandProof
	type listedVM struct{ ID, Name string }
	list := func() ([]listedVM, error) {
		data, err := r.nativeOutput(ctx, []string{"ls", "--format", "json"}, nil)
		if err != nil {
			return nil, err
		}
		var entries []listedVM
		err = json.Unmarshal(data, &entries)
		return entries, err
	}
	identity := func(entries []listedVM) (string, error) {
		id := ""
		for _, entry := range entries {
			if entry.Name != record.Target {
				continue
			}
			if entry.ID == "" || id != "" {
				return "", errors.New("native VM identity is ambiguous")
			}
			id = entry.ID
		}
		if id == "" && record.Before != nil && record.Before.VMID != "" {
			for _, entry := range entries {
				if entry.ID == record.Before.VMID {
					return "", errors.New("native VM moved to another name")
				}
			}
		}
		return id, nil
	}
	entries, err := list()
	if err != nil {
		return proof, err
	}
	proof.VMID, err = identity(entries)
	if err != nil {
		return proof, err
	}
	if proof.VMID != "" {
		data, err := r.nativeOutput(ctx, []string{"inspect", "--format", "json", record.Target}, nil)
		if err != nil {
			return proof, err
		}
		type vmConfig struct {
			Labels map[string]string `json:"labels"`
			Parent string            `json:"snapshot_parent"`
		}
		var inspection struct {
			Status string
			Config vmConfig
			Active *vmConfig `json:"active_config"`
		}
		if err := json.Unmarshal(data, &inspection); err != nil {
			return proof, err
		}
		configs := []vmConfig{inspection.Config}
		if inspection.Active != nil {
			configs = append(configs, *inspection.Active)
		}
		for _, config := range configs {
			if journal.RestoredParent == "" {
				if config.Parent != "" || config.Labels[ownerLabel] != r.cfg.peerOwner || config.Labels["kubernetes.io/pod-uid"] != journal.View.UID {
					return proof, errors.New("native VM ownership differs")
				}
			} else if config.Parent != journal.RestoredParent || config.Labels[ownerLabel] != "" && config.Labels[ownerLabel] != r.cfg.peerOwner || config.Labels["kubernetes.io/pod-uid"] != "" && config.Labels["kubernetes.io/pod-uid"] != journal.View.UID {
				return proof, errors.New("native VM restored lineage differs")
			}
		}
		proof.Status, proof.Parent = inspection.Status, inspection.Config.Parent
		confirmed, err := list()
		if err != nil {
			return proof, err
		}
		confirmedID, err := identity(confirmed)
		if err != nil || confirmedID != proof.VMID {
			return proof, errors.New("native VM identity changed during inspection")
		}
	}
	if record.Selector != "" {
		snapshot, err := r.commandSnapshot(ctx, record.Selector)
		if err != nil {
			return proof, err
		}
		proof.SnapshotID, proof.SnapshotDigest = snapshot.SnapshotID, snapshot.SnapshotDigest
		if proof.SnapshotID != "" {
			if _, err := r.nativeOutput(ctx, []string{"snap", "verify", record.Selector}, nil); err != nil {
				return proof, err
			}
			confirmed, err := r.commandSnapshot(ctx, record.Selector)
			if err != nil || confirmed != snapshot {
				return proof, errors.New("native snapshot changed during verification")
			}
		}
	}
	return proof, nil
}

func (r commandRunner) commandSnapshot(ctx context.Context, selector string) (nativeCommandProof, error) {
	var proof nativeCommandProof
	data, err := r.nativeOutput(ctx, []string{"snap", "ls", "--format", "json"}, nil)
	if err != nil {
		return proof, err
	}
	var snapshots []struct {
		Group, Name, Digest string
		ID                  string `json:"snapshot_id"`
	}
	if err := json.Unmarshal(data, &snapshots); err != nil {
		return proof, err
	}
	for _, snapshot := range snapshots {
		if snapshot.Group+":"+snapshot.Name != selector {
			continue
		}
		if proof.SnapshotID != "" || snapshot.ID == "" || snapshot.Digest == "" {
			return proof, errors.New("native snapshot identity is ambiguous")
		}
		proof.SnapshotID, proof.SnapshotDigest = snapshot.ID, snapshot.Digest
	}
	return proof, nil
}

func (r commandRunner) writeCommandRecord(record nativeCommandRecord, exclusive bool) error {
	marker := filepath.Join(r.cfg.state, "native-command.pending")
	var file *os.File
	var err error
	if exclusive {
		file, err = os.OpenFile(marker, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	} else {
		file, err = os.CreateTemp(r.cfg.state, ".native-command-")
	}
	if err != nil {
		return err
	}
	defer file.Close()
	if !exclusive {
		defer os.Remove(file.Name())
	}
	if err := json.NewEncoder(file).Encode(record); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if !exclusive {
		if err := os.Rename(file.Name(), marker); err != nil {
			return err
		}
	}
	return syncDirectory(r.cfg.state)
}

func commandTarget(args []string) (operation, target, selector string) {
	if len(args) == 0 {
		return
	}
	operation = args[0]
	start := 1
	if operation == "snap" && len(args) > 1 {
		operation += " " + args[1]
		start = 2
	}
	var positional []string
	for i := start; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			break
		}
		if arg == "--name" || arg == "--sandbox" {
			i++
			if i >= len(args) {
				return operation, "", ""
			}
			target = args[i]
			continue
		}
		if arg == "-d" || arg == "--detach" || arg == "--full" || arg == "--integrity" || arg == "--force" {
			continue
		}
		if strings.HasPrefix(arg, "--") {
			i++
			continue
		}
		positional = append(positional, arg)
		if operation == "run" {
			break
		}
	}
	if strings.HasPrefix(operation, "snap ") && len(positional) == 1 {
		selector = positional[0]
		if operation == "snap create" {
			selector = target + ":" + selector
		}
		if operation == "snap rm" {
			target, _, _ = strings.Cut(selector, ":")
		}
	} else if operation != "run" && len(positional) == 1 {
		target = positional[0]
	}
	return
}
