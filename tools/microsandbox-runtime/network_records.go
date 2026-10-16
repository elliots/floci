package microsandboxruntime

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

// Keep effective rules outside SDK metadata: SDK restore drops VM labels and
// network configuration. These records are coupled to immutable VM/artifact IDs.
type restoredBoundary struct {
	ID         string             `json:"id"`
	Name       string             `json:"name"`
	Parent     string             `json:"parent"`
	Policy     string             `json:"policy"`
	CA         string             `json:"ca"`
	DNSVersion int                `json:"dnsVersion"`
	Network    *msb.NetworkConfig `json:"network"`
}

type snapshotBoundary struct {
	ID         string             `json:"id"`
	Policy     string             `json:"policy"`
	CA         string             `json:"ca"`
	DNSVersion int                `json:"dnsVersion"`
	Network    *msb.NetworkConfig `json:"network"`
}

func networkRecordPath(home, kind, id string) string {
	digest := sha256.Sum256([]byte(id))
	return filepath.Join(home, kind, hex.EncodeToString(digest[:])+".json")
}

func boundaryRecordPath(home, id string) string {
	return networkRecordPath(home, "floci-network-boundaries", id)
}

func snapshotBoundaryPath(home, id string) string {
	return networkRecordPath(home, "floci-network-snapshots", id)
}

func snapshotParent(config string) (string, error) {
	var stored struct {
		Parent string `json:"snapshot_parent"`
	}
	if err := json.Unmarshal([]byte(config), &stored); err != nil {
		return "", err
	}
	return stored.Parent, nil
}

func (b *nativeNetworkBoundary) caIdentity() string {
	digest := sha256.Sum256(b.caBytes)
	return hex.EncodeToString(digest[:])
}

func (b *nativeNetworkBoundary) requireGuest(handle *msb.SandboxHandle, home string) error {
	_, err := b.guestRecord(handle, home)
	return err
}

func (b *nativeNetworkBoundary) guestRecord(handle *msb.SandboxHandle, home string) (restoredBoundary, error) {
	var record restoredBoundary
	if err := readNetworkRecord(boundaryRecordPath(home, handle.ID()), &record); err != nil {
		return record, err
	}
	parent, err := snapshotParent(handle.ConfigJSON())
	if err != nil {
		return record, err
	}
	if record.DNSVersion != 1 || record.CA != b.caIdentity() || !validEffectivePolicy(record.Network) {
		return record, errors.New("guest lacks a verified DNS bootstrap and network policy; recreate it")
	}
	if parent != "" {
		return record, b.verifyRestoredIdentity(record, handle.ID(), handle.Name(), parent)
	}
	config, err := handle.Config()
	if err != nil {
		return record, err
	}
	if record.ID != handle.ID() || record.Name != handle.Name() || record.Parent != "" || record.Policy != b.fingerprint {
		return record, b.requireIdentity(nil)
	}
	return record, b.requireIdentity(config.Labels)
}

func (b *nativeNetworkBoundary) verifyRestoredIdentity(record restoredBoundary, id, name, parent string) error {
	if parent == "" || record.ID != id || record.Name != name || record.Parent != parent || record.Policy != b.fingerprint {
		return b.requireIdentity(nil)
	}
	return nil
}

func (b *nativeNetworkBoundary) recordGuest(handle *msb.SandboxHandle, home string, network *msb.NetworkConfig) error {
	if !validEffectivePolicy(network) {
		return errors.New("cannot persist an unverified native network policy")
	}
	parent, err := snapshotParent(handle.ConfigJSON())
	if err != nil {
		return err
	}
	record := restoredBoundary{ID: handle.ID(), Name: handle.Name(), Parent: parent, Policy: b.fingerprint,
		CA: b.caIdentity(), DNSVersion: 1, Network: network}
	return writeNetworkRecord(boundaryRecordPath(home, handle.ID()), record)
}

func (b *nativeNetworkBoundary) recordSnapshot(home, id string, guest restoredBoundary) error {
	record := snapshotBoundary{ID: id, Policy: b.fingerprint, CA: b.caIdentity(), DNSVersion: guest.DNSVersion, Network: guest.Network}
	return writeNetworkRecord(snapshotBoundaryPath(home, id), record)
}

func (b *nativeNetworkBoundary) snapshotPolicy(home, id string) (*msb.NetworkConfig, error) {
	var record snapshotBoundary
	if err := readNetworkRecord(snapshotBoundaryPath(home, id), &record); err != nil {
		return nil, err
	}
	if record.ID != id || record.Policy != b.fingerprint || record.CA != b.caIdentity() || record.DNSVersion != 1 || !validEffectivePolicy(record.Network) {
		return nil, errors.New("checkpoint lacks a verified DNS bootstrap and effective policy; recreate it")
	}
	return record.Network, nil
}

func readNetworkRecord(path string, value any) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() > 1<<20 {
		return errors.New("native network record is missing or not a bounded private regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return errors.New("cannot open native network record")
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil || len(data) > 1<<20 {
		return errors.New("cannot read native network record")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if decoder.Decode(value) != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.New("invalid native network record")
	}
	return nil
}

func writeNetworkRecord(path string, value any) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("native network record directory must be private and cannot be a symlink")
	}
	file, err := os.CreateTemp(directory, ".pending-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	defer file.Close()
	if err := json.NewEncoder(file).Encode(value); err != nil {
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
	parent, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer parent.Close()
	return parent.Sync()
}
