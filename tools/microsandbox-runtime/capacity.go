package microsandboxruntime

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
	"strconv"
	"sync"
	"time"

	msb "github.com/superradcompany/microsandbox/sdk/go"
	"golang.org/x/sys/unix"
)

// ErrCapacityUnavailable always means this command has not been dispatched.
// A caller may retain its cold workload and retry without reconciling a mutation.
var ErrCapacityUnavailable = errors.New("native capacity admission refused; operation not started")

type CapacityLimits struct {
	MemoryMiB uint64 `json:"memoryMiB"`
	Starts    uint32 `json:"starts"`
}

type CapacityStatus struct {
	Limits        CapacityLimits `json:"limits"`
	MemoryMiB     uint64         `json:"memoryMiB"`
	PendingStarts int            `json:"pendingStarts"`
	Reservations  []string       `json:"reservations"`
}

// AdmissionRequest reserves before a compound wake consumes its stopped source.
// The arguments contain no extra CLI flags; the opaque permit travels separately.
type AdmissionRequest struct {
	Arguments  []string `json:"arguments"`
	SourceName string   `json:"sourceName,omitempty"`
}

type capacityPermit struct {
	Hash, Name, Operation, SourceName, SourceID, TargetID, SnapshotID, SnapshotDigest string
	MemoryMiB                                                                         uint64
	Dispatched                                                                        bool
}

type capacityState struct {
	Version int                       `json:"version"`
	Limits  CapacityLimits            `json:"limits"`
	Permits map[string]capacityPermit `json:"permits"`
}

type capacityGuest struct {
	ID, Name, Status string
	MemoryMiB        uint64
}

type capacitySnapshot struct {
	ID, Digest string
	MemoryMiB  uint64
}

type capacityManager struct {
	home      string
	limits    CapacityLimits
	init      sync.Once
	initError error
	inventory func(context.Context) ([]capacityGuest, error)
	snapshot  func(context.Context, string) (capacitySnapshot, error)
}

type admissionContextKey struct{}

func newCapacityManager(config Config) (*capacityManager, error) {
	limits := CapacityLimits{MemoryMiB: config.MemoryLimitMiB, Starts: config.StartLimit}
	for _, field := range []struct {
		name string
		bits int
		set  func(uint64)
	}{
		{"FLOCI_MICROSANDBOX_MEMORY_LIMIT_MIB", 63, func(n uint64) { limits.MemoryMiB = n }},
		{"FLOCI_MICROSANDBOX_START_LIMIT", 32, func(n uint64) { limits.Starts = uint32(n) }},
	} {
		if value := os.Getenv(field.name); value != "" {
			n, err := strconv.ParseUint(value, 10, field.bits)
			if err != nil {
				return nil, invalid("invalid native capacity limit")
			}
			field.set(n)
		}
	}
	m := &capacityManager{home: config.Home, limits: limits}
	m.inventory = func(ctx context.Context) ([]capacityGuest, error) {
		if err := initializeSDK(config); err != nil {
			return nil, err
		}
		return capacityInventory(ctx)
	}
	m.snapshot = func(ctx context.Context, selector string) (capacitySnapshot, error) {
		if err := initializeSDK(config); err != nil {
			return capacitySnapshot{}, err
		}
		artifact, err := msb.Snapshot.Open(ctx, selector)
		if err != nil {
			return capacitySnapshot{}, err
		}
		var record capacitySnapshot
		if err := readCapacityRecord(snapshotMemoryPath(config.Home, artifact.ID()), &record); err != nil {
			return capacitySnapshot{}, err
		}
		if record.ID != artifact.ID() || record.Digest != artifact.Digest() || record.MemoryMiB == 0 {
			return capacitySnapshot{}, errors.New("snapshot memory identity differs")
		}
		return record, nil
	}
	return m, nil
}

func (c *Client) PrepareAdmission(ctx context.Context, request AdmissionRequest) (string, error) {
	r, err := parse(request.Arguments)
	if err != nil {
		return "", err
	}
	if c.capacity == nil {
		return "", errors.New("native capacity manager unavailable")
	}
	return c.capacity.prepare(ctx, r, request.Arguments, request.SourceName)
}

func (c *Client) CancelAdmission(ctx context.Context, token string) error {
	return c.capacity.transaction(ctx, func(state *capacityState) error {
		if permit, found := state.Permits[token]; found && permit.Dispatched {
			return errors.New("cannot cancel a dispatched native operation")
		}
		delete(state.Permits, token)
		return nil
	})
}

func (c *Client) ExecuteAdmitted(ctx context.Context, args []string, input []byte, token string) (Result, error) {
	return c.Execute(context.WithValue(ctx, admissionContextKey{}, token), args, input)
}

func capacityOperation(op string) bool {
	return op == "run" || op == "start" || op == "snap restore"
}

func (c *Client) admit(ctx context.Context, request command, args []string) (func(bool), error) {
	if c.capacity == nil || !capacityOperation(request.op) {
		return func(bool) {}, nil
	}
	token, _ := ctx.Value(admissionContextKey{}).(string)
	automatic := token == ""
	if token == "" {
		var err error
		token, err = c.capacity.prepare(ctx, request, args, "")
		if err != nil {
			return nil, err
		}
	}
	if err := c.capacity.dispatch(ctx, token, args); err != nil {
		if automatic {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_ = c.CancelAdmission(cleanup, token)
		}
		return nil, errors.Join(ErrCapacityUnavailable, err)
	}
	return func(success bool) {
		if success {
			// A cancelled caller cannot erase an accepted operation's reservation.
			// Successful completion releases only after a fresh inventory accounts
			// for the guest. If that read fails, retaining the permit is safe.
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			_ = c.capacity.finish(cleanup, token)
		}
	}, nil
}

func argumentsHash(args []string) string {
	data, _ := json.Marshal(args)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func (m *capacityManager) prepare(ctx context.Context, r command, args []string, source string) (string, error) {
	if !capacityOperation(r.op) || source != "" && r.op != "snap restore" {
		return "", invalid("admission requires a native launch, start or restore")
	}
	token := rand.Text()
	err := m.transaction(ctx, func(state *capacityState) error {
		guests, err := m.inventory(ctx)
		if err != nil {
			return err
		}
		p := capacityPermit{Hash: argumentsHash(args), Name: r.name, Operation: r.op, SourceName: source, MemoryMiB: uint64(r.config.MemoryMiB)}
		for _, guest := range guests {
			if guest.Name == source {
				if guest.Status != "stopped" {
					return errors.New("restore source is not stopped")
				}
				p.SourceID = guest.ID
			}
			if guest.Name == r.name {
				if r.op != "start" && guest.Name != source || guest.Status != "stopped" {
					return errors.New("native target is not available")
				}
				if r.op == "start" {
					p.TargetID, p.MemoryMiB = guest.ID, guest.MemoryMiB
				}
			}
		}
		if source != "" && p.SourceID == "" || r.op == "start" && p.TargetID == "" {
			return errors.New("native source identity unavailable")
		}
		if r.op == "snap restore" {
			snapshot, err := m.snapshot(ctx, r.selector)
			if err != nil {
				return err
			}
			p.SnapshotID, p.SnapshotDigest, p.MemoryMiB = snapshot.ID, snapshot.Digest, snapshot.MemoryMiB
		}
		status, err := capacityUsage(state, guests)
		if err != nil {
			return err
		}
		for _, pending := range state.Permits {
			if pending.Name == p.Name || source != "" && (pending.Name == source || pending.SourceID == p.SourceID) {
				return errors.New("native target has an unresolved reservation")
			}
		}
		if len(state.Permits) >= 10000 {
			return errors.New("native reservation ledger requires reconciliation")
		}
		if state.Limits.Starts > 0 && len(state.Permits) >= int(state.Limits.Starts) {
			return errors.New("concurrent native start limit reached")
		}
		if state.Limits.MemoryMiB > 0 && (p.MemoryMiB == 0 || p.MemoryMiB > state.Limits.MemoryMiB || status.MemoryMiB > state.Limits.MemoryMiB-p.MemoryMiB) {
			return errors.New("native memory limit reached")
		}
		state.Permits[token] = p
		return nil
	})
	if err != nil {
		return "", errors.Join(ErrCapacityUnavailable, err)
	}
	return token, nil
}

func (m *capacityManager) dispatch(ctx context.Context, token string, args []string) error {
	return m.transaction(ctx, func(state *capacityState) error {
		p, found := state.Permits[token]
		if !found || p.Dispatched || p.Hash != argumentsHash(args) {
			return errors.New("native admission permit does not match")
		}
		guests, err := m.inventory(ctx)
		if err != nil {
			return err
		}
		for _, guest := range guests {
			if guest.Name == p.SourceName && (guest.ID != p.SourceID || guest.Status != "stopped") {
				return errors.New("native restore source changed")
			}
			if guest.Name == p.Name && (p.Operation != "start" || guest.ID != p.TargetID || guest.Status != "stopped") {
				return errors.New("native target changed")
			}
		}
		if p.SnapshotID != "" {
			snapshot, err := m.snapshot(ctx, p.SnapshotID)
			if err != nil || snapshot.Digest != p.SnapshotDigest || snapshot.MemoryMiB != p.MemoryMiB {
				return errors.Join(errors.New("native checkpoint changed"), err)
			}
		}
		p.Dispatched = true
		state.Permits[token] = p
		return nil
	})
}

func (m *capacityManager) finish(ctx context.Context, token string) error {
	return m.transaction(ctx, func(state *capacityState) error {
		p, found := state.Permits[token]
		if !found || !p.Dispatched {
			return errors.New("native completion has no dispatched permit")
		}
		guests, err := m.inventory(ctx)
		if err != nil {
			return err
		}
		for _, guest := range guests {
			if guest.Name == p.Name && (p.TargetID == "" || guest.ID == p.TargetID) && guest.MemoryMiB > 0 {
				delete(state.Permits, token)
				return nil
			}
		}
		return errors.New("native completion could not be reconciled")
	})
}

func capacityUsage(state *capacityState, guests []capacityGuest) (CapacityStatus, error) {
	status := CapacityStatus{Limits: state.Limits, PendingStarts: len(state.Permits), Reservations: []string{}}
	counted := map[string]bool{}
	add := func(memory uint64) error {
		if memory == 0 && state.Limits.MemoryMiB > 0 || memory > ^uint64(0)-status.MemoryMiB {
			return errors.New("native memory accounting unavailable")
		}
		status.MemoryMiB += memory
		return nil
	}
	for _, guest := range guests {
		if guest.ID == "" || counted[guest.ID] {
			return status, errors.New("native inventory identity differs")
		}
		if guest.Status != "stopped" {
			if err := add(guest.MemoryMiB); err != nil {
				return status, err
			}
			counted[guest.ID] = true
		}
	}
	for token, p := range state.Permits {
		status.Reservations = append(status.Reservations, token)
		if !counted[p.TargetID] {
			if err := add(p.MemoryMiB); err != nil {
				return status, err
			}
		}
	}
	return status, nil
}

func (c *Client) Capacity(ctx context.Context) (CapacityStatus, error) {
	var status CapacityStatus
	err := c.capacity.transaction(ctx, func(state *capacityState) error {
		guests, err := c.capacity.inventory(ctx)
		if err != nil {
			return err
		}
		status, err = capacityUsage(state, guests)
		return err
	})
	return status, err
}

func (c *Client) SetCapacityLimits(ctx context.Context, limits CapacityLimits) error {
	return c.capacity.transaction(ctx, func(state *capacityState) error {
		state.Limits = limits
		return nil
	})
}

// Every transaction locks the same private file, including across processes and
// symlink aliases of the runtime home. No VM operation runs while it is locked.
func (m *capacityManager) transaction(ctx context.Context, fn func(*capacityState) error) error {
	dir := filepath.Join(m.home, "floci-capacity")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return errors.New("native capacity directory must be private")
	}
	fd, err := unix.Open(filepath.Join(dir, "lock"), unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	for {
		err = unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			break
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	defer unix.Flock(fd, unix.LOCK_UN)
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Join(dir, "state.json")
	state := capacityState{Version: 1, Limits: m.limits, Permits: map[string]capacityPermit{}}
	if err := readCapacityRecord(path, &state); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if state.Version != 1 || state.Permits == nil {
		return errors.New("native capacity ledger is invalid")
	}
	m.init.Do(func() {
		if state.Limits.MemoryMiB == 0 && m.limits.MemoryMiB != 0 {
			state.Limits.MemoryMiB = m.limits.MemoryMiB
		}
		if state.Limits.Starts == 0 && m.limits.Starts != 0 {
			state.Limits.Starts = m.limits.Starts
		}
		if m.limits.MemoryMiB != 0 && m.limits.MemoryMiB != state.Limits.MemoryMiB || m.limits.Starts != 0 && m.limits.Starts != state.Limits.Starts {
			m.initError = errors.New("native capacity configuration differs between helpers")
			return
		}
		m.initError = writeNetworkRecord(path, state)
	})
	if m.initError != nil {
		return m.initError
	}
	if err := fn(&state); err != nil {
		return err
	}
	return writeNetworkRecord(path, state)
}

func readCapacityRecord(path string, out any) error {
	file, err := os.OpenFile(path, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 16<<20 {
		return errors.New("native capacity record is invalid")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 16<<20))
	decoder.DisallowUnknownFields()
	if decoder.Decode(out) != nil || decoder.Decode(new(any)) != io.EOF {
		return errors.New("native capacity record is invalid")
	}
	return nil
}

func snapshotMemoryPath(home, id string) string {
	digest := sha256.Sum256([]byte(id))
	return filepath.Join(home, "floci-snapshot-memory", hex.EncodeToString(digest[:])+".json")
}

func recordSnapshotMemory(home, id, digest string, memory uint64) error {
	return writeNetworkRecord(snapshotMemoryPath(home, id), capacitySnapshot{ID: id, Digest: digest, MemoryMiB: memory})
}

func capacityInventory(ctx context.Context) ([]capacityGuest, error) {
	var guests []capacityGuest
	cursor, seen := "", map[string]bool{}
	for {
		options := []msb.SandboxListOption{msb.WithListLimit(100)}
		if cursor != "" {
			options = append(options, msb.WithListCursor(cursor))
		}
		page, err := msb.ListSandboxesWith(ctx, options...)
		if err != nil {
			return nil, err
		}
		for _, handle := range page.Sandboxes {
			config, err := handle.Config()
			if err != nil {
				return nil, err
			}
			guests = append(guests, capacityGuest{ID: handle.ID(), Name: handle.Name(), Status: string(handle.Status()), MemoryMiB: uint64(max(config.MemoryMiB, config.MaxMemoryMiB))})
		}
		if page.NextCursor == nil || *page.NextCursor == "" {
			return guests, nil
		}
		cursor = *page.NextCursor
		if seen[cursor] {
			return nil, errors.New("native capacity inventory repeated a cursor")
		}
		seen[cursor] = true
	}
}
