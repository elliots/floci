package microsandboxruntime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"time"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

// StorageReport preserves unknown SDK counters as null. Allocated bytes can
// count shared blocks repeatedly; they are not exclusive physical ownership.
type StorageReport struct {
	Usage          *msb.StorageUsageReport `json:"usage"`
	AvailableBytes uint64                  `json:"availableBytes"`
}

type StoragePruneRequest struct {
	DryRun           bool     `json:"dryRun"`
	OlderThanSeconds uint64   `json:"olderThanSeconds"`
	Images           bool     `json:"images"`
	RetainImages     []string `json:"retainImages,omitempty"`
}

// DecodeStoragePruneRequest makes omission non-destructive for both host tools.
func DecodeStoragePruneRequest(reader io.Reader) (StoragePruneRequest, error) {
	request := StoragePruneRequest{DryRun: true, OlderThanSeconds: 86400}
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	input := &request
	if err := decoder.Decode(&input); err != nil || input == nil {
		return request, invalid("invalid storage cleanup request")
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return request, invalid("storage cleanup requires one JSON object")
	}
	return request, nil
}

type ImagePruneOutcome struct {
	Reference string `json:"reference"`
	State     string `json:"state"`
}

type StoragePruneReport struct {
	DryRun bool                    `json:"dryRun"`
	Memory *msb.StoragePruneReport `json:"memory,omitempty"`
	Images []ImagePruneOutcome     `json:"images"`
}

type cachedStorageImage struct {
	reference, digest string
	created, used     time.Time
}

type storageBackend interface {
	usage(context.Context) (StorageReport, error)
	pruneMemory(context.Context, msb.StoragePruneOptions) (*msb.StoragePruneReport, error)
	images(context.Context) ([]cachedStorageImage, error)
	removeImage(context.Context, cachedStorageImage) error
}

type sdkStorage struct{ config Config }

func (s sdkStorage) usage(ctx context.Context) (StorageReport, error) {
	if err := initializeSDK(s.config); err != nil {
		return StorageReport{}, err
	}
	observation, err := observeNativeDisk(ctx, s.config.Home)
	return StorageReport{Usage: observation.Usage, AvailableBytes: observation.AvailableBytes}, err
}

func (s sdkStorage) pruneMemory(ctx context.Context, options msb.StoragePruneOptions) (*msb.StoragePruneReport, error) {
	if err := initializeSDK(s.config); err != nil {
		return nil, err
	}
	return msb.PruneStorage(ctx, options)
}

func (s sdkStorage) images(ctx context.Context) ([]cachedStorageImage, error) {
	if err := initializeSDK(s.config); err != nil {
		return nil, err
	}
	images, err := msb.Image.List(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]cachedStorageImage, 0, len(images))
	for _, image := range images {
		result = append(result, cachedStorageImage{reference: image.Reference(), digest: image.ManifestDigest(), created: image.CreatedAt(), used: image.LastUsedAt()})
	}
	return result, nil
}

func (s sdkStorage) removeImage(ctx context.Context, image cachedStorageImage) error {
	// Refuse a tag changed since enumeration. The SDK additionally freezes the
	// selected manifest inside its catalog transaction and holds file leases
	// through deletion. Non-force removal retains sandbox and snapshot roots.
	current, err := msb.Image.Get(ctx, image.reference)
	if err != nil {
		return err
	}
	if current.ManifestDigest() != image.digest || current.CreatedAt() != image.created || current.LastUsedAt() != image.used {
		return errImageChanged
	}
	return msb.Image.Remove(ctx, image.reference, false)
}

var errImageChanged = errors.New("cached image changed during cleanup")

func (c *Client) Storage(ctx context.Context) (StorageReport, error) {
	if c.storage == nil {
		return StorageReport{}, errors.New("native storage inventory is unavailable")
	}
	report, err := c.storage.usage(ctx)
	if err == nil && report.Usage == nil {
		err = errors.New("native storage inventory is unavailable")
	}
	return report, safeError("storage inventory", err)
}

// PruneStorage never removes durable checkpoints, named volumes or guest disks.
// SDK ownership locks arbitrate against live reads and publishers. Image removal
// also holds the shared admission ledger so prepared cold launches cannot lose
// their preloaded images between admission and dispatch.
func (c *Client) PruneStorage(ctx context.Context, request StoragePruneRequest) (StoragePruneReport, error) {
	report := StoragePruneReport{DryRun: request.DryRun, Images: []ImagePruneOutcome{}}
	if c.storage == nil || c.capacity == nil {
		return report, errors.New("native storage maintenance is unavailable")
	}
	if request.OlderThanSeconds > uint64((1<<63-1)/int64(time.Second)) || len(request.RetainImages) > 1024 {
		return report, invalid("storage retention exceeds supported limits")
	}
	for _, reference := range request.RetainImages {
		if strings.TrimSpace(reference) == "" || strings.ContainsRune(reference, 0) || len(reference) > 4096 {
			return report, invalid("retained image reference is invalid")
		}
	}
	if request.Images {
		err := c.capacity.transaction(ctx, func(state *capacityState) error {
			if len(state.Permits) != 0 {
				return errors.New("reconcile pending native reservations before image cleanup")
			}
			images, err := c.storage.images(ctx)
			if err != nil {
				return err
			}
			slices.SortFunc(images, func(a, b cachedStorageImage) int { return strings.Compare(a.reference, b.reference) })
			cutoff := time.Now().Add(-time.Duration(request.OlderThanSeconds) * time.Second)
			for _, image := range images {
				if err := ctx.Err(); err != nil {
					return err
				}
				state := "candidate"
				switch {
				case slices.Contains(request.RetainImages, image.reference):
					state = "retained"
				case image.created.IsZero() && image.used.IsZero() || image.digest == "":
					state = "unknown"
				case image.created.After(cutoff) || image.used.After(cutoff):
					state = "recent"
				case !request.DryRun:
					err := c.storage.removeImage(ctx, image)
					switch {
					case err == nil:
						state = "removed"
					case msb.IsKind(err, msb.ErrImageInUse):
						state = "in-use"
					case msb.IsKind(err, msb.ErrImageNotFound), errors.Is(err, errImageChanged):
						state = "changed"
					default:
						state = "failed"
					}
				}
				report.Images = append(report.Images, ImagePruneOutcome{Reference: image.reference, State: state})
			}
			return nil
		})
		if err != nil {
			return report, safeError("image cleanup", err)
		}
	}
	var err error
	report.Memory, err = c.storage.pruneMemory(ctx, msb.StoragePruneOptions{DryRun: request.DryRun, OlderThanSeconds: request.OlderThanSeconds})
	return report, safeError("memory cleanup", err)
}
