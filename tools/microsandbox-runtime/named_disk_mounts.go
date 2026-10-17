package microsandboxruntime

import (
	"context"
	"errors"
	"path"
	"strings"

	msb "github.com/superradcompany/microsandbox/sdk/go"
)

var errNamedDiskPreflight = errors.New("named disk could not be verified before native dispatch")

// prepare and dispatch call this under the shared capacity lock. The resulting
// pending permit keeps ReleaseNamedDisk from changing the lease after validation.
func (c *Client) namedDiskAdmissionCheck(ctx context.Context, request command) func() error {
	return func() error {
		if len(request.config.Volumes) == 0 {
			return nil
		}
		if err := c.core.initialize(); err != nil {
			return errors.Join(errNamedDiskPreflight, err)
		}
		if err := c.validateNamedDiskMountsLocked(ctx, request); err != nil {
			return errors.Join(errNamedDiskPreflight, err)
		}
		return nil
	}
}

func parseNamedDiskMount(config *msb.SandboxConfig, value string) error {
	parts := strings.Split(value, ":")
	if len(parts) != 3 || !namedDiskName.MatchString(parts[0]) || !path.IsAbs(parts[1]) ||
		path.Clean(parts[1]) != parts[1] || parts[1] == "/" || strings.ContainsAny(parts[1], ",\r\n\t\"") {
		return invalid("named disk mount requires NAME:/guest/path:kind=disk,size=SIZE")
	}
	var size uint32
	var kind, readonly bool
	seen := map[string]bool{}
	for option := range strings.SplitSeq(parts[2], ",") {
		key, value, _ := strings.Cut(option, "=")
		if seen[key] {
			return invalid("duplicate named disk mount option")
		}
		seen[key] = true
		switch key {
		case "kind":
			kind = value == "disk"
		case "size":
			var err error
			size, err = sizeMiB(value)
			if err != nil {
				return err
			}
		case "ro":
			if option != "ro" {
				return invalid("read-only named mount uses ro")
			}
			readonly = true
		default:
			return invalid("unsupported named disk mount option")
		}
	}
	if !kind || size < 256 {
		return invalid("named disk mount requires kind=disk and size of at least 256MiB")
	}
	for guestPath, mount := range config.Volumes {
		if mount.Named == parts[0] || guestPath == parts[1] || strings.HasPrefix(guestPath, parts[1]+"/") || strings.HasPrefix(parts[1], guestPath+"/") {
			return invalid("named disks must have unique, non-overlapping guest mounts")
		}
	}
	if config.Volumes == nil {
		config.Volumes = map[string]msb.MountConfig{}
	}
	config.Volumes[parts[1]] = msb.Mount.NamedWith(parts[0], msb.MountOptions{Readonly: readonly},
		msb.NamedVolumeOptions{Mode: "existing", Kind: "disk", SizeMiB: size})
	return nil
}

func (c *Client) validateNamedDiskMounts(ctx context.Context, request command) error {
	if err := c.core.initialize(); err != nil {
		return err
	}
	return c.capacity.transaction(ctx, func(_ *capacityState) error {
		return c.validateNamedDiskMountsLocked(ctx, request)
	})
}

func (c *Client) validateNamedDiskMountsLocked(ctx context.Context, request command) error {
	for _, mount := range request.config.Volumes {
		record, err := c.readNamedDisk(mount.Named)
		if err != nil || record.Phase != "ready" || record.Identity == nil || record.Attachment == nil ||
			record.Attachment.VMName != request.name || record.Identity.CapacityMiB != mount.SizeMiB {
			return errors.Join(errors.New("named disk mount has no matching durable Pod attachment"), err)
		}
		actual, err := c.disks.inspect(ctx, mount.Named)
		if err != nil || !record.Identity.Matches(actual) {
			return errors.Join(errors.New("attached named disk identity changed before launch or restore"), err)
		}
	}
	return nil
}
