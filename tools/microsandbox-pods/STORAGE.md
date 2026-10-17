# Native local persistent volumes

The shared Pod controller can provision local ext4 disks through standard
Kubernetes PVCs and PVs. This is an opt-in local storage backend, not EBS or a CSI
driver. The disk belongs to the controller's runtime home and node. Keep both
the runtime home and controller state directory across restarts. PVC Pod journals
use format 7, which older controllers reject before destructive cleanup.

Create an explicit StorageClass; ordinary workloads keep their existing storage
classes:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: floci-native
provisioner: microsandbox.dev/local
volumeBindingMode: WaitForFirstConsumer
reclaimPolicy: Delete
allowVolumeExpansion: false
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: application-data
spec:
  storageClassName: floci-native
  accessModes: [ReadWriteOnce]
  volumeMode: Filesystem
  resources:
    requests:
      storage: 1Gi
```

Select the native node on the consuming Pod using the normal native runtime
selector and toleration. Mount the claim as an ordinary volume:

```yaml
spec:
  nodeSelector:
    microsandbox.dev/runtime: native
  tolerations:
    - key: microsandbox.dev/runtime
      operator: Equal
      value: native
      effect: NoSchedule
  volumes:
    - name: data
      persistentVolumeClaim:
        claimName: application-data
  containers:
    - name: app
      image: your-application-image
      volumeMounts:
        - name: data
          mountPath: /data
```

`WaitForFirstConsumer` lets Kubernetes select the node before the shared
controller creates the disk. The PV records that node affinity. Binding allocates
persistent storage even if proxy availability keeps the application cold; it
does not start a VM or allocate guest RAM. Requests round up to whole MiB with a
256 MiB minimum, and the PV reports the actual capacity. A storage limit smaller
than that capacity is rejected. Disk creation uses the same native disk-admission
budget as Pod roots, checkpoints and Aurora.

## Attachment and hibernation

One Pod UID can hold an attachment at a time. This is stricter than ordinary
`ReadWriteOnce`, which can permit several Pods on the same node. The restriction
also applies to read-only mounts. Containers within the same Pod can share its
volume. A cold Pod takes its lease only when it first launches.

`fsGroup` ownership is applied before init and application containers start.
Persistent volumes honor `fsGroupChangePolicy`, including `OnRootMismatch`;
temporary and projected volumes keep their Kubernetes-specific ownership rules.

A sleeping Pod retains its attachment because its RAM checkpoint can still
resume with that disk mounted. Stopped or completed Pods also keep their lease
until deletion. A second Pod waits until the old Pod's VM and all restorable
checkpoints are removed. Deleting only the Kubernetes object does not
prove that native cleanup has finished. A single-replica Deployment sharing one
claim should use `strategy.type: Recreate` so a rolling surge does not wait on
the old Pod's attachment. StatefulSets can keep their normal per-replica claims.

Pod replacement keeps data in the same bound PVC and starts fresh application
processes. Hibernation preserves both application memory and the attached disk.
Recovery verifies the PVC/PV UIDs, disk owner, creation metadata, geometry and
backing file identity before attachment. It refuses missing or replaced storage
instead of creating an empty replacement for an existing Pod.

## Reclaim and retained data

`Delete` removes the owned disk after the PVC is released and no native Pod or
checkpoint holds its attachment. The PV finalizer remains while cleanup is
blocked. `Retain` preserves the disk and PV for deliberate reuse. Kubernetes
administrators can manually rebind the same PV to another claim; the controller
requires a completed binding to the new claim UID and retains the original disk
identity. There is no automatic import of unknown disks or cross-cluster
adoption. Deleting a retained PV object keeps its disk but removes that normal
rebinding path, so retain the PV metadata until recovery is complete.

Use `kubectl describe pvc`, `kubectl describe pv` and Pod events to diagnose
pending allocation or attachment. Run `manage.py status` for runtime capacity and
uncertain native operations. Do not delete ownership journals, disk files or
checkpoints to bypass an attachment: a stopped guest can still have a valid RAM
restore point. Back up application data independently of hibernation.

## Current limits

Supported claims use `Filesystem` and exactly `ReadWriteOnce`. Expansion, raw
block devices, RWX/ROX/RWOP, cloning, volume data sources, custom mount options and
CSI drivers are unsupported. The StorageClass accepts no parameters; optional
topology constraints use `kubernetes.io/hostname`. Disk sizes are immutable after
creation. The separate `microsandbox.dev/root-disk-size` annotation sizes a Pod's
root disk and does not resize its PVCs.

Build the controller and Linux `guestfs` binary together. Local PVCs require
`--guest-fs-binary` even when no `fsGroup` is requested: the helper also verifies
filesystem writeback before checkpointing and destructive VM operations. It runs
on demand and exits. A failed or uncertain writeback keeps the guest and its
attachment fenced instead of discarding potentially buffered application data.

The managed controller role needs PVC and StorageClass reads plus PV management
permissions. Rerun `manage.py prepare` with the controller stopped to refresh the
generated role before starting an upgraded controller. Existing custom controller
roles need the same permissions. No per-Pod storage daemon or sidecar is added.
