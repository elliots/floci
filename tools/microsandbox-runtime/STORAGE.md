# Native storage maintenance

Both authenticated host tools expose `GET /v1/storage` and
`POST /v1/storage/prune`. These are local runtime management operations, separate
from AWS service APIs. The Pod controller also maps namespace, Pod UID, VM name
and immutable checkpoint IDs to the detailed runtime item names. Busy lifecycle
operations are counted in `busyWorkloads` and omitted from that mapping.

The inventory includes image caches, guest disks, checkpoints, named volumes and
runtime RAM backing files, even when disk admission limits are disabled. Unknown
sizes remain `null`. Logical bytes are file lengths, allocated bytes can count
shared copy-on-write blocks repeatedly, and neither is exclusive physical disk
ownership. Runtime SDK notes describe accounting exclusions, including host bind
mounts. Named PVCs remain independently accounted objects, not part of every
Pod's root disk total.

Cleanup defaults to a preview with a 24-hour minimum age:

```json
{"dryRun":true,"olderThanSeconds":86400,"images":false,"retainImages":[]}
```

Set `dryRun` to `false` to apply. Memory cleanup uses the SDK's locked eligibility
checks for unused runtime RAM backing. It never removes durable checkpoints,
guest disks or named volumes. Sleeping workload data is retained irrespective of
age. Preview results do not guarantee that a later application removes the same
files; ownership and files may change between calls.

Image cleanup is separately opt-in with `images: true`. It considers cached
references older than the chosen age, skips unknown timestamps and never uses
force removal. References in `retainImages` must match the canonical reference
reported by the SDK image list. The Pod endpoint always retains its pinned k3s
guest base so cold Pods remain startable. When using the host endpoint, retain
any preloaded images needed by workloads which have not created a guest yet,
particularly when disk admission requires images to be preloaded.

The shared admission ledger rejects image cleanup while any native reservation
is outstanding. SDK catalog transactions and file leases protect running and
stopped guests, durable snapshots, image readers and publishers. Age checks are
observations, not an exclusion lock against tools outside Floci retagging or
reading an image. Image previews report age-qualified `candidate` references;
only application establishes whether they are in use. Outcomes distinguish
`removed`, `in-use`, `retained`, `recent`, `unknown`, `changed` and `failed`.
Check every outcome even when the HTTP request succeeds. Failed cleanup may have
completed some removals; inspect the returned report before retrying.

No checkpoint-age eviction or blanket directory deletion is provided. The latest
checkpoint is application state. Deleting the workload through its existing
owner performs its normal checkpoint and volume lifecycle. Disk admission bounds
new allocations; it is not a filesystem quota and does not automatically evict
active workloads or application state.

The native storage test qualifies preview and applied cleanup against a stopped
guest and full RAM checkpoint, verifies the restored in-memory marker, then
checks that the unused fixture image can be removed after its owners are deleted.
Run `TestNativeStorageCleanupPreservesRAMAndImageDependencies` with the `native`
build tag and explicit `FLOCI_NATIVE_TEST_MSB`, `FLOCI_NATIVE_TEST_HOME` and
`FLOCI_NATIVE_TEST_IMAGE` in a dedicated runtime home. The fixture image is
removed on success. Other initial cached references are retained.
