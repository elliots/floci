# Guest filesystem utility

`cmd/guestfs` is a static Linux utility with no resident service. The pod controller
installs it lazily when configured through `--guest-fs-binary`.

`ownership` applies Kubernetes filesystem-group ownership: writable files gain
`0660`, read-only files gain `0440`, and directories also gain group traversal
and setgid. Existing mode bits remain intact. Symlinks receive link ownership,
without following the target. The volume root is processed last. Callers must
exclude workload writers during initial ownership changes.

`runc` wraps the pinned containerd runtime invocation, preserving all arguments.
It changes only OCI supplementary groups before `create` and `exec`, then replaces
itself with the real runtime. A private per-container record keeps exec/probes
consistent with startup. Image account files are resolved within the image root,
and Merge/Strict follow Kubernetes policies. The wrapper does not replace image
pulls, snapshots, cgroups, networking, process supervision or container lifecycle.

Run `go test -race ./guestfs ./cmd/guestfs` and `go vet ./guestfs ./cmd/guestfs`.
Ownership tests require Linux because filesystem setgid behavior differs on
other hosts. The pod qualification shares its behavioral assertions between
Docker k3s and native Pods.
