# libkrun patches for the node

`libkrun-anonymous-restore.patch` and `libkrun-readonly-input.patch` apply, in that order, to
[smol-machines/libkrun](https://github.com/smol-machines/libkrun) at
`e4ed7f836c6116ad7b9e14d9fc25ab608410e2cb`, the libkrun of the smolvm v1.30.1 Linux release
that runs pod VMs inside the node. The node image ships the patched library in place of the
release's `lib/libkrun.so`, pinned by its SHA-256 (`LIBKRUN_SHA256` in `../Dockerfile`).

## Anonymous restore

On a stock resume, libkrun maps a pod VM's guest RAM as a shared mapping of the checkpoint
file `memory.bin`. That mapping gets no transparent huge pages, and in a nested VM every 4 KiB
page is slow to map, so memory stays slow after a resume.

`libkrun-anonymous-restore.patch` changes only the Linux restore path that is neither forkable
nor layered. It maps guest RAM as new anonymous memory, 2 MiB aligned, with `MADV_HUGEPAGE`, and
reads `memory.bin` into it with parallel `pread` calls before any vCPU runs. Holes in the file
are skipped. The restored RAM then has the same shape as a cold-booted VM's, so the next pause
saves it the same way. A unit test checks the copy is exact, has no file backing and is aligned.

Measured in a nested node on the Mac with [the huge-page node kernel](../kernel/README.md), with
this patch alone. Each of these resumes also unpacked a packed checkpoint and made smolvm's private
copy of the RAM image, about 1 s that the node does not spend: it pauses to an unpacked
[local checkpoint](../../docs/architecture.md#pause) and has both patches. With them, the
[timings](../../docs/architecture.md#wake) are 0.21 s from the start of a resume to PetClinic's
first answer, and a warm request median of 4.0 ms.

| After a resume | Stock libkrun | Patched |
| --- | --- | --- |
| Read 1 GiB | 7.66 s | 0.43 s |
| Random 8-byte read | 137 µs | 9.6 ns |
| PetClinic, resume start to first answer | 4.55 s | 1.30 s |
| PetClinic, warm request median | 1,369 ms | 5.1 ms |

The copy of 1.19 GB takes 22 to 32 ms, and memory hashes match after every resume.

Cost: a resumed pod's memory is resident at once (about 780 MiB for PetClinic right after a
resume, against about 340 MiB lazily with the stock library).

## Read-only RAM input

With `libkrun-anonymous-restore.patch`, the restore only reads `memory.bin`.
`libkrun-readonly-input.patch` makes it accept a read-only file and exports
`krun_restore_reads_memory`. smolvm checks for that function and then skips its private copy and
fsync of the RAM image, which takes 0.3 to 0.7 s per resume. With any other libkrun, smolvm makes
the copy. The smolvm side is in the smolvm commit that `scripts/build.sh` pins.

## Build

```sh
tools/smolvm-pods/node/libkrun/build.sh [--test]
```

It fetches libkrun at the pinned commit, applies both patches and builds
`cargo build --locked --release -p libkrun --features blk,net,input,gpu` with partial RELRO, as the
release does, in the `rust:1.92-bookworm` image and with Rust 1.99.1 of `scripts/rust.env`, sharing
the caches of `scripts/build.sh` in the smolvm checkout. Debian packages (clang, patchelf, the
virglrenderer headers) come from the node image's Debian snapshot (`../apt`), at pinned versions. It
then removes `libvirglrenderer.so.1` from the needed libraries with `patchelf`, as the release does,
checks the SONAME and the exported functions, and writes `libkrun.so`, with `build-info` beside it,
to `target/libkrun`, or to the directory that `SMOLVM_PODS_LIBKRUN_DIR` names. `node-vm.sh build`
takes `target/libkrun/libkrun.so` whatever that variable says, so with another directory, name the
result with `node-vm.sh build --libkrun PATH`. `--test` also runs the unit tests of the patches. It
takes about 3 minutes on the M4 Max, plus the first download of the crates and the musl target.

The result is `5b526a62810fce52436637f8d27387212a1c0950a05cb5eac0f81e1006b6a5ac`: 8.6 MB, not
stripped, `NEEDED` only `libgcc_s.so.1` and `libc.so.6`, highest glibc symbol `GLIBC_2.34` (the
release links glibc 2.31 with cargo-zigbuild; this links Debian 12's glibc 2.36, the node's own),
and the release's 69 `krun_*` functions plus `krun_restore_reads_memory`. The build is
reproducible: a build from scratch gives the same bytes.

## Upstreaming

The patches are not upstream. To go upstream they need:

- a setting that chooses this eager restore or the stock lazy mapping, instead of replacing the
  mapping;
- reading the checkpoint without filling the page cache;
- a huge-page policy that also works when the host's memory is full;
- an integration test of restore, run, pause and restore with a memory hash check.
