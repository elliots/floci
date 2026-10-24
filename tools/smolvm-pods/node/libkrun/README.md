# The node's libkrun

The node runs pod VMs with smolvm's libkrun patched for smolvm-pods. The patches live in the smolvm
checkout, in `patches/libkrun`, beside the smolvm code that needs them; its `README.md` describes
each one. They apply to the commit of the checkout's `libkrun` submodule, the libkrun of the
smolvm v1.32.1 Linux release. The node image ships the patched library in place of the release's
`lib/libkrun.so`, pinned by its SHA-256 (`LIBKRUN_SHA256` in `../Dockerfile`).

## Build

```sh
tools/smolvm-pods/node/libkrun/build.sh [--test]
```

It fetches libkrun at the pinned commit, applies the patches and builds
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

The result is `a8140dd81e405fb2bd279b47429fd4d125de6d5ddd46664a9941b244a234cea3`: 8.6 MB, not
stripped, `NEEDED` only `libgcc_s.so.1` and `libc.so.6`, highest glibc symbol `GLIBC_2.34` (the
release links glibc 2.31 with cargo-zigbuild; this links Debian 12's glibc 2.36, the node's own),
and the release's 69 `krun_*` functions plus `krun_restore_reads_memory`. The build is
reproducible: a build from scratch gives the same bytes.
