# Node kernel

The node VM's kernel: smolvm's guest kernel, libkrunfw's Linux 6.18.55, with transparent huge
pages and conntrack netlink, which the stock kernel lacks. In a nested pod VM, every stage-2 fault
goes through both hypervisors; with huge pages, the node's KVM can map pod VM memory in 2 MiB
blocks instead of 4 KiB pages. A pod VM's first touch of 1 GiB takes 0.9 s on this kernel, against
32 to 43 s on the stock kernel and 0.6 s in a VM directly on macOS. `build.sh` builds the kernel
and installs a smolvm lib dir that boots it.

## Source

smolvm-floci's macOS libraries, `target/floci/release/lib` in the smolvm checkout, are smolvm
v1.32.1's release. Its `libkrunfw.5.dylib` (SHA-256 `c9733ab2...`) is the Git LFS object of
`lib/libkrunfw.5.dylib` in smolvm, built in smolvm commit `3a4237d4` from
[smol-machines/libkrunfw](https://github.com/smol-machines/libkrunfw) at `8ba5fe22`, the commit
that smolvm's `lib/libkrun.provenance` and its `libkrunfw` submodule name.

libkrunfw's Makefile downloads Linux 6.18.55, applies `patches/0*.patch` in order, copies
`config-libkrunfw_aarch64` to `.config`, runs `make olddefconfig`, and builds the kernel with a
fixed build timestamp, user and host. `bin2cbundle.py` writes `arch/arm64/boot/Image` into
`kernel.c` as a C array, padded with zeros to a multiple of 64 KiB. On macOS, the Makefile compiles
`kernel.c` into `libkrunfw.5.dylib` with `cc -shared`; upstream makes `kernel.c` in a Linux VM
first (`build_on_krunvm.sh`). When a VM starts, libkrun opens `libkrunfw.5.dylib` by name and
boots the kernel that it holds, and smolvm puts `SMOLVM_LIB_DIR` first in `DYLD_LIBRARY_PATH`.

## Build

```sh
tools/smolvm-pods/node/kernel/build.sh
```

It needs Docker and the network. On the M4 Max it takes 3 to 5 minutes, plus the first download
of the 148 MiB kernel tarball. The kernel's inputs are pinned:

| Input | Pin |
| --- | --- |
| libkrunfw | Commit `8ba5fe22` |
| Linux 6.18.55 | SHA-256 of kernel.org's `linux-6.18.55.tar.xz` (see [Verification](#verification)) |
| Builder | `debian:trixie` by digest, with gcc 14.2.0-19 and binutils 2.44, which built the release's kernel. Packages come from Debian snapshot `20261005T000000Z`, with its InRelease files and the package versions pinned |
| Other macOS libraries | Copied from `target/floci/release/lib` and checked against its `checksums.txt`, whose SHA-256 is pinned (smolvm v1.32.1's) |

Not pinned: the Mac's `cc` and SDK, which compile `kernel.c` into the dylib, so the dylib's SHA-256
differs between Macs (see [Verification](#verification)), and the smolvm binary that
`bin/smolvm-floci` runs, the local build of smolvm-floci. The Image's SHA-256 in `build/build-info`
identifies the kernel.

`build.sh --config FILE` builds a kernel from another config fragment in place of `node.config`,
for example to test an option. Set `SMOLVM_PODS_KERNEL_DIR` so that it does not replace the node
kernel in `target/floci/kernel`.

The script clones libkrunfw, merges `node.config` into `config-libkrunfw_aarch64` as the kernel's
`scripts/kconfig/merge_config.sh` does, and runs libkrunfw's Makefile up to `kernel.c` in the
container. The build fails if `olddefconfig` drops a line of `node.config`. On the Mac, the same
Makefile compiles `kernel.c` into `libkrunfw.5.dylib`, for macOS 15.2 like the release's dylib.
The script then loads the dylib as a VM start does, with `DYLD_LIBRARY_PATH` set to its directory:
it opens `libkrun.dylib` and calls `krun_create_ctx`, which opens `libkrunfw.5.dylib` by name and
starts no VM. It checks that libkrun loaded the new dylib, that the dylib holds the Image that was
built, and that libkrun from the installed lib dir loads it too. It writes, in
`target/floci/kernel`:

| Path | Content |
| --- | --- |
| `lib/` | The new `libkrunfw.5.dylib`, the `libkrunfw.dylib` link, and copies of the release's other libraries |
| `bin/smolvm-floci` | smolvm-floci with `SMOLVM_LIB_DIR` and `DYLD_LIBRARY_PATH` set to `lib/` |
| `build/` | `build-info` (inputs and checksums), `Image`, its resolved `config`, libkrunfw's own config resolved the same way (`stock.config`), their difference (`config.diff`), `System.map`, `build.log`, the load check `krunfw-check`, and the libkrunfw checkout |
| `downloads/` | The kernel tarball |

`build.sh --stock` builds libkrunfw's own config into `stock/`, installs nothing, and checks that
the kernel is the one in the release's dylib, byte for byte.

## Options

`node.config` adds:

- `CONFIG_TRANSPARENT_HUGEPAGE=y` with the `always` policy (`CONFIG_TRANSPARENT_HUGEPAGE_ALWAYS`,
  also the Kconfig default). libkrun maps a cold-booted pod VM's RAM as anonymous memory whose
  host address agrees with its guest address modulo 2 MiB, so that KVM can map 2 MiB blocks
  wherever the host backs it with huge pages. On purpose, it does not call
  `madvise(MADV_HUGEPAGE)`: with the default `defrag=madvise`, an advised fault compacts memory
  synchronously, which made a guest's first touch of 3 GiB 4 to 7 times slower
  (`map_aligned_guest_ram` in `src/vmm/src/builder.rs` of libkrun `62059976`, which smolvm
  v1.32.1's Linux libraries were built from). So with `madvise`, cold-booted pod VMs would get no
  huge pages. The node's patched libkrun does advise a resumed VM's RAM, which it fills before any
  vCPU runs (see [libkrun](../libkrun/README.md)). With `always` and the default `defrag`, a fault
  takes a huge page only if one is free, without compaction, and otherwise falls back to 4 KiB
  pages, which khugepaged collapses later. The cost is memory: a process that touches part of a
  2 MiB range gets all of it, which includes k3s, containerd, and guests that touch memory
  sparsely. The build-time choice only sets the policy at boot, and `always` needs no boot-time
  setting in the node.
- `CONFIG_NF_CT_NETLINK=y`, for kube-proxy. It is one line: `NF_CONNTRACK` and `NETFILTER_NETLINK`
  are already on, and the options that depend on it also need NFQUEUE or NFLOG, which are off.

Besides these lines, the resolved config differs from libkrunfw's own only in what Kconfig derives
from them (`build/config.diff`): the THP menu at its defaults (`PERSISTENT_HUGE_ZERO_FOLIO`,
`READ_ONLY_THP_FOR_FS` and `NO_PAGE_MAPCOUNT` off); `MM_ID`, which THP selects; symbols with no
prompt in this config, `ARM64_CONTPTE` (the contiguous bit on user mappings of large folios),
`ARCH_WANT_PMD_MKWRITE`, `ARCH_ENABLE_THP_MIGRATION`, `ARCH_SUPPORTS_HUGE_PFNMAP` and
`ARCH_SUPPORTS_PMD_PFNMAP`; and `DEV_DAX`, which THP makes visible, off.

`TRANSPARENT_HUGEPAGE` also changes file memory, whatever the policy. Without it, the page cache
holds only 4 KiB folios. With it, ext4 keeps folios of up to 2 MiB: a buffered write gets folios as
large as the write, and readahead grows them. A mapping whose addresses agree with its file offsets
modulo 2 MiB maps a 2 MiB folio with one entry, unless it opts out with `MADV_NOHUGEPAGE`. An mmap
write to a large folio marks all of it dirty (`block_page_mkwrite`), so a pod VM whose RAM is a
shared mapping of its checkpoint file may write back more than the pages it changed.

## Use

A VM boots the kernel in the lib dir of the smolvm process that starts it, on every start. The node
runs on this kernel: `node-vm.sh` creates and starts it with `target/floci/kernel/bin/smolvm-floci`
by default, the wrapper that `build.sh` writes. It runs `target/floci/bin/smolvm` with
`SMOLVM_AGENT_ROOTFS` set to `target/floci/agent-rootfs`, and `SMOLVM_LIB_DIR` and
`DYLD_LIBRARY_PATH` set to `target/floci/kernel/lib`. `node-vm.sh build` records the Image's
SHA-256 in the image's `/etc/floci-node/build-info` (`node-kernel`). `node-vm.sh start` warns, and
the end-to-end tests fail, when the node's THP policy is not `always`.

To run a node on the stock kernel, point `SMOLVM` at the same wrapper with the release's lib dir,
`target/floci/release/lib`. From the floci repository:

```sh
floci="${SMOLVM_SRC:-$PWD/../smolvm}"/target/floci
cat >"$floci/bin/smolvm-floci" <<EOF
#!/bin/sh
export SMOLVM_AGENT_ROOTFS='$floci/agent-rootfs'
export SMOLVM_LIB_DIR='$floci/release/lib'
export DYLD_LIBRARY_PATH='$floci/release/lib'
exec '$floci/bin/smolvm' "\$@"
EOF
chmod +x "$floci/bin/smolvm-floci"
export SMOLVM=$floci/bin/smolvm-floci
```

The wrapper sets `SMOLVM_LIB_DIR` itself, so setting it before calling the wrapper has no effect.
`uname -a` in the node is the same for both kernels; only this one has
`/sys/kernel/mm/transparent_hugepage`, whose `enabled` reads `[always] madvise never`.

The policy can be changed at runtime: `echo never > /sys/kernel/mm/transparent_hugepage/enabled`
before a pod VM starts gives its anonymous RAM 4 KiB pages on the same kernel. It does not change
file memory (see [Options](#options)); to compare file memory with 4 KiB folios, run the node on
the stock kernel instead. In a pod VM process's `/proc/PID/smaps_rollup`, `AnonHugePages` shows
how much of its anonymous memory has huge pages and `FilePmdMapped` how much of its file mappings
is mapped in 2 MiB entries. `thp_fault_alloc` and `thp_fault_fallback` in `/proc/vmstat` count the
anonymous faults that got a huge page and those that did not.

Pod VMs boot the pod kernel of `node/podkernel/build.sh`, from the node image: libkrunfw's own
config with the netfilter options of `pod.config`. The policy covers anonymous memory,
such as a pod VM's RAM after a cold boot. With the release's libkrun, a resumed VM's RAM is a
mapping of the checkpoint file, which the policy does not cover: it gets 2 MiB mappings only where
the page cache holds 2 MiB folios, so a resumed pod VM may get few huge pages or none. The node's
patched libkrun reads the checkpoint into anonymous memory instead (see
[libkrun](../libkrun/README.md)), and a resumed VM's RAM is then in huge pages like a cold-booted
VM's.

## Verification

**The kernel tarball.** kernel.org's `sha256sums.asc` lists the pinned SHA-256 and is signed by
the Kernel.org checksum autosigner (`B8868C80BA62A1FFFAF5FDA9632D3A06589DA6B1`), and
`linux-6.18.55.tar.sign` is signed by Greg Kroah-Hartman
(`647F28654894E3BD457199BE38DBBDC86092693E`). Check both with GnuPG and the keys from kernel.org's
WKD. The tar names commit `725bd2f3`, to which tag `v6.18.55` points in the stable tree, so it
holds the same tree as libkrunfw's own download, a git.kernel.org snapshot of that tag.

**The stock kernel.** `build.sh --stock` rebuilds the release's kernel byte for byte: it reads the
new kernel and the release's out of their dylibs through libkrun and compares them. Its dylib
differs from the release's only in its Mach-O header, code and signature, which the Mac's compiler
and SDK make, and not in the kernel.

**The node kernel.** `build.sh` fails unless `olddefconfig` keeps every line of `node.config`,
libkrun from `lib/` with `bin/smolvm-floci`'s environment loads `lib/libkrunfw.5.dylib`, the
dylib holds the Image that was built, and the other libraries in `lib/` match the release's
`checksums.txt`. The build is reproducible: builds from scratch give the same Image and config,
and on one Mac the same dylib (the SHA-256 in `build/build-info` and `stock/build-info`). For
this `node.config`:

| File | SHA-256 |
| --- | --- |
| `Image` | `2dea56fd8a6a130efe51ada8772b25f9bc1a0a54bf636c08c85f5af37c154ea8` |
| `config` | `e7f6984e12eefff87b0442fc2c02a9e1ce3d6bc7b3a651f9374a9eb908d2f247` |

The kernel's banner, the `kernel Linux version ...` line of `build-info`, is the release's:
`Linux version 6.18.55 (root@libkrunfw) (gcc (Debian 14.2.0-19) 14.2.0, GNU ld (GNU Binutils for
Debian) 2.44) #1 SMP Mon Jun  1 16:28:39 CEST 2026`. The Image has THP's boot parameters and sysfs
and vmstat names (`transparent_hugepage=`, `shmem_enabled`, `hpage_pmd_size`, `thp_fault_alloc`)
and ctnetlink's messages, which the release's Image lacks, and its `System.map` has `khugepaged`,
`hugepage_init` and `ctnetlink_init`. `bin/smolvm-floci --version` prints `smolvm 1.32.1`.
