#!/usr/bin/env bash
# Builds the node VM's kernel: libkrunfw's arm64 guest kernel with the options
# in node.config, as a libkrunfw.5.dylib in a complete smolvm lib dir.
# Run "build.sh help" for the options.
set -euo pipefail

kernel_dir=$(cd "$(dirname "$0")" && pwd)
pods_dir=$(cd "$kernel_dir/../.." && pwd)
smolvm_src=${SMOLVM_SRC:-$(cd "$pods_dir/../../.." && pwd)/smolvm}
floci_dir=$smolvm_src/target/floci
out=${SMOLVM_PODS_KERNEL_DIR:-$floci_dir/kernel}
jobs=8

# libkrunfw at the commit that smolvm v1.30.1's libkrunfw.5.dylib was built from
# (lib/libkrun.provenance in smolvm), which is also smolvm's submodule commit.
libkrunfw_url=https://github.com/smol-machines/libkrunfw.git
libkrunfw_commit=8ba5fe2219b5c407ce1824d1c2dcf39adff84d8e
# The kernel that libkrunfw's Makefile names. The checksum is the one in
# kernel.org's signed sha256sums.asc.
kernel=linux-6.18.55
kernel_url=https://cdn.kernel.org/pub/linux/kernel/v6.x/$kernel.tar.xz
kernel_sha256=f410638061a165c12f42ab871d2f3fcd525515359b5faeee80969cff84524df9
# Debian 13 with the compiler and linker that built the release's kernel. The
# packages come from the Debian snapshot that the image was built from, and its
# InRelease files, which hold the checksums of every package, are pinned.
builder_image=debian@sha256:913f6706df59a68922d1dd08f78c2476560a8d367897200a6005b00e5f67c2d5
snapshot=20261005T000000Z
in_release="0584fba32e13e0ab8285fb16c27adea1ec03a73669c18702821094fd6ca86675  snapshot.debian.org_archive_debian_${snapshot}_dists_trixie_InRelease
ef1bab53128b164c03c968625d0f2e8841e3e754ff358ab010ccc2c7dbf6124a  snapshot.debian.org_archive_debian_${snapshot}_dists_trixie-updates_InRelease
1a25f10a8e266ba293823ac2c52325b7333e49dddbc2ef74de71f8d7c6d6b7f8  snapshot.debian.org_archive_debian-security_${snapshot}_dists_trixie-security_InRelease"
packages=(
    bc=1.07.1-4+b1
    binutils=2.44-3
    bison=2:3.8.2+dfsg-1+b2
    flex=2.6.4-8.2+b4
    gcc=4:14.2.0-1
    gcc-14=14.2.0-19
    libc6-dev=2.41-12+deb13u4
    libelf-dev=0.192-4
    make=4.4.1-2
    patch=2.8-2
    python3=3.13.5-1
    python3-pyelftools=0.32-1
    xz-utils=5.8.1-1+deb13u1
)
# The smolvm release that smolvm-floci uses. The new lib dir copies its
# libraries, all but libkrunfw, whose dylib targets this macOS version. The
# release's checksums.txt, which lists every library, is pinned.
release_dir=$floci_dir/release
release_checksums_sha256=09a865e28cf12637bcf798f080ac1300eb7a25a5256874e406b782fe825b45e4
release_libs=(libkrun.dylib libepoxy.0.dylib libvirglrenderer.1.dylib libMoltenVK.dylib)
macos_target=15.2

# Runs in the builder: /src is the libkrunfw checkout, /out the build dir.
# libkrunfw's Makefile unpacks and patches the kernel, resolves the config with
# olddefconfig, builds the Image and turns it into kernel.c.
container_script=$(
    cat <<'EOF'
cat >/etc/apt/sources.list.d/debian.sources <<SOURCES
Types: deb
URIs: http://snapshot.debian.org/archive/debian/$SNAPSHOT
Suites: trixie trixie-updates
Components: main
Signed-By: /usr/share/keyrings/debian-archive-keyring.pgp

Types: deb
URIs: http://snapshot.debian.org/archive/debian-security/$SNAPSHOT
Suites: trixie-security
Components: main
Signed-By: /usr/share/keyrings/debian-archive-keyring.pgp
SOURCES
apt-get -qq -o Acquire::Check-Valid-Until=false update
(cd /var/lib/apt/lists && sha256sum --check --strict --quiet) <<<"$IN_RELEASE"
apt-get -qq install --yes --no-install-recommends $PACKAGES >/dev/null
# The kernel tree needs a case-sensitive file system, which the Mac's is not.
cp -a /src /work
cd /work
make "$KERNEL"
while IFS= read -r line; do
    [ -z "$line" ] || grep -qxF -- "$line" "$KERNEL/.config" || {
        echo "olddefconfig did not keep $line" >&2
        exit 1
    }
done <<<"$FRAGMENT"
make -j"$JOBS" kernel.c
cp kernel.c "$KERNEL/arch/arm64/boot/Image" "$KERNEL/System.map" /out/
cp "$KERNEL/.config" /out/config
cp /out/config-libkrunfw_aarch64 "$KERNEL/stock.config"
make -C "$KERNEL" KCONFIG_CONFIG=stock.config olddefconfig >/dev/null
cp "$KERNEL/stock.config" /out/stock.config
{ gcc --version | sed -n 1p; ld --version | sed -n 1p; } >/out/toolchain
EOF
)

# Loads libkrun and creates a context, which loads libkrunfw.5.dylib by name as
# a VM start does but starts no VM. Then checks that the libkrunfw that libkrun
# loaded holds an arm64 kernel Image, prints its file and saves the Image.
check_source=$(
    cat <<'EOF'
#include <dlfcn.h>
#include <mach-o/dyld.h>
#include <stdio.h>
#include <string.h>

static const char *loaded(const char *leaf)
{
	for (uint32_t i = 0; i < _dyld_image_count(); i++) {
		const char *name = _dyld_get_image_name(i);
		const char *slash = strrchr(name, '/');
		if (strcmp(slash ? slash + 1 : name, leaf) == 0)
			return name;
	}
	return NULL;
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "usage: krunfw-check LIBKRUN OUT\n");
		return 2;
	}
	void *krun = dlopen(argv[1], RTLD_NOW | RTLD_LOCAL);
	int (*create_ctx)(void) = krun ? (int (*)(void))dlsym(krun, "krun_create_ctx") : NULL;
	if (!create_ctx) {
		fprintf(stderr, "%s\n", dlerror());
		return 1;
	}
	if (create_ctx() < 0) {
		fprintf(stderr, "krun_create_ctx failed\n");
		return 1;
	}
	const char *file = loaded("libkrunfw.5.dylib");
	void *lib = file ? dlopen(file, RTLD_NOW | RTLD_NOLOAD) : NULL;
	if (!lib) {
		fprintf(stderr, "libkrun did not load libkrunfw.5.dylib\n");
		return 1;
	}
	int (*version)(void) = (int (*)(void))dlsym(lib, "krunfw_get_version");
	char *(*kernel)(size_t *, size_t *, size_t *) =
		(char *(*)(size_t *, size_t *, size_t *))dlsym(lib, "krunfw_get_kernel");
	if (!version || !kernel) {
		fprintf(stderr, "%s\n", dlerror());
		return 1;
	}
	size_t load, entry, size;
	char *image = kernel(&load, &entry, &size);
	if (size < 64 || memcmp(image + 56, "ARM\x64", 4) != 0) {
		fprintf(stderr, "%s holds no arm64 kernel Image\n", file);
		return 1;
	}
	printf("libkrun: %s\nfile: %s\n", loaded("libkrun.dylib"), file);
	printf("abi: %d\nsize: %zu\n", version(), size);
	FILE *f = fopen(argv[2], "wb");
	if (!f || fwrite(image, 1, size, f) != size || fclose(f) != 0) {
		perror(argv[2]);
		return 1;
	}
	return 0;
}
EOF
)

usage() {
    cat <<EOF
Usage: build.sh [--stock | --config FILE] [--jobs N]

Builds libkrunfw's arm64 guest kernel with node.config in Docker, packages it as
libkrunfw.5.dylib and installs:

  OUT/lib                 smolvm's macOS libraries with this libkrunfw
  OUT/bin/smolvm-floci    smolvm-floci with SMOLVM_LIB_DIR set to OUT/lib
  OUT/build/build-info    The build's inputs and checksums

The build directory, OUT/build, also keeps the kernel Image, its resolved config,
libkrunfw's own config resolved the same way (stock.config) and the difference
between the two (config.diff).

Options:
  --stock     Build libkrunfw's own config into OUT/stock instead, install
              nothing, and check that the kernel is the one in the release's
              libkrunfw.5.dylib, byte for byte
  --config FILE
              Use the config fragment FILE instead of node.config
  --jobs N    Compile jobs and container CPUs (default: $jobs)

Environment:
  SMOLVM_SRC               smolvm checkout (default: $smolvm_src)
  SMOLVM_PODS_KERNEL_DIR   OUT above (default: SMOLVM_SRC/target/floci/kernel)
EOF
}

die() {
    printf 'build.sh: %s\n' "$*" >&2
    exit 1
}

step() {
    printf '==> %s\n' "$*" >&2
}

sha256() {
    shasum -a 256 "$1" | cut -d ' ' -f 1
}

stock=
fragment_file=$kernel_dir/node.config
while [ $# -gt 0 ]; do
    case $1 in
        --stock) stock=1 ;;
        --config)
            [ $# -ge 2 ] || die "--config needs a file"
            fragment_file=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
            [ -f "$fragment_file" ] || die "$2 is not a file"
            shift
            ;;
        --jobs)
            [ $# -ge 2 ] || die "--jobs needs a value"
            jobs=$2
            shift
            ;;
        help | -h | --help)
            usage
            exit 0
            ;;
        *) die "unknown argument $1, see build.sh help" ;;
    esac
    shift
done

for tool in cc curl docker git make shasum; do
    command -v "$tool" >/dev/null || die "$tool is not installed"
done
for path in "$release_dir/checksums.txt" "$floci_dir/bin/smolvm" "$floci_dir/agent-rootfs"; do
    [ -e "$path" ] || die "$path is missing; smolvm-floci must be built first"
done
[ "$(sha256 "$release_dir/checksums.txt")" = "$release_checksums_sha256" ] ||
    die "$release_dir/checksums.txt is not the pinned smolvm v1.30.1 release's"

container=floci-kernel-build-$$
trap 'docker rm --force "$container" >/dev/null 2>&1 || true' EXIT

tarball=$out/downloads/$kernel.tar.xz
if [ ! -f "$tarball" ] || [ "$(sha256 "$tarball")" != "$kernel_sha256" ]; then
    step "Downloading $kernel_url"
    mkdir -p "$out/downloads"
    curl --fail --location --silent --show-error --retry 5 --retry-delay 5 \
        --output "$tarball.part" "$kernel_url"
    [ "$(sha256 "$tarball.part")" = "$kernel_sha256" ] || die "$kernel_url does not have the pinned SHA-256"
    mv "$tarball.part" "$tarball"
fi

if [ -n "$stock" ]; then
    dest=$out/stock
else
    dest=$out/build
fi
step "Fetching libkrunfw $libkrunfw_commit into $dest"
rm -rf "$dest"
mkdir -p "$dest"
git clone --quiet "$libkrunfw_url" "$dest/libkrunfw"
git -C "$dest/libkrunfw" -c advice.detachedHead=false checkout --quiet "$libkrunfw_commit"
mkdir "$dest/libkrunfw/tarballs"
ln "$tarball" "$dest/libkrunfw/tarballs/$kernel.tar.xz"

config=$dest/libkrunfw/config-libkrunfw_aarch64
cp "$config" "$dest/"
fragment=
if [ -z "$stock" ]; then
    # As the kernel's scripts/kconfig/merge_config.sh does: replace the config's
    # lines for the fragment's symbols with the fragment's lines.
    fragment=$(grep -E '^(# )?CONFIG_' "$fragment_file")
    symbols=$(sed -E 's/^(# )?(CONFIG_[A-Za-z0-9_]+).*/\2/' <<<"$fragment" | paste -s -d '|' -)
    { grep -vE "^(# )?($symbols)( is not set|=)" "$config" && printf '%s\n' "$fragment"; } >"$config.new"
    mv "$config.new" "$config"
fi

step "Building the kernel in Docker, log in $dest/build.log"
docker run --rm --name "$container" --platform linux/arm64 --cpus "$jobs" --memory 6g \
    -v "$dest/libkrunfw:/src:ro" -v "$dest:/out" \
    -e "SNAPSHOT=$snapshot" -e "IN_RELEASE=$in_release" -e "PACKAGES=${packages[*]}" \
    -e "KERNEL=$kernel" -e "JOBS=$jobs" -e "FRAGMENT=$fragment" \
    "$builder_image" bash -euo pipefail -c "$container_script" >"$dest/build.log" 2>&1 || {
    tail -n 20 "$dest/build.log" >&2
    die "the kernel build failed, see $dest/build.log"
}
diff "$dest/stock.config" "$dest/config" >"$dest/config.diff" || [ $? -eq 1 ]

step "Packaging libkrunfw.5.dylib"
mv "$dest/kernel.c" "$dest/libkrunfw/"
MACOSX_DEPLOYMENT_TARGET=$macos_target make -C "$dest/libkrunfw" libkrunfw.5.dylib >>"$dest/build.log" 2>&1 || {
    tail -n 20 "$dest/build.log" >&2
    die "compiling kernel.c failed, see $dest/build.log"
}
mv "$dest/libkrunfw/libkrunfw.5.dylib" "$dest/"
rm "$dest/libkrunfw/kernel.c"

step "Checking libkrunfw.5.dylib"
printf '%s\n' "$check_source" >"$dest/krunfw-check.c"
cc -O2 -o "$dest/krunfw-check" "$dest/krunfw-check.c"
# check LIB_DIR OUT: with DYLD_LIBRARY_PATH set to LIB_DIR, as smolvm-floci
# sets it, libkrun (LIB_DIR's copy if it has one, else the release's) must load
# libkrunfw.5.dylib from LIB_DIR. Its Image goes to OUT.
check() {
    local result
    result=$(DYLD_LIBRARY_PATH=$1 "$dest/krunfw-check" "$release_dir/lib/libkrun.dylib" "$2")
    printf '%s\n' "$result" >&2
    [ "$(sed -n 's/^file: //p' <<<"$result")" = "$1/libkrunfw.5.dylib" ] ||
        die "libkrun did not load libkrunfw.5.dylib from $1"
}
check "$dest" "$dest/bundle"
# The banner that the kernel prints; init/version.o's copy has no build number.
banner=$(strings -a "$dest/bundle" | grep -E '^Linux version .* #[0-9]+ ') ||
    die "the kernel has no version banner"
printf '%s\n' "$banner" >&2
# bin2cbundle.py pads the Image with zeros to a multiple of 64 KiB.
cp "$dest/Image" "$dest/Image.padded"
head -c $(($(stat -f %z "$dest/bundle") - $(stat -f %z "$dest/Image"))) /dev/zero >>"$dest/Image.padded"
cmp -s "$dest/Image.padded" "$dest/bundle" || die "the dylib does not hold the Image that was built"
rm "$dest/Image.padded"
if [ -n "$stock" ]; then
    check "$release_dir/lib" "$dest/release-bundle"
    cmp -s "$dest/bundle" "$dest/release-bundle" || die "the kernel differs from the one in $release_dir/lib/libkrunfw.5.dylib"
    rm "$dest/release-bundle"
    step "The kernel is the one in $release_dir/lib/libkrunfw.5.dylib"
fi
rm "$dest/bundle"

{
    echo "libkrunfw $libkrunfw_url $libkrunfw_commit"
    echo "kernel $kernel_url $kernel_sha256"
    [ -n "$stock" ] || echo "fragment $(basename "$fragment_file") $(sha256 "$fragment_file")"
    echo "builder $builder_image, Debian snapshot $snapshot"
    sed 's/^/toolchain /' "$dest/toolchain"
    echo "dylib compiler $(cc --version | sed -n 1p), macOS $macos_target"
    echo "kernel $banner"
    for file in Image config libkrunfw.5.dylib; do
        echo "sha256 $(sha256 "$dest/$file") $file"
    done
} >"$dest/build-info"
[ -z "$stock" ] || exit 0

step "Installing $out/lib and $out/bin/smolvm-floci"
rm -rf "$out/lib.new"
mkdir "$out/lib.new"
for lib in "${release_libs[@]}"; do
    cp -p "$release_dir/lib/$lib" "$out/lib.new/"
    want=$(awk -v file="lib/$lib" '$2 == file { print $1 }' "$release_dir/checksums.txt")
    [ "$(sha256 "$out/lib.new/$lib")" = "$want" ] || die "$release_dir/lib/$lib does not match the release's checksums.txt"
done
cp -p "$dest/libkrunfw.5.dylib" "$out/lib.new/"
ln -s libkrunfw.5.dylib "$out/lib.new/libkrunfw.dylib"
check "$out/lib.new" /dev/null
rm -rf "${out:?}/lib"
mv "$out/lib.new" "$out/lib"

mkdir -p "$out/bin"
cat >"$out/bin/smolvm-floci" <<EOF
#!/bin/sh
export SMOLVM_AGENT_ROOTFS='$floci_dir/agent-rootfs'
export SMOLVM_LIB_DIR='$out/lib'
export DYLD_LIBRARY_PATH='$out/lib'
exec '$floci_dir/bin/smolvm' "\$@"
EOF
chmod +x "$out/bin/smolvm-floci"
step "Done. Config changes against libkrunfw's own config:"
cat "$dest/config.diff" >&2
