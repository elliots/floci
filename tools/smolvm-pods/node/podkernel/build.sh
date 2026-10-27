#!/usr/bin/env bash
# Builds the pod VMs' kernel: libkrunfw's arm64 guest kernel with the options in pod.config, as the Linux libkrunfw.so
# that the node image installs in place of the smolvm release's. Run "build.sh help" for the options.
#
# The node VM's own kernel is another build (node/kernel/build.sh), a macOS dylib. A pod VM is a KVM guest inside the node,
# and boots the libkrunfw.so of the smolvm Linux release in the node image.
set -euo pipefail

podkernel_dir=$(cd "$(dirname "$0")" && pwd)
pods_dir=$(cd "$podkernel_dir/../.." && pwd)
smolvm_src=${SMOLVM_SRC:-$(cd "$pods_dir/../../.." && pwd)/smolvm}
out=${SMOLVM_PODS_POD_KERNEL_DIR:-$pods_dir/target/podkernel}
jobs=8

# The pins below are those of node/kernel/build.sh: the same libkrunfw, kernel and builder. Change them together.
libkrunfw_url=https://github.com/smol-machines/libkrunfw.git
libkrunfw_commit=8ba5fe2219b5c407ce1824d1c2dcf39adff84d8e
kernel=linux-6.18.55
kernel_url=https://cdn.kernel.org/pub/linux/kernel/v6.x/$kernel.tar.xz
kernel_sha256=f410638061a165c12f42ab871d2f3fcd525515359b5faeee80969cff84524df9
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
# What libkrunfw's Makefile names the Linux library, and the SHA-256 of the one in smolvm v1.32.1's Linux release
# (lib/libkrunfw.so.5.5.0 in its checksums.txt), which --stock must reproduce.
library=libkrunfw.so.5.5.0
release_library_sha256=e3d72d736794eb2982399b5e92c81c07fca68077246584327eb16f3e6868a5d2

# Runs in the builder: /src is the libkrunfw checkout, /out the build dir. On Linux libkrunfw's Makefile does it all: it
# unpacks and patches the kernel, resolves the config with olddefconfig, builds the Image, bundles it as kernel.c, compiles
# the shared library and strips it.
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
cp -a /src /work
cd /work
make "$KERNEL"
while IFS= read -r line; do
    [ -z "$line" ] || grep -qxF -- "$line" "$KERNEL/.config" || {
        echo "olddefconfig did not keep $line" >&2
        exit 1
    }
done <<<"$FRAGMENT"
make -j"$JOBS" "$LIBRARY"
# It must be an arm64 ELF shared object that holds the kernel libkrun asks for.
readelf -h "$LIBRARY" | grep -q 'Machine: *AArch64'
readelf -h "$LIBRARY" | grep -q 'Type: *DYN'
nm -D --defined-only "$LIBRARY" | grep -q ' krunfw_get_kernel$'
cp "$LIBRARY" "$KERNEL/arch/arm64/boot/Image" "$KERNEL/System.map" /out/
cp "$KERNEL/.config" /out/config
cp /out/config-libkrunfw_aarch64 "$KERNEL/stock.config"
make -C "$KERNEL" KCONFIG_CONFIG=stock.config olddefconfig >/dev/null
cp "$KERNEL/stock.config" /out/stock.config
{ gcc --version | sed -n 1p; ld --version | sed -n 1p; } >/out/toolchain
EOF
)

usage() {
    cat <<EOF
Usage: build.sh [--stock | --config FILE] [--jobs N]

Builds libkrunfw's arm64 guest kernel with pod.config in Docker and packages it as the Linux $library:

  OUT/$library      the library, for node-vm.sh build
  OUT/build-info          The build's inputs and checksums
  OUT/config.diff         The difference from libkrunfw's own config

Options:
  --stock     Build libkrunfw's own config into OUT/stock instead, install nothing, and check that the library is the one
              in the smolvm release, byte for byte (it shows the builder is the release's)
  --config FILE
              Use the config fragment FILE instead of pod.config
  --jobs N    Compile jobs and container CPUs (default: $jobs)

Environment:
  SMOLVM_SRC                  smolvm checkout (default: $smolvm_src); its target/floci/kernel/downloads caches the
                              kernel tarball that node/kernel/build.sh downloads
  SMOLVM_PODS_POD_KERNEL_DIR  OUT above (default: $out)
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
fragment_file=$podkernel_dir/pod.config
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

for tool in curl docker git shasum; do
    command -v "$tool" >/dev/null || die "$tool is not installed"
done

container=floci-podkernel-build-$$
trap 'docker rm --force "$container" >/dev/null 2>&1 || true' EXIT

# The kernel tarball: the one node/kernel/build.sh keeps, or a download of our own.
tarball=$smolvm_src/target/floci/kernel/downloads/$kernel.tar.xz
if [ ! -f "$tarball" ] || [ "$(sha256 "$tarball")" != "$kernel_sha256" ]; then
    tarball=$out/downloads/$kernel.tar.xz
    if [ ! -f "$tarball" ] || [ "$(sha256 "$tarball")" != "$kernel_sha256" ]; then
        step "Downloading $kernel_url"
        mkdir -p "$out/downloads"
        curl --fail --location --silent --show-error --retry 5 --retry-delay 5 \
            --output "$tarball.part" "$kernel_url"
        [ "$(sha256 "$tarball.part")" = "$kernel_sha256" ] || die "$kernel_url does not have the pinned SHA-256"
        mv "$tarball.part" "$tarball"
    fi
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
cp "$tarball" "$dest/libkrunfw/tarballs/$kernel.tar.xz"

config=$dest/libkrunfw/config-libkrunfw_aarch64
cp "$config" "$dest/"
fragment=
if [ -z "$stock" ]; then
    # As the kernel's scripts/kconfig/merge_config.sh does: replace the config's lines for the fragment's symbols with the
    # fragment's lines.
    fragment=$(grep -E '^(# )?CONFIG_' "$fragment_file")
    symbols=$(sed -E 's/^(# )?(CONFIG_[A-Za-z0-9_]+).*/\2/' <<<"$fragment" | paste -s -d '|' -)
    { grep -vE "^(# )?($symbols)( is not set|=)" "$config" && printf '%s\n' "$fragment"; } >"$config.new"
    mv "$config.new" "$config"
fi

step "Building the kernel and $library in Docker, log in $dest/build.log (3 to 10 minutes)"
docker run --rm --name "$container" --platform linux/arm64 --cpus "$jobs" --memory 6g \
    -v "$dest/libkrunfw:/src:ro" -v "$dest:/out" \
    -e "SNAPSHOT=$snapshot" -e "IN_RELEASE=$in_release" -e "PACKAGES=${packages[*]}" \
    -e "KERNEL=$kernel" -e "JOBS=$jobs" -e "FRAGMENT=$fragment" -e "LIBRARY=$library" \
    "$builder_image" bash -euo pipefail -c "$container_script" >"$dest/build.log" 2>&1 || {
    tail -n 20 "$dest/build.log" >&2
    die "the kernel build failed, see $dest/build.log"
}
diff "$dest/stock.config" "$dest/config" >"$dest/config.diff" || [ $? -eq 1 ]
built=$(sha256 "$dest/$library")

if [ -n "$stock" ]; then
    [ "$built" = "$release_library_sha256" ] ||
        die "the stock build's $library is $built, not the release's $release_library_sha256: the builder is not the release's"
    step "The stock build is the release's $library, byte for byte"
    exit 0
fi

# What the build must have changed: the fragment's options, and nothing else of consequence.
step "Config changes against libkrunfw's own config:"
cat "$dest/config.diff" >&2

{
    echo "libkrunfw $libkrunfw_url $libkrunfw_commit"
    echo "kernel $kernel_url $kernel_sha256"
    echo "fragment $(basename "$fragment_file") $(sha256 "$fragment_file")"
    echo "builder $builder_image, Debian snapshot $snapshot"
    sed 's/^/toolchain /' "$dest/toolchain"
    echo "banner $(strings -a "$dest/Image" | grep -E '^Linux version .* #[0-9]+ ' | head -n 1)"
    for file in Image config "$library"; do
        echo "sha256 $(sha256 "$dest/$file") $file"
    done
} >"$dest/build-info"

step "Installing $out/$library"
cp "$dest/$library" "$out/$library.new"
mv "$out/$library.new" "$out/$library"
cp "$dest/build-info" "$out/build-info"
cp "$dest/config.diff" "$out/config.diff"
printf '%s  %s\n' "$built" "$out/$library"
