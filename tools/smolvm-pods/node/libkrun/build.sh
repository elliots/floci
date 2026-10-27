#!/usr/bin/env bash
# Builds the node's libkrun.so: smol-machines/libkrun at the commit of the smolvm
# checkout's libkrun submodule, the libkrun of the smolvm v1.32.1 Linux release,
# with the checkout's patches/libkrun in name order, for Linux arm64 with glibc.
# Run "build.sh help" for the options.
set -euo pipefail

libkrun_dir=$(cd "$(dirname "$0")" && pwd)
pods_dir=$(cd "$libkrun_dir/../.." && pwd)
smolvm_src=${SMOLVM_SRC:-$(cd "$pods_dir/../../.." && pwd)/smolvm}
out=${SMOLVM_PODS_LIBKRUN_DIR:-$pods_dir/target/libkrun}

# The libkrun that the smolvm checkout pins, and its patches for smolvm-pods.
libkrun_url=https://github.com/smol-machines/libkrun.git
patch_dir=$smolvm_src/patches/libkrun
# The release's features. It builds with partial RELRO, and removes
# libvirglrenderer from the needed libraries, which libkrun opens itself.
features=blk,net,input,gpu
rustflags='-C relro-level=partial'
# The image and Rust of scripts/build.sh, whose caches in the smolvm checkout
# this build shares. The guest init that libkrun embeds needs the musl target.
. "$pods_dir/scripts/rust.env"
registry=$smolvm_src/target/floci/cargo-registry
rustup=$smolvm_src/target/floci/rustup
# Packages come from the Debian snapshot of the node image (../apt), whose
# InRelease files are pinned there.
packages=(
    clang=1:14.0-55.7~deb12u1
    libclang-dev=1:14.0-55.7~deb12u1
    libdrm-dev=2.4.114-1+b1
    libepoxy-dev=1.5.10-1
    libgbm-dev=22.3.6-1+deb12u1
    libvirglrenderer-dev=0.10.4-1
    patchelf=0.14.3-1+b1
    pkg-config=1.8.1-1
)
# The unit tests of the patches, and of the code they change.
tests='read_memory_image mapped_memory_image sparse_memory_image test_load_guest_memory promotion_from_backing snapshot::delta aarch64_vcpu_state_carries_its_counter_anchor'

# Runs in the container: /src/libkrun is the patched checkout, /out the output
# dir. The library records the source and registry paths, so they stay fixed.
container_script=$(
    cat <<'EOF'
rm -f /etc/apt/sources.list /etc/apt/sources.list.d/*
cp /apt/debian.sources /etc/apt/sources.list.d/debian.sources
apt-get -qq -o Acquire::Check-Valid-Until=false update
(cd /var/lib/apt/lists && sha256sum --check --strict --quiet /apt/InRelease.sha256)
apt-get -qq install --yes --no-install-recommends $PACKAGES >/dev/null
rustup toolchain list | grep -q "^$RUSTUP_TOOLCHAIN-" ||
    rustup toolchain install "$RUSTUP_TOOLCHAIN" --profile minimal \
        --component clippy --component rustfmt --no-self-update
rustup target list --installed | grep -qx aarch64-unknown-linux-musl ||
    rustup target add aarch64-unknown-linux-musl
cd /src/libkrun
cargo build --locked --release -p libkrun --features "$FEATURES"
cp /target/release/libkrun.so /out/libkrun.so
patchelf --remove-needed libvirglrenderer.so.1 /out/libkrun.so
readelf -d /out/libkrun.so | sed -n 's/.*(\(NEEDED\|SONAME\)).*\[\(.*\)\]/\1 \2/p' >/out/dynamic
objdump -T /out/libkrun.so | awk '$NF ~ /^krun_/ { print $NF }' | sort >/out/symbols
objdump -T /out/libkrun.so | grep -oE 'GLIBC_[0-9.]+' | sort -Vu | tail -n 1 >/out/glibc
{ rustc --version; cargo --version; clang --version | sed -n 1p; patchelf --version; } >/out/toolchain
if [ -n "$TESTS" ]; then
    cargo test --locked -p krun-vmm --lib -- $TESTS
fi
EOF
)

usage() {
    cat <<EOF
Usage: build.sh [--test] [--cpus N]

Builds libkrun at the commit of SMOLVM_SRC's libkrun submodule with the
patches in SMOLVM_SRC/patches/libkrun, in name order, for Linux arm64 in
Docker, and writes:

  OUT/libkrun.so    The library, for node/Dockerfile (LIBKRUN_SHA256)
  OUT/build-info    The build's inputs and checksums, its needed libraries,
                    its exported krun_* functions and its highest glibc symbol
  OUT/build.log     The build's output
  OUT/src           The patched checkout

Options:
  --test      Also run the unit tests of the patches
  --cpus N    Container CPUs (default: 8)

Environment:
  SMOLVM_SRC                smolvm checkout, with the libkrun pin and patches,
                            whose target/floci caches the cargo registry and
                            Rust (default: $smolvm_src)
  SMOLVM_PODS_LIBKRUN_DIR   OUT above (default: $pods_dir/target/libkrun)
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

run_tests=
cpus=8
while [ $# -gt 0 ]; do
    case $1 in
        --test) run_tests=1 ;;
        --cpus)
            [ $# -ge 2 ] || die "--cpus needs a value"
            cpus=$2
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

for tool in docker git shasum; do
    command -v "$tool" >/dev/null || die "$tool is not installed"
done
libkrun_commit=$(git -C "$smolvm_src" rev-parse HEAD:libkrun 2>/dev/null) ||
    die "$smolvm_src has no libkrun submodule"
patches=()
for patch in "$patch_dir"/*.patch; do
    [ -f "$patch" ] && patches+=("${patch##*/}")
done
[ ${#patches[@]} -gt 0 ] || die "$patch_dir has no patches"
docker image inspect "$rust_image" >/dev/null 2>&1 ||
    die "$rust_image is not in the local Docker image store, and this script never pulls"

step "Fetching libkrun $libkrun_commit into $out/src"
mkdir -p "$out"
rm -rf "$out/src" "$out/out"
git init --quiet "$out/src"
git -C "$out/src" fetch --quiet --depth 1 "$libkrun_url" "$libkrun_commit"
git -C "$out/src" -c advice.detachedHead=false checkout --quiet FETCH_HEAD
[ "$(git -C "$out/src" rev-parse HEAD)" = "$libkrun_commit" ] || die "fetched the wrong commit"
for patch in "${patches[@]}"; do
    git -C "$out/src" apply "$patch_dir/$patch"
done

step "Building in Docker, log in $out/build.log"
mkdir -p "$out/out" "$out/target" "$registry" "$rustup"
container=floci-libkrun-build-$$
trap 'docker rm --force "$container" >/dev/null 2>&1 || true' EXIT
docker run --rm --name "$container" --platform linux/arm64 --cpus "$cpus" --memory 12g \
    -v "$out/src:/src/libkrun:ro" -v "$out/out:/out" -v "$out/target:/target" \
    -v "$libkrun_dir/../apt:/apt:ro" \
    -v "$registry:/usr/local/cargo/registry" -v "$rustup:/rustup" \
    -e RUSTUP_HOME=/rustup -e "RUSTUP_TOOLCHAIN=$rust_toolchain" -e CARGO_TARGET_DIR=/target \
    -e "RUSTFLAGS=$rustflags" -e "FEATURES=$features" -e "PACKAGES=${packages[*]}" \
    -e "TESTS=${run_tests:+$tests}" \
    "$rust_image" bash -euo pipefail -c "$container_script" >"$out/build.log" 2>&1 || {
    tail -n 20 "$out/build.log" >&2
    die "the build failed, see $out/build.log"
}

step "Checking libkrun.so"
dynamic=$(cat "$out/out/dynamic")
grep -qx 'SONAME libkrun.so.2' <<<"$dynamic" || die "libkrun.so has no SONAME libkrun.so.2"
! grep -q 'libvirglrenderer' <<<"$dynamic" || die "libkrun.so still needs libvirglrenderer"
for symbol in krun_create_ctx krun_set_snapshot_memory_fd krun_restore_reads_memory; do
    grep -qx "$symbol" "$out/out/symbols" || die "libkrun.so does not export $symbol"
done

{
    echo "libkrun $libkrun_url $libkrun_commit"
    for patch in "${patches[@]}"; do
        echo "patch $patch $(sha256 "$patch_dir/$patch")"
    done
    echo "features $features"
    echo "rustflags $rustflags"
    echo "builder $rust_image, Debian snapshot of ../apt"
    sed 's/^/toolchain /' "$out/out/toolchain"
    sed 's/^/dynamic /' "$out/out/dynamic"
    echo "glibc $(cat "$out/out/glibc")"
    echo "exports $(wc -l <"$out/out/symbols" | tr -d ' ') krun_* functions: $(paste -s -d ' ' "$out/out/symbols")"
    echo "sha256 $(sha256 "$out/out/libkrun.so") libkrun.so"
} >"$out/build-info"
mv "$out/out/libkrun.so" "$out/libkrun.so"
rm -rf "$out/out"
cat "$out/build-info" >&2
printf '%s  %s\n' "$(sha256 "$out/libkrun.so")" "$out/libkrun.so"
