#!/bin/sh
# Runs cargo for linux/arm64 with the cached image and Rust of rust.env, by
# default the release-small build of the binary. The binary links glibc
# because smolvm loads the Linux release's libkrun.so with dlopen, which a
# static musl binary cannot do.
#
# Usage: scripts/build.sh [cargo arguments]
#   scripts/build.sh                  (cargo build --locked --profile release-small)
#   scripts/build.sh test --locked
#   scripts/build.sh clippy --locked --all-targets -- -D warnings
#
# SMOLVM_SRC is the smolvm checkout (default: ../../../smolvm). It must be a
# clean checkout of the pinned commit below; the binary records the commit,
# and the node image writes it to /etc/floci-node/build-info. The cargo
# registry and Rust are cached under its target/floci directory.
set -eu

# The smolvm commit this crate builds against. node/Dockerfile pins the
# guest agent (AGENT_SHA256) built from the same agent sources.
smolvm_commit=54c4acfb5d5b61ba7d2aed65131f469029833cd9

here=$(cd "$(dirname "$0")/.." && pwd)
smolvm=$(cd "${SMOLVM_SRC:-$here/../../../smolvm}" && pwd)
registry=$smolvm/target/floci/cargo-registry
rustup=$smolvm/target/floci/rustup
. "$here/scripts/rust.env"

commit=$(git -C "$smolvm" rev-parse HEAD)
if [ "$commit" != "$smolvm_commit" ]; then
    echo "build.sh: $smolvm is at $commit, not at the pinned smolvm commit $smolvm_commit" >&2
    exit 1
fi
if ! git -C "$smolvm" diff --quiet HEAD --; then
    echo "build.sh: $smolvm has uncommitted changes" >&2
    exit 1
fi

[ $# -gt 0 ] || set -- build --locked --profile release-small
mkdir -p "$registry" "$rustup"
docker run --rm --memory 3g --cpus 4 \
    -v "$here:/src/floci/tools/smolvm-pods" \
    -v "$smolvm:/src/smolvm:ro" \
    -v "$registry:/usr/local/cargo/registry" \
    -v "$rustup:/rustup" \
    -e RUSTUP_HOME=/rustup \
    -e RUSTUP_TOOLCHAIN="$rust_toolchain" \
    -e SMOLVM_PODS_SMOLVM_COMMIT="$commit" \
    -w /src/floci/tools/smolvm-pods \
    "$rust_image" sh -ec '
        rustup toolchain list | grep -q "^$RUSTUP_TOOLCHAIN-" ||
            rustup toolchain install "$RUSTUP_TOOLCHAIN" --profile minimal \
                --component clippy --component rustfmt --no-self-update
        exec cargo "$@"
    ' sh "$@"
