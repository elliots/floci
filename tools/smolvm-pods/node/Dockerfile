# The Kubernetes node for smolvm-pods: Debian 12 with k3s, the smolvm Linux
# release with the patched guest agent and libkrun, and the smolvm-pods shim.
#
# scripts/node-vm.sh build stages the inputs in a build context named "inputs"
# and checks the Docker images it takes them from. This file checks every
# other input against the checksums below, and a mismatch fails the build.
FROM ghcr.io/superradcompany/debian-systemd:12@sha256:a2692bbf6d1aa719bd283b970d759c6924e5dbd1c18a85d30f3af06ebd60bcb8

SHELL ["/bin/bash", "-euo", "pipefail", "-c"]

# Packages come from the Debian snapshot the base image was built from. Its
# InRelease files, which hold the checksums of every package, are pinned.
COPY apt/debian.sources /etc/apt/sources.list.d/debian.sources
RUN --mount=type=bind,source=apt/InRelease.sha256,target=/tmp/InRelease.sha256 \
    apt-get -o Acquire::Check-Valid-Until=false update \
    && (cd /var/lib/apt/lists && sha256sum --check --strict --quiet /tmp/InRelease.sha256) \
    && apt-get install --yes --no-install-recommends \
        ca-certificates=20230311+deb12u1 \
        curl=7.88.1-10+deb12u14 \
        iproute2=6.1.0-3 \
        jq=1.6-2.1+deb12u1 \
        nftables=1.0.6-2+deb12u2 \
        procps=2:4.0.2-3 \
    && rm -rf /var/lib/apt/lists/*

# /bin of the pinned k3s image: k3s and the containerd, runc, CNI and iptables
# binaries it runs. .sha256sums lists every binary in it.
ARG K3S_SUMS_SHA256=de37e05acc475bda35f0f84cc8f6dc2302eb43bb9df94877eb51be36b84c90b9
ARG K3S_SHA256=f351f0e779efc7231d4bd8ae8d5534891d5b18c7d4aaad5ae9b6a8096e6d86cf
RUN --mount=type=bind,from=inputs,source=k3s-bin.tar,target=/tmp/k3s-bin.tar \
    mkdir /opt/k3s \
    && tar -xf /tmp/k3s-bin.tar -C /opt/k3s \
    && cd /opt/k3s/bin \
    && echo "$K3S_SUMS_SHA256  .sha256sums" | sha256sum --check --quiet \
    && echo "$K3S_SHA256  k3s" | sha256sum --check --quiet \
    && sha256sum --check --quiet .sha256sums

# The smolvm v1.30.1 Linux release, with the patched guest agent in the agent
# rootfs that pod VMs boot, and the patched libkrun of node/libkrun/build.sh in
# place of the release's lib/libkrun.so. SMOLVM_SUMS_SHA256 is the digest GitHub
# publishes for the release's checksums.sha256.
ARG SMOLVM_RELEASE=smolvm-1.30.1-linux-arm64
ARG SMOLVM_RELEASE_SHA256=5f4424f880e779a4aba3c4cfd3756f6288078547e72370d035fbf11884e9a044
ARG SMOLVM_SUMS_SHA256=81e44179d385a82f631f8a49aeab8eb5691c056ef723402c4c31a27e79a0de04
ARG AGENT_SHA256=7cace4d3459dd79e9d3d774ce01dd82f8c705df22368b80a3fc3197e9e6e95f4
ARG LIBKRUN_SHA256=5b526a62810fce52436637f8d27387212a1c0950a05cb5eac0f81e1006b6a5ac
RUN --mount=type=bind,from=inputs,source=release,target=/tmp/release \
    --mount=type=bind,from=inputs,source=smolvm-agent,target=/tmp/smolvm-agent \
    --mount=type=bind,from=inputs,source=libkrun.so,target=/tmp/libkrun.so \
    cd /tmp/release \
    && echo "$SMOLVM_SUMS_SHA256  checksums.sha256" | sha256sum --check --quiet \
    && grep "  $SMOLVM_RELEASE.tar.gz\$" checksums.sha256 | sha256sum --check --quiet \
    && echo "$SMOLVM_RELEASE_SHA256  $SMOLVM_RELEASE.tar.gz" | sha256sum --check --quiet \
    && mkdir /opt/smolvm \
    && tar -xzf "$SMOLVM_RELEASE.tar.gz" -C /opt/smolvm --strip-components 1 \
    && cd /opt/smolvm \
    && sha256sum --check --quiet checksums.txt \
    && echo "$AGENT_SHA256  /tmp/smolvm-agent" | sha256sum --check --quiet \
    && install -m 0755 /tmp/smolvm-agent agent-rootfs/usr/local/bin/smolvm-agent \
    && echo "$LIBKRUN_SHA256  /tmp/libkrun.so" | sha256sum --check --quiet \
    && install -m 0755 /tmp/libkrun.so lib/libkrun.so \
    && printf 'release %s\nagent %s\nlibkrun %s\n' "$SMOLVM_RELEASE_SHA256" "$AGENT_SHA256" \
        "$LIBKRUN_SHA256" >BUILD \
    && ln -s /opt/smolvm/smolvm /usr/local/bin/smolvm

# The images k3s needs at startup, so that the node never pulls them.
COPY --from=inputs images/ /opt/floci-node/images/

COPY rootfs/ /

# The litmus test of the shim's TSO canary, a local build of
# tests/e2e/workloads/litmus: node-vm.sh passes its checksum.
ARG LITMUS_SHA256
RUN --mount=type=bind,from=inputs,source=litmus,target=/tmp/litmus \
    test -n "$LITMUS_SHA256" \
    && echo "$LITMUS_SHA256  /tmp/litmus" | sha256sum --check --quiet \
    && install -D -m 0755 /tmp/litmus /opt/smolvm-pods/litmus/litmus

# The shim is a local build: node-vm.sh passes the checksum of the copy it
# staged, and the shim names the smolvm commit it was built from. The node
# VM's kernel is not in the image: NODE_KERNEL_SHA256 is the SHA-256 of the
# kernel Image that node-vm.sh boots it with (node/kernel/build.sh).
ARG SHIM_SHA256
ARG NODE_KERNEL_SHA256
RUN --mount=type=bind,from=inputs,source=shim,target=/tmp/shim \
    test -n "$SHIM_SHA256" \
    && test -n "$NODE_KERNEL_SHA256" \
    && echo "$SHIM_SHA256  /tmp/shim" | sha256sum --check --quiet \
    && install -m 0755 /tmp/shim /usr/local/bin/containerd-shim-smolvm-pods-v2 \
    && ln -s containerd-shim-smolvm-pods-v2 /usr/local/bin/smolvm-pods \
    && if ldd /usr/local/bin/smolvm-pods | grep 'not found'; then exit 1; fi \
    && smolvm_source=$(/usr/local/bin/smolvm-pods _build-info) \
    && printf '%s %s\n' k3s "$K3S_SHA256" smolvm-release "$SMOLVM_RELEASE_SHA256" \
        smolvm-agent "$AGENT_SHA256" libkrun "$LIBKRUN_SHA256" shim "$SHIM_SHA256" \
        litmus "$LITMUS_SHA256" node-kernel "$NODE_KERNEL_SHA256" \
        >/etc/floci-node/build-info \
    && echo "$smolvm_source" >>/etc/floci-node/build-info

ENV PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/opt/k3s/bin:/opt/k3s/bin/aux
ENTRYPOINT ["/usr/local/bin/floci-node"]
CMD ["run"]
