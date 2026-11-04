FROM rust:1.95.0-alpine AS builder

ARG TARGETARCH
WORKDIR /src

ENV LIBZ_SYS_STATIC=1 \
  OPENSSL_STATIC=1

RUN apk add --no-cache build-base cmake perl pkgconf

COPY . .

RUN set -eux; \
  case "${TARGETARCH:-$(uname -m)}" in \
    amd64|x86_64) rust_target=x86_64-unknown-linux-musl ;; \
    arm64|aarch64) rust_target=aarch64-unknown-linux-musl ;; \
    *) echo "Unsupported architecture: ${TARGETARCH:-$(uname -m)}" >&2; exit 1 ;; \
  esac; \
  rustup target add "$rust_target"; \
  cargo build --locked --release --target "$rust_target"; \
  mkdir -p /rootfs; \
  cp "target/$rust_target/release/docker-volume-backups" /rootfs/; \
  # Mount points for SCRATCH_DIR and CACHE_DIR. A new named volume mounted
  # here copies this mode, so it is writable whichever user the container
  # runs as.
  mkdir -m 1777 /rootfs/scratch /rootfs/cache

FROM scratch

# Copying the directory's contents keeps the mode of /scratch and /cache,
# which copying them itself would not.
COPY --from=builder /rootfs/ /

# The binary bundles the time zone database, as scratch has none. Set TZ to
# a zone name, such as Europe/Berlin, for local times in logs and snapshots.
ENV TZ=UTC

USER 65532:65532
ENTRYPOINT ["/docker-volume-backups"]
CMD ["schedule"]
