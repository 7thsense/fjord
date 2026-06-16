# fjord broker image.
#
# fjord depends on heimq and object-log as git dependencies (public easel repos),
# so the build context is just this repo — cargo fetches the deps. Build with:
#
#   docker build -t fjord:dev .            # or: ./deploy/build-image.sh fjord:dev
#
# Note: .dockerignore excludes the local-only .cargo/config.toml path override so
# the image always builds against the pinned git deps, not absent siblings.

FROM rust:1-bookworm AS builder
# aws-lc-sys (rustls crypto for the S3 client) needs cmake + a C toolchain.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# Release build of just the broker binary (pulls postgres-backend + S3 via deps).
RUN cargo build --release -p fjord

FROM debian:bookworm-slim AS runtime
# rustls-native-certs / S3 TLS needs the system trust store.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 fjord
COPY --from=builder /src/target/release/fjord /usr/local/bin/fjord
USER 10001
EXPOSE 9092
ENTRYPOINT ["/usr/local/bin/fjord"]
