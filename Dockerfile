# fjord broker image.
#
# Build context must contain the fjord, heimq, and object-log repos side by side
# (fjord depends on the other two via ../../../ path deps). Use deploy/build-image.sh,
# which assembles a clean context (excluding target/ and .git) and runs the build.
#
#   ./deploy/build-image.sh fjord:dev

FROM rust:1-bookworm AS builder
# aws-lc-sys (rustls crypto for the S3 client) needs cmake + a C toolchain.
RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
# Sibling crates first (better layer caching: they change less often than fjord).
COPY heimq /src/heimq
COPY object-log /src/object-log
COPY fjord /src/fjord
WORKDIR /src/fjord
# Release build of just the broker binary (pulls postgres-backend + S3 via deps).
RUN cargo build --release -p fjord

FROM debian:bookworm-slim AS runtime
# rustls-native-certs / S3 TLS needs the system trust store.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 fjord
COPY --from=builder /src/fjord/target/release/fjord /usr/local/bin/fjord
USER 10001
EXPOSE 9092
ENTRYPOINT ["/usr/local/bin/fjord"]
