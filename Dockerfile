FROM rust:1.82 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    libopenblas-dev \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libopenblas0 \
    libssl-dev \
    ca-certificates \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/bluesky-data-enticher /app/

WORKDIR /app

ENTRYPOINT ["./bluesky-data-enticher"]