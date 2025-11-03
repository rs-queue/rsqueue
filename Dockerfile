# Build stage
FROM rust:1.88 AS builder

WORKDIR /app

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build the application
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd --create-home --shell /bin/bash app

WORKDIR /app

# Copy the binary from builder stage
COPY --from=builder /app/target/release/rsqueue .

# Create directory for queue specs
RUN mkdir -p queue_specs && chown -R app:app /app

USER app

EXPOSE 3000

CMD ["./rsqueue"]