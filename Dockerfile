# Build stage
FROM rust:1.88 AS builder

WORKDIR /app

# Copy manifests
COPY cargo.toml Cargo.lock ./

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

# Optional environment variables for Basic Authentication
# Set both AUTH_USER and AUTH_PASSWORD to enable authentication
# Example: docker run -e AUTH_USER=admin -e AUTH_PASSWORD=secret123 rsqueue
# hadolint ignore=DL3002
ENV AUTH_USER=""
# hadolint ignore=DL3002
ENV AUTH_PASSWORD=""

EXPOSE 4000

CMD ["./rsqueue"]