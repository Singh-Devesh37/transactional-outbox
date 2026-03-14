# =========================
# Stage 1: Builder
# =========================
FROM golang:1.23-bullseye AS builder

WORKDIR /app

# Install system deps required to build librdkafka from source
RUN apt-get update && \
    apt-get install -y build-essential pkg-config libssl-dev zlib1g-dev git && \
    rm -rf /var/lib/apt/lists/*

# Build librdkafka from source (ARM64-native)
RUN git clone --depth 1 https://github.com/edenhill/librdkafka.git /tmp/librdkafka && \
    cd /tmp/librdkafka && \
    ./configure --prefix=/usr/local --enable-static --disable-shared && \
    make -j$(nproc) && \
    make install

# Copy Go module files and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Make sure Go picks up system-installed librdkafka (not vendored amd64)
ENV CGO_ENABLED=1
ENV CGO_LDFLAGS="-L/usr/local/lib -lrdkafka"
ENV CGO_CFLAGS="-I/usr/local/include"
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig

RUN pkg-config --libs --cflags rdkafka

# Build your Go binary
RUN go build -o outbox ./cmd/app/main.go

# =========================
# Stage 2: Runtime
# =========================
FROM debian:bullseye-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates tzdata && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binary and config
COPY --from=builder /app/outbox .
COPY ./config.yaml ./config.yaml

# Copy librdkafka shared libs to runtime
COPY --from=builder /usr/local/lib /usr/local/lib
COPY --from=builder /usr/local/include /usr/local/include
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /usr/local/share /usr/local/share
RUN ldconfig

# Create non-root user
RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser
USER appuser

EXPOSE 8080

ENTRYPOINT ["./outbox"]