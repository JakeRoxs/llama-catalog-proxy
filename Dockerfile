# syntax=docker/dockerfile:1

FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS builder
WORKDIR /src

ARG TARGETOS=linux
ARG TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build \
    -trimpath \
    -ldflags="-s -w -X main.version=$VERSION -X main.commit=$COMMIT -X main.buildDate=$BUILD_DATE" \
    -o /out/llama-catalog-proxy \
    ./cmd/llama-catalog-proxy

FROM gcr.io/distroless/static-debian13:nonroot

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_DATE=unknown

LABEL org.opencontainers.image.title="llama-catalog-proxy" \
      org.opencontainers.image.description="Model-aware router for independent OpenAI-compatible inference backends" \
      org.opencontainers.image.source="https://github.com/JakeRoxs/llama-catalog-proxy" \
      org.opencontainers.image.version=$VERSION \
      org.opencontainers.image.revision=$COMMIT \
      org.opencontainers.image.created=$BUILD_DATE \
      org.opencontainers.image.licenses="MIT"

COPY --from=builder /out/llama-catalog-proxy /llama-catalog-proxy

EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/llama-catalog-proxy"]
CMD ["--config", "/config/config.yaml"]
