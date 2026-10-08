# The builder always runs on the build host's own architecture and
# cross-compiles for the target, so the binary matches the platform the image
# is published for.
FROM --platform=$BUILDPLATFORM golang:1.26 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /app

COPY go.mod go.sum /app/
RUN go mod download
# The whole module, not a list of directories: a package added later is part
# of the build without anyone remembering to copy it.
COPY . /app

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -ldflags="-s -w" -o /app/manager .

FROM gcr.io/distroless/static-debian13:nonroot
WORKDIR /app
COPY --from=builder /app/manager /app/manager
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 65532:65532

ENTRYPOINT ["/app/manager"]
