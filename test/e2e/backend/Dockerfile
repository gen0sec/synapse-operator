FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY main.go .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /backend main.go

FROM scratch
COPY --from=build /backend /backend
USER 65532
ENTRYPOINT ["/backend"]
