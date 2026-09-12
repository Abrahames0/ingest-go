# Centli ingest — built by Dokploy (build type: Dockerfile) or by hand:
#   docker build -t centli-ingest --build-arg VERSION=1.2.0 --build-arg COMMIT=$(git rev-parse --short HEAD) .
#   docker run --rm --env-file .env -p 8080:8080 centli-ingest
FROM golang:1.25-alpine AS build
ARG VERSION=dev
ARG COMMIT=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath \
  -ldflags="-s -w -X github.com/xonix/centli-ingest/internal/server.Version=${VERSION} -X github.com/xonix/centli-ingest/internal/server.Commit=${COMMIT}" \
  -o /ingest .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /ingest /ingest
EXPOSE 8080
USER nonroot
# The binary probes itself: distroless ships no shell or curl.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 CMD ["/ingest", "-check"]
ENTRYPOINT ["/ingest"]
