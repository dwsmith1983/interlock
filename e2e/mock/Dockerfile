# The e2e mock clock and job server. Built from the repo root so the module
# path resolves; the mock is stdlib only, so nothing is downloaded.
FROM golang:1.26-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
COPY e2e/mock ./e2e/mock
RUN CGO_ENABLED=0 go build -o /out/mock ./e2e/mock

# Alpine for busybox wget, which the compose healthcheck uses.
FROM alpine:3.24
COPY --from=build /out/mock /usr/local/bin/mock
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/mock"]
