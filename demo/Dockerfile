# ── Builder ──────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /interlock ./cmd/interlock

# ── Runtime ──────────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache bash

COPY --from=builder /interlock /usr/local/bin/interlock

# Demo configuration
COPY demo/interlock.yaml   /app/interlock.yaml
COPY demo/archetypes/      /app/archetypes/
COPY demo/evaluators/      /app/evaluators/
RUN mkdir -p /app/pipelines && chmod +x /app/evaluators/*

ENV PATH="/app/evaluators:${PATH}"
WORKDIR /app
EXPOSE 3000

ENTRYPOINT ["interlock"]
CMD ["serve"]
