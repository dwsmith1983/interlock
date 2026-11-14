# The e2e harness: builds the e2e Lambdas, deploys them to LocalStack and runs
# the e2e tests. The source is copied in (the compose file mounts nothing).
FROM golang:1.26-bookworm

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-venv zip \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

COPY deploy/localstack/requirements.txt /tmp/requirements.txt
RUN python3 -m venv /opt/venv && /opt/venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt
ENV PATH=/opt/venv/bin:$PATH

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
