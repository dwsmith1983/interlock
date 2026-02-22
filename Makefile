.PHONY: build test test-unit test-integration clean lint fmt vet build-lambda cdk-synth cdk-diff cdk-deploy cdk-test e2e-test e2e-test-teardown local-e2e-test local-e2e-test-teardown

BINARY := interlock
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags "-X main.version=$(VERSION)"
PLATFORMS := linux/amd64 linux/arm64 darwin/amd64 darwin/arm64

build:
	go build $(LDFLAGS) -o $(BINARY) ./cmd/interlock

test:
	go test ./...

test-unit:
	go test ./internal/engine/... ./internal/archetype/... ./internal/lifecycle/... ./internal/evaluator/... ./internal/config/...

test-integration:
	go test ./internal/provider/redis/... -tags=integration

fmt:
	go fmt ./...

vet:
	go vet ./...

lint: fmt vet
	golangci-lint run ./...

clean:
	rm -f $(BINARY)
	rm -rf dist/

dist:
	@mkdir -p dist
	@for platform in $(PLATFORMS); do \
		os=$${platform%/*}; \
		arch=$${platform#*/}; \
		output=dist/$(BINARY)-$$os-$$arch; \
		if [ "$$os" = "windows" ]; then output=$$output.exe; fi; \
		echo "Building $$output..."; \
		GOOS=$$os GOARCH=$$arch go build $(LDFLAGS) -o $$output ./cmd/interlock; \
	done

build-lambda:
	bash deploy/build.sh

cdk-synth: build-lambda
	cd deploy/cdk && cdk synth

cdk-diff: build-lambda
	cd deploy/cdk && cdk diff

cdk-deploy: build-lambda
	cd deploy/cdk && cdk deploy

cdk-test:
	cd deploy/cdk && go test -v ./...

e2e-test:
	bash demo/aws/e2e-test.sh run

e2e-test-teardown:
	bash demo/aws/e2e-test.sh teardown

local-e2e-test:
	bash demo/local/e2e-test.sh run

local-e2e-test-teardown:
	bash demo/local/e2e-test.sh teardown

.DEFAULT_GOAL := build
