.PHONY: test build-lambda lint fmt vet clean audit

test:
	go test -race -count=1 ./...

build-lambda:
	bash deploy/build.sh

fmt:
	go fmt ./...

vet:
	go vet ./...

lint:
	golangci-lint run ./...
	go test -race -count=1 ./...

audit:
	golangci-lint run ./...
	go test -race -count=1 -vet=off ./...

clean:
	rm -rf deploy/dist/

.DEFAULT_GOAL := build-lambda
