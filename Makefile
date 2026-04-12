.PHONY: test build-lambda lint fmt vet clean

test:
	go test -race -count=1 ./...

build-lambda:
	bash deploy/build.sh

fmt:
	go fmt ./...

vet:
	go vet ./...

lint:
	go vet ./...
	staticcheck ./...
	go test -race -count=1 ./...

clean:
	rm -rf deploy/dist/

.DEFAULT_GOAL := build-lambda
