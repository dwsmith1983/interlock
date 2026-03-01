.PHONY: test build-lambda lint fmt vet clean

test:
	go test ./...

build-lambda:
	bash deploy/build.sh

fmt:
	go fmt ./...

vet:
	go vet ./...

lint: fmt vet

clean:
	rm -rf deploy/dist/

.DEFAULT_GOAL := test
