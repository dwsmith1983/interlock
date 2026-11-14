.PHONY: test build-lambda lint fmt vet clean audit e2e-check guards

# Only e2e builds may link this package; audit checks both directions.
E2EHOOKS_PKG := github.com/dwsmith1983/interlock/internal/e2ehooks

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

e2e-check:
	go vet -tags e2e ./...
	go test -race -count=1 -tags e2e ./internal/e2ehooks/...
	go test -race -count=1 -tags e2e -run 'TestApplyE2EHooks' ./internal/lambda/

guards:
	@missing=$$(grep -L 'ApplyE2EHooks(' cmd/lambda/*/main.go); \
	if [ -n "$$missing" ]; then \
		echo "audit: FAIL: Lambda mains without ApplyE2EHooks: $$missing" >&2; exit 1; \
	fi; \
	echo 'audit: ok: every cmd/lambda main calls ApplyE2EHooks'
	@deps=$$(go list -deps ./cmd/lambda/...) || exit 1; \
	if printf '%s\n' "$$deps" | grep -qx '$(E2EHOOKS_PKG)'; then \
		echo 'audit: FAIL: untagged (production) Lambda build links $(E2EHOOKS_PKG)' >&2; exit 1; \
	fi; \
	echo 'audit: ok: untagged Lambda build does not link $(E2EHOOKS_PKG)'
	@deps=$$(go list -deps -tags e2e ./cmd/lambda/...) || exit 1; \
	if ! printf '%s\n' "$$deps" | grep -qx '$(E2EHOOKS_PKG)'; then \
		echo 'audit: FAIL: -tags e2e Lambda build does not link $(E2EHOOKS_PKG)' >&2; exit 1; \
	fi; \
	echo 'audit: ok: -tags e2e Lambda build links $(E2EHOOKS_PKG)'

audit: lint test e2e-check guards

clean:
	rm -rf deploy/dist/

.DEFAULT_GOAL := build-lambda
