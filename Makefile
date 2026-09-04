fmt:
	gofmt -w internal
fmt-check:
	@test -z "$$(gofmt -l internal)"
test:
	go test ./...
vet:
	go vet ./...
check: fmt-check test vet
