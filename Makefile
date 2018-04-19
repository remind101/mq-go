test-unit:
	go test ./...

test-integration:
	go test -tags=integration ./...

test-all: test-unit test-integration

test:
	docker-compose run --rm test