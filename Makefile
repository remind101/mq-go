test:
	go test ./...

integration:
	go test -v -tags=integration ./...

dockertest:
	docker-compose run --rm test