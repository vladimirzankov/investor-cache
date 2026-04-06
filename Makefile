.PHONY: up down test bench seed logs clean build demo

up:
	docker compose up -d --build

down:
	docker compose down -v

build:
	docker compose build middleware profile-service outbox-relay invalidation-worker

test:
	go test ./... -v -race -count=1

bench:
	go test ./... -bench=. -benchmem -run=^$

seed:
	docker compose exec postgres psql -U investor -d investordb -f /docker-entrypoint-initdb.d/02-seed.sql

logs:
	docker compose logs -f middleware

demo:
	@./scripts/demo.sh

clean:
	docker compose down -v --rmi local
	rm -rf tmp/
