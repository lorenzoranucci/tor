up:
	docker compose up --wait --remove-orphans --force-recreate --build

down:
	docker compose down

watch-ps:
	watch docker compose -p tor ps

logs:
	docker compose -p tor logs -f

run-tor-locally:
	go run github.com/lorenzoranucci/tor/example/kafka-redis-router run --config=./tor-bare-metal.yaml
