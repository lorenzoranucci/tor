devenv:
	cd ./.devenv && docker compose up -d --detach --wait --remove-orphans --force-recreate

devenv-watch-ps:
	watch docker compose -p tor ps

devenv-logs:
	docker compose -p tor logs -f

run:
	go run main.go run --config=./.devenv/transactional-outbox-router-bare-metal.yaml
