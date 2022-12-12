up:
	docker compose --project-directory .devenv up --wait --remove-orphans --force-recreate --build

down:
	docker compose --project-directory .devenv down

watch-ps:
	watch docker compose -p tor ps

logs:
	docker compose -p tor logs -f

run-tor-locally:
	go run github.com/lorenzoranucci/tor/example/tor run --config=./tor-bare-metal.yaml

fmt:
	@find . -name *.go -type f -not -path '*/vendor/*' \
	| sed 's/^\.\///g' \
	| xargs -I {} bash -c 'echo "formatting {}" && gofmt -w -s {}'

imports:
	@find . -name *.go -type f -not -path '*/vendor/*' \
	| sed 's/^\.\///g' \
	| xargs -I {} bash -c 'echo "fixing imports for {}" && goimports --local "${_PROJECT_PREFIX}" -v -w {}'

cs-fix: fmt imports

lint:
	@find . -name go.mod \
     | sed 's/\/[^\/]*$$//' \
     | xargs -I {} bash -c \
     'echo "linting {}" && golangci-lint run --deadline=5m0s --out-format=line-number  -exclude-use-default=false  {}/...'

test:
	@find . -name go.mod \
     | sed 's/\/[^\/]*$$//' \
     | xargs -I {} bash -c \
     'echo "testing {}" && go test --race {}/...'
