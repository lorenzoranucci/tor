# syntax=docker/dockerfile:1

FROM golang:1.19 AS build

COPY ../ /srv/

WORKDIR /srv/.devenv

RUN go build -o ../bin/tor github.com/lorenzoranucci/tor/example/kafka-redis-router

FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /srv/bin/tor /tor

USER nonroot:nonroot

ENTRYPOINT ["/tor"]
