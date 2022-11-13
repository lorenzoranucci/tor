# syntax=docker/dockerfile:1

FROM golang:1.19 AS build

WORKDIR /srv

COPY . .

RUN go build -o bin/transactional-outbox-router main.go

FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /srv/bin/transactional-outbox-router /transactional-outbox-router

USER nonroot:nonroot

ENTRYPOINT ["/transactional-outbox-router"]
