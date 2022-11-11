# syntax=docker/dockerfile:1

FROM golang:1.19 AS builder

WORKDIR /srv

COPY . .

RUN go build -o bin/transactional-outbox-router main.go

FROM ubuntu:22.04

WORKDIR /srv

COPY --from=builder /srv/bin/transactional-outbox-router ./transactional-outbox-router

ENTRYPOINT ["/srv/transactional-outbox-router"]
