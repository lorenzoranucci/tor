# syntax=docker/dockerfile:1

FROM golang:1.19 AS build

COPY ../ /srv/

WORKDIR /srv

RUN go build -o ./bin/tor ./example/tor/main.go

FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /srv/bin/tor /tor

USER nonroot:nonroot

ENTRYPOINT ["/tor"]
