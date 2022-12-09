# syntax=docker/dockerfile:1

FROM golang:1.19 AS build

WORKDIR /srv

COPY . .

RUN go build -o bin/tor github.com/lorenzoranucci/tor/example/tor

FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /srv/bin/tor /tor

USER nonroot:nonroot

ENTRYPOINT ["/tor"]
