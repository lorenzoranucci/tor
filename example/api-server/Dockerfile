# syntax=docker/dockerfile:1

FROM golang:1.19 AS build

WORKDIR /srv

COPY . .

RUN go build -o bin/api-server github.com/lorenzoranucci/tor/example/api-server

FROM gcr.io/distroless/base-debian10

WORKDIR /

COPY --from=build /srv/bin/api-server /api-server

USER nonroot:nonroot

ENTRYPOINT ["/api-server"]
