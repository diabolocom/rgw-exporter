FROM alpine AS base

RUN apk add bash curl && addgroup -g 1001 -S app && adduser -u 1001 -S app -G app

FROM golang:1.24-alpine AS build

WORKDIR /app

COPY go.mod go.sum .

RUN go mod download

COPY . .

RUN go build -o /rgw-exporter .

EXPOSE 9198

FROM base

USER app

COPY --from=build /rgw-exporter /rgw-exporter

CMD ["/rgw-exporter"]
