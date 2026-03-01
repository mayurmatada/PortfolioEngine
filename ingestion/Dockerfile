# syntax=docker/dockerfile:1
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o /bin/ingestion

FROM alpine:latest
RUN apk add --no-cache ca-certificates
COPY --from=builder /bin/ingestion /bin/ingestion
ENTRYPOINT ["/bin/ingestion"]
