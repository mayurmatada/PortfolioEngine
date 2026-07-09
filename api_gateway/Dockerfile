FROM golang:1.26-alpine AS build
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/api_gateway .

FROM alpine:3.20
RUN adduser -D appuser
USER appuser

COPY --from=build /bin/api_gateway /usr/local/bin/api_gateway

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/api_gateway"]
