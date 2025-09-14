
FROM golang:1.23.3 AS build
WORKDIR /app

COPY go.mod  ./
RUN go mod download || true

COPY . .

# Статическая сборка (чтобы нормально работало в alpine)
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -o /out/pipeline .

# Стадия рантайма
FROM alpine:3.20
LABEL version="1.0.0"
LABEL maintainer="Albert Shomakhov"

WORKDIR /root/
COPY --from=build /out/pipeline /usr/local/bin/pipeline
ENTRYPOINT ["pipeline"]
