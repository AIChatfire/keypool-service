# ---- build stage ----
FROM golang:1.22-alpine AS build

WORKDIR /src

# 先拷 go.mod/go.sum 以利用层缓存
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/keypool ./cmd/keypool

# ---- runtime stage ----
FROM alpine:3.20

RUN adduser -D -u 10001 keypool
COPY --from=build /out/keypool /usr/local/bin/keypool

USER keypool
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/keypool"]
