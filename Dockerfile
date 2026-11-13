FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/groundhog ./cmd/groundhog

FROM alpine:3.21
RUN apk add --no-cache ca-certificates \
 && adduser -D -u 10001 groundhog
COPY --from=build /out/groundhog /usr/local/bin/groundhog
USER groundhog
ENTRYPOINT ["groundhog"]
CMD ["api"]
