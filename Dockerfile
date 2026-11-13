FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /out/onceonly ./cmd/onceonly

FROM alpine:3.21
RUN apk add --no-cache ca-certificates \
 && adduser -D -u 10001 onceonly
COPY --from=build /out/onceonly /usr/local/bin/onceonly
USER onceonly
ENTRYPOINT ["onceonly"]
CMD ["api"]
