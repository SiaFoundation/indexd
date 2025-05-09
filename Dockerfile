FROM golang:1.24 AS builder

WORKDIR /indexd

# get dependencies
COPY go.mod go.sum ./
RUN go mod download

# copy source
COPY . .
# codegen
RUN go generate ./...
# build
RUN go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w'  ./cmd/indexd

FROM debian:bookworm-slim

LABEL maintainer="The Sia Foundation <info@sia.tech>" \
    org.opencontainers.image.description.vendor="The Sia Foundation" \
    org.opencontainers.image.description="A indexd container - connect to apps and store data on the Sia network" \
    org.opencontainers.image.source="https://github.com/SiaFoundation/indexd" \
    org.opencontainers.image.licenses=MIT

# copy binary and certificates
COPY --from=builder /indexd/bin/* /usr/bin/
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENV INDEXD_DATA_DIR=/data
ENV INDEXD_CONFIG_FILE=/data/indexd.yml

VOLUME [ "/data" ]

# API port
EXPOSE 9980/tcp
# Syncer port
EXPOSE 9981/tcp

ENTRYPOINT [ "indexd", "-api.admin", ":9980" ]
