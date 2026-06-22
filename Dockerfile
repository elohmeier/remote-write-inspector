# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26.0

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS=linux
ARG TARGETARCH=amd64
ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
	-trimpath \
	-ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
	-o /out/remote-write-inspector \
	./cmd/remote-write-inspector

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/remote-write-inspector /usr/local/bin/remote-write-inspector
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/remote-write-inspector"]
