# -- multistage docker build: stage #1: build stage
FROM golang:1.14-alpine AS build

RUN mkdir -p /go/src/github.com/kaspanet/kasparov

WORKDIR /go/src/github.com/kaspanet/kasparov

RUN apk add --no-cache curl git openssh binutils gcc musl-dev
RUN go get -u golang.org/x/lint/golint

COPY go.mod .
COPY go.sum .

# For development it's useful to have kaspad from filesystem, but for deployment
# we should use the one in github
RUN go mod edit -dropreplace github.com/kaspanet/kaspad
ARG KASPAD_VERSION
RUN if [ -n "${KASPAD_VERSION}" ]; then \
        # use replace instead of require - to propagate into nested dependancies
        go mod edit -replace github.com/kaspanet/kaspad=github.com/kaspanet/kaspad@$KASPAD_VERSION ;\
    fi
RUN cp go.mod go.mod.bu

RUN go mod download

COPY . .

# Restore go.mod because `COPY . .` overwrote it
RUN mv go.mod.bu go.mod

# Run linters and tests
RUN GOFMT_RESULT=`go fmt ./...`; echo $GOFMT_RESULT; test -z "$GOFMT_RESULT"
RUN go vet ./...
RUN golint -set_exit_status ./...
RUN go test ./...

RUN cd kasparovd && GOOS=linux go build -a -installsuffix cgo -o kasparovd .

# --- multistage docker build: stage #2: runtime image
FROM alpine
WORKDIR /app

RUN apk add --no-cache tini

COPY --from=build /go/src/github.com/kaspanet/kasparov/kasparovd/ /app/
COPY --from=build /go/src/github.com/kaspanet/kasparov/database/migrations/ /database/migrations/

ENTRYPOINT ["/sbin/tini", "--"]
CMD ["/app/kasparovd"]
