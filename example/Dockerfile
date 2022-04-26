FROM golang:alpine AS base
COPY . /src/
WORKDIR /src/example

FROM base AS kafka-consumer
RUN export GOPROXY="goproxy.cn,direct" && go install ./main.go
CMD ["/go/bin/main"]
