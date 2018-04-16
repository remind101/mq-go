FROM golang:1.10
RUN curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
COPY . /go/src/github.com/remind101/mq-go
RUN dep ensure
WORKDIR /go/src/github.com/remind101/mq-go
