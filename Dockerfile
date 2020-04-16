FROM golang:1-buster
LABEL vendor="JAM Just-Add-Music GmbH"
RUN apt update


RUN mkdir /app /app/saerch
ADD . /app
WORKDIR /app

RUN go build -o main .
CMD ["/app/main"]
