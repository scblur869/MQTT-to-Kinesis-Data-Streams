FROM golang:alpine AS build-env
ENV CGO_ENABLED=0
RUN apk --no-cache add build-base gcc git ca-certificates
ENV GOPROXY=direct
ADD . /src
WORKDIR /src
RUN go build -o /mqtt_to_kinesis

# final stage
FROM alpine
WORKDIR /app
ENV PORT=1883
ENV WEBSOCKET=8083
ENV BROKER=broker.emqx.io
ENV USER=emqx
ENV PASS=public
ENV CLIENTID=somesbrokerclientid
ENV TOPIC=topic/test/#
ENV BACKLOG_COUNT=2000
ENV KIN_STREAM_NAME=myKinesisStream
ENV KIN_MAXCONN=1
ENV KIN_SESSION_NAME=mysession-1234
ENV KIN_PARTITION_KEY=someMeaningfulValue
ENV AWS_ACCESS_KEY_ID=12344523424
ENV AWS_SECRET_ACCESS_KEY=23123512351235
ENV AWS_REGION=us-east-1
ENV DOCKERLOG=true
COPY --from=build-env /mqtt_to_kinesis .
CMD ["./mqtt_to_kinesis"]
