## mqtt-to-kinesis
 
The idea behind  this service is to run it as a container on the edge, subscribe to a topic from MQTT and then batch those payloads up and send them to kinesis data stream --> firehose --> datalake

I have tested this with various free mqtt brokers and it seems to work pretty well.

this service written in Go is deployed via a container using parameterization.
see the Dockerfile.example
  
it can also be ran from command line using an .env file
below is an example of what that file could look like
```console
 PORT=1883
 WEBSOCKET=8080
 BROKER=public.mqtthq.com
 USER=
 PASS=
 CLIENTID=client-123
 TOPIC=homeassistant/sensor/#
 QOS=0
 BACKLOG_COUNT=2000
 KIN_STREAM_NAME=kinesis-data-stream
 KIN_MAXCONN=10
 KIN_SESSION_NAME=mysession-1234
 KIN_PARTITION_KEY=sensor-data
 AWS_ACCESS_KEY_ID=nnnnnnnnnnnnnnnnnnnnn
 AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
 AWS_ASSUME_ROLE_ARN=arn:aws:iam::1111111111111:role/service-role/kinesis-endpoint-access
 AWS_REGION=us-east-1
 STDOUT=true
```