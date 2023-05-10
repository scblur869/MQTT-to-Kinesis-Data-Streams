/*
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.

 * Contributors:
 *    Matt Brittan primary mqtt contributor "github.com/eclipse/paho.mqtt.golang"
 *    Brad Ross added some parameterization, kinesis producer functionality
 */

package main

// Connect to the broker, subscribe, and write messages received to a file

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/service/kinesis"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/joho/godotenv"
	producer "github.com/northvolt/kinesis-producer"
	"github.com/sirupsen/logrus"
)

type AWSKinesis struct {
	stream          string
	region          string
	endpoint        string
	accessKeyID     string
	secretAccessKey string
	sessionToken    string
	assumeRoleARN   string
	MaxConnections  string
	PartitionKey    string
	sessionName     string
}

type MQTTCONFIG struct {
	broker   string
	port     string
	user     string
	pass     string
	topic    string
	clientID string
	qos      string
}

var (
	kin    AWSKinesis
	mqc    MQTTCONFIG
	STDOUT string
)

type handler struct {
	p *producer.Producer
}

func init() {

	e := godotenv.Load() //Load .env file
	if e != nil {
		fmt.Print(e)
	}

	kin = AWSKinesis{
		stream:          os.Getenv("KIN_STREAM_NAME"),
		region:          os.Getenv("AWS_REGION"),
		endpoint:        os.Getenv("AWS_ENDPOINT"),
		accessKeyID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		secretAccessKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		sessionToken:    os.Getenv("AWS_SESSION_TOKEN"),
		assumeRoleARN:   os.Getenv("AWS_ASSUME_ROLE_ARN"),
		MaxConnections:  os.Getenv("KIN_MAXCONN"),
		PartitionKey:    os.Getenv("KIN_PARTITION_KEY"),
		sessionName:     os.Getenv("KIN_SESSION_NAME"),
	}

	mqc = MQTTCONFIG{
		broker:   os.Getenv("BROKER"),
		port:     os.Getenv("PORT"),
		user:     os.Getenv("USER"),
		pass:     os.Getenv("PASS"),
		topic:    os.Getenv("TOPIC"),
		clientID: os.Getenv("CLIENTID"),
		qos:      os.Getenv("QOS"),
	}
	STDOUT = os.Getenv("STDOUT")
}

func NewHandler() *handler {

	mc, err := strconv.Atoi(kin.MaxConnections)

	if err != nil {
		fmt.Println("err converting string")
	}
	sess, err := AssumedRoleV1Session(kin.assumeRoleARN, kin.region)
	if err != nil {
		fmt.Println("FAILED Getting Assume Role Credentials:\n", err)
		os.Exit(0) // we dont want to do anything else if the assume role fails

	}

	// if we have a clean session then lets start producing
	kcl := kinesis.New(sess)
	pr := producer.New(&producer.Config{
		StreamName:     kin.stream,
		BacklogCount:   2000,
		MaxConnections: mc,
		Client:         kcl,
	})

	pr.Start()
	return &handler{p: pr}
}

// Stop() stops the kinesis producer via *handler
func (k *handler) Stop() {
	if k.p != nil {
		k.p.Stop()
	}
}

// Message struct incase i wanted to covert this to something like JSON...etc
/* type Message struct {
	data interface{}
}
*/

// handle is called when a message is received
func (k *handler) handle(_ mqtt.Client, msg mqtt.Message) {
	//	var m Message
	STDOUTPUT, err := strconv.ParseBool(STDOUT)
	if err != nil {
		fmt.Println("error converting to bool")
	}
	// I commented this out after testing and realizing its not really needed
	//	myString := string(msg.Payload()[:])
	//	fmt.Println(myString)
	// if err := json.Unmarshal(msg.Payload(), &m.data); err != nil {
	//	fmt.Printf("Message could not be parsed (%s): %s", msg.Payload(), err)
	//	}
	if k.p != nil {
		err := k.p.Put([]byte(msg.Payload()), kin.PartitionKey)
		k.p.Verbose = true
		if err != nil {
			logrus.WithError(err).Fatal("error producing")
		}
	}

	if STDOUTPUT {

		fmt.Printf("key: %s\nvalue: %s\n", msg.Topic(), msg.Payload())

	}
}

func main() {
	QOSINT, err := strconv.Atoi(mqc.qos)
	if err != nil {
		fmt.Println(err) //
	}

	// Create a handler that will deal with incoming messages
	h := NewHandler()
	defer h.Stop()

	// Now we establish the connection to the mqtt broker
	opts := mqtt.NewClientOptions()
	opts.AddBroker("tcp://" + mqc.broker + ":" + mqc.port)
	opts.SetClientID(mqc.clientID)

	opts.SetOrderMatters(false)       // Allow out of order messages (use this option unless in order delivery is essential)
	opts.ConnectTimeout = time.Second // Minimal delays on connect
	opts.WriteTimeout = time.Second   // Minimal delays on writes
	opts.KeepAlive = 10               // Keepalive every 10 seconds so we quickly detect network outages
	opts.PingTimeout = time.Second    // local broker so response should be quick
	opts.Username = mqc.user
	opts.Password = mqc.pass
	// Automate connection management (will keep trying to connect and will reconnect if network drops)
	opts.ConnectRetry = true
	opts.AutoReconnect = true

	// If using QOS2 and CleanSession = FALSE then it is possible that we will receive messages on topics that we
	// have not subscribed to here (if they were previously subscribed to they are part of the session and survive
	// disconnect/reconnect). Adding a DefaultPublishHandler lets us detect this.
	opts.DefaultPublishHandler = func(_ mqtt.Client, msg mqtt.Message) {
		fmt.Printf("UNEXPECTED MESSAGE: %s\n", msg)
	}

	// Log events
	opts.OnConnectionLost = func(cl mqtt.Client, err error) {
		fmt.Println("connection lost")
	}

	opts.OnConnect = func(c mqtt.Client) {
		fmt.Println("connection established to" + mqc.broker + "on port" + mqc.port)
		fmt.Println("authenticated as ", mqc.user)
		fmt.Println("currently subscribing to :", mqc.topic)
		// Establish the subscription - doing this here means that it will happen every time a connection is established
		// (useful if opts.CleanSession is TRUE or the broker does not reliably store session data)
		t := c.Subscribe(mqc.topic, byte(QOSINT), h.handle)
		// the connection handler is called in a goroutine so blocking here would hot cause an issue. However as blocking
		// in other handlers does cause problems its best to just assume we should not block
		go func() {
			_ = t.Wait() // Can also use '<-t.Done()' in releases > 1.2.0
			if t.Error() != nil {
				fmt.Printf("ERROR SUBSCRIBING: %s\n", t.Error())
			} else {
				fmt.Println("subscribed to: ", mqc.topic)
			}
		}()
	}
	opts.OnReconnecting = func(mqtt.Client, *mqtt.ClientOptions) {
		fmt.Println("attempting to reconnect")
	}

	//
	// Connect to the broker
	//
	client := mqtt.NewClient(opts)

	// If using QOS2 and CleanSession = FALSE then messages may be transmitted to us before the subscribe completes.
	// Adding routes prior to connecting is a way of ensuring that these messages are processed
	client.AddRoute(mqc.topic, h.handle)

	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	fmt.Println("Connection is up")

	// Messages will be delivered asynchronously so we just need to wait for a signal to shutdown
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	signal.Notify(sig, syscall.SIGTERM)

	<-sig
	fmt.Println("signal caught - exiting")
	client.Disconnect(1000)
	fmt.Println("shutdown complete")
}
