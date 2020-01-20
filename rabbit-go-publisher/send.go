package main

import (
	"encoding/json"
	"fmt"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"
)

type QuoteMessage struct {
	Message string `json:"message"`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func getMessages() []QuoteMessage {
	var quoteMessages []QuoteMessage
	raw, err := ioutil.ReadFile("./data.json")
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	json.Unmarshal(raw, &quoteMessages)
	return quoteMessages
}

func (message QuoteMessage) toString() string {
	bytes, err := json.Marshal(message)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	return string(bytes)
}

func main() {
	//connect to RabbitMQ server
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()
	//create a channel, which is where most of the API for getting things done resides:
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()
	// declare a queue for us to send to; then we can publish a message to the queue
	// Declaring a queue is idempotent - it will only be created if it doesn't exist already
	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")
	// read data file
	messages := getMessages()
	// initialize global pseudo random generator
	rand.Seed(time.Now().Unix())
	
	for {
		message := messages[rand.Intn(len(messages))]
		//fmt.Println(message.Message)
		log.Printf("Sending a message: %s", message.toString())
		err = ch.Publish(
			"",     // exchange
			q.Name, // routing key
			false,  // mandatory
			false,  // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte(message.toString()),
			})
		failOnError(err, "Failed to publish a message")
		time.Sleep(time.Second)
	}
}
