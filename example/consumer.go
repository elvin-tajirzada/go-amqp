package main

import (
	"github.com/elvin-tacirzade/go-amqp"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	rabbitMQUser := "test"
	rabbitMQPassword := "12345678"
	rabbitMQHost := "localhost"
	rabbitMQPort := "5672"
	reconnectTime := time.Second * 10
	prefetchCount := 5
	var deliveries []string

	rabbitMQ, err := amqp.Init(rabbitMQUser, rabbitMQPassword, rabbitMQHost, rabbitMQPort, reconnectTime)
	if err != nil {
		log.Fatal(err)
	}

	exchangeErr := rabbitMQ.ExchangeDeclare("test", "fanout", true, false, false, false, nil)
	if exchangeErr != nil {
		log.Fatal(exchangeErr)
	}

	queueErr := rabbitMQ.QueueDeclare("test", true, false, false, false, nil)
	if queueErr != nil {
		log.Fatal(queueErr)
	}

	queueBindErr := rabbitMQ.QueueBind("", "test", false, nil)
	if queueBindErr != nil {
		log.Fatal(queueBindErr)
	}

	qosErr := rabbitMQ.Qos(prefetchCount, 0, false)
	if qosErr != nil {
		log.Fatal(qosErr)
	}

	messages, messagesErr := rabbitMQ.Consume("", false, false, false, false, nil)
	if messagesErr != nil {
		log.Fatal(messagesErr)
	}

	go func() {
		for message := range messages {

			deliveries = append(deliveries, string(message.Body))

			if len(deliveries) == prefetchCount {
				log.Println(deliveries)
				message.Ack(true)
				deliveries = []string{}
			}
		}
	}()

	log.Println("[*] Waiting for logs. To exit press CTRL+C")
	checkSignal(rabbitMQ)
}

func checkSignal(rabbitMQ *amqp.RabbitMQ) {
	signals := make(chan os.Signal)
	done := make(chan bool)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-signals

		channelCloseErr := rabbitMQ.Channel.Close()
		if channelCloseErr != nil {
			log.Fatal(channelCloseErr)
		}

		connCloseErr := rabbitMQ.Connection.Close()
		if connCloseErr != nil {
			log.Fatal(connCloseErr)
		}

		log.Printf("Received signal %v\n", sig)
		done <- true
	}()

	<-done
	log.Println("Stopping consumer")
}
