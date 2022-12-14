package amqp

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

// reconnectConnection function reconnect a connection
func reconnectConnection(rabbitMQ *RabbitMQ, url string) {
	for {
		chanError := make(chan *amqp.Error)

		closeReason, ok := <-rabbitMQ.Connection.NotifyClose(chanError)
		if !ok {
			log.Println("connection closed by developer")
			break
		}

		log.Printf("reason for the connection closed: %v\n", closeReason)

		for {
			time.Sleep(rabbitMQ.delay)

			conn, connErr := amqp.Dial(url)
			if connErr == nil {
				rabbitMQ.Connection = conn
				log.Println("connection successfully reconnected")
				break
			}

			log.Printf("failed to reconnect connection: %v\n", connErr)
		}
	}
}

// reconnectChannel function reconnect a channel
func reconnectChannel(rabbitMQ *RabbitMQ) {
	for {
		chanError := make(chan *amqp.Error)

		closeReason, ok := <-rabbitMQ.Channel.NotifyClose(chanError)

		if !ok {
			rabbitMQ.done <- true
			log.Println("channel closed by developer")
			break
		}

		log.Printf("reason for the channel closed: %v\n", closeReason)

		for {
			time.Sleep(rabbitMQ.delay)

			ch, chErr := rabbitMQ.Connection.Channel()
			if chErr == nil {
				rabbitMQ.Channel = ch
				log.Println("channel successfully reconnected")
				rabbitMQ.done <- false
				break
			}

			log.Printf("failed to reconnect channel: %v\n", chErr)
		}
	}
}
