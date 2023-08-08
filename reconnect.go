package amqp

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

// reconnectConnection reconnect a connection
func (r *RabbitMQ) reconnectConnection() {
	for {
		chanError := make(chan *amqp.Error)

		closeReason, ok := <-r.Connection.NotifyClose(chanError)
		if !ok {
			log.Println("connection closed by developer")
			break
		}

		log.Printf("reason for the connection closed: %v\n", closeReason)

		for {
			time.Sleep(r.delay)

			conn, connErr := amqp.Dial(r.dsn)
			if connErr == nil {
				r.Connection = conn
				log.Println("connection successfully reconnected")
				break
			}

			log.Printf("failed to reconnect connection: %v\n", connErr)
		}
	}
}

// reconnectChannel reconnect a channel
func (r *RabbitMQ) reconnectChannel() {
	for {
		chanError := make(chan *amqp.Error)

		closeReason, ok := <-r.Channel.NotifyClose(chanError)

		if !ok {
			r.done <- true
			log.Println("channel closed by developer")
			break
		}

		log.Printf("reason for the channel closed: %v\n", closeReason)

		for {
			time.Sleep(r.delay)

			ch, chErr := r.Connection.Channel()
			if chErr == nil {
				r.Channel = ch
				log.Println("channel successfully reconnected")
				r.done <- false
				break
			}

			log.Printf("failed to reconnect channel: %v\n", chErr)
		}
	}
}
