// Package amqp provide auto reconnect for rabbitmq
package amqp

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

// RabbitMQ contains Connection and Channel
type RabbitMQ struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	queue      amqp.Queue
	dsn        string
	delay      time.Duration
	done       chan bool
}

// New connects to RabbitMQ and open a channel
func New(user, password, host, port string, reconnectTime time.Duration) (*RabbitMQ, error) {
	dsn := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, password, host, port)

	conn, connErr := amqp.Dial(dsn)
	if connErr != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", connErr)
	}

	ch, chErr := conn.Channel()
	if chErr != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", chErr)
	}

	done := make(chan bool)

	rabbitMQ := &RabbitMQ{Connection: conn, Channel: ch, dsn: dsn, delay: reconnectTime, done: done}

	go rabbitMQ.reconnectConnection()
	go rabbitMQ.reconnectChannel()

	return rabbitMQ, nil
}

// ExchangeDeclare declare an exchange
func (r *RabbitMQ) ExchangeDeclare(name, exchangeType string, durable, autoDelete, internal, noWait bool, arguments amqp.Table) error {
	err := r.Channel.ExchangeDeclare(name, exchangeType, durable, autoDelete, internal, noWait, arguments)
	if err != nil {
		return fmt.Errorf("failed to declare an exchange: %v", err)
	}
	return nil
}

// QueueDeclare declare a queue
func (r *RabbitMQ) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, arguments amqp.Table) error {
	queue, queueErr := r.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, arguments)
	if queueErr != nil {
		return fmt.Errorf("failed to declare a queue: %v", queueErr)
	}
	r.queue = queue
	return nil
}

// QueueBind bind a queue
func (r *RabbitMQ) QueueBind(key, exchangeName string, noWait bool, arguments amqp.Table) error {
	err := r.Channel.QueueBind(r.queue.Name, key, exchangeName, noWait, arguments)
	if err != nil {
		return fmt.Errorf("failed to bind a queue: %v", err)
	}
	return nil
}

// Qos controls how many messages or how many bytes the server will try to keep on
// the network for consumers before receiving delivery ack
func (r *RabbitMQ) Qos(prefetchCount, prefetchSize int, global bool) error {
	err := r.Channel.Qos(prefetchCount, prefetchSize, global)
	if err != nil {
		return fmt.Errorf("failed to set Qos: %v", err)
	}
	return nil
}

// Consume starts delivering queued messages
func (r *RabbitMQ) Consume(consumer string, autoAck, exclusive, noLocal, noWait bool, arguments amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			messages, messagesErr := r.Channel.Consume(r.queue.Name, consumer, autoAck, exclusive, noLocal, noWait, arguments)
			if messagesErr != nil {
				log.Printf("failed to register a consumer: %v", messagesErr)
				if <-r.done {
					log.Println("consume closed by developer")
					break
				}
				time.Sleep(r.delay)
				continue
			}

			for message := range messages {
				deliveries <- message
			}
		}
	}()

	return deliveries, nil
}
