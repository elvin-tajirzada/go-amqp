package amqp

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"time"
)

// RabbitMQ struct defines Connection, Channel, queue, delay
type RabbitMQ struct {
	Connection *amqp.Connection
	Channel    *amqp.Channel
	queue      amqp.Queue
	delay      time.Duration
}

// Init function connect to RabbitMQ and open a channel
func Init(user, password, host, port string, reconnectTime time.Duration) (*RabbitMQ, error) {
	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, password, host, port)

	conn, connErr := amqp.Dial(url)
	if connErr != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", connErr)
	}

	ch, chErr := conn.Channel()
	if chErr != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", chErr)
	}

	rabbitMQ := &RabbitMQ{Connection: conn, Channel: ch, delay: reconnectTime}

	go reconnectConnection(rabbitMQ, url)
	go reconnectChannel(rabbitMQ)

	return rabbitMQ, nil
}

// ExchangeDeclare function declare an exchange
func (r *RabbitMQ) ExchangeDeclare(name, exchangeType string, durable, autoDelete, internal, noWait bool, arguments amqp.Table) error {
	err := r.Channel.ExchangeDeclare(name, exchangeType, durable, autoDelete, internal, noWait, arguments)
	if err != nil {
		return fmt.Errorf("failed to declare an exchange: %v", err)
	}
	return nil
}

// QueueDeclare function declare a queue
func (r *RabbitMQ) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, arguments amqp.Table) error {
	queue, queueErr := r.Channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, arguments)
	if queueErr != nil {
		return fmt.Errorf("failed to declare a queue: %v", queueErr)
	}
	r.queue = queue
	return nil
}

// QueueBind function bind a queue
func (r *RabbitMQ) QueueBind(key, exchangeName string, noWait bool, arguments amqp.Table) error {
	err := r.Channel.QueueBind(r.queue.Name, key, exchangeName, noWait, arguments)
	if err != nil {
		return fmt.Errorf("failed to bind a queue: %v", err)
	}
	return nil
}

// Qos function controls how many messages or how many bytes the server will try to keep on
// the network for consumers before receiving delivery ack
func (r *RabbitMQ) Qos(prefetchCount, prefetchSize int, global bool) error {
	err := r.Channel.Qos(prefetchCount, prefetchSize, global)
	if err != nil {
		return fmt.Errorf("failed to set Qos: %v", err)
	}
	return nil
}

// Consume function starts delivering queued messages
func (r *RabbitMQ) Consume(consumer string, autoAck, exclusive, noLocal, noWait bool, arguments amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			messages, messagesErr := r.Channel.Consume(r.queue.Name, consumer, autoAck, exclusive, noLocal, noWait, arguments)
			if messagesErr != nil {
				log.Printf("failed to register a consumer: %v", messagesErr)
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
