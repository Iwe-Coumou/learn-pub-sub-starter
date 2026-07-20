package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

type SimpleQueueType int

const (
	Durable SimpleQueueType = iota
	Transient
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: data})
	if err != nil {
		return err
	}
	return nil
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	var isDurable bool
	if queueType == Durable {
		isDurable = true
	}

	q, err := ch.QueueDeclare(queueName, isDurable, !isDurable, !isDurable, false, amqp.Table{"x-dead-letter-exchange": "peril_dlx"})
	if err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	if err := ch.QueueBind(queueName, key, exchange, false, nil); err != nil {
		return &amqp.Channel{}, amqp.Queue{}, err
	}

	return ch, q, nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T, *amqp.Channel) AckType) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveryChan, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			var data T
			if err := json.Unmarshal(delivery.Body, &data); err != nil {
				continue
			}

			ackType := handler(data, ch)
			switch ackType {
			case Ack:
				fmt.Println("Ack")
				delivery.Ack(false)
			case NackRequeue:
				fmt.Println("NackRequeue")
				delivery.Nack(false, true)
			case NackDiscard:
				fmt.Println("NackDiscard")
				delivery.Nack(false, false)
			}
		}
	}()

	return nil
}
