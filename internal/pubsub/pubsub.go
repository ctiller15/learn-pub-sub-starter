package pubsub

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)

	err := encoder.Encode(val)
	if err != nil {
		return err
	}

	ctx := context.Background()

	msg := amqp.Publishing{
		ContentType: "application/gob",
		Body:        buf.Bytes(),
	}

	err = ch.PublishWithContext(ctx, exchange, key, false, false, msg)
	if err != nil {
		return err
	}

	return nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	bytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	ctx := context.Background()

	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        bytes,
	}

	err = ch.PublishWithContext(ctx, exchange, key, false, false, msg)
	if err != nil {
		return err
	}

	return nil
}

type SimpleQueueType int

const (
	QueueTypeDurable SimpleQueueType = iota
	QueueTypeTransient
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType, // "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	var durable bool
	var autoDelete bool
	var exclusive bool

	if simpleQueueType == QueueTypeDurable {
		durable = true
	} else if simpleQueueType == QueueTypeTransient {
		autoDelete = true
		exclusive = true
	}

	table := amqp.Table{
		"x-dead-letter-exchange": routing.ExchangePerilDLX,
	}

	newQueue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,
		table,
	)

	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(newQueue.Name, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, newQueue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		simpleQueueType,
	)

	if err != nil {
		return err
	}

	deliveryCh, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	go func() {
		for delivery := range deliveryCh {
			message := delivery.Body

			fmt.Printf("Received raw message: %s\n", string(message))

			strMessage := string(message)
			var decodedMessage []byte
			if len(strMessage) >= 2 && strMessage[0] == '"' && strMessage[len(strMessage)-1] == '"' {
				strMessage = strMessage[1 : len(strMessage)-1]

				// Check here on decoding.
				// Decode base64
				decodedBytes, err := base64.StdEncoding.DecodeString(strMessage)
				if err != nil {
					fmt.Printf("Error decoding base64: %v\n", err)
					delivery.Ack(false)
					continue
				}
				decodedMessage = decodedBytes
			} else {
				decodedMessage = []byte(strMessage)
			}

			var container T
			err = json.Unmarshal(decodedMessage, &container)

			if err != nil {
				// Handle error - at least log it
				fmt.Printf("Error unmarshaling message: %v\n", err)
				delivery.Ack(false) // Still acknowledge to remove from queue
				continue
			}

			result := handler(container)

			switch result {
			case Ack:
				err = delivery.Ack(false)
				if err != nil {
					fmt.Printf("Error acknowledging delivery: %v\n", err)

				}
			case NackDiscard:
				err = delivery.Nack(false, false)
				if err != nil {
					fmt.Printf("Error acknowledging delivery: %v\n", err)
				}
			case NackRequeue:
				err = delivery.Nack(false, true)
				if err != nil {
					fmt.Printf("Error acknowledging delivery: %v\n", err)
				}
			}
		}
	}()

	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	simpleQueueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		simpleQueueType,
	)

	if err != nil {
		return err
	}

	ch.Qos(10, 0, false)

	deliveryCh, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)

	go func() {
		for delivery := range deliveryCh {
			message := delivery.Body

			fmt.Printf("Received raw message: %s\n", string(message))

			buf := bytes.NewBuffer(message)
			decoder := gob.NewDecoder(buf)

			var container T
			err := decoder.Decode(&container)
			if err != nil {
				fmt.Printf("Error decoding gob: %v\n", err)
				delivery.Ack(false)
				continue
			}

			result := handler(container)

			switch result {
			case Ack:
				err = delivery.Ack(false)
				if err != nil {
					fmt.Printf("Error acknowledging delivery: %v\n", err)

				}
			case NackDiscard:
				err = delivery.Nack(false, false)
				if err != nil {
					fmt.Printf("Error acknowledging delivery: %v\n", err)
				}
			case NackRequeue:
				err = delivery.Nack(false, true)
				if err != nil {
					fmt.Printf("Error acknowledging delivery: %v\n", err)
				}
			}
		}
	}()

	return nil
}
