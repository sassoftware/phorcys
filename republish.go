package main

import (
	"context"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RepublishMessages connects to the broker and publishes all payloads to the default exchange
// using targetQueue as the routing key, so each message is delivered back to the original queue.
func RepublishMessages(ctx context.Context, amqpURL, targetQueue string, payloads [][]byte) error {
	conn, err := amqp.Dial(amqpURL)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer ch.Close()

	pubCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// TODO: This is temporary
	_, qErr := ch.QueueDeclare(targetQueue,
		true,
		false,
		false,
		true,
		amqp.Table{})
	if qErr != nil {
		return fmt.Errorf("Failed to recreate queue : %w", qErr)
	}

	for i, payload := range payloads {
		err = ch.PublishWithContext(pubCtx,
			"",          // default exchange
			targetQueue, // routing key = queue name
			false,
			false,
			amqp.Publishing{
				ContentType:  "application/octet-stream",
				Body:         payload,
				DeliveryMode: amqp.Persistent,
			},
		)
		if err != nil {
			return fmt.Errorf("failed publishing message %d: %w", i, err)
		}
	}

	log.Printf("[Republish] Published %d messages to queue %s", len(payloads), targetQueue)
	return nil
}
