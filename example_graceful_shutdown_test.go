package mq_test

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	mq "github.com/remind101/mq-go"
)

func Example_gracefulShutdown() {
	queueURL := "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"

	h := mq.HandlerFunc(func(m *mq.Message) error {
		fmt.Printf("Received message: %s", aws.StringValue(m.SQSMessage.Body))

		// Returning no error signifies the message was processed successfully.
		// The Server will queue the message for deletion.
		return nil
	})

	// Configure mq.Server
	s := mq.NewServer(queueURL, h)

	// Handle SIGINT and SIGTERM gracefully.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// We received an interrupt signal, shut down gracefully.
		if err := s.Shutdown(ctx); err != nil {
			fmt.Printf("SQS server shutdown: %v\n", err)
		}
	}()

	// Start a loop to receive SQS messages and pass them to the Handler.
	s.Start()
}
