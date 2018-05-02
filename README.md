# MQ - A package for consuming SQS message queues

The goal of this project is to provide tooling to utilize SQS effectively in Go.

## Features

* Familiar `net/http` Handler interface.
* Retry with expontial backoff via visibility timeouts and dead letter queues.
* Router Handler for multiplexing messages over a single queue.
* Server with configurable concurrency and graceful shutdown.
* Automatic batch fetching and deletion.

## QuickStart

``` golang
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/remind101/mq-go"
)

func main() {
	queueURL := "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"

	h := mq.HandlerFunc(func(m *mq.Message) error {
		fmt.Printf("Received message: %s", aws.String(m.SQSMessage.Body))

		// Returning no error signifies the message was processed successfully.
		// The Server will queue the message for deletion.
		return nil
	})

	// Configure mq.Server
	s := mq.NewServer(aws.String(queueURL), h)

	// Handle SIGINT and SIGTERM gracefully.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigCh

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
```
