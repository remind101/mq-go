# MQ - A package for consuming SQS message queues

The goal of this project is to provide tooling to utilize SQS effectively in Go.

## Features

* Familiar `net/http` Handler interface.
* Retry with expontial backoff via visibility timeouts and dead letter queues.
* Router Handler for multiplexing messages over a single queue.
* Server with configurable concurrency and graceful shutdown.
* Automatic batch fetching and deletion.
* Opentracing support

## Documentation

https://godoc.org/github.com/remind101/mq-go

## QuickStart

``` golang
	queueURL := "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"

	h := mq.HandlerFunc(func(m *mq.Message) error {
		fmt.Printf("Received message: %s", aws.StringValue(m.SQSMessage.Body))

		// Returning no error signifies the message was processed successfully.
		// The Server will queue the message for deletion.
		return nil
	})

	// Configure mq.Server
	s := mq.NewServer(queueURL, h)

	// Start a loop to receive SQS messages and pass them to the Handler.
	s.Start()
```
