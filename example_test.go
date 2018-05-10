package mq_test

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	mq "github.com/remind101/mq-go"
)

func Example() {
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
}
