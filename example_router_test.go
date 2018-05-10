package mq_test

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/aws/aws-sdk-go/aws"
	mq "github.com/remind101/mq-go"
)

func Example_router() {
	queueURL := "https://sqs.us-east-2.amazonaws.com/123456789012/MyQueue"

	r := mq.NewRouter()
	r.Handle("foo-jobs", mq.HandlerFunc(func(m *mq.Message) error {
		fmt.Printf("Received foo message: %s", aws.StringValue(m.SQSMessage.Body))
		return nil
	}))

	r.Handle("bar-jobs", mq.HandlerFunc(func(m *mq.Message) error {
		fmt.Printf("Received bar message: %s", aws.StringValue(m.SQSMessage.Body))
		return nil
	}))

	// Configure mq.Server
	s := mq.NewServer(queueURL, r)

	// Start a loop to receive SQS messages and pass them to the Handler.
	go s.Start()

	// Publish a foo message
	publish(s.Client, queueURL, "foo-jobs", "this will route to foo-jobs handler func")

	// Publish a bar message
	publish(s.Client, queueURL, "bar-jobs", "this will route to bar-jobs handler func")
}

func publish(client sqsiface.SQSAPI, queueURL, route, message string) error {
	input := &sqs.SendMessageInput{
		QueueUrl: aws.String(queueURL),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			mq.MessageAttributeNameRoute: &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(route),
			},
		},
		MessageBody: aws.String(message),
	}

	_, err := client.SendMessage(input)
	return err
}
