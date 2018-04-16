package mq // import "github.com/remind101/mq-go"

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// A Handler processes a Message.
type Handler interface {
	HandleMessage(sqsiface.SQSAPI, *Message) error
}

// HandlerFunc is an adaptor to allow the use of ordinary functions as message Handlers.
type HandlerFunc func(sqsiface.SQSAPI, *Message) error

func (h HandlerFunc) HandleMessage(c sqsiface.SQSAPI, m *Message) error {
	return h(c, m)
}

// Message wraps an sqs.Message.
type Message struct {
	QueueURL   string
	SQSMessage *sqs.Message
	Retryer    Retryer

	ctx context.Context
}

func (m *Message) Context() context.Context {
	return m.ctx
}

func DeleteMessage(c sqsiface.SQSAPI, m *Message) error {
	_, err := c.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(m.QueueURL),
		ReceiptHandle: m.SQSMessage.ReceiptHandle,
	})
	return err
}
