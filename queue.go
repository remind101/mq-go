package mq // import "github.com/remind101/mq-go"

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// A Handler processes a Message.
type Handler interface {
	HandleMessage(*Message) error
}

// HandlerFunc is an adaptor to allow the use of ordinary functions as message Handlers.
type HandlerFunc func(*Message) error

func (h HandlerFunc) HandleMessage(m *Message) error {
	return h(m)
}

// Message wraps an sqs.Message.
type Message struct {
	QueueURL    string
	SQSMessage  *sqs.Message
	RetryPolicy RetryPolicy

	client sqsiface.SQSAPI
	ctx    context.Context
}

// Delete removes the message from the queue.
func (m *Message) Delete() error {
	return deleteMessage(mustClient(m.client), m)
}

// ChangeVisibility changes the VisibilityTimeout to timeout seconds.
func (m *Message) ChangeVisibility(timeout *int64) error {
	return changeMessageVisibility(mustClient(m.client), m, timeout)
}

// Context returns the message context.
func (m *Message) Context() context.Context {
	return m.ctx
}

func deleteMessage(c sqsiface.SQSAPI, m *Message) error {
	_, err := c.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(m.QueueURL),
		ReceiptHandle: m.SQSMessage.ReceiptHandle,
	})
	return err
}

func changeMessageVisibility(c sqsiface.SQSAPI, m *Message, timeout *int64) error {
	_, err := c.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(m.QueueURL),
		ReceiptHandle:     m.SQSMessage.ReceiptHandle,
		VisibilityTimeout: timeout,
	})

	return err
}

func mustClient(c sqsiface.SQSAPI) sqsiface.SQSAPI {
	if c == nil {
		panic("client is nil")
	}
	return c
}
