package mq // import "github.com/remind101/mq-go"

import (
	"context"
	"strconv"

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
	QueueURL   string
	SQSMessage *sqs.Message
	Retryer    Retryer

	client sqsiface.SQSAPI
	ctx    context.Context
}

// Delete removes the message from the queue.
func (m *Message) Delete() error {
	return deleteMessage(m.client, m)
}

// ChangeVisibility changes the VisibilityTimeout to timeout seconds.
func (m *Message) ChangeVisibility(timeout *int64) error {
	return changeMessageVisibility(m.client, m, timeout)
}

// Context returns the message context.
func (m *Message) Context() context.Context {
	return m.ctx
}

// DelayVisibility will extend the visibility of a message based on its retrier and receive count.
func DelayVisibility(m *Message) error {
	v := m.SQSMessage.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]
	receiveCount, _ := strconv.Atoi(*v)

	delay := m.Retryer.RetryDelay(receiveCount)
	return m.ChangeVisibility(aws.Int64(int64(delay)))
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
