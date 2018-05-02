package mq

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// Message wraps an sqs.Message.
type Message struct {
	QueueURL    string
	SQSMessage  *sqs.Message
	RetryPolicy RetryPolicy

	client sqsiface.SQSAPI
	ctx    context.Context
}

// NewMessage returns a fully initialized Message.
func NewMessage(queueURL string, sqsMessage *sqs.Message, client sqsiface.SQSAPI) *Message {
	return &Message{
		QueueURL:    queueURL,
		SQSMessage:  sqsMessage,
		RetryPolicy: DefaultRetryPolicy,
		client:      client,
		ctx:         context.Background(),
	}
}

// Delete removes the message from the queue. Use is discouraged however, since
// the Server will handle message deletion more efficiently.
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

// WithContext returns a shallow copy of the message its context changed to ctx.
func (m *Message) WithContext(ctx context.Context) *Message {
	if ctx == nil {
		panic("nil context")
	}
	m2 := new(Message)
	*m2 = *m
	m2.ctx = ctx

	return m2
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
