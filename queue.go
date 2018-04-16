package mq // import "github.com/remind101/mq-go"

import (
	"context"
	"errors"
	"fmt"
	"strconv"

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

// Mux will route a message based on MessageAttributes to other registered Handlers.
type Mux struct {
	Resolver func(*Message) string
	handlers map[string]Handler
}

func NewMux() *Mux {
	return &Mux{
		Resolver: func(m *Message) string {
			r := ""
			if v, ok := m.SQSMessage.MessageAttributes["route"]; ok && v.DataType == aws.String("String") {
				r = *v.StringValue
			}
			return r
		},
	}
}

func (m *Mux) Handle(route string, h Handler) {
	m.handlers[route] = h
}

func (mux *Mux) HandleMessage(c sqsiface.SQSAPI, m *Message) error {
	r := mux.Resolver(m)
	if h, ok := mux.handlers[r]; ok {
		return h.HandleMessage(c, m)
	}

	return errors.New("No route matched for message")
}

// RetryHandler retries on error.
func RetryHandler(h Handler) Handler {
	return HandlerFunc(func(c sqsiface.SQSAPI, m *Message) error {
		// Process message
		if err := h.HandleMessage(c, m); err != nil {
			if e := RetryMessage(c, m); e != nil {
				return e // Retry was unsuccessful, don't delete the message.
			}
		}

		return DeleteMessage(c, m)
	})
}

func DeleteMessage(c sqsiface.SQSAPI, m *Message) error {
	_, err := c.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(m.QueueURL),
		ReceiptHandle: m.SQSMessage.ReceiptHandle,
	})
	return err
}

func RetryMessage(c sqsiface.SQSAPI, m *Message) error {
	retryCount := 0

	// Get retry count
	if v, ok := m.SQSMessage.MessageAttributes["retry_count"]; ok && v.DataType == aws.String("Number") {
		retryCount, _ = strconv.Atoi(*v.StringValue)
	}

	if m.Retryer.ShouldRetry(retryCount) {
		delay := m.Retryer.RetryDelay(retryCount)
		m.SQSMessage.MessageAttributes["retry_count"] = &sqs.MessageAttributeValue{
			DataType:    aws.String("Number"),
			StringValue: aws.String(fmt.Sprintf("%d", retryCount+1)),
		}

		_, err := c.SendMessage(&sqs.SendMessageInput{
			QueueUrl:          aws.String(m.QueueURL),
			DelaySeconds:      aws.Int64(int64(delay)),
			MessageAttributes: m.SQSMessage.MessageAttributes,
			MessageBody:       m.SQSMessage.Body,
		})

		return err
	}

	return nil
}
