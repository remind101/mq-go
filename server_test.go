package mq_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	mq "github.com/remind101/mq-go"
	"github.com/remind101/mq-go/pkg/memsqs"
	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	qURL := "jobs"
	done := make(chan struct{})
	c := memsqs.New()

	c.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String(`{"name":"test"}`),
	})

	h := mq.HandlerFunc(func(m *mq.Message) error {
		assert.Equal(t, `{"name":"test"}`, aws.StringValue(m.SQSMessage.Body))
		close(done)
		return nil
	})

	sp := mq.NewServer(qURL, h, func(s *mq.Server) {
		s.Client = c
		s.ErrorHandler = func(err error) {
			t.Fatal(err)
		}
	})

	sp.Start()
	<-done             // Wait for server to handle message
	closeServer(t, sp) // Wait for server to shutdown
	assert.Empty(t, len(c.Queue(qURL)))
}

func TestServerWaitsForProcessingToFinish(t *testing.T) {
	qURL := "jobs"
	done := make(chan struct{})
	c := memsqs.New()

	c.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String(`{"name":"test"}`),
	})

	h := mq.HandlerFunc(func(m *mq.Message) error {
		close(done)
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	sp := mq.NewServer(qURL, h, func(s *mq.Server) {
		s.Client = c
		s.ErrorHandler = func(err error) {
			t.Fatal(err)
		}
	})

	sp.Start()
	<-done             // Wait for server to receive message
	closeServer(t, sp) // Wait for server to shutdown
	assert.Empty(t, len(c.Queue(qURL)))
}

func closeServer(t *testing.T, s *mq.Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	assert.NoError(t, s.Shutdown(ctx))
}
