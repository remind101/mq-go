package mq_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	mq "github.com/remind101/mq-go"
	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	done := make(chan struct{})
	qURL := "jobs"
	c := NewMemSQSClient()

	h := mq.HandlerFunc(func(m *mq.Message) error {
		assert.Equal(t, `{"name":"test"}`, *m.SQSMessage.Body)
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
	defer sp.Shutdown(context.Background())

	c.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String(`{"name":"test"}`),
	})

	<-done
}
