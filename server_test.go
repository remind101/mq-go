package mq_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	mq "github.com/remind101/mq-go"
	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	done := make(chan struct{})
	qURL := "jobs"
	c := &memSQSClient{}

	sp := mq.NewServer(func(s *mq.Server) {
		s.Client = c
		s.QueueURL = qURL
		s.Handler = mq.HandlerFunc(func(c sqsiface.SQSAPI, m *mq.Message) error {
			assert.Equal(t, `{"name":"test"}`, *m.SQSMessage.Body)
			close(done)
			return nil
		})
		s.ErrorHandler = func(err error) {
			t.Fatal(err)
		}
		s.Concurrency = 1
	})

	sp.Start()
	defer sp.Shutdown(context.Background())

	c.SendMessage(&sqs.SendMessageInput{
		QueueUrl:    aws.String(qURL),
		MessageBody: aws.String(`{"name":"test"}`),
	})

	<-done
}
