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

type mockSQSClient struct {
	sqsiface.SQSAPI
	queues map[string][]*sqs.Message
}

func (m *mockSQSClient) SendMessage(params *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	if len(m.queues) == 0 {
		m.queues = map[string][]*sqs.Message{}
	}

	message := &sqs.Message{
		Body:              params.MessageBody,
		MessageAttributes: params.MessageAttributes,
	}

	m.queues[*params.QueueUrl] = append(m.queues[*params.QueueUrl], message)

	return &sqs.SendMessageOutput{}, nil
}

func (m *mockSQSClient) ReceiveMessage(params *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	data := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{},
	}

	if m.queues == nil {
		return data, nil
	}

	if q, ok := m.queues[*params.QueueUrl]; ok {
		n := int(*params.MaxNumberOfMessages)
		if len(q) < n {
			n = len(q)
		}

		data.Messages = q[:n]
		m.queues[*params.QueueUrl] = q[n:]
	}

	return data, nil
}
func TestConsumerPool(t *testing.T) {
	done := make(chan struct{})
	qURL := "jobs"
	c := &mockSQSClient{}

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
