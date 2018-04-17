// +build integration

package mq_test

import (
	"context"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	mq "github.com/remind101/mq-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func TestRouterIntegration(t *testing.T) {
	c := newClient()

	// Create a new queue.
	queueURL := createQueue(t, c, "jobs")

	// Send a message.
	sendMessage(t, c, queueURL, aws.String("worker.1"), aws.String("do some work"))

	done := make(chan struct{})

	// Init handler.
	r := mq.NewRouter()

	// Route certain messages to a specific handler.
	r.Handle("worker.1", mq.HandlerFunc(func(m *mq.Message) (err error) {
		defer func() {
			err = m.Delete()
			assert.NoError(t, err)
		}
		assert.Equal(t, "do some work", aws.StringValue(m.SQSMessage.Body))
		close(done)
		return
	}))

	// Run server until message is received.
	s := mq.NewServer(*queueURL, r, mq.WithClient(c))
	s.Start()
	defer s.Shutdown(context.Background())

	<-done
}

func newClient() sqsiface.SQSAPI {
	endpoint := os.Getenv("ELASTICMQ_URL")
	return sqs.New(session.New(&aws.Config{
		Endpoint:    aws.String(endpoint),
		Region:      aws.String("local"),
		Credentials: credentials.NewStaticCredentials("id", "secret", "token"),
	}))
}

func createQueue(t *testing.T, c sqsiface.SQSAPI, name string) *string {
	out, err := c.CreateQueue(&sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	require.NoError(t, err)

	_, err = c.PurgeQueue(&sqs.PurgeQueueInput{QueueUrl: out.QueueUrl})
	require.NoError(t, err)

	return out.QueueUrl
}

func sendMessage(t *testing.T, c sqsiface.SQSAPI, qURL, route, body *string) {
	_, err := c.SendMessage(&sqs.SendMessageInput{
		QueueUrl: qURL,
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			mq.MessageAttributeNameRoute: &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: route,
			},
		},
		MessageBody: body,
	})
	require.NoError(t, err)
}
