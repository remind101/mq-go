package mq_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/stretchr/testify/assert"

	mq "github.com/remind101/mq-go"
)

type mockSQSClient struct {
	sqsiface.SQSAPI
}

func TestRouter(t *testing.T) {
	r := mq.NewRouter()
	r.Handle("foo", mq.HandlerFunc(func(c sqsiface.SQSAPI, m *mq.Message) error { return nil }))

	// No routing key
	err := r.HandleMessage(&mockSQSClient{}, &mq.Message{
		SQSMessage: &sqs.Message{},
	})
	assert.Error(t, err)
	assert.Equal(t, "no routing key for message", err.Error())

	// Routing key does not match
	err = r.HandleMessage(&mockSQSClient{}, &mq.Message{
		SQSMessage: &sqs.Message{
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				mq.MessageAttributeNameRoute: &sqs.MessageAttributeValue{
					StringValue: aws.String("bar"),
				},
			},
		},
	})
	assert.Error(t, err)
	assert.Equal(t, "no handler matched for routing key: bar", err.Error())

	// Matching routing key
	err = r.HandleMessage(&mockSQSClient{}, &mq.Message{
		SQSMessage: &sqs.Message{
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				mq.MessageAttributeNameRoute: &sqs.MessageAttributeValue{
					StringValue: aws.String("foo"),
				},
			},
		},
	})
	assert.NoError(t, err)
}
