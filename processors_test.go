package mq_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	mq "github.com/remind101/mq-go"
	"github.com/remind101/mq-go/pkg/memsqs"
)

func TestPartitionedProcessor(t *testing.T) {
	qURL := "jobs"
	done := make(chan struct{})
	c := memsqs.New()
	numMessages := 10
	messageBodies := make([]string, numMessages)

	// Push enough messages into the queue to trigger a batch delete call.
	for i := 0; i < numMessages; i++ {
		body := fmt.Sprintf("message %d", i)
		messageBodies[i] = body

		c.SendMessage(&sqs.SendMessageInput{
			QueueUrl:    aws.String(qURL),
			MessageBody: aws.String(body),
			MessageAttributes: map[string]*sqs.MessageAttributeValue{
				mq.MessageAttributeNamePartitionKey: &sqs.MessageAttributeValue{
					StringValue: aws.String("partition 1"),
					DataType:    aws.String("String"),
				},
			},
		})
	}

	messagesReceived := 0
	messageBodiesReceived := []string{}

	h := mq.HandlerFunc(func(m *mq.Message) error {
		messagesReceived++
		messageBodiesReceived = append(messageBodiesReceived, aws.StringValue(m.SQSMessage.Body))
		if messagesReceived >= numMessages {
			close(done)
		}
		return nil
	})

	sp := newServer(t, qURL, h, c, mq.WithConcurrency(numMessages), mq.WithPartitionedProcessor)
	sp.Start()
	defer closeServer(t, sp)

	// Wait for server to receive all messages, batch deletion should
	// trigger immediately at this point.
	<-done

	assert.Equal(t, messageBodies, messageBodiesReceived)
}
