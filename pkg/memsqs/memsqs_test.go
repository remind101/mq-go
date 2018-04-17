package memsqs_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/remind101/mq-go/pkg/memsqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendMessage(t *testing.T) {
	c := memsqs.New()
	_, err := c.SendMessage(&sqs.SendMessageInput{
		QueueUrl: aws.String("jobs"),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"group": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String("/jobs/priority"),
			},
		},
		MessageBody: aws.String("hello"),
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(c.Queue("jobs")))
	assert.Equal(t, "hello", aws.StringValue(c.Queue("jobs")[0].SQSMessage.Body))
}

func TestReceiveMessage(t *testing.T) {
	c := memsqs.New()

	// Send a message.
	_, err := c.SendMessage(&sqs.SendMessageInput{
		QueueUrl: aws.String("jobs"),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"group": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String("/jobs/priority"),
			},
		},
		MessageBody: aws.String("job1"),
	})
	require.NoError(t, err)

	// Receive message.
	out, err := c.ReceiveMessage(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(1),
		QueueUrl:            aws.String("jobs"),
		VisibilityTimeout:   aws.Int64(1),
	})

	// Assert message no longer visible.
	require.Equal(t, 1, len(out.Messages))
	require.Equal(t, 1, len(c.Queue("jobs")))
	require.Equal(t, 0, len(c.VisibleQueue("jobs")))

	assert.Equal(t, "job1", aws.StringValue(out.Messages[0].Body))
	receiveCount, ok := out.Messages[0].MessageAttributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]
	require.True(t, ok)
	assert.Equal(t, "1", aws.StringValue(receiveCount.StringValue))

	time.Sleep(1 * time.Second)
	time.Sleep(1 * time.Millisecond)

	// Assert message is visible again.
	require.Equal(t, 1, len(c.VisibleQueue("jobs")))

	// Assert receive count increments.
	out, err = c.ReceiveMessage(&sqs.ReceiveMessageInput{
		MaxNumberOfMessages: aws.Int64(1),
		QueueUrl:            aws.String("jobs"),
		VisibilityTimeout:   aws.Int64(1),
	})
	receiveCount, ok = out.Messages[0].MessageAttributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]
	require.True(t, ok)
	assert.Equal(t, "2", aws.StringValue(receiveCount.StringValue))

	// Delete message.
	_, err = c.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String("jobs"),
		ReceiptHandle: out.Messages[0].ReceiptHandle,
	})
	require.NoError(t, err)

	// Assert message no longer exists.
	require.Equal(t, 0, len(c.VisibleQueue("jobs")))
	require.Equal(t, 0, len(c.Queue("jobs")))
}
