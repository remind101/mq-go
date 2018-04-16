package mq

import (
	"math"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

const MaxVisibilityTimeout = 43200

type Retryer interface {
	RetryDelay(receiveCount int) int // Seconds
}

type defaultRetryer struct{}

func (r *defaultRetryer) RetryDelay(receiveCount int) int {
	return int(math.Min(math.Exp2(float64(receiveCount)), float64(MaxVisibilityTimeout)))
}

var DefaultRetrier = &defaultRetryer{}

// RetryHandler will retry a message by extending its visibility timout.
// This handler should only be used with queues that have a dead letter queue.
func RetryHandler(h Handler) Handler {
	return HandlerFunc(func(c sqsiface.SQSAPI, m *Message) error {
		// Process message
		if err := h.HandleMessage(c, m); err != nil {
			return DelayVisibility(c, m)
		}

		return DeleteMessage(c, m)
	})
}

// DelayVisibility will extend the visibility of a message based on its receive count.
func DelayVisibility(c sqsiface.SQSAPI, m *Message) error {
	v := m.SQSMessage.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]
	receiveCount, _ := strconv.Atoi(*v)

	delay := m.Retryer.RetryDelay(receiveCount)
	_, err := c.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(m.QueueURL),
		ReceiptHandle:     m.SQSMessage.ReceiptHandle,
		VisibilityTimeout: aws.Int64(int64(delay)),
	})

	return err
}
