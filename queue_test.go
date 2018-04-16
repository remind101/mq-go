package mq_test

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/google/uuid"
)

type memSQSClient struct {
	sync.Mutex
	sqsiface.SQSAPI
	queues map[string][]*message
}

type message struct {
	message      *sqs.Message
	visibleAfter time.Time
}

func NewMemSQSClient() *memSQSClient {
	return &memSQSClient{
		queues: map[string][]*message{},
	}
}

func (c *memSQSClient) SendMessage(params *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	c.Lock()
	defer c.Unlock()

	msg := &message{
		message: &sqs.Message{
			Body:              params.MessageBody,
			MessageAttributes: params.MessageAttributes,
			ReceiptHandle:     aws.String(uuid.New().String()),
		},
		visibleAfter: time.Now(),
	}

	if params.DelaySeconds != nil {
		msg.visibleAfter = msg.visibleAfter.Add(time.Duration(*params.DelaySeconds) * time.Second)
	}

	c.queues[*params.QueueUrl] = append(c.queues[*params.QueueUrl], msg)

	return &sqs.SendMessageOutput{}, nil
}

func (c *memSQSClient) ReceiveMessage(params *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	c.Lock()
	defer c.Unlock()

	data := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{},
	}

	if c.queues == nil {
		return data, nil
	}

	if q, ok := c.queues[*params.QueueUrl]; ok {
		max := int(*params.MaxNumberOfMessages)
		if len(q) < max {
			max = len(q)
		}

		now := time.Now()
		vt := 30 * time.Second
		if params.VisibilityTimeout != nil {
			vt = time.Duration(*params.VisibilityTimeout) * time.Second
		}

		for _, m := range q {
			if m.visibleAfter.Unix() <= now.Unix() {
				data.Messages = append(data.Messages, m.message)
				m.visibleAfter = now.Add(vt)
			}
			if len(data.Messages) >= max {
				return data, nil
			}
		}
	}

	return data, nil
}

func (c *memSQSClient) DeleteMessage(params *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	c.Lock()
	defer c.Unlock()
	data := &sqs.DeleteMessageOutput{}

	if q, ok := c.queues[*params.QueueUrl]; ok {
		for i, m := range q {
			if *m.message.ReceiptHandle == *params.ReceiptHandle {
				c.queues[*params.QueueUrl] = append(q[:i], q[i+1:]...)
				break
			}
		}
	}

	return data, nil
}
