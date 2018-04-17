package memsqs

import (
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/google/uuid"
)

const DefaultVisibilityTimeout = 30 * time.Second

// Client is an in-memory implementation of sqsiface.SQSAPI.
// It is NOT a complete implementation yet. Functionality will
// added as needed by the tests in mq-go.
type Client struct {
	sync.Mutex
	sqsiface.SQSAPI
	queues map[string][]*message
}

type message struct {
	message      *sqs.Message
	receivedTime time.Time
	visibleAfter time.Time
}

func New() *Client {
	return &Client{
		queues: map[string][]*message{},
	}
}

func (c *Client) Queue(name string) []*message {
	if q, ok := c.queues[name]; ok {
		return q
	}
	return []*message{}
}

func (c *Client) SendMessage(params *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
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

	msg.visibleAfter = msg.visibleAfter.Add(time.Duration(aws.Int64Value(params.DelaySeconds)) * time.Second)
	c.queues[aws.StringValue(params.QueueUrl)] = append(c.queues[aws.StringValue(params.QueueUrl)], msg)

	return &sqs.SendMessageOutput{}, nil
}

func (c *Client) ReceiveMessage(params *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	c.Lock()
	defer c.Unlock()

	data := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{},
	}

	q := c.Queue(aws.StringValue(params.QueueUrl))

	max := int(aws.Int64Value(params.MaxNumberOfMessages))
	if len(q) < max {
		max = len(q)
	}

	now := time.Now()
	vt := DefaultVisibilityTimeout
	if params.VisibilityTimeout != nil {
		vt = time.Duration(aws.Int64Value(params.VisibilityTimeout)) * time.Second
	}

	for _, m := range q {
		if m.visibleAfter.Unix() <= now.Unix() {
			if m.receivedTime.IsZero() {
				m.receivedTime = now
			}
			data.Messages = append(data.Messages, m.message)
			m.visibleAfter = m.receivedTime.Add(vt)
		}
		if len(data.Messages) >= max {
			return data, nil
		}
	}

	return data, nil
}

func (c *Client) ChangeMessageVisibility(params *sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error) {
	c.Lock()
	defer c.Unlock()
	data := &sqs.ChangeMessageVisibilityOutput{}

	vt := time.Duration(aws.Int64Value(params.VisibilityTimeout)) * time.Second

	q := c.Queue(aws.StringValue(params.QueueUrl))
	for _, m := range q {
		if aws.StringValue(m.message.ReceiptHandle) == aws.StringValue(params.ReceiptHandle) {
			m.visibleAfter = m.receivedTime.Add(vt)
			break
		}
	}

	return data, nil
}

func (c *Client) DeleteMessage(params *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	c.Lock()
	defer c.Unlock()
	data := &sqs.DeleteMessageOutput{}

	q := c.Queue(aws.StringValue(params.QueueUrl))
	for i, m := range q {
		if aws.StringValue(m.message.ReceiptHandle) == aws.StringValue(params.ReceiptHandle) {
			c.queues[aws.StringValue(params.QueueUrl)] = append(q[:i], q[i+1:]...)
			break
		}
	}

	return data, nil
}
