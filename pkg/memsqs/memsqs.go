package memsqs

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/google/uuid"
)

// DefaultVisibilityTimeout is the SQS default.
const DefaultVisibilityTimeout time.Duration = 30 * time.Second

// Client is an in-memory implementation of sqsiface.SQSAPI.
// It is NOT a complete implementation yet. Functionality will
// added as needed by the tests in mq-go.
type Client struct {
	sync.Mutex
	sqsiface.SQSAPI
	queues map[string][]*Message
}

// Message wraps an sqs.Message with metadata.
type Message struct {
	SQSMessage   *sqs.Message
	ReceivedTime time.Time
	VisibleAfter time.Time
}

// New returns a new memsqs.Client.
func New() *Client {
	return &Client{
		queues: map[string][]*Message{},
	}
}

// VisibleQueue returns only items that are currently visible.
func (c *Client) VisibleQueue(name string) []*Message {
	rq := []*Message{}
	if q, ok := c.queues[name]; ok {
		now := time.Now()
		for _, m := range q {
			if m.VisibleAfter.Before(now) {
				rq = append(rq, m)
			}
		}
	}
	return rq
}

// Queue provides direct access to a queue of messages.
func (c *Client) Queue(name string) []*Message {
	if q, ok := c.queues[name]; ok {
		return q
	}
	return []*Message{}
}

// SendMessage satisfies the sqsiface.SQSAPI interface.
func (c *Client) SendMessage(params *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	c.Lock()
	defer c.Unlock()

	if params.MessageAttributes == nil {
		params.MessageAttributes = map[string]*sqs.MessageAttributeValue{}
	}

	msg := &Message{
		SQSMessage: &sqs.Message{
			Body:              params.MessageBody,
			MessageAttributes: params.MessageAttributes,
			ReceiptHandle:     aws.String(uuid.New().String()),
			MessageId:         aws.String(uuid.New().String()),
		},
		VisibleAfter: time.Now(),
	}

	msg.VisibleAfter = msg.VisibleAfter.Add(time.Duration(aws.Int64Value(params.DelaySeconds)) * time.Second)
	c.queues[aws.StringValue(params.QueueUrl)] = append(c.queues[aws.StringValue(params.QueueUrl)], msg)

	return &sqs.SendMessageOutput{}, nil
}

// SendMessageBatch satisfies the sqsiface.SQSAPI interface.
func (c *Client) SendMessageBatch(params *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	c.Lock()
	defer c.Unlock()

	data := &sqs.SendMessageBatchOutput{
		Failed:     []*sqs.BatchResultErrorEntry{},
		Successful: []*sqs.SendMessageBatchResultEntry{},
	}

	for _, entry := range params.Entries {

		if entry.MessageAttributes == nil {
			entry.MessageAttributes = map[string]*sqs.MessageAttributeValue{}
		}

		msg := &Message{
			SQSMessage: &sqs.Message{
				Body:              entry.MessageBody,
				MessageAttributes: entry.MessageAttributes,
				ReceiptHandle:     aws.String(uuid.New().String()),
				MessageId:         aws.String(uuid.New().String()),
			},
			VisibleAfter: time.Now(),
		}

		msg.VisibleAfter = msg.VisibleAfter.Add(time.Duration(aws.Int64Value(entry.DelaySeconds)) * time.Second)
		c.queues[aws.StringValue(params.QueueUrl)] = append(c.queues[aws.StringValue(params.QueueUrl)], msg)
	}

	return data, nil
}

// ReceiveMessage satisfies the sqsiface.SQSAPI interface.
func (c *Client) ReceiveMessage(params *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	c.Lock()
	defer c.Unlock()

	data := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{},
	}

	q := c.VisibleQueue(aws.StringValue(params.QueueUrl))

	max := int(aws.Int64Value(params.MaxNumberOfMessages))
	if len(q) < max {
		max = len(q)
	}

	vt := DefaultVisibilityTimeout
	if params.VisibilityTimeout != nil {
		vt = time.Duration(aws.Int64Value(params.VisibilityTimeout)) * time.Second
	}

	for _, m := range q {
		data.Messages = append(data.Messages, m.SQSMessage)
		m.VisibleAfter = m.ReceivedTime.Add(vt)
		setMessageReceived(m, vt)

		if len(data.Messages) >= max {
			return data, nil
		}
	}

	return data, nil
}

func setMessageReceived(m *Message, vt time.Duration) {
	now := time.Now()
	// Set ReceivedTime
	if m.ReceivedTime.IsZero() {
		m.ReceivedTime = now
	}

	// Set VisibilityTimeout
	m.VisibleAfter = m.ReceivedTime.Add(vt)

	// Set ApproximateReceiveCount
	rc := 0
	if v, ok := m.SQSMessage.MessageAttributes[sqs.MessageSystemAttributeNameApproximateReceiveCount]; ok {
		rc, _ = strconv.Atoi(aws.StringValue(v.StringValue))
	}
	rc++
	rcStringValue := strconv.Itoa(rc)
	m.SQSMessage.MessageAttributes[sqs.MessageSystemAttributeNameApproximateReceiveCount] = &sqs.MessageAttributeValue{
		DataType:    aws.String("Number"),
		StringValue: aws.String(rcStringValue),
	}
}

// ChangeMessageVisibility satisfies the sqsiface.SQSAPI interface.
func (c *Client) ChangeMessageVisibility(params *sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error) {
	c.Lock()
	defer c.Unlock()
	data := &sqs.ChangeMessageVisibilityOutput{}

	vt := time.Duration(aws.Int64Value(params.VisibilityTimeout)) * time.Second

	q := c.Queue(aws.StringValue(params.QueueUrl))
	for _, m := range q {
		if aws.StringValue(m.SQSMessage.ReceiptHandle) == aws.StringValue(params.ReceiptHandle) {
			m.VisibleAfter = m.ReceivedTime.Add(vt)
			break
		}
	}

	return data, nil
}

// DeleteMessage satisfies the sqsiface.SQSAPI interface.
func (c *Client) DeleteMessage(params *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	c.Lock()
	defer c.Unlock()
	data := &sqs.DeleteMessageOutput{}

	c.deleteMessage(params.QueueUrl, params.ReceiptHandle)
	return data, nil
}

func (c *Client) deleteMessage(queueURL, receiptHandle *string) bool {
	q := c.Queue(aws.StringValue(queueURL))
	for i, m := range q {
		if aws.StringValue(m.SQSMessage.ReceiptHandle) == aws.StringValue(receiptHandle) {
			c.queues[aws.StringValue(queueURL)] = append(q[:i], q[i+1:]...)
			return true
		}
	}
	return false
}

// DeleteMessageBatch satisfies the sqsiface.SQSAPI interface.
func (c *Client) DeleteMessageBatch(params *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
	c.Lock()
	defer c.Unlock()

	data := &sqs.DeleteMessageBatchOutput{
		Failed:     []*sqs.BatchResultErrorEntry{},
		Successful: []*sqs.DeleteMessageBatchResultEntry{},
	}

	for _, e := range params.Entries {
		if c.deleteMessage(params.QueueUrl, e.ReceiptHandle) {
			data.Successful = append(data.Successful, &sqs.DeleteMessageBatchResultEntry{
				Id: e.Id,
			})
		} else {
			// NOTE: Code and error message may not be accurate here.
			data.Failed = append(data.Failed, &sqs.BatchResultErrorEntry{
				Code:        aws.String("NotFound"),
				Id:          e.Id,
				Message:     aws.String(fmt.Sprintf("ReceiptHandle %s not found.", aws.StringValue(e.ReceiptHandle))),
				SenderFault: aws.Bool(true),
			})
		}
	}

	return data, nil
}
