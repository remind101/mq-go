package mq_test

import (
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type memSQSClient struct {
	sqsiface.SQSAPI
	queues map[string][]*sqs.Message
}

func (m *memSQSClient) SendMessage(params *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
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

func (m *memSQSClient) ReceiveMessage(params *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
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
