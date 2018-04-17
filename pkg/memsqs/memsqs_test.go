package memsqs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/remind101/mq-go/pkg/memsqs"
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
		MessageBody: aws.String(""),
	})
	require.NoError(t, err)

}
