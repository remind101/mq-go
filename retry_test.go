package mq_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	mq "github.com/remind101/mq-go"
	"github.com/stretchr/testify/assert"
)

func TestRetry(t *testing.T) {
	tests := []struct {
		ReceiveCount int
		Delay        *int64
	}{
		{
			ReceiveCount: 1,
			Delay:        aws.Int64(2),
		},
		{
			ReceiveCount: 2,
			Delay:        aws.Int64(4),
		},
		{
			ReceiveCount: 3,
			Delay:        aws.Int64(8),
		},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.Delay, mq.DefaultRetryPolicy.Delay(tt.ReceiveCount))
	}
}
