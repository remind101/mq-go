package mq

import (
	"math"

	"github.com/aws/aws-sdk-go/aws"
)

// MaxVisibilityTimeout is the maximum allowed VisibilityTimeout by SQS.
const MaxVisibilityTimeout = 43200 // 12 hours

// RetryPolicy defines an interface to determine when to retry a Message.
type RetryPolicy interface {
	// Amount to delay the message visibility in seconds from the time the
	// message was first received, based on the number of times it has been
	// received so far.
	Delay(receiveCount int) *int64 // Seconds
}

type defaultRetryPolicy struct{}

func (r *defaultRetryPolicy) Delay(receiveCount int) *int64 {
	exp2 := math.Exp2(float64(receiveCount))
	min := math.Min(exp2, float64(MaxVisibilityTimeout))
	return aws.Int64(int64(min))
}

// DefaultRetryPolicy increases the Message VisibilityTimeout exponentially
// based on the received count up to MaxVisibilityTimeout and MaxReceives
var DefaultRetryPolicy = &defaultRetryPolicy{}
