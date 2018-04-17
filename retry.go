package mq

import (
	"math"

	"github.com/aws/aws-sdk-go/aws"
)

const MaxVisibilityTimeout = 43200
const MaxReceives = 15

type RetryPolicy interface {
	Delay(receiveCount int) *int64 // Seconds
	Exhausted(receiveCount int) bool
}

type defaultRetryPolicy struct{}

func (r *defaultRetryPolicy) Delay(receiveCount int) *int64 {
	exp2 := math.Exp2(float64(receiveCount))
	min := math.Min(exp2, float64(MaxVisibilityTimeout))
	return aws.Int64(int64(min))
}

func (r *defaultRetryPolicy) Exhausted(receivedCount int) bool {
	if receivedCount >= MaxReceives {
		return true
	}
	return false
}

// DefaultRetryPolicy increases the Message VisibilityTimeout exponentially
// based on the received count up to MaxVisibilityTimeout and MaxReceives
var DefaultRetryPolicy = &defaultRetryPolicy{}
