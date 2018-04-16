package mq

import (
	"math"
)

const MaxVisibilityTimeout = 43200

type Retryer interface {
	RetryDelay(receiveCount int) int // Seconds
}

type defaultRetryer struct{}

func (r *defaultRetryer) RetryDelay(receiveCount int) int {
	return int(math.Min(math.Exp2(float64(receiveCount)), float64(MaxVisibilityTimeout)))
}

// DefaultRetrier increases the Message VisibilityTimeout exponentially based on the received count.
var DefaultRetrier = &defaultRetryer{}
