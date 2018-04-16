package mq

type Retryer interface {
	RetryDelay(numRetries int) int // Seconds
	ShouldRetry(numRetries int) bool
}

type retryer struct {
	MaxRetries int
}

func (r *retryer) ShouldRetry(n int) bool {
	return n < r.MaxRetries
}

func (r *retryer) RetryDelay(n int) int {
	return n * n
}

var DefaultRetrier = &retryer{MaxRetries: 25}
