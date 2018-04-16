package mq

import (
	"errors"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// Router will route a message based on MessageAttributes to other registered Handlers.
type Router struct {
	sync.Mutex

	// Resolver maps a Message to a string identifier used to match to a registered Handler. The
	// default implementation returns a MessageAttribute named "route".
	Resolver func(*Message) string

	// A map of handlers to route to. The return value of Resolver should match a key in this map.
	handlers map[string]Handler
}

func NewRouter() *Router {
	return &Router{
		Resolver: func(m *Message) string {
			r := ""
			if v, ok := m.SQSMessage.MessageAttributes["route"]; ok && v.DataType == aws.String("String") {
				r = *v.StringValue
			}
			return r
		},
	}
}

func (m *Router) Handle(route string, h Handler) {
	r.Lock()
	defer r.Unlock()

	r.handlers[route] = h
}

func (r *Router) HandleMessage(c sqsiface.SQSAPI, m *Message) error {
	key := r.Resolver(m)
	if h, ok := r.handlers[key]; ok {
		return h.HandleMessage(c, m)
	}

	return fmt.Errorf("no route matched for key: %s", key)
}
