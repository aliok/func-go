package kafka

import "context"

// Handler is a function instance which can handle a Kafka message.
type Handler interface {
	// Handle a Kafka message.
	Handle(context.Context, Message) error
}

// Starter is an instance which has defined the Start hook.
type Starter interface {
	// Start instance event hook.
	Start(context.Context, map[string]string) error
}

// Stopper is an instance which has defined the Stop hook.
type Stopper interface {
	// Stop instance event hook.
	Stop(context.Context) error
}

// ReadinessReporter is an instance which reports its readiness.
type ReadinessReporter interface {
	// Ready to be invoked or not.
	Ready(context.Context) (bool, error)
}

// LivenessReporter is an instance which reports it is alive.
type LivenessReporter interface {
	// Alive allows the instance to report its liveness status.
	Alive(context.Context) (bool, error)
}

// DefaultHandler wraps a static function for use with the Kafka middleware.
type DefaultHandler struct {
	Handler func(context.Context, Message) error
}

func (f DefaultHandler) Handle(ctx context.Context, msg Message) error {
	return f.Handler(ctx, msg)
}
