package mock

import (
	"context"

	"knative.dev/func-go/kafka"
)

type Function struct {
	OnStart  func(context.Context, map[string]string) error
	OnStop   func(context.Context) error
	OnHandle func(context.Context, kafka.Message) error
}

func (f *Function) Start(ctx context.Context, cfg map[string]string) error {
	if f.OnStart != nil {
		return f.OnStart(ctx, cfg)
	}
	return nil
}

func (f *Function) Stop(ctx context.Context) error {
	if f.OnStop != nil {
		return f.OnStop(ctx)
	}
	return nil
}

func (f *Function) Handle(ctx context.Context, msg kafka.Message) error {
	if f.OnHandle != nil {
		return f.OnHandle(ctx, msg)
	}
	return nil
}
