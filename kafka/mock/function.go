package mock

import "context"

// Function is a mock for testing lifecycle hooks (Start/Stop).
// It does not implement kafka.Handler to avoid import cycles.
// Tests that need a full kafka.Handler should define one inline.
type Function struct {
	OnStart func(context.Context, map[string]string) error
	OnStop  func(context.Context) error
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
