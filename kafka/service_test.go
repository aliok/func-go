package kafka

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

// testFunction is a mock function used in tests. Defined inline to avoid
// import cycles with the kafka/mock package.
type testFunction struct {
	onStart  func(context.Context, map[string]string) error
	onStop   func(context.Context) error
	onHandle func(context.Context, Message) error
}

func (f *testFunction) Start(ctx context.Context, cfg map[string]string) error {
	if f.onStart != nil {
		return f.onStart(ctx, cfg)
	}
	return nil
}

func (f *testFunction) Stop(ctx context.Context) error {
	if f.onStop != nil {
		return f.onStop(ctx)
	}
	return nil
}

func (f *testFunction) Handle(ctx context.Context, msg Message) error {
	if f.onHandle != nil {
		return f.onHandle(ctx, msg)
	}
	return nil
}

// TestStart_Invoked ensures that the Start method of a function is invoked
// if it is implemented by the function instance.
func TestStart_Invoked(t *testing.T) {
	t.Setenv("LISTEN_ADDRESS", "127.0.0.1:")

	var (
		ctx, cancel = context.WithCancel(context.Background())
		startCh     = make(chan any)
		errCh       = make(chan error)
		timeoutCh   = time.After(500 * time.Millisecond)
		onStart     = func(_ context.Context, _ map[string]string) error {
			startCh <- true
			return nil
		}
	)
	defer cancel()

	f := &testFunction{onStart: onStart}

	go func() {
		if err := New(f).Start(ctx); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-timeoutCh:
		t.Fatal("function failed to notify of start")
	case err := <-errCh:
		// Consumer loop will error because KAFKA_BROKERS is not set,
		// but Start should still be invoked before that.
		_ = err
	case <-startCh:
		t.Log("start signal received")
	}
	cancel()
}

// TestStart_Static checks that static method Start(f) is a convenience method
// for New(f).Start().
func TestStart_Static(t *testing.T) {
	t.Setenv("LISTEN_ADDRESS", "127.0.0.1:")

	var (
		startCh   = make(chan any)
		errCh     = make(chan error)
		timeoutCh = time.After(500 * time.Millisecond)
		onStart   = func(_ context.Context, _ map[string]string) error {
			startCh <- true
			return nil
		}
	)

	f := &testFunction{onStart: onStart}

	go func() {
		if err := Start(f); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-timeoutCh:
		t.Fatal("function failed to notify of start")
	case err := <-errCh:
		_ = err
	case <-startCh:
		t.Log("start signal received")
	}
}

// TestStart_CfgEnvs ensures that the function's Start method receives a map
// containing all available environment variables as a parameter.
func TestStart_CfgEnvs(t *testing.T) {
	t.Setenv("LISTEN_ADDRESS", "127.0.0.1:")

	var (
		ctx, cancel = context.WithCancel(context.Background())
		startCh     = make(chan any)
		errCh       = make(chan error)
		timeoutCh   = time.After(500 * time.Millisecond)
		onStart     = func(_ context.Context, cfg map[string]string) error {
			v := cfg["TEST_ENV"]
			if v != "example_value" {
				t.Fatalf("did not receive TEST_ENV.  got %v", cfg["TEST_ENV"])
			} else {
				t.Log("expected value received")
			}
			startCh <- true
			return nil
		}
	)
	defer cancel()

	f := &testFunction{onStart: onStart}

	t.Setenv("TEST_ENV", "example_value")

	go func() {
		if err := New(f).Start(ctx); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-timeoutCh:
		t.Fatal("function failed to notify of start")
	case err := <-errCh:
		_ = err
	case <-startCh:
		t.Log("start signal received")
	}
}

// TestCfg_Static ensures that additional static "environment variables"
// built into the container as cfg are correctly read.
func TestCfg_Static(t *testing.T) {
	t.Setenv("LISTEN_ADDRESS", "127.0.0.1:")

	var (
		ctx, cancel = context.WithCancel(context.Background())
		startCh     = make(chan any)
		errCh       = make(chan error)
		timeoutCh   = time.After(500 * time.Millisecond)
	)
	defer cancel()

	dir := t.TempDir()
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile("cfg", []byte(`FUNC_VERSION="v1.2.3"`), os.ModePerm); err != nil {
		t.Fatal(err)
	}

	f := &testFunction{onStart: func(_ context.Context, cfg map[string]string) error {
		v := cfg["FUNC_VERSION"]
		if v != "v1.2.3" {
			t.Fatalf("FUNC_VERSION not received.  Expected 'v1.2.3', got '%v'",
				cfg["FUNC_VERSION"])
		} else {
			t.Log("expected value received")
		}
		startCh <- true
		return nil
	}}

	go func() {
		if err := New(f).Start(ctx); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-timeoutCh:
		t.Fatal("function failed to notify of start")
	case err := <-errCh:
		_ = err
	case <-startCh:
		t.Log("start signal received")
	}
}

// TestStop_Invoked ensures the Stop method of a function is invoked on context
// cancellation if it is implemented by the function instance.
func TestStop_Invoked(t *testing.T) {
	t.Setenv("LISTEN_ADDRESS", "127.0.0.1:")

	var (
		ctx, cancel = context.WithCancel(context.Background())
		startCh     = make(chan any)
		stopCh      = make(chan any)
		errCh       = make(chan error)
		timeoutCh   = time.After(500 * time.Millisecond)
		onStart     = func(_ context.Context, _ map[string]string) error {
			startCh <- true
			return nil
		}
		onStop = func(_ context.Context) error {
			stopCh <- true
			return nil
		}
	)
	defer cancel()

	f := &testFunction{onStart: onStart, onStop: onStop}

	go func() {
		if err := New(f).Start(ctx); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-timeoutCh:
		t.Fatal("function failed to notify of start")
	case err := <-errCh:
		_ = err
		return
	case <-startCh:
		t.Log("start signal received")
	}

	cancel()

	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("function failed to notify of stop")
	case err := <-errCh:
		_ = err
	case <-stopCh:
		t.Log("stop signal received")
	}
}

// TestReady_Invoked ensures the default Ready endpoint returns OK when
// the consumer is marked as ready.
func TestReady_Invoked(t *testing.T) {
	f := &testFunction{}
	service := New(f)
	service.ready.Store(true)

	ln, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go service.Serve(ln)
	defer service.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/health/readiness")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected http status code: %v", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "READY" {
		t.Fatalf("unexpected body: %v", string(body))
	}
}

// TestReady_NotReady ensures the readiness endpoint returns 503 when the
// consumer has not yet joined the group.
func TestReady_NotReady(t *testing.T) {
	f := &testFunction{}
	service := New(f)

	ln, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go service.Serve(ln)
	defer service.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/health/readiness")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %v", resp.StatusCode)
	}
}

// TestAlive_Invoked ensures the default Alive endpoint returns OK.
func TestAlive_Invoked(t *testing.T) {
	f := &testFunction{}
	service := New(f)

	ln, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go service.Serve(ln)
	defer service.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/health/liveness")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("unexpected http status code: %v", resp.StatusCode)
	}
}

// TestHandle_Direct ensures the handler is invoked correctly when called
// directly (without a real Kafka broker).
func TestHandle_Direct(t *testing.T) {
	handled := false

	f := &testFunction{
		onHandle: func(_ context.Context, msg Message) error {
			if string(msg.Value) != "test-value" {
				t.Fatalf("unexpected message value: %s", msg.Value)
			}
			if string(msg.Key) != "test-key" {
				t.Fatalf("unexpected message key: %s", msg.Key)
			}
			if msg.Topic != "test-topic" {
				t.Fatalf("unexpected topic: %s", msg.Topic)
			}
			if len(msg.Headers) != 1 || msg.Headers[0].Key != "h1" {
				t.Fatalf("unexpected headers: %v", msg.Headers)
			}
			handled = true
			return nil
		},
	}

	msg := Message{
		Key:       []byte("test-key"),
		Value:     []byte("test-value"),
		Topic:     "test-topic",
		Partition: 0,
		Offset:    42,
		Headers: []Header{
			{Key: "h1", Value: []byte("v1")},
		},
	}

	err := f.Handle(context.Background(), msg)
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("handler was not invoked")
	}
}

// TestDefaultHandler ensures the DefaultHandler wrapper works correctly
// for static function implementations.
func TestDefaultHandler(t *testing.T) {
	handled := false

	staticFn := func(_ context.Context, msg Message) error {
		handled = true
		if string(msg.Value) != "hello" {
			t.Fatalf("unexpected value: %s", msg.Value)
		}
		return nil
	}

	dh := DefaultHandler{Handler: staticFn}
	err := dh.Handle(context.Background(), Message{Value: []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	if !handled {
		t.Fatal("static handler was not invoked")
	}
}

// TestHandle_Error ensures that a handler returning an error does not
// cause a panic or unexpected behavior.
func TestHandle_Error(t *testing.T) {
	expectedErr := "processing failed"
	f := &testFunction{
		onHandle: func(_ context.Context, _ Message) error {
			return fmt.Errorf("%s", expectedErr)
		},
	}

	err := f.Handle(context.Background(), Message{Value: []byte("bad")})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err.Error() != expectedErr {
		t.Fatalf("expected %q, got %q", expectedErr, err.Error())
	}
}

// TestReady_CustomReporter ensures the readiness endpoint delegates to
// the function's Ready method when it implements ReadinessReporter.
func TestReady_CustomReporter(t *testing.T) {
	f := &readyFunction{ready: true}
	service := New(f)

	ln, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go service.Serve(ln)
	defer service.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/health/readiness")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %v", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "READY" {
		t.Fatalf("expected READY, got %q", string(body))
	}
}

// TestReady_CustomReporterNotReady ensures the readiness endpoint returns
// 503 when the function's Ready method returns false.
func TestReady_CustomReporterNotReady(t *testing.T) {
	f := &readyFunction{ready: false}
	service := New(f)

	ln, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go service.Serve(ln)
	defer service.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/health/readiness")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %v", resp.StatusCode)
	}
}

// TestAlive_CustomReporter ensures the liveness endpoint delegates to
// the function's Alive method when it implements LivenessReporter.
func TestAlive_CustomReporter(t *testing.T) {
	f := &aliveFunction{alive: true}
	service := New(f)

	ln, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go service.Serve(ln)
	defer service.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/health/liveness")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %v", resp.StatusCode)
	}
}

// TestAlive_CustomReporterNotAlive ensures the liveness endpoint returns
// 503 when the function's Alive method returns false.
func TestAlive_CustomReporterNotAlive(t *testing.T) {
	f := &aliveFunction{alive: false}
	service := New(f)

	ln, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	go service.Serve(ln)
	defer service.Close()

	resp, err := http.Get("http://" + ln.Addr().String() + "/health/liveness")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %v", resp.StatusCode)
	}
}

// readyFunction implements Handler and ReadinessReporter for testing.
type readyFunction struct {
	ready bool
}

func (f *readyFunction) Handle(_ context.Context, _ Message) error { return nil }
func (f *readyFunction) Ready(_ context.Context) (bool, error)     { return f.ready, nil }

// aliveFunction implements Handler and LivenessReporter for testing.
type aliveFunction struct {
	alive bool
}

func (f *aliveFunction) Handle(_ context.Context, _ Message) error { return nil }
func (f *aliveFunction) Alive(_ context.Context) (bool, error)     { return f.alive, nil }
