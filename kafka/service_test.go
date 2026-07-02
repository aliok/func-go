package kafka

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
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
	case <-errCh:
		select {
		case <-startCh:
			t.Log("start signal received")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Start hook was not invoked")
		}
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
	case <-errCh:
		select {
		case <-startCh:
			t.Log("start signal received")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Start hook was not invoked")
		}
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
	case <-errCh:
		select {
		case <-startCh:
			t.Log("start signal received")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Start hook was not invoked")
		}
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

	origDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(origDir) })

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
	case <-errCh:
		select {
		case <-startCh:
			t.Log("start signal received")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Start hook was not invoked")
		}
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
	case <-errCh:
		select {
		case <-startCh:
			t.Log("start signal received")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Start hook was not invoked")
		}
	case <-startCh:
		t.Log("start signal received")
	}

	cancel()

	select {
	case <-time.After(500 * time.Millisecond):
		t.Fatal("function failed to notify of stop")
	case <-errCh:
		select {
		case <-stopCh:
			t.Log("stop signal received")
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Stop hook was not invoked")
		}
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

// mockSession implements sarama.ConsumerGroupSession for testing.
type mockSession struct {
	ctx    context.Context
	marked []*sarama.ConsumerMessage
}

func (s *mockSession) Claims() map[string][]int32               { return nil }
func (s *mockSession) MemberID() string                         { return "test" }
func (s *mockSession) GenerationID() int32                      { return 1 }
func (s *mockSession) MarkOffset(string, int32, int64, string)  {}
func (s *mockSession) Commit()                                  {}
func (s *mockSession) ResetOffset(string, int32, int64, string) {}
func (s *mockSession) Context() context.Context                 { return s.ctx }
func (s *mockSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	s.marked = append(s.marked, msg)
}

// mockClaim implements sarama.ConsumerGroupClaim for testing.
type mockClaim struct {
	ch chan *sarama.ConsumerMessage
}

func (c *mockClaim) Topic() string                            { return "test-topic" }
func (c *mockClaim) Partition() int32                         { return 0 }
func (c *mockClaim) InitialOffset() int64                     { return 0 }
func (c *mockClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *mockClaim) Messages() <-chan *sarama.ConsumerMessage { return c.ch }

// TestConsumeClaim_Success ensures that ConsumeClaim converts sarama messages
// to kafka.Message, calls the handler, and marks the message on success.
func TestConsumeClaim_Success(t *testing.T) {
	var received Message
	f := &testFunction{
		onHandle: func(_ context.Context, msg Message) error {
			received = msg
			return nil
		},
	}

	var ready atomic.Bool
	h := &consumerGroupHandler{f: f, ready: &ready}

	ch := make(chan *sarama.ConsumerMessage, 1)
	ch <- &sarama.ConsumerMessage{
		Key:       []byte("k1"),
		Value:     []byte("v1"),
		Topic:     "my-topic",
		Partition: 2,
		Offset:    99,
		Headers: []*sarama.RecordHeader{
			{Key: []byte("hk"), Value: []byte("hv")},
		},
	}
	close(ch)

	session := &mockSession{ctx: context.Background()}
	claim := &mockClaim{ch: ch}

	if err := h.ConsumeClaim(session, claim); err != nil {
		t.Fatal(err)
	}

	if string(received.Key) != "k1" {
		t.Fatalf("expected key 'k1', got '%s'", received.Key)
	}
	if string(received.Value) != "v1" {
		t.Fatalf("expected value 'v1', got '%s'", received.Value)
	}
	if received.Topic != "my-topic" {
		t.Fatalf("expected topic 'my-topic', got '%s'", received.Topic)
	}
	if received.Partition != 2 {
		t.Fatalf("expected partition 2, got %d", received.Partition)
	}
	if received.Offset != 99 {
		t.Fatalf("expected offset 99, got %d", received.Offset)
	}
	if len(received.Headers) != 1 || received.Headers[0].Key != "hk" || string(received.Headers[0].Value) != "hv" {
		t.Fatalf("unexpected headers: %v", received.Headers)
	}
	if len(session.marked) != 1 {
		t.Fatalf("expected 1 marked message, got %d", len(session.marked))
	}
}

// TestConsumeClaim_Error ensures that when the handler returns an error,
// the message is not marked and processing continues.
func TestConsumeClaim_Error(t *testing.T) {
	callCount := 0
	f := &testFunction{
		onHandle: func(_ context.Context, msg Message) error {
			callCount++
			if string(msg.Value) == "bad" {
				return fmt.Errorf("handle error")
			}
			return nil
		},
	}

	var ready atomic.Bool
	h := &consumerGroupHandler{f: f, ready: &ready}

	ch := make(chan *sarama.ConsumerMessage, 2)
	ch <- &sarama.ConsumerMessage{Value: []byte("bad"), Topic: "t", Partition: 0, Offset: 1}
	ch <- &sarama.ConsumerMessage{Value: []byte("good"), Topic: "t", Partition: 0, Offset: 2}
	close(ch)

	session := &mockSession{ctx: context.Background()}
	claim := &mockClaim{ch: ch}

	if err := h.ConsumeClaim(session, claim); err != nil {
		t.Fatal(err)
	}

	if callCount != 2 {
		t.Fatalf("expected handler called 2 times, got %d", callCount)
	}
	if len(session.marked) != 1 {
		t.Fatalf("expected 1 marked message (only the good one), got %d", len(session.marked))
	}
	if session.marked[0].Offset != 2 {
		t.Fatalf("expected marked offset 2, got %d", session.marked[0].Offset)
	}
}

// TestConsumeClaim_SetupCleanup ensures Setup sets ready=true and
// Cleanup sets ready=false.
func TestConsumeClaim_SetupCleanup(t *testing.T) {
	var ready atomic.Bool
	h := &consumerGroupHandler{
		f:     &testFunction{},
		ready: &ready,
	}

	if ready.Load() {
		t.Fatal("expected ready=false initially")
	}

	if err := h.Setup(nil); err != nil {
		t.Fatal(err)
	}
	if !ready.Load() {
		t.Fatal("expected ready=true after Setup")
	}

	if err := h.Cleanup(nil); err != nil {
		t.Fatal(err)
	}
	if ready.Load() {
		t.Fatal("expected ready=false after Cleanup")
	}
}

func TestSplitAndTrim(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{"simple", "a,b,c", []string{"a", "b", "c"}},
		{"with spaces", "a, b, c", []string{"a", "b", "c"}},
		{"trailing comma", "a,b,", []string{"a", "b"}},
		{"empty entries", "a,,b", []string{"a", "b"}},
		{"spaces only", " , , ", nil},
		{"single value", "a", []string{"a"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitAndTrim(tt.input)
			if len(got) != len(tt.expected) {
				t.Fatalf("expected %v, got %v", tt.expected, got)
			}
			for i := range got {
				if got[i] != tt.expected[i] {
					t.Fatalf("expected %v, got %v", tt.expected, got)
				}
			}
		})
	}
}
