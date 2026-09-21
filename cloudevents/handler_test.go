package cloudevents

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"
)

// TestHandler_ConcurrentCancellation is a regression test for the CloudEvents
// receiver wedge: the SDK's NewHTTPReceiveHandler routes every request through a
// single unbuffered, process-shared channel with a non-cancellable send, so a
// client that cancels a request mid-flight can orphan a concurrent request's
// send. Under sustained concurrent load with occasional cancellations, healthy
// requests then stall for their full deadline.
//
// ceHandler decodes and dispatches inline with no shared channel, so healthy
// requests must never stall no matter how many concurrent requests are
// cancelled. This test hammers the handler with concurrent clients — a fraction
// forcing mid-flight cancellation — and fails if any healthy, instant-handler
// request takes longer than a generous slack.
func TestHandler_ConcurrentCancellation(t *testing.T) {
	// Instant handler: any multi-hundred-ms latency below is pure stall.
	h := newCloudeventHandler(DefaultHandler{Handler: func(_ context.Context, _ event.Event) (*event.Event, error) {
		return nil, nil
	}})
	srv := httptest.NewServer(h)
	defer srv.Close()

	const (
		workers  = 8
		duration = 2 * time.Second
		slack    = 1 * time.Second
	)

	// A single client with keep-alives and a bounded pool, so hammering
	// localhost does not exhaust ephemeral ports (which would masquerade as
	// stalls). Genuine wedge stalls surface as context deadline exceeded.
	client := &http.Client{Transport: &http.Transport{
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		MaxConnsPerHost:     200,
	}}
	defer client.CloseIdleConnections()

	post := func(ctx context.Context) error {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL, bytes.NewReader([]byte("x")))
		req.Header.Set("Ce-Specversion", "1.0")
		req.Header.Set("Ce-Id", "id")
		req.Header.Set("Ce-Source", "example/uri")
		req.Header.Set("Ce-Type", "example.type")
		req.Header.Set("Content-Type", "application/octet-stream")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		return nil
	}

	var (
		healthyOK, healthyStalled, connErr, cancelled int64
		wg                                            sync.WaitGroup
	)
	deadline := time.Now().Add(duration)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			i := 0
			for time.Now().Before(deadline) {
				i++
				// A quarter of the workers cancel a quarter of their requests
				// almost immediately, to force mid-flight cancellation.
				if w%4 == 0 && i%4 == 0 {
					ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
					_ = post(ctx)
					cancel()
					atomic.AddInt64(&cancelled, 1)
					time.Sleep(time.Millisecond)
					continue
				}
				// Healthy request: generous deadline against an instant handler.
				ctx, cancel := context.WithTimeout(context.Background(), slack)
				start := time.Now()
				err := post(ctx)
				elapsed := time.Since(start)
				cancel()
				switch {
				case errors.Is(err, context.DeadlineExceeded) || elapsed > slack/2:
					// The wedge symptom: an instant handler took ~the full deadline.
					atomic.AddInt64(&healthyStalled, 1)
				case err != nil:
					// Localhost harness noise (e.g. transient dial errors); tolerated.
					atomic.AddInt64(&connErr, 1)
				default:
					atomic.AddInt64(&healthyOK, 1)
				}
				time.Sleep(time.Millisecond)
			}
		}(w)
	}
	wg.Wait()

	t.Logf("cancelled(forced)=%d healthyOK=%d healthyStalled=%d connErr(tolerated)=%d",
		cancelled, healthyOK, healthyStalled, connErr)
	if cancelled == 0 {
		t.Fatal("test did not exercise any cancellations")
	}
	if healthyStalled > 0 {
		t.Fatalf("%d healthy instant-handler requests stalled under concurrent cancellation "+
			"(the shared-channel receiver wedge has regressed)", healthyStalled)
	}
}

// TestHandler_Signatures verifies the inline dispatcher accepts and correctly
// invokes representative CloudEvents SDK function signatures, including the
// response-returning form.
func TestHandler_Signatures(t *testing.T) {
	respEvent := func() *event.Event { e := event.New(); e.SetType("resp.type"); e.SetSource("resp/uri"); return &e }

	cases := []struct {
		name       string
		fn         any
		wantStatus int
		wantCeResp bool
	}{
		{"no-args", func() {}, http.StatusOK, false},
		{"ctx-only", func(_ context.Context) {}, http.StatusOK, false},
		{"event-err", func(_ event.Event) error { return nil }, http.StatusOK, false},
		{"ctx-event-err", func(_ context.Context, _ event.Event) error { return nil }, http.StatusOK, false},
		{"ctx-event-resp", func(_ context.Context, _ event.Event) (*event.Event, error) { return respEvent(), nil }, http.StatusOK, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newCloudeventHandler(DefaultHandler{Handler: tc.fn})
			srv := httptest.NewServer(h)
			defer srv.Close()

			req, _ := http.NewRequest(http.MethodPost, srv.URL, bytes.NewReader([]byte("{}")))
			req.Header.Set("Ce-Specversion", "1.0")
			req.Header.Set("Ce-Id", "id")
			req.Header.Set("Ce-Source", "example/uri")
			req.Header.Set("Ce-Type", "example.type")
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()

			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			gotCeResp := resp.Header.Get("Ce-Type") != ""
			if gotCeResp != tc.wantCeResp {
				t.Fatalf("response CloudEvent present = %v, want %v", gotCeResp, tc.wantCeResp)
			}
		})
	}
}
