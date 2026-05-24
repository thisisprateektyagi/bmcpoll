package poller

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"testing"
	"time"

	"bmcpoll/internal/mock"
)

func newTestPoller(cfg Config) *Poller {
	logger := log.New(testWriter{}, "", 0)
	return New(cfg, logger)
}

type testWriter struct{}

func (testWriter) Write(p []byte) (int, error) { return len(p), nil }

func TestRun_HTTPErrorClassification(t *testing.T) {
	t.Parallel()

	bmc := mock.NewBMC()
	defer bmc.Close()

	targets := []Target{
		{ID: "ok", URL: bmc.URL("/fast")},
		{ID: "500", URL: bmc.URL("/500")},
	}
	p := newTestPoller(Config{
		Concurrency:    2,
		PerCallTimeout: 1 * time.Second,
		MaxRetries:     1,
		BackoffBase:    1 * time.Millisecond,
		BackoffMax:     10 * time.Millisecond,
	})
	s := p.Run(context.Background(), targets)

	byID := make(map[string]Result)
	for _, r := range s.Results {
		byID[r.Target.ID] = r
	}
	if byID["ok"].Status != StatusSuccess {
		t.Errorf("ok: want success, got %s", byID["ok"].Status)
	}
	if got := byID["500"].Status; got != StatusHTTPError {
		t.Errorf("500: want http_error, got %s", got)
	}
	if byID["500"].Attempts != 2 {
		t.Errorf("500: want 2 attempts (1 initial + 1 retry), got %d", byID["500"].Attempts)
	}
}

func TestRun_BoundedConcurrency(t *testing.T) {
	t.Parallel()

	bmc := mock.NewBMC()
	defer bmc.Close()

	const (
		n       = 500
		maxConc = 10
	)
	targets := make([]Target, n)
	for i := range targets {
		targets[i] = Target{
			ID:  fmt.Sprintf("t%d", i),
			URL: bmc.URL("/slow?ms=20"),
		}
	}

	p := newTestPoller(Config{
		Concurrency:    maxConc,
		PerCallTimeout: 2 * time.Second,
		MaxRetries:     0,
		BackoffBase:    10 * time.Millisecond,
		BackoffMax:     100 * time.Millisecond,
	})

	summary := p.Run(context.Background(), targets)

	if got := bmc.MaxInFlight(); got > maxConc {
		t.Fatalf("max in-flight %d exceeded cap %d", got, maxConc)
	}
	if summary.Success != n {
		t.Fatalf("expected %d successes, got %d (failed=%d, by-status=%v)",
			n, summary.Success, summary.Failed, summary.ByStatus)
	}
}

func TestRun_HungEndpointDoesNotBlock(t *testing.T) {
	t.Parallel()

	bmc := mock.NewBMC()
	defer bmc.Close()

	targets := []Target{
		{ID: "fast-1", URL: bmc.URL("/fast")},
		{ID: "hang-1", URL: bmc.URL("/hang")},
		{ID: "fast-2", URL: bmc.URL("/fast")},
		{ID: "hang-2", URL: bmc.URL("/hang")},
		{ID: "fast-3", URL: bmc.URL("/fast")},
	}

	p := newTestPoller(Config{
		Concurrency:    5,
		PerCallTimeout: 100 * time.Millisecond,
		MaxRetries:     0,
		BackoffBase:    10 * time.Millisecond,
		BackoffMax:     50 * time.Millisecond,
	})

	deadline := time.Now().Add(2 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	start := time.Now()
	summary := p.Run(ctx, targets)
	elapsed := time.Since(start)

	if elapsed > 1*time.Second {
		t.Fatalf("Run took %s - hung endpoint blocked the run", elapsed)
	}

	byID := make(map[string]Result, len(summary.Results))
	for _, r := range summary.Results {
		byID[r.Target.ID] = r
	}
	for _, id := range []string{"fast-1", "fast-2", "fast-3"} {
		if byID[id].Status != StatusSuccess {
			t.Errorf("%s: expected success, got %s (err=%v)", id, byID[id].Status, byID[id].Err)
		}
	}
	for _, id := range []string{"hang-1", "hang-2"} {
		if byID[id].Status != StatusTimeout {
			t.Errorf("%s: expected timeout, got %s (err=%v)", id, byID[id].Status, byID[id].Err)
		}
	}
}

func TestRun_GracefulShutdownNoLeak(t *testing.T) {
	bmc := mock.NewBMC()
	defer bmc.Close()

	const n = 100
	targets := make([]Target, n)
	for i := range targets {
		targets[i] = Target{
			ID:  fmt.Sprintf("t%d", i),
			URL: bmc.URL("/slow?ms=2000"),
		}
	}

	p := newTestPoller(Config{
		Concurrency:    20,
		PerCallTimeout: 5 * time.Second,
		MaxRetries:     0,
		BackoffBase:    10 * time.Millisecond,
		BackoffMax:     50 * time.Millisecond,
	})

	// Let runtime settle before snapshotting baseline.
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Summary, 1)
	go func() {
		done <- p.Run(ctx, targets)
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case s := <-done:
		if s.Success == n {
			t.Fatalf("expected cancelled run to short-circuit, but all %d succeeded", n)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s after cancellation")
	}

	// Give the httptest server's spawned handlers (the in-flight /slow
	// requests we canceled) a moment to wind down. They are server-side
	// goroutines, not poller goroutines, but they share the goroutine count.
	deadline := time.Now().Add(3 * time.Second)
	var after int
	for time.Now().Before(deadline) {
		runtime.GC()
		after = runtime.NumGoroutine()
		if after <= baseline+5 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("goroutine leak suspected: baseline=%d after=%d (delta=%d)",
		baseline, after, after-baseline)
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		cfg  Config
		ok   bool
	}{
		{"zero concurrency", Config{Concurrency: 0, PerCallTimeout: time.Second}, false},
		{"zero timeout", Config{Concurrency: 1, PerCallTimeout: 0}, false},
		{"negative retries", Config{Concurrency: 1, PerCallTimeout: time.Second, MaxRetries: -1}, false},
		{"base > max", Config{Concurrency: 1, PerCallTimeout: time.Second, BackoffBase: 2 * time.Second, BackoffMax: time.Second}, false},
		{"good", Config{Concurrency: 10, PerCallTimeout: time.Second, MaxRetries: 3, BackoffBase: 100 * time.Millisecond, BackoffMax: time.Second}, true},
	}
	for _, c := range cases {
		err := c.cfg.Validate()
		if (err == nil) != c.ok {
			t.Errorf("%s: ok=%v err=%v", c.name, c.ok, err)
		}
	}
}
