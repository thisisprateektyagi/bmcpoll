package poller

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	pollerrors "bmcpoll/errors"
)

type Config struct {
	Concurrency    int
	PerCallTimeout time.Duration
	MaxRetries     int
	BackoffBase    time.Duration
	BackoffMax     time.Duration
}

func (c Config) Validate() error {
	if c.Concurrency <= 0 {
		return fmt.Errorf("concurrency must be > 0, got %d", c.Concurrency)
	}
	if c.PerCallTimeout <= 0 {
		return fmt.Errorf("per-call timeout must be > 0, got %s", c.PerCallTimeout)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("max retries must be >= 0, got %d", c.MaxRetries)
	}
	if c.BackoffBase < 0 || c.BackoffMax < 0 {
		return fmt.Errorf("backoff durations must be >= 0")
	}
	if c.BackoffMax > 0 && c.BackoffBase > c.BackoffMax {
		return fmt.Errorf("backoff-base (%s) must be <= backoff-max (%s)", c.BackoffBase, c.BackoffMax)
	}
	return nil
}

type Poller struct {
	cfg    Config
	client *http.Client
	logger *log.Logger
}

func New(cfg Config, logger *log.Logger) *Poller {
	if logger == nil {
		logger = log.Default()
	}
	// No client.Timeout - cancellation is entirely ctx-driven so we don't
	// race the per-call ctx against an independent client deadline.
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        200,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	return &Poller{cfg: cfg, client: client, logger: logger}
}

// Run polls every target with bounded concurrency. It blocks until all
// targets are processed or ctx is cancelled. Cancellation drains in-flight
// work cleanly: dispatcher stops feeding new targets, workers return as
// their per-call ctx fires.
func (p *Poller) Run(ctx context.Context, targets []Target) Summary {
	start := time.Now()
	if len(targets) == 0 {
		return newSummary(nil, time.Since(start))
	}

	p.logger.Printf("poller: starting run targets=%d concurrency=%d per_call_timeout=%s",
		len(targets), p.cfg.Concurrency, p.cfg.PerCallTimeout)

	sem := make(chan struct{}, p.cfg.Concurrency)
	// Buffered to len(targets) so a worker can always send its Result
	// without coordinating with the aggregator - avoids the
	// "worker blocked on send while main blocks on WaitGroup" deadlock.
	results := make(chan Result, len(targets))
	var wg sync.WaitGroup

	dispatched := 0
dispatch:
	for _, t := range targets {
		select {
		case <-ctx.Done():
			break dispatch
		case sem <- struct{}{}:
		}
		wg.Add(1)
		dispatched++
		go func(t Target) {
			defer wg.Done()
			defer func() { <-sem }()
			results <- p.pollOne(ctx, t)
		}(t)
	}

	skipped := targets[dispatched:]
	if len(skipped) > 0 {
		p.logger.Printf("poller: dispatcher halted after ctx.Done dispatched=%d skipped=%d",
			dispatched, len(skipped))
	}
	for _, t := range skipped {
		results <- Result{
			Target: t,
			Status: StatusSkipped,
			Err:    pollerrors.CreateCancelledError(ctx.Err()),
		}
	}

	wg.Wait()
	close(results)

	collected := make([]Result, 0, len(targets))
	for r := range results {
		collected = append(collected, r)
	}
	return newSummary(collected, time.Since(start))
}

func (p *Poller) pollOne(ctx context.Context, t Target) Result {
	start := time.Now()

	attemptFn := func(callCtx context.Context) attemptResult {
		perCallCtx, cancel := context.WithTimeout(callCtx, p.cfg.PerCallTimeout)
		defer cancel()
		return httpAttempt(p.client, t.URL)(perCallCtx)
	}

	res, attempts := doWithRetry(ctx, attemptFn, p.cfg.MaxRetries, p.cfg.BackoffBase, p.cfg.BackoffMax)
	r := Result{
		Target:   t,
		HTTPCode: res.httpCode,
		Attempts: attempts,
		Duration: time.Since(start),
	}
	r.Status, r.Err = classify(ctx, res)
	return r
}

func classify(parent context.Context, res attemptResult) (string, error) {
	// Parent cancellation wins, regardless of what attempt returned.
	if parent.Err() != nil {
		return StatusCancelled, pollerrors.CreateCancelledError(parent.Err())
	}

	// Successful HTTP response.
	if res.httpCode >= 200 && res.httpCode < 300 {
		return StatusSuccess, nil
	}

	// Any HTTP-coded response (4xx or 5xx, possibly after retries) is an
	// HTTP error. The error in res.err is our retry-classification wrapper
	// (NonRetryable or MaxRetriesExceeded) - surface it but label by domain.
	if res.httpCode >= 400 {
		return StatusHTTPError, res.err
	}

	// No HTTP code - transport-level failure.
	if errors.Is(res.err, context.DeadlineExceeded) {
		return StatusTimeout, pollerrors.CreateTimeoutError(res.err)
	}
	if res.err != nil {
		return StatusConnectionError, res.err
	}
	return StatusHTTPError, pollerrors.CreateMaxRetriesExceededError(nil)
}
