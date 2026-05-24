package poller

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"time"

	pollerrors "bmcpoll/errors"
)

type attemptResult struct {
	httpCode int
	err      error
}

// doWithRetry executes fn with exponential backoff + jitter, up to maxRetries
// retries (so up to maxRetries+1 total attempts). It is fully ctx-aware:
// cancellation aborts the loop immediately, even mid-backoff.
//
// Classification:
//   - 2xx          -> success, stop.
//   - 4xx          -> non-retryable, stop.
//   - 5xx / err    -> retry until budget exhausted.
//   - ctx done     -> stop with cancelled or timeout (caller maps).
func doWithRetry(
	ctx context.Context,
	fn func(ctx context.Context) attemptResult,
	maxRetries int,
	base, maxBackoff time.Duration,
) (final attemptResult, attempts int) {
	for attempt := 0; ; attempt++ {
		attempts = attempt + 1

		res := fn(ctx)
		final = res

		if res.err == nil && res.httpCode >= 200 && res.httpCode < 300 {
			return res, attempts
		}

		if res.err == nil && res.httpCode >= 400 && res.httpCode < 500 {
			final.err = pollerrors.CreateNonRetryableError(nil)
			return final, attempts
		}

		if errors.Is(res.err, context.Canceled) || errors.Is(res.err, context.DeadlineExceeded) {
			if ctx.Err() != nil {
				return res, attempts
			}
		}

		if attempt >= maxRetries {
			if res.err == nil {
				final.err = pollerrors.CreateMaxRetriesExceededError(nil)
			} else {
				final.err = pollerrors.CreateMaxRetriesExceededError(res.err)
			}
			return final, attempts
		}

		sleep := backoffFor(attempt, base, maxBackoff)
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return res, attempts
		case <-timer.C:
		}
	}
}

func backoffFor(attempt int, base, maxBackoff time.Duration) time.Duration {
	// base == 0 means "no backoff between retries"; short-circuit before
	// touching rand.Int63n, which panics on a zero argument.
	if base <= 0 {
		return 0
	}
	d := base << attempt
	if d <= 0 || d > maxBackoff {
		d = maxBackoff
	}
	jitter := time.Duration(rand.Int63n(int64(base)))
	return d + jitter
}

// httpAttempt is the single-attempt body - kept here so retry & poller stay
// decoupled. It returns (httpCode, err) where err is non-nil for transport
// errors only; HTTP status is conveyed via httpCode.
func httpAttempt(client *http.Client, url string) func(ctx context.Context) attemptResult {
	return func(ctx context.Context) attemptResult {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return attemptResult{err: err}
		}
		resp, err := client.Do(req)
		if err != nil {
			return attemptResult{err: err}
		}
		defer resp.Body.Close()
		return attemptResult{httpCode: resp.StatusCode}
	}
}
