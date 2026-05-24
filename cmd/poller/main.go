package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pollerrors "bmcpoll/errors"
	"bmcpoll/internal/poller"
)

func main() {
	var (
		targetsPath  = flag.String("targets", "", "path to file of URLs (one per line, '-' for stdin) - required")
		concurrency  = flag.Int("concurrency", 50, "max in-flight requests")
		perCall      = flag.Duration("timeout", 3*time.Second, "per-call timeout")
		maxRetries   = flag.Int("max-retries", 3, "retries per target on retryable failure")
		backoffBase  = flag.Duration("backoff-base", 200*time.Millisecond, "initial retry backoff")
		backoffMax   = flag.Duration("backoff-max", 5*time.Second, "max retry backoff")
		totalTimeout = flag.Duration("total-timeout", 5*time.Minute, "overall run timeout (0 = none)")
		jsonOutput   = flag.Bool("json", false, "emit summary as JSON in addition to human-readable text")
	)
	flag.Parse()

	cfg := poller.Config{
		Concurrency:    *concurrency,
		PerCallTimeout: *perCall,
		MaxRetries:     *maxRetries,
		BackoffBase:    *backoffBase,
		BackoffMax:     *backoffMax,
	}
	if err := cfg.Validate(); err != nil {
		fatal("config", pollerrors.CreateInvalidTargetsError(err))
	}

	if *targetsPath == "" {
		fatal("config", pollerrors.CreateInvalidTargetsError(fmt.Errorf("--targets is required")))
	}

	targets, err := readTargets(*targetsPath)
	if err != nil {
		fatal("targets", err)
	}
	if len(targets) == 0 {
		fatal("targets", pollerrors.CreateInvalidTargetsError(fmt.Errorf("no targets found in %s", *targetsPath)))
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *totalTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *totalTimeout)
		defer cancel()
	}

	log.Printf("polling %d targets with concurrency=%d timeout=%s max-retries=%d",
		len(targets), cfg.Concurrency, cfg.PerCallTimeout, cfg.MaxRetries)

	summary := poller.New(cfg, log.Default()).Run(ctx, targets)
	printSummary(summary, *jsonOutput)

	if summary.Failed > 0 {
		os.Exit(1)
	}
}

func fatal(prefix string, err error) {
	log.Fatalf("%s: %v", prefix, err)
}

func readTargets(path string) ([]poller.Target, error) {
	var src *os.File
	if path == "-" {
		src = os.Stdin
	} else {
		f, err := os.Open(path)
		if err != nil {
			return nil, pollerrors.CreateReadTargetsFailedError(err)
		}
		defer f.Close()
		src = f
	}

	var targets []poller.Target
	scanner := bufio.NewScanner(src)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	lineNo := 0
	for scanner.Scan() {
		lineNo++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Allow "id<TAB>url" or "id url"; if no whitespace, derive id from line number.
		var id, raw string
		if fields := strings.Fields(line); len(fields) >= 2 {
			id, raw = fields[0], fields[1]
		} else {
			id, raw = fmt.Sprintf("L%d", lineNo), line
		}
		if _, err := url.ParseRequestURI(raw); err != nil {
			return nil, pollerrors.CreateInvalidTargetsError(
				fmt.Errorf("line %d: %q is not a valid URL: %w", lineNo, raw, err))
		}
		targets = append(targets, poller.Target{ID: id, URL: raw})
	}
	if err := scanner.Err(); err != nil {
		return nil, pollerrors.CreateReadTargetsFailedError(err)
	}
	return targets, nil
}

func printSummary(s poller.Summary, asJSON bool) {
	parts := make([]string, 0, len(s.ByStatus))
	for status, count := range s.ByStatus {
		parts = append(parts, fmt.Sprintf("%d %s", count, status))
	}
	fmt.Fprintf(os.Stderr, "completed in %s - %s\n", s.Duration.Round(time.Millisecond), strings.Join(parts, ", "))

	if asJSON {
		out := jsonSummary{
			Total:    s.Total,
			Success:  s.Success,
			Failed:   s.Failed,
			Duration: s.Duration.String(),
			ByStatus: s.ByStatus,
			Results:  make([]jsonResult, len(s.Results)),
		}
		for i, r := range s.Results {
			jr := jsonResult{
				ID:       r.Target.ID,
				URL:      r.Target.URL,
				Status:   r.Status,
				HTTPCode: r.HTTPCode,
				Attempts: r.Attempts,
				Duration: r.Duration.String(),
			}
			if r.Err != nil {
				jr.Error = r.Err.Error()
			}
			out.Results[i] = jr
		}
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
	}
}

type jsonSummary struct {
	Total    int            `json:"total"`
	Success  int            `json:"success"`
	Failed   int            `json:"failed"`
	Duration string         `json:"duration"`
	ByStatus map[string]int `json:"by_status"`
	Results  []jsonResult   `json:"results"`
}

type jsonResult struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	Status   string `json:"status"`
	HTTPCode int    `json:"http_code,omitempty"`
	Attempts int    `json:"attempts"`
	Duration string `json:"duration"`
	Error    string `json:"error,omitempty"`
}
