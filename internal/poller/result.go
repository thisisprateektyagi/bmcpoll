package poller

import "time"

type Target struct {
	ID  string
	URL string
}

const (
	StatusSuccess         = "success"
	StatusTimeout         = "timeout"
	StatusHTTPError       = "http_error"
	StatusConnectionError = "connection_error"
	StatusCancelled       = "cancelled"
	StatusSkipped         = "skipped"
)

type Result struct {
	Target   Target
	Status   string
	HTTPCode int
	Attempts int
	Duration time.Duration
	Err      error
}

type Summary struct {
	Total    int
	Success  int
	Failed   int
	ByStatus map[string]int
	Results  []Result
	Duration time.Duration
}

func newSummary(results []Result, runDuration time.Duration) Summary {
	s := Summary{
		Total:    len(results),
		ByStatus: make(map[string]int),
		Results:  results,
		Duration: runDuration,
	}
	for _, r := range results {
		s.ByStatus[r.Status]++
		if r.Status == StatusSuccess {
			s.Success++
		} else {
			s.Failed++
		}
	}
	return s
}
