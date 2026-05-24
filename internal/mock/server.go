package mock

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"time"
)

// BMC is a fake BMC HTTP server used by tests. It exposes:
//
//	GET /fast        -> immediate 200
//	GET /slow?ms=N   -> sleep N ms, then 200
//	GET /hang        -> block until client disconnects (caller's ctx cancels)
//	GET /500         -> immediate 500
//
// It also tracks max-observed in-flight requests for bounded-concurrency tests.
type BMC struct {
	Server   *httptest.Server
	inFlight atomic.Int32
	maxSeen  atomic.Int32
}

func NewBMC() *BMC {
	b := &BMC{}
	mux := http.NewServeMux()
	mux.HandleFunc("/fast", b.wrap(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	mux.HandleFunc("/slow", b.wrap(func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}))
	mux.HandleFunc("/hang", b.wrap(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	mux.HandleFunc("/500", b.wrap(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	b.Server = httptest.NewServer(mux)
	return b
}

func (b *BMC) Close() {
	b.Server.Close()
}

// MaxInFlight returns the high-watermark of concurrent in-flight requests.
func (b *BMC) MaxInFlight() int32 {
	return b.maxSeen.Load()
}

func (b *BMC) URL(path string) string {
	return b.Server.URL + path
}

func (b *BMC) wrap(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cur := b.inFlight.Add(1)
		defer b.inFlight.Add(-1)
		for {
			seen := b.maxSeen.Load()
			if cur <= seen || b.maxSeen.CompareAndSwap(seen, cur) {
				break
			}
		}
		h(w, r)
	}
}
