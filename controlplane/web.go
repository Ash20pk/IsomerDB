package main

import (
	"embed"
	"io/fs"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// The sign-up page: a static app in web/, compiled into the binary and served on the API's own
// origin, so it calls the API without CORS. It holds the caller's token in sessionStorage only.
//
//go:embed web
var webFiles embed.FS

func webHandler() http.Handler {
	sub, _ := fs.Sub(webFiles, "web")
	files := http.FileServerFS(sub)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", "default-src 'self'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Cache-Control", "no-cache")
		files.ServeHTTP(w, r)
	})
}

// limit allows each client address a burst of attempts at signing up or logging in, refilled at
// one every few seconds: enough for a person who mistypes a password, not for guessing one.
// CP_CLIENT_IP_HEADER names a header carrying the client's address when the control plane sits
// behind a proxy that sets it (X-Forwarded-For's first entry, for example); without it the peer
// address is used.
const (
	limitBurst  = 10
	limitRefill = 6 * time.Second
)

var limiter = struct {
	sync.Mutex
	seen map[string]*bucket
}{seen: map[string]*bucket{}}

type bucket struct {
	tokens float64
	at     time.Time
}

func clientIP(r *http.Request) string {
	if name := os.Getenv("CP_CLIENT_IP_HEADER"); name != "" {
		if v := strings.TrimSpace(strings.Split(r.Header.Get(name), ",")[0]); v != "" {
			return v
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func allow(ip string, now time.Time) bool {
	limiter.Lock()
	defer limiter.Unlock()
	if len(limiter.seen) > 100_000 {
		for k, b := range limiter.seen {
			if now.Sub(b.at) > limitBurst*limitRefill {
				delete(limiter.seen, k)
			}
		}
	}
	b, ok := limiter.seen[ip]
	if !ok {
		b = &bucket{tokens: limitBurst, at: now}
		limiter.seen[ip] = b
	}
	b.tokens = min(limitBurst, b.tokens+now.Sub(b.at).Seconds()/limitRefill.Seconds())
	b.at = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func limit(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allow(clientIP(r), time.Now()) {
			w.Header().Set("Retry-After", "6")
			problem(w, http.StatusTooManyRequests, "too many attempts from this address; try again shortly")
			return
		}
		h(w, r)
	}
}
