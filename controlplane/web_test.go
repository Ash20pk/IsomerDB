package main

import (
	"testing"
	"time"
)

func TestAllowRefills(t *testing.T) {
	now := time.Unix(1_000_000, 0)
	for i := 0; i < limitBurst; i++ {
		if !allow("198.51.100.7", now) {
			t.Fatalf("attempt %d refused within the burst", i+1)
		}
	}
	if allow("198.51.100.7", now) {
		t.Fatal("an attempt past the burst was allowed")
	}
	if !allow("198.51.100.8", now) {
		t.Fatal("another address shares the first one's limit")
	}
	if !allow("198.51.100.7", now.Add(limitRefill)) {
		t.Fatal("no attempt allowed after one refill interval")
	}
	if allow("198.51.100.7", now.Add(limitRefill)) {
		t.Fatal("one refill interval allowed two attempts")
	}
}
