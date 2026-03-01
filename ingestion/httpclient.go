package main

import (
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"time"
)

// ---------------------------------------------------------------------------
// HTTP retry helper — respects Binance rate-limit / error semantics
// ---------------------------------------------------------------------------
//
// Status codes handled (per Binance docs):
//   429  – Rate limit exceeded.  Back off (honour Retry-After header).
//   418  – IP auto-banned (sent too many requests after 429).
//          Bans scale 2 min → 3 days.  Honour Retry-After, then retry.
//   403  – WAF (Web Application Firewall) limit hit.
//   408  – Upstream timeout.
//   503  – "Service Unavailable" / system-level throttle (-1008).
//   5XX  – Internal Binance errors — retry later.
//
// The helper uses exponential back-off starting at 200 ms, doubling each
// attempt up to a cap of 30 s, for a maximum of 5 retries.  If the server
// sends a Retry-After header (seconds), that value is preferred.
// ---------------------------------------------------------------------------

const (
	maxRetries     = 5
	initialBackoff = 200 * time.Millisecond
	maxBackoff     = 30 * time.Second
)

// retryableGet performs an HTTP GET with exponential back-off on retryable
// Binance status codes.  Returns the response body on success or an error
// after all retries are exhausted.
func retryableGet(client *http.Client, url string) ([]byte, error) {
	backoff := initialBackoff

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("[backfill] retry %d/%d for %s in %v", attempt, maxRetries, url, backoff)
			time.Sleep(backoff)
		}

		resp, err := client.Get(url)
		if err != nil {
			// Network-level error — retry with backoff.
			backoff = nextBackoff(backoff)
			log.Printf("[backfill] network error (attempt %d): %v", attempt, err)
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			backoff = nextBackoff(backoff)
			log.Printf("[backfill] body read error (attempt %d): %v", attempt, err)
			continue
		}

		switch {
		case resp.StatusCode == http.StatusOK:
			return body, nil

		case resp.StatusCode == 429:
			// Rate-limit hit — back off, ideally using Retry-After.
			backoff = retryAfterOrBackoff(resp, backoff)
			log.Printf("[backfill] 429 rate-limited — backing off %v", backoff)

		case resp.StatusCode == 418:
			// IP auto-banned.  Retry-After tells us how long the ban lasts.
			backoff = retryAfterOrBackoff(resp, maxBackoff)
			log.Printf("[backfill] 418 IP banned — waiting %v before retry", backoff)

		case resp.StatusCode == 403:
			// WAF limit — back off aggressively.
			backoff = retryAfterOrBackoff(resp, maxBackoff)
			log.Printf("[backfill] 403 WAF limit — waiting %v", backoff)

		case resp.StatusCode == 408:
			// Upstream timeout — safe to retry immediately with normal backoff.
			backoff = nextBackoff(backoff)
			log.Printf("[backfill] 408 timeout — retrying")

		case resp.StatusCode == 503:
			// "Service Unavailable" or system-level throttle.
			backoff = retryAfterOrBackoff(resp, backoff)
			log.Printf("[backfill] 503 service unavailable — backing off %v: %s", backoff, string(body))

		case resp.StatusCode >= 500:
			// Other internal errors.
			backoff = nextBackoff(backoff)
			log.Printf("[backfill] %d server error — retrying: %s", resp.StatusCode, string(body))

		default:
			// 4XX client error (bad request, invalid symbol, etc.) — not retryable.
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		}
	}

	return nil, fmt.Errorf("exhausted %d retries for %s", maxRetries, url)
}

// retryAfterOrBackoff reads the Retry-After header (seconds) if present;
// otherwise falls back to exponential back-off from the current value.
func retryAfterOrBackoff(resp *http.Response, current time.Duration) time.Duration {
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	return nextBackoff(current)
}

// nextBackoff doubles the interval, capped at maxBackoff.
func nextBackoff(current time.Duration) time.Duration {
	next := time.Duration(math.Min(float64(current*2), float64(maxBackoff)))
	if next < initialBackoff {
		next = initialBackoff
	}
	return next
}
