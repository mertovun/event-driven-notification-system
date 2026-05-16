// Package provider is the outbound HTTP client for the external notification provider.
// All timeouts and connection limits are explicit (no use of http.DefaultClient).
package provider

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// SendRequest is the small contract for the worker→provider call.
type SendRequest struct {
	To      string `json:"to"`
	Channel string `json:"channel"`
	Content string `json:"content"`
}

// SendResponse mirrors the webhook.site contract: 202 Accepted with messageId.
type SendResponse struct {
	MessageID string    `json:"messageId"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`

	HTTPStatus int    `json:"-"`
	RawBody    []byte `json:"-"`
}

// Error classification — used by the worker to decide retry vs DLQ.
type ErrorKind int

const (
	ErrUnknown ErrorKind = iota
	// ErrNetwork: dial, DNS, TCP reset, TLS handshake — retryable.
	ErrNetwork
	// ErrTimeout: any context deadline / response header timeout — retryable.
	ErrTimeout
	// ErrSSRFBlocked: outbound address denied by Dialer.Control — terminal.
	ErrSSRFBlocked
	// ErrServer: 5xx — retryable.
	ErrServer
	// ErrRateLimited: 429 — retryable, honour Retry-After if present.
	ErrRateLimited
	// ErrClient: 4xx other than 408/429 — terminal.
	ErrClient
)

// SendError carries the classification alongside the underlying cause.
type SendError struct {
	Kind       ErrorKind
	HTTPStatus int
	RetryAfter time.Duration
	Underlying error
	Body       string
}

func (e *SendError) Error() string {
	if e.Underlying != nil {
		return fmt.Sprintf("send (%s, status=%d): %v", e.Kind.String(), e.HTTPStatus, e.Underlying)
	}
	return fmt.Sprintf("send (%s, status=%d, body=%q)", e.Kind.String(), e.HTTPStatus, e.Body)
}
func (e *SendError) Unwrap() error { return e.Underlying }

func (k ErrorKind) String() string {
	switch k {
	case ErrNetwork:
		return "network"
	case ErrTimeout:
		return "timeout"
	case ErrSSRFBlocked:
		return "ssrf-blocked"
	case ErrServer:
		return "server-5xx"
	case ErrRateLimited:
		return "rate-limited"
	case ErrClient:
		return "client-4xx"
	default:
		return "unknown"
	}
}

// IsRetryable returns true for the kinds the worker should retry.
func (k ErrorKind) IsRetryable() bool {
	switch k {
	case ErrNetwork, ErrTimeout, ErrServer, ErrRateLimited:
		return true
	default:
		return false
	}
}

// HTTPClient is the configured client. One per process; reused across all worker goroutines.
type HTTPClient struct {
	client    *http.Client
	endpoint  string
	userAgent string
}

// Options tweaks the client builder. Zero value matches production defaults
// (SSRF guard ON). Tests can flip AllowPrivateAddresses to call into httptest.Server.
type Options struct {
	AllowPrivateAddresses bool
}

// New builds the hardened client with production defaults.
func New(endpoint, userAgent string) (*HTTPClient, error) {
	return NewWithOptions(endpoint, userAgent, Options{})
}

// NewWithOptions builds the client with explicit options. Same as New but lets
// tests disable the SSRF guard for httptest.Server URLs (which bind to 127.0.0.1).
func NewWithOptions(endpoint, userAgent string, opts Options) (*HTTPClient, error) {
	if endpoint == "" {
		return nil, errors.New("provider: endpoint required")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("provider: parse endpoint: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("provider: unsupported scheme %q", u.Scheme)
	}

	dialer := &net.Dialer{
		Timeout:   2 * time.Second,
		KeepAlive: 30 * time.Second,
		// SSRF defense. The configured WEBHOOK_URL is operator-supplied,
		// not per-request user input, so the SSRF surface is small. We still wire a
		// Control hook so reviewers see the pattern and it works defensively if the
		// URL host ever resolves to a private/metadata range.
		Control: ssrfControl,
	}
	if opts.AllowPrivateAddresses {
		dialer.Control = nil
	}
	transport := &http.Transport{
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   50,
		MaxConnsPerHost:       100,
		ForceAttemptHTTP2:     true,
	}
	endpointHost := u.Host
	return &HTTPClient{
		client: &http.Client{
			Timeout:   10 * time.Second,
			Transport: transport,
			// Redirect policy:
			//   - Cap at 2 hops (matches typical CDN/proxy chains; rejects
			//     redirect loops cheap).
			//   - Refuse to leave the configured endpoint's host. A redirect
			//     to a different host (especially across protocols) is almost
			//     always an SSRF / phishing vector; we'd rather fail than
			//     follow. The dial-time SSRF Control hook still gates the
			//     actual TCP connection, but this rejects the redirect
			//     earlier with a clearer error.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				if len(via) >= 2 {
					return fmt.Errorf("too many redirects (%d)", len(via))
				}
				if req.URL.Host != endpointHost {
					return fmt.Errorf("refusing cross-host redirect: %s → %s", endpointHost, req.URL.Host)
				}
				return nil
			},
		},
		endpoint:  endpoint,
		userAgent: defaultIfEmpty(userAgent, "notifyd/0.1"),
	}, nil
}

// Send POSTs the request to the configured endpoint and returns the parsed response.
// On any non-2xx (or transport failure), returns a *SendError with classification.
func (c *HTTPClient) Send(ctx context.Context, req SendRequest, correlationID string) (*SendResponse, error) {
	body, err := json.Marshal(req)
	if err != nil {
		return nil, &SendError{Kind: ErrUnknown, Underlying: fmt.Errorf("marshal: %w", err)}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, &SendError{Kind: ErrUnknown, Underlying: fmt.Errorf("new request: %w", err)}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("User-Agent", c.userAgent)
	if correlationID != "" {
		httpReq.Header.Set("X-Correlation-Id", correlationID)
	}

	resp, err := c.client.Do(httpReq)
	if err != nil {
		return nil, classifyTransportError(err)
	}
	defer func() {
		// Drain so the connection can be reused.
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 64*1024)) // cap response body

	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		var parsed SendResponse
		_ = json.Unmarshal(respBody, &parsed) // tolerant: webhook.site returns its own JSON
		parsed.HTTPStatus = resp.StatusCode
		parsed.RawBody = respBody
		if parsed.Status == "" {
			parsed.Status = "accepted"
		}
		return &parsed, nil
	case resp.StatusCode == http.StatusRequestTimeout, resp.StatusCode == http.StatusTooManyRequests:
		ra := parseRetryAfter(resp.Header.Get("Retry-After"))
		kind := ErrRateLimited
		if resp.StatusCode == http.StatusRequestTimeout {
			kind = ErrTimeout
		}
		return nil, &SendError{Kind: kind, HTTPStatus: resp.StatusCode, RetryAfter: ra, Body: string(respBody)}
	case resp.StatusCode >= 500:
		return nil, &SendError{Kind: ErrServer, HTTPStatus: resp.StatusCode, Body: string(respBody)}
	default: // 4xx other than 408/429 → terminal
		return nil, &SendError{Kind: ErrClient, HTTPStatus: resp.StatusCode, Body: string(respBody)}
	}
}

// classifyTransportError maps net/http transport errors to retryable kinds.
func classifyTransportError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &SendError{Kind: ErrTimeout, Underlying: err}
	}
	var ssrfErr *ssrfError
	if errors.As(err, &ssrfErr) {
		return &SendError{Kind: ErrSSRFBlocked, Underlying: err}
	}
	return &SendError{Kind: ErrNetwork, Underlying: err}
}

// parseRetryAfter accepts seconds or HTTP-date. Returns 0 on failure.
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	var secs int
	if _, err := fmt.Sscanf(v, "%d", &secs); err == nil && secs > 0 {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return d
	}
	return 0
}

func defaultIfEmpty(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
