package prom

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

// Client is a Prometheus HTTP API client that delegates all network calls to
// the injected Transport. The Client itself is stateless with respect to
// discovery — callers are responsible for constructing an appropriate
// Transport (direct HTTP, kubectl port-forward, or any other tunnel).
type Client struct {
	t Transport
}

// NewClient wraps the given Transport.
func NewClient(t Transport) *Client {
	return &Client{t: t}
}

// Query executes an instant PromQL query.
func (c *Client) Query(ctx context.Context, promQL string) (*QueryResult, error) {
	return c.issueQuery(ctx, "/api/v1/query", url.Values{"query": {promQL}})
}

// QueryRange executes a PromQL range query.
func (c *Client) QueryRange(ctx context.Context, promQL string, start, end time.Time, step time.Duration) (*QueryResult, error) {
	params := url.Values{
		"query": {promQL},
		"start": {strconv.FormatInt(start.Unix(), 10)},
		"end":   {strconv.FormatInt(end.Unix(), 10)},
		"step":  {fmt.Sprintf("%.0f", step.Seconds())},
	}
	return c.issueQuery(ctx, "/api/v1/query_range", params)
}

func (c *Client) issueQuery(ctx context.Context, path string, params url.Values) (*QueryResult, error) {
	body, err := c.t.Do(ctx, "GET", path, params)
	if err != nil {
		return nil, err
	}

	var pr promResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, fmt.Errorf("prom: parse response from %s: %w", c.t.Address(), err)
	}
	if pr.Status != "success" {
		return nil, fmt.Errorf("prom: query error from %s: %s (%s)", c.t.Address(), pr.Error, pr.ErrorType)
	}
	return parseQueryResult(pr.Data)
}

// ProbeReason explains a Probe result. An empty string on true = ok.
// On false, Reason indicates why discovery should skip this candidate.
type ProbeReason string

const (
	ProbeReasonTransportError ProbeReason = "transport_error" // network/HTTP failure
	ProbeReasonAuthError      ProbeReason = "auth_error"      // HTTP 401/403 — credentials rejected
	ProbeReasonNotPrometheus  ProbeReason = "not_prometheus"  // 200 but response body isn't prom JSON (captive portal, login page)
	ProbeReasonPromError      ProbeReason = "prom_error"      // prom responded with status=error
	ProbeReasonEmptyInstance  ProbeReason = "empty_instance"  // prom responded success but zero "up" results
)

// Probe checks if a Prometheus endpoint is reachable and has at least one
// active scrape target. Returns (ok, reason). When ok is true the reason is
// empty; when ok is false the reason indicates why (callers may use this
// for targeted logging — e.g., warn once per empty-instance discovery
// skip).
//
// Uses a 3-second timeout regardless of the context deadline to fail fast.
func (c *Client) Probe(ctx context.Context) (bool, ProbeReason) {
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	body, err := c.t.Do(probeCtx, "GET", "/api/v1/query", url.Values{"query": {"up"}})
	if err != nil {
		var httpErr *HTTPError
		if errors.As(err, &httpErr) && (httpErr.StatusCode == 401 || httpErr.StatusCode == 403) {
			return false, ProbeReasonAuthError
		}
		return false, ProbeReasonTransportError
	}

	var pr struct {
		Status string `json:"status"`
		Data   struct {
			Result []json.RawMessage `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &pr); err != nil {
		return false, ProbeReasonNotPrometheus
	}
	if pr.Status != "success" {
		return false, ProbeReasonPromError
	}
	if len(pr.Data.Result) == 0 {
		return false, ProbeReasonEmptyInstance
	}
	return true, ""
}

func parseQueryResult(data json.RawMessage) (*QueryResult, error) {
	var raw struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string `json:"metric"`
			Values [][]interface{}   `json:"values"` // for matrix
			Value  []interface{}     `json:"value"`  // for vector
		} `json:"result"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("prom: parse result: %w", err)
	}

	result := &QueryResult{
		ResultType: raw.ResultType,
		Series:     make([]Series, 0, len(raw.Result)),
	}

	for _, r := range raw.Result {
		series := Series{Labels: r.Metric}

		switch raw.ResultType {
		case "matrix":
			series.DataPoints = make([]DataPoint, 0, len(r.Values))
			for _, v := range r.Values {
				if dp, ok := parseDataPoint(v); ok {
					series.DataPoints = append(series.DataPoints, dp)
				}
			}
		case "vector":
			if r.Value != nil {
				if dp, ok := parseDataPoint(r.Value); ok {
					series.DataPoints = []DataPoint{dp}
				}
			}
		}

		result.Series = append(result.Series, series)
	}

	return result, nil
}

func parseDataPoint(v []interface{}) (DataPoint, bool) {
	if len(v) != 2 {
		return DataPoint{}, false
	}

	ts, ok := v[0].(float64)
	if !ok {
		return DataPoint{}, false
	}

	valStr, sok := v[1].(string)
	if !sok {
		return DataPoint{}, false
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return DataPoint{}, false
	}

	return DataPoint{Timestamp: int64(ts), Value: val}, true
}
