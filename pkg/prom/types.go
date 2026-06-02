// Package prom provides a Prometheus HTTP API client with a pluggable
// Transport so the same query, parsing, and discovery logic can be used
// from any context that can reach a Prometheus endpoint — directly, via
// kubectl port-forward, or through a tunneled proxy.
//
// The package is intentionally pure: no global state, no singletons, no
// k8s client dependency in the Client itself. K8s-aware discovery is a
// separate step that constructs a Transport.
package prom

import "encoding/json"

// ServiceInfo describes a Prometheus-compatible service discovered in the
// cluster. Used by discovery helpers and returned in Status.
type ServiceInfo struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Port      int    `json:"port"`
	BasePath  string `json:"basePath,omitempty"` // e.g. "/select/0/prometheus" for vmselect
}

// Status represents the current Prometheus connection status as exposed to
// callers/UI. Address is the effective URL (may be port-forwarded, a
// tunneled proxy URL, or a direct service URL depending on the Transport).
type Status struct {
	Available   bool         `json:"available"`
	Connected   bool         `json:"connected"`
	Address     string       `json:"address,omitempty"`
	Service     *ServiceInfo `json:"service,omitempty"`
	ContextName string       `json:"contextName,omitempty"`
	Error       string       `json:"error,omitempty"`
}

// QueryResult is the parsed result of a Prometheus query.
type QueryResult struct {
	ResultType string   `json:"resultType"`
	Series     []Series `json:"series"`
}

// Series is a single time series from a Prometheus query.
type Series struct {
	Labels     map[string]string `json:"labels"`
	DataPoints []DataPoint       `json:"dataPoints"`
}

// DataPoint is a single (timestamp, value) pair.
type DataPoint struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

// promResponse is the raw shape returned by Prometheus HTTP API
// /api/v1/query and /api/v1/query_range endpoints.
type promResponse struct {
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data"`
	ErrorType string          `json:"errorType,omitempty"`
	Error     string          `json:"error,omitempty"`
}
