package prom

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeProm returns an HTTPTransport pointed at a test server with a scripted
// response for /api/v1/query and /api/v1/query_range.
func fakeProm(t *testing.T, handler http.HandlerFunc) *HTTPTransport {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewHTTPTransport(srv.URL, "", nil)
}

func TestClient_Query_ParsesVector(t *testing.T) {
	body := `{
	  "status":"success",
	  "data":{
	    "resultType":"vector",
	    "result":[
	      {"metric":{"namespace":"checkout"},"value":[1700000000, "42.5"]}
	    ]
	  }
	}`
	tr := fakeProm(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/api/v1/query") {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("query"); got != "up" {
			t.Errorf("query param = %q, want up", got)
		}
		_, _ = w.Write([]byte(body))
	})

	c := NewClient(tr)
	res, err := c.Query(context.Background(), "up")
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if res.ResultType != "vector" || len(res.Series) != 1 {
		t.Fatalf("bad result: %+v", res)
	}
	s := res.Series[0]
	if s.Labels["namespace"] != "checkout" {
		t.Errorf("label: %v", s.Labels)
	}
	if len(s.DataPoints) != 1 || s.DataPoints[0].Timestamp != 1700000000 || s.DataPoints[0].Value != 42.5 {
		t.Errorf("datapoint: %+v", s.DataPoints)
	}
}

func TestClient_QueryRange_ParsesMatrix(t *testing.T) {
	body := `{
	  "status":"success",
	  "data":{
	    "resultType":"matrix",
	    "result":[
	      {"metric":{"pod":"p1"},"values":[[1700000000,"1"],[1700000060,"2"]]}
	    ]
	  }
	}`
	tr := fakeProm(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/api/v1/query_range") {
			t.Errorf("unexpected path %q", r.URL.Path)
		}
		if r.URL.Query().Get("step") == "" {
			t.Error("step missing")
		}
		_, _ = w.Write([]byte(body))
	})

	c := NewClient(tr)
	res, err := c.QueryRange(context.Background(), `rate(x[1m])`,
		time.Unix(1700000000, 0), time.Unix(1700000060, 0), 30*time.Second)
	if err != nil {
		t.Fatalf("QueryRange: %v", err)
	}
	if res.ResultType != "matrix" || len(res.Series[0].DataPoints) != 2 {
		t.Fatalf("bad result: %+v", res)
	}
}

func TestClient_Query_PropagatesPromError(t *testing.T) {
	tr := fakeProm(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"error","errorType":"bad_data","error":"parse error"}`))
	})
	c := NewClient(tr)
	_, err := c.Query(context.Background(), "up")
	if err == nil || !strings.Contains(err.Error(), "parse error") {
		t.Errorf("expected prom error, got %v", err)
	}
}

func TestClient_Query_HTTPErrorIsTyped(t *testing.T) {
	tr := fakeProm(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream busy"))
	})
	c := NewClient(tr)
	_, err := c.Query(context.Background(), "up")
	if err == nil {
		t.Fatal("expected error")
	}
	var httpErr *HTTPError
	if !errors.As(err, &httpErr) {
		t.Fatalf("want *HTTPError, got %T: %v", err, err)
	}
	if httpErr.StatusCode != http.StatusBadGateway {
		t.Errorf("status: %d", httpErr.StatusCode)
	}
}

func TestClient_Probe_RejectsEmptyInstance(t *testing.T) {
	tr := fakeProm(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	})
	c := NewClient(tr)
	ok, reason := c.Probe(context.Background())
	if ok {
		t.Error("probe should reject instance with empty up result")
	}
	if reason != ProbeReasonEmptyInstance {
		t.Errorf("reason = %q, want empty_instance", reason)
	}
}

func TestClient_Probe_AcceptsActiveInstance(t *testing.T) {
	tr := fakeProm(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1,"1"]}]}}`))
	})
	c := NewClient(tr)
	ok, reason := c.Probe(context.Background())
	if !ok {
		t.Error("probe should accept active instance")
	}
	if reason != "" {
		t.Errorf("reason should be empty on success, got %q", reason)
	}
}

func TestClient_Probe_RejectsNonPromBody(t *testing.T) {
	tr := fakeProm(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`<html>captive portal</html>`))
	})
	c := NewClient(tr)
	ok, reason := c.Probe(context.Background())
	if ok {
		t.Error("probe should reject non-JSON body")
	}
	if reason != ProbeReasonNotPrometheus {
		t.Errorf("reason = %q, want not_prometheus", reason)
	}
}

func TestHTTPTransport_BasePathIncluded(t *testing.T) {
	var capturedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[]}}`))
	}))
	defer srv.Close()

	tr := NewHTTPTransport(srv.URL, "/select/0/prometheus", nil)
	c := NewClient(tr)
	_, _ = c.Query(context.Background(), "up")
	if capturedPath != "/select/0/prometheus/api/v1/query" {
		t.Errorf("base path not applied: got %q", capturedPath)
	}
}

