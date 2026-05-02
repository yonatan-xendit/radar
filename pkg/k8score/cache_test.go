package k8score

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestNewResourceCache_Basic(t *testing.T) {
	client := fake.NewSimpleClientset()

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods:        true,
			Services:    true,
			Deployments: true,
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	if !rc.IsSyncComplete() {
		t.Error("expected IsSyncComplete() = true after NewResourceCache returns")
	}

	if rc.Pods() == nil {
		t.Error("expected Pods() lister to be non-nil")
	}
	if rc.Services() == nil {
		t.Error("expected Services() lister to be non-nil")
	}
	if rc.Deployments() == nil {
		t.Error("expected Deployments() lister to be non-nil")
	}

	// Disabled resources should return nil listers
	if rc.Secrets() != nil {
		t.Error("expected Secrets() lister to be nil (not enabled)")
	}
	if rc.Nodes() != nil {
		t.Error("expected Nodes() lister to be nil (not enabled)")
	}
}

func TestNewResourceCache_DeferredSync(t *testing.T) {
	client := fake.NewSimpleClientset()

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods:       true,
			Services:   true,
			ConfigMaps: true,
			Secrets:    true,
		},
		DeferredTypes: map[string]bool{
			ConfigMaps: true,
			Secrets:    true,
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	// Critical resources should be available immediately
	if rc.Pods() == nil {
		t.Error("expected Pods() lister to be available (critical)")
	}

	// Wait for deferred to complete (fake client syncs immediately)
	select {
	case <-rc.DeferredDone():
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for deferred sync")
	}

	if !rc.IsDeferredSynced() {
		t.Error("expected IsDeferredSynced() = true")
	}

	if rc.ConfigMaps() == nil {
		t.Error("expected ConfigMaps() lister to be available after deferred sync")
	}
	if rc.Secrets() == nil {
		t.Error("expected Secrets() lister to be available after deferred sync")
	}
}

// TestNewResourceCache_DeferredSync_PartialFailure verifies that a permanently
// failing deferred informer (e.g. HPA autoscaling/v2 on a K8s <1.23 cluster,
// which responds with "the server could not find the requested resource")
// does not block sibling deferred informers from becoming ready. It also
// verifies the DeferredSyncTimeout path flips deferredFailed so stragglers
// return false from IsDeferredPending.
func TestNewResourceCache_DeferredSync_PartialFailure(t *testing.T) {
	client := fake.NewSimpleClientset()
	// Make HPA LIST fail forever, as happens when the v2 API isn't served.
	client.PrependReactor("list", "horizontalpodautoscalers", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("the server could not find the requested resource")
	})

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods:                     true,
			ConfigMaps:               true,
			Secrets:                  true,
			HorizontalPodAutoscalers: true,
		},
		DeferredTypes: map[string]bool{
			ConfigMaps:               true,
			Secrets:                  true,
			HorizontalPodAutoscalers: true,
		},
		DeferredSyncTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	// Poll for ConfigMaps and Secrets to become ready. A failing sibling
	// (HPA) must not block them.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rc.ConfigMaps() != nil && rc.Secrets() != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if rc.ConfigMaps() == nil {
		t.Fatal("expected ConfigMaps() to become ready despite sibling HPA failing")
	}
	if rc.Secrets() == nil {
		t.Fatal("expected Secrets() to become ready despite sibling HPA failing")
	}

	// Pre-timeout contract check: while HPA is still stuck and the deadline
	// hasn't fired, IsDeferredPending must report HPA pending (HTTP handlers
	// return 503) and ConfigMaps not-pending (handlers serve data). This is
	// the 503-vs-403 distinction the fix is built around.
	if !rc.IsDeferredPending(HorizontalPodAutoscalers) {
		t.Error("pre-timeout: expected IsDeferredPending(HPA)=true while informer still stuck")
	}
	if rc.IsDeferredPending(ConfigMaps) {
		t.Error("pre-timeout: ConfigMaps synced, expected IsDeferredPending=false")
	}

	// deferredDone must close even though HPA never syncs — otherwise the
	// SSE warmup completion never fires.
	select {
	case <-rc.DeferredDone():
	case <-time.After(3 * time.Second):
		t.Fatal("deferredDone never closed after DeferredSyncTimeout")
	}

	// Post-timeout: HPA flips from pending to not-pending because
	// deferredFailed is now set — stops the perpetual-503 spinner.
	// ConfigMaps stays not-pending (it was already synced).
	if rc.IsDeferredPending(HorizontalPodAutoscalers) {
		t.Error("post-timeout: expected IsDeferredPending(HPA)=false (deferredFailed signals give-up)")
	}
	if rc.IsDeferredPending(ConfigMaps) {
		t.Error("post-timeout: ConfigMaps synced, expected IsDeferredPending=false")
	}
}

// TestNewResourceCache_MinimalSet_AllFast verifies that when the patience
// window is set but every informer syncs quickly, NewResourceCache returns
// via the all-critical-synced path (not the minimal-set fallback). No
// informers should be promoted, syncProgress should fire to completion.
func TestNewResourceCache_MinimalSet_AllFast(t *testing.T) {
	client := fake.NewSimpleClientset()

	var lastSynced, lastTotal int
	var lastReady bool
	var progMu sync.Mutex

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods:        true,
			Services:    true,
			Deployments: true,
			Ingresses:   true,
		},
		PatienceWindow: 2 * time.Second,
		MinimalSet: map[string]bool{
			Pods:     true,
			Services: true,
		},
		SyncProgress: func(synced, total int, ready bool) {
			progMu.Lock()
			lastSynced, lastTotal, lastReady = synced, total, ready
			progMu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	// All four critical informers should be synced — no promotion.
	if got := rc.PromotedKinds(); len(got) != 0 {
		t.Errorf("expected no promoted kinds when all critical synced fast, got %v", got)
	}

	progMu.Lock()
	defer progMu.Unlock()
	if lastTotal != 4 {
		t.Errorf("expected SyncProgress.total=4, got %d", lastTotal)
	}
	if lastSynced != 4 {
		t.Errorf("expected SyncProgress.synced=4, got %d", lastSynced)
	}
	if !lastReady {
		t.Error("expected final SyncProgress to report minimalReady=true")
	}
}

// TestNewResourceCache_MinimalSet_Promotion verifies the slow-cluster path:
// when a non-minimal critical informer can't sync within the patience
// window but minimal-set members are ready, NewResourceCache returns and
// promotes the slow informer to deferred. The promoted informer must
// continue running and eventually become available.
func TestNewResourceCache_MinimalSet_Promotion(t *testing.T) {
	client := fake.NewSimpleClientset()

	// Make ingresses LIST fail. The reflector retries with backoff, so
	// HasSynced() stays false, but the failure returns immediately —
	// no shared tracker lock held, pods/services LIST proceed normally.
	// Ingress is critical (not in MinimalSet) so it should be the one
	// that gets promoted.
	client.PrependReactor("list", "ingresses", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("simulated slow API: ingresses unavailable")
	})

	start := time.Now()
	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods:      true,
			Services:  true,
			Ingresses: true,
		},
		PatienceWindow: 200 * time.Millisecond,
		MinimalSet: map[string]bool{
			Pods:     true,
			Services: true,
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	elapsed := time.Since(start)
	if elapsed < 200*time.Millisecond {
		t.Errorf("expected return after at least the patience window (200ms), got %v", elapsed)
	}
	// Cap upper bound generously: patience window + a few jitter ticks.
	if elapsed > 3*time.Second {
		t.Errorf("returned far later than patience window — minimal-set fallback didn't trigger? elapsed=%v", elapsed)
	}

	promoted := rc.PromotedKinds()
	if len(promoted) != 1 || promoted[0] != "Ingress" {
		t.Errorf("expected Ingress to be promoted, got %v", promoted)
	}

	// Minimal-set listers should be available immediately.
	if rc.Pods() == nil {
		t.Error("expected Pods() lister available after first paint")
	}
	if rc.Services() == nil {
		t.Error("expected Services() lister available after first paint")
	}
}

// TestNewResourceCache_SyncTimeout_Promotion covers the legacy hard-cap
// path used by skyhook-connector. Without PatienceWindow/MinimalSet, a
// stuck critical informer must promote at SyncTimeout and the cache
// must return.
func TestNewResourceCache_SyncTimeout_Promotion(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "ingresses", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("simulated stuck API")
	})

	start := time.Now()
	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods:      true,
			Ingresses: true,
		},
		SyncTimeout: 200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	elapsed := time.Since(start)
	if elapsed < 200*time.Millisecond {
		t.Errorf("expected return after SyncTimeout (200ms), got %v", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Errorf("returned far later than SyncTimeout — promotion didn't fire? elapsed=%v", elapsed)
	}

	promoted := rc.PromotedKinds()
	if len(promoted) != 1 || promoted[0] != "Ingress" {
		t.Errorf("expected Ingress to be promoted, got %v", promoted)
	}
}

// TestNewResourceCache_MinimalSet_UnknownKey verifies the validation
// log path: a typo or kind not enabled in ResourceTypes results in an
// empty effective minimal set; the cache returns at PatienceWindow with
// nothing meaningfully gating first paint. This is intentionally
// permissive (we don't fail construction) but the operator must see a
// warning. We don't capture log output here — just verify the cache
// still returns and shape is consistent.
func TestNewResourceCache_MinimalSet_UnknownKey(t *testing.T) {
	client := fake.NewSimpleClientset()

	start := time.Now()
	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods: true,
		},
		PatienceWindow: 200 * time.Millisecond,
		MinimalSet: map[string]bool{
			"pod": true, // typo — should be "pods"
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	// Pods syncs instantly on fake client, so allCritical fires before
	// patience even elapses — promoted should be empty.
	if got := rc.PromotedKinds(); len(got) != 0 {
		t.Errorf("expected no promoted kinds (everything synced), got %v", got)
	}
	if rc.Pods() == nil {
		t.Error("expected Pods() lister to be available")
	}
	// Sanity: returned in reasonable time despite the bogus minimal-set key
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("returned far too late with bogus MinimalSet key: %v", elapsed)
	}
}

// TestNewResourceCache_PatienceWindow_WithoutMinimalSet verifies the
// edge case where only PatienceWindow is set: useMinimalSet is false,
// so behavior degrades to "wait indefinitely for all critical" with no
// hard cap. With a fake client that syncs instantly, this should just
// return on the all-synced path.
func TestNewResourceCache_PatienceWindow_WithoutMinimalSet(t *testing.T) {
	client := fake.NewSimpleClientset()

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods: true,
		},
		PatienceWindow: 100 * time.Millisecond,
		// MinimalSet intentionally nil
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	if got := rc.PromotedKinds(); len(got) != 0 {
		t.Errorf("expected no promotion without MinimalSet, got %v", got)
	}
	if rc.Pods() == nil {
		t.Error("expected Pods() lister to be available")
	}
}

// TestPendingPromotedKinds verifies the live-filtered accessor that the
// dashboard banner relies on: starts equal to PromotedKinds, and shrinks
// as informers eventually sync.
func TestPendingPromotedKinds(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "ingresses", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("simulated slow API")
	})

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods:      true,
			Services:  true,
			Ingresses: true,
		},
		PatienceWindow: 200 * time.Millisecond,
		MinimalSet: map[string]bool{
			Pods:     true,
			Services: true,
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	// At first paint, Ingress is both Promoted and Pending.
	if got := rc.PromotedKinds(); len(got) != 1 || got[0] != "Ingress" {
		t.Fatalf("PromotedKinds: expected [Ingress], got %v", got)
	}
	if got := rc.PendingPromotedKinds(); len(got) != 1 || got[0] != "Ingress" {
		t.Errorf("PendingPromotedKinds: expected [Ingress], got %v", got)
	}
	// PromotedKinds is a stable historical snapshot, PendingPromotedKinds
	// is the live view — verify they are distinct concepts and don't
	// share backing storage.
	promoted := rc.PromotedKinds()
	pending := rc.PendingPromotedKinds()
	if len(promoted) > 0 && len(pending) > 0 && &promoted[0] == &pending[0] {
		t.Error("PromotedKinds and PendingPromotedKinds must not share backing array")
	}
}

// TestPendingPromotedKinds_Drains verifies the live-filtering claim: as
// a promoted informer eventually catches up and reports HasSynced=true,
// it leaves PendingPromotedKinds. PromotedKinds (the snapshot) does not
// change. Without this, a UI banner would list kinds forever even after
// they finished loading.
func TestPendingPromotedKinds_Drains(t *testing.T) {
	client := fake.NewSimpleClientset()

	// Toggle: fail Ingress LIST until we flip the flag, then succeed.
	// The reflector retries on backoff so HasSynced flips when LIST
	// stops failing.
	var failIngress atomic.Bool
	failIngress.Store(true)
	client.PrependReactor("list", "ingresses", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if failIngress.Load() {
			return true, nil, fmt.Errorf("simulated transient failure")
		}
		return false, nil, nil // pass through to default tracker
	})

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods:      true,
			Services:  true,
			Ingresses: true,
		},
		PatienceWindow: 200 * time.Millisecond,
		MinimalSet: map[string]bool{
			Pods:     true,
			Services: true,
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	// Sanity: at construction, Ingress is pending.
	if got := rc.PendingPromotedKinds(); len(got) != 1 || got[0] != "Ingress" {
		t.Fatalf("PendingPromotedKinds at start: expected [Ingress], got %v", got)
	}

	// Flip the reactor to succeed, then poll for the live view to drain.
	failIngress.Store(false)

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if len(rc.PendingPromotedKinds()) == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if got := rc.PendingPromotedKinds(); len(got) != 0 {
		t.Errorf("PendingPromotedKinds did not drain after Ingress LIST began succeeding; still pending: %v", got)
	}

	// PromotedKinds is the snapshot — must NOT shrink.
	if got := rc.PromotedKinds(); len(got) != 1 || got[0] != "Ingress" {
		t.Errorf("PromotedKinds (snapshot) should not shrink; expected [Ingress], got %v", got)
	}
}

// TestNewResourceCache_MinimalSet_BackstopFires covers the worst case on
// the patience+minimal-set path: a kind that's IN the minimal set never
// syncs. Without a backstop the cache would block in NewResourceCache
// forever, trapping the caller on a connecting screen. SyncTimeout is
// the safety net — it must promote everything still pending (including
// minimal-set members) and let the cache return.
func TestNewResourceCache_MinimalSet_BackstopFires(t *testing.T) {
	client := fake.NewSimpleClientset()
	// Pods is in MinimalSet — make its LIST fail forever.
	client.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("simulated permanently-stuck API")
	})

	start := time.Now()
	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods:     true,
			Services: true,
		},
		PatienceWindow: 100 * time.Millisecond,
		MinimalSet: map[string]bool{
			Pods:     true,
			Services: true,
		},
		SyncTimeout: 500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	elapsed := time.Since(start)
	if elapsed < 500*time.Millisecond {
		t.Errorf("expected return after at least SyncTimeout (500ms), got %v", elapsed)
	}
	if elapsed > 3*time.Second {
		t.Errorf("returned far later than SyncTimeout — backstop didn't fire? elapsed=%v", elapsed)
	}

	// Pods stuck → must be promoted; Services synced → must not be promoted.
	promoted := rc.PromotedKinds()
	if len(promoted) != 1 || promoted[0] != "Pod" {
		t.Errorf("expected only Pod to be promoted by backstop, got %v", promoted)
	}

	// Lister still available (it's just empty).
	if rc.Pods() == nil {
		t.Error("expected Pods() lister to be available even after backstop promotion")
	}
}

func TestNewResourceCache_Callbacks(t *testing.T) {
	// Pre-create a pod so the informer fires an add event
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			UID:       "test-uid",
		},
	}
	client := fake.NewSimpleClientset(pod)

	var mu sync.Mutex
	var changes []ResourceChange

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods: true,
		},
		OnChange: func(change ResourceChange, obj, oldObj any) {
			mu.Lock()
			changes = append(changes, change)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	// Wait for the informer add event to propagate
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	got := len(changes)
	mu.Unlock()

	if got == 0 {
		t.Error("expected OnChange to be called for the pre-existing pod add")
	}
}

func TestNewResourceCache_SuppressInitialAdds(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "existing-pod",
			Namespace: "default",
			UID:       "uid-1",
		},
	}
	client := fake.NewSimpleClientset(pod)

	var mu sync.Mutex
	var callbackChanges []ResourceChange

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods: true,
		},
		SuppressInitialAdds: true,
		OnChange: func(change ResourceChange, obj, oldObj any) {
			mu.Lock()
			callbackChanges = append(callbackChanges, change)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	// Wait briefly for any events
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	got := len(callbackChanges)
	mu.Unlock()

	// With SuppressInitialAdds, the OnChange callback should NOT fire for
	// pre-existing resources during sync. However, the add events still
	// go to the changes channel. Note: since NewResourceCache blocks until
	// sync completes, the add fires DURING construction when syncComplete
	// is still false, so callback should be suppressed.
	if got != 0 {
		t.Errorf("expected 0 callback changes with SuppressInitialAdds, got %d", got)
	}
}

func TestNewResourceCache_OnReceived(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			UID:       "test-uid",
		},
	}
	client := fake.NewSimpleClientset(pod)

	var mu sync.Mutex
	receivedKinds := map[string]int{}

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods: true,
		},
		OnReceived: func(kind string) {
			mu.Lock()
			receivedKinds[kind]++
			mu.Unlock()
		},
		// Even with noisy filter that always returns true, OnReceived should fire
		IsNoisyResource: func(kind, name, op string) bool {
			return true // everything is noisy
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	podCount := receivedKinds["Pod"]
	mu.Unlock()

	if podCount == 0 {
		t.Error("expected OnReceived to fire even when IsNoisyResource returns true")
	}
}

func TestNewResourceCache_NamespaceScopedValidation(t *testing.T) {
	client := fake.NewSimpleClientset()

	_, err := NewResourceCache(CacheConfig{
		Client:          client,
		NamespaceScoped: true,
		Namespace:       "", // empty namespace with NamespaceScoped=true
		ResourceTypes:   map[string]bool{Pods: true},
	})
	if err == nil {
		t.Fatal("expected error when NamespaceScoped=true with empty Namespace")
	}
}

func TestNewResourceCache_CallbackPanicRecovery(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "panic-pod",
			Namespace: "default",
			UID:       "panic-uid",
		},
	}
	client := fake.NewSimpleClientset(pod)

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods: true,
		},
		OnChange: func(change ResourceChange, obj, oldObj any) {
			panic("test panic in OnChange")
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	// Wait for event to fire — the panic should be recovered, not crash
	time.Sleep(200 * time.Millisecond)

	// If we get here without crashing, the test passes
	if rc.Pods() == nil {
		t.Error("expected Pods() lister to still work after callback panic")
	}
}

func TestNewResourceCache_MapCloning(t *testing.T) {
	client := fake.NewSimpleClientset()

	resourceTypes := map[string]bool{
		Pods:     true,
		Services: true,
	}

	rc, err := NewResourceCache(CacheConfig{
		Client:        client,
		ResourceTypes: resourceTypes,
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}
	defer rc.Stop()

	// Mutate the original map after construction
	resourceTypes[Pods] = false
	resourceTypes["bogus"] = true

	// The cache should not be affected
	if rc.Pods() == nil {
		t.Error("expected Pods() lister to still work after caller mutates resourceTypes map")
	}
	enabled := rc.GetEnabledResources()
	if !enabled[Pods] {
		t.Error("expected Pods to still be enabled after caller mutates resourceTypes map")
	}
}

func TestNewResourceCache_NilClient(t *testing.T) {
	_, err := NewResourceCache(CacheConfig{
		Client: nil,
	})
	if err == nil {
		t.Fatal("expected error with nil client")
	}
}

func TestNewResourceCache_NoEnabledResources(t *testing.T) {
	client := fake.NewSimpleClientset()

	rc, err := NewResourceCache(CacheConfig{
		Client:        client,
		ResourceTypes: map[string]bool{},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rc.Stop()

	if !rc.IsSyncComplete() {
		t.Error("expected IsSyncComplete() = true even with no resources")
	}
	if rc.GetResourceCount() != 0 {
		t.Errorf("expected 0 resource count, got %d", rc.GetResourceCount())
	}
}

func TestNewResourceCache_StopLifecycle(t *testing.T) {
	client := fake.NewSimpleClientset()

	rc, err := NewResourceCache(CacheConfig{
		Client: client,
		ResourceTypes: map[string]bool{
			Pods: true,
		},
	})
	if err != nil {
		t.Fatalf("NewResourceCache failed: %v", err)
	}

	// Stop should be safe to call multiple times
	rc.Stop()
	rc.Stop()

	// Methods should be safe to call after stop
	_ = rc.Pods()
	_ = rc.Changes()
	_ = rc.GetResourceCount()
}

func TestDropManagedFields(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test",
			Namespace: "default",
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "test-manager"},
			},
			Annotations: map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": `{"big":"json"}`,
				"keep-this": "yes",
			},
		},
	}

	result, err := DropManagedFields(pod)
	if err != nil {
		t.Fatalf("DropManagedFields failed: %v", err)
	}

	p := result.(*corev1.Pod)
	if len(p.ManagedFields) != 0 {
		t.Error("expected managedFields to be nil/empty")
	}
	if _, ok := p.Annotations["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		t.Error("expected last-applied-configuration annotation to be removed")
	}
	if p.Annotations["keep-this"] != "yes" {
		t.Error("expected other annotations to be preserved")
	}
}

func TestDropManagedFields_Event(t *testing.T) {
	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-event",
			Namespace: "default",
			UID:       "event-uid",
			ManagedFields: []metav1.ManagedFieldsEntry{
				{Manager: "test-manager"},
			},
			Labels: map[string]string{"should": "be-stripped"},
		},
		Reason:  "Created",
		Message: "Pod created",
		Type:    "Normal",
		Count:   1,
	}

	result, err := DropManagedFields(event)
	if err != nil {
		t.Fatalf("DropManagedFields failed: %v", err)
	}

	e := result.(*corev1.Event)
	if e.Reason != "Created" {
		t.Errorf("expected Reason=Created, got %s", e.Reason)
	}
	if e.Labels != nil {
		t.Error("expected Labels to be stripped from event")
	}
	if len(e.ManagedFields) != 0 {
		t.Error("expected managedFields to be stripped from event")
	}
}
