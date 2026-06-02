package k8s

import (
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

func argoApp(name, ns, health, sync, phase string, automated bool, conds []any) *unstructured.Unstructured {
	status := map[string]any{}
	if health != "" {
		status["health"] = map[string]any{"status": health}
	}
	if sync != "" {
		status["sync"] = map[string]any{"status": sync}
	}
	if phase != "" {
		status["operationState"] = map[string]any{"phase": phase}
	}
	if conds != nil {
		status["conditions"] = conds
	}
	spec := map[string]any{}
	if automated {
		spec["syncPolicy"] = map[string]any{"automated": map[string]any{}}
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "argoproj.io/v1alpha1",
		"kind":       "Application",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec":       spec,
		"status":     status,
	}}
}

func fluxKust(name, ns string, suspend bool, generation, observed int64, readyStatus, reason string) *unstructured.Unstructured {
	meta := map[string]any{"name": name, "namespace": ns}
	if generation > 0 {
		meta["generation"] = generation
	}
	status := map[string]any{
		"conditions": []any{
			map[string]any{"type": "Ready", "status": readyStatus, "reason": reason, "message": reason + " detail"},
		},
	}
	if observed > 0 {
		status["observedGeneration"] = observed
	}
	spec := map[string]any{}
	if suspend {
		spec["suspend"] = true
	}
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kustomize.toolkit.fluxcd.io/v1",
		"kind":       "Kustomization",
		"metadata":   meta,
		"spec":       spec,
		"status":     status,
	}}
}

func TestDetectGitOpsProblems(t *testing.T) {
	defer ResetTestDynamicState()

	appGVR := schema.GroupVersionResource{Group: "argoproj.io", Version: "v1alpha1", Resource: "applications"}
	kustGVR := schema.GroupVersionResource{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Resource: "kustomizations"}

	comparisonErr := []any{
		map[string]any{"type": "ComparisonError", "message": "app path does not exist"},
	}

	objs := []runtime.Object{
		// Argo — should flag.
		argoApp("degraded", "argocd", "Degraded", "Synced", "", true, nil),                      // critical HealthDegraded
		argoApp("missing-auto", "argocd", "Missing", "OutOfSync", "", true, nil),                // high HealthMissing
		argoApp("drift-auto", "argocd", "Healthy", "OutOfSync", "", true, nil),                  // high OutOfSync
		argoApp("comparison", "argocd", "Healthy", "Unknown", "", false, comparisonErr),         // high ComparisonError (even manual)
		argoApp("degraded-and-error", "argocd", "Degraded", "Unknown", "", true, comparisonErr), // critical: Degraded outranks the error condition
		// Argo — should NOT flag.
		argoApp("missing-manual", "argocd", "Missing", "OutOfSync", "", false, nil), // manual app: expected un-synced
		argoApp("suspended", "argocd", "Suspended", "OutOfSync", "", true, nil),     // intentionally paused
		argoApp("progressing", "argocd", "Progressing", "OutOfSync", "", true, nil), // mid-sync
		argoApp("syncing", "argocd", "Degraded", "OutOfSync", "Running", true, nil), // operation in flight
		argoApp("healthy", "argocd", "Healthy", "Synced", "", true, nil),            // all good
		// Flux — should flag.
		fluxKust("recon-failed", "flux", false, 0, 0, "False", "ReconciliationFailed"),
		fluxKust("artifact-failed", "flux", false, 0, 0, "False", "ArtifactFailed"), // genuine stuck (narrow transient set)
		// Flux — should NOT flag.
		fluxKust("reconciling", "flux", false, 0, 0, "False", "Progressing"),
		fluxKust("suspended", "flux", true, 0, 0, "False", "ReconciliationFailed"),
		fluxKust("stale-gen", "flux", false, 5, 3, "False", "ReconciliationFailed"),
		fluxKust("ready", "flux", false, 0, 0, "True", "ReconciliationSucceeded"),
	}

	scheme := runtime.NewScheme()
	dynClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		scheme,
		map[schema.GroupVersionResource]string{
			appGVR:  "ApplicationList",
			kustGVR: "KustomizationList",
		},
		objs...,
	)
	if err := InitTestDynamicResourceCache(dynClient, []APIResource{
		{Group: "argoproj.io", Version: "v1alpha1", Kind: "Application", Name: "applications", Namespaced: true, Verbs: []string{"list", "watch"}},
		{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Kind: "Kustomization", Name: "kustomizations", Namespaced: true, Verbs: []string{"list", "watch"}},
	}); err != nil {
		t.Fatalf("InitTestDynamicResourceCache: %v", err)
	}
	dynCache := GetDynamicResourceCache()
	discovery := GetResourceDiscovery()
	for _, gvr := range []schema.GroupVersionResource{appGVR, kustGVR} {
		if err := dynCache.EnsureWatching(gvr); err != nil {
			t.Fatalf("EnsureWatching %s: %v", gvr, err)
		}
		if !dynCache.WaitForSync(gvr, 2*time.Second) {
			t.Fatalf("dynamic cache for %s did not sync", gvr)
		}
	}

	problems := DetectGitOpsProblems(dynCache, discovery, "")

	bySubject := map[string]Detection{}
	for _, p := range problems {
		bySubject[p.Name] = p
	}

	wantFlag := map[string]struct {
		severity, reason string
	}{
		"degraded":           {"critical", "HealthDegraded"},
		"degraded-and-error": {"critical", "HealthDegraded"},
		"missing-auto":       {"critical", "HealthMissing"},   // auto-synced resources gone → critical
		"drift-auto":         {"high", "OutOfSync"},           // drift self-heals → stays warning
		"comparison":         {"critical", "ComparisonError"}, // sync failure → critical
		"recon-failed":       {"critical", "ReconciliationFailed"},
		"artifact-failed":    {"critical", "ArtifactFailed"},
	}
	for name, want := range wantFlag {
		p, ok := bySubject[name]
		if !ok {
			t.Errorf("expected %q to be flagged, but it was not. got=%+v", name, problems)
			continue
		}
		if p.Severity != want.severity || p.Reason != want.reason {
			t.Errorf("%q: got severity=%q reason=%q, want %q/%q", name, p.Severity, p.Reason, want.severity, want.reason)
		}
	}

	wantSkip := []string{"missing-manual", "suspended", "progressing", "syncing", "healthy", "reconciling", "stale-gen", "ready"}
	// Two Flux objects share the name "suspended"/"ready" semantics but live in
	// different namespaces from Argo ones with similar names; assert by checking
	// no flagged problem carries a skip-name that isn't also a flagged name.
	for _, name := range wantSkip {
		if _, ok := wantFlag[name]; ok {
			continue
		}
		if p, ok := bySubject[name]; ok {
			t.Errorf("%q should NOT be flagged, but got %+v", name, p)
		}
	}
}

func TestDetectFluxProblems_HelmRelease(t *testing.T) {
	now := time.Now()
	hr := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "helm.toolkit.fluxcd.io/v2",
		"kind":       "HelmRelease",
		"metadata":   map[string]any{"name": "hr", "namespace": "flux"},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "False", "reason": "InstallFailed", "message": "chart install failed"},
		}},
	}}
	got := detectFluxProblems([]*unstructured.Unstructured{hr}, "HelmRelease", fluxHelmGrp, now)
	if len(got) != 1 || got[0].Kind != "HelmRelease" || got[0].Reason != "InstallFailed" {
		t.Fatalf("want 1 HelmRelease InstallFailed problem, got %+v", got)
	}
}

func TestDetectArgoAppProblems_EnabledFalseIsManual(t *testing.T) {
	now := time.Now()
	// automated present but enabled:false => manual => Missing/OutOfSync must NOT flag.
	disabled := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "argoproj.io/v1alpha1", "kind": "Application",
		"metadata": map[string]any{"name": "auto-off", "namespace": "argocd"},
		"spec":     map[string]any{"syncPolicy": map[string]any{"automated": map[string]any{"enabled": false}}},
		"status":   map[string]any{"health": map[string]any{"status": "Missing"}, "sync": map[string]any{"status": "OutOfSync"}},
	}}
	if got := detectArgoAppProblems([]*unstructured.Unstructured{disabled}, now); len(got) != 0 {
		t.Errorf("automated.enabled:false is manual — Missing/OutOfSync must NOT flag, got %+v", got)
	}
	// automated present without enabled (the common case) => automated => flags.
	enabled := disabled.DeepCopy()
	_ = unstructured.SetNestedMap(enabled.Object, map[string]any{}, "spec", "syncPolicy", "automated")
	if got := detectArgoAppProblems([]*unstructured.Unstructured{enabled}, now); len(got) != 1 {
		t.Errorf("automated present (no enabled key) should flag Missing, got %+v", got)
	}
}

func TestEstimateCronMinInterval(t *testing.T) {
	day := 24 * time.Hour
	cases := []struct {
		schedule string
		wantOK   bool
		atLeast  time.Duration // returned interval must be >= this
	}{
		{"*/5 * * * *", true, time.Hour}, // every 5 min → intra-day floor
		{"0 * * * *", true, time.Hour},   // hourly (minute 0, every hour) → intra-day floor
		{"0 0 * * *", true, day},         // daily
		{"0 0 * * 1", true, 7 * day},     // weekly
		{"0 0 1 * *", true, 28 * day},    // monthly (specific dom)
		{"0 0 1 */4 *", true, 28 * day},  // quarterly (constrained month) — the hubble FP
		{"@daily", true, day},            //
		{"@weekly", true, 7 * day},       //
		{"not a schedule", false, 0},     //
	}
	for _, c := range cases {
		got, ok := estimateCronMinInterval(c.schedule)
		if ok != c.wantOK {
			t.Errorf("%q: ok=%v want %v", c.schedule, ok, c.wantOK)
			continue
		}
		if ok && got < c.atLeast {
			t.Errorf("%q: interval=%s, want >= %s", c.schedule, got, c.atLeast)
		}
	}
}

func TestDetectCronJobProblems_CadenceAware(t *testing.T) {
	now := time.Now()
	mk := func(name, schedule string, lastRunAgo time.Duration) *batchv1.CronJob {
		last := metav1.NewTime(now.Add(-lastRunAgo))
		return &batchv1.CronJob{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", CreationTimestamp: metav1.NewTime(now.Add(-365 * 24 * time.Hour))},
			Spec:       batchv1.CronJobSpec{Schedule: schedule},
			Status:     batchv1.CronJobStatus{LastScheduleTime: &last},
		}
	}
	cjs := []*batchv1.CronJob{
		mk("quarterly", "0 0 1 */4 *", 29*24*time.Hour), // ran 29d ago, on schedule → NOT stale (the hubble FP)
		mk("daily-stale", "0 0 * * *", 3*24*time.Hour),  // daily, silent 3d → stale
	}
	stale := map[string]bool{}
	for _, p := range DetectCronJobProblems(cjs) {
		if p.Problem == "stale" {
			stale[p.Name] = true
		}
	}
	if stale["quarterly"] {
		t.Error("on-schedule quarterly CronJob must NOT be flagged stale")
	}
	if !stale["daily-stale"] {
		t.Error("daily CronJob silent for 3 days must be flagged stale")
	}
}
