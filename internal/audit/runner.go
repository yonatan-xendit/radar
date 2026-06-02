package audit

import (
	"log"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/skyhook-io/radar/internal/k8s"
	bp "github.com/skyhook-io/radar/pkg/audit"
)

// RunOptions provides optional data sources for checks that need them.
type RunOptions struct {
	ClusterVersion string   // e.g. "1.30"
	ServedAPIs     []string // e.g. ["apps/v1", "batch/v1beta1"]
}

// RunFromCache fetches resources from Radar's cache and runs all best-practice
// checks. namespaces filters to specific namespaces; empty = all.
func RunFromCache(cache *k8s.ResourceCache, namespaces []string, opts *RunOptions) *bp.ScanResults {
	if cache == nil {
		return &bp.ScanResults{Summary: bp.ScanSummary{Categories: map[string]bp.CategorySummary{}}}
	}

	input := &bp.CheckInput{
		Pods:                     listNamespaced(cache.Pods(), namespaces),
		Deployments:              listNamespaced(cache.Deployments(), namespaces),
		StatefulSets:             listNamespaced(cache.StatefulSets(), namespaces),
		DaemonSets:               listNamespaced(cache.DaemonSets(), namespaces),
		Services:                 listNamespaced(cache.Services(), namespaces),
		Ingresses:                listNamespaced(cache.Ingresses(), namespaces),
		HorizontalPodAutoscalers: listNamespaced(cache.HorizontalPodAutoscalers(), namespaces),
		PodDisruptionBudgets:     listNamespaced(cache.PodDisruptionBudgets(), namespaces),
		ConfigMaps:               listNamespaced(cache.ConfigMaps(), namespaces),
		Secrets:                  listNamespaced(cache.Secrets(), namespaces),
		ServiceAccounts:          listNamespaced(cache.ServiceAccounts(), namespaces),
		LimitRanges:              listNamespaced(cache.LimitRanges(), namespaces),
	}

	if opts != nil {
		input.ClusterVersion = opts.ClusterVersion
		input.ServedAPIs = opts.ServedAPIs
	}

	// Crossplane Managed Resources / Composites / Claims live in the dynamic
	// cache (unbounded kind set — one CRD per provider service). Listing here
	// is best-effort: if Crossplane isn't installed, discovery is unavailable,
	// or the dynamic cache hasn't synced, we leave the fields nil and the
	// crossplaneStuck check no-ops.
	mrs, xrs := listCrossplaneDynamic(namespaces)
	input.ManagedResources = mrs
	input.CompositeResources = xrs

	return bp.RunChecks(input)
}

// listCrossplaneDynamic enumerates the dynamic cache's already-watching
// informers and classifies their contents by spec shape (providerConfigRef
// for MRs, resourceRefs / resourceRef+compositionRef for XRs/Claims). No
// API-group filter — XRs and Claims live in user-defined groups (e.g.
// platform.example.com) that an enumerator can't predict, and a group-
// based filter would silently miss them.
//
// We deliberately don't iterate API discovery and call cache.List() per
// CRD: that path calls EnsureWatching, which starts a persistent informer
// per GVR. On a cluster with the Upbound AWS provider (~1000 CRDs), one
// audit run would permanently start informers for every kind and the
// process would grow unbounded. Instead, the audit acts on what Radar is
// already observing — MRs/XRs in groups nobody has navigated to yet won't
// surface until they're watched for some other reason. Acceptable trade-
// off for an audit pass.
func listCrossplaneDynamic(namespaces []string) (mrs, xrs []*unstructured.Unstructured) {
	cache := k8s.GetDynamicResourceCache()
	if cache == nil {
		return nil, nil
	}
	nsSet := make(map[string]bool, len(namespaces))
	for _, ns := range namespaces {
		nsSet[ns] = true
	}
	for _, gvr := range cache.WatchedGVRs() {
		// Skip Crossplane meta groups — XRDs / Compositions / Provider
		// packages have neither providerConfigRef nor resourceRefs so the
		// shape-detection would no-op anyway, but skipping early saves the
		// indexer walk.
		if gvr.Group == "pkg.crossplane.io" || gvr.Group == "apiextensions.crossplane.io" {
			continue
		}
		items, err := cache.ListWatched(gvr)
		if err != nil {
			if !apierrors.IsForbidden(err) && !apierrors.IsUnauthorized(err) {
				log.Printf("[audit] Crossplane scan: skipping %s/%s: %v", gvr.GroupResource(), gvr.Version, err)
			}
			continue
		}
		if len(items) == 0 {
			continue
		}
		for _, u := range items {
			if u == nil {
				continue
			}
			if len(namespaces) > 0 {
				if ns := u.GetNamespace(); ns != "" && !nsSet[ns] {
					continue
				}
			}
			if isCrossplaneMR(u) {
				mrs = append(mrs, u)
			} else if isCrossplaneComposite(u) {
				xrs = append(xrs, u)
			}
		}
	}
	return mrs, xrs
}

// isCrossplaneMR mirrors the frontend heuristic — a Managed Resource always
// has a providerConfigRef (v1: spec.providerConfigRef, v2: spec.crossplane.providerConfigRef).
// We require the ref to be a non-empty object, not just a present key: TS-side
// uses truthiness (a `null` ref is "no ref"), and a key-existence check would
// flag `providerConfigRef: null` as an MR here but not in the UI.
func isCrossplaneMR(u *unstructured.Unstructured) bool {
	if u == nil {
		return false
	}
	spec, ok := u.Object["spec"].(map[string]interface{})
	if !ok {
		return false
	}
	if _, ok := spec["providerConfigRef"].(map[string]interface{}); ok {
		return true
	}
	if cp, ok := spec["crossplane"].(map[string]interface{}); ok {
		if _, ok := cp["providerConfigRef"].(map[string]interface{}); ok {
			return true
		}
	}
	return false
}

// isCrossplaneComposite matches XRs and v1 Claims. XRs expose
// spec.resourceRefs (v1) or spec.crossplane.resourceRefs (v2); v1 Claims
// expose singular spec.resourceRef + spec.compositionRef pointing at their
// bound XR. Without the singular-ref arm a stuck Claim would never appear in
// crossplaneStuck findings — a documented audit feature would be silently
// missing for the entire Claim category.
// MRs are excluded by the providerConfigRef check above; they share the same
// group set as XRs/Claims and need to be discriminated by spec shape.
func isCrossplaneComposite(u *unstructured.Unstructured) bool {
	if u == nil {
		return false
	}
	if isCrossplaneMR(u) {
		return false
	}
	spec, ok := u.Object["spec"].(map[string]interface{})
	if !ok {
		return false
	}
	if _, ok := spec["resourceRefs"].([]interface{}); ok {
		return true
	}
	if cp, ok := spec["crossplane"].(map[string]interface{}); ok {
		if _, ok := cp["resourceRefs"].([]interface{}); ok {
			return true
		}
	}
	// v1 Claim: singular resourceRef pointing at the XR, plus compositionRef.
	// Both must be present — singular resourceRef alone shows up in unrelated
	// CRDs.
	if _, hasRef := spec["resourceRef"].(map[string]interface{}); hasRef {
		if _, hasComp := spec["compositionRef"].(map[string]interface{}); hasComp {
			return true
		}
	}
	return false
}

// lister is a generic interface that all typed K8s listers satisfy.
type lister[T any] interface {
	List(selector labels.Selector) ([]*T, error)
}

// listNamespaced fetches all objects from a lister, optionally filtered by namespaces.
func listNamespaced[T any, L lister[T]](l L, namespaces []string) []*T {
	var zero L
	if any(l) == any(zero) {
		return nil
	}
	if len(namespaces) == 0 {
		items, _ := l.List(labels.Everything())
		return items
	}
	// For namespace-filtered queries we rely on the global list + filter approach
	// since typed listers use different namespace lister types that don't share
	// a common interface. This is simple and fast for cached data.
	all, _ := l.List(labels.Everything())
	nsSet := make(map[string]bool, len(namespaces))
	for _, ns := range namespaces {
		nsSet[ns] = true
	}
	var filtered []*T
	for _, item := range all {
		if ns := extractNamespace(item); ns == "" || nsSet[ns] {
			filtered = append(filtered, item)
		}
	}
	return filtered
}

// extractNamespace uses type assertions for known types to get namespace.
func extractNamespace(obj any) string {
	switch v := obj.(type) {
	case *corev1.Pod:
		return v.Namespace
	case *appsv1.Deployment:
		return v.Namespace
	case *appsv1.StatefulSet:
		return v.Namespace
	case *appsv1.DaemonSet:
		return v.Namespace
	case *corev1.Service:
		return v.Namespace
	case *networkingv1.Ingress:
		return v.Namespace
	case *autoscalingv2.HorizontalPodAutoscaler:
		return v.Namespace
	case *policyv1.PodDisruptionBudget:
		return v.Namespace
	case *corev1.ConfigMap:
		return v.Namespace
	case *corev1.Secret:
		return v.Namespace
	case *corev1.ServiceAccount:
		return v.Namespace
	case *corev1.LimitRange:
		return v.Namespace
	}
	return ""
}

