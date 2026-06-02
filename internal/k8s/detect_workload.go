package k8s

import (
	"fmt"
	"strings"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// HPAProblem describes a detected issue with an HPA.
type HPAProblem struct {
	Name      string
	Namespace string
	Problem   string // "maxed"
	Reason    string
}

// DetectHPAProblems finds HPAs that have hit their replica ceiling OR that
// cannot scale because the autoscaler can't fetch metrics. The latter is
// the silent-broken-HPA case: spec is valid, target exists, but
// status.conditions[?type=ScalingActive].status=False means the controller
// gave up — metrics-server unavailable, broken adapter, missing resource
// requests on target pods, etc. K8s autoscaler condition reasons are
// stable across versions (FailedGetResourceMetric / FailedGetScale /
// FailedGetExternalMetric / FailedGetObjectMetric).
func DetectHPAProblems(hpas []*autoscalingv2.HorizontalPodAutoscaler) []HPAProblem {
	var problems []HPAProblem
	for _, hpa := range hpas {
		// "maxed" — at replica ceiling and wanting more.
		if hpa.Spec.MaxReplicas > 0 && hpa.Status.CurrentReplicas >= hpa.Spec.MaxReplicas && hpa.Status.DesiredReplicas >= hpa.Spec.MaxReplicas {
			problems = append(problems, HPAProblem{
				Name:      hpa.Name,
				Namespace: hpa.Namespace,
				Problem:   "maxed",
				Reason:    fmt.Sprintf("%d/%d replicas (wants %d)", hpa.Status.CurrentReplicas, hpa.Spec.MaxReplicas, hpa.Status.DesiredReplicas),
			})
		}
		// "cannot scale" — the autoscaler controller reports it can't get
		// metrics or scale calls are failing. Emitted as a separate problem
		// so the maxed-check above isn't masked by an unrelated metrics
		// outage on the same HPA.
		for _, cond := range hpa.Status.Conditions {
			if cond.Type == autoscalingv2.ScalingActive && cond.Status == corev1.ConditionFalse {
				reason := cond.Reason
				if reason == "" {
					reason = "ScalingActive=False"
				}
				msg := cond.Message
				if msg == "" {
					msg = "HPA controller reports it cannot scale this workload"
				}
				problems = append(problems, HPAProblem{
					Name:      hpa.Name,
					Namespace: hpa.Namespace,
					Problem:   "cannot-scale",
					Reason:    fmt.Sprintf("%s: %s", reason, msg),
				})
				break
			}
		}
	}
	return problems
}

// CronJobProblem describes a detected issue with a CronJob.
type CronJobProblem struct {
	Name      string
	Namespace string
	Problem   string // "stale" or "never-scheduled"
	Reason    string
}

// estimateCronMinInterval returns a coarse lower bound on the time between runs
// of a standard 5-field cron schedule (minute hour dom month dow), plus the
// common @-macros. It is deliberately approximate — its only job is to keep
// DetectCronJobProblems from flagging a rare-cadence job (weekly / monthly /
// quarterly) as "stale" against a flat daily threshold. ok=false for schedules
// it can't parse; the caller then falls back to the flat threshold.
func estimateCronMinInterval(schedule string) (time.Duration, bool) {
	const day = 24 * time.Hour
	s := strings.TrimSpace(schedule)
	switch s {
	case "@yearly", "@annually":
		return 365 * day, true
	case "@monthly":
		return 28 * day, true
	case "@weekly":
		return 7 * day, true
	case "@daily", "@midnight":
		return day, true
	case "@hourly":
		return time.Hour, true
	}
	fields := strings.Fields(s)
	if len(fields) != 5 {
		return 0, false
	}
	hour, dom, month, dow := fields[1], fields[2], fields[3], fields[4]
	switch {
	case month != "*":
		// Constrained to certain months → at most monthly, often far less.
		return 28 * day, true
	case dom != "*":
		// Specific day(s)-of-month → monthly cadence.
		return 28 * day, true
	case dow != "*":
		// Specific day(s)-of-week → weekly is the conservative lower bound.
		return 7 * day, true
	case hour != "*" && !strings.HasPrefix(hour, "*/"):
		// Specific hour(s) each day → daily.
		return day, true
	default:
		// Intra-day cadence (every minute / */n minutes or hours).
		return time.Hour, true
	}
}

// DetectCronJobProblems finds non-suspended CronJobs that haven't run recently.
func DetectCronJobProblems(cronjobs []*batchv1.CronJob) []CronJobProblem {
	var problems []CronJobProblem
	now := time.Now()
	for _, cj := range cronjobs {
		if cj.Spec.Suspend != nil && *cj.Spec.Suspend {
			continue
		}
		// Staleness is relative to the schedule's cadence, not a flat day: a
		// quarterly job that ran on schedule 29 days ago is healthy, not stale.
		// Floor at 24h so frequent jobs keep the original sensitivity.
		threshold := 24 * time.Hour
		if interval, ok := estimateCronMinInterval(cj.Spec.Schedule); ok {
			if grace := interval + interval/2; grace > threshold {
				threshold = grace
			}
		}
		if cj.Status.LastScheduleTime != nil {
			sinceLast := now.Sub(cj.Status.LastScheduleTime.Time)
			if sinceLast > threshold {
				problems = append(problems, CronJobProblem{
					Name:      cj.Name,
					Namespace: cj.Namespace,
					Problem:   "stale",
					Reason:    fmt.Sprintf("last run %dh ago", int(sinceLast.Hours())),
				})
			}
		} else if now.Sub(cj.CreationTimestamp.Time) > threshold {
			problems = append(problems, CronJobProblem{
				Name:      cj.Name,
				Namespace: cj.Namespace,
				Problem:   "never-scheduled",
				Reason:    "created but never ran",
			})
		}
	}
	return problems
}
