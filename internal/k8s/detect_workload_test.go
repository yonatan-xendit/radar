package k8s

import (
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
)

func TestDetectHPAProblems(t *testing.T) {
	tests := []struct {
		name        string
		hpas        []*autoscalingv2.HorizontalPodAutoscaler
		wantCount   int
		wantProblem string
	}{
		{
			name: "maxed HPA",
			hpas: []*autoscalingv2.HorizontalPodAutoscaler{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
					Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 10},
					Status:     autoscalingv2.HorizontalPodAutoscalerStatus{CurrentReplicas: 10, DesiredReplicas: 10},
				},
			},
			wantCount:   1,
			wantProblem: "maxed",
		},
		{
			name: "not maxed",
			hpas: []*autoscalingv2.HorizontalPodAutoscaler{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "default"},
					Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 10},
					Status:     autoscalingv2.HorizontalPodAutoscalerStatus{CurrentReplicas: 5, DesiredReplicas: 5},
				},
			},
			wantCount: 0,
		},
		{
			name: "zero replicas",
			hpas: []*autoscalingv2.HorizontalPodAutoscaler{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "idle", Namespace: "default"},
					Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 10},
					Status:     autoscalingv2.HorizontalPodAutoscalerStatus{CurrentReplicas: 0, DesiredReplicas: 0},
				},
			},
			wantCount: 0,
		},
		{
			name: "maxReplicas zero is not a problem",
			hpas: []*autoscalingv2.HorizontalPodAutoscaler{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "broken", Namespace: "default"},
					Spec:       autoscalingv2.HorizontalPodAutoscalerSpec{MaxReplicas: 0},
					Status:     autoscalingv2.HorizontalPodAutoscalerStatus{CurrentReplicas: 0, DesiredReplicas: 0},
				},
			},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems := DetectHPAProblems(tt.hpas)
			if len(problems) != tt.wantCount {
				t.Errorf("DetectHPAProblems() returned %d problems, want %d", len(problems), tt.wantCount)
			}
			if tt.wantCount > 0 && len(problems) > 0 {
				if problems[0].Problem != tt.wantProblem {
					t.Errorf("problem = %q, want %q", problems[0].Problem, tt.wantProblem)
				}
			}
		})
	}
}

func TestDetectCronJobProblems(t *testing.T) {
	now := time.Now()
	suspended := true
	notSuspended := false
	oldTime := metav1.NewTime(now.Add(-48 * time.Hour))
	freshTime := metav1.NewTime(now.Add(-1 * time.Hour))

	tests := []struct {
		name        string
		cronjobs    []*batchv1.CronJob
		wantCount   int
		wantProblem string
	}{
		{
			name: "stale cronjob",
			cronjobs: []*batchv1.CronJob{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: "default", CreationTimestamp: metav1.NewTime(now.Add(-72 * time.Hour))},
					Spec:       batchv1.CronJobSpec{Schedule: "0 2 * * *", Suspend: &notSuspended},
					Status:     batchv1.CronJobStatus{LastScheduleTime: &oldTime},
				},
			},
			wantCount:   1,
			wantProblem: "stale",
		},
		{
			name: "suspended old cronjob is ok",
			cronjobs: []*batchv1.CronJob{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: "default", CreationTimestamp: metav1.NewTime(now.Add(-72 * time.Hour))},
					Spec:       batchv1.CronJobSpec{Schedule: "0 2 * * *", Suspend: &suspended},
					Status:     batchv1.CronJobStatus{LastScheduleTime: &oldTime},
				},
			},
			wantCount: 0,
		},
		{
			name: "fresh cronjob is ok",
			cronjobs: []*batchv1.CronJob{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: "default", CreationTimestamp: metav1.NewTime(now.Add(-72 * time.Hour))},
					Spec:       batchv1.CronJobSpec{Schedule: "0 2 * * *", Suspend: &notSuspended},
					Status:     batchv1.CronJobStatus{LastScheduleTime: &freshTime},
				},
			},
			wantCount: 0,
		},
		{
			name: "never-scheduled cronjob",
			cronjobs: []*batchv1.CronJob{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "new-cron", Namespace: "default", CreationTimestamp: metav1.NewTime(now.Add(-48 * time.Hour))},
					Spec:       batchv1.CronJobSpec{Schedule: "0 * * * *", Suspend: &notSuspended},
					Status:     batchv1.CronJobStatus{},
				},
			},
			wantCount:   1,
			wantProblem: "never-scheduled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems := DetectCronJobProblems(tt.cronjobs)
			if len(problems) != tt.wantCount {
				t.Errorf("DetectCronJobProblems() returned %d problems, want %d", len(problems), tt.wantCount)
			}
			if tt.wantCount > 0 && len(problems) > 0 {
				if problems[0].Problem != tt.wantProblem {
					t.Errorf("problem = %q, want %q", problems[0].Problem, tt.wantProblem)
				}
			}
		})
	}
}
