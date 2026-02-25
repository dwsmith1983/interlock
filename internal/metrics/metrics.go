// Package metrics exposes runtime counters via expvar.
package metrics

import "expvar"

var (
	EvaluationsTotal        = expvar.NewInt("evaluations_total")
	EvaluationErrors        = expvar.NewInt("evaluation_errors")
	TriggersTotal           = expvar.NewInt("triggers_total")
	TriggersFailed          = expvar.NewInt("triggers_failed")
	AlertsDispatched        = expvar.NewInt("alerts_dispatched")
	AlertsFailed            = expvar.NewInt("alerts_failed")
	RetriesScheduled        = expvar.NewInt("retries_scheduled")
	SLABreaches             = expvar.NewInt("sla_breaches")
	MonitoringDriftDetected = expvar.NewInt("monitoring_drift_detected")
	SchedulesMissed         = expvar.NewInt("schedules_missed")
	RunsStuck               = expvar.NewInt("runs_stuck")
	LifecycleEventsPublished = expvar.NewInt("lifecycle_events_published")
)
