// Package types defines the public domain types for the Interlock STAMP-based safety framework.
package types

// DefaultScheduleID is the implicit schedule name for pipelines without explicit schedules.
const DefaultScheduleID = "daily"

// TraitStatus represents the evaluation outcome of a trait.
type TraitStatus string

// TraitStatus values enumerate the possible trait evaluation outcomes.
const (
	TraitPass  TraitStatus = "PASS"
	TraitFail  TraitStatus = "FAIL"
	TraitError TraitStatus = "ERROR"
	TraitStale TraitStatus = "STALE"
)

// ReadinessStatus represents whether a pipeline is ready to execute.
type ReadinessStatus string

// ReadinessStatus values indicate whether a pipeline is ready or not.
const (
	Ready    ReadinessStatus = "READY"
	NotReady ReadinessStatus = "NOT_READY"
)

// RunStatus represents the lifecycle state of a pipeline run.
type RunStatus string

// RunStatus values represent the lifecycle states of a pipeline run.
const (
	RunPending             RunStatus = "PENDING"
	RunTriggering          RunStatus = "TRIGGERING"
	RunRunning             RunStatus = "RUNNING"
	RunCompleted           RunStatus = "COMPLETED"
	RunCompletedMonitoring RunStatus = "COMPLETED_MONITORING"
	RunFailed              RunStatus = "FAILED"
	RunCancelled           RunStatus = "CANCELLED"
)

// ReadinessRuleType defines how traits are combined to determine readiness.
type ReadinessRuleType string

// ReadinessRuleType values define the supported readiness evaluation strategies.
const (
	AllRequiredPass ReadinessRuleType = "all-required-pass"
	MajorityPass    ReadinessRuleType = "majority-pass"
)

// TriggerType defines how a pipeline is triggered.
type TriggerType string

// TriggerType values enumerate the supported pipeline trigger mechanisms.
const (
	TriggerHTTP          TriggerType = "http"
	TriggerCommand       TriggerType = "command"
	TriggerAirflow       TriggerType = "airflow"
	TriggerGlue          TriggerType = "glue"
	TriggerEMR           TriggerType = "emr"
	TriggerEMRServerless TriggerType = "emr-serverless"
	TriggerDatabricks    TriggerType = "databricks"
	TriggerStepFunction  TriggerType = "step-function"
)

// AlertType defines the alert sink type.
type AlertType string

// AlertType values enumerate the supported alert sink backends.
const (
	AlertConsole AlertType = "console"
	AlertWebhook AlertType = "webhook"
	AlertFile    AlertType = "file"
	AlertSNS     AlertType = "sns"
	AlertS3      AlertType = "s3"
)

// AlertLevel replaces string-typed alert levels with a proper enum.
type AlertLevel string

const (
	AlertLevelError   AlertLevel = "error"
	AlertLevelWarning AlertLevel = "warning"
	AlertLevelInfo    AlertLevel = "info"
)

// FailureCategory classifies why a trait evaluation or trigger failed.
type FailureCategory string

const (
	FailureTransient      FailureCategory = "TRANSIENT"
	FailurePermanent      FailureCategory = "PERMANENT"
	FailureTimeout        FailureCategory = "TIMEOUT"
	FailureEvaluatorCrash FailureCategory = "EVALUATOR_CRASH"
)

// EventKind classifies the type of audit event.
type EventKind string

// EventKind values enumerate the categories of recorded events.
const (
	EventTraitEvaluated        EventKind = "TRAIT_EVALUATED"
	EventReadinessChecked      EventKind = "READINESS_CHECKED"
	EventRunStateChanged       EventKind = "RUN_STATE_CHANGED"
	EventTriggerFired          EventKind = "TRIGGER_FIRED"
	EventTriggerFailed         EventKind = "TRIGGER_FAILED"
	EventCallbackReceived      EventKind = "CALLBACK_RECEIVED"
	EventRetryScheduled        EventKind = "RETRY_SCHEDULED"
	EventRetryExhausted        EventKind = "RETRY_EXHAUSTED"
	EventSLABreached           EventKind = "SLA_BREACHED"
	EventWatcherEvaluation     EventKind = "WATCHER_EVALUATION"
	EventTraitPushed           EventKind = "TRAIT_PUSHED"
	EventRerunRequested        EventKind = "RERUN_REQUESTED"
	EventRerunCompleted        EventKind = "RERUN_COMPLETED"
	EventMonitoringStarted     EventKind = "MONITORING_STARTED"
	EventMonitoringDrift       EventKind = "MONITORING_DRIFT_DETECTED"
	EventMonitoringCompleted   EventKind = "MONITORING_COMPLETED"
	EventScheduleMissed        EventKind = "SCHEDULE_MISSED"
	EventPipelineCompleted     EventKind = "PIPELINE_COMPLETED"
	EventPipelineFailed        EventKind = "PIPELINE_FAILED"
	EventRunStuck              EventKind = "RUN_STUCK"
	EventReadinessCacheChecked EventKind = "READINESS_CACHE_CHECKED"
	EventCircuitBreakerTripped EventKind = "CIRCUIT_BREAKER_TRIPPED"
	EventWatchdogRetrigger     EventKind = "WATCHDOG_RETRIGGER"
	EventDataQuarantined       EventKind = "DATA_QUARANTINED"
)

// EvaluationSessionStatus tracks the lifecycle of an evaluation session.
type EvaluationSessionStatus string

const (
	SessionRunning   EvaluationSessionStatus = "RUNNING"
	SessionCompleted EvaluationSessionStatus = "COMPLETED"
	SessionFailed    EvaluationSessionStatus = "FAILED"
)
