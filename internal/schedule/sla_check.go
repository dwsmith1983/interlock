package schedule

import (
	"time"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// SLACheckResult contains the result of an SLA deadline check.
type SLACheckResult struct {
	// Breached is true if the current time is past the deadline.
	Breached bool
	// AtRisk is true if the current time is within the at-risk lead window.
	AtRisk bool
	// Deadline is the resolved absolute deadline time.
	Deadline time.Time
}

// CheckEvaluationSLA determines if the evaluation SLA is breached or at-risk
// for the given pipeline. Returns a zero-value result with no error if the
// pipeline has no evaluation SLA configured.
func CheckEvaluationSLA(pipeline types.PipelineConfig, now time.Time, refTime time.Time) (SLACheckResult, error) {
	if pipeline.SLA == nil || pipeline.SLA.EvaluationDeadline == "" {
		return SLACheckResult{}, nil
	}

	deadline, err := ParseSLADeadline(pipeline.SLA.EvaluationDeadline, pipeline.SLA.Timezone, now, refTime)
	if err != nil {
		return SLACheckResult{}, err
	}

	result := SLACheckResult{Deadline: deadline}
	result.Breached = IsBreached(deadline, now)

	if pipeline.SLA.AtRiskLeadTime != "" {
		leadTime, ltErr := time.ParseDuration(pipeline.SLA.AtRiskLeadTime)
		if ltErr == nil {
			result.AtRisk = IsAtRisk(deadline, now, leadTime)
		}
	}

	return result, nil
}

// CheckCompletionSLA determines if the completion SLA is breached or at-risk
// for the given pipeline and schedule. Returns a zero-value result with no
// error if no completion deadline is configured.
func CheckCompletionSLA(pipeline types.PipelineConfig, scheduleID string, now time.Time, refTime time.Time) (SLACheckResult, error) {
	var sched types.ScheduleConfig
	for _, s := range types.ResolveSchedules(pipeline) {
		if s.Name == scheduleID {
			sched = s
			break
		}
	}

	deadline, ok := ScheduleDeadline(sched, pipeline, now, refTime)
	if !ok {
		return SLACheckResult{}, nil
	}

	result := SLACheckResult{Deadline: deadline}
	result.Breached = IsBreached(deadline, now)

	if pipeline.SLA != nil && pipeline.SLA.AtRiskLeadTime != "" {
		leadTime, ltErr := time.ParseDuration(pipeline.SLA.AtRiskLeadTime)
		if ltErr == nil {
			result.AtRisk = IsAtRisk(deadline, now, leadTime)
		}
	}

	return result, nil
}

// CheckValidationTimeout determines if the validation timeout has been
// exceeded for the given pipeline. Returns (false, nil) if the pipeline
// has no validation timeout configured.
func CheckValidationTimeout(pipeline types.PipelineConfig, now time.Time, refTime time.Time) (bool, error) {
	if pipeline.SLA == nil || pipeline.SLA.ValidationTimeout == "" {
		return false, nil
	}

	deadline, err := ParseSLADeadline(pipeline.SLA.ValidationTimeout, pipeline.SLA.Timezone, now, refTime)
	if err != nil {
		return false, err
	}

	return IsBreached(deadline, now), nil
}
