package validation

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/dwsmith1983/interlock/pkg/types"
)

// MaxPipelineIDLen is the longest pipeline ID for which every name built from
// it fits its AWS limit with no truncation or hashing. With schedule ID
// "stream", date "YYYY-MM-DDThh" (13), a 10-digit Unix time, a 3-digit rerun
// attempt and a "-r99" retry:
//   - EventBridge Scheduler (64): <id>-stream-<date>-sla-warning adds
//     1+6+1+13+5+7 = 33, so the ID gets 31. This is the binding limit.
//   - Step Functions (80): a rerun's retry
//     <id>-stream-<date>-rerun<attempt>-<unix>-r99 adds 1+6+1+13+6+3+1+10+4
//     = 45 (ID up to 35), and a first run's retry <id>-stream-<date>-<unix>-r99
//     adds 36 (ID up to 44).
const MaxPipelineIDLen = 31

// pipelineIDRe is the pipeline ID alphabet. It is a subset of both the Step
// Functions and the Scheduler name alphabets.
var pipelineIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// triggerTypes is every job.type the orchestrator can launch. Keep it in step
// with the trigger registry in internal/lambda; a test there fails when a
// registered type is missing here.
var triggerTypes = map[types.TriggerType]bool{
	types.TriggerHTTP:          true,
	types.TriggerAirflow:       true,
	types.TriggerGlue:          true,
	types.TriggerEMR:           true,
	types.TriggerEMRServerless: true,
	types.TriggerDatabricks:    true,
	types.TriggerStepFunction:  true,
	types.TriggerLambda:        true,
}

// ParseCron parses expr as a standard five-field cron expression (minute,
// hour, day of month, month, day of week) with robfig/cron's ParseStandard.
// ParseStandard also takes descriptors ("@daily", "@every 1h") and a "TZ=" or
// "CRON_TZ=" prefix; the field count rejects them. A field of only commas
// ("0 8 , * *") parses with no values and never fires, so it is rejected too.
func ParseCron(expr string) (*cron.SpecSchedule, error) {
	if len(strings.Fields(expr)) != 5 {
		return nil, fmt.Errorf("cron %q does not have five fields", expr)
	}
	// ParseStandard panics on a TZ prefix with no space after it, so this
	// check must run before it.
	if strings.HasPrefix(expr, "TZ=") || strings.HasPrefix(expr, "CRON_TZ=") {
		return nil, fmt.Errorf("cron %q has a time zone prefix", expr)
	}
	parsed, err := cron.ParseStandard(expr)
	if err != nil {
		return nil, fmt.Errorf("parse cron %q: %w", expr, err)
	}
	spec, ok := parsed.(*cron.SpecSchedule)
	if !ok {
		return nil, fmt.Errorf("cron %q is not a field schedule", expr)
	}
	if spec.Minute == 0 || spec.Hour == 0 || spec.Dom == 0 || spec.Month == 0 || spec.Dow == 0 {
		return nil, fmt.Errorf("cron %q has a field with no values", expr)
	}
	return spec, nil
}

// ValidatePipelineConfig checks the pipeline ID and that all retry and
// timeout fields are within safe bounds. Returns a list of human-readable
// validation errors.
func ValidatePipelineConfig(cfg *types.PipelineConfig) []string {
	var errs []string

	if id := cfg.Pipeline.ID; len(id) > MaxPipelineIDLen || !pipelineIDRe.MatchString(id) {
		errs = append(errs, fmt.Sprintf("pipeline.id %q must be 1-%d characters of A-Z, a-z, 0-9, _ and -", id, MaxPipelineIDLen))
	}

	if t := cfg.Job.Type; t != "" && !triggerTypes[t] {
		errs = append(errs, fmt.Sprintf("job.type %q is not a supported trigger type", t))
	}

	if cfg.Job.MaxRetries < 0 || cfg.Job.MaxRetries > 10 {
		errs = append(errs, fmt.Sprintf("job.maxRetries %d out of range [0,10]", cfg.Job.MaxRetries))
	}

	if p := cfg.Job.MaxDriftReruns; p != nil && (*p < 0 || *p > 5) {
		errs = append(errs, fmt.Sprintf("job.maxDriftReruns %d out of range [0,5]", *p))
	}

	if p := cfg.Job.MaxManualReruns; p != nil && (*p < 0 || *p > 5) {
		errs = append(errs, fmt.Sprintf("job.maxManualReruns %d out of range [0,5]", *p))
	}

	if p := cfg.Job.MaxCodeRetries; p != nil && (*p < 0 || *p > 3) {
		errs = append(errs, fmt.Sprintf("job.maxCodeRetries %d out of range [0,3]", *p))
	}

	// Zero means "use default" and is valid; only reject explicitly set
	// values that are negative, below the minimum, or above 24 hours.
	if p := cfg.Job.JobPollWindowSeconds; p != nil && *p != 0 && (*p < 60 || *p > 86400) {
		errs = append(errs, fmt.Sprintf("job.jobPollWindowSeconds %d out of range [60,86400]", *p))
	}

	if c := cfg.Schedule.Cron; c != "" {
		if _, err := ParseCron(c); err != nil {
			errs = append(errs, fmt.Sprintf("schedule.cron %q is not a valid five-field cron expression", c))
		}
	}

	// Cron and Include are mutually exclusive.
	if cfg.Schedule.Cron != "" && cfg.Schedule.Include != nil {
		errs = append(errs, "schedule.cron and schedule.include are mutually exclusive")
	}

	// Inclusion calendar validation.
	if inc := cfg.Schedule.Include; inc != nil {
		if len(inc.Dates) == 0 {
			errs = append(errs, "schedule.include.dates must not be empty")
		}
		for i, d := range inc.Dates {
			if _, err := time.Parse("2006-01-02", d); err != nil {
				errs = append(errs, fmt.Sprintf("schedule.include.dates[%d] invalid format %q (expected YYYY-MM-DD)", i, d))
			}
		}
	}

	// Relative SLA (maxDuration) validation.
	if cfg.SLA != nil && cfg.SLA.MaxDuration != "" {
		if cfg.Schedule.Trigger == nil {
			errs = append(errs, "sla.maxDuration requires schedule.trigger (relative SLA needs a sensor signal)")
		}
		d, err := time.ParseDuration(cfg.SLA.MaxDuration)
		if err != nil {
			errs = append(errs, fmt.Sprintf("sla.maxDuration invalid Go duration %q", cfg.SLA.MaxDuration))
		} else if d > 24*time.Hour {
			errs = append(errs, fmt.Sprintf("sla.maxDuration exceeds 24h (%s)", cfg.SLA.MaxDuration))
		}
	}

	if cfg.SLA != nil && cfg.SLA.DeadlineDayOffset != 0 {
		switch off := cfg.SLA.DeadlineDayOffset; {
		case off < 0 || off > 7:
			errs = append(errs, fmt.Sprintf("sla.deadlineDayOffset %d out of range [0,7]", off))
		case cfg.SLA.Deadline == "":
			errs = append(errs, "sla.deadlineDayOffset is not supported without sla.deadline")
		case strings.HasPrefix(cfg.SLA.Deadline, ":"):
			errs = append(errs, `sla.deadlineDayOffset is not supported with hourly ":MM" deadlines`)
		}
	}

	// Dry-run validation.
	if cfg.DryRun {
		if cfg.Job.Type == "" {
			errs = append(errs, "dryRun requires job.type to be configured")
		}
		if cfg.Schedule.Trigger == nil {
			errs = append(errs, "dryRun requires schedule.trigger (sensor-driven evaluation)")
		}
	}

	return errs
}
