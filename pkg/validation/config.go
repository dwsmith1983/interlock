package validation

import (
	"fmt"
	"regexp"
	"time"

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

// ValidatePipelineConfig checks the pipeline ID and that all retry and
// timeout fields are within safe bounds. Returns a list of human-readable
// validation errors.
func ValidatePipelineConfig(cfg *types.PipelineConfig) []string {
	var errs []string

	if id := cfg.Pipeline.ID; len(id) > MaxPipelineIDLen || !pipelineIDRe.MatchString(id) {
		errs = append(errs, fmt.Sprintf("pipeline.id %q must be 1-%d characters of A-Z, a-z, 0-9, _ and -", id, MaxPipelineIDLen))
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
