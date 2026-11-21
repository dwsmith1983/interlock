package validation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	// Validation and the Lambdas must load the same zones whatever the runtime image has.
	_ "time/tzdata"

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

// cutoffRe is a schedule.trigger.cutoff: "HH:MM" (daily) or ":MM" (hourly).
var cutoffRe = regexp.MustCompile(`^([01]\d|2[0-3])?:[0-5]\d$`)

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

	if check := triggerConfigChecks[cfg.Job.Type]; check != nil {
		errs = append(errs, check(cfg.Job.Config)...)
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

	if tz := cfg.Schedule.Timezone; !isZoneName(tz) {
		errs = append(errs, fmt.Sprintf("schedule.timezone %q is not an IANA time zone name", tz))
	}
	if cfg.SLA != nil && !isZoneName(cfg.SLA.Timezone) {
		errs = append(errs, fmt.Sprintf("sla.timezone %q is not an IANA time zone name", cfg.SLA.Timezone))
	}

	// Cron and Include are mutually exclusive.
	if cfg.Schedule.Cron != "" && cfg.Schedule.Include != nil {
		errs = append(errs, "schedule.cron and schedule.include are mutually exclusive")
	}
	if cfg.Schedule.Cron != "" && cfg.Schedule.Time != "" {
		errs = append(errs, "schedule.time is not used with schedule.cron; the cron time is the start-by time")
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
		// expectedDuration is the warning offset before the relative breach.
		if exp := cfg.SLA.ExpectedDuration; err == nil && exp != "" {
			if e, perr := time.ParseDuration(exp); perr != nil {
				errs = append(errs, fmt.Sprintf("sla.expectedDuration invalid Go duration %q", exp))
			} else if e <= 0 || e >= d {
				errs = append(errs, fmt.Sprintf("sla.expectedDuration %q must be positive and below sla.maxDuration %q", exp, cfg.SLA.MaxDuration))
			}
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

	if tr := cfg.Schedule.Trigger; tr != nil {
		if tr.Cutoff != "" && !cutoffRe.MatchString(tr.Cutoff) {
			errs = append(errs, fmt.Sprintf("schedule.trigger.cutoff %q must be HH:MM or :MM", tr.Cutoff))
		}
		switch off := tr.CutoffDayOffset; {
		case off == 0:
		case off < 0 || off > 7:
			errs = append(errs, fmt.Sprintf("schedule.trigger.cutoffDayOffset %d out of range [0,7]", off))
		case tr.Cutoff == "":
			errs = append(errs, "schedule.trigger.cutoffDayOffset is not supported without schedule.trigger.cutoff")
		case strings.HasPrefix(tr.Cutoff, ":"):
			errs = append(errs, `schedule.trigger.cutoffDayOffset is not supported with hourly ":MM" cutoffs`)
		}
	}

	if cfg.PostRun != nil {
		errs = append(errs, validatePostRun(cfg.PostRun)...)
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

// triggerConfigChecks checks the job.config of each job.type: the fields its
// trigger rejects when empty, and the values it cannot use.
var triggerConfigChecks = map[types.TriggerType]func(map[string]interface{}) []string{
	types.TriggerHTTP: decodeTrigger(func(c *types.HTTPTriggerConfig) []string {
		errs := required("url", c.URL)
		if c.Method != "" && !slices.Contains(httpMethods, c.Method) {
			errs = append(errs, fmt.Sprintf("job.config.method %q must be empty or one of %s", c.Method, strings.Join(httpMethods, ", ")))
		}
		return errs
	}),
	types.TriggerAirflow: decodeTrigger(func(c *airflowConfig) []string {
		errs := required("url", c.URL, "dagId", c.DagID)
		if v, ok := airflowAPIVersion(c.APIVersion); !ok {
			errs = append(errs, fmt.Sprintf("job.config.apiVersion %s must be 2 or 3", v))
		}
		return errs
	}),
	types.TriggerGlue: decodeTrigger(func(c *types.GlueTriggerConfig) []string {
		return required("jobName", c.JobName)
	}),
	types.TriggerEMR: decodeTrigger(func(c *types.EMRTriggerConfig) []string {
		return required("clusterId", c.ClusterID, "stepName", c.StepName, "command", c.Command)
	}),
	types.TriggerEMRServerless: decodeTrigger(func(c *types.EMRServerlessTriggerConfig) []string {
		errs := required("applicationId", c.ApplicationID, "jobName", c.JobName,
			"executionRoleArn", c.ExecutionRoleARN, "entryPoint", c.EntryPoint)
		return append(errs, dateFlagErrors(c.EntryPointArguments)...)
	}),
	types.TriggerDatabricks: decodeTrigger(func(c *databricksConfig) []string {
		errs := required("workspaceUrl", c.WorkspaceURL)
		if msg := databricksJobIDError(c.JobID); msg != "" {
			errs = append(errs, "job.config.jobId "+msg)
		}
		return errs
	}),
	types.TriggerStepFunction: decodeTrigger(func(c *types.StepFunctionTriggerConfig) []string {
		return required("stateMachineArn", c.StateMachineARN)
	}),
	types.TriggerLambda: decodeTrigger(func(c *types.LambdaTriggerConfig) []string {
		return required("functionName", c.FunctionName)
	}),
}

// httpMethods are the methods an http trigger may set. They are upper case,
// as the redirect rule compares the method with "GET" and "HEAD".
var httpMethods = []string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE"}

// airflowConfig and databricksConfig keep apiVersion and jobId as raw JSON,
// so a value of the wrong type is reported as it was written.
type airflowConfig struct {
	types.AirflowTriggerConfig
	APIVersion json.RawMessage `json:"apiVersion"`
}

type databricksConfig struct {
	types.DatabricksTriggerConfig
	JobID json.RawMessage `json:"jobId"`
}

// decodeTrigger returns a check that decodes a job.config into T through
// JSON, as the orchestrator decodes the trigger config (keys match without
// regard to case), refusing keys T does not have, and then runs check on it.
// A config that does not decode reports only the decode error.
func decodeTrigger[T any](check func(*T) []string) func(map[string]interface{}) []string {
	return func(conf map[string]interface{}) []string {
		data, err := json.Marshal(conf)
		if err != nil {
			return []string{"job.config is not encodable as JSON: " + err.Error()}
		}
		var c T
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&c); err != nil {
			return []string{decodeError(err)}
		}
		return check(&c)
	}
}

// jsonValues names each JSON value kind as a decode error reports it.
var jsonValues = map[string]string{
	"string": "a string", "number": "a number", "bool": "a boolean", "object": "an object", "array": "a list",
}

// expectedValue names the JSON value a Go type of kind k decodes from.
func expectedValue(k reflect.Kind) string {
	switch {
	case k == reflect.String:
		return "a string"
	case k >= reflect.Int && k <= reflect.Uint64:
		return "an integer"
	case k == reflect.Float32 || k == reflect.Float64:
		return "a number"
	case k == reflect.Bool:
		return "a boolean"
	case k == reflect.Map || k == reflect.Struct:
		return "an object"
	case k == reflect.Slice || k == reflect.Array:
		return "a list"
	}
	return "another value"
}

// decodeError describes a job.config decode error by the struct's JSON key,
// whatever case the config used (DAGID: 5 reports job.config.dagId). A
// wrong-type error's Field is a dotted path, led by an embedded
// struct's name for airflowConfig and databricksConfig, whose last element is
// the top-level JSON key: the trigger configs have no nested structs, and the
// error for a map value or list element names the map or list.
func decodeError(err error) string {
	var typeErr *json.UnmarshalTypeError
	if !errors.As(err, &typeErr) || typeErr.Field == "" {
		return "job.config: " + strings.TrimPrefix(err.Error(), "json: ")
	}
	field := typeErr.Field[strings.LastIndex(typeErr.Field, ".")+1:]
	// Value is the kind, then for a number the number itself: never shown.
	kind, _, _ := strings.Cut(typeErr.Value, " ")
	got, ok := jsonValues[kind]
	if !ok {
		got = "a value"
	}
	// Type is the Go type of the element the value was meant for, so a map
	// value or list element names what the entry needs.
	return fmt.Sprintf("job.config.%s holds %s where %s is expected", field, got, expectedValue(typeErr.Type.Kind()))
}

// required reports each empty value of the name, value pairs, in order.
func required(pairs ...string) []string {
	var errs []string
	for i := 0; i+1 < len(pairs); i += 2 {
		if pairs[i+1] == "" {
			errs = append(errs, "job.config."+pairs[i]+" is required")
		}
	}
	return errs
}

// dateFlagErrors reports a --par_day or --par_hour entry point argument with
// no value after it: the orchestrator writes the run date into the argument
// that follows the flag, which would otherwise be another flag or missing.
func dateFlagErrors(args []string) []string {
	var errs []string
	for _, flag := range []string{"--par_day", "--par_hour"} {
		if i := slices.Index(args, flag); i >= 0 && (i+1 == len(args) || strings.HasPrefix(args[i+1], "--")) {
			errs = append(errs, "job.config.entryPointArguments has no value after "+flag)
		}
	}
	return errs
}

// airflowAPIVersion reports whether an airflow apiVersion is unset, 2 or 3,
// and returns it as JSON for the error.
func airflowAPIVersion(raw json.RawMessage) (string, bool) {
	if raw == nil {
		return "", true
	}
	var n int
	if json.Unmarshal(raw, &n) != nil {
		return string(raw), false
	}
	return string(raw), n == 0 || n == 2 || n == 3
}

// databricksJobIDError says what is wrong with a databricks jobId, or returns
// "" for a string holding a positive base-10 integer. A number is refused, as
// the orchestrator's decode into a string fails on it.
func databricksJobIDError(raw json.RawMessage) string {
	if raw == nil || string(raw) == "null" {
		return "is required"
	}
	var id string
	if json.Unmarshal(raw, &id) != nil {
		return string(raw) + " must be a string: quote the job ID, as a number loses precision above 2^53"
	}
	if id == "" {
		return "is required"
	}
	if n, err := strconv.ParseInt(id, 10, 64); err != nil || n <= 0 {
		return fmt.Sprintf("%q must be a positive base-10 integer job ID", id)
	}
	return ""
}

// isZoneName reports whether tz is empty (UTC) or a zone time.LoadLocation
// loads. "Local" is rejected: it is the host's zone, not a named one.
func isZoneName(tz string) bool {
	if tz == "" {
		return true
	}
	if tz == "Local" {
		return false
	}
	_, err := time.LoadLocation(tz)
	return err == nil
}

// maxPostRunWatchWindow bounds postRun.watchWindow: job and rerun rows live 30 days.
const maxPostRunWatchWindow = 720 * time.Hour

// validatePostRun checks the post-run durations. With rules, the watchdog's
// sensor timeout must end inside the watch window (both at their defaults
// when unset); an unparseable value is reported once, on its own.
func validatePostRun(pr *types.PostRunConfig) []string {
	var errs []string
	window, windowOK := types.DefaultPostRunWatchWindow, true
	if pr.WatchWindow != "" {
		d, err := time.ParseDuration(pr.WatchWindow)
		switch {
		case err != nil:
			errs, windowOK = append(errs, fmt.Sprintf("postRun.watchWindow invalid Go duration %q", pr.WatchWindow)), false
		case d <= 0 || d > maxPostRunWatchWindow:
			errs, windowOK = append(errs, fmt.Sprintf("postRun.watchWindow %q out of range (0s,720h]", pr.WatchWindow)), false
		default:
			window = d
		}
	}
	timeout, timeoutOK := types.DefaultPostRunSensorTimeout, true
	if pr.SensorTimeout != "" {
		d, err := time.ParseDuration(pr.SensorTimeout)
		switch {
		case err != nil:
			errs, timeoutOK = append(errs, fmt.Sprintf("postRun.sensorTimeout invalid Go duration %q", pr.SensorTimeout)), false
		case d <= 0:
			errs, timeoutOK = append(errs, fmt.Sprintf("postRun.sensorTimeout %q must be positive", pr.SensorTimeout)), false
		default:
			timeout = d
		}
	}
	if len(pr.Rules) > 0 && windowOK && timeoutOK && timeout > window {
		errs = append(errs, fmt.Sprintf("postRun.sensorTimeout %s exceeds postRun.watchWindow %s",
			withDefaultMark(timeout, pr.SensorTimeout), withDefaultMark(window, pr.WatchWindow)))
	}
	return errs
}

// withDefaultMark formats d, adding " (default)" when the setting was unset.
func withDefaultMark(d time.Duration, set string) string {
	if set == "" {
		return d.String() + " (default)"
	}
	return d.String()
}
