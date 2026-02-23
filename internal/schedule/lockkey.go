package schedule

// LockKey returns the distributed lock key for a pipeline schedule evaluation.
func LockKey(pipelineID, scheduleID string) string {
	return "eval:" + pipelineID + ":" + scheduleID
}
