package main

// StackConfig holds configuration for the Interlock CDK stack.
type StackConfig struct {
	TableName        string
	MemorySize       float64
	Timeout          float64
	LambdaDistDir    string
	LayerDistDir     string
	EvaluatorBaseURL string
	ReadinessTTL     string
	RetentionTTL     string
	LogRetentionDays float64
	ASLPath          string

	// Opt-in trigger permissions
	EnableGlueTrigger          bool
	EnableEMRTrigger           bool
	EnableEMRServerlessTrigger bool
	EnableStepFunctionTrigger  bool
}

// DefaultConfig returns a StackConfig with sensible defaults.
func DefaultConfig() StackConfig {
	return StackConfig{
		TableName:        "interlock",
		MemorySize:       256,
		Timeout:          60,
		LambdaDistDir:    "../dist/lambda",
		LayerDistDir:     "../dist/layer",
		ReadinessTTL:     "1h",
		RetentionTTL:     "168h",
		LogRetentionDays: 7,
		ASLPath:          "../statemachine.asl.json",
	}
}
