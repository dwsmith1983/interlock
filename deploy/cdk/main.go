package main

import (
	"os"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
)

func main() {
	defer jsii.Close()

	app := awscdk.NewApp(nil)
	cfg := DefaultConfig()

	if name := os.Getenv("INTERLOCK_TABLE_NAME"); name != "" {
		cfg.TableName = name
	}
	if url := os.Getenv("EVALUATOR_BASE_URL"); url != "" {
		cfg.EvaluatorBaseURL = url
	}
	cfg.DestroyOnDelete = os.Getenv("INTERLOCK_DESTROY_ON_DELETE") == "true"

	stackName := "InterlockStack"
	if name := os.Getenv("INTERLOCK_STACK_NAME"); name != "" {
		stackName = name
	}

	NewInterlockStack(app, stackName, cfg)
	app.Synth(nil)
}
