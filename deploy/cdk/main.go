package main

import (
	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/jsii-runtime-go"
)

func main() {
	defer jsii.Close()

	app := awscdk.NewApp(nil)
	cfg := DefaultConfig()
	NewInterlockStack(app, "InterlockStack", cfg)
	app.Synth(nil)
}
