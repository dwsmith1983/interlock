//go:build !e2e

package lambda

import "github.com/aws/aws-sdk-go-v2/aws"

// ApplyE2EHooks does nothing in production builds. Binaries built with
// -tags e2e get the version in e2e_on.go, which puts the Lambda on the e2e
// harness's simulated clock.
func ApplyE2EHooks(_ *Deps, _ aws.Config) {}
