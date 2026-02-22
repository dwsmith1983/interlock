package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/assertions"
	"github.com/aws/jsii-runtime-go"
	"github.com/stretchr/testify/require"
)

// setupTestDirs creates temp directories with dummy bootstrap files and a
// minimal ASL file so CDK asset resolution succeeds without a real build.
func setupTestDirs(t *testing.T) StackConfig {
	t.Helper()
	tmp := t.TempDir()

	lambdaDir := filepath.Join(tmp, "lambda")
	handlers := []string{"stream-router", "evaluator", "orchestrator", "trigger", "run-checker"}
	for _, h := range handlers {
		dir := filepath.Join(lambdaDir, h)
		require.NoError(t, os.MkdirAll(dir, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, "bootstrap"), []byte("#!/bin/sh\n"), 0o755))
	}

	layerDir := filepath.Join(tmp, "layer")
	arcDir := filepath.Join(layerDir, "archetypes")
	require.NoError(t, os.MkdirAll(arcDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(arcDir, "test.yaml"), []byte("name: test\n"), 0o644))

	// Write minimal ASL
	asl := map[string]interface{}{
		"StartAt": "End",
		"States": map[string]interface{}{
			"End": map[string]interface{}{"Type": "Succeed"},
		},
	}
	aslBytes, _ := json.Marshal(asl)
	aslPath := filepath.Join(tmp, "statemachine.asl.json")
	require.NoError(t, os.WriteFile(aslPath, aslBytes, 0o644))

	cfg := DefaultConfig()
	cfg.LambdaDistDir = lambdaDir
	cfg.LayerDistDir = layerDir
	cfg.ASLPath = aslPath
	return cfg
}

func synthTemplate(t *testing.T, cfg StackConfig) assertions.Template {
	t.Helper()
	app := awscdk.NewApp(nil)
	stack := NewInterlockStack(app, "TestStack", cfg)
	return assertions.Template_FromStack(stack, nil)
}

func TestDynamoDBTable(t *testing.T) {
	cfg := setupTestDirs(t)
	tmpl := synthTemplate(t, cfg)

	tmpl.HasResourceProperties(jsii.String("AWS::DynamoDB::GlobalTable"), map[string]interface{}{
		"TableName": jsii.String("interlock"),
		"KeySchema": &[]interface{}{
			map[string]interface{}{"AttributeName": jsii.String("PK"), "KeyType": jsii.String("HASH")},
			map[string]interface{}{"AttributeName": jsii.String("SK"), "KeyType": jsii.String("RANGE")},
		},
		"TimeToLiveSpecification": map[string]interface{}{
			"AttributeName": jsii.String("ttl"),
			"Enabled":       true,
		},
		"StreamSpecification": map[string]interface{}{
			"StreamViewType": jsii.String("NEW_IMAGE"),
		},
	})
}

func TestGSI1(t *testing.T) {
	cfg := setupTestDirs(t)
	tmpl := synthTemplate(t, cfg)

	tmpl.HasResourceProperties(jsii.String("AWS::DynamoDB::GlobalTable"), map[string]interface{}{
		"GlobalSecondaryIndexes": assertions.Match_ArrayWith(&[]interface{}{
			assertions.Match_ObjectLike(&map[string]interface{}{
				"IndexName": jsii.String("GSI1"),
				"KeySchema": &[]interface{}{
					map[string]interface{}{"AttributeName": jsii.String("GSI1PK"), "KeyType": jsii.String("HASH")},
					map[string]interface{}{"AttributeName": jsii.String("GSI1SK"), "KeyType": jsii.String("RANGE")},
				},
			}),
		}),
	})
}

func TestSNSTopic(t *testing.T) {
	cfg := setupTestDirs(t)
	tmpl := synthTemplate(t, cfg)

	tmpl.HasResourceProperties(jsii.String("AWS::SNS::Topic"), map[string]interface{}{
		"TopicName": jsii.String("interlock-alerts"),
	})
}

func TestLambdaFunctionCount(t *testing.T) {
	cfg := setupTestDirs(t)
	tmpl := synthTemplate(t, cfg)

	// 5 handler functions + 1 CDK log-retention custom resource
	tmpl.ResourceCountIs(jsii.String("AWS::Lambda::Function"), jsii.Number(6))
}

func TestLambdaRuntimeAndArch(t *testing.T) {
	cfg := setupTestDirs(t)
	tmpl := synthTemplate(t, cfg)

	names := []string{"orchestrator", "evaluator", "trigger", "run-checker", "stream-router"}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			tmpl.HasResourceProperties(jsii.String("AWS::Lambda::Function"), map[string]interface{}{
				"FunctionName": jsii.String("interlock-" + name),
				"Runtime":      jsii.String("provided.al2023"),
				"Architectures": &[]interface{}{
					jsii.String("arm64"),
				},
				"Handler": jsii.String("bootstrap"),
			})
		})
	}
}

func TestOrchestratorEnvVars(t *testing.T) {
	cfg := setupTestDirs(t)
	tmpl := synthTemplate(t, cfg)

	tmpl.HasResourceProperties(jsii.String("AWS::Lambda::Function"), map[string]interface{}{
		"FunctionName": jsii.String("interlock-orchestrator"),
		"Environment": assertions.Match_ObjectLike(&map[string]interface{}{
			"Variables": assertions.Match_ObjectLike(&map[string]interface{}{
				"ARCHETYPE_DIR": jsii.String("/opt/archetypes"),
				"READINESS_TTL": jsii.String("1h"),
				"RETENTION_TTL": jsii.String("168h"),
			}),
		}),
	})
}

func TestStepFunctionSubstitutions(t *testing.T) {
	cfg := setupTestDirs(t)
	tmpl := synthTemplate(t, cfg)

	tmpl.HasResourceProperties(jsii.String("AWS::StepFunctions::StateMachine"), map[string]interface{}{
		"StateMachineName": jsii.String("interlock-pipeline"),
		"StateMachineType": jsii.String("STANDARD"),
	})

	tmpl.HasResourceProperties(jsii.String("AWS::StepFunctions::StateMachine"), map[string]interface{}{
		"DefinitionSubstitutions": assertions.Match_ObjectLike(&map[string]interface{}{
			"OrchestratorFunctionArn": assertions.Match_ObjectLike(&map[string]interface{}{}),
			"EvaluatorFunctionArn":    assertions.Match_ObjectLike(&map[string]interface{}{}),
			"TriggerFunctionArn":      assertions.Match_ObjectLike(&map[string]interface{}{}),
			"RunCheckerFunctionArn":   assertions.Match_ObjectLike(&map[string]interface{}{}),
		}),
	})
}

func TestEventSourceMapping(t *testing.T) {
	cfg := setupTestDirs(t)
	tmpl := synthTemplate(t, cfg)

	tmpl.HasResourceProperties(jsii.String("AWS::Lambda::EventSourceMapping"), map[string]interface{}{
		"StartingPosition": jsii.String("LATEST"),
		"BatchSize":        jsii.Number(10),
	})
}

func TestStackOutputs(t *testing.T) {
	cfg := setupTestDirs(t)
	tmpl := synthTemplate(t, cfg)

	tmpl.HasOutput(jsii.String("TableName"), map[string]interface{}{})
	tmpl.HasOutput(jsii.String("TopicArn"), map[string]interface{}{})
	tmpl.HasOutput(jsii.String("StateMachineArn"), map[string]interface{}{})
}

func TestNoGluePermissionsWhenDisabled(t *testing.T) {
	cfg := setupTestDirs(t)
	cfg.EnableGlueTrigger = false
	tmpl := synthTemplate(t, cfg)

	tpl := tmpl.ToJSON()
	tplBytes, _ := json.Marshal(tpl)
	require.NotContains(t, string(tplBytes), "glue:StartJobRun")
}

func TestGluePermissionsWhenEnabled(t *testing.T) {
	cfg := setupTestDirs(t)
	cfg.EnableGlueTrigger = true
	tmpl := synthTemplate(t, cfg)

	tmpl.HasResourceProperties(jsii.String("AWS::IAM::Policy"), map[string]interface{}{
		"PolicyDocument": assertions.Match_ObjectLike(&map[string]interface{}{
			"Statement": assertions.Match_ArrayWith(&[]interface{}{
				assertions.Match_ObjectLike(&map[string]interface{}{
					"Action": assertions.Match_ArrayWith(&[]interface{}{
						jsii.String("glue:StartJobRun"),
					}),
				}),
			}),
		}),
	})
}

func TestEMRPermissionsWhenEnabled(t *testing.T) {
	cfg := setupTestDirs(t)
	cfg.EnableEMRTrigger = true
	tmpl := synthTemplate(t, cfg)

	tpl := tmpl.ToJSON()
	tplBytes, _ := json.Marshal(tpl)
	require.Contains(t, string(tplBytes), "elasticmapreduce:AddJobFlowSteps")
	require.Contains(t, string(tplBytes), "elasticmapreduce:DescribeStep")
}

func TestRunCheckerReadOnlyDynamoDB(t *testing.T) {
	cfg := setupTestDirs(t)
	tmpl := synthTemplate(t, cfg)

	tmpl.HasResourceProperties(jsii.String("AWS::IAM::Policy"), map[string]interface{}{
		"PolicyDocument": assertions.Match_ObjectLike(&map[string]interface{}{
			"Statement": assertions.Match_ArrayWith(&[]interface{}{
				assertions.Match_ObjectLike(&map[string]interface{}{
					"Action": assertions.Match_ArrayWith(&[]interface{}{
						jsii.String("dynamodb:BatchGetItem"),
					}),
				}),
			}),
		}),
	})
}
