package main

import (
	"os"
	"path/filepath"

	"github.com/aws/aws-cdk-go/awscdk/v2"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsdynamodb"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsiam"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambda"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslambdaeventsources"
	"github.com/aws/aws-cdk-go/awscdk/v2/awslogs"
	"github.com/aws/aws-cdk-go/awscdk/v2/awssns"
	"github.com/aws/aws-cdk-go/awscdk/v2/awsstepfunctions"
	"github.com/aws/constructs-go/constructs/v10"
	"github.com/aws/jsii-runtime-go"
)

func NewInterlockStack(scope constructs.Construct, id string, cfg StackConfig) awscdk.Stack {
	stack := awscdk.NewStack(scope, &id, nil)

	// 3a. DynamoDB Table
	table := awsdynamodb.NewTableV2(stack, jsii.String("Table"), &awsdynamodb.TablePropsV2{
		TableName: jsii.String(cfg.TableName),
		PartitionKey: &awsdynamodb.Attribute{
			Name: jsii.String("PK"),
			Type: awsdynamodb.AttributeType_STRING,
		},
		SortKey: &awsdynamodb.Attribute{
			Name: jsii.String("SK"),
			Type: awsdynamodb.AttributeType_STRING,
		},
		Billing:             awsdynamodb.Billing_OnDemand(nil),
		DynamoStream:        awsdynamodb.StreamViewType_NEW_IMAGE,
		TimeToLiveAttribute: jsii.String("ttl"),
		RemovalPolicy:       removalPolicy(cfg.DestroyOnDelete),
		GlobalSecondaryIndexes: &[]*awsdynamodb.GlobalSecondaryIndexPropsV2{
			{
				IndexName: jsii.String("GSI1"),
				PartitionKey: &awsdynamodb.Attribute{
					Name: jsii.String("GSI1PK"),
					Type: awsdynamodb.AttributeType_STRING,
				},
				SortKey: &awsdynamodb.Attribute{
					Name: jsii.String("GSI1SK"),
					Type: awsdynamodb.AttributeType_STRING,
				},
			},
		},
	})

	// 3b. SNS Topic
	topic := awssns.NewTopic(stack, jsii.String("AlertTopic"), &awssns.TopicProps{
		TopicName: jsii.String(cfg.TableName + "-alerts"),
	})

	// 3c. Lambda Layer (archetype YAML files)
	layer := awslambda.NewLayerVersion(stack, jsii.String("ArchetypeLayer"), &awslambda.LayerVersionProps{
		Code:                    awslambda.Code_FromAsset(jsii.String(cfg.LayerDistDir), nil),
		CompatibleRuntimes:      &[]awslambda.Runtime{awslambda.Runtime_PROVIDED_AL2023()},
		CompatibleArchitectures: &[]awslambda.Architecture{awslambda.Architecture_ARM_64()},
		Description:             jsii.String("Interlock archetype definitions"),
	})

	// Common Lambda props
	commonEnv := &map[string]*string{
		"TABLE_NAME":    table.TableName(),
		"SNS_TOPIC_ARN": topic.TopicArn(),
		"ARCHETYPE_DIR": jsii.String("/opt/archetypes"),
		"READINESS_TTL": jsii.String(cfg.ReadinessTTL),
		"RETENTION_TTL": jsii.String(cfg.RetentionTTL),
	}
	if cfg.EvaluatorBaseURL != "" {
		(*commonEnv)["EVALUATOR_BASE_URL"] = jsii.String(cfg.EvaluatorBaseURL)
	}

	timeout := awscdk.Duration_Seconds(jsii.Number(cfg.Timeout))
	memorySize := jsii.Number(cfg.MemorySize)
	logRetention := logRetentionDays(cfg.LogRetentionDays)

	makeFn := func(name, codePath string, env *map[string]*string) awslambda.Function {
		return awslambda.NewFunction(stack, jsii.String(name), &awslambda.FunctionProps{
			FunctionName: jsii.String(cfg.TableName + "-" + name),
			Runtime:      awslambda.Runtime_PROVIDED_AL2023(),
			Handler:      jsii.String("bootstrap"),
			Code:         awslambda.Code_FromAsset(jsii.String(codePath), nil),
			Architecture: awslambda.Architecture_ARM_64(),
			MemorySize:   memorySize,
			Timeout:      timeout,
			Environment:  env,
			Layers:       &[]awslambda.ILayerVersion{layer},
			LogRetention: logRetention,
		})
	}

	// 3d. Lambda Functions
	streamRouterFn := awslambda.NewFunction(stack, jsii.String("stream-router"), &awslambda.FunctionProps{
		FunctionName: jsii.String(cfg.TableName + "-stream-router"),
		Runtime:      awslambda.Runtime_PROVIDED_AL2023(),
		Handler:      jsii.String("bootstrap"),
		Code:         awslambda.Code_FromAsset(jsii.String(filepath.Join(cfg.LambdaDistDir, "stream-router")), nil),
		Architecture: awslambda.Architecture_ARM_64(),
		MemorySize:   memorySize,
		Timeout:      timeout,
		Environment:  &map[string]*string{
			// STATE_MACHINE_ARN set after SFN creation
		},
		LogRetention: logRetention,
	})

	orchestratorFn := makeFn("orchestrator", filepath.Join(cfg.LambdaDistDir, "orchestrator"), commonEnv)
	evaluatorFn := makeFn("evaluator", filepath.Join(cfg.LambdaDistDir, "evaluator"), commonEnv)
	triggerFn := makeFn("trigger", filepath.Join(cfg.LambdaDistDir, "trigger"), commonEnv)
	runCheckerFn := makeFn("run-checker", filepath.Join(cfg.LambdaDistDir, "run-checker"), commonEnv)

	// 3e. IAM Grants

	// DynamoDB: read/write for orchestrator, evaluator, trigger; read-only for run-checker
	table.GrantReadWriteData(orchestratorFn)
	table.GrantReadWriteData(evaluatorFn)
	table.GrantReadWriteData(triggerFn)
	table.GrantReadData(runCheckerFn)

	// SNS: publish for orchestrator and trigger
	topic.GrantPublish(orchestratorFn)
	topic.GrantPublish(triggerFn)

	// Opt-in trigger permissions
	addTriggerPermissions(triggerFn, runCheckerFn, cfg)

	// 3f. Step Function
	aslJSON := loadASL(cfg.ASLPath)
	sfnRole := awsiam.NewRole(stack, jsii.String("SfnRole"), &awsiam.RoleProps{
		AssumedBy: awsiam.NewServicePrincipal(jsii.String("states.amazonaws.com"), nil),
	})
	sfnRole.AddToPolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Actions: &[]*string{jsii.String("lambda:InvokeFunction")},
		Resources: &[]*string{
			orchestratorFn.FunctionArn(),
			evaluatorFn.FunctionArn(),
			triggerFn.FunctionArn(),
			runCheckerFn.FunctionArn(),
		},
	}))

	sfnMachine := awsstepfunctions.NewCfnStateMachine(stack, jsii.String("StateMachine"), &awsstepfunctions.CfnStateMachineProps{
		StateMachineName: jsii.String(cfg.TableName + "-pipeline"),
		StateMachineType: jsii.String("STANDARD"),
		RoleArn:          sfnRole.RoleArn(),
		DefinitionString: jsii.String(aslJSON),
		DefinitionSubstitutions: map[string]*string{
			"OrchestratorFunctionArn": orchestratorFn.FunctionArn(),
			"EvaluatorFunctionArn":    evaluatorFn.FunctionArn(),
			"TriggerFunctionArn":      triggerFn.FunctionArn(),
			"RunCheckerFunctionArn":   runCheckerFn.FunctionArn(),
		},
	})

	// Grant stream-router permission to start executions on this SFN
	streamRouterFn.AddEnvironment(jsii.String("STATE_MACHINE_ARN"), sfnMachine.AttrArn(), nil)
	streamRouterFn.AddToRolePolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
		Actions:   &[]*string{jsii.String("states:StartExecution")},
		Resources: &[]*string{sfnMachine.AttrArn()},
	}))

	// 3g. Event Source Mapping: DynamoDB Stream → stream-router
	streamRouterFn.AddEventSource(awslambdaeventsources.NewDynamoEventSource(table, &awslambdaeventsources.DynamoEventSourceProps{
		StartingPosition: awslambda.StartingPosition_LATEST,
		BatchSize:        jsii.Number(10),
	}))

	// 3h. Stack Outputs
	awscdk.NewCfnOutput(stack, jsii.String("TableName"), &awscdk.CfnOutputProps{
		Value: table.TableName(),
	})
	awscdk.NewCfnOutput(stack, jsii.String("TopicArn"), &awscdk.CfnOutputProps{
		Value: topic.TopicArn(),
	})
	awscdk.NewCfnOutput(stack, jsii.String("StateMachineArn"), &awscdk.CfnOutputProps{
		Value: sfnMachine.AttrArn(),
	})

	return stack
}

func addTriggerPermissions(triggerFn, runCheckerFn awslambda.Function, cfg StackConfig) {
	allResources := &[]*string{jsii.String("*")}

	if cfg.EnableGlueTrigger {
		triggerFn.AddToRolePolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
			Actions:   &[]*string{jsii.String("glue:StartJobRun"), jsii.String("glue:GetJobRun")},
			Resources: allResources,
		}))
		runCheckerFn.AddToRolePolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
			Actions:   &[]*string{jsii.String("glue:GetJobRun")},
			Resources: allResources,
		}))
	}

	if cfg.EnableEMRTrigger {
		triggerFn.AddToRolePolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
			Actions:   &[]*string{jsii.String("elasticmapreduce:AddJobFlowSteps"), jsii.String("elasticmapreduce:DescribeStep")},
			Resources: allResources,
		}))
		runCheckerFn.AddToRolePolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
			Actions:   &[]*string{jsii.String("elasticmapreduce:DescribeStep")},
			Resources: allResources,
		}))
	}

	if cfg.EnableEMRServerlessTrigger {
		triggerFn.AddToRolePolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
			Actions:   &[]*string{jsii.String("emr-serverless:StartJobRun"), jsii.String("emr-serverless:GetJobRun")},
			Resources: allResources,
		}))
		runCheckerFn.AddToRolePolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
			Actions:   &[]*string{jsii.String("emr-serverless:GetJobRun")},
			Resources: allResources,
		}))
	}

	if cfg.EnableStepFunctionTrigger {
		triggerFn.AddToRolePolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
			Actions:   &[]*string{jsii.String("states:StartExecution"), jsii.String("states:DescribeExecution")},
			Resources: allResources,
		}))
		runCheckerFn.AddToRolePolicy(awsiam.NewPolicyStatement(&awsiam.PolicyStatementProps{
			Actions:   &[]*string{jsii.String("states:DescribeExecution")},
			Resources: allResources,
		}))
	}
}

func loadASL(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		panic("failed to read ASL file: " + err.Error())
	}
	return string(data)
}

func removalPolicy(destroy bool) awscdk.RemovalPolicy {
	if destroy {
		return awscdk.RemovalPolicy_DESTROY
	}
	return awscdk.RemovalPolicy_RETAIN
}

func logRetentionDays(days float64) awslogs.RetentionDays {
	switch days {
	case 1:
		return awslogs.RetentionDays_ONE_DAY
	case 3:
		return awslogs.RetentionDays_THREE_DAYS
	case 5:
		return awslogs.RetentionDays_FIVE_DAYS
	case 7:
		return awslogs.RetentionDays_ONE_WEEK
	case 14:
		return awslogs.RetentionDays_TWO_WEEKS
	case 30:
		return awslogs.RetentionDays_ONE_MONTH
	case 60:
		return awslogs.RetentionDays_TWO_MONTHS
	case 90:
		return awslogs.RetentionDays_THREE_MONTHS
	case 365:
		return awslogs.RetentionDays_ONE_YEAR
	default:
		return awslogs.RetentionDays_ONE_WEEK
	}
}
