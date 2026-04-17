#!/usr/bin/env python3
"""Deploy Interlock infrastructure to LocalStack.

Mirrors the production Terraform stack (deploy/terraform/) using boto3. Resource
names, stream view types, batch sizes, and Lambda env vars match the production
shape so code behaves the same locally.

Skipped relative to production (Pro-only or not useful locally):
  - EventBridge Scheduler (Pro-only). Sla-monitor Lambdas get SKIP_SCHEDULER=true
    so their schedule-creation paths no-op.
  - CloudWatch alarms / alarm-driven EventBridge rules.
  - KMS-encrypted SQS queues (LocalStack community uses a stubbed KMS).

Usage:
    python deploy.py deploy     # create all resources (idempotent)
    python deploy.py teardown   # delete all resources
    python deploy.py smoke      # sanity-check what was deployed
"""
from __future__ import annotations

import io
import json
import os
import platform
import sys
import time
import zipfile
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ENVIRONMENT = os.environ.get("INTERLOCK_ENV", "local")
PREFIX = f"{ENVIRONMENT}-interlock"

REGION = "us-east-1"
ENDPOINT_URL = os.environ.get("AWS_ENDPOINT_URL", "http://localhost:4566")
# The endpoint Lambdas use when calling AWS services from *inside* the
# LocalStack container network. When LocalStack runs via the Makefile target
# with container name `interlock-localstack`, the Lambda Docker containers
# reach it as `http://localstack:4566` on the shared Docker network. Override
# with LAMBDA_AWS_ENDPOINT_URL if running a different topology.
LAMBDA_ENDPOINT_URL = os.environ.get(
    "LAMBDA_AWS_ENDPOINT_URL", "http://localstack:4566"
)

DIST_DIR = Path(__file__).resolve().parent / "dist"
STATE_MACHINE_FILE = Path(__file__).resolve().parent.parent / "statemachine.asl.json"

DUMMY_ACCOUNT_ID = "000000000000"
LAMBDA_ROLE_NAME = "lambda-role"
LAMBDA_ROLE_ARN = f"arn:aws:iam::{DUMMY_ACCOUNT_ID}:role/{LAMBDA_ROLE_NAME}"
SFN_ROLE_NAME = "sfn-role"
SFN_ROLE_ARN = f"arn:aws:iam::{DUMMY_ACCOUNT_ID}:role/{SFN_ROLE_NAME}"

LAMBDA_HANDLERS = [
    "stream-router",
    "orchestrator",
    "sla-monitor",
    "watchdog",
    "event-sink",
    "alert-dispatcher",
]

# Table names — match Terraform exactly
TABLE_CONTROL = f"{PREFIX}-control"
TABLE_JOBLOG = f"{PREFIX}-joblog"
TABLE_EVENTS = f"{PREFIX}-events"
TABLE_RERUN = f"{PREFIX}-rerun"

# EventBridge
EVENT_BUS_NAME = f"{PREFIX}-events"
RULE_ALL_EVENTS = f"{PREFIX}-events-to-sink"
RULE_ALERT_EVENTS = f"{PREFIX}-events-to-alerts"

# SQS
ALERT_QUEUE_NAME = f"{PREFIX}-alerts"

# Step Functions
STATE_MACHINE_NAME = f"{PREFIX}-pipeline"

# Alert-worthy detail-types — mirror production eventbridge.tf
ALERT_DETAIL_TYPES = [
    "SLA_WARNING",
    "SLA_BREACH",
    "JOB_FAILED",
    "VALIDATION_EXHAUSTED",
]

# ---------------------------------------------------------------------------
# boto3 clients
# ---------------------------------------------------------------------------


def _client(service: str):
    """Build a boto3 client targeted at LocalStack with dummy credentials."""
    return boto3.client(
        service,
        region_name=REGION,
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


def detect_lambda_architecture() -> str:
    """Return the Lambda architecture string matching the host machine.

    LocalStack's Lambda executor runs binaries inside Docker containers on the
    host kernel, so the bootstrap binary must match host arch.
    """
    machine = platform.machine().lower()
    if machine in ("arm64", "aarch64"):
        return "arm64"
    return "x86_64"


# ---------------------------------------------------------------------------
# IAM
# ---------------------------------------------------------------------------


def ensure_iam_roles() -> None:
    iam = _client("iam")

    lambda_trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    sfn_trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "states.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    for role_name, trust in ((LAMBDA_ROLE_NAME, lambda_trust), (SFN_ROLE_NAME, sfn_trust)):
        try:
            iam.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust),
                Description=f"LocalStack dummy role for {role_name}",
            )
            print(f"  [iam] created role {role_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "EntityAlreadyExists":
                print(f"  [iam] role {role_name} already exists")
            else:
                raise


def delete_iam_roles() -> None:
    iam = _client("iam")
    for role_name in (LAMBDA_ROLE_NAME, SFN_ROLE_NAME):
        try:
            iam.delete_role(RoleName=role_name)
            print(f"  [iam] deleted role {role_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] in ("NoSuchEntity", "ResourceNotFoundException"):
                pass
            else:
                raise


# ---------------------------------------------------------------------------
# DynamoDB
# ---------------------------------------------------------------------------


def _table_spec(name: str, with_events_gsi: bool = False) -> dict[str, Any]:
    attrs = [
        {"AttributeName": "PK", "AttributeType": "S"},
        {"AttributeName": "SK", "AttributeType": "S"},
    ]
    spec: dict[str, Any] = {
        "TableName": name,
        "BillingMode": "PAY_PER_REQUEST",
        "KeySchema": [
            {"AttributeName": "PK", "KeyType": "HASH"},
            {"AttributeName": "SK", "KeyType": "RANGE"},
        ],
        "AttributeDefinitions": attrs,
        "StreamSpecification": {
            "StreamEnabled": True,
            "StreamViewType": "NEW_IMAGE",
        },
    }
    if with_events_gsi:
        spec["AttributeDefinitions"] = attrs + [
            {"AttributeName": "eventType", "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "N"},
        ]
        spec["GlobalSecondaryIndexes"] = [
            {
                "IndexName": "GSI1",
                "KeySchema": [
                    {"AttributeName": "eventType", "KeyType": "HASH"},
                    {"AttributeName": "timestamp", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ]
        # Events table has no stream in production
        spec.pop("StreamSpecification")
    return spec


def ensure_tables() -> dict[str, str]:
    """Create the four DynamoDB tables. Returns stream_arn for streamed tables."""
    ddb = _client("dynamodb")
    stream_arns: dict[str, str] = {}

    specs = [
        (TABLE_CONTROL, False, True),
        (TABLE_JOBLOG, False, True),
        (TABLE_RERUN, False, False),
        (TABLE_EVENTS, True, False),
    ]

    for table_name, is_events, has_stream in specs:
        spec = _table_spec(table_name, with_events_gsi=is_events)
        try:
            ddb.create_table(**spec)
            print(f"  [ddb] created table {table_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceInUseException":
                print(f"  [ddb] table {table_name} already exists")
            else:
                raise

        # Wait for ACTIVE
        waiter = ddb.get_waiter("table_exists")
        waiter.wait(TableName=table_name, WaiterConfig={"Delay": 1, "MaxAttempts": 30})

        if has_stream:
            desc = ddb.describe_table(TableName=table_name)
            stream_arns[table_name] = desc["Table"]["LatestStreamArn"]

    return stream_arns


def delete_tables() -> None:
    ddb = _client("dynamodb")
    for table_name in (TABLE_CONTROL, TABLE_JOBLOG, TABLE_RERUN, TABLE_EVENTS):
        try:
            ddb.delete_table(TableName=table_name)
            print(f"  [ddb] deleted table {table_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                pass
            else:
                raise


# ---------------------------------------------------------------------------
# SQS
# ---------------------------------------------------------------------------


def ensure_alert_queue() -> tuple[str, str]:
    """Create the alert SQS queue. Returns (queue_url, queue_arn)."""
    sqs = _client("sqs")
    try:
        resp = sqs.create_queue(
            QueueName=ALERT_QUEUE_NAME,
            Attributes={
                "MessageRetentionPeriod": "86400",
                "VisibilityTimeout": "60",
            },
        )
        queue_url = resp["QueueUrl"]
        print(f"  [sqs] created queue {ALERT_QUEUE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] in (
            "QueueAlreadyExists",
            "ResourceAlreadyExistsException",
        ):
            queue_url = sqs.get_queue_url(QueueName=ALERT_QUEUE_NAME)["QueueUrl"]
            print(f"  [sqs] queue {ALERT_QUEUE_NAME} already exists")
        else:
            raise

    attrs = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
    queue_arn = attrs["Attributes"]["QueueArn"]
    return queue_url, queue_arn


def delete_alert_queue() -> None:
    sqs = _client("sqs")
    try:
        url = sqs.get_queue_url(QueueName=ALERT_QUEUE_NAME)["QueueUrl"]
        sqs.delete_queue(QueueUrl=url)
        print(f"  [sqs] deleted queue {ALERT_QUEUE_NAME}")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in (
            "AWS.SimpleQueueService.NonExistentQueue",
            "QueueDoesNotExist",
            "ResourceNotFoundException",
        ):
            return
        raise


# ---------------------------------------------------------------------------
# EventBridge
# ---------------------------------------------------------------------------


def ensure_event_bus() -> str:
    events = _client("events")
    try:
        resp = events.create_event_bus(Name=EVENT_BUS_NAME)
        print(f"  [events] created bus {EVENT_BUS_NAME}")
        return resp["EventBusArn"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            desc = events.describe_event_bus(Name=EVENT_BUS_NAME)
            print(f"  [events] bus {EVENT_BUS_NAME} already exists")
            return desc["Arn"]
        raise


def ensure_event_rule_to_sink(event_sink_arn: str) -> None:
    events = _client("events")
    pattern = {"source": ["interlock"]}
    events.put_rule(
        Name=RULE_ALL_EVENTS,
        EventBusName=EVENT_BUS_NAME,
        EventPattern=json.dumps(pattern),
        State="ENABLED",
        Description="Route all interlock events to the event-sink Lambda",
    )
    events.put_targets(
        Rule=RULE_ALL_EVENTS,
        EventBusName=EVENT_BUS_NAME,
        Targets=[{"Id": "event-sink-lambda", "Arn": event_sink_arn}],
    )
    print(f"  [events] rule {RULE_ALL_EVENTS} -> event-sink")


def ensure_event_rule_to_alerts(alert_queue_arn: str) -> None:
    events = _client("events")
    pattern = {
        "source": ["interlock"],
        "detail-type": ALERT_DETAIL_TYPES,
    }
    events.put_rule(
        Name=RULE_ALERT_EVENTS,
        EventBusName=EVENT_BUS_NAME,
        EventPattern=json.dumps(pattern),
        State="ENABLED",
        Description="Route alert-worthy interlock events to SQS",
    )
    events.put_targets(
        Rule=RULE_ALERT_EVENTS,
        EventBusName=EVENT_BUS_NAME,
        Targets=[{"Id": "alert-sqs", "Arn": alert_queue_arn}],
    )
    print(f"  [events] rule {RULE_ALERT_EVENTS} -> alert SQS")


RULE_WATCHDOG_SCHEDULE = f"{PREFIX}-watchdog-schedule"


def ensure_watchdog_schedule(watchdog_arn: str) -> None:
    """Create a 1-minute EventBridge schedule rule for the watchdog Lambda.

    This replaces the production Terraform ``aws_cloudwatch_event_rule`` for
    the watchdog. Running every minute in LocalStack ensures the watchdog
    fires during the E2E test window.
    """
    events = _client("events")
    events.put_rule(
        Name=RULE_WATCHDOG_SCHEDULE,
        ScheduleExpression="rate(1 minute)",
        State="ENABLED",
        Description="Invoke watchdog Lambda every minute for local E2E testing",
    )
    events.put_targets(
        Rule=RULE_WATCHDOG_SCHEDULE,
        Targets=[{"Id": "watchdog", "Arn": watchdog_arn}],
    )
    # Grant EventBridge permission to invoke the Lambda
    lam = _client("lambda")
    try:
        lam.add_permission(
            FunctionName=f"{PREFIX}-watchdog",
            StatementId="watchdog-schedule",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
        )
    except ClientError:
        pass  # Permission may already exist
    print(f"  [events] watchdog schedule rule (rate 1 min)")


def delete_event_resources() -> None:
    events = _client("events")
    # Clean up watchdog schedule (default bus, not custom)
    for rule_name_default_bus in (RULE_WATCHDOG_SCHEDULE,):
        try:
            targets = events.list_targets_by_rule(Rule=rule_name_default_bus)
            ids = [t["Id"] for t in targets.get("Targets", [])]
            if ids:
                events.remove_targets(Rule=rule_name_default_bus, Ids=ids)
            events.delete_rule(Name=rule_name_default_bus)
            print(f"  [events] deleted rule {rule_name_default_bus}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                pass
            else:
                raise

    for rule_name in (RULE_ALL_EVENTS, RULE_ALERT_EVENTS):
        try:
            targets = events.list_targets_by_rule(Rule=rule_name, EventBusName=EVENT_BUS_NAME)
            ids = [t["Id"] for t in targets.get("Targets", [])]
            if ids:
                events.remove_targets(Rule=rule_name, EventBusName=EVENT_BUS_NAME, Ids=ids)
            events.delete_rule(Name=rule_name, EventBusName=EVENT_BUS_NAME)
            print(f"  [events] deleted rule {rule_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                continue
            raise
    try:
        events.delete_event_bus(Name=EVENT_BUS_NAME)
        print(f"  [events] deleted bus {EVENT_BUS_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return
        raise


# ---------------------------------------------------------------------------
# Lambda
# ---------------------------------------------------------------------------


def _zip_bytes(zip_path: Path) -> bytes:
    return zip_path.read_bytes()


def _lambda_env(handler: str, sfn_arn: str | None) -> dict[str, str]:
    """Environment variables for a Lambda. Matches production Terraform, plus
    AWS_ENDPOINT_URL so the Go SDK calls the LocalStack container instead of
    real AWS.
    """
    base = {
        # CRITICAL: force the AWS SDK inside the Lambda to talk to LocalStack
        "AWS_ENDPOINT_URL": LAMBDA_ENDPOINT_URL,
        "AWS_REGION": REGION,
        "CONTROL_TABLE": TABLE_CONTROL,
        "JOBLOG_TABLE": TABLE_JOBLOG,
        "RERUN_TABLE": TABLE_RERUN,
        "EVENTS_TABLE": TABLE_EVENTS,
        "EVENT_BUS_NAME": EVENT_BUS_NAME,
        "EVENTS_TTL_DAYS": "30",
    }
    if sfn_arn:
        base["STATE_MACHINE_ARN"] = sfn_arn
    base["SFN_TIMEOUT_SECONDS"] = "3600"

    if handler == "orchestrator":
        # Allow HTTP triggers to reach Docker-internal addresses (mock-job-server).
        # Production never sets this; SSRF protection stays active in real AWS.
        base["DISABLE_SSRF_PROTECTION"] = "true"

    if handler == "sla-monitor":
        # EventBridge Scheduler is Pro-only; tell the Lambda to no-op
        # schedule-creation paths. Dummy ARNs satisfy the envcheck guard so
        # the Lambda can boot against LocalStack Community.
        base["SKIP_SCHEDULER"] = "true"
        base["SLA_MONITOR_ARN"] = (
            f"arn:aws:lambda:{REGION}:{DUMMY_ACCOUNT_ID}:function:{PREFIX}-sla-monitor"
        )
        base["SCHEDULER_ROLE_ARN"] = (
            f"arn:aws:iam::{DUMMY_ACCOUNT_ID}:role/dummy-scheduler-role"
        )
        base["SCHEDULER_GROUP_NAME"] = "default"
    if handler == "alert-dispatcher":
        # envcheck.go requires SLACK_CHANNEL_ID to be non-empty; use a dummy
        # value for LocalStack. Real deployments get this from Terraform vars.
        base["SLACK_CHANNEL_ID"] = "C000000LOCAL"
        base.setdefault("SLACK_BOT_TOKEN", "")
        base.setdefault("SLACK_SECRET_ARN", "")

    return base


def ensure_lambdas(sfn_arn: str | None) -> dict[str, str]:
    """Create or update all 6 Lambdas. Returns {handler: function_arn}."""
    lam = _client("lambda")
    arch = detect_lambda_architecture()
    arns: dict[str, str] = {}

    timeout_by_handler = {
        "stream-router": 60,
        "orchestrator": 120,
        "sla-monitor": 30,
        "watchdog": 60,
        "event-sink": 30,
        "alert-dispatcher": 30,
    }
    memory_by_handler = {
        "event-sink": 128,
        "alert-dispatcher": 128,
    }

    for handler in LAMBDA_HANDLERS:
        fn_name = f"{PREFIX}-{handler}"
        zip_path = DIST_DIR / f"{handler}.zip"
        if not zip_path.exists():
            raise FileNotFoundError(f"missing build artifact: {zip_path} — run build.sh first")
        code_bytes = _zip_bytes(zip_path)
        env = _lambda_env(handler, sfn_arn)

        try:
            resp = lam.create_function(
                FunctionName=fn_name,
                Runtime="provided.al2023",
                Role=LAMBDA_ROLE_ARN,
                Handler="bootstrap",
                Code={"ZipFile": code_bytes},
                Timeout=timeout_by_handler.get(handler, 60),
                MemorySize=memory_by_handler.get(handler, 256),
                Environment={"Variables": env},
                Architectures=[arch],
                Publish=False,
            )
            print(f"  [lambda] created {fn_name}")
            arns[handler] = resp["FunctionArn"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceConflictException":
                lam.update_function_code(FunctionName=fn_name, ZipFile=code_bytes)
                lam.update_function_configuration(
                    FunctionName=fn_name,
                    Environment={"Variables": env},
                    Timeout=timeout_by_handler.get(handler, 60),
                    MemorySize=memory_by_handler.get(handler, 256),
                )
                desc = lam.get_function(FunctionName=fn_name)
                arns[handler] = desc["Configuration"]["FunctionArn"]
                print(f"  [lambda] updated {fn_name}")
            else:
                raise

        # Wait until Lambda is ready before event source mapping attaches
        for _ in range(30):
            desc = lam.get_function(FunctionName=fn_name)
            state = desc["Configuration"].get("State", "Active")
            last = desc["Configuration"].get("LastUpdateStatus", "Successful")
            if state == "Active" and last == "Successful":
                break
            time.sleep(1)

    return arns


def delete_lambdas() -> None:
    lam = _client("lambda")
    for handler in LAMBDA_HANDLERS:
        fn_name = f"{PREFIX}-{handler}"
        try:
            lam.delete_function(FunctionName=fn_name)
            print(f"  [lambda] deleted {fn_name}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                continue
            raise


# ---------------------------------------------------------------------------
# Event source mappings
# ---------------------------------------------------------------------------


def _find_mapping(lam, source_arn: str, function_name: str) -> str | None:
    resp = lam.list_event_source_mappings(
        EventSourceArn=source_arn, FunctionName=function_name
    )
    items = resp.get("EventSourceMappings", [])
    return items[0]["UUID"] if items else None


def ensure_stream_mapping(stream_arn: str, function_name: str) -> None:
    lam = _client("lambda")
    existing = _find_mapping(lam, stream_arn, function_name)
    if existing:
        print(f"  [esm] mapping {stream_arn} -> {function_name} already exists")
        return
    lam.create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=function_name,
        StartingPosition="LATEST",
        BatchSize=10,
        FunctionResponseTypes=["ReportBatchItemFailures"],
    )
    print(f"  [esm] created mapping {stream_arn} -> {function_name}")


def ensure_sqs_mapping(queue_arn: str, function_name: str) -> None:
    lam = _client("lambda")
    existing = _find_mapping(lam, queue_arn, function_name)
    if existing:
        print(f"  [esm] mapping {queue_arn} -> {function_name} already exists")
        return
    lam.create_event_source_mapping(
        EventSourceArn=queue_arn,
        FunctionName=function_name,
        BatchSize=1,
        FunctionResponseTypes=["ReportBatchItemFailures"],
    )
    print(f"  [esm] created mapping {queue_arn} -> {function_name}")


def delete_all_mappings() -> None:
    lam = _client("lambda")
    try:
        paginator = lam.get_paginator("list_event_source_mappings")
        pages = paginator.paginate()
    except ClientError:
        return
    try:
        for page in pages:
            for m in page.get("EventSourceMappings", []):
                fn = m.get("FunctionArn", "")
                if PREFIX in fn:
                    try:
                        lam.delete_event_source_mapping(UUID=m["UUID"])
                        print(f"  [esm] deleted {m['UUID']}")
                    except ClientError:
                        pass
    except ClientError:
        return


# ---------------------------------------------------------------------------
# Step Functions
# ---------------------------------------------------------------------------


def _render_state_machine(orch_arn: str, sla_arn: str) -> str:
    tmpl = STATE_MACHINE_FILE.read_text()
    replacements = {
        "${orchestrator_arn}": orch_arn,
        "${sla_monitor_arn}": sla_arn,
        "${sfn_timeout_seconds}": "3600",
        "${trigger_max_attempts}": "3",
    }
    for key, value in replacements.items():
        tmpl = tmpl.replace(key, value)
    return tmpl


def ensure_state_machine(orch_arn: str, sla_arn: str) -> str:
    sfn = _client("stepfunctions")
    definition = _render_state_machine(orch_arn, sla_arn)
    # Validate JSON early
    json.loads(definition)

    arn = f"arn:aws:states:{REGION}:{DUMMY_ACCOUNT_ID}:stateMachine:{STATE_MACHINE_NAME}"
    try:
        sfn.describe_state_machine(stateMachineArn=arn)
        sfn.update_state_machine(
            stateMachineArn=arn,
            definition=definition,
            roleArn=SFN_ROLE_ARN,
        )
        print(f"  [sfn] updated {STATE_MACHINE_NAME}")
        return arn
    except ClientError as e:
        if e.response["Error"]["Code"] != "StateMachineDoesNotExist":
            raise
    resp = sfn.create_state_machine(
        name=STATE_MACHINE_NAME,
        definition=definition,
        roleArn=SFN_ROLE_ARN,
        type="STANDARD",
    )
    print(f"  [sfn] created {STATE_MACHINE_NAME}")
    return resp["stateMachineArn"]


def delete_state_machine() -> None:
    sfn = _client("stepfunctions")
    arn = f"arn:aws:states:{REGION}:{DUMMY_ACCOUNT_ID}:stateMachine:{STATE_MACHINE_NAME}"
    try:
        sfn.delete_state_machine(stateMachineArn=arn)
        print(f"  [sfn] deleted {STATE_MACHINE_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] in (
            "StateMachineDoesNotExist",
            "ResourceNotFoundException",
        ):
            return
        raise


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_deploy() -> None:
    print(f"==> Deploying Interlock to LocalStack at {ENDPOINT_URL}")
    print(f"==> Environment prefix: {PREFIX}")
    print(f"==> Lambda architecture: {detect_lambda_architecture()}")
    print(f"==> Lambda AWS_ENDPOINT_URL: {LAMBDA_ENDPOINT_URL}")

    print("--> IAM roles")
    ensure_iam_roles()

    print("--> DynamoDB tables")
    stream_arns = ensure_tables()

    print("--> SQS queue")
    alert_queue_url, alert_queue_arn = ensure_alert_queue()

    print("--> EventBridge bus")
    ensure_event_bus()

    # Lambdas need to be created before Step Functions (SFN references their
    # ARNs) but we also need SFN's ARN for the stream-router Lambda's
    # STATE_MACHINE_ARN env var. We break the cycle by creating Lambdas first
    # with a computed SFN ARN (deterministic from name).
    predicted_sfn_arn = (
        f"arn:aws:states:{REGION}:{DUMMY_ACCOUNT_ID}:stateMachine:{STATE_MACHINE_NAME}"
    )

    print("--> Lambda functions")
    lambda_arns = ensure_lambdas(sfn_arn=predicted_sfn_arn)

    print("--> Step Functions state machine")
    sfn_arn = ensure_state_machine(
        orch_arn=lambda_arns["orchestrator"],
        sla_arn=lambda_arns["sla-monitor"],
    )
    # In case the predicted ARN differs, update stream-router's env
    if sfn_arn != predicted_sfn_arn:
        lam = _client("lambda")
        env = _lambda_env("stream-router", sfn_arn)
        lam.update_function_configuration(
            FunctionName=f"{PREFIX}-stream-router",
            Environment={"Variables": env},
        )

    print("--> EventBridge rules")
    ensure_event_rule_to_sink(lambda_arns["event-sink"])
    ensure_event_rule_to_alerts(alert_queue_arn)
    ensure_watchdog_schedule(lambda_arns["watchdog"])

    print("--> Event source mappings")
    ensure_stream_mapping(stream_arns[TABLE_CONTROL], f"{PREFIX}-stream-router")
    ensure_stream_mapping(stream_arns[TABLE_JOBLOG], f"{PREFIX}-stream-router")
    ensure_sqs_mapping(alert_queue_arn, f"{PREFIX}-alert-dispatcher")

    print("==> Deploy complete")


def cmd_teardown() -> None:
    print(f"==> Tearing down Interlock stack at {ENDPOINT_URL}")
    delete_all_mappings()
    delete_state_machine()
    delete_event_resources()
    delete_lambdas()
    delete_alert_queue()
    delete_tables()
    delete_iam_roles()
    print("==> Teardown complete")


def cmd_smoke() -> None:
    print(f"==> Smoke test against {ENDPOINT_URL}")

    # 1. 6 Lambdas present
    lam = _client("lambda")
    functions = lam.list_functions().get("Functions", [])
    interlock_fns = sorted(f["FunctionName"] for f in functions if PREFIX in f["FunctionName"])
    print(f"  [lambda] found {len(interlock_fns)} interlock functions: {interlock_fns}")
    expected_fns = sorted(f"{PREFIX}-{h}" for h in LAMBDA_HANDLERS)
    if interlock_fns != expected_fns:
        print(f"  FAIL: expected {expected_fns}, got {interlock_fns}")
        sys.exit(1)

    # 2. 4 DynamoDB tables present
    ddb = _client("dynamodb")
    tables = ddb.list_tables().get("TableNames", [])
    interlock_tables = sorted(t for t in tables if PREFIX in t)
    expected_tables = sorted([TABLE_CONTROL, TABLE_JOBLOG, TABLE_EVENTS, TABLE_RERUN])
    print(f"  [ddb] found {len(interlock_tables)} tables: {interlock_tables}")
    if interlock_tables != expected_tables:
        print(f"  FAIL: expected {expected_tables}, got {interlock_tables}")
        sys.exit(1)

    # 3. A Lambda has AWS_ENDPOINT_URL set
    probe = f"{PREFIX}-stream-router"
    desc = lam.get_function(FunctionName=probe)
    env = desc["Configuration"].get("Environment", {}).get("Variables", {})
    if "AWS_ENDPOINT_URL" not in env:
        print(f"  FAIL: {probe} is missing AWS_ENDPOINT_URL env var")
        sys.exit(1)
    print(f"  [lambda] {probe} AWS_ENDPOINT_URL={env['AWS_ENDPOINT_URL']}")

    print("==> Smoke test PASSED")


def main() -> None:
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "deploy":
        cmd_deploy()
    elif cmd == "teardown":
        cmd_teardown()
    elif cmd == "smoke":
        cmd_smoke()
    else:
        print(f"unknown command: {cmd}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()
