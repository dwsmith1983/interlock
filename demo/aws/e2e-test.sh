#!/usr/bin/env bash
# Interlock AWS E2E Test Script
#
# Usage:
#   ./demo/aws/e2e-test.sh run       — deploy + run all scenarios + collect results
#   ./demo/aws/e2e-test.sh teardown  — destroy CDK stack + test-evaluator Lambda
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/e2e-results"
CDK_DIR="$REPO_ROOT/deploy/cdk"

# Defaults — override via environment
# E2E uses isolated names so it never touches a production stack
TABLE_NAME="${TABLE_NAME:-interlock-e2e}"
STACK_NAME="${STACK_NAME:-InterlockStack-e2e}"
AWS_REGION="${AWS_REGION:-ap-southeast-1}"
TEST_EVALUATOR_NAME="interlock-e2e-test-evaluator"
TEST_EVALUATOR_ROLE="interlock-e2e-test-evaluator-role"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

# Counters
PASS_COUNT=0
FAIL_COUNT=0

# ── Logging ──────────────────────────────────────────────────

log()  { echo -e "${CYAN}[interlock-e2e]${NC} $*"; }
ok()   { echo -e "${GREEN}  ✓${NC} $*"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { echo -e "${RED}  ✗${NC} $*"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
warn() { echo -e "${YELLOW}  ⚠${NC} $*"; }

# ── DynamoDB Helpers ─────────────────────────────────────────

put_item() {
    local item_json="$1"
    aws dynamodb put-item \
        --table-name "$TABLE_NAME" \
        --item "$item_json" \
        --region "$AWS_REGION" \
        --no-cli-pager
}

delete_item() {
    local pk="$1" sk="$2"
    aws dynamodb delete-item \
        --table-name "$TABLE_NAME" \
        --key "{\"PK\":{\"S\":\"$pk\"},\"SK\":{\"S\":\"$sk\"}}" \
        --region "$AWS_REGION" \
        --no-cli-pager
}

get_item() {
    local pk="$1" sk="$2"
    aws dynamodb get-item \
        --table-name "$TABLE_NAME" \
        --key "{\"PK\":{\"S\":\"$pk\"},\"SK\":{\"S\":\"$sk\"}}" \
        --region "$AWS_REGION" \
        --no-cli-pager 2>/dev/null || echo "{}"
}

seed_pipeline() {
    local name="$1" config_json="$2"
    put_item "{
        \"PK\": {\"S\": \"PIPELINE#$name\"},
        \"SK\": {\"S\": \"CONFIG\"},
        \"data\": {\"S\": $config_json}
    }"
    log "Seeded pipeline: $name"
}

write_marker() {
    local pipeline="$1" marker_id="$2"
    put_item "{
        \"PK\": {\"S\": \"PIPELINE#$pipeline\"},
        \"SK\": {\"S\": \"MARKER#$marker_id\"}
    }"
    log "Wrote marker: PIPELINE#$pipeline / MARKER#$marker_id"
}

seed_runlog() {
    local pipeline="$1" date="$2" schedule="$3" status="$4" run_id="$5"
    put_item "{
        \"PK\": {\"S\": \"PIPELINE#$pipeline\"},
        \"SK\": {\"S\": \"RUNLOG#$date#$schedule\"},
        \"status\": {\"S\": \"$status\"},
        \"runID\": {\"S\": \"$run_id\"},
        \"date\": {\"S\": \"$date\"},
        \"scheduleID\": {\"S\": \"$schedule\"}
    }"
    log "Seeded runlog: $pipeline / $date / $schedule → $status"
}

update_sensor() {
    local name="$1" data_json="$2"
    put_item "{
        \"PK\": {\"S\": \"SENSOR#$name\"},
        \"SK\": {\"S\": \"STATE\"},
        \"data\": $data_json
    }"
    log "Updated sensor: $name"
}

delete_runlog() {
    local pipeline="$1" date="$2" schedule="$3"
    delete_item "PIPELINE#$pipeline" "RUNLOG#$date#$schedule"
    log "Deleted runlog: $pipeline / $date / $schedule"
}

# ── Step Function Helpers ────────────────────────────────────

start_execution() {
    local exec_name="$1" input_json="$2"
    local result
    result=$(aws stepfunctions start-execution \
        --state-machine-arn "$STATE_MACHINE_ARN" \
        --name "$exec_name" \
        --input "$input_json" \
        --region "$AWS_REGION" \
        --no-cli-pager \
        --output json 2>&1) || true
    echo "$result"
}

wait_execution() {
    local exec_arn="$1"
    local timeout="${2:-120}"
    local elapsed=0

    while [ "$elapsed" -lt "$timeout" ]; do
        local status
        status=$(aws stepfunctions describe-execution \
            --execution-arn "$exec_arn" \
            --region "$AWS_REGION" \
            --no-cli-pager \
            --query "status" \
            --output text 2>/dev/null)

        if [ "$status" = "SUCCEEDED" ] || [ "$status" = "FAILED" ] || [ "$status" = "TIMED_OUT" ] || [ "$status" = "ABORTED" ]; then
            echo "$status"
            return 0
        fi

        sleep 3
        elapsed=$((elapsed + 3))
    done

    echo "TIMEOUT"
    return 1
}

get_execution_output() {
    local exec_arn="$1"
    aws stepfunctions describe-execution \
        --execution-arn "$exec_arn" \
        --region "$AWS_REGION" \
        --no-cli-pager \
        --output json 2>/dev/null
}

get_history() {
    local exec_arn="$1"
    aws stepfunctions get-execution-history \
        --execution-arn "$exec_arn" \
        --region "$AWS_REGION" \
        --no-cli-pager \
        --output json 2>/dev/null
}

verify_states() {
    local exec_arn="$1"
    shift
    local expected_states=("$@")
    local history
    history=$(get_history "$exec_arn")

    local all_found=true
    for state in "${expected_states[@]}"; do
        if echo "$history" | grep -q "\"name\":\"$state\""; then
            ok "State entered: $state"
        else
            fail "State NOT entered: $state"
            all_found=false
        fi
    done

    $all_found
}

# ── Result Collection ────────────────────────────────────────

log_result() {
    local scenario="$1" round="$2"
    shift 2
    local data="$*"
    local file="$RESULTS_DIR/${scenario}-${round}.json"
    echo "$data" | python3 -m json.tool > "$file" 2>/dev/null || echo "$data" > "$file"
    log "Saved result: $file"
}

print_summary() {
    echo ""
    echo "════════════════════════════════════════════════════════════"
    echo "  E2E Test Summary"
    echo "════════════════════════════════════════════════════════════"
    echo -e "  ${GREEN}Passed: $PASS_COUNT${NC}"
    echo -e "  ${RED}Failed: $FAIL_COUNT${NC}"
    echo "  Results: $RESULTS_DIR/"
    echo "════════════════════════════════════════════════════════════"

    if [ "$FAIL_COUNT" -gt 0 ]; then
        return 1
    fi
}

# ── Deploy ───────────────────────────────────────────────────

deploy_test_evaluator() {
    log "Building test-evaluator Lambda..."
    local build_dir="$SCRIPT_DIR/test-evaluator"
    local bootstrap="$build_dir/bootstrap"

    CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
        go build -tags lambda.norpc -ldflags="-s -w" \
        -o "$bootstrap" "$build_dir"

    log "Creating IAM role for test-evaluator..."
    local trust_policy='{
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }'

    local role_arn
    role_arn=$(aws iam get-role --role-name "$TEST_EVALUATOR_ROLE" \
        --query "Role.Arn" --output text --no-cli-pager 2>/dev/null) || true

    if [ -z "$role_arn" ] || [ "$role_arn" = "None" ]; then
        role_arn=$(aws iam create-role \
            --role-name "$TEST_EVALUATOR_ROLE" \
            --assume-role-policy-document "$trust_policy" \
            --query "Role.Arn" --output text --no-cli-pager)

        aws iam attach-role-policy \
            --role-name "$TEST_EVALUATOR_ROLE" \
            --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" \
            --no-cli-pager

        # DynamoDB read/write for sensor data
        aws iam put-role-policy \
            --role-name "$TEST_EVALUATOR_ROLE" \
            --policy-name "dynamodb-access" \
            --policy-document "{
                \"Version\": \"2012-10-17\",
                \"Statement\": [{
                    \"Effect\": \"Allow\",
                    \"Action\": [\"dynamodb:GetItem\", \"dynamodb:PutItem\"],
                    \"Resource\": \"arn:aws:dynamodb:$AWS_REGION:*:table/$TABLE_NAME\"
                }]
            }" \
            --no-cli-pager

        log "Waiting for IAM role propagation..."
        sleep 10
    fi
    log "IAM role: $role_arn"

    log "Creating test-evaluator Lambda..."
    local zip_file
    zip_file=$(mktemp /tmp/test-evaluator-XXXXXX.zip)
    (cd "$build_dir" && zip -j "$zip_file" bootstrap)

    # Delete existing function if present
    aws lambda delete-function \
        --function-name "$TEST_EVALUATOR_NAME" \
        --region "$AWS_REGION" \
        --no-cli-pager 2>/dev/null || true

    aws lambda create-function \
        --function-name "$TEST_EVALUATOR_NAME" \
        --runtime "provided.al2023" \
        --handler "bootstrap" \
        --role "$role_arn" \
        --zip-file "fileb://$zip_file" \
        --architectures arm64 \
        --memory-size 256 \
        --timeout 30 \
        --environment "Variables={TABLE_NAME=$TABLE_NAME}" \
        --region "$AWS_REGION" \
        --no-cli-pager >/dev/null

    rm -f "$zip_file" "$bootstrap"

    log "Waiting for Lambda to become active..."
    aws lambda wait function-active-v2 \
        --function-name "$TEST_EVALUATOR_NAME" \
        --region "$AWS_REGION" \
        --no-cli-pager

    log "Creating Function URL..."
    # Remove existing URL config if present
    aws lambda delete-function-url-config \
        --function-name "$TEST_EVALUATOR_NAME" \
        --region "$AWS_REGION" \
        --no-cli-pager 2>/dev/null || true

    aws lambda create-function-url-config \
        --function-name "$TEST_EVALUATOR_NAME" \
        --auth-type NONE \
        --region "$AWS_REGION" \
        --no-cli-pager >/dev/null

    # Allow public invocation via Function URL
    aws lambda add-permission \
        --function-name "$TEST_EVALUATOR_NAME" \
        --statement-id "FunctionURLAllowPublicAccess" \
        --action "lambda:InvokeFunctionUrl" \
        --principal "*" \
        --function-url-auth-type NONE \
        --region "$AWS_REGION" \
        --no-cli-pager >/dev/null 2>&1 || true

    EVALUATOR_BASE_URL=$(aws lambda get-function-url-config \
        --function-name "$TEST_EVALUATOR_NAME" \
        --region "$AWS_REGION" \
        --query "FunctionUrl" \
        --output text --no-cli-pager)

    # Strip trailing slash
    EVALUATOR_BASE_URL="${EVALUATOR_BASE_URL%/}"
    log "Evaluator URL: $EVALUATOR_BASE_URL"
}

deploy_cdk_stack() {
    log "Building Lambda handlers..."
    make -C "$REPO_ROOT" build-lambda

    log "Deploying CDK stack: $STACK_NAME (table: $TABLE_NAME)..."
    export INTERLOCK_DESTROY_ON_DELETE=true
    export INTERLOCK_TABLE_NAME="$TABLE_NAME"
    export INTERLOCK_STACK_NAME="$STACK_NAME"
    export EVALUATOR_BASE_URL

    (cd "$CDK_DIR" && cdk deploy "$STACK_NAME" --require-approval never --outputs-file "$RESULTS_DIR/stack-outputs.json")

    # Parse stack outputs (key is the stack name)
    TABLE_NAME=$(jq -r ".[\"$STACK_NAME\"].TableName" "$RESULTS_DIR/stack-outputs.json")
    STATE_MACHINE_ARN=$(jq -r ".[\"$STACK_NAME\"].StateMachineArn" "$RESULTS_DIR/stack-outputs.json")

    log "Table: $TABLE_NAME"
    log "State Machine: $STATE_MACHINE_ARN"

    # Update test-evaluator with actual table name
    aws lambda update-function-configuration \
        --function-name "$TEST_EVALUATOR_NAME" \
        --environment "Variables={TABLE_NAME=$TABLE_NAME}" \
        --region "$AWS_REGION" \
        --no-cli-pager >/dev/null

    aws lambda wait function-updated-v2 \
        --function-name "$TEST_EVALUATOR_NAME" \
        --region "$AWS_REGION" \
        --no-cli-pager
}

# ── Scenarios ────────────────────────────────────────────────

TODAY=$(date -u +%Y-%m-%d)
TODAY_WEEKDAY=$(date -u +%A)

seed_initial_data() {
    log "Seeding initial sensor data..."
    update_sensor "freshness" '{"M":{"lag":{"N":"300"}}}'
    update_sensor "record-count" '{"M":{"count":{"N":"500"}}}'
    update_sensor "upstream-check" '{"M":{"complete":{"BOOL":false}}}'

    log "Seeding e2e-progressive pipeline..."
    local pipeline_config
    pipeline_config=$(cat <<'PIPELINEJSON'
{
    "name": "e2e-progressive",
    "archetype": "batch-ingestion",
    "traits": {
        "source-freshness": {
            "evaluator": "freshness",
            "config": {"maxLagSeconds": 60},
            "timeout": 15,
            "ttl": 300
        },
        "upstream-dependency": {
            "evaluator": "upstream-check",
            "config": {"expectComplete": true},
            "timeout": 15,
            "ttl": 300
        },
        "resource-availability": {
            "evaluator": "record-count",
            "config": {"threshold": 1000},
            "timeout": 15,
            "ttl": 300
        }
    },
    "trigger": {
        "type": "http",
        "method": "POST",
        "url": "EVALUATOR_BASE_URL_PLACEHOLDER/trigger-endpoint"
    }
}
PIPELINEJSON
)
    pipeline_config="${pipeline_config//EVALUATOR_BASE_URL_PLACEHOLDER/$EVALUATOR_BASE_URL}"
    seed_pipeline "e2e-progressive" "$(echo "$pipeline_config" | jq -c '.' | jq -Rs '.')"
}

run_scenario_1() {
    log ""
    log "━━━ Scenario 1: Progressive Readiness → Trigger → Completion ━━━"

    # Round 1: All traits fail
    log ""
    log "── Round 1: All traits fail ──"
    local exec_name="e2e-s1r1-${TODAY}-daily"
    local result
    result=$(start_execution "$exec_name" "{\"pipelineID\":\"e2e-progressive\",\"scheduleID\":\"daily\"}")

    local exec_arn
    exec_arn=$(echo "$result" | jq -r '.executionArn // empty')
    if [ -z "$exec_arn" ]; then
        fail "Scenario 1 Round 1: Failed to start execution"
        log_result "scenario-1" "round-1" "$result"
        return
    fi

    local status
    status=$(wait_execution "$exec_arn" 120)
    local output
    output=$(get_execution_output "$exec_arn")
    log_result "scenario-1" "round-1" "$output"

    if [ "$status" = "SUCCEEDED" ]; then
        ok "Scenario 1 Round 1: Execution succeeded (expected — not ready is not an error)"
    else
        fail "Scenario 1 Round 1: Execution status=$status (expected SUCCEEDED)"
    fi

    verify_states "$exec_arn" "CheckExclusion" "AcquireLock" "CheckRunLog" "ResolvePipeline" "EvaluateTraits" "CheckReadiness" || true

    # Round 2: Partial pass (freshness passes)
    log ""
    log "── Round 2: Partial pass (1/3 — freshness) ──"
    update_sensor "freshness" '{"M":{"lag":{"N":"30"}}}'
    delete_runlog "e2e-progressive" "$TODAY" "daily"
    sleep 2

    exec_name="e2e-s1r2-${TODAY}-daily"
    result=$(start_execution "$exec_name" "{\"pipelineID\":\"e2e-progressive\",\"scheduleID\":\"daily\"}")
    exec_arn=$(echo "$result" | jq -r '.executionArn // empty')
    if [ -z "$exec_arn" ]; then
        fail "Scenario 1 Round 2: Failed to start execution"
        log_result "scenario-1" "round-2" "$result"
        return
    fi

    status=$(wait_execution "$exec_arn" 120)
    output=$(get_execution_output "$exec_arn")
    log_result "scenario-1" "round-2" "$output"

    if [ "$status" = "SUCCEEDED" ]; then
        ok "Scenario 1 Round 2: Execution succeeded (partial readiness)"
    else
        fail "Scenario 1 Round 2: Execution status=$status (expected SUCCEEDED)"
    fi

    # Round 3: All pass → trigger → complete
    log ""
    log "── Round 3: All pass → trigger → complete ──"
    update_sensor "record-count" '{"M":{"count":{"N":"1500"}}}'
    update_sensor "upstream-check" '{"M":{"complete":{"BOOL":true}}}'
    delete_runlog "e2e-progressive" "$TODAY" "daily"
    sleep 2

    exec_name="e2e-s1r3-${TODAY}-daily"
    result=$(start_execution "$exec_name" "{\"pipelineID\":\"e2e-progressive\",\"scheduleID\":\"daily\"}")
    exec_arn=$(echo "$result" | jq -r '.executionArn // empty')
    if [ -z "$exec_arn" ]; then
        fail "Scenario 1 Round 3: Failed to start execution"
        log_result "scenario-1" "round-3" "$result"
        return
    fi

    status=$(wait_execution "$exec_arn" 120)
    output=$(get_execution_output "$exec_arn")
    log_result "scenario-1" "round-3" "$output"

    if [ "$status" = "SUCCEEDED" ]; then
        ok "Scenario 1 Round 3: Execution succeeded"
        # Check if trigger was invoked
        if echo "$output" | jq -e '.output' >/dev/null 2>&1; then
            local sfn_output
            sfn_output=$(echo "$output" | jq -r '.output')
            if echo "$sfn_output" | jq -e '.logCompleted' >/dev/null 2>&1; then
                ok "Scenario 1 Round 3: Pipeline triggered and completed"
            else
                warn "Scenario 1 Round 3: Could not confirm completion in output"
            fi
        fi
    else
        fail "Scenario 1 Round 3: Execution status=$status (expected SUCCEEDED)"
    fi

    verify_states "$exec_arn" "TriggerPipeline" "CheckCompletionSLA" "LogCompleted" "ReleaseLock" || true
}

run_scenario_2() {
    log ""
    log "━━━ Scenario 2: Re-run After Data Quality Drop ━━━"

    # Reset record-count sensor to simulate quality drop
    delete_runlog "e2e-progressive" "$TODAY" "daily"
    update_sensor "record-count" '{"M":{"count":{"N":"800"}}}'
    sleep 2

    log "── Round 1: Data quality dropped — count below threshold ──"
    local exec_name="e2e-s2r1-${TODAY}-daily"
    local result
    result=$(start_execution "$exec_name" "{\"pipelineID\":\"e2e-progressive\",\"scheduleID\":\"daily\"}")
    local exec_arn
    exec_arn=$(echo "$result" | jq -r '.executionArn // empty')
    if [ -z "$exec_arn" ]; then
        fail "Scenario 2 Round 1: Failed to start execution"
        log_result "scenario-2" "round-1" "$result"
        return
    fi

    local status
    status=$(wait_execution "$exec_arn" 120)
    local output
    output=$(get_execution_output "$exec_arn")
    log_result "scenario-2" "round-1" "$output"

    if [ "$status" = "SUCCEEDED" ]; then
        ok "Scenario 2 Round 1: Execution succeeded (not ready due to quality drop)"
    else
        fail "Scenario 2 Round 1: Execution status=$status"
    fi

    # Restore quality and re-run
    log "── Round 2: Quality restored → trigger → complete ──"
    update_sensor "record-count" '{"M":{"count":{"N":"1200"}}}'
    delete_runlog "e2e-progressive" "$TODAY" "daily"
    sleep 2

    exec_name="e2e-s2r2-${TODAY}-daily"
    result=$(start_execution "$exec_name" "{\"pipelineID\":\"e2e-progressive\",\"scheduleID\":\"daily\"}")
    exec_arn=$(echo "$result" | jq -r '.executionArn // empty')
    if [ -z "$exec_arn" ]; then
        fail "Scenario 2 Round 2: Failed to start execution"
        log_result "scenario-2" "round-2" "$result"
        return
    fi

    status=$(wait_execution "$exec_arn" 120)
    output=$(get_execution_output "$exec_arn")
    log_result "scenario-2" "round-2" "$output"

    if [ "$status" = "SUCCEEDED" ]; then
        ok "Scenario 2 Round 2: Recovery execution succeeded"
    else
        fail "Scenario 2 Round 2: Execution status=$status"
    fi

    verify_states "$exec_arn" "TriggerPipeline" "LogCompleted" "ReleaseLock" || true
}

run_scenario_3() {
    log ""
    log "━━━ Scenario 3: Already Completed (Dedup) ━━━"

    # Ensure a COMPLETED runlog exists from Scenario 2
    seed_runlog "e2e-progressive" "$TODAY" "daily" "COMPLETED" "e2e-dedup-run"

    local exec_name="e2e-s3-${TODAY}-daily"
    local result
    result=$(start_execution "$exec_name" "{\"pipelineID\":\"e2e-progressive\",\"scheduleID\":\"daily\"}")
    local exec_arn
    exec_arn=$(echo "$result" | jq -r '.executionArn // empty')
    if [ -z "$exec_arn" ]; then
        fail "Scenario 3: Failed to start execution"
        log_result "scenario-3" "result" "$result"
        return
    fi

    local status
    status=$(wait_execution "$exec_arn" 60)
    local output
    output=$(get_execution_output "$exec_arn")
    log_result "scenario-3" "result" "$output"

    if [ "$status" = "SUCCEEDED" ]; then
        ok "Scenario 3: Execution succeeded (dedup — skipped)"
    else
        fail "Scenario 3: Execution status=$status"
    fi

    # Should have gone through CheckRunLog → ShouldProceed → ReleaseLock → End
    verify_states "$exec_arn" "CheckExclusion" "AcquireLock" "CheckRunLog" "ReleaseLock" || true
}

run_scenario_4() {
    log ""
    log "━━━ Scenario 4: Excluded Day ━━━"

    # Seed a pipeline with today's weekday excluded
    local pipeline_config
    pipeline_config=$(cat <<EXJSON
{
    "name": "e2e-excluded",
    "archetype": "batch-ingestion",
    "exclusions": {
        "days": ["$TODAY_WEEKDAY"]
    },
    "traits": {
        "source-freshness": {
            "evaluator": "freshness",
            "config": {"maxLagSeconds": 60},
            "timeout": 15,
            "ttl": 300
        }
    },
    "trigger": {
        "type": "http",
        "method": "POST",
        "url": "$EVALUATOR_BASE_URL/trigger-endpoint"
    }
}
EXJSON
)
    seed_pipeline "e2e-excluded" "$(echo "$pipeline_config" | jq -c '.' | jq -Rs '.')"

    local exec_name="e2e-s4-${TODAY}-daily"
    local result
    result=$(start_execution "$exec_name" "{\"pipelineID\":\"e2e-excluded\",\"scheduleID\":\"daily\"}")
    local exec_arn
    exec_arn=$(echo "$result" | jq -r '.executionArn // empty')
    if [ -z "$exec_arn" ]; then
        fail "Scenario 4: Failed to start execution"
        log_result "scenario-4" "result" "$result"
        return
    fi

    local status
    status=$(wait_execution "$exec_arn" 60)
    local output
    output=$(get_execution_output "$exec_arn")
    log_result "scenario-4" "result" "$output"

    if [ "$status" = "SUCCEEDED" ]; then
        ok "Scenario 4: Execution succeeded (excluded day → skipped)"
    else
        fail "Scenario 4: Execution status=$status"
    fi

    verify_states "$exec_arn" "CheckExclusion" || true
}

run_scenario_5() {
    log ""
    log "━━━ Scenario 5: Pipeline Not Found ━━━"

    local exec_name="e2e-s5-${TODAY}-daily"
    local result
    result=$(start_execution "$exec_name" "{\"pipelineID\":\"e2e-nonexistent\",\"scheduleID\":\"daily\"}")
    local exec_arn
    exec_arn=$(echo "$result" | jq -r '.executionArn // empty')
    if [ -z "$exec_arn" ]; then
        fail "Scenario 5: Failed to start execution"
        log_result "scenario-5" "result" "$result"
        return
    fi

    local status
    status=$(wait_execution "$exec_arn" 60)
    local output
    output=$(get_execution_output "$exec_arn")
    log_result "scenario-5" "result" "$output"

    # Pipeline not found may result in SUCCEEDED (checkExclusion returns skip/error → End)
    # or FAILED depending on error handling
    if [ "$status" = "SUCCEEDED" ] || [ "$status" = "FAILED" ]; then
        ok "Scenario 5: Execution completed with status=$status (graceful handling)"
    else
        fail "Scenario 5: Unexpected status=$status"
    fi

    verify_states "$exec_arn" "CheckExclusion" || true
}

run_scenario_6() {
    log ""
    log "━━━ Scenario 6: Stream-Router Integration ━━━"

    # Seed a simple pipeline for stream testing
    local pipeline_config
    pipeline_config=$(cat <<STREAMJSON
{
    "name": "e2e-stream-test",
    "archetype": "batch-ingestion",
    "traits": {
        "source-freshness": {
            "evaluator": "freshness",
            "config": {"maxLagSeconds": 9999},
            "timeout": 15,
            "ttl": 300
        }
    },
    "trigger": {
        "type": "http",
        "method": "POST",
        "url": "$EVALUATOR_BASE_URL/trigger-endpoint"
    }
}
STREAMJSON
)
    seed_pipeline "e2e-stream-test" "$(echo "$pipeline_config" | jq -c '.' | jq -Rs '.')"

    log "Writing MARKER# record to trigger stream-router..."
    write_marker "e2e-stream-test" "freshness#$(date -u +%Y-%m-%dT%H:%M:%SZ)"

    log "Waiting for DynamoDB Stream → stream-router → SFN execution (up to 30s)..."
    local found=false
    local elapsed=0
    local exec_arn=""

    while [ "$elapsed" -lt 30 ]; do
        sleep 5
        elapsed=$((elapsed + 5))

        local executions
        executions=$(aws stepfunctions list-executions \
            --state-machine-arn "$STATE_MACHINE_ARN" \
            --max-results 10 \
            --region "$AWS_REGION" \
            --no-cli-pager \
            --output json 2>/dev/null)

        # Look for an execution matching our stream-test pipeline
        exec_arn=$(echo "$executions" | jq -r '.executions[] | select(.name | contains("e2e-stream-test")) | .executionArn' | head -1)
        if [ -n "$exec_arn" ]; then
            found=true
            break
        fi
    done

    if $found; then
        ok "Scenario 6: Stream-router started SFN execution"
        log "  Execution: $exec_arn"

        local status
        status=$(wait_execution "$exec_arn" 120)
        local output
        output=$(get_execution_output "$exec_arn")
        log_result "scenario-6" "result" "$output"

        if [ "$status" = "SUCCEEDED" ] || [ "$status" = "FAILED" ]; then
            ok "Scenario 6: Stream-triggered execution completed (status=$status)"
        else
            warn "Scenario 6: Execution status=$status"
        fi
    else
        fail "Scenario 6: No SFN execution found for e2e-stream-test after 30s"
        log_result "scenario-6" "result" '{"error": "no execution found"}'
    fi
}

# ── Teardown ─────────────────────────────────────────────────

do_teardown() {
    log "Starting teardown..."

    log "Destroying CDK stack: $STACK_NAME..."
    (cd "$CDK_DIR" && INTERLOCK_DESTROY_ON_DELETE=true INTERLOCK_TABLE_NAME="$TABLE_NAME" INTERLOCK_STACK_NAME="$STACK_NAME" cdk destroy "$STACK_NAME" --force) || warn "CDK destroy failed (stack may not exist)"

    log "Deleting test-evaluator Lambda..."
    aws lambda delete-function-url-config \
        --function-name "$TEST_EVALUATOR_NAME" \
        --region "$AWS_REGION" \
        --no-cli-pager 2>/dev/null || true
    aws lambda delete-function \
        --function-name "$TEST_EVALUATOR_NAME" \
        --region "$AWS_REGION" \
        --no-cli-pager 2>/dev/null || true

    log "Cleaning up IAM role..."
    aws iam delete-role-policy \
        --role-name "$TEST_EVALUATOR_ROLE" \
        --policy-name "dynamodb-access" \
        --no-cli-pager 2>/dev/null || true
    aws iam detach-role-policy \
        --role-name "$TEST_EVALUATOR_ROLE" \
        --policy-arn "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole" \
        --no-cli-pager 2>/dev/null || true
    aws iam delete-role \
        --role-name "$TEST_EVALUATOR_ROLE" \
        --no-cli-pager 2>/dev/null || true

    log "Cleaning up results..."
    rm -rf "$RESULTS_DIR"

    log "Teardown complete."
}

# ── Main ─────────────────────────────────────────────────────

do_run() {
    mkdir -p "$RESULTS_DIR"

    log "Interlock AWS E2E Test Suite"
    log "Date: $TODAY ($TODAY_WEEKDAY)"
    log ""

    # Deploy
    deploy_test_evaluator
    deploy_cdk_stack

    # Seed
    seed_initial_data

    # Run scenarios
    run_scenario_1
    run_scenario_2
    run_scenario_3
    run_scenario_4
    run_scenario_5
    run_scenario_6

    print_summary
}

case "${1:-}" in
    run)
        do_run
        ;;
    teardown)
        do_teardown
        ;;
    *)
        echo "Usage: $0 {run|teardown}"
        echo ""
        echo "  run       Deploy everything, run all E2E scenarios, collect results"
        echo "  teardown  Destroy CDK stack and test-evaluator Lambda"
        exit 1
        ;;
esac
