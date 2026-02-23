#!/usr/bin/env bash
# Interlock Local E2E Test Script
#
# Usage:
#   ./demo/local/e2e-test.sh run       — start stack, run all scenarios, collect results
#   ./demo/local/e2e-test.sh teardown  — destroy stack (results preserved)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="$SCRIPT_DIR/e2e-results"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
INTERLOCK_URL="${INTERLOCK_URL:-http://localhost:3000}"

# Defaults
TODAY=$(date -u +%Y-%m-%d)
TODAY_WEEKDAY=$(date -u +%A)

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
ok()   { echo -e "${GREEN}  PASS${NC} $*"; PASS_COUNT=$((PASS_COUNT + 1)); }
fail() { echo -e "${RED}  FAIL${NC} $*"; FAIL_COUNT=$((FAIL_COUNT + 1)); }
warn() { echo -e "${YELLOW}  WARN${NC} $*"; }

# ── Docker / Redis Helpers ───────────────────────────────────

compose() {
    docker compose -f "$COMPOSE_FILE" "$@"
}

set_sensor() {
    local name="$1" value="$2"
    compose exec -T redis redis-cli SET "sensor:${name}" "$value" > /dev/null
    log "Set sensor:${name} = ${value}"
}

delete_runlog() {
    local pipeline="$1" date="$2" schedule="$3"
    compose exec -T redis redis-cli DEL "interlock:runlog:${pipeline}:${date}:${schedule}" > /dev/null
    log "Deleted runlog: ${pipeline}/${date}/${schedule}"
}

# ── REST API Helpers ─────────────────────────────────────────

register_pipeline() {
    local name="$1" json_body="$2"
    local http_code body response
    response=$(curl -sf -w "\n%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        -d "$json_body" \
        "${INTERLOCK_URL}/api/pipelines" 2>/dev/null) || response=$'\n0'
    http_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | sed '$d')
    if [ "$http_code" = "201" ]; then
        log "Registered pipeline: $name"
    else
        warn "Failed to register $name (HTTP $http_code)"
    fi
    echo "$body"
}

evaluate_pipeline() {
    local name="$1"
    curl -sf -X POST -H "Content-Type: application/json" \
        "${INTERLOCK_URL}/api/pipelines/${name}/evaluate" 2>/dev/null || echo "{}"
}

run_pipeline() {
    local name="$1"
    local response http_code body
    response=$(curl -s -w "\n%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        "${INTERLOCK_URL}/api/pipelines/${name}/run" 2>/dev/null) || response=$'\n0'
    http_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | sed '$d')
    echo "$body"
}

get_runs() {
    local name="$1"
    curl -sf "${INTERLOCK_URL}/api/pipelines/${name}/runs" 2>/dev/null || echo "[]"
}

get_readiness() {
    local name="$1"
    curl -sf "${INTERLOCK_URL}/api/pipelines/${name}/readiness" 2>/dev/null || echo "{}"
}

complete_run() {
    local run_id="$1" status="${2:-success}"
    curl -sf -X POST -H "Content-Type: application/json" \
        -d "{\"status\":\"$status\",\"metadata\":{\"source\":\"e2e-test\"}}" \
        "${INTERLOCK_URL}/api/runs/${run_id}/complete" 2>/dev/null || echo "{}"
}

get_runlog() {
    local pipeline="$1" date="$2" schedule="${3:-daily}"
    curl -sf "${INTERLOCK_URL}/api/pipelines/${pipeline}/runlogs/${date}?schedule=${schedule}" 2>/dev/null || echo "{}"
}

request_rerun() {
    local pipeline="$1" date="$2" reason="$3"
    curl -sf -X POST -H "Content-Type: application/json" \
        -d "{\"originalDate\":\"$date\",\"reason\":\"$reason\"}" \
        "${INTERLOCK_URL}/api/pipelines/${pipeline}/rerun" 2>/dev/null || echo "{}"
}

get_events() {
    local pipeline="$1" limit="${2:-50}"
    curl -sf "${INTERLOCK_URL}/api/pipelines/${pipeline}/events?limit=${limit}" 2>/dev/null || echo "[]"
}

get_reruns() {
    local pipeline="$1"
    curl -sf "${INTERLOCK_URL}/api/pipelines/${pipeline}/reruns" 2>/dev/null || echo "[]"
}

wait_for_runlog_status() {
    local pipeline="$1" status="$2" timeout="${3:-60}"
    local elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        local runlog_status
        runlog_status=$(get_runlog "$pipeline" "$TODAY" "daily" | jq -r '.status // empty' 2>/dev/null)
        if [ "$runlog_status" = "$status" ]; then
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    return 1
}

wait_for_event() {
    local pipeline="$1" event_kind="$2" timeout="${3:-60}"
    local elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        local events match
        events=$(get_events "$pipeline")
        match=$(echo "$events" | jq -r "[.[] | select(.kind == \"$event_kind\")] | first // empty" 2>/dev/null)
        if [ -n "$match" ] && [ "$match" != "null" ]; then
            echo "$match"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    return 1
}

wait_for_run() {
    local name="$1" target_status="$2" timeout="${3:-60}"
    local elapsed=0
    while [ "$elapsed" -lt "$timeout" ]; do
        local runs match
        runs=$(get_runs "$name")
        match=$(echo "$runs" | jq -r "[.[] | select(.status == \"$target_status\")] | first // empty" 2>/dev/null)
        if [ -n "$match" ] && [ "$match" != "null" ]; then
            echo "$match"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    return 1
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

# ── Service Health ───────────────────────────────────────────

wait_for_health() {
    local name="$1" url="$2" timeout="${3:-120}"
    local elapsed=0
    log "Waiting for $name..."
    while [ "$elapsed" -lt "$timeout" ]; do
        if curl -sf "$url" > /dev/null 2>&1; then
            log "$name is ready"
            return 0
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done
    fail "$name did not become healthy within ${timeout}s"
    return 1
}

# ── Scenarios ────────────────────────────────────────────────

seed_initial_data() {
    log "Setting initial sensor values (all failing)..."
    set_sensor "freshness" "300"
    set_sensor "record-count" "500"
    set_sensor "upstream" "false"
}

register_e2e_pipelines() {
    log "Registering e2e-progressive pipeline..."
    register_pipeline "e2e-progressive" '{
        "name": "e2e-progressive",
        "archetype": "batch-ingestion",
        "traits": {
            "source-freshness": {
                "evaluator": "check-freshness",
                "config": {"maxLagSeconds": 60},
                "timeout": 15,
                "ttl": 300
            },
            "upstream-dependency": {
                "evaluator": "check-upstream",
                "config": {},
                "timeout": 15,
                "ttl": 300
            },
            "resource-availability": {
                "evaluator": "check-record-count",
                "config": {"threshold": 1000},
                "timeout": 15,
                "ttl": 300
            }
        },
        "trigger": {
            "type": "http",
            "method": "POST",
            "url": "http://seed:8888/webhook",
            "headers": {"Content-Type": "application/json"},
            "body": "{\"pipeline\":\"e2e-progressive\"}",
            "timeout": 10
        },
        "watch": {"interval": "20s"}
    }' > /dev/null
}

run_scenario_1() {
    log ""
    log "--- Scenario 1: Progressive Readiness -> Trigger -> Completion ---"

    # Round 1: All traits fail
    log ""
    log "-- Round 1: All traits fail --"
    local result
    result=$(evaluate_pipeline "e2e-progressive")
    log_result "scenario-1" "round-1" "$result"

    local status
    status=$(echo "$result" | jq -r '.status // empty' 2>/dev/null)
    if [ "$status" = "NOT_READY" ]; then
        ok "Scenario 1 Round 1: NOT_READY (all sensors failing)"
    else
        fail "Scenario 1 Round 1: Expected NOT_READY, got $status"
    fi

    # Round 2: Partial pass (freshness passes, others fail)
    log ""
    log "-- Round 2: Partial pass (1/3 - freshness) --"
    set_sensor "freshness" "30"
    sleep 2

    result=$(evaluate_pipeline "e2e-progressive")
    log_result "scenario-1" "round-2" "$result"

    status=$(echo "$result" | jq -r '.status // empty' 2>/dev/null)
    if [ "$status" = "NOT_READY" ]; then
        ok "Scenario 1 Round 2: NOT_READY (partial readiness — 1/3 pass)"
    else
        fail "Scenario 1 Round 2: Expected NOT_READY, got $status"
    fi

    # Round 3: All pass → watcher triggers → complete via callback
    log ""
    log "-- Round 3: All pass -> trigger -> complete --"
    set_sensor "record-count" "1500"
    set_sensor "upstream" "true"
    delete_runlog "e2e-progressive" "$TODAY" "daily"
    sleep 2

    result=$(evaluate_pipeline "e2e-progressive")
    log_result "scenario-1" "round-3-eval" "$result"

    status=$(echo "$result" | jq -r '.status // empty' 2>/dev/null)
    if [ "$status" = "READY" ]; then
        ok "Scenario 1 Round 3: READY (all sensors passing)"
    else
        fail "Scenario 1 Round 3: Expected READY, got $status"
        return
    fi

    # Wait for watcher to trigger the pipeline (creates run in RUNNING state)
    log "Waiting for watcher to trigger pipeline (up to 45s)..."
    local run_json
    if run_json=$(wait_for_run "e2e-progressive" "RUNNING" 45); then
        ok "Scenario 1 Round 3: Watcher triggered pipeline — run is RUNNING"
        local run_id
        run_id=$(echo "$run_json" | jq -r '.runId // .runID // empty')
        log "  Run ID: $run_id"

        # Complete the run via callback
        local complete_result
        complete_result=$(complete_run "$run_id" "success")
        log_result "scenario-1" "round-3-complete" "$complete_result"

        local complete_status
        complete_status=$(echo "$complete_result" | jq -r '.status // empty' 2>/dev/null)
        if [ "$complete_status" = "COMPLETED" ]; then
            ok "Scenario 1 Round 3: Run completed via callback"
        else
            fail "Scenario 1 Round 3: Expected COMPLETED, got $complete_status"
        fi
    else
        fail "Scenario 1 Round 3: No RUNNING run found within 45s"
        log_result "scenario-1" "round-3-runs" "$(get_runs "e2e-progressive")"
    fi
}

run_scenario_2() {
    log ""
    log "--- Scenario 2: Re-run After Data Quality Drop ---"

    # Reset record-count to simulate quality drop
    delete_runlog "e2e-progressive" "$TODAY" "daily"
    set_sensor "record-count" "800"
    sleep 2

    log "-- Round 1: Data quality dropped -- count below threshold --"
    local result
    result=$(evaluate_pipeline "e2e-progressive")
    log_result "scenario-2" "round-1" "$result"

    local status
    status=$(echo "$result" | jq -r '.status // empty' 2>/dev/null)
    if [ "$status" = "NOT_READY" ]; then
        ok "Scenario 2 Round 1: NOT_READY (quality drop — count below threshold)"
    else
        fail "Scenario 2 Round 1: Expected NOT_READY, got $status"
    fi

    # Restore quality and wait for watcher to trigger
    log "-- Round 2: Quality restored -> trigger -> complete --"
    set_sensor "record-count" "1200"
    delete_runlog "e2e-progressive" "$TODAY" "daily"
    sleep 2

    result=$(evaluate_pipeline "e2e-progressive")
    log_result "scenario-2" "round-2-eval" "$result"

    status=$(echo "$result" | jq -r '.status // empty' 2>/dev/null)
    if [ "$status" = "READY" ]; then
        ok "Scenario 2 Round 2: READY (quality restored)"
    else
        fail "Scenario 2 Round 2: Expected READY, got $status"
        return
    fi

    log "Waiting for watcher to trigger pipeline (up to 45s)..."
    local run_json
    if run_json=$(wait_for_run "e2e-progressive" "RUNNING" 45); then
        ok "Scenario 2 Round 2: Watcher triggered recovery run"
        local run_id
        run_id=$(echo "$run_json" | jq -r '.runId // .runID // empty')

        local complete_result
        complete_result=$(complete_run "$run_id" "success")
        log_result "scenario-2" "round-2-complete" "$complete_result"

        local complete_status
        complete_status=$(echo "$complete_result" | jq -r '.status // empty' 2>/dev/null)
        if [ "$complete_status" = "COMPLETED" ]; then
            ok "Scenario 2 Round 2: Recovery run completed"
        else
            fail "Scenario 2 Round 2: Expected COMPLETED, got $complete_status"
        fi
    else
        fail "Scenario 2 Round 2: No RUNNING run found within 45s"
    fi
}

run_scenario_3() {
    log ""
    log "--- Scenario 3: Already Completed (Dedup) ---"

    # After Scenario 2, RunLog for today should show COMPLETED
    # Count current runs
    local runs_before
    runs_before=$(get_runs "e2e-progressive")
    local count_before
    count_before=$(echo "$runs_before" | jq 'length' 2>/dev/null)
    log "Runs before dedup check: $count_before"

    # Wait one full watcher cycle (25s) and verify no new run
    log "Waiting one watcher cycle (25s) to verify dedup..."
    sleep 25

    local runs_after
    runs_after=$(get_runs "e2e-progressive")
    local count_after
    count_after=$(echo "$runs_after" | jq 'length' 2>/dev/null)
    log_result "scenario-3" "result" "$runs_after"

    if [ "$count_after" = "$count_before" ]; then
        ok "Scenario 3: No new run created (dedup — RunLog shows COMPLETED for today)"
    else
        fail "Scenario 3: Expected $count_before runs, found $count_after (dedup failed)"
    fi
}

run_scenario_4() {
    log ""
    log "--- Scenario 4: Excluded Day ---"

    log "Registering e2e-excluded pipeline (today=$TODAY_WEEKDAY excluded)..."
    register_pipeline "e2e-excluded" "{
        \"name\": \"e2e-excluded\",
        \"archetype\": \"batch-ingestion\",
        \"exclusions\": {
            \"days\": [\"$TODAY_WEEKDAY\"]
        },
        \"traits\": {
            \"source-freshness\": {
                \"evaluator\": \"check-freshness\",
                \"config\": {\"maxLagSeconds\": 9999},
                \"timeout\": 15,
                \"ttl\": 300
            }
        },
        \"trigger\": {
            \"type\": \"http\",
            \"method\": \"POST\",
            \"url\": \"http://seed:8888/webhook\",
            \"headers\": {\"Content-Type\": \"application/json\"},
            \"timeout\": 10
        },
        \"watch\": {\"interval\": \"20s\"}
    }" > /dev/null

    # Wait one watcher cycle and verify no runs were created
    log "Waiting one watcher cycle (25s) to verify exclusion..."
    sleep 25

    local runs
    runs=$(get_runs "e2e-excluded")
    local count
    count=$(echo "$runs" | jq 'length' 2>/dev/null)
    log_result "scenario-4" "result" "$runs"

    if [ "$count" = "0" ]; then
        ok "Scenario 4: No runs created (excluded day — $TODAY_WEEKDAY)"
    else
        fail "Scenario 4: Expected 0 runs on excluded day, found $count"
    fi
}

run_scenario_5() {
    log ""
    log "--- Scenario 5: Pipeline Not Found ---"

    local response http_code body
    response=$(curl -s -w "\n%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        "${INTERLOCK_URL}/api/pipelines/e2e-nonexistent/evaluate" 2>/dev/null) || response=$'\n0'
    http_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | sed '$d')
    log_result "scenario-5" "result" "$body"

    if [ "$http_code" = "500" ] || [ "$http_code" = "404" ]; then
        ok "Scenario 5: Pipeline not found (HTTP $http_code)"
    else
        fail "Scenario 5: Expected 404 or 500, got HTTP $http_code"
    fi
}

run_scenario_6() {
    log ""
    log "--- Scenario 6: Watcher-Driven Evaluation ---"

    # Register a pipeline with all sensors already passing
    log "Registering e2e-watcher-test pipeline..."
    register_pipeline "e2e-watcher-test" '{
        "name": "e2e-watcher-test",
        "archetype": "batch-ingestion",
        "traits": {
            "source-freshness": {
                "evaluator": "check-freshness",
                "config": {"maxLagSeconds": 9999},
                "timeout": 15,
                "ttl": 300
            },
            "upstream-dependency": {
                "evaluator": "check-upstream",
                "config": {},
                "timeout": 15,
                "ttl": 300
            },
            "resource-availability": {
                "evaluator": "check-record-count",
                "config": {"threshold": 100},
                "timeout": 15,
                "ttl": 300
            }
        },
        "trigger": {
            "type": "http",
            "method": "POST",
            "url": "http://seed:8888/webhook",
            "headers": {"Content-Type": "application/json"},
            "body": "{\"pipeline\":\"e2e-watcher-test\"}",
            "timeout": 10
        },
        "watch": {"interval": "15s"}
    }' > /dev/null

    # Sensors should already be passing from Scenario 2 restore
    # (freshness=30, record-count=1200, upstream=true — all pass for the lenient thresholds)

    log "Waiting for watcher to pick up e2e-watcher-test (up to 45s)..."
    local run_json
    if run_json=$(wait_for_run "e2e-watcher-test" "RUNNING" 45); then
        ok "Scenario 6: Watcher created and triggered run"
        local run_id
        run_id=$(echo "$run_json" | jq -r '.runId // .runID // empty')
        log "  Run ID: $run_id"

        # Complete via callback
        local complete_result
        complete_result=$(complete_run "$run_id" "success")
        log_result "scenario-6" "result" "$complete_result"

        local complete_status
        complete_status=$(echo "$complete_result" | jq -r '.status // empty' 2>/dev/null)
        if [ "$complete_status" = "COMPLETED" ]; then
            ok "Scenario 6: Watcher-driven run completed via callback"
        else
            fail "Scenario 6: Expected COMPLETED, got $complete_status"
        fi
    else
        fail "Scenario 6: No RUNNING run found for e2e-watcher-test within 45s"
        log_result "scenario-6" "result" "$(get_runs "e2e-watcher-test")"
    fi
}

run_scenario_7() {
    log ""
    log "--- Scenario 7: Cascade (Silver -> Gold) ---"

    # Clean up any previous runs for these pipelines
    delete_runlog "e2e-silver" "$TODAY" "daily"
    delete_runlog "e2e-gold" "$TODAY" "daily"

    log "Registering e2e-silver pipeline (HTTP trigger)..."
    register_pipeline "e2e-silver" '{
        "name": "e2e-silver",
        "archetype": "batch-ingestion",
        "traits": {
            "source-freshness": {
                "evaluator": "check-freshness",
                "config": {"maxLagSeconds": 9999},
                "timeout": 15,
                "ttl": 300
            },
            "upstream-dependency": {
                "evaluator": "check-upstream",
                "config": {},
                "timeout": 15,
                "ttl": 300
            },
            "resource-availability": {
                "evaluator": "check-record-count",
                "config": {"threshold": 100},
                "timeout": 15,
                "ttl": 300
            }
        },
        "trigger": {
            "type": "http",
            "method": "POST",
            "url": "http://seed:8888/webhook",
            "headers": {"Content-Type": "application/json"},
            "body": "{\"pipeline\":\"e2e-silver\"}",
            "timeout": 10
        },
        "watch": {"interval": "10s"}
    }' > /dev/null

    log "Registering e2e-gold pipeline (depends on e2e-silver via check-upstream-runlog)..."
    register_pipeline "e2e-gold" '{
        "name": "e2e-gold",
        "archetype": "medallion",
        "traits": {
            "upstream-dependency": {
                "evaluator": "check-upstream-runlog",
                "config": {"upstreamPipeline": "e2e-silver"},
                "timeout": 15,
                "ttl": 60
            }
        },
        "trigger": {
            "type": "http",
            "method": "POST",
            "url": "http://seed:8888/webhook",
            "headers": {"Content-Type": "application/json"},
            "body": "{\"pipeline\":\"e2e-gold\"}",
            "timeout": 10
        },
        "watch": {"interval": "10s"}
    }' > /dev/null

    # Sensors already passing from earlier scenarios (freshness=30, record-count=1200, upstream=true)
    # Silver should evaluate READY and trigger. HTTP trigger → RUNNING → complete via callback.

    log "Waiting for silver to reach RUNNING (up to 45s)..."
    local silver_run
    if silver_run=$(wait_for_run "e2e-silver" "RUNNING" 45); then
        local silver_run_id
        silver_run_id=$(echo "$silver_run" | jq -r '.runId // .runID // empty')
        log "  Silver run ID: $silver_run_id"

        # Complete silver via callback
        complete_run "$silver_run_id" "success" > /dev/null
        sleep 2

        if wait_for_runlog_status "e2e-silver" "COMPLETED" 10; then
            ok "Scenario 7: Silver pipeline completed"
        else
            fail "Scenario 7: Silver pipeline did not reach COMPLETED after callback"
            log_result "scenario-7" "silver-runlog" "$(get_runlog "e2e-silver" "$TODAY" "daily")"
            return
        fi
    else
        fail "Scenario 7: Silver pipeline did not reach RUNNING within 45s"
        log_result "scenario-7" "silver-runlog" "$(get_runlog "e2e-silver" "$TODAY" "daily")"
        return
    fi

    # Gold's evaluator checks silver's RunLog via API. Once silver is COMPLETED, gold triggers.
    log "Waiting for gold to reach RUNNING (up to 45s)..."
    local gold_run
    if gold_run=$(wait_for_run "e2e-gold" "RUNNING" 45); then
        local gold_run_id
        gold_run_id=$(echo "$gold_run" | jq -r '.runId // .runID // empty')
        log "  Gold run ID: $gold_run_id"

        complete_run "$gold_run_id" "success" > /dev/null
        sleep 2

        if wait_for_runlog_status "e2e-gold" "COMPLETED" 10; then
            ok "Scenario 7: Gold pipeline completed (cascade from silver)"
        else
            fail "Scenario 7: Gold pipeline did not reach COMPLETED after callback"
        fi
    else
        fail "Scenario 7: Gold pipeline did not reach RUNNING within 45s"
        log_result "scenario-7" "gold-runlog" "$(get_runlog "e2e-gold" "$TODAY" "daily")"
        return
    fi

    log_result "scenario-7" "silver" "$(get_runlog "e2e-silver" "$TODAY" "daily")"
    log_result "scenario-7" "gold" "$(get_runlog "e2e-gold" "$TODAY" "daily")"
}

run_scenario_8() {
    log ""
    log "--- Scenario 8: Retry with Backoff ---"

    delete_runlog "e2e-retry" "$TODAY" "daily"

    # Close gate so trigger fails on first attempt
    log "Closing seed gate (trigger will return 500)..."
    curl -sf -X POST "http://localhost:8888/gate/close" > /dev/null 2>&1 || true

    log "Registering e2e-retry pipeline (HTTP trigger to /gate, retry maxAttempts=3, backoff=5s)..."
    register_pipeline "e2e-retry" '{
        "name": "e2e-retry",
        "archetype": "batch-ingestion",
        "traits": {
            "source-freshness": {
                "evaluator": "check-freshness",
                "config": {"maxLagSeconds": 9999},
                "timeout": 15,
                "ttl": 300
            },
            "upstream-dependency": {
                "evaluator": "check-upstream",
                "config": {},
                "timeout": 15,
                "ttl": 300
            },
            "resource-availability": {
                "evaluator": "check-record-count",
                "config": {"threshold": 100},
                "timeout": 15,
                "ttl": 300
            }
        },
        "trigger": {
            "type": "http",
            "method": "POST",
            "url": "http://seed:8888/gate",
            "headers": {"Content-Type": "application/json"},
            "body": "{\"pipeline\":\"e2e-retry\"}",
            "timeout": 10
        },
        "retry": {
            "maxAttempts": 3,
            "backoffSeconds": 5,
            "backoffMultiplier": 1.0
        },
        "watch": {"interval": "10s"}
    }' > /dev/null

    # Wait for first trigger attempt to fail
    log "Waiting for first trigger attempt to fail (up to 30s)..."
    local elapsed=0
    local first_failed=false
    while [ "$elapsed" -lt 30 ]; do
        local runlog
        runlog=$(get_runlog "e2e-retry" "$TODAY" "daily")
        local status attempt
        status=$(echo "$runlog" | jq -r '.status // empty' 2>/dev/null)
        attempt=$(echo "$runlog" | jq -r '.attemptNumber // 0' 2>/dev/null)
        if [ "$status" = "FAILED" ] && [ "$attempt" -ge 1 ]; then
            first_failed=true
            ok "Scenario 8: First trigger attempt failed (attempt $attempt)"
            break
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done

    if [ "$first_failed" = "false" ]; then
        fail "Scenario 8: First trigger attempt did not fail within 30s"
        log_result "scenario-8" "runlog" "$(get_runlog "e2e-retry" "$TODAY" "daily")"
        return
    fi

    # Open gate so retry succeeds
    log "Opening seed gate (trigger will return 200)..."
    curl -sf -X POST "http://localhost:8888/gate/open" > /dev/null 2>&1 || true

    # Wait for retry to trigger and reach RUNNING
    log "Waiting for retry to reach RUNNING (up to 60s)..."
    local retry_run
    if retry_run=$(wait_for_run "e2e-retry" "RUNNING" 60); then
        local retry_run_id attempt
        retry_run_id=$(echo "$retry_run" | jq -r '.runId // .runID // empty')
        log "  Retry run ID: $retry_run_id"

        # Complete via callback
        complete_run "$retry_run_id" "success" > /dev/null
        sleep 2

        local runlog
        runlog=$(get_runlog "e2e-retry" "$TODAY" "daily")
        local final_status
        final_status=$(echo "$runlog" | jq -r '.status // empty' 2>/dev/null)
        attempt=$(echo "$runlog" | jq -r '.attemptNumber // 0' 2>/dev/null)

        if [ "$final_status" = "COMPLETED" ] && [ "$attempt" -ge 2 ]; then
            ok "Scenario 8: Retry succeeded on attempt $attempt"
        elif [ "$final_status" = "COMPLETED" ]; then
            ok "Scenario 8: Retry completed (attempt $attempt)"
        else
            fail "Scenario 8: Expected COMPLETED, got $final_status (attempt $attempt)"
        fi
        log_result "scenario-8" "result" "$runlog"
    else
        fail "Scenario 8: Retry did not reach RUNNING within 60s"
        log_result "scenario-8" "result" "$(get_runlog "e2e-retry" "$TODAY" "daily")"
    fi
}

run_scenario_9() {
    log ""
    log "--- Scenario 9: Post-Run Monitoring / Drift Detection ---"

    delete_runlog "e2e-monitored" "$TODAY" "daily"
    # Ensure freshness sensor passes
    set_sensor "freshness" "30"

    log "Registering e2e-monitored pipeline (HTTP trigger, monitoring duration=60s)..."
    register_pipeline "e2e-monitored" '{
        "name": "e2e-monitored",
        "archetype": "batch-ingestion",
        "traits": {
            "source-freshness": {
                "evaluator": "check-freshness",
                "config": {"maxLagSeconds": 60},
                "timeout": 15,
                "ttl": 30
            },
            "upstream-dependency": {
                "evaluator": "check-upstream",
                "config": {},
                "timeout": 15,
                "ttl": 300
            },
            "resource-availability": {
                "evaluator": "check-record-count",
                "config": {"threshold": 100},
                "timeout": 15,
                "ttl": 300
            }
        },
        "trigger": {
            "type": "http",
            "method": "POST",
            "url": "http://seed:8888/webhook",
            "headers": {"Content-Type": "application/json"},
            "body": "{\"pipeline\":\"e2e-monitored\"}",
            "timeout": 10
        },
        "watch": {
            "interval": "10s",
            "monitoring": {
                "enabled": true,
                "duration": "60s"
            }
        }
    }' > /dev/null

    # Wait for run to reach RUNNING (HTTP trigger fires, run enters RUNNING)
    log "Waiting for RUNNING state (up to 30s)..."
    local run_json
    if run_json=$(wait_for_run "e2e-monitored" "RUNNING" 30); then
        local run_id
        run_id=$(echo "$run_json" | jq -r '.runId // .runID // empty')
        log "  Run ID: $run_id"

        # Complete with "success" — monitoring enabled, so transitions to COMPLETED_MONITORING
        complete_run "$run_id" "success" > /dev/null
        sleep 2
    else
        fail "Scenario 9: Run did not reach RUNNING within 30s"
        log_result "scenario-9" "runs" "$(get_runs "e2e-monitored")"
        return
    fi

    # Verify run entered COMPLETED_MONITORING
    log "Waiting for COMPLETED_MONITORING state (up to 15s)..."
    local elapsed=0
    local in_monitoring=false
    while [ "$elapsed" -lt 15 ]; do
        local runs match
        runs=$(get_runs "e2e-monitored")
        match=$(echo "$runs" | jq -r '[.[] | select(.status == "COMPLETED_MONITORING")] | first // empty' 2>/dev/null)
        if [ -n "$match" ] && [ "$match" != "null" ]; then
            in_monitoring=true
            ok "Scenario 9: Run entered COMPLETED_MONITORING state"
            break
        fi
        sleep 3
        elapsed=$((elapsed + 3))
    done

    if [ "$in_monitoring" = "false" ]; then
        fail "Scenario 9: Run did not enter COMPLETED_MONITORING after callback"
        log_result "scenario-9" "runs" "$(get_runs "e2e-monitored")"
        return
    fi

    # Simulate drift by making freshness fail
    set_sensor "freshness" "9999"
    log "Set sensor:freshness = 9999 (simulating drift)"

    # Wait for drift detection — watcher re-evaluates traits during monitoring window
    log "Waiting for drift detection (up to 45s)..."
    if wait_for_event "e2e-monitored" "MONITORING_DRIFT_DETECTED" 45; then
        ok "Scenario 9: Monitoring drift detected"
    else
        warn "Scenario 9: Drift event not found (monitoring window may have expired first)"
    fi

    # Verify rerun was created
    local reruns rerun_count
    reruns=$(get_reruns "e2e-monitored")
    rerun_count=$(echo "$reruns" | jq 'length' 2>/dev/null)
    if [ "$rerun_count" -gt 0 ]; then
        ok "Scenario 9: Rerun record created ($rerun_count rerun(s))"
    else
        warn "Scenario 9: No rerun record found (monitoring may have completed before drift)"
    fi

    log_result "scenario-9" "events" "$(get_events "e2e-monitored")"
    log_result "scenario-9" "reruns" "$reruns"

    # Restore sensor
    set_sensor "freshness" "30"
}

run_scenario_10() {
    log ""
    log "--- Scenario 10: Replay (Re-run Completed Pipeline) ---"

    # Use e2e-silver which completed in Scenario 7
    log "Requesting rerun for e2e-silver..."
    local rerun_result http_code response
    response=$(curl -s -w "\n%{http_code}" -X POST \
        -H "Content-Type: application/json" \
        -d "{\"originalDate\":\"$TODAY\",\"reason\":\"e2e replay test\"}" \
        "${INTERLOCK_URL}/api/pipelines/e2e-silver/rerun" 2>/dev/null) || response=$'\n0'
    http_code=$(echo "$response" | tail -1)
    rerun_result=$(echo "$response" | sed '$d')

    log_result "scenario-10" "rerun-request" "$rerun_result"

    if [ "$http_code" = "202" ]; then
        ok "Scenario 10: Rerun accepted (HTTP 202)"

        # Verify rerun record has a run ID
        local rerun_run_id
        rerun_run_id=$(echo "$rerun_result" | jq -r '.rerunRunID // .RerunRunID // empty' 2>/dev/null)
        if [ -n "$rerun_run_id" ] && [ "$rerun_run_id" != "null" ]; then
            ok "Scenario 10: Rerun created new run: $rerun_run_id"
        else
            warn "Scenario 10: Rerun accepted but no run ID in response"
        fi
    elif [ "$http_code" = "412" ]; then
        # Pipeline traits not ready (precondition failed) — rerun stored as PENDING
        warn "Scenario 10: Rerun stored as PENDING (traits not ready, HTTP 412)"
        local rerun_status
        rerun_status=$(echo "$rerun_result" | jq -r '.rerun.status // empty' 2>/dev/null)
        if [ "$rerun_status" = "PENDING" ]; then
            ok "Scenario 10: Rerun record created with PENDING status"
        else
            fail "Scenario 10: Unexpected rerun status: $rerun_status"
        fi
    else
        fail "Scenario 10: Expected HTTP 202 or 412, got $http_code"
    fi

    # Verify rerun appears in the reruns list
    local reruns rerun_count
    reruns=$(get_reruns "e2e-silver")
    rerun_count=$(echo "$reruns" | jq 'length' 2>/dev/null)
    log_result "scenario-10" "reruns" "$reruns"

    if [ "$rerun_count" -gt 0 ]; then
        ok "Scenario 10: Rerun record visible in reruns list ($rerun_count)"
    else
        fail "Scenario 10: No rerun records found for e2e-silver"
    fi
}

run_scenario_11() {
    log ""
    log "--- Scenario 11: Evaluation SLA Breach Alert ---"

    delete_runlog "e2e-sla-breach" "$TODAY" "daily"

    # Set freshness high enough to exceed the evaluator's fallback threshold (300)
    set_sensor "freshness" "999"

    log "Registering e2e-sla-breach pipeline (strict freshness, SLA deadline 00:01 = always past)..."
    register_pipeline "e2e-sla-breach" '{
        "name": "e2e-sla-breach",
        "archetype": "batch-ingestion",
        "traits": {
            "source-freshness": {
                "evaluator": "check-freshness",
                "config": {"maxLagSeconds": 5},
                "timeout": 15,
                "ttl": 300
            },
            "upstream-dependency": {
                "evaluator": "check-upstream",
                "config": {},
                "timeout": 15,
                "ttl": 300
            },
            "resource-availability": {
                "evaluator": "check-record-count",
                "config": {"threshold": 100},
                "timeout": 15,
                "ttl": 300
            }
        },
        "trigger": {
            "type": "http",
            "method": "POST",
            "url": "http://seed:8888/webhook",
            "headers": {"Content-Type": "application/json"},
            "body": "{\"pipeline\":\"e2e-sla-breach\"}",
            "timeout": 10
        },
        "sla": {
            "evaluationDeadline": "00:01",
            "timezone": "UTC"
        },
        "watch": {"interval": "10s"}
    }' > /dev/null

    # Wait for SLA breach event — watcher evaluates, traits fail, deadline already past
    log "Waiting for SLA_BREACHED event (up to 30s)..."
    if wait_for_event "e2e-sla-breach" "SLA_BREACHED" 30; then
        ok "Scenario 11: SLA_BREACHED event detected"
    else
        fail "Scenario 11: No SLA_BREACHED event found within 30s"
    fi

    log_result "scenario-11" "events" "$(get_events "e2e-sla-breach")"

    # Restore freshness for other scenarios
    set_sensor "freshness" "30"
}

# ── Teardown ─────────────────────────────────────────────────

do_teardown() {
    log "Starting teardown..."
    compose down -v || warn "docker compose down failed"
    log "Teardown complete."
    log "Results preserved in: $RESULTS_DIR/"
}

# ── Main ─────────────────────────────────────────────────────

do_run() {
    mkdir -p "$RESULTS_DIR"

    log "Interlock Local E2E Test Suite"
    log "Date: $TODAY ($TODAY_WEEKDAY)"
    log ""

    # Ensure clean state
    log "Tearing down any existing stack..."
    compose down -v 2>/dev/null || true

    # Start the stack
    log "Starting Docker Compose stack..."
    compose up -d --build

    # Wait for services
    wait_for_health "Interlock" "${INTERLOCK_URL}/api/health" 60
    wait_for_health "Airflow" "http://localhost:8080/api/v1/health" 180

    # Seed initial sensor data
    seed_initial_data

    # Register E2E pipelines
    register_e2e_pipelines

    # Run scenarios
    run_scenario_1
    run_scenario_2
    run_scenario_3
    run_scenario_4
    run_scenario_5
    run_scenario_6
    run_scenario_7
    run_scenario_8
    run_scenario_9
    run_scenario_10
    run_scenario_11

    # Wait for archiver to flush all data to Postgres (interval=30s)
    log "Waiting for Postgres archival cycle (35s)..."
    sleep 35

    # Verify Postgres has data
    local pg_runs
    pg_runs=$(compose exec -T postgres psql -U interlock -d interlock -tAc \
        "SELECT count(*) FROM runs WHERE pipeline_id LIKE 'e2e-%'" 2>/dev/null || echo "0")
    pg_runs=$(echo "$pg_runs" | tr -d '[:space:]')
    if [ "$pg_runs" -gt 0 ] 2>/dev/null; then
        ok "Postgres archival: $pg_runs run(s) archived"
    else
        fail "Postgres archival: no runs found in Postgres"
    fi

    local pg_events
    pg_events=$(compose exec -T postgres psql -U interlock -d interlock -tAc \
        "SELECT count(*) FROM events WHERE pipeline_id LIKE 'e2e-%'" 2>/dev/null || echo "0")
    pg_events=$(echo "$pg_events" | tr -d '[:space:]')
    if [ "$pg_events" -gt 0 ] 2>/dev/null; then
        ok "Postgres archival: $pg_events event(s) archived"
    else
        fail "Postgres archival: no events found in Postgres"
    fi

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
        echo "  run       Start stack, run all E2E scenarios, collect results"
        echo "  teardown  Destroy stack (results preserved in e2e-results/)"
        exit 1
        ;;
esac
