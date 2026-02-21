"""Seed script: register demo pipelines, run callback server, and drive the http-callback scenario."""

import json
import os
import sys
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import Request, urlopen
from urllib.error import URLError

INTERLOCK = os.environ.get("INTERLOCK_URL", "http://interlock:3000")
AIRFLOW = os.environ.get("AIRFLOW_URL", "http://airflow:8080")
AIRFLOW_AUTH = "Basic YWRtaW46YWRtaW4="  # admin:admin


# ── Helpers ────────────────────────────────────────────────

def api(method, url, body=None, headers=None):
    """Make an HTTP request and return parsed JSON (or None on failure)."""
    hdrs = {"Content-Type": "application/json"}
    if headers:
        hdrs.update(headers)
    data = json.dumps(body).encode() if body else None
    req = Request(url, data=data, headers=hdrs, method=method)
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except Exception as exc:
        print(f"  [{method} {url}] error: {exc}", flush=True)
        return None


def wait_for_service(name, check_fn, max_wait=300):
    """Poll check_fn() until it returns True, with exponential backoff."""
    start = time.time()
    delay = 2
    while time.time() - start < max_wait:
        try:
            if check_fn():
                print(f"  {name}: ready", flush=True)
                return True
        except Exception:
            pass
        print(f"  {name}: waiting ({int(time.time() - start)}s)…", flush=True)
        time.sleep(delay)
        delay = min(delay * 1.5, 15)
    print(f"  {name}: timed out after {max_wait}s", flush=True)
    return False


# ── Service health checks ─────────────────────────────────

def check_interlock():
    r = api("GET", f"{INTERLOCK}/api/health")
    return r and r.get("status") == "ok"


def check_airflow():
    r = api("GET", f"{AIRFLOW}/api/v1/health", headers={"Authorization": AIRFLOW_AUTH})
    return r and r.get("metadatabase", {}).get("status") == "healthy"


def check_dag(dag_id):
    r = api("GET", f"{AIRFLOW}/api/v1/dags/{dag_id}", headers={"Authorization": AIRFLOW_AUTH})
    return r and r.get("dag_id") == dag_id


# ── Pipeline definitions ──────────────────────────────────

PIPELINES = [
    {
        "name": "airflow-success",
        "archetype": "demo-simple",
        "tier": 1,
        "traits": {
            "data-check": {
                "evaluator": "always-pass",
                "config": {},
            },
        },
        "trigger": {
            "type": "airflow",
            "url": AIRFLOW,
            "dagID": "demo_success",
            "headers": {"Authorization": AIRFLOW_AUTH},
            "timeout": 30,
            "pollInterval": "10s",
        },
        "watch": {"interval": "20s"},
    },
    {
        "name": "airflow-failure",
        "archetype": "demo-simple",
        "tier": 1,
        "traits": {
            "data-check": {
                "evaluator": "always-pass",
                "config": {},
            },
        },
        "trigger": {
            "type": "airflow",
            "url": AIRFLOW,
            "dagID": "demo_failure",
            "headers": {"Authorization": AIRFLOW_AUTH},
            "timeout": 30,
            "pollInterval": "10s",
        },
        "watch": {"interval": "20s"},
    },
    {
        "name": "blocked-pipeline",
        "archetype": "demo-simple",
        "tier": 3,
        "traits": {
            "data-check": {
                "evaluator": "always-fail",
                "config": {},
            },
        },
        "watch": {"interval": "20s"},
    },
    {
        "name": "http-callback",
        "archetype": "demo-simple",
        "tier": 2,
        "traits": {
            "data-check": {
                "evaluator": "always-pass",
                "config": {},
            },
        },
        "trigger": {
            "type": "http",
            "method": "POST",
            "url": "http://seed:8888/webhook",
            "headers": {"Content-Type": "application/json"},
            "body": '{"pipeline":"http-callback"}',
            "timeout": 10,
        },
        "watch": {"interval": "20s"},
    },
]


# ── Webhook server (receives trigger from Interlock) ──────

class WebhookHandler(BaseHTTPRequestHandler):
    """Accepts POST /webhook and logs the trigger."""

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b""
        print(f"[webhook] received trigger: {body.decode()}", flush=True)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"status":"accepted"}')

    def log_message(self, fmt, *args):
        pass  # suppress default stderr logging


def run_webhook_server():
    server = HTTPServer(("0.0.0.0", 8888), WebhookHandler)
    print("[webhook] listening on :8888", flush=True)
    server.serve_forever()


# ── Callback loop: complete http-callback runs ────────────

def callback_loop():
    """Poll for RUNNING http-callback runs and send completion callbacks after a delay."""
    print("[callback] starting callback loop", flush=True)
    completed_runs = set()
    while True:
        time.sleep(10)
        try:
            runs = api("GET", f"{INTERLOCK}/api/pipelines/http-callback/runs")
            if not runs:
                continue
            for run in runs:
                run_id = run.get("runId", "")
                status = run.get("status", "")
                if status == "RUNNING" and run_id not in completed_runs:
                    print(f"[callback] found RUNNING run {run_id}, sending completion in 5s…", flush=True)
                    time.sleep(5)
                    result = api("POST", f"{INTERLOCK}/api/runs/{run_id}/complete",
                                 body={"status": "success", "metadata": {"source": "demo-callback"}})
                    if result:
                        print(f"[callback] completed run {run_id}: {result.get('status')}", flush=True)
                        completed_runs.add(run_id)
                    else:
                        print(f"[callback] failed to complete run {run_id}", flush=True)
        except Exception as exc:
            print(f"[callback] error: {exc}", flush=True)


# ── Main ──────────────────────────────────────────────────

def main():
    print("=" * 60, flush=True)
    print("Interlock Demo Seed", flush=True)
    print("=" * 60, flush=True)

    # Start webhook server in background
    webhook_thread = threading.Thread(target=run_webhook_server, daemon=True)
    webhook_thread.start()

    # Wait for services
    print("\n[seed] waiting for services…", flush=True)
    if not wait_for_service("interlock", check_interlock):
        sys.exit(1)
    if not wait_for_service("airflow", check_airflow):
        sys.exit(1)
    if not wait_for_service("dag:demo_success", lambda: check_dag("demo_success")):
        sys.exit(1)
    if not wait_for_service("dag:demo_failure", lambda: check_dag("demo_failure")):
        sys.exit(1)

    # Register pipelines
    print("\n[seed] registering pipelines…", flush=True)
    for pipeline in PIPELINES:
        result = api("POST", f"{INTERLOCK}/api/pipelines", body=pipeline)
        if result:
            print(f"  registered: {pipeline['name']}", flush=True)
        else:
            print(f"  FAILED: {pipeline['name']}", flush=True)

    # List registered pipelines
    print("\n[seed] verifying registration…", flush=True)
    pipelines = api("GET", f"{INTERLOCK}/api/pipelines")
    if pipelines:
        print(f"  {len(pipelines)} pipeline(s) registered", flush=True)
        for p in pipelines:
            print(f"    - {p.get('name')} (archetype: {p.get('archetype')}, tier: {p.get('tier', 'n/a')})", flush=True)

    print("\n[seed] setup complete — watcher will drive evaluation and triggers", flush=True)
    print("[seed] entering callback loop for http-callback pipeline…\n", flush=True)

    # Run callback loop in foreground (keeps container alive)
    callback_loop()


if __name__ == "__main__":
    main()
