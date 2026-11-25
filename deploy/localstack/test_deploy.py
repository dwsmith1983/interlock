"""Unit tests for deploy.py. No network or LocalStack: every AWS client is mocked.

Run from deploy/localstack:
    python -m unittest -v test_deploy
"""
from __future__ import annotations

import contextlib
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import deploy

SFN_ARN = f"arn:aws:states:{deploy.REGION}:{deploy.DUMMY_ACCOUNT_ID}:stateMachine:{deploy.STATE_MACHINE_NAME}"
SCHEDULER_VARS = ("SLA_MONITOR_ARN", "SCHEDULER_ROLE_ARN", "SCHEDULER_GROUP_NAME")
CLOCK_URL = f"{deploy.E2E_MOCK_URL}/clock"


class LambdaEnvTest(unittest.TestCase):
    def test_lambda_env_default_unchanged(self) -> None:
        for handler in deploy.LAMBDA_HANDLERS:
            with self.subTest(handler=handler):
                env = deploy._lambda_env(handler, SFN_ARN)
                self.assertEqual([k for k in env if k.startswith("E2E_")], [])
                self.assertNotIn("JOB_CHECK_INTERVAL_SECONDS", env)
                self.assertNotIn("RETRY_DELAY_MAX_SECONDS", env)
        self.assertEqual(deploy._lambda_env("sla-monitor", SFN_ARN)["SKIP_SCHEDULER"], "true")
        watchdog = deploy._lambda_env("watchdog", SFN_ARN)
        self.assertEqual([v for v in SCHEDULER_VARS if v in watchdog], [])

    def test_lambda_env_e2e_sets_hooks(self) -> None:
        for handler in deploy.LAMBDA_HANDLERS:
            with self.subTest(handler=handler):
                env = deploy._lambda_env(handler, SFN_ARN, e2e=True)
                self.assertEqual(env["E2E_CLOCK_URL"], CLOCK_URL)
                self.assertEqual(env["E2E_SCHEDULE_TABLE"], deploy.TABLE_SCHEDULES)
                self.assertEqual(env["JOB_CHECK_INTERVAL_SECONDS"], "5")
                self.assertEqual(env["RETRY_DELAY_MAX_SECONDS"], "5")
                self.assertNotIn("SKIP_SCHEDULER", env)

    def test_lambda_env_e2e_scheduler_env(self) -> None:
        for handler in ("watchdog", "sla-monitor"):
            with self.subTest(handler=handler):
                env = deploy._lambda_env(handler, SFN_ARN, e2e=True)
                for var in SCHEDULER_VARS:
                    self.assertTrue(env.get(var), f"{handler} missing {var}")


class ParseArgsTest(unittest.TestCase):
    def test_parse_args(self) -> None:
        self.assertFalse(deploy.parse_args(["deploy"]).e2e)
        self.assertTrue(deploy.parse_args(["deploy", "--e2e"]).e2e)
        self.assertEqual(deploy.parse_args(["teardown"]).command, "teardown")
        self.assertEqual(deploy.parse_args(["smoke"]).command, "smoke")
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as cm:
            deploy.parse_args(["bogus"])
        self.assertNotEqual(cm.exception.code, 0)


class CmdDeployTest(unittest.TestCase):
    def _run_deploy(
        self, e2e: bool, sfn_arn: str = SFN_ARN
    ) -> tuple[dict[str, mock.MagicMock], mock.MagicMock]:
        client = mock.MagicMock()
        stubs = {
            "ensure_iam_roles": None,
            "ensure_schedules_table": None,
            "ensure_tables": {deploy.TABLE_CONTROL: "ctl-stream", deploy.TABLE_JOBLOG: "log-stream"},
            "ensure_alert_queue": ("queue-url", "queue-arn"),
            "ensure_event_bus": "bus-arn",
            "ensure_lambdas": {h: f"arn-{h}" for h in deploy.LAMBDA_HANDLERS},
            "ensure_state_machine": sfn_arn,
            "ensure_event_rule_to_sink": None,
            "ensure_event_rule_to_alerts": None,
            "ensure_watchdog_schedule": None,
            "ensure_stream_mapping": None,
            "ensure_sqs_mapping": None,
        }
        mocks: dict[str, mock.MagicMock] = {}
        with contextlib.ExitStack() as stack:
            for name, ret in stubs.items():
                mocks[name] = stack.enter_context(
                    mock.patch.object(deploy, name, return_value=ret)
                )
            stack.enter_context(mock.patch.object(deploy, "_client", return_value=client))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            deploy.cmd_deploy(e2e=e2e)
        return mocks, client

    def test_deploy_e2e_skips_watchdog_rule_and_sqs_mapping(self) -> None:
        mocks, _ = self._run_deploy(e2e=True)
        mocks["ensure_watchdog_schedule"].assert_not_called()
        mocks["ensure_sqs_mapping"].assert_not_called()
        mocks["ensure_schedules_table"].assert_called_once_with()
        mocks["ensure_lambdas"].assert_called_once_with(sfn_arn=SFN_ARN, e2e=True)

        mocks, _ = self._run_deploy(e2e=False)
        mocks["ensure_watchdog_schedule"].assert_called_once()
        mocks["ensure_sqs_mapping"].assert_called_once()
        mocks["ensure_schedules_table"].assert_not_called()
        mocks["ensure_lambdas"].assert_called_once_with(sfn_arn=SFN_ARN, e2e=False)

    def test_deploy_e2e_sfn_starters_env_update_keeps_hooks(self) -> None:
        _, client = self._run_deploy(e2e=True, sfn_arn="arn:other")
        calls = client.update_function_configuration.call_args_list
        updated = {c.kwargs["FunctionName"]: c.kwargs["Environment"]["Variables"] for c in calls}
        want = sorted(f"{deploy.PREFIX}-{h}" for h in ("orchestrator", "stream-router", "watchdog"))
        self.assertEqual(len(calls), 3)
        self.assertEqual(sorted(updated), want)
        for name, env in updated.items():
            with self.subTest(function=name):
                self.assertEqual(env["STATE_MACHINE_ARN"], "arn:other")
                self.assertEqual(env["E2E_CLOCK_URL"], CLOCK_URL)
        orch = updated[f"{deploy.PREFIX}-orchestrator"]
        self.assertEqual(orch["DISABLE_SSRF_PROTECTION"], "true")
        watchdog = updated[f"{deploy.PREFIX}-watchdog"]
        for var in (*SCHEDULER_VARS, "E2E_SCHEDULE_TABLE"):
            self.assertTrue(watchdog.get(var), f"watchdog lost {var}")

    def test_ensure_lambdas_e2e_forwards_hooks(self) -> None:
        client = mock.MagicMock()
        client.create_function.return_value = {"FunctionArn": "arn"}
        with tempfile.TemporaryDirectory() as dist, contextlib.ExitStack() as stack:
            for h in deploy.LAMBDA_HANDLERS:
                (Path(dist) / f"{h}.zip").touch()
            stack.enter_context(mock.patch.object(deploy, "DIST_DIR", Path(dist)))
            stack.enter_context(mock.patch.object(deploy, "_client", return_value=client))
            stack.enter_context(mock.patch.object(deploy, "_zip_bytes", return_value=b""))
            stack.enter_context(mock.patch.object(deploy.time, "sleep"))
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            deploy.ensure_lambdas(SFN_ARN, e2e=True)
        calls = client.create_function.call_args_list
        self.assertEqual(len(calls), len(deploy.LAMBDA_HANDLERS))
        for c in calls:
            env = c.kwargs["Environment"]["Variables"]
            with self.subTest(function=c.kwargs["FunctionName"]):
                self.assertEqual(env["E2E_CLOCK_URL"], CLOCK_URL)
                self.assertEqual(env["E2E_SCHEDULE_TABLE"], deploy.TABLE_SCHEDULES)


class AlertAllowlistTest(unittest.TestCase):
    def test_config_invalid_alerts(self) -> None:
        self.assertIn("CONFIG_INVALID", deploy.ALERT_DETAIL_TYPES)


class CmdSmokeTest(unittest.TestCase):
    def test_smoke_ignores_schedules_table(self) -> None:
        client = mock.MagicMock()
        client.list_functions.return_value = {
            "Functions": [{"FunctionName": f"{deploy.PREFIX}-{h}"} for h in deploy.LAMBDA_HANDLERS]
        }
        client.list_tables.return_value = {"TableNames": [
            deploy.TABLE_CONTROL, deploy.TABLE_JOBLOG, deploy.TABLE_EVENTS,
            deploy.TABLE_RERUN, deploy.TABLE_SCHEDULES,
        ]}
        client.get_function.return_value = {
            "Configuration": {"Environment": {"Variables": {"AWS_ENDPOINT_URL": "x"}}}
        }
        with mock.patch.object(deploy, "_client", return_value=client), \
                contextlib.redirect_stdout(io.StringIO()):
            deploy.cmd_smoke()  # exits 1 on a mismatch


if __name__ == "__main__":
    unittest.main()
