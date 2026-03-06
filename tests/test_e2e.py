"""
End-to-end integration tests for metaflow-kestra.

These tests require a running Kestra instance (see docker-compose.yaml).
Run them with:

    pytest tests/test_e2e.py -m integration -v

Or with the Kestra host:

    KESTRA_HOST=http://localhost:8080 pytest tests/test_e2e.py -m integration -v

The tests:
1. Deploy a Kestra flow via the REST API
2. Trigger an execution
3. Wait for completion
4. Verify the Metaflow run produced the expected artifacts
"""

import os
import sys
import time
import tempfile
import subprocess
import json

import pytest

from conftest import FLOWS_DIR, compile_flow

pytestmark = pytest.mark.integration

METAFLOW_HOME = os.environ.get(
    "METAFLOW_DATASTORE_SYSROOT_LOCAL",
    os.path.join(os.path.expanduser("~"), ".metaflow"),
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _deploy_yaml(kestra_client, yaml_content: str):
    """Deploy or update a Kestra flow YAML via REST API. Returns the flow dict."""
    import yaml as _yaml
    host = kestra_client._kestra_host
    resp = kestra_client.post(
        "%s/api/v1/flows" % host,
        data=yaml_content.encode("utf-8"),
        headers={"Content-Type": "application/x-yaml"},
    )
    if resp.status_code in (200, 201):
        return resp.json()
    # Flow already exists — update via PUT /api/v1/flows/{namespace}/{id}
    try:
        flow_meta = _yaml.safe_load(yaml_content)
        ns = flow_meta.get("namespace", "")
        fid = flow_meta.get("id", "")
    except Exception:
        ns, fid = "", ""
    resp2 = kestra_client.put(
        "%s/api/v1/flows/%s/%s" % (host, ns, fid),
        data=yaml_content.encode("utf-8"),
        headers={"Content-Type": "application/x-yaml"},
    )
    assert resp2.status_code in (200, 201), (
        "Deploy failed HTTP %d: %s" % (resp2.status_code, resp2.text[:500])
    )
    return resp2.json()


def _trigger(kestra_client, kestra_ns: str, flow_id: str, inputs: dict = None) -> str:
    """Trigger an execution. Returns execution_id."""
    host = kestra_client._kestra_host
    url = "%s/api/v1/executions/%s/%s" % (host, kestra_ns, flow_id)
    if inputs:
        # Kestra requires multipart/form-data for inputs (not url-encoded).
        files = {k: (None, str(v)) for k, v in inputs.items()}
        resp = kestra_client.post(url, files=files)
    else:
        resp = kestra_client.post(url)
    assert resp.status_code in (200, 201), (
        "Trigger failed HTTP %d: %s" % (resp.status_code, resp.text[:500])
    )
    return resp.json()["id"]


def _wait(kestra_client, execution_id: str, timeout_secs: int = 300) -> str:
    """Poll until execution reaches a terminal state. Returns final state."""
    host = kestra_client._kestra_host
    url = "%s/api/v1/executions/%s" % (host, execution_id)
    terminal = {"SUCCESS", "FAILED", "KILLED", "WARNING"}
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        resp = kestra_client.get(url)
        if resp.status_code == 200:
            state = resp.json().get("state", {}).get("current", "UNKNOWN")
            if state in terminal:
                return state
        time.sleep(5)
    raise TimeoutError("Execution %s did not finish within %ds" % (execution_id, timeout_secs))


def _get_run_id_from_execution(kestra_client, exec_id: str) -> str:
    """Extract the Metaflow run_id from the Kestra execution outputs."""
    host = kestra_client._kestra_host
    resp = kestra_client.get("%s/api/v1/executions/%s" % (host, exec_id))
    data = resp.json()
    for task_run in data.get("taskRunList", []):
        if task_run.get("taskId") == "metaflow_init":
            outputs = task_run.get("outputs", {})
            # Kestra stores Kestra.outputs() values under "vars" in the API response
            run_id = outputs.get("vars", {}).get("run_id") or outputs.get("run_id")
            if run_id:
                return run_id
    return None


def _get_metaflow_run(flow_name: str, run_id: str = None) -> object:
    """Return a Metaflow Run by run_id, or the most recent run if run_id is None.

    Forces local metadata provider (not Netflix-internal) and disables namespace
    filtering so runs created by the Kestra worker user are visible.
    """
    import metaflow
    metaflow.metadata("local")
    metaflow.namespace(None)
    if run_id:
        try:
            return metaflow.Run("%s/%s" % (flow_name, run_id))
        except Exception:
            return None
    runs = list(metaflow.Flow(flow_name).runs())
    if not runs:
        return None
    return runs[0]


# ---------------------------------------------------------------------------
# Linear flow E2E
# ---------------------------------------------------------------------------

class TestLinearE2E:
    FLOW_FILE = os.path.join(FLOWS_DIR, "linear_flow.py")
    KESTRA_NS = "metaflow"
    FLOW_ID = "linearflow"

    def test_deploy_and_run(self, kestra_client, tmp_path):
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "linear.yaml"))

        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(kestra_client, self.KESTRA_NS, self.FLOW_ID)
        final_state = _wait(kestra_client, exec_id)
        assert final_state == "SUCCESS", "Execution ended with state: %s" % final_state

    def test_metaflow_artifacts(self, kestra_client, tmp_path):
        """After execution, verify Metaflow artifacts are accessible."""
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "linear2.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(kestra_client, self.KESTRA_NS, self.FLOW_ID)
        _wait(kestra_client, exec_id)

        run_id = _get_run_id_from_execution(kestra_client, exec_id)
        assert run_id is not None, "Could not find run_id in Kestra execution outputs"
        run = _get_metaflow_run("LinearFlow", run_id)
        assert run is not None, "No Metaflow run found for LinearFlow/%s" % run_id
        end_task = run["end"].task
        assert end_task.data.result == "HELLO FROM START"

    def test_execution_outputs_contain_artifact_hint(self, kestra_client, tmp_path):
        """Verify that task outputs in Kestra contain the metaflow_snippet."""
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "linear3.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(kestra_client, self.KESTRA_NS, self.FLOW_ID)
        _wait(kestra_client, exec_id)

        host = kestra_client._kestra_host
        resp = kestra_client.get("%s/api/v1/executions/%s" % (host, exec_id))
        data = resp.json()
        # Look for metaflow_snippet in any task run output.
        # Kestra stores Kestra.outputs() values under the "vars" key in the API response.
        found_hint = False
        for task_run in data.get("taskRunList", []):
            outputs = task_run.get("outputs", {})
            vars_out = outputs.get("vars", outputs)
            if "metaflow_snippet" in vars_out:
                found_hint = True
                snippet = vars_out["metaflow_snippet"]
                assert "from metaflow import Task" in snippet
                break
        assert found_hint, "No metaflow_snippet found in any task run output"


# ---------------------------------------------------------------------------
# Param flow E2E
# ---------------------------------------------------------------------------

class TestParamE2E:
    FLOW_FILE = os.path.join(FLOWS_DIR, "param_flow.py")
    KESTRA_NS = "metaflow"
    FLOW_ID = "paramflow"

    def test_deploy_and_run_with_inputs(self, kestra_client, tmp_path):
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "param.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(
            kestra_client, self.KESTRA_NS, self.FLOW_ID,
            inputs={"greeting": "world", "count": 2},
        )
        final_state = _wait(kestra_client, exec_id)
        assert final_state == "SUCCESS"

    def test_default_params(self, kestra_client, tmp_path):
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "param_default.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(kestra_client, self.KESTRA_NS, self.FLOW_ID)
        final_state = _wait(kestra_client, exec_id)
        assert final_state == "SUCCESS"


# ---------------------------------------------------------------------------
# Branch flow E2E
# ---------------------------------------------------------------------------

class TestBranchE2E:
    FLOW_FILE = os.path.join(FLOWS_DIR, "branch_flow.py")
    KESTRA_NS = "metaflow"
    FLOW_ID = "branchflow"

    def test_deploy_and_run(self, kestra_client, tmp_path):
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "branch.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(kestra_client, self.KESTRA_NS, self.FLOW_ID)
        final_state = _wait(kestra_client, exec_id, timeout_secs=120)
        assert final_state == "SUCCESS"

    def test_both_branches_produced_artifacts(self, kestra_client, tmp_path):
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "branch2.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(kestra_client, self.KESTRA_NS, self.FLOW_ID)
        _wait(kestra_client, exec_id, timeout_secs=120)

        run_id = _get_run_id_from_execution(kestra_client, exec_id)
        run = _get_metaflow_run("BranchFlow", run_id)
        assert run is not None
        end_task = run["end"].task
        assert end_task.data.total == 15   # sum([1,2,3,4,5])
        assert end_task.data.maximum == 5  # max([1,2,3,4,5])


# ---------------------------------------------------------------------------
# Foreach flow E2E
# ---------------------------------------------------------------------------

class TestForeachE2E:
    FLOW_FILE = os.path.join(FLOWS_DIR, "foreach_flow.py")
    KESTRA_NS = "metaflow"
    FLOW_ID = "foreachflow"

    def test_deploy_and_run(self, kestra_client, tmp_path):
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "foreach.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(kestra_client, self.KESTRA_NS, self.FLOW_ID)
        final_state = _wait(kestra_client, exec_id, timeout_secs=180)
        assert final_state == "SUCCESS"

    def test_foreach_results_correct(self, kestra_client, tmp_path):
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "foreach2.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(kestra_client, self.KESTRA_NS, self.FLOW_ID)
        _wait(kestra_client, exec_id, timeout_secs=180)

        run_id = _get_run_id_from_execution(kestra_client, exec_id)
        run = _get_metaflow_run("ForeachFlow", run_id)
        assert run is not None
        end_task = run["end"].task
        results = sorted(end_task.data.results)
        assert results == [10, 20, 30]

    def test_kestra_metadata_in_metaflow(self, kestra_client, tmp_path):
        """Verify that kestra-execution-id metadata is recorded in Metaflow."""
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "foreach3.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(kestra_client, self.KESTRA_NS, self.FLOW_ID)
        _wait(kestra_client, exec_id, timeout_secs=180)

        run_id = _get_run_id_from_execution(kestra_client, exec_id)
        run = _get_metaflow_run("ForeachFlow", run_id)
        assert run is not None
        # Check that at least one task has kestra-execution-id metadata.
        # metadata_dict is a flat {field: value} dict.
        start_task = run["start"].task
        metadata = start_task.metadata_dict
        assert "kestra-execution-id" in metadata


# ---------------------------------------------------------------------------
# Conditional flow E2E
# ---------------------------------------------------------------------------

class TestConditionalE2E:
    FLOW_FILE = os.path.join(FLOWS_DIR, "conditional_flow.py")
    KESTRA_NS = "metaflow"
    FLOW_ID = "conditionalflow"

    def test_deploy_and_run_low_branch(self, kestra_client, tmp_path):
        """Default value=42 should take the low branch."""
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "conditional.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(kestra_client, self.KESTRA_NS, self.FLOW_ID)
        final_state = _wait(kestra_client, exec_id, timeout_secs=180)
        assert final_state == "SUCCESS", "Execution ended with state: %s" % final_state

    def test_low_branch_artifacts(self, kestra_client, tmp_path):
        """With value=42 (< 50), low_branch should execute and label should be 'low'."""
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "conditional_low.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(kestra_client, self.KESTRA_NS, self.FLOW_ID)
        _wait(kestra_client, exec_id, timeout_secs=180)

        run_id = _get_run_id_from_execution(kestra_client, exec_id)
        assert run_id is not None
        run = _get_metaflow_run("ConditionalFlow", run_id)
        assert run is not None
        end_task = run["end"].task
        assert end_task.data.label == "low"
        assert end_task.data.doubled == 84  # 42 * 2

    def test_high_branch_artifacts(self, kestra_client, tmp_path):
        """With value=75 (>= 50), high_branch should execute and label should be 'high'."""
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "conditional_high.yaml"))
        _deploy_yaml(kestra_client, yaml_str)
        exec_id = _trigger(
            kestra_client, self.KESTRA_NS, self.FLOW_ID,
            inputs={"value": 75},
        )
        _wait(kestra_client, exec_id, timeout_secs=180)

        run_id = _get_run_id_from_execution(kestra_client, exec_id)
        assert run_id is not None
        run = _get_metaflow_run("ConditionalFlow", run_id)
        assert run is not None
        end_task = run["end"].task
        assert end_task.data.label == "high"
        assert end_task.data.doubled == 150  # 75 * 2


# ---------------------------------------------------------------------------
# Schedule flow E2E (just deploy, don't wait for trigger)
# ---------------------------------------------------------------------------

class TestScheduleE2E:
    FLOW_FILE = os.path.join(FLOWS_DIR, "schedule_flow.py")
    KESTRA_NS = "metaflow"

    def test_deploy_with_schedule(self, kestra_client, tmp_path):
        yaml_str = compile_flow(self.FLOW_FILE, str(tmp_path / "schedule.yaml"))
        result = _deploy_yaml(kestra_client, yaml_str)
        assert result is not None
        # The YAML should have a trigger; Kestra registers it on deploy


# ---------------------------------------------------------------------------
# CLI run command E2E
# ---------------------------------------------------------------------------

class TestCLIRunE2E:
    FLOW_FILE = os.path.join(FLOWS_DIR, "linear_flow.py")

    def test_cli_run(self, kestra_client, kestra_host):
        """Test `python flow.py kestra run --wait` CLI command end-to-end."""
        user = os.environ.get("KESTRA_USER", "admin@kestra.io")
        password = os.environ.get("KESTRA_PASSWORD", "Kestra1234!")
        cmd = [
            sys.executable, self.FLOW_FILE,
            "--no-pylint",
            "kestra", "run",
            "--kestra-host", kestra_host,
            "--kestra-user", user,
            "--kestra-password", password,
            "--wait",
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, (
            "CLI run failed:\nSTDOUT: %s\nSTDERR: %s" % (result.stdout, result.stderr)
        )
        # Metaflow's obj.echo() writes to stderr; check both streams.
        combined = (result.stdout + result.stderr).lower()
        assert "successfully" in combined or "completed" in combined
