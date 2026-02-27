"""
Unit tests for the Kestra YAML compiler.

These tests run entirely offline — no Kestra server is required.
They verify that the generated YAML is syntactically correct and contains
the expected structure for each graph type.
"""

import os
import subprocess
import sys
import tempfile

import pytest

from conftest import FLOWS_DIR, compile_flow

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_yaml(path: str) -> dict:
    """Parse a YAML file. Requires pyyaml."""
    try:
        import yaml
    except ImportError:
        pytest.skip("pyyaml not installed — skipping YAML parse checks")
    with open(path) as f:
        return yaml.safe_load(f)


def _task_ids(yaml_dict: dict) -> list:
    """Recursively collect all task IDs from a Kestra flow YAML dict."""
    ids = []

    def _collect(tasks):
        for t in tasks or []:
            ids.append(t.get("id", ""))
            # Recurse into nested tasks (Parallel, ForEach)
            for subtask_key in ("tasks",):
                nested = t.get(subtask_key, [])
                if nested:
                    _collect(nested)

    _collect(yaml_dict.get("tasks", []))
    return ids


# ---------------------------------------------------------------------------
# Linear flow
# ---------------------------------------------------------------------------

class TestLinearFlow:
    def test_create_yaml(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "linear.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "LinearFlow" in yaml_str or "linearflow" in yaml_str.lower()
        assert "metaflow_init" in yaml_str
        assert "start" in yaml_str
        assert "process" in yaml_str
        assert "end" in yaml_str

    def test_yaml_valid(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "linear.yaml")
        compile_flow(flow_file, out)
        data = _load_yaml(out)
        assert data["namespace"] == "metaflow"
        ids = _task_ids(data)
        assert "metaflow_init" in ids
        assert "start" in ids
        assert "process" in ids
        assert "end" in ids

    def test_no_params_task(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "linear.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "metaflow_params" not in yaml_str

    def test_step_references_init(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "linear.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "outputs.metaflow_init.vars.run_id" in yaml_str

    def test_artifact_hint_present(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "linear.yaml")
        yaml_str = compile_flow(flow_file, out)
        # Artifact hint code should be embedded
        assert "metaflow_snippet" in yaml_str or "metaflow_artifacts" in yaml_str


# ---------------------------------------------------------------------------
# Param flow
# ---------------------------------------------------------------------------

class TestParamFlow:
    def test_inputs_section(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "param_flow.py")
        out = str(tmp_path / "param.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "inputs:" in yaml_str
        assert "greeting" in yaml_str
        assert "count" in yaml_str

    def test_params_task_present(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "param_flow.py")
        out = str(tmp_path / "param.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "metaflow_params" in yaml_str

    def test_kestra_input_refs(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "param_flow.py")
        out = str(tmp_path / "param.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "inputs.greeting" in yaml_str
        assert "inputs.count" in yaml_str

    def test_yaml_structure(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "param_flow.py")
        out = str(tmp_path / "param.yaml")
        compile_flow(flow_file, out)
        data = _load_yaml(out)
        input_ids = [i["id"] for i in data.get("inputs", [])]
        assert "greeting" in input_ids
        assert "count" in input_ids
        ids = _task_ids(data)
        assert "metaflow_params" in ids


# ---------------------------------------------------------------------------
# Branch flow (split/join)
# ---------------------------------------------------------------------------

class TestBranchFlow:
    def test_parallel_task(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "branch_flow.py")
        out = str(tmp_path / "branch.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "io.kestra.plugin.core.flow.Parallel" in yaml_str

    def test_branch_tasks_present(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "branch_flow.py")
        out = str(tmp_path / "branch.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "branch_a" in yaml_str
        assert "branch_b" in yaml_str
        assert "join" in yaml_str

    def test_join_input_paths(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "branch_flow.py")
        out = str(tmp_path / "branch.yaml")
        yaml_str = compile_flow(flow_file, out)
        # Join must reference both branch outputs
        assert "outputs.branch_a.vars.input_path" in yaml_str
        assert "outputs.branch_b.vars.input_path" in yaml_str

    def test_yaml_valid(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "branch_flow.py")
        out = str(tmp_path / "branch.yaml")
        compile_flow(flow_file, out)
        data = _load_yaml(out)
        assert data is not None


# ---------------------------------------------------------------------------
# Foreach flow
# ---------------------------------------------------------------------------

class TestForeachFlow:
    def test_foreach_task(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "foreach_flow.py")
        out = str(tmp_path / "foreach.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "io.kestra.plugin.core.flow.ForEach" in yaml_str

    def test_foreach_values_ref(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "foreach_flow.py")
        out = str(tmp_path / "foreach.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "foreach_values" in yaml_str

    def test_body_task_inside_foreach(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "foreach_flow.py")
        out = str(tmp_path / "foreach.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "process" in yaml_str
        assert "taskrun.value" in yaml_str

    def test_join_uses_foreach_count(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "foreach_flow.py")
        out = str(tmp_path / "foreach.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "foreach_count" in yaml_str

    def test_yaml_valid(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "foreach_flow.py")
        out = str(tmp_path / "foreach.yaml")
        compile_flow(flow_file, out)
        data = _load_yaml(out)
        assert data is not None


# ---------------------------------------------------------------------------
# Retry / timeout flow
# ---------------------------------------------------------------------------

class TestRetryFlow:
    def test_retry_present(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "retry_flow.py")
        out = str(tmp_path / "retry.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "maxAttempt" in yaml_str

    def test_timeout_present(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "retry_flow.py")
        out = str(tmp_path / "retry.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "timeout" in yaml_str


# ---------------------------------------------------------------------------
# Schedule flow
# ---------------------------------------------------------------------------

class TestScheduleFlow:
    def test_schedule_trigger(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "schedule_flow.py")
        out = str(tmp_path / "schedule.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "triggers:" in yaml_str
        assert "io.kestra.plugin.core.trigger.Schedule" in yaml_str
        assert "0 * * * *" in yaml_str


# ---------------------------------------------------------------------------
# Project flow
# ---------------------------------------------------------------------------

class TestProjectFlow:
    def test_project_label(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "project_flow.py")
        out = str(tmp_path / "project.yaml")
        yaml_str = compile_flow(flow_file, out)
        assert "myteam" in yaml_str

    def test_project_in_flow_name(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "project_flow.py")
        out = str(tmp_path / "project.yaml")
        yaml_str = compile_flow(flow_file, out)
        # @project qualifies the flow name
        assert "myteam" in yaml_str.lower() or "projectflow" in yaml_str.lower()


# ---------------------------------------------------------------------------
# --with option
# ---------------------------------------------------------------------------

class TestWithDecorators:
    def test_with_in_step_command(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "with.yaml")
        yaml_str = compile_flow(flow_file, out, extra_args=["--with=sandbox"])
        assert "--with=sandbox" in yaml_str

    def test_multiple_with(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "with_multi.yaml")
        yaml_str = compile_flow(
            flow_file, out, extra_args=["--with=sandbox", "--with=resources:cpu=4"]
        )
        assert "--with=sandbox" in yaml_str
        assert "--with=resources:cpu=4" in yaml_str


# ---------------------------------------------------------------------------
# Custom kestra-namespace
# ---------------------------------------------------------------------------

class TestKestraNamespace:
    def test_custom_namespace(self, tmp_path):
        flow_file = os.path.join(FLOWS_DIR, "linear_flow.py")
        out = str(tmp_path / "ns.yaml")
        yaml_str = compile_flow(
            flow_file, out, extra_args=["--kestra-namespace=company.team"]
        )
        assert "namespace: company.team" in yaml_str
