"""Unit tests for KestraInternalDecorator.

The decorator is injected by the kestra plugin onto every step at compile
time and runs inside the Kestra container. It records kestra metadata
in metaflow's metadata store and writes per-task output to a sidecar
JSON file that the Kestra orchestration layer reads.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

# Importing `metaflow` first triggers full plugin resolution. Importing the
# kestra decorator module *during* metaflow's plugin scan hits a chicken-and-egg
# (the module is mid-import when the loader looks for the class). Forcing the
# scan to complete before we touch the decorator module sidesteps that.
import metaflow  # noqa: F401
import pytest

from metaflow_extensions.kestra.plugins.kestra.kestra_decorator import (  # noqa: E402
    KestraInternalDecorator,
)

# ---------------------------------------------------------------------------
# task_pre_step — registers Kestra execution metadata
# ---------------------------------------------------------------------------


class TestTaskPreStepMetadata:
    """task_pre_step should register MetaDatum entries for each Kestra-related
    env var present, and skip register_metadata entirely when none are set."""

    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        for v in (
            "METAFLOW_KESTRA_EXECUTION_ID",
            "METAFLOW_KESTRA_NAMESPACE",
            "METAFLOW_KESTRA_FLOW_ID",
        ):
            monkeypatch.delenv(v, raising=False)

    def _call(self, metadata, retry_count=0):
        deco = KestraInternalDecorator()
        deco.task_pre_step(
            step_name="start",
            task_datastore=MagicMock(),
            metadata=metadata,
            run_id="run-1",
            task_id="task-1",
            flow=MagicMock(),
            graph=MagicMock(),
            retry_count=retry_count,
            max_user_code_retries=0,
            ubf_context=None,
            inputs=None,
        )

    def test_registers_all_three_entries_when_all_env_vars_set(self, monkeypatch):
        monkeypatch.setenv("METAFLOW_KESTRA_EXECUTION_ID", "exec-abc")
        monkeypatch.setenv("METAFLOW_KESTRA_NAMESPACE", "metaflow")
        monkeypatch.setenv("METAFLOW_KESTRA_FLOW_ID", "myflow")
        metadata = MagicMock()
        self._call(metadata, retry_count=2)

        metadata.register_metadata.assert_called_once()
        args, _ = metadata.register_metadata.call_args
        run_id, step_name, task_id, entries = args
        assert run_id == "run-1"
        assert step_name == "start"
        assert task_id == "task-1"
        # Three MetaDatum entries, one per env var.
        fields = sorted(e.field for e in entries)
        assert fields == [
            "kestra-execution-id",
            "kestra-flow-id",
            "kestra-namespace",
        ]
        # All entries should carry the retry-count tag.
        for e in entries:
            assert "attempt_id:2" in e.tags

    def test_registers_only_present_env_vars(self, monkeypatch):
        monkeypatch.setenv("METAFLOW_KESTRA_EXECUTION_ID", "exec-only")
        # Namespace and flow id intentionally not set.
        metadata = MagicMock()
        self._call(metadata)

        args, _ = metadata.register_metadata.call_args
        entries = args[3]
        fields = [e.field for e in entries]
        assert fields == ["kestra-execution-id"]
        assert entries[0].value == "exec-only"

    def test_no_register_call_when_no_env_vars_set(self):
        metadata = MagicMock()
        self._call(metadata)

        metadata.register_metadata.assert_not_called()


# ---------------------------------------------------------------------------
# task_finished — writes sidecar JSON with task outcome + foreach/branch info
# ---------------------------------------------------------------------------


class TestTaskFinishedSidecar:
    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        monkeypatch.delenv("METAFLOW_KESTRA_OUTPUT_FILE", raising=False)

    @staticmethod
    def _flow(foreach_num_splits=None, transition=None):
        flow = MagicMock()
        if foreach_num_splits is not None:
            flow._foreach_num_splits = foreach_num_splits
        else:
            # avoid MagicMock auto-attr: keep getattr fallback well-behaved
            flow.spec_set = []
        flow._transition = transition
        return flow

    @staticmethod
    def _graph(node_type):
        node = MagicMock()
        node.type = node_type
        graph = MagicMock()
        graph.__getitem__.return_value = node
        return graph

    def _call(self, flow, graph, *, is_task_ok, output_file=None, monkeypatch=None):
        if output_file is not None:
            monkeypatch.setenv("METAFLOW_KESTRA_OUTPUT_FILE", output_file)
        deco = KestraInternalDecorator()
        deco.task_finished(
            step_name="any",
            flow=flow,
            graph=graph,
            is_task_ok=is_task_ok,
            retry_count=0,
            max_user_code_retries=0,
        )

    def test_writes_task_ok_for_linear_step(self, tmp_path, monkeypatch):
        out = tmp_path / "out.json"
        self._call(
            self._flow(),
            self._graph(node_type="linear"),
            is_task_ok=True,
            output_file=str(out),
            monkeypatch=monkeypatch,
        )
        data = json.loads(out.read_text())
        assert data == {"task_ok": True}

    def test_writes_foreach_cardinality(self, tmp_path, monkeypatch):
        out = tmp_path / "out.json"
        self._call(
            self._flow(foreach_num_splits=4),
            self._graph(node_type="foreach"),
            is_task_ok=True,
            output_file=str(out),
            monkeypatch=monkeypatch,
        )
        data = json.loads(out.read_text())
        assert data == {"task_ok": True, "foreach_cardinality": 4}

    def test_writes_branch_taken_for_split_switch(self, tmp_path, monkeypatch):
        out = tmp_path / "out.json"
        # transition is metaflow's `(["branch_name"], ...)` tuple shape.
        self._call(
            self._flow(transition=(["high"],)),
            self._graph(node_type="split-switch"),
            is_task_ok=True,
            output_file=str(out),
            monkeypatch=monkeypatch,
        )
        data = json.loads(out.read_text())
        assert data == {"task_ok": True, "branch_taken": "high"}

    def test_no_output_file_no_write(self, monkeypatch):
        # Without METAFLOW_KESTRA_OUTPUT_FILE the decorator silently drops
        # the sidecar — used when the step runs outside Kestra orchestration.
        self._call(
            self._flow(),
            self._graph(node_type="linear"),
            is_task_ok=False,
        )
        # No assertion target — the only thing to verify is that no exception
        # bubbles up. Reaching this line means the no-write path is exercised.

    def test_swallows_oserror_on_write(self, tmp_path, monkeypatch):
        # Output file pointing at a non-existent directory → OSError.
        # The decorator must NOT raise (it's running inside metaflow's
        # task_finished hook, an exception would mark the task failed).
        self._call(
            self._flow(),
            self._graph(node_type="linear"),
            is_task_ok=True,
            output_file=str(tmp_path / "no_such_dir" / "out.json"),
            monkeypatch=monkeypatch,
        )
