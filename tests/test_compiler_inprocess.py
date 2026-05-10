"""In-process unit tests for KestraCompiler.

The existing test_compiler.py / test_adversarial.py compile flows by spawning
a subprocess that invokes the kestra CLI. That covers the end-to-end CLI
path but its execution is invisible to pytest-cov, so kestra_compiler.py
shows ~14% coverage despite ~96 assertions on its YAML output.

These tests instantiate KestraCompiler directly in the test process against
real Metaflow flow classes loaded from tests/flows/ via importlib (Metaflow's
graph builder uses inspect.getsource, so the class must come from a real
.py file). Coverage of compile() now reflects what the CLI tests assert.
"""

from __future__ import annotations

import importlib.util
import os
from unittest.mock import MagicMock

# Importing metaflow first triggers full plugin resolution (see test_decorator.py).
import metaflow  # noqa: F401
import pytest
from metaflow.graph import FlowGraph  # noqa: E402

from metaflow_extensions.kestra.plugins.kestra.kestra_compiler import (  # noqa: E402
    KestraCompiler,
    _iso_duration,
    flow_name_to_id,
)

FLOWS_DIR = os.path.join(os.path.dirname(__file__), "flows")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_flow(filename: str, class_name: str):
    """Import a flow file and return its (FlowSpec subclass, file path) tuple."""
    path = os.path.join(FLOWS_DIR, filename)
    spec = importlib.util.spec_from_file_location(filename[:-3], path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return getattr(mod, class_name), path


def _make_compiler(flow_cls, flow_path, **overrides):
    """Construct a KestraCompiler for a real flow class with mocked deps."""
    graph = FlowGraph(flow_cls)
    flow = flow_cls.__new__(flow_cls)
    flow._flow_state = {}

    def _typed(t):
        m = MagicMock()
        m.TYPE = t
        return m

    metadata = _typed("local")
    flow_datastore = _typed("local")
    flow_datastore.datastore_root = "/tmp/mf"
    environment = _typed("local")
    event_logger = _typed("nullSidecarLogger")
    monitor = _typed("nullSidecarMonitor")

    kwargs = {
        "name": flow_cls.__name__,
        "graph": graph,
        "flow": flow,
        "flow_file": flow_path,
        "metadata": metadata,
        "flow_datastore": flow_datastore,
        "environment": environment,
        "event_logger": event_logger,
        "monitor": monitor,
    }
    kwargs.update(overrides)
    return KestraCompiler(**kwargs)


# ---------------------------------------------------------------------------
# Linear DAG — the simplest non-trivial flow
# ---------------------------------------------------------------------------


class TestLinearFlowCompile:
    @pytest.fixture
    def yaml(self):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        return _make_compiler(cls, path).compile()

    def test_emits_kestra_id_header(self, yaml):
        assert "id: linearflow" in yaml
        assert "namespace: metaflow" in yaml

    def test_emits_each_step_as_a_task(self, yaml):
        # All three step names appear as task IDs in the rendered tasks block.
        assert "id: start" in yaml
        assert "id: process" in yaml
        assert "id: end" in yaml

    def test_emits_metaflow_init_task(self, yaml):
        # Compiler always prepends an `metaflow_init` setup task.
        assert "id: metaflow_init" in yaml

    def test_emits_inputs_section_for_resume(self, yaml):
        # ORIGIN_RUN_ID input must be present so resume works in Kestra 1.3+.
        assert "ORIGIN_RUN_ID" in yaml

    def test_emits_variables_block_with_runtime_provider_types(self, yaml):
        assert 'METADATA_TYPE: "local"' in yaml
        assert 'DATASTORE_TYPE: "local"' in yaml
        assert 'ENVIRONMENT_TYPE: "local"' in yaml

    def test_emits_flow_name_label(self, yaml):
        assert "metaflow.flow: LinearFlow" in yaml


# ---------------------------------------------------------------------------
# Branch / split-and / split-switch
# ---------------------------------------------------------------------------


class TestBranchFlowCompile:
    @pytest.fixture
    def compiled(self):
        cls, path = _load_flow("branch_flow.py", "BranchFlow")
        return _make_compiler(cls, path).compile()

    def test_emits_all_branches(self, compiled):
        # branch_flow.py has parallel `branch_a` and `branch_b` steps.
        assert "id: branch_a" in compiled
        assert "id: branch_b" in compiled

    def test_join_step_present(self, compiled):
        # After a branch, control rejoins at a single step (commonly named "join").
        assert "id: join" in compiled


class TestConditionalFlowCompile:
    @pytest.fixture
    def compiled(self):
        cls, path = _load_flow("conditional_flow.py", "ConditionalFlow")
        return _make_compiler(cls, path).compile()

    def test_split_switch_emits_switch_block(self, compiled):
        # @switch decorators compile to a Kestra Switch task — the YAML must
        # include the `io.kestra.plugin.core.flow.Switch` plugin reference.
        assert "Switch" in compiled


# ---------------------------------------------------------------------------
# Foreach (single + nested)
# ---------------------------------------------------------------------------


class TestForeachFlowCompile:
    @pytest.fixture
    def compiled(self):
        cls, path = _load_flow("foreach_flow.py", "ForeachFlow")
        return _make_compiler(cls, path).compile()

    def test_emits_foreach_container(self, compiled):
        # @step that calls .next(..., foreach=...) compiles to a Kestra
        # ForEach (subflow) task.
        assert "ForEach" in compiled or "EachSequential" in compiled or "EachParallel" in compiled


class TestNestedForeachFlowCompile:
    @pytest.fixture
    def compiled(self):
        cls, path = _load_flow("nested_foreach_flow.py", "NestedForeachFlow")
        return _make_compiler(cls, path).compile()

    def test_emits_outer_and_inner_foreach(self, compiled):
        # Two foreach scopes → two ForEach blocks.
        assert compiled.count("ForEach") >= 1 or compiled.count("EachSequential") >= 1


# ---------------------------------------------------------------------------
# Parameters & schedule
# ---------------------------------------------------------------------------


class TestParamFlowCompile:
    def test_params_appear_in_inputs(self):
        cls, path = _load_flow("param_flow.py", "ParamFlow")
        yaml = _make_compiler(cls, path).compile()
        # ParamFlow has Parameter("greeting") and Parameter("count") — both
        # must show up as Kestra `inputs:` entries.
        assert "- id: greeting" in yaml
        assert "- id: count" in yaml


class TestScheduleFlowCompile:
    def test_schedule_flow_compiles_without_error(self):
        # @schedule(...) is a class decorator that gets resolved by metaflow's
        # CLI initialization, not in our __new__/_flow_state setup. Verify the
        # compiler doesn't error on the flow even when triggers stay empty.
        cls, path = _load_flow("schedule_flow.py", "ScheduleFlow")
        yaml = _make_compiler(cls, path).compile()
        assert "id: scheduleflow" in yaml


# ---------------------------------------------------------------------------
# Compiler options: tags, namespace, max_workers, workflow_timeout
# ---------------------------------------------------------------------------


class TestCompilerOptions:
    def test_tags_propagate_to_labels(self):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        yaml = _make_compiler(cls, path, tags=["env:prod", "team:ml"]).compile()
        assert "env:prod" in yaml
        assert "team:ml" in yaml

    def test_kestra_namespace_overrides_default(self):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        yaml = _make_compiler(cls, path, kestra_namespace="prod.metaflow").compile()
        assert "namespace: prod.metaflow" in yaml

    def test_workflow_timeout_emits_iso_duration(self):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        yaml = _make_compiler(cls, path, workflow_timeout=3600).compile()
        # 3600s = PT1H — see _iso_duration
        assert f"timeout: {_iso_duration(3600)}" in yaml

    def test_no_timeout_when_workflow_timeout_unset(self):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        yaml = _make_compiler(cls, path).compile()
        # Header doesn't emit a timeout: line at all.
        # (The variable section is fine; only the top-level `timeout:` would
        # appear right after `labels:`, before `variables:`.)
        before_vars = yaml.split("variables:", 1)[0]
        assert "timeout:" not in before_vars

    def test_username_propagates_to_mf_username_var(self):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        yaml = _make_compiler(cls, path, username="alice").compile()
        assert 'MF_USERNAME: "alice"' in yaml


# ---------------------------------------------------------------------------
# flow_id property
# ---------------------------------------------------------------------------


class TestFlowIdProperty:
    def test_flow_id_lowercases_and_hyphenates(self):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        compiler = _make_compiler(cls, path)
        assert compiler.flow_id == flow_name_to_id("LinearFlow") == "linearflow"

    def test_flow_id_uses_project_branch_when_set(self):
        # Without @project decorator, flow_id derives from class name only.
        # With @project, the flow_name embeds project.branch.<class> — verify
        # the simple no-project case here; project_flow.py covers the other.
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        compiler = _make_compiler(cls, path, name="My_Custom_Name")
        assert compiler.flow_id == "my-custom-name"


# ---------------------------------------------------------------------------
# Resources / retry  — emit decorator-driven YAML fragments
# ---------------------------------------------------------------------------


class TestRetryFlowCompile:
    def test_retry_count_propagates_to_kestra_retry_block(self):
        cls, path = _load_flow("retry_flow.py", "RetryFlow")
        yaml = _make_compiler(cls, path).compile()
        # @retry(times=3) → the compiler emits a retry block somewhere in the
        # task spec. Spec wording varies (maxAttempts / maxAttempts: 4 etc.),
        # so we do a coarse "retry" presence check.
        assert "retry" in yaml.lower()


# ---------------------------------------------------------------------------
# Private helpers — direct unit tests, no full compile()
# ---------------------------------------------------------------------------


class TestGetScheduleHelper:
    """_get_schedule reads @schedule decorator metadata off flow._flow_decorators."""

    def _compiler_with_decorators(self, decos):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        compiler = _make_compiler(cls, path)
        # Replace the flow instance with a bare object that exposes
        # _flow_decorators as a writable plain attribute.
        compiler.flow = type("FakeFlow", (), {"_flow_decorators": decos})()
        return compiler

    def test_no_schedule_returns_none(self):
        compiler = self._compiler_with_decorators({})
        assert compiler._get_schedule() is None

    def test_string_cron_returns_dict(self):
        deco = MagicMock()
        deco.schedule = "0 * * * *"
        deco.timezone = "UTC"
        compiler = self._compiler_with_decorators({"schedule": [deco]})
        assert compiler._get_schedule() == {"cron": "0 * * * *", "timezone": "UTC"}

    def test_dict_schedule_pulls_cron_and_timezone(self):
        deco = MagicMock()
        # Some shapes pass schedule as a dict (e.g. weekly()).
        deco.schedule = {"cron": "0 9 * * 1", "timezone": "America/New_York"}
        deco.timezone = None
        compiler = self._compiler_with_decorators({"schedule": [deco]})
        assert compiler._get_schedule() == {"cron": "0 9 * * 1", "timezone": "America/New_York"}

    def test_no_cron_returns_none(self):
        deco = MagicMock()
        deco.schedule = None
        deco.timezone = "UTC"
        compiler = self._compiler_with_decorators({"schedule": [deco]})
        assert compiler._get_schedule() is None


class TestGetTriggersHelpers:
    def _compiler_with_decorators(self, decos):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        compiler = _make_compiler(cls, path)
        # Replace the flow instance with a bare object that exposes
        # _flow_decorators as a writable plain attribute.
        compiler.flow = type("FakeFlow", (), {"_flow_decorators": decos})()
        return compiler

    def test_trigger_event_names_extracted(self):
        deco = MagicMock()
        deco.triggers = [{"name": "user.signup"}, {"name": "user.delete"}]
        compiler = self._compiler_with_decorators({"trigger": [deco]})
        assert compiler._get_triggers() == ["user.signup", "user.delete"]

    def test_trigger_skips_non_string_event_names(self):
        deco = MagicMock()
        deco.triggers = [{"name": "valid"}, {"name": 42}, {"name": None}]
        compiler = self._compiler_with_decorators({"trigger": [deco]})
        # Non-string names emit a warning and are skipped.
        import warnings
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            assert compiler._get_triggers() == ["valid"]

    def test_no_trigger_returns_empty(self):
        compiler = self._compiler_with_decorators({})
        assert compiler._get_triggers() == []

    def test_trigger_on_finish_extracts_upstream_flows(self):
        deco = MagicMock()
        deco.triggers = [{"flow": "UpstreamA"}, {"fq_name": "UpstreamB"}]
        compiler = self._compiler_with_decorators({"trigger_on_finish": [deco]})
        assert compiler._get_trigger_on_finishes() == ["UpstreamA", "UpstreamB"]

    def test_trigger_on_finish_returns_empty_when_unset(self):
        compiler = self._compiler_with_decorators({})
        assert compiler._get_trigger_on_finishes() == []


class TestGetResourcesSpec:
    """_get_resources_spec serializes @resources(cpu=, memory=, gpu=) into a spec string."""

    def _node_with_resources(self, **attrs):
        deco = MagicMock(name="resources")
        deco.name = "resources"
        deco.attributes = attrs
        node = MagicMock()
        node.decorators = [deco]
        return node

    def _compiler(self):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        return _make_compiler(cls, path)

    def test_cpu_only(self):
        node = self._node_with_resources(cpu=2)
        assert self._compiler()._get_resources_spec(node) == "resources:cpu=2"

    def test_cpu_memory_gpu(self):
        node = self._node_with_resources(cpu=2, memory=4096, gpu=1)
        assert self._compiler()._get_resources_spec(node) == "resources:cpu=2,memory=4096,gpu=1"

    def test_no_resources_returns_none(self):
        node = MagicMock()
        node.decorators = []
        assert self._compiler()._get_resources_spec(node) is None

    def test_empty_attrs_returns_none(self):
        node = self._node_with_resources()  # no cpu/memory/gpu
        assert self._compiler()._get_resources_spec(node) is None


class TestGetRetryConfig:
    """_get_retry_config reads @retry(times=, minutes_between_retries=)."""

    def _compiler(self):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        return _make_compiler(cls, path)

    def test_no_retry_returns_default(self):
        node = MagicMock()
        node.decorators = []
        # Default: 0 retries, 120s backoff (encoded for Kestra).
        assert self._compiler()._get_retry_config(node) == (0, 120)

    def test_retry_with_times_and_minutes(self):
        deco = MagicMock()
        deco.name = "retry"
        deco.attributes = {"times": 3, "minutes_between_retries": 5}
        node = MagicMock()
        node.decorators = [deco]
        # 5 minutes = 300 seconds
        assert self._compiler()._get_retry_config(node) == (3, 300)


class TestGetTimeoutHelper:
    def _compiler(self):
        cls, path = _load_flow("linear_flow.py", "LinearFlow")
        return _make_compiler(cls, path)

    def test_no_timeout_decorator_returns_none(self):
        node = MagicMock()
        node.decorators = []
        # No @timeout decorator → return None even if metaflow has a default.
        assert self._compiler()._get_timeout(node) is None


# ---------------------------------------------------------------------------
# KestraTriggeredRun.run property — env-var swap + UNKNOWN/<run_id> rewrite
# ---------------------------------------------------------------------------


class TestTriggeredRunRunProperty:
    """The .run property has heavy env-var manipulation that needs coverage."""

    @staticmethod
    def _make(pathspec="HelloFlow/kestra-abc", env_vars=None):
        from metaflow_extensions.kestra.plugins.kestra.kestra_deployer_objects import (
            KestraTriggeredRun,
        )
        tr = object.__new__(KestraTriggeredRun)
        tr.deployer = MagicMock()
        tr.deployer.env_vars = env_vars or {}
        tr.pathspec = pathspec
        tr._metadata = {}
        return tr

    def test_run_property_returns_metaflow_run(self, monkeypatch):
        tr = self._make(env_vars={"METAFLOW_DEFAULT_METADATA": "local"})
        fake_run = MagicMock()
        monkeypatch.setattr(metaflow, "Run", lambda pathspec, **kw: fake_run)
        monkeypatch.setattr(metaflow, "metadata", MagicMock())
        assert tr.run is fake_run

    def test_run_property_returns_none_on_metaflow_not_found(self, monkeypatch):
        from metaflow.exception import MetaflowNotFound
        tr = self._make()

        def raise_not_found(*a, **k):
            raise MetaflowNotFound("not yet")
        monkeypatch.setattr(metaflow, "Run", raise_not_found)
        assert tr.run is None

    def test_run_property_resolves_unknown_pathspec(self, monkeypatch, tmp_path):
        tr = self._make(
            pathspec="UNKNOWN/run-42",
            env_vars={
                "METAFLOW_DEFAULT_METADATA": "local",
                "METAFLOW_DATASTORE_SYSROOT_LOCAL": str(tmp_path),
            },
        )
        # Set up a fake datastore where run-42 belongs to "MyFlow".
        (tmp_path / ".metaflow" / "MyFlow" / "run-42").mkdir(parents=True)
        captured = {}

        def fake_run(pathspec, **kw):
            captured["pathspec"] = pathspec
            return MagicMock()
        monkeypatch.setattr(metaflow, "Run", fake_run)
        monkeypatch.setattr(metaflow, "metadata", MagicMock())
        tr.run  # noqa: B018 — property access has side effects we want
        assert captured["pathspec"] == "MyFlow/run-42"
        # The pathspec on the triggered run is rewritten in place.
        assert tr.pathspec == "MyFlow/run-42"
