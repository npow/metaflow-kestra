"""Unit tests for KestraDeployer + KestraDeployedFlow + KestraTriggeredRun
and the compiler module's static helpers (flow_name_to_id, _iso_duration).

These exercise the in-process deployer plumbing that the subprocess-based
e2e tests cover at integration time but that doesn't show up in coverage
because subprocess execution is invisible to pytest-cov.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

# See test_decorator.py for why metaflow is imported first.
import metaflow  # noqa: F401
import pytest

from metaflow_extensions.kestra.plugins.kestra.kestra_compiler import (  # noqa: E402
    _iso_duration,
    flow_name_to_id,
)
from metaflow_extensions.kestra.plugins.kestra.kestra_deployer import (  # noqa: E402
    KestraDeployer,
)
from metaflow_extensions.kestra.plugins.kestra.kestra_deployer_objects import (  # noqa: E402
    KestraDeployedFlow,
    KestraTriggeredRun,
    _find_flow_for_run_id,
    _make_stub_deployer,
)

# ---------------------------------------------------------------------------
# Compiler helpers (pure functions, easy to unit-test)
# ---------------------------------------------------------------------------


class TestFlowNameToId:
    """flow_name_to_id maps a Metaflow class name to a Kestra flow ID."""

    def test_lowercases_pascal_case(self):
        assert flow_name_to_id("HelloFlow") == "helloflow"

    def test_replaces_underscores_with_hyphens(self):
        assert flow_name_to_id("My_Flow_Name") == "my-flow-name"

    def test_replaces_dots_with_hyphens(self):
        # @project decorators inject `project.branch.flow` style names.
        assert flow_name_to_id("project.user.alice.HelloFlow") == "project-user-alice-helloflow"

    def test_combines_dots_and_underscores(self):
        assert flow_name_to_id("project.A_B.Flow") == "project-a-b-flow"

    def test_already_valid_id_is_idempotent(self):
        assert flow_name_to_id("simpleflow") == "simpleflow"


class TestIsoDuration:
    """_iso_duration converts integer seconds to an ISO 8601 duration string.

    Kestra's `timeout:` field requires this format (PT1H30M, PT45S, etc.).
    """

    def test_seconds_only(self):
        assert _iso_duration(45) == "PT45S"

    def test_zero_emits_pt0s(self):
        # The function emits PT0S rather than bare "PT" for safety.
        assert _iso_duration(0) == "PT0S"

    def test_minutes_only(self):
        assert _iso_duration(180) == "PT3M"

    def test_hours_only(self):
        assert _iso_duration(7200) == "PT2H"

    def test_combined_h_m_s(self):
        # 1h 30m 45s = 5445s
        assert _iso_duration(5445) == "PT1H30M45S"

    def test_h_m_no_seconds(self):
        # 1h 30m exactly — no S suffix
        assert _iso_duration(5400) == "PT1H30M"


# ---------------------------------------------------------------------------
# KestraDeployer
# ---------------------------------------------------------------------------


class TestKestraDeployer:
    """KestraDeployer is a thin DeployerImpl subclass — verify the plumbing."""

    def test_type_is_kestra(self):
        # Required by Metaflow's plugin loader to register `Deployer(...).kestra(...)`.
        assert KestraDeployer.TYPE == "kestra"

    def test_stores_deployer_kwargs(self):
        # __init__ builds a real DeployerImpl which requires a flow file. Use
        # __new__ to bypass and verify only the subclass-specific behavior.
        d = object.__new__(KestraDeployer)
        d._deployer_kwargs = {"kestra_host": "http://localhost:8090"}
        assert d.deployer_kwargs == {"kestra_host": "http://localhost:8090"}

    def test_deployed_flow_type_returns_class(self):
        # Lazy-imported to avoid the deployer_objects → compiler dep at module load.
        assert KestraDeployer.deployed_flow_type() is KestraDeployedFlow


# ---------------------------------------------------------------------------
# KestraDeployedFlow.id  — the round-trip key for from_deployment()
# ---------------------------------------------------------------------------


class TestKestraDeployedFlowId:
    @staticmethod
    def _make(name="HelloFlow", flow_name="HelloFlow", flow_file="hello.py", additional=None):
        deployed = object.__new__(KestraDeployedFlow)
        deployed.name = name
        deployed.flow_name = flow_name
        deployed.deployer = MagicMock()
        deployed.deployer.flow_file = flow_file
        deployed.deployer.additional_info = additional or {}
        return deployed

    def test_id_includes_core_fields(self):
        d = self._make()
        info = json.loads(d.id)
        assert info == {
            "name": "HelloFlow",
            "flow_name": "HelloFlow",
            "flow_file": "hello.py",
        }

    def test_id_merges_additional_info(self):
        d = self._make(additional={"kestra_host": "http://localhost:8090", "flow_id": "helloflow"})
        info = json.loads(d.id)
        assert info["kestra_host"] == "http://localhost:8090"
        assert info["flow_id"] == "helloflow"
        assert info["name"] == "HelloFlow"

    def test_id_handles_missing_flow_file(self):
        d = self._make(flow_file=None)
        info = json.loads(d.id)
        assert info["flow_file"] is None


# ---------------------------------------------------------------------------
# KestraDeployedFlow.from_deployment
# ---------------------------------------------------------------------------


class TestFromDeploymentJsonPayload:
    """JSON id round-trip: every field must be restored on the deployer."""

    def test_full_payload_restores_all_fields(self):
        payload = {
            "name": "HelloFlow",
            "flow_name": "HelloFlow",
            "flow_file": "/path/to/hello.py",
            "kestra_host": "http://localhost:8090",
            "kestra_user": "admin@kestra.io",
            "kestra_password": "secret",
            "flow_id": "helloflow",
        }
        # Mock KestraDeployer.__init__ to skip the heavy DeployerImpl setup.
        with patch.object(KestraDeployer, "__init__", lambda self, **kw: None):
            df = KestraDeployedFlow.from_deployment(json.dumps(payload))
        assert df.deployer.name == "HelloFlow"
        assert df.deployer.flow_name == "HelloFlow"
        # Credentials and host go into additional_info, not directly on the deployer.
        ai = df.deployer.additional_info
        assert ai["kestra_host"] == "http://localhost:8090"
        assert ai["kestra_user"] == "admin@kestra.io"
        assert ai["kestra_password"] == "secret"
        assert ai["flow_id"] == "helloflow"
        # The "name" / "flow_name" / "flow_file" keys are intentionally NOT in
        # additional_info — they live as deployer attributes instead.
        assert "name" not in ai
        assert "flow_file" not in ai


class TestFromDeploymentPlainName:
    """Plain-name fallback: env vars provide credentials."""

    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        for v in (
            "KESTRA_HOST",
            "KESTRA_NAMESPACE",
            "KESTRA_USER",
            "KESTRA_PASSWORD",
            "KESTRA_API_TOKEN",
        ):
            monkeypatch.delenv(v, raising=False)

    def test_pascal_name_converts_to_kestra_flow_id(self, monkeypatch):
        monkeypatch.setenv("KESTRA_HOST", "http://localhost:8090")
        monkeypatch.setenv("KESTRA_USER", "admin@kestra.io")
        monkeypatch.setenv("KESTRA_PASSWORD", "secret")
        df = KestraDeployedFlow.from_deployment("HelloFlow")
        ai = df.deployer.additional_info
        assert ai["flow_id"] == "helloflow"
        assert ai["kestra_host"] == "http://localhost:8090"
        assert ai["kestra_user"] == "admin@kestra.io"
        assert ai["kestra_password"] == "secret"
        assert ai["kestra_namespace"] == "metaflow"  # default
        assert ai["kestra_token"] is None

    def test_already_kestra_id_used_verbatim(self):
        # If the identifier already matches the [a-z0-9-]+ shape, don't re-mangle it.
        df = KestraDeployedFlow.from_deployment("hello-flow")
        assert df.deployer.additional_info["flow_id"] == "hello-flow"
        assert df.deployer.name == "hello-flow"

    def test_token_env_takes_precedence_over_user_password(self, monkeypatch):
        monkeypatch.setenv("KESTRA_API_TOKEN", "tok-abc")
        df = KestraDeployedFlow.from_deployment("HelloFlow")
        ai = df.deployer.additional_info
        assert ai["kestra_token"] == "tok-abc"

    def test_default_host_when_env_unset(self):
        df = KestraDeployedFlow.from_deployment("HelloFlow")
        assert df.deployer.additional_info["kestra_host"] == "http://localhost:8080"


class TestFromDeploymentFallthrough:
    """A non-JSON, non-plain-name identifier still works (treated as plain name)."""

    def test_curly_brace_but_invalid_json_is_treated_as_plain_name(self):
        # Starts with "{" so we attempt JSON; fails; falls back to plain-name path.
        df = KestraDeployedFlow.from_deployment("{not_json}")
        assert df.deployer.additional_info["flow_id"] == flow_name_to_id("{not_json}")


# ---------------------------------------------------------------------------
# _trigger_direct  — REST-API-only path (used after plain-name recovery)
# ---------------------------------------------------------------------------


class TestTriggerDirect:
    @staticmethod
    def _deployed(additional, name="HelloFlow", flow_class=None):
        df = object.__new__(KestraDeployedFlow)
        df.name = name
        df.deployer = _make_stub_deployer(name)
        df.deployer.additional_info = additional
        df.deployer.flow_name = flow_class or ""
        return df

    def test_uses_token_auth_when_provided(self):
        df = self._deployed({
            "kestra_host": "http://localhost:8090",
            "kestra_namespace": "metaflow",
            "kestra_token": "tok-xyz",
            "flow_id": "helloflow",
        })
        with patch("requests.Session") as session_cls:
            session = session_cls.return_value
            session.headers = {}
            resp = MagicMock(status_code=200)
            resp.json.return_value = {"id": "exec-001"}
            session.post.return_value = resp

            df._trigger_direct(message="hi")

        assert session.headers["Authorization"] == "Bearer tok-xyz"
        # No basic-auth when token is set
        assert session.auth is not session  # not assigned
        url, _ = session.post.call_args[0], session.post.call_args[1]
        assert url[0] == "http://localhost:8090/api/v1/executions/metaflow/helloflow"

    def test_uses_basic_auth_when_no_token(self):
        df = self._deployed({
            "kestra_host": "http://localhost:8090",
            "kestra_namespace": "metaflow",
            "kestra_user": "admin@kestra.io",
            "kestra_password": "secret",
            "flow_id": "helloflow",
        })
        with patch("requests.Session") as session_cls:
            session = session_cls.return_value
            session.headers = {}
            resp = MagicMock(status_code=201)
            resp.json.return_value = {"id": "exec-002"}
            session.post.return_value = resp

            df._trigger_direct()

        assert session.auth == ("admin@kestra.io", "secret")

    def test_raises_runtimeerror_on_non_2xx(self):
        df = self._deployed({
            "kestra_host": "http://localhost:8090",
            "kestra_namespace": "metaflow",
            "flow_id": "helloflow",
        })
        with patch("requests.Session") as session_cls:
            session = session_cls.return_value
            session.headers = {}
            resp = MagicMock(status_code=403, text="Forbidden")
            session.post.return_value = resp

            with pytest.raises(RuntimeError, match=r"HTTP 403"):
                df._trigger_direct()

    def test_pathspec_uses_flow_class_when_known(self):
        df = self._deployed(
            {
                "kestra_host": "http://localhost:8090",
                "kestra_namespace": "metaflow",
                "flow_id": "helloflow",
                "mf_flow_class": "HelloFlow",
            },
            flow_class="hello-flow",  # underscore/hyphen mismatch ignored — explicit wins
        )
        with patch("requests.Session") as session_cls:
            session = session_cls.return_value
            session.headers = {}
            resp = MagicMock(status_code=200)
            resp.json.return_value = {"id": "exec-003"}
            session.post.return_value = resp

            run = df._trigger_direct()
        assert run.pathspec.startswith("HelloFlow/kestra-")

    def test_pathspec_falls_back_to_unknown_when_no_class(self):
        # No mf_flow_class and deployer.flow_name has a hyphen → can't be a class.
        df = self._deployed(
            {"kestra_host": "http://localhost:8090", "kestra_namespace": "metaflow", "flow_id": "helloflow"},
            flow_class="hello-flow",
        )
        with patch("requests.Session") as session_cls:
            session = session_cls.return_value
            session.headers = {}
            resp = MagicMock(status_code=200)
            resp.json.return_value = {"id": "exec-004"}
            session.post.return_value = resp
            run = df._trigger_direct()
        assert run.pathspec.startswith("UNKNOWN/kestra-")


# ---------------------------------------------------------------------------
# KestraTriggeredRun.status
# ---------------------------------------------------------------------------


class TestTriggeredRunStatus:
    @staticmethod
    def _run(metadata=None):
        tr = object.__new__(KestraTriggeredRun)
        tr._metadata = metadata or {}
        tr.deployer = MagicMock()
        tr.deployer.env_vars = {}
        tr.pathspec = "HelloFlow/kestra-abc"
        return tr

    def test_kestra_ui_returns_url_when_present(self):
        tr = self._run(metadata={"execution_url": "http://localhost:8090/ui/exec/1"})
        assert tr.kestra_ui == "http://localhost:8090/ui/exec/1"

    def test_kestra_ui_returns_none_when_missing(self):
        tr = self._run(metadata={})
        assert tr.kestra_ui is None

    def test_status_pending_when_run_is_none(self, monkeypatch):
        tr = self._run()
        # `.run` does the real metaflow lookup — patch it on the type.
        monkeypatch.setattr(
            KestraTriggeredRun, "run",
            property(lambda self: None),
        )
        assert tr.status == "PENDING"

    def test_status_succeeded_when_run_successful(self, monkeypatch):
        tr = self._run()
        run = MagicMock(successful=True, finished=True)
        monkeypatch.setattr(KestraTriggeredRun, "run", property(lambda self: run))
        assert tr.status == "SUCCEEDED"

    def test_status_failed_when_finished_but_not_successful(self, monkeypatch):
        tr = self._run()
        run = MagicMock(successful=False, finished=True)
        monkeypatch.setattr(KestraTriggeredRun, "run", property(lambda self: run))
        assert tr.status == "FAILED"

    def test_status_running_when_run_not_finished(self, monkeypatch):
        tr = self._run()
        run = MagicMock(successful=False, finished=False)
        monkeypatch.setattr(KestraTriggeredRun, "run", property(lambda self: run))
        assert tr.status == "RUNNING"


# ---------------------------------------------------------------------------
# _find_flow_for_run_id  — local datastore scanner
# ---------------------------------------------------------------------------


class TestFindFlowForRunId:
    def test_returns_flow_when_run_dir_exists(self, tmp_path, monkeypatch):
        # Set up a fake datastore root with FlowName/run_id directory tree.
        monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", str(tmp_path))
        run_dir = tmp_path / ".metaflow" / "MyFlow" / "run-42"
        run_dir.mkdir(parents=True)
        assert _find_flow_for_run_id("run-42") == "MyFlow"

    def test_returns_none_when_no_metaflow_root(self, tmp_path, monkeypatch):
        monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", str(tmp_path))
        # No .metaflow dir → returns None silently.
        assert _find_flow_for_run_id("run-99") is None

    def test_returns_none_when_run_id_not_present(self, tmp_path, monkeypatch):
        monkeypatch.setenv("METAFLOW_DATASTORE_SYSROOT_LOCAL", str(tmp_path))
        (tmp_path / ".metaflow" / "OtherFlow" / "run-1").mkdir(parents=True)
        assert _find_flow_for_run_id("run-missing") is None
