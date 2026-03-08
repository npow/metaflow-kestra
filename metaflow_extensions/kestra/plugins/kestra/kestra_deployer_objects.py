"""DeployedFlow and TriggeredRun objects for the Kestra Deployer plugin."""

from __future__ import annotations

import json
import os
import sys
from typing import TYPE_CHECKING, ClassVar, Optional

import metaflow
from metaflow.exception import MetaflowNotFound
from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo
from metaflow.runner.subprocess_manager import SubprocessManager

if TYPE_CHECKING:
    import metaflow.runner.deployer_impl


def _find_flow_for_run_id(run_id: str) -> Optional[str]:
    """Scan the local Metaflow datastore to find which flow class owns ``run_id``.

    Returns the flow class name (e.g. ``"HelloFromDeploymentFlow"``) or ``None``.
    This is used by ``_trigger_direct`` when no flow class name is available.
    """
    try:
        import metaflow as _mf
        _mf.namespace(None)
        # The local datastore root is at METAFLOW_DATASTORE_SYSROOT_LOCAL/.metaflow/
        sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL") or os.path.expanduser("~")
        mf_root = os.path.join(sysroot, ".metaflow")
        if not os.path.isdir(mf_root):
            return None
        for entry in os.listdir(mf_root):
            flow_dir = os.path.join(mf_root, entry)
            if os.path.isdir(flow_dir) and os.path.isdir(os.path.join(flow_dir, run_id)):
                return entry
    except Exception:
        pass
    return None


def _make_stub_deployer(name: str):
    """Return a minimal deployer stub for a Kestra flow recovered without a flow file.

    This avoids calling ``DeployerImpl.__init__`` (which requires a valid flow file
    to build the MetaflowAPI).  The stub only supports REST-API-based triggering
    (``KestraDeployedFlow._trigger_direct``).
    """
    from .kestra_deployer import KestraDeployer

    stub = object.__new__(KestraDeployer)
    stub._deployer_kwargs = {}
    stub.flow_file = ""
    stub.show_output = False
    stub.profile = None
    stub.env = None
    stub.cwd = os.getcwd()
    stub.file_read_timeout = 3600
    stub.env_vars = os.environ.copy()
    stub.spm = SubprocessManager()
    stub.top_level_kwargs = {}
    stub.api = None
    stub.name = name
    stub.flow_name = name
    stub.metadata = "{}"
    stub.additional_info = {}
    return stub


class KestraTriggeredRun(TriggeredRun):
    """A Kestra execution that was triggered via the Deployer API.

    Overrides ``.run`` from :class:`~metaflow.runner.deployer.TriggeredRun` to apply
    deployer env vars (local metadata overrides) before polling Metaflow for the run
    with ``pathspec`` (``FlowName/kestra-<execution_id>``).
    """

    @property
    def kestra_ui(self) -> Optional[str]:
        """URL to the Kestra UI for this execution, if available."""
        try:
            metadata = self._metadata  # type: ignore[attr-defined]
            url = metadata.get("execution_url")
            if url:
                return url
        except Exception:
            pass
        return None

    @property
    def run(self):
        """Retrieve the Run object, applying deployer env vars so local metadata works."""
        env_vars = getattr(self.deployer, "env_vars", {}) or {}
        meta_type = env_vars.get("METAFLOW_DEFAULT_METADATA")
        sysroot = env_vars.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")

        old_meta = os.environ.get("METAFLOW_DEFAULT_METADATA")
        old_sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
        try:
            if meta_type:
                os.environ["METAFLOW_DEFAULT_METADATA"] = meta_type
                metaflow.metadata(meta_type)
            if meta_type == "local" and sysroot is None:
                sysroot = os.path.expanduser("~")
            if sysroot:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = sysroot

            # If the pathspec uses an unknown flow class (from a plain-name recovery),
            # try to resolve it by scanning the local datastore for the run_id.
            pathspec = self.pathspec
            if pathspec and pathspec.startswith("UNKNOWN/"):
                run_id = pathspec.split("/", 1)[1]
                flow_name = _find_flow_for_run_id(run_id)
                if flow_name:
                    pathspec = "%s/%s" % (flow_name, run_id)
                    self.pathspec = pathspec

            return metaflow.Run(pathspec, _namespace_check=False)
        except MetaflowNotFound:
            # Run not yet written — execution still in progress
            return None
        finally:
            if old_meta is None:
                os.environ.pop("METAFLOW_DEFAULT_METADATA", None)
            else:
                os.environ["METAFLOW_DEFAULT_METADATA"] = old_meta
            if old_sysroot is None:
                os.environ.pop("METAFLOW_DATASTORE_SYSROOT_LOCAL", None)
            else:
                os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = old_sysroot

    @property
    def status(self) -> Optional[str]:
        """Return a simple status string based on the underlying Metaflow run."""
        run = self.run
        if run is None:
            return "PENDING"
        if run.successful:
            return "SUCCEEDED"
        if run.finished:
            return "FAILED"
        return "RUNNING"


class KestraDeployedFlow(DeployedFlow):
    """A Metaflow flow deployed as a Kestra flow."""

    TYPE: ClassVar[Optional[str]] = "kestra"

    @property
    def id(self) -> str:
        """Deployment identifier encoding all info needed for ``from_deployment``."""
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        return json.dumps({
            "name": self.name,
            "flow_name": self.flow_name,
            "flow_file": getattr(self.deployer, "flow_file", None),
            **additional_info,
        })

    def run(self, **kwargs) -> KestraTriggeredRun:
        """Trigger a new execution of this deployed Kestra flow.

        Parameters
        ----------
        **kwargs : Any
            Flow parameters as keyword arguments (e.g. ``message="hello"``).

        Returns
        -------
        KestraTriggeredRun
        """
        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        flow_file = getattr(self.deployer, "flow_file", "") or ""

        # When the deployer was recovered via from_deployment() with a plain name
        # (no flow file available), trigger directly via the Kestra REST API.
        if not flow_file:
            return self._trigger_direct(**kwargs)

        # Convert kwargs to "key=value" strings for --run-param.
        # Must be a list (not tuple) so it passes the Optional[Union[List[str], Tuple[str]]]
        # type check in the Metaflow click API — Tuple[str] means exactly one element,
        # but List[str] accepts any number of elements.
        run_params = list("%s=%s" % (k, v) for k, v in kwargs.items())

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            trigger_kwargs = {"deployer_attribute_file": attribute_file_path}
            if run_params:
                trigger_kwargs["run_params"] = run_params
            for key in ("flow_id", "kestra_namespace", "kestra_host", "kestra_user", "kestra_password", "kestra_token"):
                val = additional_info.get(key)
                if val:
                    trigger_kwargs[key] = val
            command = get_lower_level_group(
                self.deployer.api,
                self.deployer.top_level_kwargs,
                self.deployer.TYPE,
                self.deployer.deployer_kwargs,
            ).trigger(**trigger_kwargs)

            pid = self.deployer.spm.run_command(
                [sys.executable, *command],
                env=self.deployer.env_vars,
                cwd=self.deployer.cwd,
                show_output=self.deployer.show_output,
            )

            command_obj = self.deployer.spm.get(pid)
            content = handle_timeout(
                attribute_file_fd, command_obj, self.deployer.file_read_timeout
            )
            command_obj.sync_wait()
            if command_obj.process.returncode == 0:
                return KestraTriggeredRun(deployer=self.deployer, content=content)

        raise RuntimeError(
            "Error triggering Kestra flow %r" % self.deployer.flow_file
        )

    trigger = run

    def _trigger_direct(self, **kwargs) -> "KestraTriggeredRun":
        """Trigger a Kestra execution directly via REST API (no flow file needed).

        Used when this DeployedFlow was recovered via ``from_deployment()`` with
        a plain name identifier and no flow file is available.
        """
        import hashlib

        additional_info = getattr(self.deployer, "additional_info", {}) or {}
        kestra_host = additional_info.get("kestra_host", "http://localhost:8080")
        kestra_namespace = additional_info.get("kestra_namespace", "metaflow")
        kestra_user = additional_info.get("kestra_user")
        kestra_password = additional_info.get("kestra_password")
        kestra_token = additional_info.get("kestra_token")
        flow_id = additional_info.get("flow_id")

        # Build the flow ID from name if not explicitly stored.
        if not flow_id:
            from .kestra_compiler import flow_name_to_id
            flow_id = flow_name_to_id(self.name)

        try:
            import requests
        except ImportError:
            raise RuntimeError("The `requests` package is required to trigger Kestra flows.")

        session = requests.Session()
        if kestra_token:
            session.headers["Authorization"] = "Bearer %s" % kestra_token
        elif kestra_user and kestra_password:
            session.auth = (kestra_user, kestra_password)

        url = "%s/api/v1/executions/%s/%s" % (kestra_host, kestra_namespace, flow_id)
        inputs = {k: str(v) for k, v in kwargs.items()} if kwargs else None

        if inputs:
            files = {k: (None, v) for k, v in inputs.items()}
            resp = session.post(url, files=files, headers={"Content-Type": None})
        else:
            resp = session.post(url, headers={"Content-Type": None})

        if resp.status_code not in (200, 201):
            raise RuntimeError(
                "Failed to trigger Kestra flow (HTTP %d): %s"
                % (resp.status_code, resp.text[:500])
            )

        execution_id = resp.json()["id"]
        run_id = "kestra-" + hashlib.md5(execution_id.encode()).hexdigest()[:16]
        # Use the Metaflow class name (not the Kestra flow ID) for the pathspec.
        # Prefer the explicitly stored class name; otherwise use "UNKNOWN" as a
        # placeholder.  The KestraTriggeredRun.run property will scan the local
        # datastore to resolve the class name once the run is written.
        flow_class_name = (
            additional_info.get("mf_flow_class")
            or (self.deployer.flow_name if self.deployer.flow_name and "-" not in self.deployer.flow_name else None)
        )
        if not flow_class_name:
            flow_class_name = "UNKNOWN"
        pathspec = "%s/%s" % (flow_class_name, run_id)

        content_dict = {
            "pathspec": pathspec,
            "name": self.name,
            "execution_id": execution_id,
            "execution_url": "%s/ui/executions/%s/%s/%s" % (
                kestra_host, kestra_namespace, flow_id, execution_id
            ),
            "metadata": "{}",
        }
        return KestraTriggeredRun(deployer=self.deployer, content=json.dumps(content_dict))

    @classmethod
    def from_deployment(cls, identifier: str, metadata: Optional[str] = None) -> "KestraDeployedFlow":
        """Recover a KestraDeployedFlow from a deployment identifier.

        ``identifier`` can be either:

        - A JSON string produced by :attr:`id` (preferred – carries all credentials).
        - A plain flow name (e.g. ``"HelloFlow"``).  In this case the Kestra host
          and credentials are read from the environment variables ``KESTRA_HOST``,
          ``KESTRA_USER``, and ``KESTRA_PASSWORD``.  The Kestra flow ID is derived
          from the flow name (lowercase, hyphens).
        """
        from .kestra_deployer import KestraDeployer
        from .kestra_compiler import flow_name_to_id

        # Try to parse as JSON first (the full id payload).
        info = None
        if identifier.startswith("{"):
            try:
                info = json.loads(identifier)
            except (ValueError, TypeError):
                pass

        if info is not None:
            # Full JSON payload – use as-is.
            deployer = KestraDeployer(flow_file=info.get("flow_file") or "", deployer_kwargs={})
            deployer.name = info["name"]
            deployer.flow_name = info["flow_name"]
            deployer.metadata = metadata or "{}"
            deployer.additional_info = {
                k: v for k, v in info.items()
                if k not in ("name", "flow_name", "flow_file")
            }
        else:
            # Plain name – fall back to environment variables for credentials.
            # Build a stub deployer without a real flow file; the deployer will
            # trigger executions directly via the Kestra REST API.
            kestra_host = os.environ.get("KESTRA_HOST", "http://localhost:8080")
            kestra_namespace = os.environ.get("KESTRA_NAMESPACE", "metaflow")
            kestra_user = os.environ.get("KESTRA_USER")
            kestra_password = os.environ.get("KESTRA_PASSWORD")
            kestra_token = os.environ.get("KESTRA_API_TOKEN")
            # If identifier already looks like a Kestra flow ID (lowercase/hyphens),
            # use it as-is. Otherwise convert from a Metaflow class name.
            import re as _re
            if _re.match(r'^[a-z0-9-]+$', identifier):
                flow_id = identifier
            else:
                flow_id = flow_name_to_id(identifier)
            deployer = _make_stub_deployer(identifier)
            deployer.name = identifier
            deployer.flow_name = identifier
            deployer.metadata = metadata or "{}"
            deployer.additional_info = {
                "flow_id": flow_id,
                "kestra_namespace": kestra_namespace,
                "kestra_host": kestra_host,
                "kestra_user": kestra_user,
                "kestra_password": kestra_password,
                "kestra_token": kestra_token,
            }

        return cls(deployer=deployer)
