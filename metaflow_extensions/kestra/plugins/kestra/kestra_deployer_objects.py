"""DeployedFlow and TriggeredRun objects for the Kestra Deployer plugin."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, ClassVar, Optional

from metaflow.runner.deployer import DeployedFlow, TriggeredRun
from metaflow.runner.utils import get_lower_level_group, handle_timeout, temporary_fifo

if TYPE_CHECKING:
    import metaflow
    import metaflow.runner.deployer_impl


class KestraTriggeredRun(TriggeredRun):
    """A Kestra execution that was triggered via the Deployer API.

    Inherits ``.run`` from :class:`~metaflow.runner.deployer.TriggeredRun`, which polls
    Metaflow until the run with ``pathspec`` (``FlowName/kestra-<execution_id>``) appears.
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
        import os
        import metaflow
        from metaflow.exception import MetaflowNotFound

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
            return metaflow.Run(self.pathspec, _namespace_check=False)
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
        import json
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
        # Convert kwargs to "key=value" strings for --run-param.
        run_params = tuple("%s=%s" % (k, v) for k, v in kwargs.items())

        with temporary_fifo() as (attribute_file_path, attribute_file_fd):
            trigger_kwargs = dict(deployer_attribute_file=attribute_file_path)
            if run_params:
                trigger_kwargs["run_params"] = run_params
            additional_info = getattr(self.deployer, "additional_info", {}) or {}
            for key in ("flow_id", "kestra_namespace", "kestra_host", "kestra_user", "kestra_password"):
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

    @classmethod
    def from_deployment(cls, identifier: str, metadata: Optional[str] = None) -> "KestraDeployedFlow":
        """Recover a KestraDeployedFlow from a deployment identifier."""
        import json
        from .kestra_deployer import KestraDeployer

        info = json.loads(identifier)
        deployer = KestraDeployer(flow_file=info["flow_file"], deployer_kwargs={})
        deployer.name = info["name"]
        deployer.flow_name = info["flow_name"]
        deployer.metadata = metadata or "{}"
        deployer.additional_info = {
            k: v for k, v in info.items()
            if k not in ("name", "flow_name", "flow_file")
        }
        return cls(deployer=deployer)
