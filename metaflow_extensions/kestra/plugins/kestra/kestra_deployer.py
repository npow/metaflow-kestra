"""Metaflow Deployer plugin for Kestra.

Registers ``TYPE = "kestra"`` so that ``Deployer(flow_file).kestra(...)``
is available and the Metaflow Deployer API can be used with Kestra.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Dict, Optional, Type

from metaflow.runner.deployer_impl import DeployerImpl

if TYPE_CHECKING:
    from metaflow_extensions.kestra.plugins.kestra.kestra_deployer_objects import (
        KestraDeployedFlow,
    )


class KestraDeployer(DeployerImpl):
    """Deployer implementation for Kestra.

    Parameters
    ----------
    name : str, optional
        Override the Kestra flow ID (default: derived from flow class name).
    kestra_namespace : str, optional
        Kestra namespace to deploy the flow into (default: "metaflow").
    kestra_host : str, optional
        Kestra server base URL (default: "http://localhost:8080").
    max_workers : int, optional
        Maximum concurrent ForEach body tasks (default: 10).
    """

    TYPE: ClassVar[Optional[str]] = "kestra"

    def __init__(self, deployer_kwargs: Dict[str, str], **kwargs) -> None:
        self._deployer_kwargs = deployer_kwargs
        super().__init__(**kwargs)

    @property
    def deployer_kwargs(self) -> Dict[str, str]:
        return self._deployer_kwargs

    @staticmethod
    def deployed_flow_type() -> Type["KestraDeployedFlow"]:
        from .kestra_deployer_objects import KestraDeployedFlow

        return KestraDeployedFlow

    def create(self, **kwargs) -> "KestraDeployedFlow":
        """Deploy this flow to a running Kestra instance.

        Parameters
        ----------
        kestra_host : str, optional
            Kestra server base URL.
        kestra_namespace : str, optional
            Kestra namespace to deploy into.
        max_workers : int, optional
            Maximum concurrent ForEach body tasks.
        deployer_attribute_file : str, optional
            Write deployment info JSON here (Metaflow Deployer API internal).

        Returns
        -------
        KestraDeployedFlow
        """
        from .kestra_deployer_objects import KestraDeployedFlow

        return self._create(KestraDeployedFlow, **kwargs)
