"""
Kestra CLI commands for Metaflow.

Registered via mfextinit_kestra.py as the `kestra` CLI group.

Commands
--------
  compile  Compile the flow to a Kestra YAML file (no server needed).
  create   Compile and upload the flow to a running Kestra instance.
  run      Compile, deploy, trigger an execution and stream its logs.
"""

import hashlib
import json
import os
import re as _re
import sys
import time

from metaflow._vendor import click
from metaflow.exception import MetaflowException
from metaflow.util import get_username

from .exception import KestraException, NotSupportedException
from .kestra_compiler import KestraCompiler, flow_name_to_id

# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_workflow(flow, graph):
    """Raise MetaflowException / NotSupportedException for unsupported features."""
    seen = set()
    for _, param in flow._get_parameters():
        norm = param.name.lower()
        if norm in seen:
            raise MetaflowException(
                f"Parameter *{param.name}* is specified twice. "
                "Parameter names are case-insensitive."
            )
        seen.add(norm)
        if "default" not in param.kwargs:
            raise MetaflowException(
                f"Parameter *{param.name}* does not have a default value. "
                "A default value is required when deploying to Kestra."
            )

    for node in graph:
        if node.parallel_foreach:
            raise NotSupportedException(
                f"Step *{node.name}* uses @parallel which is not supported with Kestra."
            )
        # @resources is supported: CPU/memory/GPU hints are forwarded as --with flags
        # at runtime so compute backends (e.g. @kubernetes, @sandbox) receive them.
        if any(d.name == "batch" for d in node.decorators):
            raise NotSupportedException(
                f"Step *{node.name}* uses @batch which is not supported with Kestra. "
                "Use @kubernetes or remove the decorator."
            )
        if any(d.name == "slurm" for d in node.decorators):
            raise NotSupportedException(
                f"Step *{node.name}* uses @slurm which is not supported with Kestra."
            )

    # @exit_hook is the only flow-level decorator that remains unsupported
    for bad in ("exit_hook",):
        decos = getattr(flow, "_flow_decorators", {}).get(bad)
        if decos:
            raise NotSupportedException(
                f"@{bad} is not supported with Kestra."
            )


# ---------------------------------------------------------------------------
# Click command group
# ---------------------------------------------------------------------------

@click.group()
def cli():
    pass


@cli.group(help="Commands related to Kestra orchestration.")
@click.option(
    "--name",
    default=None,
    type=str,
    help="Override the Kestra flow ID (default: derived from flow class name).",
)
@click.pass_obj
def kestra(obj, name=None):
    obj.check(obj.graph, obj.flow, obj.environment, pylint=obj.pylint)
    obj.kestra_flow_name = name or obj.graph.name


# ---------------------------------------------------------------------------
# compile
# ---------------------------------------------------------------------------

@kestra.command(name="compile", help="Compile this flow to a Kestra YAML file.")
@click.argument("file", required=True)
@click.option("--tag", "tags", multiple=True, help="Tag all Metaflow run objects.")
@click.option(
    "--namespace",
    default=None,
    help="Metaflow namespace for all runs.",
)
@click.option(
    "--kestra-namespace",
    default="metaflow",
    show_default=True,
    help="Kestra namespace to deploy the flow into.",
)
@click.option(
    "--max-workers",
    default=10,
    show_default=True,
    type=int,
    help="Max concurrent ForEach body tasks.",
)
@click.option(
    "--with",
    "with_decorators",
    multiple=True,
    help="Inject a step decorator, e.g. --with=kubernetes or --with='resources:cpu=4'.",
)
@click.option(
    "--workflow-timeout",
    default=None,
    type=int,
    help="Maximum seconds a single execution may run (emitted as flow-level timeout in Kestra YAML).",
)
@click.option(
    "--branch",
    default=None,
    help="@project branch name (default: user.<username>).",
)
@click.option(
    "--production",
    is_flag=True,
    default=False,
    help="Deploy to the @project production branch.",
)
@click.pass_obj
def compile(
    obj,
    file,
    tags,
    namespace,
    kestra_namespace,
    max_workers,
    with_decorators,
    workflow_timeout,
    branch,
    production,
):
    if os.path.abspath(sys.argv[0]) == os.path.abspath(file):
        raise MetaflowException("Output file cannot be the same as the flow file.")

    _validate_workflow(obj.flow, obj.graph)

    obj.echo(f"Compiling *{obj.kestra_flow_name}* to Kestra YAML...", bold=True)

    compiler = _build_compiler(
        obj, kestra_namespace, max_workers, with_decorators, workflow_timeout,
        branch, production, namespace=namespace, tags=tags,
    )
    yaml_content = compiler.compile()

    with open(file, "w") as f:
        f.write(yaml_content)

    obj.echo(
        f"Flow *{obj.kestra_flow_name}* compiled to Kestra YAML successfully → *{file}*\n"
        f"Deploy with: python {os.path.basename(sys.argv[0])} kestra create",
        bold=True,
    )


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------

@kestra.command(help="Compile and deploy this flow to a running Kestra instance.")
@click.option(
    "--kestra-host",
    default="http://localhost:8080",
    show_default=True,
    envvar="KESTRA_HOST",
    help="Kestra server base URL.",
)
@click.option(
    "--kestra-user",
    default=None,
    envvar="KESTRA_USER",
    help="Kestra basic-auth username.",
)
@click.option(
    "--kestra-password",
    default=None,
    envvar="KESTRA_PASSWORD",
    help="Kestra basic-auth password.",
)
@click.option(
    "--kestra-token",
    default=None,
    envvar="KESTRA_API_TOKEN",
    help="Kestra API token (Enterprise Edition).",
)
@click.option("--tag", "tags", multiple=True)
@click.option("--namespace", default=None)
@click.option(
    "--kestra-namespace",
    default="metaflow",
    show_default=True,
    help="Kestra namespace to deploy the flow into.",
)
@click.option("--max-workers", default=10, show_default=True, type=int)
@click.option("--with", "with_decorators", multiple=True)
@click.option("--workflow-timeout", default=None, type=int)
@click.option("--branch", default=None)
@click.option("--production", is_flag=True, default=False)
@click.option(
    "--deployer-attribute-file",
    default=None,
    hidden=True,
    help="Write deployment info JSON here (used by Metaflow Deployer API).",
)
@click.pass_obj
def create(
    obj,
    kestra_host,
    kestra_user,
    kestra_password,
    kestra_token,
    tags,
    namespace,
    kestra_namespace,
    max_workers,
    with_decorators,
    workflow_timeout,
    branch,
    production,
    deployer_attribute_file,
):
    _validate_workflow(obj.flow, obj.graph)

    obj.echo(f"Compiling *{obj.kestra_flow_name}* to Kestra YAML...", bold=True)

    compiler = _build_compiler(
        obj, kestra_namespace, max_workers, with_decorators, workflow_timeout,
        branch, production, namespace=namespace, tags=tags,
    )
    yaml_content = compiler.compile()

    client = _make_client(kestra_host, kestra_user, kestra_password, kestra_token)
    _deploy_flow(client, yaml_content, kestra_namespace, obj)

    if deployer_attribute_file:
        flow_id = compiler.flow_id
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    # Store the Kestra flow ID (e.g. "hello-world-user-npow-helloflow")
                    # as the deployer name so that DeployedFlow.from_deployment()
                    # can recover the deployment from just the name.
                    "name": flow_id,
                    "flow_name": obj.flow.name,
                    "metadata": "{}",
                    "additional_info": {
                        "flow_id": flow_id,
                        "kestra_namespace": kestra_namespace,
                        "kestra_host": kestra_host,
                        "kestra_user": kestra_user,
                        "kestra_password": kestra_password,
                        "kestra_token": kestra_token,
                        # Store the Metaflow flow class name so _trigger_direct can
                        # construct the correct pathspec when flow_file is unavailable.
                        "mf_flow_class": obj.flow.name,
                    },
                },
                f,
            )


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

@kestra.command(help="Compile, deploy, and trigger an execution on Kestra.")
@click.option("--kestra-host", default="http://localhost:8080", show_default=True, envvar="KESTRA_HOST")
@click.option("--kestra-user", default=None, envvar="KESTRA_USER")
@click.option("--kestra-password", default=None, envvar="KESTRA_PASSWORD")
@click.option("--kestra-token", default=None, envvar="KESTRA_API_TOKEN")
@click.option("--tag", "tags", multiple=True)
@click.option("--namespace", default=None)
@click.option("--kestra-namespace", default="metaflow", show_default=True)
@click.option("--max-workers", default=10, show_default=True, type=int)
@click.option("--with", "with_decorators", multiple=True)
@click.option("--workflow-timeout", default=None, type=int)
@click.option("--branch", default=None)
@click.option("--production", is_flag=True, default=False)
@click.option(
    "--wait/--no-wait",
    default=True,
    show_default=True,
    help="Wait for the execution to complete.",
)
@click.pass_obj
def run(
    obj,
    kestra_host,
    kestra_user,
    kestra_password,
    kestra_token,
    tags,
    namespace,
    kestra_namespace,
    max_workers,
    with_decorators,
    workflow_timeout,
    branch,
    production,
    wait,
):
    _validate_workflow(obj.flow, obj.graph)

    obj.echo(f"Compiling *{obj.kestra_flow_name}*...", bold=True)

    compiler = _build_compiler(
        obj, kestra_namespace, max_workers, with_decorators, workflow_timeout,
        branch, production, namespace=namespace, tags=tags,
    )
    yaml_content = compiler.compile()
    flow_id = compiler.flow_id

    client = _make_client(kestra_host, kestra_user, kestra_password, kestra_token)

    _deploy_flow(client, yaml_content, kestra_namespace, obj)
    obj.echo("Triggering execution...", bold=True)
    execution_id = _trigger_execution(client, kestra_namespace, flow_id)
    execution_url = f"{kestra_host}/ui/executions/{kestra_namespace}/{flow_id}/{execution_id}"
    obj.echo(f"Execution started: *{execution_url}*")

    if wait:
        obj.echo("Waiting for execution to complete...")
        final_state = _wait_for_execution(client, execution_id, obj)
        if final_state == "SUCCESS":
            obj.echo(f"Execution *{execution_id}* completed successfully.", bold=True)
        else:
            raise KestraException(
                f"Execution {execution_id} finished with state: {final_state}\nURL: {execution_url}"
            )
    else:
        obj.echo(f"Execution ID: {execution_id}")
        obj.echo(f"Track it at: {execution_url}")


# ---------------------------------------------------------------------------
# trigger
# ---------------------------------------------------------------------------

@kestra.command(help="Trigger a run for a previously deployed Kestra flow.")
@click.option("--kestra-host", default="http://localhost:8080", show_default=True, envvar="KESTRA_HOST")
@click.option("--kestra-user", default=None, envvar="KESTRA_USER")
@click.option("--kestra-password", default=None, envvar="KESTRA_PASSWORD")
@click.option("--kestra-token", default=None, envvar="KESTRA_API_TOKEN")
@click.option(
    "--kestra-namespace",
    default="metaflow",
    show_default=True,
    help="Kestra namespace the flow is deployed in.",
)
@click.option(
    "--flow-id",
    default=None,
    hidden=True,
    help="Kestra flow ID to trigger (overrides computed default).",
)
@click.option(
    "--deployer-attribute-file",
    default=None,
    hidden=True,
    help="Write triggered-run info JSON here (used by Metaflow Deployer API).",
)
@click.option(
    "--run-param",
    "run_params",
    multiple=True,
    default=None,
    help="Flow parameter as key=value (repeatable).",
)
@click.pass_obj
def trigger(
    obj,
    kestra_host,
    kestra_user,
    kestra_password,
    kestra_token,
    kestra_namespace,
    flow_id,
    deployer_attribute_file,
    run_params,
):
    if flow_id is None:
        flow_id = flow_name_to_id(obj.kestra_flow_name)

    # Parse run params into a dict (key=value pairs)
    params = {}
    for kv in run_params:
        k, _, v = kv.partition("=")
        params[k.strip()] = v.strip()

    client = _make_client(kestra_host, kestra_user, kestra_password, kestra_token)

    obj.echo(f"Triggering execution of *{flow_id}* in namespace *{kestra_namespace}*...", bold=True)
    execution_id = _trigger_execution(client, kestra_namespace, flow_id, inputs=params or None)
    execution_url = f"{kestra_host}/ui/executions/{kestra_namespace}/{flow_id}/{execution_id}"
    obj.echo(f"Execution started: *{execution_url}*")

    if deployer_attribute_file:
        _run_id = "kestra-" + hashlib.md5(execution_id.encode()).hexdigest()[:16]
        pathspec = f"{obj.flow.name}/{_run_id}"
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "pathspec": pathspec,
                    "name": obj.kestra_flow_name,
                    "execution_id": execution_id,
                    "execution_url": execution_url,
                    "metadata": "{}",
                },
                f,
            )


# ---------------------------------------------------------------------------
# resume
# ---------------------------------------------------------------------------

@kestra.command(help="Resume a failed run, skipping steps that already completed.")
@click.option("--clone-run-id", "clone_run_id", required=True,
              help="Metaflow run ID of the failed run to resume (e.g. kestra-<hex>).")
@click.option("--kestra-host", default="http://localhost:8080", show_default=True, envvar="KESTRA_HOST")
@click.option("--kestra-user", default=None, envvar="KESTRA_USER")
@click.option("--kestra-password", default=None, envvar="KESTRA_PASSWORD")
@click.option("--kestra-token", default=None, envvar="KESTRA_API_TOKEN")
@click.option("--kestra-namespace", default="metaflow", show_default=True,
              help="Kestra namespace the flow is deployed in.")
@click.option("--flow-id", default=None, hidden=True,
              help="Kestra flow ID (overrides computed default).")
@click.option("--run-param", "run_params", multiple=True, default=None,
              help="Flow parameter as key=value (repeatable).")
@click.option("--wait/--no-wait", default=True, show_default=True,
              help="Wait for the resumed execution to complete.")
@click.option("--deployer-attribute-file", default=None, hidden=True,
              help="Write resumed-run info JSON here (used by Metaflow Deployer API).")
@click.pass_obj
def resume(
    obj,
    clone_run_id,
    kestra_host,
    kestra_user,
    kestra_password,
    kestra_token,
    kestra_namespace,
    flow_id,
    run_params,
    wait,
    deployer_attribute_file,
):
    if flow_id is None:
        flow_id = flow_name_to_id(obj.kestra_flow_name)

    # Parse run params, then inject ORIGIN_RUN_ID for resume
    params = {"ORIGIN_RUN_ID": clone_run_id}
    for kv in run_params:
        k, _, v = kv.partition("=")
        params[k.strip()] = v.strip()

    client = _make_client(kestra_host, kestra_user, kestra_password, kestra_token)

    obj.echo(
        f"Resuming *{flow_id}* from run *{clone_run_id}* in namespace *{kestra_namespace}*...",
        bold=True,
    )
    execution_id = _trigger_execution(client, kestra_namespace, flow_id, inputs=params)
    execution_url = f"{kestra_host}/ui/executions/{kestra_namespace}/{flow_id}/{execution_id}"
    obj.echo(f"Execution started: *{execution_url}*")

    if deployer_attribute_file:
        _run_id = "kestra-" + hashlib.md5(execution_id.encode()).hexdigest()[:16]
        pathspec = f"{obj.flow.name}/{_run_id}"
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "pathspec": pathspec,
                    "name": obj.kestra_flow_name,
                    "execution_id": execution_id,
                    "execution_url": execution_url,
                    "metadata": "{}",
                    "origin_run_id": clone_run_id,
                },
                f,
            )

    if wait:
        obj.echo("Waiting for resumed execution to complete...")
        final_state = _wait_for_execution(client, execution_id, obj)
        if final_state == "SUCCESS":
            obj.echo(f"Execution *{execution_id}* completed successfully.", bold=True)
        else:
            raise KestraException(
                f"Execution {execution_id} finished with state: {final_state}\nURL: {execution_url}"
            )
    else:
        obj.echo(f"Execution ID: {execution_id}")
        obj.echo(f"Track it at: {execution_url}")


# ---------------------------------------------------------------------------
# Kestra API helpers
# ---------------------------------------------------------------------------

def _build_compiler(
    obj,
    kestra_namespace,
    max_workers,
    with_decorators,
    workflow_timeout,
    branch,
    production,
    namespace=None,
    tags=(),
) -> KestraCompiler:
    """Construct a KestraCompiler from a Metaflow CLI obj and shared options."""
    # If --branch was not passed to `kestra create` directly, check whether the
    # top-level @project decorator already resolved a branch from --branch at the
    # python flow.py level.  This happens when the Deployer API passes --branch
    # as a top-level arg (e.g. Deployer(..., branch='myfeature').kestra().create()).
    # In that case, current.branch_name == "test.myfeature" and we must bake
    # --branch=myfeature into the compiled YAML so Kestra workers run steps under
    # the same branch, regardless of the container's USERNAME.
    effective_branch = branch
    if effective_branch is None and not production:
        try:
            from metaflow import current as _current
            bn = getattr(_current, "branch_name", None)
            # Only inherit if the branch was explicitly set (not a user.* default).
            if bn and not bn.startswith("user.") and not bn.startswith("prod"):
                # Strip the "test." prefix to recover the raw branch name.
                if bn.startswith("test."):
                    effective_branch = bn[len("test."):]
                else:
                    effective_branch = bn
        except Exception:
            pass

    return KestraCompiler(
        name=obj.kestra_flow_name,
        graph=obj.graph,
        flow=obj.flow,
        flow_file=os.path.abspath(sys.argv[0]),
        metadata=obj.metadata,
        flow_datastore=obj.flow_datastore,
        environment=obj.environment,
        event_logger=obj.event_logger,
        monitor=obj.monitor,
        tags=list(tags),
        namespace=namespace,
        username=get_username(),
        max_workers=max_workers,
        with_decorators=list(with_decorators),
        workflow_timeout=workflow_timeout,
        kestra_namespace=kestra_namespace,
        branch=effective_branch,
        production=production,
    )


def _make_client(host: str, user, password, token):
    """Return a requests.Session configured for the given Kestra instance."""
    try:
        import requests
    except ImportError:
        raise KestraException(
            "The `requests` package is required for deploy/run commands. "
            "Install it with: pip install requests"
        )
    session = requests.Session()
    if token:
        session.headers["Authorization"] = f"Bearer {token}"
    elif user and password:
        session.auth = (user, password)
    session.headers["Content-Type"] = "application/x-yaml"
    session._kestra_host = host
    return session


def _deploy_flow(client, yaml_content: str, kestra_namespace: str, obj):
    """Create or update a Kestra flow via the REST API."""
    host = client._kestra_host
    url = f"{host}/api/v1/flows"
    try:
        resp = client.post(url, data=yaml_content.encode("utf-8"))
        if resp.status_code in (200, 201):
            obj.echo("Flow deployed successfully to Kestra.")
            return
        # 409 or 422 = flow already exists — update via PUT /api/v1/flows/{namespace}/{id}
        if resp.status_code in (409, 422):
            # Extract namespace and id from YAML to build the correct PUT URL
            ns_match = _re.search(r"^namespace:\s*(\S+)", yaml_content, _re.MULTILINE)
            id_match = _re.search(r"^id:\s*(\S+)", yaml_content, _re.MULTILINE)
            ns = ns_match.group(1) if ns_match else kestra_namespace
            fid = id_match.group(1) if id_match else ""
            put_url = f"{host}/api/v1/flows/{ns}/{fid}"
            resp2 = client.put(put_url, data=yaml_content.encode("utf-8"))
            if resp2.status_code in (200, 201):
                obj.echo("Flow updated successfully on Kestra.")
                return
            raise KestraException(
                f"Failed to update flow on Kestra (HTTP {resp2.status_code}): "
                f"{resp2.text[:500]}"
            )
        raise KestraException(
            f"Failed to deploy flow to Kestra (HTTP {resp.status_code}): "
            f"{resp.text[:500]}"
        )
    except Exception as exc:
        if isinstance(exc, KestraException):
            raise
        raise KestraException(f"Failed to connect to Kestra at {host}: {exc}") from exc


def _trigger_execution(client, kestra_namespace: str, flow_id: str, inputs: dict = None) -> str:
    """Trigger a flow execution and return the execution ID.

    Parameters
    ----------
    inputs : dict, optional
        Flow input values (Metaflow Parameters). Sent as multipart/form-data
        because Kestra returns 404 when Content-Type is application/x-yaml.
    """
    host = client._kestra_host
    url = f"{host}/api/v1/executions/{kestra_namespace}/{flow_id}"
    try:
        if inputs:
            # Kestra requires multipart/form-data for inputs; url-encoded returns 404.
            # Must clear the session Content-Type (application/x-yaml) so requests can
            # set the correct multipart/form-data boundary from the files= parameter.
            # requests only sets Content-Type from files= when it's not already present
            # in headers, so we must explicitly remove the session-level value.
            files = {k: (None, str(v)) for k, v in inputs.items()}
            resp = client.post(url, files=files, headers={"Content-Type": None})
        else:
            # Clear Content-Type so requests sends a plain POST with no body.
            # Kestra returns 404 if Content-Type is set (e.g. application/x-yaml from session).
            resp = client.post(url, headers={"Content-Type": None})
        if resp.status_code not in (200, 201):
            raise KestraException(
                f"Failed to trigger execution (HTTP {resp.status_code}): "
                f"{resp.text[:500]}"
            )
        return resp.json()["id"]
    except Exception as exc:
        if isinstance(exc, KestraException):
            raise
        raise KestraException(f"Failed to trigger execution: {exc}") from exc


def _wait_for_execution(client, execution_id: str, obj, poll_interval: int = 5) -> str:
    """Poll until the execution reaches a terminal state and return the state."""
    host = client._kestra_host
    terminal_states = {"SUCCESS", "FAILED", "KILLED", "WARNING"}
    url = f"{host}/api/v1/executions/{execution_id}"

    seen_running = False

    while True:
        try:
            # Override Content-Type per-request; GET needs no body content type.
            resp = client.get(url, headers={"Content-Type": None})
            if resp.status_code == 200:
                data = resp.json()
                state = data.get("state", {}).get("current", "UNKNOWN")
                if not seen_running and state == "RUNNING":
                    obj.echo("Execution is running...")
                    seen_running = True
                if state in terminal_states:
                    return state
            else:
                obj.echo(f"Warning: could not poll execution status (HTTP {resp.status_code})")
        except Exception as exc:
            obj.echo(f"Warning: error polling execution: {exc}")

        time.sleep(poll_interval)
