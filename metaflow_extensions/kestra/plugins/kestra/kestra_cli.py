"""
Kestra CLI commands for Metaflow.

Registered via mfextinit_kestra.py as the `kestra` CLI group.

Commands
--------
  compile  Compile the flow to a Kestra YAML file (no server needed).
  create   Compile and upload the flow to a running Kestra instance.
  run      Compile, deploy, trigger an execution and stream its logs.
"""

import json
import os
import sys
import time
import warnings

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
    for var, param in flow._get_parameters():
        norm = param.name.lower()
        if norm in seen:
            raise MetaflowException(
                "Parameter *%s* is specified twice. "
                "Parameter names are case-insensitive." % param.name
            )
        seen.add(norm)
        if "default" not in param.kwargs:
            raise MetaflowException(
                "Parameter *%s* does not have a default value. "
                "A default value is required when deploying to Kestra." % param.name
            )

    for node in graph:
        if node.parallel_foreach:
            raise NotSupportedException(
                "Step *%s* uses @parallel which is not supported with Kestra." % node.name
            )
        for deco in node.decorators:
            if deco.name == "resources":
                warnings.warn(
                    "Step *%s* uses @resources. Resource requirements are passed through "
                    "as --with flags but are not enforced by Kestra scheduling — "
                    "configure resources on your Kestra worker directly." % node.name,
                    UserWarning,
                    stacklevel=2,
                )
        if any(d.name == "batch" for d in node.decorators):
            raise NotSupportedException(
                "Step *%s* uses @batch which is not supported with Kestra. "
                "Use @kubernetes or remove the decorator." % node.name
            )
        if any(d.name == "slurm" for d in node.decorators):
            raise NotSupportedException(
                "Step *%s* uses @slurm which is not supported with Kestra." % node.name
            )

    for bad in ("trigger", "trigger_on_finish", "exit_hook"):
        decos = getattr(flow, "_flow_decorators", {}).get(bad)
        if decos:
            raise NotSupportedException(
                "@%s is not supported with Kestra." % bad
            )

    # Validate foreach: no nested foreach
    _validate_foreach(graph)


def _validate_foreach(graph):
    """Verify no nested foreach (foreach inside foreach body)."""
    def _traverse(node, inside_foreach):
        if node.type == "foreach" and inside_foreach:
            raise NotSupportedException(
                "Step *%s* is a foreach step inside another foreach. "
                "Nested foreach is not supported with Kestra "
                "(Kestra's EachSequential/concurrentEach tasks do not support nesting)." % node.name
            )
        new_inside = inside_foreach or (node.type == "foreach")
        if node.type in ("start", "linear", "join", "foreach", "split", "split-switch"):
            for next_step in node.out_funcs:
                _traverse(graph[next_step], new_inside)

    _traverse(graph["start"], False)


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
# create
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
    help="Maximum seconds a single execution may run (not enforced by Kestra itself).",
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

    obj.echo("Compiling *%s* to Kestra YAML..." % obj.kestra_flow_name, bold=True)

    compiler = _build_compiler(
        obj, kestra_namespace, max_workers, with_decorators, workflow_timeout,
        branch, production, namespace=namespace, tags=tags,
    )
    yaml_content = compiler.compile()

    with open(file, "w") as f:
        f.write(yaml_content)

    obj.echo(
        "Flow *{flow_name}* compiled to Kestra YAML successfully → *{file}*\n"
        "Deploy with: python {flow_file} kestra create".format(
            flow_name=obj.kestra_flow_name,
            file=file,
            flow_file=os.path.basename(sys.argv[0]),
        ),
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

    obj.echo("Compiling *%s* to Kestra YAML..." % obj.kestra_flow_name, bold=True)

    compiler = _build_compiler(
        obj, kestra_namespace, max_workers, with_decorators, workflow_timeout,
        branch, production, namespace=namespace, tags=tags,
    )
    yaml_content = compiler.compile()

    client = _make_client(kestra_host, kestra_user, kestra_password, kestra_token)
    _deploy_flow(client, yaml_content, kestra_namespace, obj)

    if deployer_attribute_file:
        with open(deployer_attribute_file, "w") as f:
            json.dump(
                {
                    "name": obj.kestra_flow_name,
                    "flow_name": obj.flow.name,
                    "metadata": "{}",
                    "additional_info": {
                        "flow_id": flow_id,
                        "kestra_namespace": kestra_namespace,
                        "kestra_host": kestra_host,
                        "kestra_user": kestra_user,
                        "kestra_password": kestra_password,
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

    obj.echo("Compiling *%s*..." % obj.kestra_flow_name, bold=True)

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
    execution_url = "%s/ui/executions/%s/%s/%s" % (
        kestra_host, kestra_namespace, flow_id, execution_id
    )
    obj.echo("Execution started: *%s*" % execution_url)

    if wait:
        obj.echo("Waiting for execution to complete...")
        final_state = _wait_for_execution(client, execution_id, obj)
        if final_state == "SUCCESS":
            obj.echo("Execution *%s* completed successfully." % execution_id, bold=True)
        else:
            raise KestraException(
                "Execution %s finished with state: %s\nURL: %s"
                % (execution_id, final_state, execution_url)
            )
    else:
        obj.echo("Execution ID: %s" % execution_id)
        obj.echo("Track it at: %s" % execution_url)


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

    obj.echo("Triggering execution of *%s* in namespace *%s*..." % (flow_id, kestra_namespace), bold=True)
    execution_id = _trigger_execution(client, kestra_namespace, flow_id, inputs=params or None)
    execution_url = "%s/ui/executions/%s/%s/%s" % (
        kestra_host, kestra_namespace, flow_id, execution_id
    )
    obj.echo("Execution started: *%s*" % execution_url)

    if deployer_attribute_file:
        import hashlib as _hashlib
        _run_id = "kestra-" + _hashlib.md5(execution_id.encode()).hexdigest()[:16]
        pathspec = "%s/%s" % (obj.flow.name, _run_id)
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
        branch=branch,
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
        session.headers["Authorization"] = "Bearer %s" % token
    elif user and password:
        session.auth = (user, password)
    session.headers["Content-Type"] = "application/x-yaml"
    session._kestra_host = host
    return session


def _deploy_flow(client, yaml_content: str, kestra_namespace: str, obj):
    """Create or update a Kestra flow via the REST API."""
    import re as _re
    host = client._kestra_host
    url = "%s/api/v1/flows" % host
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
            put_url = "%s/api/v1/flows/%s/%s" % (host, ns, fid)
            resp2 = client.put(put_url, data=yaml_content.encode("utf-8"))
            if resp2.status_code in (200, 201):
                obj.echo("Flow updated successfully on Kestra.")
                return
            raise KestraException(
                "Failed to update flow on Kestra (HTTP %d): %s"
                % (resp2.status_code, resp2.text[:500])
            )
        raise KestraException(
            "Failed to deploy flow to Kestra (HTTP %d): %s"
            % (resp.status_code, resp.text[:500])
        )
    except Exception as exc:
        if isinstance(exc, KestraException):
            raise
        raise KestraException("Failed to connect to Kestra at %s: %s" % (host, exc))


def _trigger_execution(client, kestra_namespace: str, flow_id: str, inputs: dict = None) -> str:
    """Trigger a flow execution and return the execution ID.

    Parameters
    ----------
    inputs : dict, optional
        Flow input values (Metaflow Parameters). Sent as multipart/form-data
        because Kestra returns 404 when Content-Type is application/x-yaml.
    """
    host = client._kestra_host
    url = "%s/api/v1/executions/%s/%s" % (host, kestra_namespace, flow_id)
    try:
        if inputs:
            # Kestra requires multipart/form-data for inputs; url-encoded returns 404.
            files = {k: (None, str(v)) for k, v in inputs.items()}
            resp = client.post(url, files=files)
        else:
            # Clear Content-Type so requests sends a plain POST with no body.
            # Kestra returns 404 if Content-Type is set (e.g. application/x-yaml from session).
            resp = client.post(url, headers={"Content-Type": None})
        if resp.status_code not in (200, 201):
            raise KestraException(
                "Failed to trigger execution (HTTP %d): %s"
                % (resp.status_code, resp.text[:500])
            )
        return resp.json()["id"]
    except Exception as exc:
        if isinstance(exc, KestraException):
            raise
        raise KestraException("Failed to trigger execution: %s" % exc)


def _wait_for_execution(client, execution_id: str, obj, poll_interval: int = 5) -> str:
    """Poll until the execution reaches a terminal state and return the state."""
    host = client._kestra_host
    terminal_states = {"SUCCESS", "FAILED", "KILLED", "WARNING"}
    url = "%s/api/v1/executions/%s" % (host, execution_id)

    seen_running = False
    client.headers.pop("Content-Type", None)  # GET doesn't need Content-Type

    while True:
        try:
            resp = client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                state = data.get("state", {}).get("current", "UNKNOWN")
                if not seen_running and state == "RUNNING":
                    obj.echo("Execution is running...")
                    seen_running = True
                if state in terminal_states:
                    return state
            else:
                obj.echo("Warning: could not poll execution status (HTTP %d)" % resp.status_code)
        except Exception as exc:
            obj.echo("Warning: error polling execution: %s" % exc)

        time.sleep(poll_interval)
