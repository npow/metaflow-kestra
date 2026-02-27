"""
Kestra YAML compiler for Metaflow flows.

Converts a Metaflow FlowGraph into a complete Kestra flow YAML definition.
Each Metaflow step becomes a `io.kestra.plugin.scripts.python.Script` task.
Foreach fan-out uses `io.kestra.plugin.core.flow.ForEach` and split/join
uses `io.kestra.plugin.core.flow.Parallel`.

Generated flow structure:
  metaflow_init        - computes a stable run_id from Kestra's execution.id
  metaflow_params      - initialises flow parameters (only if flow has params)
  <step>...            - one task per Metaflow step, with nesting for
                         foreach (ForEach) and split/join (Parallel)

Artifact hints are embedded in every task output so that users can copy
the Metaflow Client snippet directly from the Kestra UI.
"""

import json
import os
import sys
from datetime import datetime
from textwrap import indent
from typing import List, Optional

from metaflow.exception import MetaflowException

from .exception import KestraException, NotSupportedException

try:
    from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
except ImportError:
    get_run_time_limit_for_task = None


# ---------------------------------------------------------------------------
# YAML helpers
# ---------------------------------------------------------------------------

def _yaml_str(s: str) -> str:
    """Return a YAML-safe single-line string (double-quoted)."""
    # Escape backslashes and double-quotes, then wrap in double quotes.
    escaped = s.replace("\\", "\\\\").replace('"', '\\"')
    return '"%s"' % escaped


def _yaml_block(text: str, indent_level: int = 4) -> str:
    """Return text as a YAML literal block scalar with the given indent."""
    pad = " " * indent_level
    lines = [pad + "  |"]
    for line in text.splitlines():
        lines.append(pad + "  " + line if line.strip() else "")
    return "\n".join(lines)


def _iso_duration(seconds: int) -> str:
    """Convert seconds to ISO 8601 duration string (e.g. PT3600S)."""
    if seconds < 60:
        return "PT%dS" % seconds
    minutes = seconds // 60
    if minutes < 60:
        return "PT%dM" % minutes
    hours = minutes // 60
    remaining_minutes = minutes % 60
    if remaining_minutes:
        return "PT%dH%dM" % (hours, remaining_minutes)
    return "PT%dH" % hours


# ---------------------------------------------------------------------------
# Compiler
# ---------------------------------------------------------------------------

class KestraCompiler:
    """Compiles a Metaflow flow into a Kestra YAML definition."""

    # Internal task IDs — chosen to avoid clashes with typical step names.
    INIT_TASK_ID = "metaflow_init"
    PARAMS_TASK_ID = "metaflow_params"

    def __init__(
        self,
        name: str,
        graph,
        flow,
        flow_file: str,
        metadata,
        flow_datastore,
        environment,
        event_logger,
        monitor,
        tags: Optional[List[str]] = None,
        namespace: Optional[str] = None,
        username: Optional[str] = None,
        max_workers: int = 10,
        with_decorators: Optional[List[str]] = None,
        workflow_timeout: Optional[int] = None,
        kestra_namespace: str = "metaflow",
        branch: Optional[str] = None,
        production: bool = False,
    ):
        self.name = name
        self.graph = graph
        self.flow = flow
        self.flow_file = flow_file
        self.metadata = metadata
        self.flow_datastore = flow_datastore
        self.environment = environment
        self.event_logger = event_logger
        self.monitor = monitor
        self.tags = tags or []
        self.namespace = namespace
        self.username = username or ""
        self.max_workers = max_workers
        self.with_decorators = with_decorators or []
        self.workflow_timeout = workflow_timeout
        self.kestra_namespace = kestra_namespace
        self.branch = branch
        self.production = production

        self._project_info = self._get_project()
        self._flow_name = (
            self._project_info["flow_name"] if self._project_info else name
        )

        # Merge tags with project tags
        self._tags = list(self.tags)
        if self._project_info:
            self._tags += [
                "project:%s" % self._project_info["name"],
                "project_branch:%s" % self._project_info["branch"],
            ]

        # Runtime provider info
        self._metadata_type = metadata.TYPE
        self._datastore_type = flow_datastore.TYPE
        self._datastore_root = (
            getattr(flow_datastore, "datastore_root", None) or ""
        )
        self._environment_type = environment.TYPE
        self._event_logger_type = event_logger.TYPE
        self._monitor_type = monitor.TYPE

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compile(self) -> str:
        """Return the complete Kestra flow YAML as a string."""
        sections = []
        sections.append(self._render_header())
        sections.append(self._render_variables())
        params = self._get_parameters()
        if params:
            sections.append(self._render_inputs(params))
        sections.append(self._render_plugin_defaults())
        schedule = self._get_schedule()
        if schedule:
            sections.append(self._render_triggers(schedule))
        sections.append(self._render_tasks())
        return "\n".join(sections) + "\n"

    # ------------------------------------------------------------------
    # Top-level section renderers
    # ------------------------------------------------------------------

    def _render_header(self) -> str:
        flow_id = self._flow_name.lower().replace(".", "-").replace("_", "-")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        flow_file_base = os.path.basename(self.flow_file)
        lines = [
            "id: %s" % flow_id,
            "namespace: %s" % self.kestra_namespace,
            "description: |",
            "  Generated by metaflow-kestra on %s." % now,
            "  Regenerate: python %s kestra create" % flow_file_base,
            "  Flow: %s" % self._flow_name,
        ]
        if self.flow.__doc__:
            for doc_line in self.flow.__doc__.strip().splitlines():
                lines.append("  %s" % doc_line)
        lines += [
            "labels:",
            "  metaflow.flow: %s" % self._flow_name,
            "  metaflow.generated: \"true\"",
        ]
        if self._project_info:
            lines.append("  metaflow.project: %s" % self._project_info["name"])
            lines.append("  metaflow.branch: %s" % self._project_info["branch"])
        return "\n".join(lines)

    def _render_variables(self) -> str:
        lines = ["variables:"]
        lines.append("  FLOW_FILE: %s" % self.flow_file)
        lines.append("  FLOW_NAME: %s" % self._flow_name)
        lines.append("  METADATA_TYPE: %s" % self._metadata_type)
        lines.append("  DATASTORE_TYPE: %s" % self._datastore_type)
        lines.append("  DATASTORE_ROOT: %s" % (self._datastore_root or "~/.metaflow"))
        lines.append("  ENVIRONMENT_TYPE: %s" % self._environment_type)
        lines.append("  EVENT_LOGGER_TYPE: %s" % self._event_logger_type)
        lines.append("  MONITOR_TYPE: %s" % self._monitor_type)
        lines.append("  KESTRA_NAMESPACE: %s" % self.kestra_namespace)
        # Collect any METAFLOW_SERVICE_* vars from the current environment
        for key, val in os.environ.items():
            if key.startswith("METAFLOW_SERVICE") or key.startswith("METAFLOW_DEFAULT"):
                lines.append("  %s: %s" % (key, val))
        return "\n".join(lines)

    def _render_inputs(self, params: dict) -> str:
        lines = ["inputs:"]
        for var, param in params.items():
            default = param.get("default")
            ptype = self._infer_kestra_type(default)
            lines.append("  - id: %s" % var)
            lines.append("    type: %s" % ptype)
            if default is not None:
                lines.append("    defaults: %s" % json.dumps(default))
        return "\n".join(lines)

    def _render_plugin_defaults(self) -> str:
        lines = [
            "pluginDefaults:",
            "  - type: io.kestra.plugin.scripts.python.Script",
            "    values:",
            "      warningOnStdErr: false",
            "      taskRunner:",
            "        type: io.kestra.plugin.core.runner.Process",
            "      beforeCommands:",
            "        - pip show kestra >/dev/null 2>&1 || pip install kestra --quiet 2>&1 | tail -3",
            "        - pip show metaflow >/dev/null 2>&1 || pip install metaflow --quiet 2>&1 | tail -3",
        ]
        return "\n".join(lines)

    def _render_triggers(self, schedule: dict) -> str:
        lines = ["triggers:"]
        cron = schedule.get("cron")
        if cron:
            lines.append("  - id: schedule")
            lines.append("    type: io.kestra.plugin.core.trigger.Schedule")
            lines.append("    cron: \"%s\"" % cron)
            if schedule.get("timezone"):
                lines.append("    timezone: %s" % schedule["timezone"])
        return "\n".join(lines)

    def _render_tasks(self) -> str:
        lines = ["tasks:"]
        # Init task — always first; runs metaflow init to create _parameters artifact
        lines.append(self._render_init_task(indent=2))
        # Walk the graph
        tasks_yaml = []
        visited = set()
        self._visit_node("start", tasks_yaml, visited, indent=2, context={})
        lines.extend(tasks_yaml)
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Graph walker
    # ------------------------------------------------------------------

    def _visit_node(
        self,
        step_name: str,
        out: list,
        visited: set,
        indent: int,
        context: dict,
    ):
        """Recursively visit graph nodes and emit YAML task definitions."""
        if step_name in visited:
            return
        visited.add(step_name)

        node = self.graph[step_name]
        ntype = node.type

        if ntype == "end":
            out.append(self._render_step_task(node, indent=indent, context=context))
            return

        if ntype in ("start", "linear"):
            out.append(self._render_step_task(node, indent=indent, context=context))
            for next_step in node.out_funcs:
                self._visit_node(next_step, out, visited, indent=indent, context=context)

        elif ntype == "foreach":
            # Emit the foreach parent step itself
            out.append(self._render_step_task(node, indent=indent, context=context))
            # The foreach body is the first (and only) out_func
            body_step = node.out_funcs[0]
            body_node = self.graph[body_step]
            # Emit a ForEach wrapper around the body step
            out.append(
                self._render_foreach_wrapper(node, body_node, indent=indent)
            )
            # After foreach body, continue with the join step
            join_step = body_node.out_funcs[0]
            visited.add(body_step)  # body is inside ForEach, don't emit top-level
            self._visit_node(join_step, out, visited, indent=indent, context=context)

        elif ntype == "split":
            # First run the split step itself (it may compute data before branching)
            out.append(self._render_step_task(node, indent=indent, context=context))
            # Then emit the Parallel wrapper for the branches
            out.append(
                self._render_parallel_wrapper(node, out_parent=out, visited=visited, indent=indent)
            )
            # Find the join node for this split and continue from there
            join_step = self._find_join_step(step_name)
            if join_step:
                self._visit_node(join_step, out, visited, indent=indent, context=context)

        elif ntype == "join":
            out.append(self._render_step_task(node, indent=indent, context=context))
            for next_step in node.out_funcs:
                self._visit_node(next_step, out, visited, indent=indent, context=context)

    # ------------------------------------------------------------------
    # Task renderers
    # ------------------------------------------------------------------

    def _render_init_task(self, indent: int) -> str:
        pad = " " * indent
        script = self._init_script()
        return self._task_block(
            task_id=self.INIT_TASK_ID,
            task_type="io.kestra.plugin.scripts.python.Script",
            script=script,
            indent=indent,
        )

    def _render_params_task(self, params: dict, indent: int) -> str:
        script = self._params_script(params)
        return self._task_block(
            task_id=self.PARAMS_TASK_ID,
            task_type="io.kestra.plugin.scripts.python.Script",
            script=script,
            indent=indent,
        )

    def _render_step_task(self, node, indent: int, context: dict) -> str:
        script = self._step_script(node, context)
        timeout = self._get_timeout(node)
        retries = self._get_retries(node)
        retry_delay = self._get_retry_delay(node)

        extras = {}
        if timeout:
            extras["timeout"] = _iso_duration(timeout)
        if retries > 0:
            extras["retry"] = {
                "type": "constant",
                "interval": _iso_duration(retry_delay),
                "maxAttempt": retries,
            }

        return self._task_block(
            task_id=node.name,
            task_type="io.kestra.plugin.scripts.python.Script",
            script=script,
            indent=indent,
            extras=extras,
        )

    def _render_foreach_wrapper(self, parent_node, body_node, indent: int) -> str:
        """Emit a ForEach task wrapping the body step."""
        pad = " " * indent
        wrapper_id = "foreach_%s" % parent_node.name
        body_script = self._step_script(body_node, context={}, is_foreach_body=True, foreach_parent=parent_node.name)
        timeout = self._get_timeout(body_node)
        retries = self._get_retries(body_node)
        retry_delay = self._get_retry_delay(body_node)

        lines = [
            "%s- id: %s" % (pad, wrapper_id),
            "%s  type: io.kestra.plugin.core.flow.ForEach" % pad,
            "%s  values: \"{{ outputs.%s.vars.foreach_values }}\"" % (pad, parent_node.name),
            "%s  concurrencyLimit: %d" % (pad, self.max_workers),
            "%s  tasks:" % pad,
        ]

        body_lines = []
        body_extras = {}
        if timeout:
            body_extras["timeout"] = _iso_duration(timeout)
        if retries > 0:
            body_extras["retry"] = {
                "type": "constant",
                "interval": _iso_duration(retry_delay),
                "maxAttempt": retries,
            }

        body_yaml = self._task_block(
            task_id=body_node.name,
            task_type="io.kestra.plugin.scripts.python.Script",
            script=body_script,
            indent=indent + 4,
            extras=body_extras,
        )
        lines.append(body_yaml)
        return "\n".join(lines)

    def _render_parallel_wrapper(self, split_node, out_parent: list, visited: set, indent: int) -> str:
        """Emit a Parallel task containing all branch tasks for a split node."""
        pad = " " * indent
        wrapper_id = "parallel_%s" % split_node.name

        lines = [
            "%s- id: %s" % (pad, wrapper_id),
            "%s  type: io.kestra.plugin.core.flow.Parallel" % pad,
            "%s  tasks:" % pad,
        ]

        for branch_step in split_node.out_funcs:
            # Walk the branch until we hit a join node
            branch_tasks = []
            self._visit_branch(branch_step, branch_tasks, visited, indent + 4)
            lines.extend(branch_tasks)

        return "\n".join(lines)

    def _visit_branch(self, step_name: str, out: list, visited: set, indent: int):
        """Visit steps in a split branch until reaching a join node."""
        if step_name in visited:
            return
        node = self.graph[step_name]
        if node.type == "join":
            return
        visited.add(step_name)
        out.append(self._render_step_task(node, indent=indent, context={}))
        for next_step in node.out_funcs:
            next_node = self.graph[next_step]
            if next_node.type != "join":
                self._visit_branch(next_step, out, visited, indent)

    # ------------------------------------------------------------------
    # Script generators
    # ------------------------------------------------------------------

    def _init_script(self) -> str:
        params = self._get_parameters()
        # Build param args for init command (parameters are passed via Kestra inputs)
        param_args_lines = []
        for var in params:
            param_args_lines.append('    "--%(var)s", str(params["%(var)s"]),' % {"var": var})
        param_args_str = "\n".join(param_args_lines)

        if params:
            params_items = ['    "%(var)s": "{{ inputs.%(var)s }}"' % {"var": var} for var in params]
            params_block = 'params = {\n' + ',\n'.join(params_items) + '\n}'
        else:
            params_block = "params = {}"

        return """\
import hashlib, os, subprocess, sys
from kestra import Kestra

execution_id = "{{ execution.id }}"
run_id = "kestra-" + hashlib.md5(execution_id.encode()).hexdigest()[:16]
params_task_id = run_id + "-params"
%(params_block)s

# Allow runtime override of local datastore root via environment variable.
# METAFLOW_DATASTORE_SYSROOT_LOCAL is the *parent* of the .metaflow directory.
_datastore_root = "{{ vars.DATASTORE_ROOT }}"
if "{{ vars.DATASTORE_TYPE }}" == "local":
    _sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
    if _sysroot:
        _datastore_root = os.path.join(_sysroot, ".metaflow")

# Run metaflow init to create the _parameters artifact (required by all start steps)
cmd = [
    sys.executable, "{{ vars.FLOW_FILE }}",
    "--quiet", "--no-pylint",
    "--metadata={{ vars.METADATA_TYPE }}",
    "--datastore={{ vars.DATASTORE_TYPE }}",
    "--datastore-root", _datastore_root,
    "init",
    "--run-id", run_id,
    "--task-id", params_task_id,
%(param_args)s
]
result = subprocess.run(cmd, capture_output=True, text=True)
if result.stdout:
    print(result.stdout[-2000:])
if result.stderr:
    print(result.stderr[-2000:], file=sys.stderr)
if result.returncode != 0:
    raise RuntimeError(
        "Metaflow init failed (exit %%d):\\nSTDOUT: %%s\\nSTDERR: %%s"
        %% (result.returncode, result.stdout[-1000:], result.stderr[-1000:])
    )

Kestra.outputs({"run_id": run_id, "params_task_id": params_task_id})
""" % {"params_block": params_block, "param_args": param_args_str}

    def _step_script(
        self,
        node,
        context: dict,
        is_foreach_body: bool = False,
        foreach_parent: Optional[str] = None,
    ) -> str:
        """Generate the Python script for a single Metaflow step task."""
        step_name = node.name
        ntype = node.type
        flow_file = self.flow_file
        flow_name = self._flow_name
        meta = self._metadata_type
        ds = self._datastore_type
        max_retries = self._get_retries(node)

        # ----------------------------------------------------------------
        # Build input_paths expression
        # ----------------------------------------------------------------
        input_paths_expr = self._build_input_paths_expr(node, is_foreach_body, foreach_parent)

        # ----------------------------------------------------------------
        # Build task_id expression
        # ----------------------------------------------------------------
        if is_foreach_body:
            task_id_expr = (
                '"{{ outputs.metaflow_init.vars.run_id }}" + "-%(step_name)s-" + str(int("{{ taskrun.value }}"))'
                % {"step_name": step_name}
            )
        else:
            task_id_expr = (
                '"{{ outputs.metaflow_init.vars.run_id }}" + "-%s"' % step_name
            )

        # ----------------------------------------------------------------
        # Build top-level CLI args
        # ----------------------------------------------------------------
        top_args = [
            '"--quiet"',
            '"--no-pylint"',
            '"--metadata={{ vars.METADATA_TYPE }}"',
            '"--datastore={{ vars.DATASTORE_TYPE }}"',
            '"--datastore-root=" + DATASTORE_ROOT',
            '"--environment={{ vars.ENVIRONMENT_TYPE }}"',
            '"--with=kestra_internal"',
        ]
        for deco in self.with_decorators:
            top_args.append('"--with=%s"' % deco)
        for tag in self._tags:
            top_args.append('"--tag=%s"' % tag)
        # Add any step-level decorator specs (e.g. @kubernetes forwarded via --with)
        for deco_spec in self._get_decorator_specs(node):
            top_args.append('"--with=%s"' % deco_spec)
        top_args_str = ", ".join(top_args)

        # ----------------------------------------------------------------
        # Build step CLI args
        # ----------------------------------------------------------------
        step_args = [
            '"step"',
            '"%s"' % step_name,
            '"--run-id"', 'run_id',
            '"--task-id"', 'task_id',
            '"--retry-count"', '"0"',
            '"--max-user-code-retries"', '"%d"' % max_retries,
        ]
        if is_foreach_body:
            step_args += ['"--split-index"', 'str(split_index)']
        step_args_str = ", ".join(step_args)

        # ----------------------------------------------------------------
        # Build env overrides (from @environment decorator)
        # ----------------------------------------------------------------
        env_deco = [d for d in node.decorators if d.name == "environment"]
        env_overrides = {}
        if env_deco:
            env_overrides.update(env_deco[0].attributes.get("vars", {}))
        env_lines = []
        for k, v in env_overrides.items():
            env_lines.append('    "%(k)s": "%(v)s",' % {"k": k, "v": v})
        env_overrides_str = "\n".join(env_lines)

        # ----------------------------------------------------------------
        # Foreach outputs block
        # ----------------------------------------------------------------
        foreach_outputs = ""
        if ntype == "foreach":
            foreach_outputs = """\
if out.get("foreach_cardinality", 0) > 0:
    fc = out["foreach_cardinality"]
    kestra_out["foreach_count"] = fc
    kestra_out["foreach_values"] = list(range(fc))
"""

        init_input_block = ""

        # ----------------------------------------------------------------
        # Foreach body: split_index assignment
        # ----------------------------------------------------------------
        split_index_block = ""
        if is_foreach_body:
            split_index_block = 'split_index = int("{{ taskrun.value }}")'

        # ----------------------------------------------------------------
        # Artifact hint block
        # ----------------------------------------------------------------
        artifact_hint_block = """\
try:
    import metaflow as _mf
    _old_root = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
    # The metaflow client API expects METAFLOW_DATASTORE_SYSROOT_LOCAL to be the
    # *parent* of the .metaflow directory (it appends ".metaflow" internally).
    os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = os.path.dirname(DATASTORE_ROOT)
    try:
        _mf.namespace(None)
        _task = _mf.Task("%(flow_name)s/%%s/%(step_name)s/%%s" %% (run_id, task_id))
        _names = [a.id for a in _task.artifacts if not a.id.startswith("_")]
        if _names:
            kestra_out["metaflow_artifacts"] = ", ".join(_names)
            kestra_out["metaflow_snippet"] = (
                "from metaflow import Task\\n"
                "task = Task(\\'%(flow_name)s/%%s/%(step_name)s/%%s\\' %% (run_id, task_id))\\n"
                + "\\n".join("# task.data.%%s  # or task[\\'%%s\\'].data" %% (n, n) for n in _names)
            )
    except Exception:
        pass
    finally:
        if _old_root is None:
            os.environ.pop("METAFLOW_DATASTORE_SYSROOT_LOCAL", None)
        else:
            os.environ["METAFLOW_DATASTORE_SYSROOT_LOCAL"] = _old_root
except Exception:
    pass
""" % {"flow_name": flow_name, "step_name": step_name}

        # ----------------------------------------------------------------
        # Foreach join: build input_paths from all body task IDs
        # ----------------------------------------------------------------
        join_input_block = ""
        if ntype == "join":
            split_parents = getattr(node, "split_parents", [])
            if split_parents:
                innermost = split_parents[-1]
                innermost_node = self.graph[innermost]
                if innermost_node.type == "foreach":
                    # foreach join — body tasks have deterministic IDs
                    body_step = node.in_funcs[0]
                    join_input_block = """\
# Foreach join: reconstruct input paths from body task IDs
foreach_count = int("{{ outputs.%(parent)s.vars.foreach_count }}")
body_task_ids = [run_id + "-%(body_step)s-" + str(i) for i in range(foreach_count)]
input_paths = ",".join(
    run_id + "/%(body_step)s/" + tid for tid in body_task_ids
)
""" % {"parent": innermost, "body_step": body_step}
                else:
                    # split join — collect input_paths from all branches
                    branch_input_parts = []
                    for branch in node.in_funcs:
                        branch_input_parts.append(
                            '"{{ outputs.%(b)s.vars.input_path }}"' % {"b": branch}
                        )
                    join_input_block = (
                        "input_paths = \",\".join([%s])\n" % ", ".join(branch_input_parts)
                    )

        # ----------------------------------------------------------------
        # Assemble the script
        # ----------------------------------------------------------------
        if is_foreach_body:
            input_paths_line = 'input_paths = "{{ outputs.%(parent)s.vars.input_path }}"' % {"parent": foreach_parent}
        elif join_input_block:
            input_paths_line = ""  # handled in join_input_block
        else:
            input_paths_line = "input_paths = %s" % input_paths_expr

        script = """\
import json, os, subprocess, sys, tempfile
from kestra import Kestra

FLOW_FILE = "{{ vars.FLOW_FILE }}"
METADATA_TYPE = "{{ vars.METADATA_TYPE }}"
DATASTORE_TYPE = "{{ vars.DATASTORE_TYPE }}"
DATASTORE_ROOT = "{{ vars.DATASTORE_ROOT }}"
if DATASTORE_TYPE == "local":
    _sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
    if _sysroot:
        DATASTORE_ROOT = os.path.join(_sysroot, ".metaflow")

run_id = "{{ outputs.metaflow_init.vars.run_id }}"
task_id = %(task_id_expr)s
%(split_index_block)s
%(input_paths_line)s
%(join_input_block)s%(init_input_block)s
output_fd, output_file = tempfile.mkstemp(suffix=".json")
os.close(output_fd)

env = dict(os.environ)
env.update({
    "METAFLOW_KESTRA_OUTPUT_FILE": output_file,
    "METAFLOW_KESTRA_EXECUTION_ID": "{{ execution.id }}",
    "METAFLOW_KESTRA_NAMESPACE": "{{ vars.KESTRA_NAMESPACE }}",
    "METAFLOW_KESTRA_FLOW_ID": "{{ flow.id }}",
%(env_overrides)s
})

cmd = [
    sys.executable, FLOW_FILE,
    %(top_args)s,
    %(step_args)s,
]
if input_paths:
    cmd += ["--input-paths", input_paths]
result = subprocess.run(cmd, env=env, capture_output=True, text=True)
if result.stdout:
    print(result.stdout[-3000:])
if result.stderr:
    print(result.stderr[-3000:], file=sys.stderr)
if result.returncode != 0:
    raise RuntimeError(
        "Step '%(step_name)s' failed (exit %%d):\\nSTDOUT: %%s\\nSTDERR: %%s"
        %% (result.returncode, result.stdout[-2000:], result.stderr[-2000:])
    )

out = {}
if os.path.exists(output_file) and os.path.getsize(output_file) > 0:
    try:
        with open(output_file) as _f:
            out = json.load(_f)
    except Exception:
        pass
try:
    os.unlink(output_file)
except OSError:
    pass

kestra_out = {
    "task_id": task_id,
    "input_path": run_id + "/%(step_name)s/" + task_id,
}
%(foreach_outputs)s
%(artifact_hint)s
Kestra.outputs(kestra_out)
""" % {
            "task_id_expr": task_id_expr,
            "split_index_block": split_index_block,
            "input_paths_line": input_paths_line,
            "join_input_block": join_input_block,
            "init_input_block": init_input_block,
            "env_overrides": env_overrides_str,
            "top_args": top_args_str,
            "step_args": step_args_str,
            "step_name": step_name,
            "foreach_outputs": foreach_outputs,
            "artifact_hint": artifact_hint_block,
        }

        # Clean up extra blank lines
        import re
        script = re.sub(r"\n{3,}", "\n\n", script)
        return script.strip()

    # ------------------------------------------------------------------
    # YAML block builder
    # ------------------------------------------------------------------

    def _task_block(
        self,
        task_id: str,
        task_type: str,
        script: str,
        indent: int,
        extras: Optional[dict] = None,
    ) -> str:
        """Render a complete YAML task block."""
        pad = " " * indent
        lines = [
            "%s- id: %s" % (pad, task_id),
            "%s  type: %s" % (pad, task_type),
        ]
        if extras:
            for k, v in extras.items():
                if isinstance(v, dict):
                    lines.append("%s  %s:" % (pad, k))
                    for vk, vv in v.items():
                        lines.append("%s    %s: %s" % (pad, vk, json.dumps(vv)))
                else:
                    lines.append("%s  %s: %s" % (pad, k, v))

        # Emit script as a literal block scalar
        lines.append("%s  script: |" % pad)
        for sline in script.splitlines():
            lines.append("%s    %s" % (pad, sline) if sline.strip() else "")

        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Input path expression builder
    # ------------------------------------------------------------------

    def _build_input_paths_expr(
        self, node, is_foreach_body: bool, foreach_parent: Optional[str]
    ) -> str:
        """Return a Python expression (as a string) for the input_paths value."""
        step_name = node.name
        ntype = node.type
        in_funcs = list(node.in_funcs)
        split_parents = list(getattr(node, "split_parents", []))

        if step_name == "start":
            # Always use the _parameters artifact created by metaflow_init
            return (
                '"{{ outputs.metaflow_init.vars.run_id }}"'
                ' + "/_parameters/" + '
                '"{{ outputs.metaflow_init.vars.params_task_id }}"'
            )

        if is_foreach_body:
            # Handled directly in _step_script
            return '"{{ outputs.%s.vars.input_path }}"' % foreach_parent

        if ntype == "join" and split_parents:
            # Handled via join_input_block in _step_script
            return '""'

        if len(in_funcs) == 1:
            return '"{{ outputs.%s.vars.input_path }}"' % in_funcs[0]

        # Multiple parents (shouldn't happen for non-join after-split, but handle gracefully)
        parts = ['\"{{ outputs.%s.vars.input_path }}\"' % p for p in in_funcs]
        return '",".join([%s])' % ", ".join(parts)

    # ------------------------------------------------------------------
    # Graph utilities
    # ------------------------------------------------------------------

    def _find_join_step(self, split_step_name: str) -> Optional[str]:
        """Find the join step corresponding to a split step."""
        for node in self.graph:
            if node.type == "join":
                parents = list(getattr(node, "split_parents", []))
                if parents and parents[-1] == split_step_name:
                    return node.name
        return None

    # ------------------------------------------------------------------
    # Decorator / metadata extraction
    # ------------------------------------------------------------------

    def _get_parameters(self) -> dict:
        params = {}
        for var, param in self.flow._get_parameters():
            default = param.kwargs.get("default")
            if callable(default):
                default = None
            params[var] = {"name": param.name, "default": default}
        return params

    def _get_schedule(self) -> Optional[dict]:
        try:
            flow_decos = getattr(self.flow, "_flow_decorators", {})
            schedule_list = flow_decos.get("schedule", [])
            if not schedule_list:
                return None
            d = schedule_list[0]
            raw = getattr(d, "schedule", None)
            timezone = getattr(d, "timezone", None)
            if isinstance(raw, dict):
                cron = raw.get("cron")
                if timezone is None:
                    timezone = raw.get("timezone")
            else:
                cron = raw
            if cron:
                return {"cron": cron, "timezone": timezone}
        except Exception:
            pass
        return None

    def _get_project(self) -> Optional[dict]:
        try:
            from metaflow.plugins.project_decorator import format_name

            flow_decos = getattr(self.flow, "_flow_decorators", {})
            project_list = flow_decos.get("project", [])
            if not project_list:
                return None
            d = project_list[0]
            project_name = d.attributes.get("name")
            if not project_name:
                return None
            project_flow_name, branch_name = format_name(
                self.name,
                project_name,
                self.production,
                self.branch,
                self.username or "",
            )
            return {
                "name": project_name,
                "flow_name": project_flow_name,
                "branch": branch_name,
            }
        except Exception:
            return None

    def _get_timeout(self, node) -> Optional[int]:
        try:
            if get_run_time_limit_for_task is not None:
                limit = get_run_time_limit_for_task(node.decorators)
                if limit:
                    return limit
        except Exception:
            pass
        return None

    def _get_retries(self, node) -> int:
        for deco in node.decorators:
            if deco.name == "retry":
                return int(deco.attributes.get("times", 0))
        return 0

    def _get_retry_delay(self, node) -> int:
        for deco in node.decorators:
            if deco.name == "retry":
                minutes = float(deco.attributes.get("minutes_between_retries", 2))
                return int(minutes * 60)
        return 120

    def _get_decorator_specs(self, node) -> list:
        """Return --with-compatible spec strings for user-defined step decorators
        that should be forwarded as subcommand flags."""
        _skip = {
            "kestra_internal", "retry", "timeout", "environment",
            "project", "trigger", "trigger_on_finish", "schedule", "card",
        }
        specs = []
        for d in node.decorators:
            if d.name in _skip:
                continue
            try:
                spec = d.make_decorator_spec()
                if spec:
                    specs.append(spec)
            except Exception:
                pass
        return specs

    @staticmethod
    def _infer_kestra_type(default) -> str:
        if isinstance(default, bool):
            return "BOOLEAN"
        if isinstance(default, int):
            return "INT"
        if isinstance(default, float):
            return "FLOAT"
        return "STRING"
