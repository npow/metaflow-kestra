"""
Kestra YAML compiler for Metaflow flows.

Converts a Metaflow FlowGraph into a complete Kestra flow YAML definition.
Each Metaflow step becomes an `io.kestra.plugin.scripts.python.Script` task.

Supported Metaflow graph patterns:

  linear       - sequential steps
  split/join   - io.kestra.plugin.core.flow.Parallel wraps the branches
  foreach      - io.kestra.plugin.core.flow.ForEach with one body task
  nested foreach - ForEach tasks nested inside each other (arbitrary depth)
  split-switch - io.kestra.plugin.core.flow.Switch routes to the chosen branch;
                 the convergence step uses Pebble null-coalescing (??) to pick
                 whichever branch's output is populated

Generated top-level task order:
  metaflow_init  - computes a stable run_id from Kestra's execution.id and
                   runs `metaflow init` to create the _parameters artifact
  <step>...      - one task per step (foreach body / branch steps are nested)
"""

import json
import os
import re
from datetime import datetime
from typing import List, Optional

from .exception import NotSupportedException

try:
    from metaflow.plugins.timeout_decorator import get_run_time_limit_for_task
except ImportError:
    get_run_time_limit_for_task = None


# ---------------------------------------------------------------------------
# YAML helpers
# ---------------------------------------------------------------------------

def flow_name_to_id(name: str) -> str:
    """Convert a Metaflow flow name to a Kestra flow ID (lowercase, hyphens only)."""
    return name.lower().replace(".", "-").replace("_", "-")


def _iso_duration(seconds: int) -> str:
    """Convert seconds to ISO 8601 duration string (e.g. PT1H30M45S)."""
    minutes, secs = divmod(seconds, 60)
    hours, mins = divmod(minutes, 60)
    parts = "PT"
    if hours:
        parts += "%dH" % hours
    if mins:
        parts += "%dM" % mins
    if secs or not (hours or mins):
        parts += "%dS" % secs
    return parts


# ---------------------------------------------------------------------------
# Compiler
# ---------------------------------------------------------------------------

class KestraCompiler:
    """Compiles a Metaflow flow into a Kestra YAML definition."""

    # Internal task ID — chosen to avoid clashes with typical step names.
    INIT_TASK_ID = "metaflow_init"

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
        self._workflow_timeout = workflow_timeout
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
        # Always emit the inputs section so that {{ inputs.ORIGIN_RUN_ID }}
        # (used in every task script for resume support) resolves in Kestra 1.3+,
        # which throws IllegalVariableEvaluationException for undefined inputs.
        sections.append(self._render_inputs(params))
        sections.append(self._render_plugin_defaults())
        triggers_yaml = self._render_triggers()
        if triggers_yaml:
            sections.append(triggers_yaml)
        sections.append(self._render_tasks(params))
        return "\n\n".join(sections) + "\n"

    # ------------------------------------------------------------------
    # Top-level section renderers
    # ------------------------------------------------------------------

    @property
    def flow_id(self) -> str:
        """Kestra flow ID derived from the flow name (lowercase, hyphens only)."""
        return flow_name_to_id(self._flow_name)

    def _render_header(self) -> str:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        flow_file_base = os.path.basename(self.flow_file)
        lines = [
            "id: %s" % self.flow_id,
            "namespace: %s" % self.kestra_namespace,
            "description: |",
            "  Generated by metaflow-kestra on %s." % now,
            "  Regenerate: python %s kestra compile" % flow_file_base,
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
            lines.append("  metaflow.project: \"%s\"" % self._project_info["name"].replace("\\", "\\\\").replace('"', '\\"'))
            lines.append("  metaflow.branch: \"%s\"" % self._project_info["branch"].replace("\\", "\\\\").replace('"', '\\"'))
        if self._workflow_timeout:
            lines.append("timeout: %s" % _iso_duration(self._workflow_timeout))
        return "\n".join(lines)

    def _render_variables(self) -> str:
        lines = ["variables:"]
        # All values are double-quoted to handle paths with spaces or YAML special chars.
        fixed = [
            ("FLOW_FILE",        self.flow_file),
            ("FLOW_NAME",        self._flow_name),
            ("METADATA_TYPE",    self._metadata_type),
            ("DATASTORE_TYPE",   self._datastore_type),
            ("DATASTORE_ROOT",   self._datastore_root or "~/.metaflow"),
            ("ENVIRONMENT_TYPE", self._environment_type),
            ("EVENT_LOGGER_TYPE",self._event_logger_type),
            ("MONITOR_TYPE",     self._monitor_type),
            ("KESTRA_NAMESPACE", self.kestra_namespace),
        ]
        for key, val in fixed:
            safe = str(val).replace("\\", "\\\\").replace('"', '\\"')
            lines.append('  %s: "%s"' % (key, safe))
        # Forward any METAFLOW_SERVICE_* / METAFLOW_DEFAULT_* env vars so steps
        # inherit the same backend configuration as the compile-time environment.
        for key, val in os.environ.items():
            if key.startswith("METAFLOW_SERVICE") or key.startswith("METAFLOW_DEFAULT"):
                safe = val.replace("\\", "\\\\").replace('"', '\\"')
                lines.append('  %s: "%s"' % (key, safe))
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
        # Always add the hidden resume input so any deployed flow supports
        # `kestra resume --clone-run-id <run_id>` without redeployment.
        lines += [
            "  - id: ORIGIN_RUN_ID",
            "    type: STRING",
            '    defaults: ""',
            "    description: 'Leave empty for a normal run. Set to a previous Metaflow run ID to resume (skip already-completed steps).'",
        ]
        return "\n".join(lines)

    @staticmethod
    def _render_plugin_defaults() -> str:
        # Keep pluginDefaults as a best-effort fallback for Kestra versions that honour it.
        # Each task also sets taskRunner/warningOnStdErr/beforeCommands explicitly via
        # _TASK_RUNNER_YAML so that the behaviour is correct regardless of whether
        # pluginDefaults is applied by the running Kestra version.
        lines = [
            "pluginDefaults:",
            "  - type: io.kestra.plugin.scripts.python.Script",
            "    values:",
            "      warningOnStdErr: false",
            "      taskRunner:",
            "        type: io.kestra.plugin.core.runner.Process",
            "      beforeCommands:",
            "        - python3 -m pip show kestra >/dev/null 2>&1 || python3 -m pip install kestra --quiet 2>&1 | tail -3",
            "        - python3 -m pip show metaflow >/dev/null 2>&1 || python3 -m pip install metaflow --quiet 2>&1 | tail -3",
        ]
        return "\n".join(lines)

    # YAML fragment emitted directly into every Script task so the Process runner
    # and beforeCommands are applied even when pluginDefaults is not honoured.
    _TASK_RUNNER_YAML = (
        "warningOnStdErr: false\n"
        "taskRunner:\n"
        "  type: io.kestra.plugin.core.runner.Process\n"
        "beforeCommands:\n"
        "  - python3 -m pip show kestra >/dev/null 2>&1 || python3 -m pip install kestra --quiet 2>&1 | tail -3\n"
        "  - python3 -m pip show metaflow >/dev/null 2>&1 || python3 -m pip install metaflow --quiet 2>&1 | tail -3"
    )

    def _render_triggers(self) -> str:
        """Render the triggers section combining @schedule, @trigger, and @trigger_on_finish."""
        trigger_blocks = []

        # @schedule decorator → Kestra Schedule trigger
        schedule = self._get_schedule()
        if schedule:
            cron = schedule.get("cron")
            if cron:
                block = ["  - id: schedule", "    type: io.kestra.plugin.core.trigger.Schedule"]
                block.append("    cron: \"%s\"" % cron)
                if schedule.get("timezone"):
                    block.append("    timezone: %s" % schedule["timezone"])
                trigger_blocks.append("\n".join(block))

        # @trigger(event="foo") → Kestra Flow trigger listening for a named event
        for idx, event_name in enumerate(self._get_triggers()):
            tid = "on_event_%d" % idx if idx > 0 else "on_event"
            block = [
                "  - id: %s" % tid,
                "    type: io.kestra.plugin.core.trigger.Flow",
                "    conditions:",
                "      - type: io.kestra.plugin.core.condition.ExecutionLabelsCondition",
                "        labels:",
                "          metaflow.event: \"%s\"" % event_name.replace('"', '\\"'),
            ]
            trigger_blocks.append("\n".join(block))

        # @trigger_on_finish(flow="UpstreamFlow") → trigger when the upstream Kestra flow completes
        for idx, upstream_flow in enumerate(self._get_trigger_on_finishes()):
            upstream_id = flow_name_to_id(upstream_flow)
            tid = "on_finish_%d" % idx if idx > 0 else "on_finish"
            block = [
                "  - id: %s" % tid,
                "    type: io.kestra.plugin.core.trigger.Flow",
                "    conditions:",
                "      - type: io.kestra.plugin.core.condition.ExecutionFlowCondition",
                "        namespace: %s" % self.kestra_namespace,
                "        flowId: %s" % upstream_id,
                "      - type: io.kestra.plugin.core.condition.ExecutionStatusCondition",
                "        in:",
                "          - SUCCESS",
            ]
            trigger_blocks.append("\n".join(block))

        if not trigger_blocks:
            return ""
        return "triggers:\n" + "\n".join(trigger_blocks)

    def _render_tasks(self, params: dict) -> str:
        lines = ["tasks:"]
        # Init task — always first; runs metaflow init to create _parameters artifact
        lines.append(self._render_init_task(indent=2, params=params))
        # Walk the graph
        tasks_yaml = []
        visited = set()
        self._visit_node("start", tasks_yaml, visited, indent=2)
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
    ):
        """Recursively visit graph nodes and emit YAML task definitions."""
        if step_name in visited:
            return
        visited.add(step_name)

        node = self.graph[step_name]
        ntype = node.type

        if ntype == "end":
            out.append(self._render_step_task(node, indent=indent))
            return

        if ntype in ("start", "linear"):
            out.append(self._render_step_task(node, indent=indent))
            for next_step in node.out_funcs:
                self._visit_node(next_step, out, visited, indent=indent)

        elif ntype == "foreach":
            # Emit the foreach parent step itself
            out.append(self._render_step_task(node, indent=indent))
            # Build the full nested foreach chain (may be depth > 1)
            chain = self._build_foreach_chain(node.name)
            # Mark all body/join steps inside the chain as visited so they
            # are not emitted again at the top level.
            for _foreach_name, body_name, join_name in chain:
                visited.add(body_name)
                visited.add(join_name)
            # Emit the outermost ForEach wrapper (which nests inner ones recursively)
            out.append(self._render_foreach_chain(chain, indent=indent))
            # Continue with the step after the outermost join
            outermost_join = chain[0][2]
            self._visit_node(outermost_join, out, visited, indent=indent)

        elif ntype == "split":
            # First run the split step itself (it may compute data before branching)
            out.append(self._render_step_task(node, indent=indent))
            # Then emit the Parallel wrapper for the branches
            out.append(
                self._render_parallel_wrapper(node, visited=visited, indent=indent)
            )
            # Find the join node for this split and continue from there
            join_step = self._find_join_step(step_name)
            if join_step:
                self._visit_node(join_step, out, visited, indent=indent)

        elif ntype == "split-switch":
            # Run the switch step (which writes branch_taken to the output JSON)
            out.append(self._render_step_task(node, indent=indent))
            # Emit a Kestra Switch task that routes to the correct branch
            out.append(
                self._render_switch_wrapper(node, visited=visited, indent=indent)
            )
            # Find the join step for this split-switch and continue from there
            join_step = self._find_switch_join_step(step_name)
            if join_step:
                self._visit_node(join_step, out, visited, indent=indent)

        elif ntype == "join":
            out.append(self._render_step_task(node, indent=indent))
            for next_step in node.out_funcs:
                self._visit_node(next_step, out, visited, indent=indent)

        else:
            raise NotSupportedException(
                "Graph node *%s* has unsupported type %r. "
                "Please report this as a bug." % (step_name, ntype)
            )

    # ------------------------------------------------------------------
    # Task renderers
    # ------------------------------------------------------------------

    def _render_init_task(self, indent: int, params: dict) -> str:
        script = self._init_script(params)
        return self._task_block(
            task_id=self.INIT_TASK_ID,
            task_type="io.kestra.plugin.scripts.python.Script",
            script=script,
            indent=indent,
        )

    def _render_step_task(self, node, indent: int) -> str:
        script = self._step_script(node)
        return self._task_block(
            task_id=node.name,
            task_type="io.kestra.plugin.scripts.python.Script",
            script=script,
            indent=indent,
            extras=self._build_task_extras(node),
        )

    def _build_task_extras(self, node) -> dict:
        """Return the extras dict (timeout, retry) for a task block."""
        extras = {}
        timeout = self._get_timeout(node)
        if timeout:
            extras["timeout"] = _iso_duration(timeout)
        retries, retry_delay = self._get_retry_config(node)
        if retries > 0:
            extras["retry"] = {
                "type": "constant",
                "interval": _iso_duration(retry_delay),
                "maxAttempt": retries,
            }
        return extras

    def _build_foreach_chain(self, foreach_step_name: str) -> list:
        """Return a list of (foreach_name, body_name, join_name) tuples for nested foreach.

        The list is ordered outermost-first.  For a simple (non-nested) foreach
        the list has exactly one entry.  For two levels of nesting it has two
        entries, and so on.
        """
        chain = []
        current = foreach_step_name
        while True:
            node = self.graph[current]
            body_name = node.out_funcs[0]
            body_node = self.graph[body_name]
            # Find the join step for this foreach level
            join_name = self._find_foreach_join(current)
            chain.append((current, body_name, join_name))
            # If the body step is itself a foreach, recurse into the next level
            if body_node.type == "foreach":
                current = body_name
            else:
                break
        return chain

    def _find_foreach_join(self, foreach_step_name: str) -> str:
        """Return the join step name that corresponds to the given foreach step."""
        for node in self.graph:
            if node.type == "join":
                parents = list(getattr(node, "split_parents", []))
                if parents and parents[-1] == foreach_step_name:
                    return node.name
        # Fallback: use the body's out_func (simple case)
        body_name = self.graph[foreach_step_name].out_funcs[0]
        return self.graph[body_name].out_funcs[0]

    def _render_foreach_chain(self, chain: list, indent: int) -> str:
        """Recursively render nested ForEach wrappers for the given chain.

        chain: list of (foreach_name, body_name, join_name) tuples, outermost first.
        Returns the YAML string for the outermost ForEach task (inner ones are nested).
        """
        foreach_name, body_name, _join_name = chain[0]
        parent_node = self.graph[foreach_name]
        body_node = self.graph[body_name]
        pad = " " * indent

        lines = [
            "%s- id: foreach_%s" % (pad, foreach_name),
            "%s  type: io.kestra.plugin.core.flow.ForEach" % pad,
            "%s  values: \"{{ outputs.%s.vars.foreach_values }}\"" % (pad, foreach_name),
            "%s  concurrencyLimit: %d" % (pad, self.max_workers),
            "%s  tasks:" % pad,
        ]

        if len(chain) > 1:
            # The body step is itself a foreach: emit it as a step task first, then
            # nest its own ForEach wrapper inside this one.
            inner_body_script = self._step_script(
                body_node, is_foreach_body=True, foreach_parent=foreach_name
            )
            lines.append(self._task_block(
                task_id=body_node.name,
                task_type="io.kestra.plugin.scripts.python.Script",
                script=inner_body_script,
                indent=indent + 4,
                extras=self._build_task_extras(body_node),
            ))
            # Recursively render the inner foreach chain
            lines.append(self._render_foreach_chain(chain[1:], indent=indent + 4))
        else:
            # Leaf level: render the actual body step
            body_script = self._step_script(
                body_node, is_foreach_body=True, foreach_parent=foreach_name
            )
            lines.append(self._task_block(
                task_id=body_node.name,
                task_type="io.kestra.plugin.scripts.python.Script",
                script=body_script,
                indent=indent + 4,
                extras=self._build_task_extras(body_node),
            ))

        return "\n".join(lines)

    def _render_parallel_wrapper(self, split_node, visited: set, indent: int) -> str:
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

    def _render_switch_wrapper(self, switch_node, visited: set, indent: int) -> str:
        """Emit a Kestra Switch task routing conditional branches for a split-switch node."""
        pad = " " * indent
        wrapper_id = "switch_%s" % switch_node.name
        # Find the convergence step so branch visitors can stop before it
        convergence_step = self._find_switch_join_step(switch_node.name)

        lines = [
            "%s- id: %s" % (pad, wrapper_id),
            "%s  type: io.kestra.plugin.core.flow.Switch" % pad,
            "%s  value: \"{{ outputs.%s.vars.branch_taken }}\"" % (pad, switch_node.name),
            "%s  cases:" % pad,
        ]

        for branch_step in switch_node.out_funcs:
            branch_tasks = []
            self._visit_branch(
                branch_step, branch_tasks, visited, indent + 6,
                stop_at=convergence_step,
            )
            if branch_tasks:
                lines.append("%s    %s:" % (pad, branch_step))
                lines.extend(branch_tasks)

        return "\n".join(lines)

    def _find_switch_join_step(self, switch_step_name: str) -> Optional[str]:
        """Find the convergence step that follows a split-switch step.

        Metaflow guarantees all conditional branches converge at a single
        downstream step. We find it by following the first branch one step.
        """
        switch_node = self.graph[switch_step_name]
        if switch_node.out_funcs:
            first_branch = self.graph[switch_node.out_funcs[0]]
            if first_branch.out_funcs:
                return first_branch.out_funcs[0]
        return None

    def _visit_branch(
        self,
        step_name: str,
        out: list,
        visited: set,
        indent: int,
        stop_at: Optional[str] = None,
    ):
        """Visit steps in a split branch until reaching a join or convergence node."""
        if step_name in visited:
            return
        node = self.graph[step_name]
        # Stop at join nodes (parallel split) or the designated convergence step
        if node.type == "join":
            return
        if stop_at is not None and step_name == stop_at:
            return
        visited.add(step_name)
        out.append(self._render_step_task(node, indent=indent))
        for next_step in node.out_funcs:
            next_node = self.graph[next_step]
            if next_node.type != "join" and next_step != stop_at:
                self._visit_branch(next_step, out, visited, indent, stop_at=stop_at)

    # ------------------------------------------------------------------
    # Script generators
    # ------------------------------------------------------------------

    def _init_script(self, params: dict) -> str:
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
origin_run_id = "{{ inputs.ORIGIN_RUN_ID }}"
if origin_run_id:
    cmd += ["--clone-run-id", origin_run_id]
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
        is_foreach_body: bool = False,
        foreach_parent: Optional[str] = None,
    ) -> str:
        """Generate the Python script for a single Metaflow step task."""
        max_retries, _ = self._get_retry_config(node)
        script = """\
import json, os, subprocess, sys, tempfile
from kestra import Kestra

FLOW_FILE = "{{ vars.FLOW_FILE }}"
DATASTORE_TYPE = "{{ vars.DATASTORE_TYPE }}"
DATASTORE_ROOT = "{{ vars.DATASTORE_ROOT }}"
if DATASTORE_TYPE == "local":
    _sysroot = os.environ.get("METAFLOW_DATASTORE_SYSROOT_LOCAL")
    if _sysroot:
        DATASTORE_ROOT = os.path.join(_sysroot, ".metaflow")

run_id = "{{ outputs.metaflow_init.vars.run_id }}"
task_id = %(task_id_expr)s
%(split_index_block)s
%(input_paths_code)s
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
_origin_run_id = "{{ inputs.ORIGIN_RUN_ID }}"
if _origin_run_id:
    cmd += ["--clone-run-id", _origin_run_id]
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
%(kestra_outputs_code)s
%(artifact_hint)s
Kestra.outputs(kestra_out)
""" % {
            "task_id_expr": self._build_task_id_expr(node.name, is_foreach_body),
            "split_index_block": 'split_index = int("{{ taskrun.value }}")' if is_foreach_body else "",
            "input_paths_code": self._build_input_paths_code(node, is_foreach_body, foreach_parent),
            "env_overrides": self._build_env_overrides_str(node),
            "top_args": self._build_top_args_str(node),
            "step_args": self._build_step_args_str(node.name, max_retries, is_foreach_body),
            "step_name": node.name,
            "kestra_outputs_code": self._build_kestra_outputs_code(node.type),
            "artifact_hint": self._build_artifact_hint_code(node.name),
        }
        script = re.sub(r"\n{3,}", "\n\n", script)
        return script.strip()

    def _build_task_id_expr(self, step_name: str, is_foreach_body: bool) -> str:
        """Python expression string that evaluates to the Metaflow task ID at runtime."""
        if is_foreach_body:
            return (
                '"{{ outputs.metaflow_init.vars.run_id }}" + "-%(step_name)s-" + str(int("{{ taskrun.value }}"))'
                % {"step_name": step_name}
            )
        return '"{{ outputs.metaflow_init.vars.run_id }}" + "-%s"' % step_name

    def _build_input_paths_code(
        self,
        node,
        is_foreach_body: bool,
        foreach_parent: Optional[str],
    ) -> str:
        """Return Python statement(s) that assign ``input_paths`` for this step.

        Handles all cases: foreach body, start step, split-switch convergence,
        foreach join, split join, and the common single-parent case.
        """
        ntype = node.type

        if is_foreach_body:
            return 'input_paths = "{{ outputs.%s.vars.input_path }}"' % foreach_parent

        if node.name == "start":
            return (
                'input_paths = ('
                '"{{ outputs.metaflow_init.vars.run_id }}"'
                ' + "/_parameters/" + '
                '"{{ outputs.metaflow_init.vars.params_task_id }}"'
                ')'
            )

        switch_parent = self._find_switch_parent_for_join(node)
        if switch_parent is not None:
            # split-switch convergence: only one branch ran; null-coalesce to find it
            branch_steps = list(self.graph[switch_parent].out_funcs)
            coalesce = " ?? ".join("outputs.%s.vars.input_path" % b for b in branch_steps)
            return 'input_paths = "{{ %s }}"' % coalesce

        if ntype == "join":
            split_parents = list(getattr(node, "split_parents", []))
            if split_parents:
                innermost = split_parents[-1]
                if self.graph[innermost].type == "foreach":
                    # foreach join: reconstruct paths from deterministic body task IDs
                    body_step = node.in_funcs[0]
                    return """\
# Foreach join: reconstruct input paths from body task IDs
foreach_count = int("{{ outputs.%(parent)s.vars.foreach_count }}")
body_task_ids = [run_id + "-%(body_step)s-" + str(i) for i in range(foreach_count)]
input_paths = ",".join(
    run_id + "/%(body_step)s/" + tid for tid in body_task_ids
)""" % {"parent": innermost, "body_step": body_step}
                else:
                    # split join: collect input_paths from all branches
                    parts = ['"{{ outputs.%s.vars.input_path }}"' % b for b in node.in_funcs]
                    return 'input_paths = ",".join([%s])' % ", ".join(parts)

        # Common case: single parent (or multiple, handled gracefully)
        in_funcs = list(node.in_funcs)
        if len(in_funcs) == 1:
            return 'input_paths = "{{ outputs.%s.vars.input_path }}"' % in_funcs[0]
        parts = ['"{{ outputs.%s.vars.input_path }}"' % p for p in in_funcs]
        return 'input_paths = ",".join([%s])' % ", ".join(parts)

    def _build_top_args_str(self, node) -> str:
        """Comma-separated Python string literals for the top-level CLI args."""
        args = [
            '"--quiet"',
            '"--no-pylint"',
            '"--metadata={{ vars.METADATA_TYPE }}"',
            '"--datastore={{ vars.DATASTORE_TYPE }}"',
            '"--datastore-root=" + DATASTORE_ROOT',
            '"--environment={{ vars.ENVIRONMENT_TYPE }}"',
            '"--with=kestra_internal"',
        ]
        for deco in self.with_decorators:
            args.append('"--with=%s"' % deco.replace("\\", "\\\\").replace('"', '\\"'))
        if self.namespace:
            args.append('"--namespace=%s"' % self.namespace.replace("\\", "\\\\").replace('"', '\\"'))
        if self.branch:
            args.append('"--branch=%s"' % self.branch.replace("\\", "\\\\").replace('"', '\\"'))
        # Note: --tag is a step-level option, not top-level; tags go in step_args.
        for spec in self._get_decorator_specs(node):
            args.append('"--with=%s"' % spec.replace("\\", "\\\\").replace('"', '\\"'))
        # Forward @resources as a --with flag so compute backends (kubernetes, batch, etc.)
        # receive CPU/memory/GPU hints at runtime.
        resources_spec = self._get_resources_spec(node)
        if resources_spec:
            args.append('"--with=%s"' % resources_spec.replace("\\", "\\\\").replace('"', '\\"'))
        return ", ".join(args)

    def _build_step_args_str(self, step_name: str, max_retries: int, is_foreach_body: bool) -> str:
        """Comma-separated Python string literals for the step sub-command CLI args."""
        args = [
            '"step"',
            '"%s"' % step_name,
            '"--run-id"', 'run_id',
            '"--task-id"', 'task_id',
            '"--retry-count"', '"0"',
            '"--max-user-code-retries"', '"%d"' % max_retries,
        ]
        for tag in self._tags:
            safe_tag = tag.replace("\\", "\\\\").replace('"', '\\"')
            args += ['"--tag"', '"%s"' % safe_tag]
        if is_foreach_body:
            args += ['"--split-index"', 'str(split_index)']
        return ", ".join(args)

    def _build_env_overrides_str(self, node) -> str:
        """Indented ``"key": "val",`` lines for @environment vars to inject at runtime."""
        env_deco = [d for d in node.decorators if d.name == "environment"]
        if not env_deco:
            return ""
        env_vars = env_deco[0].attributes.get("vars", {})
        lines = [
            '    "%s": "%s",' % (k, str(v).replace("\\", "\\\\").replace('"', '\\"'))
            for k, v in env_vars.items()
        ]
        return "\n".join(lines)

    def _build_kestra_outputs_code(self, ntype: str) -> str:
        """Python code to populate extra Kestra outputs based on step type."""
        if ntype == "foreach":
            return """\
if out.get("foreach_cardinality", 0) > 0:
    fc = out["foreach_cardinality"]
    kestra_out["foreach_count"] = fc
    kestra_out["foreach_values"] = list(range(fc))"""
        if ntype == "split-switch":
            return """\
if out.get("branch_taken"):
    kestra_out["branch_taken"] = out["branch_taken"]"""
        return ""

    def _build_artifact_hint_code(self, step_name: str) -> str:
        """Python code that reads Metaflow artifacts and posts them as Kestra outputs."""
        return """\
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
    pass""" % {"flow_name": self._flow_name, "step_name": step_name}

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

        # Emit taskRunner/warningOnStdErr/beforeCommands directly on every Script task.
        # This ensures the Process runner is used even when pluginDefaults is not honoured
        # by the running Kestra version.
        for yaml_line in self._TASK_RUNNER_YAML.splitlines():
            lines.append("%s  %s" % (pad, yaml_line))

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
    # Graph utilities
    # ------------------------------------------------------------------

    def _find_switch_parent_for_join(self, node) -> Optional[str]:
        """Return the split-switch step name that converges at this node, or None.

        This handles both traditional join steps (type='join') and linear steps
        that have multiple in_funcs due to conditional branching (type='linear').
        """
        for in_step in node.in_funcs:
            in_node = self.graph[in_step]
            for parent in in_node.in_funcs:
                parent_node = self.graph[parent]
                if parent_node.type == "split-switch":
                    return parent
        return None

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
            params[var] = {"default": default}
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
        # Only emit a Kestra task timeout when the step has an explicit @timeout
        # decorator. The Metaflow default runtime limit (120h) should not be
        # forwarded as a Kestra timeout because it would appear on every task.
        if get_run_time_limit_for_task is None:
            return None
        if not any(d.name == "timeout" for d in node.decorators):
            return None
        try:
            return get_run_time_limit_for_task(node.decorators) or None
        except Exception:
            return None

    def _get_retry_config(self, node) -> tuple:
        """Return (max_retries, delay_seconds) from @retry, or (0, 120) if absent."""
        for deco in node.decorators:
            if deco.name == "retry":
                times = int(deco.attributes.get("times", 0))
                minutes = float(deco.attributes.get("minutes_between_retries", 2))
                return times, int(minutes * 60)
        return 0, 120

    def _get_decorator_specs(self, node) -> list:
        """Return --with-compatible spec strings for user-defined step decorators
        that should be forwarded as subcommand flags.

        Note: @resources is excluded here because it is handled separately in
        _get_resources_spec to build an explicit resources: spec string.
        """
        _skip = {
            "kestra_internal", "retry", "timeout", "environment", "resources",
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

    def _get_resources_spec(self, node) -> Optional[str]:
        """Return a 'resources:cpu=N,memory=M,gpu=G' spec string for @resources, or None.

        The spec is passed as a --with flag at runtime so that Metaflow compute
        backends (e.g. @kubernetes, @sandbox) receive the resource hints.
        """
        for deco in node.decorators:
            if deco.name == "resources":
                parts = []
                for attr in ("cpu", "memory", "gpu"):
                    val = deco.attributes.get(attr)
                    if val is not None:
                        parts.append("%s=%s" % (attr, val))
                if parts:
                    return "resources:%s" % ",".join(parts)
        return None

    def _get_triggers(self) -> list:
        """Return a list of event names from @trigger(event=...) decorators."""
        import warnings
        try:
            flow_decos = getattr(self.flow, "_flow_decorators", {})
            trigger_list = flow_decos.get("trigger", [])
            if not trigger_list:
                return []
            raw_triggers = getattr(trigger_list[0], "triggers", None) or []
            result = []
            for t in raw_triggers:
                if not isinstance(t, dict):
                    continue
                name = t.get("name")
                if not name or not isinstance(name, str):
                    warnings.warn(
                        "@trigger entry has a non-string or deploy-time event name %r — "
                        "skipping this trigger." % (name,),
                        UserWarning,
                        stacklevel=2,
                    )
                    continue
                result.append(name)
            return result
        except Exception:
            return []

    def _get_trigger_on_finishes(self) -> list:
        """Return a list of upstream flow names from @trigger_on_finish(flow=...) decorators."""
        import warnings
        try:
            flow_decos = getattr(self.flow, "_flow_decorators", {})
            tof_list = flow_decos.get("trigger_on_finish", [])
            if not tof_list:
                return []
            raw_triggers = getattr(tof_list[0], "triggers", None) or []
            result = []
            for t in raw_triggers:
                if not isinstance(t, dict):
                    continue
                flow_name = t.get("flow") or t.get("fq_name")
                if not flow_name or not isinstance(flow_name, str):
                    warnings.warn(
                        "@trigger_on_finish entry has a non-string or missing flow name %r — "
                        "skipping this trigger." % (flow_name,),
                        UserWarning,
                        stacklevel=2,
                    )
                    continue
                result.append(flow_name)
            return result
        except Exception:
            return []

    @staticmethod
    def _infer_kestra_type(default) -> str:
        if isinstance(default, bool):
            return "BOOLEAN"
        if isinstance(default, int):
            return "INT"
        if isinstance(default, float):
            return "FLOAT"
        return "STRING"
