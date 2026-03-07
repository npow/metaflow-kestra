import json
import os

from metaflow.decorators import StepDecorator
from metaflow.metadata_provider import MetaDatum


class KestraInternalDecorator(StepDecorator):
    """Internal step decorator injected by the Kestra executor.

    Records Kestra execution metadata in Metaflow's metadata store and
    writes the foreach cardinality (when applicable) to a sidecar JSON file
    that the Kestra task reads to fan-out ForEach iterations.
    """

    name = "kestra_internal"

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_user_code_retries,
        ubf_context,
        inputs,
    ):
        entries = []
        execution_id = os.environ.get("METAFLOW_KESTRA_EXECUTION_ID")
        if execution_id:
            entries.append(
                MetaDatum(
                    field="kestra-execution-id",
                    value=execution_id,
                    type="kestra-execution-id",
                    tags=["attempt_id:{0}".format(retry_count)],
                )
            )
        namespace = os.environ.get("METAFLOW_KESTRA_NAMESPACE")
        if namespace:
            entries.append(
                MetaDatum(
                    field="kestra-namespace",
                    value=namespace,
                    type="kestra-namespace",
                    tags=["attempt_id:{0}".format(retry_count)],
                )
            )
        flow_id = os.environ.get("METAFLOW_KESTRA_FLOW_ID")
        if flow_id:
            entries.append(
                MetaDatum(
                    field="kestra-flow-id",
                    value=flow_id,
                    type="kestra-flow-id",
                    tags=["attempt_id:{0}".format(retry_count)],
                )
            )
        if entries:
            metadata.register_metadata(run_id, step_name, task_id, entries)

    def task_finished(
        self, step_name, flow, graph, is_task_ok, retry_count, max_user_code_retries
    ):
        output = {"task_ok": is_task_ok}
        node = graph[step_name]
        if node.type == "foreach":
            output["foreach_cardinality"] = getattr(flow, "_foreach_num_splits", 0)
        if node.type == "split-switch":
            # Record which branch was chosen so the Kestra Switch task can route.
            transition = getattr(flow, "_transition", None)
            if transition and transition[0]:
                output["branch_taken"] = transition[0][0]
        output_file = os.environ.get("METAFLOW_KESTRA_OUTPUT_FILE")
        if output_file:
            try:
                with open(output_file, "w") as f:
                    json.dump(output, f)
            except (OSError, TypeError):
                pass
