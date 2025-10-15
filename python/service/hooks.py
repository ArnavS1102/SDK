# service/hooks.py
import os
from worker_sdk.contracts import WorkerHooks
from worker_sdk.io_sqs import build_output_prefix  # you already have this

class ServiceHooks(WorkerHooks):
    def load_pipeline(self, ctx):
        ctx.logger.info("Loaded dummy pipeline")
        return {"dummy": True}

    def process(self, ctx, msg):
        ctx.logger.info("Processing message", {"task_id": msg["task_id"]})

        # (1) Do your work here; runner will write result/metrics for you.
        result = {"status": "success", "task": msg["task_id"]}

        # (2) Describe exactly ONE child task for the next step (PUBLISH).
        #     Make task_id deterministic:
        child_task_id = f"{msg['task_id']}-publish-000"
        child_step = "COMPLETION"

        # Build the child's output_prefix in the same WORK_BUCKET/job
        child_prefix = build_output_prefix(
            bucket=ctx.config["work_bucket"],
            job_id=msg["job_id"],
            step=child_step,
            task_id=child_task_id,
        )

        # Child will read the output we just produced (runner saves result.json)
        child_input_uri = f"{msg['output_prefix']}result.json"

        fanout = [{
            "task_id": child_task_id,
            "step": child_step,
            "input_uri": child_input_uri,
            "output_prefix": child_prefix,
            "params": {"source": "image-stage"}  # keep small + JSON-safe
        }]

        return {
            "result": result,               # runner -> {output_prefix}/result.json
            "metrics": {"duration": 0.1},   # runner -> {output_prefix}/metrics.json
            "fanout": fanout                # runner publishes to NEXT_QUEUE_URL
        }
