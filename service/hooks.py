# service/hooks.py
from worker_sdk.hooks import WorkerHooks

class ServiceHooks(WorkerHooks):
    def load_pipeline(self, ctx):
        ctx["logger"].info("Loaded dummy pipeline")
        return {"dummy": True}

    def process(self, ctx, msg):
        # msg is a TaskMessage dataclass from validation
        task_id = msg.task_id if hasattr(msg, "task_id") else msg.get("task_id")
        ctx["logger"].info("Processing message", {"task_id": task_id})

        # Do your work here - runner handles everything else
        result = {
            "status": "success",
            "task": task_id,
            "message": "Dummy image generation complete"
        }

        return {
            "result": result,               # runner -> {output_prefix}/result.json
            "metrics": {"duration": 0.1},   # runner -> {output_prefix}/metrics.json
            # No fanout - runner will handle this based on config.yaml
        }
