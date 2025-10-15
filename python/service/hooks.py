# service/hooks.py
from worker_sdk.contracts import WorkerHooks

class ServiceHooks(WorkerHooks):
    def load_pipeline(self, ctx):
        ctx.logger.info("Loaded dummy pipeline")
        return {"dummy": True}

    def process(self, ctx, msg):
        ctx.logger.info("Processing message", {"task_id": msg["task_id"]})
        return {
            "result": {"status": "success", "task": msg["task_id"]},
            "metrics": {"duration": 0.1}
        }
