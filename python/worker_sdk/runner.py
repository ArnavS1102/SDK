import os
import sys
import time
import json
from typing import Any, Dict, List

from .constants import (
    CONFIG,
    WORK_BUCKET,
    get_queue_url,
    get_next_steps,
    get_primary_filetype,
    make_output_prefix,
)
from .io_s3 import S3Storage, ensure_within_prefix, guess_content_type
from .io_sqs import SQSClient
from .logging import get_logger
from .message import validate_message
from .idempotency import result_exists


# ==========================================================
# Helpers
# ==========================================================

PRIMARY_BY_FILETYPE = {
    "json": "result.json",
    "png": "result.png",
    "mp4": "result.mp4",
    "mp3": "result.mp3",
    "wav": "result.wav",
}


def _get_primary_name(step: str) -> str:
    """Return main output filename for a step."""
    ftype = get_primary_filetype(step)
    return PRIMARY_BY_FILETYPE.get(ftype, "result.json")


def _det_task_id(step: str, index: int) -> str:
    """Generate simple readable task IDs like S1, I2, V3."""
    return f"{step[0].upper()}{index + 1}"


# ==========================================================
# Core Runner Logic
# ==========================================================

def main():
    """Main entry point for the worker."""
    step = os.environ.get("STEP")
    if not step:
        raise RuntimeError("Missing STEP environment variable")

    hooks_path = os.environ.get("HOOKS_PATH", "service.hooks.ServiceHooks")
    log_level = os.environ.get("LOG_LEVEL", "INFO")

    queue_url = get_queue_url(step)
    if not queue_url:
        raise RuntimeError(f"No queue URL found for step={step}")

    logger = get_logger("runner", level=log_level)
    logger.info("Starting worker", {"step": step, "queue_url": queue_url})

    # Initialize I/O adapters
    storage = S3Storage()
    sqs_client = SQSClient(region=CONFIG["aws"]["region"])

    # Load hooks
    import importlib
    mod, cls = hooks_path.rsplit(".", 1)
    hooks = getattr(importlib.import_module(mod), cls)()

    # Build context for hooks
    init_ctx = {"config": {"work_bucket": WORK_BUCKET, "step": step}, "logger": logger, "storage": storage}
    
    logger.info("Loading model/pipeline...")
    hooks.model = hooks.load_pipeline(init_ctx)
    logger.info("Model ready ✓")

    while True:
        try:
            msgs = sqs_client.receive_messages(queue_url, max_messages=1, wait_seconds=20)
            if not msgs:
                continue

            for raw in msgs:
                _process(storage, sqs_client, raw, step, hooks, logger, queue_url)

        except KeyboardInterrupt:
            logger.info("Graceful shutdown (Ctrl+C)")
            sys.exit(0)
        except Exception as e:
            logger.error(e, {"context": "main_loop"})
            time.sleep(5)

# ==========================================================
# Task Processing
# ==========================================================

def _process(storage: S3Storage, sqs_client: SQSClient, raw_msg: Dict[str, Any],
             step: str, hooks, logger, queue_url: str):
    """Process one message through the full pipeline."""
    receipt = None
    task_id = "unknown"

    try:
        task_dict, receipt, receive_count = sqs_client.parse_message(raw_msg)
        
        # Validate message schema
        try:
            validated = validate_message(task_dict, bucket_allowlist=[WORK_BUCKET])
        except (ValueError, TypeError) as e:
            # Invalid message - delete it to stop retries
            logger.error(f"Invalid message (deleting): {e}", {
                "task_id": task_dict.get("task_id", "unknown"),
                "error": str(e)
            })
            sqs_client.delete_message(queue_url, receipt)
            return

        job_id = validated.job_id
        task_id = validated.task_id
        output_prefix = validated.output_prefix
        log = logger.bind(job_id=job_id, task_id=task_id, step=step)

        # Skip poison pill logic for simplicity
        if result_exists(storage, output_prefix):
            log.info("Skipped (already done - idempotent)")
            sqs_client.delete_message(queue_url, receipt)
            return

        log.info("Processing task...")
        # Build task context
        task_ctx = {"config": {"work_bucket": WORK_BUCKET, "step": step}, "logger": log, "storage": storage}
        outputs = hooks.process(task_ctx, validated)

        _write_outputs(storage, output_prefix, step, outputs)
        _emit_children(storage, sqs_client, validated, outputs, step)

        sqs_client.delete_message(queue_url, receipt)
        log.info("Task completed ✓")

    except Exception as e:
        logger.error(e, {"context": "process", "task_id": task_id})


# ==========================================================
# Output Writing
# ==========================================================

def _write_outputs(storage: S3Storage, prefix: str, step: str, outputs: Dict[str, Any]):
    """Write model outputs (metrics, result, aux)."""
    if not prefix.startswith("s3://"):
        raise ValueError(f"Invalid S3 prefix: {prefix}")

    primary_name = _get_primary_name(step)

    # Write metrics
    if "metrics" in outputs and isinstance(outputs["metrics"], dict):
        storage.put_json(prefix, "metrics.json", outputs["metrics"])

    # Write result
    result = outputs.get("result")
    if result is not None:
        if isinstance(result, dict):
            storage.put_json(prefix, primary_name, result)
        elif isinstance(result, bytes):
            storage.put_bytes(prefix, primary_name, result, content_type=guess_content_type(primary_name))
        else:
            raise TypeError(f"Unsupported result type: {type(result)}")

    # Write auxiliary data
    aux = outputs.get("aux", {})
    for name, data in aux.items():
        ensure_within_prefix(prefix, name)
        if isinstance(data, bytes):
            storage.put_bytes(prefix, name, data, content_type=guess_content_type(name))
        elif isinstance(data, dict):
            storage.put_json(prefix, name, data)


# ==========================================================
# Fan-out to Next Step(s)
# ==========================================================

def _emit_children(storage: S3Storage, sqs_client: SQSClient,
                   parent_task, outputs: Dict[str, Any], step: str):
    """Emit fan-out messages for next steps (from YAML config)."""
    next_steps = get_next_steps(step)
    if not next_steps:
        return

    for next_step in next_steps:
        items = outputs.get("result", {}).get("scenes", [])
        messages: List[Dict[str, Any]] = []

        # Sequential IDs (I1, I2, etc.)
        for idx, item in enumerate(items):
            child_task_id = _det_task_id(next_step, idx)
            child_prefix = make_output_prefix(parent_task.job_id, next_step, child_task_id)

            msg = {
                "job_id": parent_task.job_id,
                "task_id": child_task_id,
                "user_id": parent_task.user_id,
                "schema": parent_task.schema,
                "step": next_step,
                "input_uri": parent_task.output_prefix + _get_primary_name(step),
                "output_prefix": child_prefix,
                "params": {"index": idx, "scene": item},
                "parent_task_id": parent_task.task_id,
                "trace_id": parent_task.trace_id,
                "retry_count": 0,
            }
            messages.append(msg)

        next_q = get_queue_url(next_step)
        if next_q:
            for i in range(0, len(messages), 10):
                sqs_client.send_messages_batch(next_q, messages[i:i + 10])


# ==========================================================
# Entrypoint
# ==========================================================

if __name__ == "__main__":
    main()
