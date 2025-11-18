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
    get_hooks_path,
    make_output_prefix,
)
from .io_s3 import S3Storage, ensure_within_prefix, guess_content_type
from .io_sqs import SQSClient
from .logging import get_logger
from .message import validate_message
from .idempotency import result_exists
from .io_db import PostgresDB, TaskRecord


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


def _det_task_id(step: str, index: int, explicit_idx: int = None) -> str:
    """Generate simple readable task IDs like S1, I2, V3."""
    if explicit_idx is not None:
        return f"{step[0].upper()}{explicit_idx + 1}"
    return f"{step[0].upper()}{index + 1}"


# ==========================================================
# Core Runner Logic
# ==========================================================

def main(step: str, log_level: str = "INFO", hooks_path: str = None):
    """
    Main entry point for the worker.
    
    Args:
        step: Processing step name (e.g., "IMAGE_GENERATION")
        log_level: Logging level (default: "INFO")
        hooks_path: Optional override for hooks class path (default: from config)
    """
    if not step:
        raise RuntimeError("step parameter is required")

    # Get hooks path from config if not provided
    if not hooks_path:
        hooks_path = get_hooks_path(step)
    if not hooks_path:
        raise RuntimeError(f"No hooks_path defined for step={step} in config")
    
    queue_url = get_queue_url(step)
    if not queue_url:
        raise RuntimeError(f"No queue URL found for step={step}")

    logger = get_logger("runner", level=log_level)
    logger.info("Starting worker", {"step": step, "queue_url": queue_url, "hooks_path": hooks_path})

    # Initialize I/O adapters
    storage = S3Storage()
    sqs_client = SQSClient(region=CONFIG["aws"]["region"])
    # Initialize DB (best-effort)
    db = None
    try:
        db = PostgresDB()
        db.migrate()
        logger.info("DB ready ✓")
    except Exception as e:
        logger.warning("DB unavailable - proceeding without persistence", {"error": str(e)})

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
                _process(storage, sqs_client, raw, step, hooks, logger, queue_url, db)

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
             step: str, hooks, logger, queue_url: str, db: PostgresDB = None):
    """Process one message through the full pipeline."""
    receipt = None
    task_id = "unknown"

    try:
        task_dict, receipt, receive_count = sqs_client.parse_message(raw_msg)
        
        # Validate message schema
        try:
            validated = validate_message(task_dict, bucket_allowlist=[WORK_BUCKET, "my-s3io-tests"])
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

        # Persist job/task (best-effort)
        if db:
            try:
                db.create_or_get_job(
                    job_id=job_id,
                    user_id=validated.user_id,
                    schema=validated.schema,
                    status="QUEUED",
                    trace_id=validated.trace_id,
                    attrs={},
                )
                # TODO: Update job status based on task lifecycle:
                # - Set job to RUNNING when first task starts
                # - Set job to SUCCEEDED when all tasks complete
                # - Set job to FAILED if any task fails
                # Need to query all tasks for a job to determine completion status
                db.upsert_task(task=TaskRecord(
                    task_id=validated.task_id,
                    job_id=validated.job_id,
                    user_id=validated.user_id,
                    step=validated.step,
                    status="QUEUED",
                    retry_count=int(validated.retry_count or 0),
                    parent_task_id=validated.parent_task_id,
                    input_uri=validated.input_uri,
                    output_prefix=validated.output_prefix,
                    params=validated.params,
                ))
            except Exception as e:
                log.warning("DB upsert failed (continuing)", {"error": str(e)})

        # Skip poison pill logic for simplicity
        if result_exists(storage, output_prefix):
            log.info("Skipped (already done - idempotent)")
            sqs_client.delete_message(queue_url, receipt)
            return

        log.info("Processing task...")
        if db:
            try:
                db.set_task_started(task_id=task_id)
            except Exception as e:
                log.warning("DB set_task_started failed", {"error": str(e)})
        # Build task context
        task_ctx = {"config": {"work_bucket": WORK_BUCKET, "step": step}, "logger": log, "storage": storage}
        outputs = hooks.process(task_ctx, validated)

        write_info = _write_outputs(storage, output_prefix, step, outputs, validated.task_id)
        if db:
            try:
                db.save_task_result(
                    task_id=task_id,
                    primary_result_uri=write_info.get("primary_result_uri"),
                    metrics_uri=write_info.get("metrics_uri"),
                    primary_mime=write_info.get("primary_mime"),
                    primary_size_bytes=write_info.get("primary_size_bytes"),
                    extra=write_info.get("extra"),
                )
                db.set_task_succeeded(task_id=task_id)
            except Exception as e:
                log.warning("DB save_task_result/set_task_succeeded failed", {"error": str(e)})
        _emit_children(storage, sqs_client, validated, outputs, step)

        sqs_client.delete_message(queue_url, receipt)
        log.info("Task completed ✓")

    except Exception as e:
        logger.error(e, {"context": "process", "task_id": task_id})
        try:
            if db and task_id != "unknown":
                db.set_task_failed(task_id=task_id, error_code="PROCESS_ERROR", error_message=str(e))
        except Exception:
            pass


# ==========================================================
# Output Writing
# ==========================================================

def _write_outputs(storage: S3Storage, prefix: str, step: str, outputs: Dict[str, Any], task_id: str = None) -> Dict[str, Any]:
    """Write model outputs - handles bytes, list[bytes], dict, list[dict].
    Returns basic result metadata for DB."""
    if not prefix.startswith("s3://"):
        raise ValueError(f"Invalid S3 prefix: {prefix}")

    primary_name = _get_primary_name(step)
    primary_result_uri = None
    primary_mime = None
    primary_size_bytes = None
    metrics_uri = None
    extra_out: Dict[str, Any] = {}
    
    # Write metrics
    if "metrics" in outputs and isinstance(outputs["metrics"], dict):
        metrics_uri = storage.put_json(prefix, "metrics.json", outputs["metrics"])
    
    result = outputs.get("result")
    
    # Handle different result types
    if result is None:
        pass  # No result to write
    
    elif isinstance(result, bytes):
        # Single binary file
        primary_result_uri = storage.put_bytes(prefix, primary_name, result, 
                         content_type=guess_content_type(primary_name))
        primary_mime = guess_content_type(primary_name)
        primary_size_bytes = len(result)
    
    elif isinstance(result, dict):
        # Single JSON result
        primary_result_uri = storage.put_json(prefix, primary_name, result)
        primary_mime = guess_content_type(primary_name)
    
    elif isinstance(result, list):
        # Multiple results - create individual task folders for each item
        if len(result) > 1:
            # Create individual tasks for each item
            for idx, item in enumerate(result):
                if isinstance(item, bytes):
                    # Create individual task folder for this item
                    individual_task_id = f"{task_id}-{idx:03d}"
                    # Extract bucket, user_id and job_id from prefix: s3://bucket/user_id/job_id/step/task_id/
                    prefix_parts = prefix.split('/')
                    bucket = prefix_parts[2]
                    user_id = prefix_parts[3]
                    job_id = prefix_parts[4]
                    individual_prefix = f"s3://{bucket}/{user_id}/{job_id}/{step}/{individual_task_id}/"
                    
                    # Save as result.png in individual folder
                    uri = storage.put_bytes(individual_prefix, "result.png", item,
                                    content_type=guess_content_type("result.png"))
                    extra_out[f"item_{idx}"] = {"result_uri": uri}
                elif isinstance(item, dict):
                    # Create individual task folder for this JSON item
                    individual_task_id = f"{task_id}-{idx:03d}"
                    # Extract bucket, user_id and job_id from prefix: s3://bucket/user_id/job_id/step/task_id/
                    prefix_parts = prefix.split('/')
                    bucket = prefix_parts[2]
                    user_id = prefix_parts[3]
                    job_id = prefix_parts[4]
                    individual_prefix = f"s3://{bucket}/{user_id}/{job_id}/{step}/{individual_task_id}/"
                    
                    # Save as result.json in individual folder
                    uri = storage.put_json(individual_prefix, "result.json", item)
                    extra_out[f"item_{idx}"] = {"result_uri": uri}
                else:
                    raise TypeError(f"Unsupported list item type: {type(item)}")
        else:
            # Single item in list - save normally
            if isinstance(result[0], bytes):
                primary_result_uri = storage.put_bytes(prefix, primary_name, result[0],
                                content_type=guess_content_type(primary_name))
                primary_mime = guess_content_type(primary_name)
                primary_size_bytes = len(result[0])
            elif isinstance(result[0], dict):
                primary_result_uri = storage.put_json(prefix, primary_name, result[0])
                primary_mime = guess_content_type(primary_name)
            else:
                raise TypeError(f"Unsupported list item type: {type(result[0])}")
    else:
        raise TypeError(f"Unsupported result type: {type(result)}")
    
    # Write auxiliary data
    aux = outputs.get("aux", {})
    for name, data in aux.items():
        ensure_within_prefix(prefix, name)
        if isinstance(data, bytes):
            uri = storage.put_bytes(prefix, name, data, 
                            content_type=guess_content_type(name))
            extra_out[name] = {"result_uri": uri}
        elif isinstance(data, dict):
            uri = storage.put_json(prefix, name, data)
            extra_out[name] = {"result_uri": uri}
    
    return {
        "primary_result_uri": primary_result_uri,
        "primary_mime": primary_mime,
        "primary_size_bytes": primary_size_bytes,
        "metrics_uri": metrics_uri,
        "extra": extra_out or None,
    }


# ==========================================================
# Fan-out to Next Step(s)
# ==========================================================

def _emit_children(storage: S3Storage, sqs_client: SQSClient,
                   parent_task, outputs: Dict[str, Any], step: str):
    """
    Emit fan-out messages using explicit 'children' list from hooks.
    
    Hooks return:
    {
        "result": <any type>,
        "metrics": {...},
        "children": [          # Optional - data for child tasks
            {...},             # Each becomes params for a child task
            {...}
        ]
    }
    """
    next_steps = get_next_steps(step)
    if not next_steps:
        return
    
    # Get explicit children list from hooks
    children = outputs.get("children")
    
    # No children = no fan-out (totally fine for final steps)
    if not children:
        return
    
    # Validate children is a non-empty list
    if not isinstance(children, list) or not children:
        return
    
    for next_step in next_steps:
        messages: List[Dict[str, Any]] = []
        
        # Each child becomes a separate task
        for idx, child_data in enumerate(children):
            # Use explicit idx if provided, otherwise use enumerate idx
            explicit_idx = child_data.get("idx") if isinstance(child_data, dict) else None
            child_task_id = _det_task_id(next_step, idx, explicit_idx)
            child_prefix = make_output_prefix(parent_task.user_id, parent_task.job_id, next_step, child_task_id)
            
            # Convert child_data to params dict
            if isinstance(child_data, dict):
                params = child_data
            else:
                # Wrap non-dict data
                params = {"data": child_data, "index": idx}
            
            # Construct input_uri - use individual task folder if available
            if explicit_idx is not None:
                # Use individual task folder created by list processing
                individual_task_id = f"{parent_task.task_id}-{explicit_idx:03d}"
                # Derive bucket/user_id/job_id from the parent output_prefix
                _parts = parent_task.output_prefix.split('/')
                _bucket = _parts[2]
                _user_id = _parts[3]
                _job_id = _parts[4]
                individual_prefix = f"s3://{_bucket}/{_user_id}/{_job_id}/{parent_task.step}/{individual_task_id}/"
                input_uri = individual_prefix + "result.png"
            else:
                # Fallback to primary name
                input_uri = parent_task.output_prefix + _get_primary_name(step)
            
            msg = {
                "job_id": parent_task.job_id,
                "task_id": child_task_id,
                "user_id": parent_task.user_id,
                "schema": parent_task.schema,
                "step": next_step,
                "input_uri": input_uri,
                "output_prefix": child_prefix,
                "params": params,
                "parent_task_id": parent_task.task_id,
                "trace_id": parent_task.trace_id,
                "retry_count": 0,
            }
            messages.append(msg)
        
        # Send in batches of 10
        next_q = get_queue_url(next_step)
        if next_q and messages:
            for i in range(0, len(messages), 10):
                sqs_client.send_messages_batch(next_q, messages[i:i + 10])


# ==========================================================
# Entrypoint
# ==========================================================

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m worker_sdk.runner <STEP> [LOG_LEVEL]")
        print("Example: python -m worker_sdk.runner IMAGE_GENERATION DEBUG")
        sys.exit(1)
    
    step_arg = sys.argv[1]
    log_level_arg = sys.argv[2] if len(sys.argv) > 2 else "INFO"
    
    main(step=step_arg, log_level=log_level_arg)