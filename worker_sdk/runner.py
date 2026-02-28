import sys
import time
import json
import os
from typing import Any, Dict, List, Optional

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

def _sanitize_filename(filename: str) -> Optional[str]:
    """
    Sanitize filename to prevent path traversal and invalid characters.
    Returns None if filename is invalid.
    """
    if not filename or not isinstance(filename, str):
        return None
    
    # Remove path separators and traversal attempts
    filename = filename.replace('/', '').replace('\\', '')
    if '..' in filename:
        return None
    
    # Remove null bytes and other dangerous characters
    filename = filename.replace('\x00', '')
    if any(c in filename for c in [';', '|', '&', '$', '`']):
        return None
    
    # Get just the basename (no directory components)
    filename = os.path.basename(filename)
    
    # Ensure it's not empty and not just dots
    if not filename or filename in ['.', '..']:
        return None
    
    # Limit length (reasonable filename limit)
    if len(filename) > 255:
        return None
    
    return filename


def _is_dict_of_bytes(d: Dict[str, Any]) -> bool:
    """Check if dict contains only bytes values."""
    if not d:
        return False
    return all(isinstance(v, bytes) for v in d.values())


def _find_primary_result_key(d: Dict[str, Any]) -> Optional[str]:
    """
    Find the primary result key in a dict.
    Prefers keys starting with 'result' (e.g., 'result.png', 'result.jpg').
    Falls back to first key if no 'result' key found.
    """
    if not d:
        return None
    
    # First, try to find a key starting with 'result'
    for key in d.keys():
        if isinstance(key, str) and key.lower().startswith('result'):
            return key
    
    # Fall back to first key
    return next(iter(d.keys()), None)

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
    """Generate slot-style task IDs for child tasks: S0, S1, S2, ..."""
    if explicit_idx is not None:
        return f"S{explicit_idx}"
    return f"S{index}"


def _step_prefix(output_prefix: str) -> str:
    """Derive step-level prefix from output_prefix (with or without task_id)."""
    parts = output_prefix.rstrip("/").split("/")
    if len(parts) >= 6:
        return "s3://" + "/".join(parts[2:6]) + "/"
    return output_prefix


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

        step_prefix = _step_prefix(output_prefix)
        log.info("Message validated, starting processing", {"input_uri": validated.input_uri, "output_prefix": output_prefix, "step_prefix": step_prefix})

        # Keep message invisible during long-running work (e.g. COMBINED can take 10+ min; default visibility is 300s)
        visibility_timeout = (CONFIG.get("queues") or {}).get("timeout", 300)
        visibility_timeout = max(60, min(int(visibility_timeout), 43200))  # 1 min to 12h (SQS max)
        heartbeat_every = max(30, visibility_timeout // 4)

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
        log.info("Checking if task already completed (idempotency check)...")
        if result_exists(storage, step_prefix):
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

        # Extend visibility periodically so long-running tasks (e.g. COMBINED) don't lose the message mid-run
        with sqs_client.visibility_heartbeat(
            queue_url, receipt, base_timeout=visibility_timeout, heartbeat_every=heartbeat_every
        ):
            outputs = hooks.process(task_ctx, validated)

            write_info = _write_outputs(storage, step_prefix, step, outputs, validated.task_id)
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
            _emit_children(storage, sqs_client, validated, outputs, step, write_info=write_info)

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

def _slot_prefix(step_prefix: str, index: int) -> str:
    """Return S3 prefix for slot index: step_prefix + S0/, S1/, ..."""
    return f"{step_prefix.rstrip('/')}/S{index}/"


def _write_outputs(storage: S3Storage, prefix: str, step: str, outputs: Dict[str, Any], task_id: str = None) -> Dict[str, Any]:
    """Write model outputs under step-level prefix with clean hierarchy.

    Layout: prefix = .../job_id/STEP/
      - metrics.json at prefix
      - Single result -> prefix/S0/result.*
      - List of results -> prefix/S0/, prefix/S1/, ...

    Handles: bytes, dict, list[bytes], list[dict], dict[bytes], list[dict[bytes]].
    Returns basic result metadata for DB and child_input_uris for fan-out."""
    if not prefix.startswith("s3://"):
        raise ValueError(f"Invalid S3 prefix: {prefix}")

    primary_name = _get_primary_name(step)
    primary_result_uri = None
    primary_mime = None
    primary_size_bytes = None
    metrics_uri = None
    extra_out: Dict[str, Any] = {}
    child_input_uris: List[str] = []

    # Metrics at step level
    if "metrics" in outputs and isinstance(outputs["metrics"], dict):
        metrics_uri = storage.put_json(prefix, "metrics.json", outputs["metrics"])

    result = outputs.get("result")

    if result is None:
        pass

    elif isinstance(result, bytes):
        slot = _slot_prefix(prefix, 0)
        primary_result_uri = storage.put_bytes(slot, primary_name, result,
                         content_type=guess_content_type(primary_name))
        primary_mime = guess_content_type(primary_name)
        primary_size_bytes = len(result)
        if outputs.get("children"):
            child_input_uris.append(primary_result_uri)

    elif isinstance(result, dict):
        slot = _slot_prefix(prefix, 0)
        if _is_dict_of_bytes(result):
            saved_files = {}
            primary_key = _find_primary_result_key(result)
            for key, file_bytes in result.items():
                sanitized_name = _sanitize_filename(key)
                if sanitized_name is None:
                    continue
                try:
                    uri = storage.put_bytes(slot, sanitized_name, file_bytes,
                                           content_type=guess_content_type(sanitized_name))
                    saved_files[sanitized_name] = uri
                    if key == primary_key:
                        primary_result_uri = uri
                        primary_mime = guess_content_type(sanitized_name)
                        primary_size_bytes = len(file_bytes)
                except Exception:
                    continue
            if primary_result_uri is None and saved_files:
                first_file = next(iter(saved_files.items()))
                primary_result_uri = first_file[1]
                primary_mime = guess_content_type(first_file[0])
            if outputs.get("children"):
                child_input_uris.append(primary_result_uri)
        else:
            primary_result_uri = storage.put_json(slot, primary_name, result)
            primary_mime = guess_content_type(primary_name)
            if outputs.get("children"):
                child_input_uris.append(primary_result_uri)

    elif isinstance(result, list):
        if len(result) > 1:
            for idx, item in enumerate(result):
                slot = _slot_prefix(prefix, idx)
                if isinstance(item, bytes):
                    uri = storage.put_bytes(slot, "result.png", item,
                                           content_type=guess_content_type("result.png"))
                    extra_out[f"item_{idx}"] = {"result_uri": uri}
                    child_input_uris.append(uri)
                elif isinstance(item, dict):
                    if _is_dict_of_bytes(item):
                        saved_files = {}
                        primary_key = _find_primary_result_key(item)
                        primary_uri = None
                        for key, file_bytes in item.items():
                            sanitized_name = _sanitize_filename(key)
                            if sanitized_name is None:
                                continue
                            try:
                                uri = storage.put_bytes(slot, sanitized_name, file_bytes,
                                                       content_type=guess_content_type(sanitized_name))
                                saved_files[sanitized_name] = uri
                                if key == primary_key:
                                    primary_uri = uri
                            except Exception:
                                continue
                        result_uri = primary_uri if primary_uri else (next(iter(saved_files.values())) if saved_files else None)
                        if result_uri:
                            extra_out[f"item_{idx}"] = {"result_uri": result_uri}
                            child_input_uris.append(result_uri)
                    else:
                        uri = storage.put_json(slot, "result.json", item)
                        extra_out[f"item_{idx}"] = {"result_uri": uri}
                        child_input_uris.append(uri)
                else:
                    raise TypeError(f"Unsupported list item type: {type(item)}")
        else:
            # Single item in list -> S0/
            slot = _slot_prefix(prefix, 0)
            if isinstance(result[0], bytes):
                primary_result_uri = storage.put_bytes(slot, primary_name, result[0],
                                                      content_type=guess_content_type(primary_name))
                primary_mime = guess_content_type(primary_name)
                primary_size_bytes = len(result[0])
                child_input_uris.append(primary_result_uri)
            elif isinstance(result[0], dict):
                if _is_dict_of_bytes(result[0]):
                    saved_files = {}
                    primary_key = _find_primary_result_key(result[0])
                    for key, file_bytes in result[0].items():
                        sanitized_name = _sanitize_filename(key)
                        if sanitized_name is None:
                            continue
                        try:
                            uri = storage.put_bytes(slot, sanitized_name, file_bytes,
                                                   content_type=guess_content_type(sanitized_name))
                            saved_files[sanitized_name] = uri
                            if key == primary_key:
                                primary_result_uri = uri
                                primary_mime = guess_content_type(sanitized_name)
                                primary_size_bytes = len(file_bytes)
                        except Exception:
                            continue
                    if primary_result_uri is None and saved_files:
                        first_file = next(iter(saved_files.items()))
                        primary_result_uri = first_file[1]
                        primary_mime = guess_content_type(first_file[0])
                    if primary_result_uri is not None:
                        child_input_uris.append(primary_result_uri)
                else:
                    primary_result_uri = storage.put_json(slot, primary_name, result[0])
                    primary_mime = guess_content_type(primary_name)
                    child_input_uris.append(primary_result_uri)
            else:
                raise TypeError(f"Unsupported list item type: {type(result[0])}")
    else:
        raise TypeError(f"Unsupported result type: {type(result)}")

    # Auxiliary data at step level
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
        "child_input_uris": child_input_uris,
    }


# ==========================================================
# Fan-out to Next Step(s)
# ==========================================================

def _emit_children(storage: S3Storage, sqs_client: SQSClient,
                   parent_task, outputs: Dict[str, Any], step: str,
                   write_info: Optional[Dict[str, Any]] = None):
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
            
            # Construct input_uri: single source of truth from writer when available
            child_uris = (write_info or {}).get("child_input_uris") if write_info else []
            if child_uris and idx < len(child_uris):
                input_uri = child_uris[idx]
            else:
                # Fallback: step-level layout .../STEP/S0/result.*
                parent_step_prefix = _step_prefix(parent_task.output_prefix)
                slot_idx = explicit_idx if explicit_idx is not None else idx
                input_uri = _slot_prefix(parent_step_prefix, slot_idx) + _get_primary_name(step)
            
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