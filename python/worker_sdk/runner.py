#!/usr/bin/env python3
"""
Worker Runner (MVP)
-------------------
Bootstraps the service runtime using modular S3Storage and SQSClient.

Flow:
1. Load config from env
2. Instantiate S3Storage and SQSClient (modular, testable)
3. Build Ctx and load service hooks
4. Loop forever: receive → idempotency check → process → write → ACK
"""

import os
import sys
import time
import importlib
from typing import Any, Dict, Optional

from .contracts import Ctx, WorkerHooks
from .io_s3 import S3Storage
from .io_sqs import SQSClient
from .logging import get_logger
from .idempotency import result_exists


# =============================
# 1️⃣  Initialization
# =============================

def main():
    """
    Main entry point for the worker.
    
    Expected env vars:
    - QUEUE_URL: SQS queue to poll
    - WORK_BUCKET: S3 bucket for outputs
    - HOOKS_PATH: Python path to service hooks class (e.g., "service.hooks.ServiceHooks")
    - AWS_REGION: Optional AWS region
    - LOG_LEVEL: Optional logging level (DEBUG, INFO, WARNING, ERROR)
    """
    # --- Load config from env ---
    queue_url = os.environ["QUEUE_URL"]
    work_bucket = os.environ["WORK_BUCKET"]
    hooks_path = os.environ.get("HOOKS_PATH", "service.hooks.ServiceHooks")
    region = os.environ.get("AWS_REGION")
    log_level = os.environ.get("LOG_LEVEL", "INFO")
    next_queue_url = os.environ.get("NEXT_QUEUE_URL")
    
    # --- Initialize logger ---
    logger = get_logger("runner", level=log_level)
    logger.info("Initializing worker", {
        "queue_url": queue_url,
        "work_bucket": work_bucket,
        "hooks_path": hooks_path,
    })
    
    # --- Build modular adapters (IO) ---
    storage = S3Storage()  # Modular S3 client
    sqs_client = SQSClient(region=region)  # Modular SQS client
    db = None  # Optional MVP skip
    
    # --- Build context ---
    ctx = Ctx(
        config={"work_bucket": work_bucket, "queue_url": queue_url, "next_queue_url": next_queue_url},
        storage=storage,
        sqs=sqs_client.sqs,  # Pass underlying boto3 client for compatibility
        db=db,
        logger=logger,
    )
    
    # --- Import and instantiate service hooks ---
    logger.info("Loading service hooks", {"hooks_path": hooks_path})
    hooks_cls = _load_hooks_class(hooks_path)
    hooks = hooks_cls()
    
    # --- Preload model/pipeline once ---
    logger.info("Loading pipeline...")
    pipeline = hooks.load_pipeline(ctx)
    hooks.model = pipeline
    logger.info("Pipeline loaded and ready ✓")
    
    # =============================
    # 2️⃣  Main loop
    # =============================
    
    logger.info("Starting main loop...")
    while True:
        try:
            msgs = sqs_client.receive_messages(queue_url, max_messages=1, wait_seconds=20)
            
            if not msgs:
                # No messages, continue polling
                continue
            
            for raw_msg in msgs:
                _process_message(ctx, hooks, sqs_client, storage, queue_url, raw_msg, logger)
        
        except KeyboardInterrupt:
            logger.info("Shutting down (KeyboardInterrupt)")
            sys.exit(0)
        except Exception as e:
            logger.error(e, {"context": "main_loop"})
            time.sleep(5)  # Back off on unexpected errors


def _process_message(
    ctx: Ctx,
    hooks: WorkerHooks,
    sqs_client: SQSClient,
    storage: S3Storage,
    queue_url: str,
    raw_msg: Dict[str, Any],
    logger,
):
    """Process a single SQS message through the full pipeline."""
    try:
        # Parse message
        task, receipt_handle, receive_count = sqs_client.parse_message(raw_msg)
        task_id = task.get("task_id", "unknown")
        output_prefix = task.get("output_prefix", "")
        
        # Bind task context to logger
        task_logger = logger.bind(
            job_id=task.get("job_id"),
            task_id=task_id,
            step=task.get("step"),
        )
        
        task_logger.info("Received task")
        
        # 1️⃣ Poison pill check
        if sqs_client.is_poison_pill(receive_count):
            task_logger.warning("Poison pill detected", {"receive_count": receive_count})
            # Move to DLQ if configured
            dlq_url = os.environ.get("DLQ_URL")
            if dlq_url:
                sqs_client.move_to_dlq(dlq_url, task, reason="max_retries_exceeded")
            sqs_client.delete_message(queue_url, receipt_handle)
            return
        
        if result_exists(storage, output_prefix):
            task_logger.info("Skipped (already done - idempotent)")
            sqs_client.delete_message(queue_url, receipt_handle)
            return
        
        task_logger.info("Processing...")
        outputs = hooks.process(ctx, task)
        
        task_logger.info("Writing outputs to S3", {"output_prefix": output_prefix})
        _write_outputs(storage, output_prefix, outputs)
        
        fanout = outputs.get("fanout", [])
        if fanout:
            task_logger.info("Publishing fan-out tasks", {"count": len(fanout)})
            _publish_fanout(ctx, sqs_client, task, fanout)
        
        sqs_client.delete_message(queue_url, receipt_handle)
        task_logger.info("Task completed successfully ✓")
    
    except Exception as e:
        logger.error(e, {
            "context": "process_message",
            "task_id": task.get("task_id", "unknown") if 'task' in locals() else "unknown",
        })
        # Message will become visible again for retry

def _load_hooks_class(path: str):
    """
    Import the service's hooks dynamically.
    
    Example:
        path = "service.hooks.ServiceHooks"
        returns: ServiceHooks class
    """
    mod_name, cls_name = path.rsplit(".", 1)
    module = importlib.import_module(mod_name)
    return getattr(module, cls_name)


# Removed: _result_exists() - now using result_exists() from idempotency.py


def _write_outputs(storage: S3Storage, output_prefix: str, outputs: Dict[str, Any]):
    """
    Write service outputs to S3.
    
    Expected outputs structure:
    {
        "result": <dict or bytes>,  # Primary output (result.json or result.png/mp4)
        "metrics": <dict>,          # Metrics (metrics.json)
        "aux": {                    # Optional auxiliary files
            "debug.png": <bytes>,
            "overlay.png": <bytes>,
        }
    }
    """
    if not output_prefix or not output_prefix.startswith("s3://"):
        raise ValueError(f"Invalid output_prefix: {output_prefix}")
    
    result = outputs.get("result")
    if result is not None:
        if isinstance(result, dict):
            storage.put_json(output_prefix, "result.json", result)
        elif isinstance(result, bytes):
            # Infer extension from content or use generic .bin
            storage.put_bytes(output_prefix, "result.png", result)  # TODO: better extension inference
        else:
            raise TypeError(f"result must be dict or bytes, got {type(result)}")
    
    metrics = outputs.get("metrics")
    if metrics and isinstance(metrics, dict):
        storage.put_json(output_prefix, "metrics.json", metrics)
    
    # Write auxiliary files
    aux = outputs.get("aux")
    if aux and isinstance(aux, dict):
        for filename, data in aux.items():
            if isinstance(data, bytes):
                storage.put_bytes(output_prefix, filename, data)
            elif isinstance(data, dict):
                storage.put_json(output_prefix, filename, data)


def _publish_fanout(
    ctx: Ctx,
    sqs_client: SQSClient,
    parent_task: Dict[str, Any],
    fanout: list,
):
    """
    Publish fan-out tasks to the next queue.
    
    Each fanout entry should be a dict with at least:
    - task_id
    - input_uri
    - output_prefix
    """
    next_queue_url = ctx.config.get("next_queue_url")
    if not next_queue_url:
        raise ValueError("next_queue_url not configured for fan-out")
    
    # Build child messages
    child_messages = []
    for child in fanout:
        msg = {
            "job_id": parent_task.get("job_id"),
            "task_id": child.get("task_id"),
            "user_id": parent_task.get("user_id"),
            "schema": parent_task.get("schema", "v1"),
            "step": child.get("step"),
            "input_uri": child.get("input_uri"),
            "output_prefix": child.get("output_prefix"),
            "params": child.get("params", {}),
            "parent_task_id": parent_task.get("task_id"),
            "trace_id": parent_task.get("trace_id"),
            "retry_count": 0,
        }
        child_messages.append(msg)
    
    sqs_client.send_messages_batch(next_queue_url, child_messages)


if __name__ == "__main__":
    main()
