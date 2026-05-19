"""
Worker SDK: shared work-queue contract, Postgres, S3/SQS I/O, Lambda+SQS helpers.

Import submodules explicitly in hot paths (e.g. ``worker_sdk.io_s3``). This package root
does **not** import ``runner`` or ``hooks`` (GPU worker entrypoints).
"""

from worker_sdk.task_ids import (
    scope_task_id,
    task_path_segment,
    task_result_lookup_keys,
)
from worker_sdk.pipeline_s3_paths import (
    STEP_MODEL_PROFILE,
    model_profile_uris,
    pipeline_step_base_prefix,
)
from worker_sdk.io_db import (
    JobRecord,
    PostgresDB,
    TaskRecord,
    VALID_JOB_STATUSES,
    VALID_TASK_STATUSES,
    validate_job_status,
    validate_task_status,
)
from worker_sdk.io_lambda import ParsedSqsRecord, parse_sqs_event, partial_batch_response
from worker_sdk.work_queue_message import (
    WORK_QUEUE_MESSAGE_KEYS,
    WORK_QUEUE_MESSAGE_VERSION,
    WORK_QUEUE_VALIDATION_RULES,
    WorkQueueMessage,
    assemble_work_queue_message,
    from_json_dict,
    normalize_job_id,
    normalize_s3_uri,
    parse_work_queue_message,
    parse_work_queue_message_json,
    prepare_task_uris,
)

__all__ = [
    "scope_task_id",
    "task_path_segment",
    "task_result_lookup_keys",
    "STEP_MODEL_PROFILE",
    "model_profile_uris",
    "pipeline_step_base_prefix",
    "VALID_JOB_STATUSES",
    "VALID_TASK_STATUSES",
    "JobRecord",
    "PostgresDB",
    "TaskRecord",
    "WORK_QUEUE_MESSAGE_KEYS",
    "WORK_QUEUE_MESSAGE_VERSION",
    "WORK_QUEUE_VALIDATION_RULES",
    "WorkQueueMessage",
    "assemble_work_queue_message",
    "from_json_dict",
    "normalize_job_id",
    "normalize_s3_uri",
    "parse_work_queue_message",
    "parse_work_queue_message_json",
    "prepare_task_uris",
    "validate_job_status",
    "validate_task_status",
    "ParsedSqsRecord",
    "parse_sqs_event",
    "partial_batch_response",
]
