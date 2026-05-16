"""
AWS Lambda + SQS event helpers (CPU workers).

No HTTP frameworks. Used by Lambda handlers triggered from SQS.
See: https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html#services-sqs-batchfailurereporting
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, MutableMapping, Sequence, Union

RawSqsRecord = Mapping[str, Any]
LambdaSqsEvent = MutableMapping[str, Any]


@dataclass(frozen=True)
class ParsedSqsRecord:
    """One SQS record after JSON decode of the body."""

    message_id: str
    receipt_handle: str
    body: Dict[str, Any]


def parse_sqs_event(event: Union[LambdaSqsEvent, Mapping[str, Any]]) -> List[ParsedSqsRecord]:
    """
    Parse a Lambda event from an SQS event source mapping.

    Raises:
        ValueError: missing Records, bad record shape, or body is not a JSON object.
    """
    records = event.get("Records")
    if not isinstance(records, list) or not records:
        raise ValueError("SQS event must contain a non-empty Records list")

    out: List[ParsedSqsRecord] = []
    for i, rec in enumerate(records):
        if not isinstance(rec, Mapping):
            raise ValueError(f"Records[{i}] must be an object")
        mid = rec.get("messageId")
        rh = rec.get("receiptHandle")
        if not isinstance(mid, str) or not mid:
            raise ValueError(f"Records[{i}].messageId is required")
        if not isinstance(rh, str) or not rh:
            raise ValueError(f"Records[{i}].receiptHandle is required")
        raw_body = rec.get("body")
        if not isinstance(raw_body, str):
            raise ValueError(f"Records[{i}].body must be a string")
        try:
            parsed = json.loads(raw_body)
        except json.JSONDecodeError as e:
            raise ValueError(f"Records[{i}].body is not valid JSON: {e}") from e
        if not isinstance(parsed, dict):
            raise ValueError(f"Records[{i}].body JSON must be an object, got {type(parsed).__name__}")
        out.append(ParsedSqsRecord(message_id=mid, receipt_handle=rh, body=parsed))
    return out


def partial_batch_response(failed_message_ids: Sequence[str]) -> Dict[str, Any]:
    """
    Return the Lambda payload for partial SQS batch failure reporting.

    ``failed_message_ids`` must be SQS ``messageId`` values for failed items only.
    """
    failures = [{"itemIdentifier": mid} for mid in failed_message_ids]
    return {"batchItemFailures": failures}
