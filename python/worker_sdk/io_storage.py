"""
S3/R2 I/O: read input_uri, write to output_prefix.

Core file operations for S3/R2 with:
- Smart memory management (spills large files to disk)
- Atomic writes (tmp → final)
- Retry logic with exponential backoff
- Security (prefix boundaries, extension allowlist)
"""

import io
import json
import mimetypes
import os
import random
import re
import tempfile
import time
import uuid
from typing import Optional, Dict, Any, List, IO, Tuple

from botocore.exceptions import (
    ClientError,
    EndpointConnectionError,
    ReadTimeoutError,
    ConnectionClosedError,
    ConnectTimeoutError,
)

from .constants import VALID_STEPS, SUPPORTED_FILE_EXTENSIONS, EXTENSION_MIME_TYPES


# ============================================================================
# PATH HELPERS (moved from contracts.py)
# ============================================================================

def build_output_prefix(bucket: str, job_id: str, step: str, task_id: str) -> str:
    """
    Build canonical output_prefix: s3://<bucket>/work/<job_id>/<step>/<task_id>/
    Validates bucket, job_id, task_id format and step enum. Normalizes step to uppercase.
    """
    if not all([bucket, job_id, step, task_id]):
        raise ValueError("All parameters required: bucket, job_id, step, task_id")
    
    bucket = bucket.strip().strip('/')
    job_id = job_id.strip().strip('/')
    step = step.strip().strip('/').upper()
    task_id = task_id.strip().strip('/')
    
    # Validate bucket (S3 rules: 3-63 chars, lowercase, digits, dots, hyphens)
    if not re.match(r'^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$', bucket):
        raise ValueError(f"Invalid bucket name: {bucket}")
    
    # Validate job_id and task_id
    if not re.match(r'^[a-zA-Z0-9_-]{1,128}$', job_id):
        raise ValueError(f"Invalid job_id: {job_id}")
    if not re.match(r'^[a-zA-Z0-9_-]{1,256}$', task_id):
        raise ValueError(f"Invalid task_id: {task_id}")
    
    # Validate step against enum
    if step not in VALID_STEPS:
        raise ValueError(f"Invalid step: {step}. Must be one of {VALID_STEPS}")
    
    # Block path traversal
    if '..' in bucket or '..' in job_id or '..' in step or '..' in task_id:
        raise ValueError("Path traversal not allowed")
    
    return f"s3://{bucket}/work/{job_id}/{step}/{task_id}/"


def extract_job_id_from_prefix(output_prefix: str) -> Optional[str]:
    """
    Parse job_id from output_prefix. Returns None if path doesn't match contract pattern.
    """
    if not output_prefix or not isinstance(output_prefix, str):
        return None
    
    # Match exact contract pattern
    pattern = r'^s3://[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]/work/([a-zA-Z0-9_-]+)/[^/]+/[^/]+/?$'
    match = re.match(pattern, output_prefix)
    
    return match.group(1) if match else None


# ============================================================================
# CORE I/O FUNCTIONS
# ============================================================================

def head(uri: str) -> Dict[str, Any]:
    """
    Get object metadata without downloading content.
    Returns: {size, content_type, etag, last_modified, metadata}
    """
    bucket, key = _parse_s3_uri(uri)
    s3 = _get_s3_client()
    
    transient_codes = {"500", "503", "RequestTimeout", "SlowDown", "InternalError", "ServiceUnavailable"}
    network_excs = (EndpointConnectionError, ReadTimeoutError, ConnectionClosedError, ConnectTimeoutError)

    for attempt in range(3):
        try:
            response = s3.head_object(Bucket=bucket, Key=key)

            size = response.get("ContentLength")
            content_type = response.get("ContentType", "application/octet-stream")
            etag = response.get("ETag", "")
            if isinstance(etag, str):
                etag = etag.strip('"')
            last_modified = response.get("LastModified")
            user_metadata = response.get("Metadata", {})

            return {
                "size": size,
                "content_type": content_type,
                "etag": etag,
                "last_modified": last_modified,
                "metadata": user_metadata,
            }

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NotFound", "NoSuchKey"):
                raise FileNotFoundError(f"File not found: {uri}") from e
            if code == "NoSuchBucket":
                raise FileNotFoundError(f"Bucket not found: {uri}") from e
            if code in ("AccessDenied", "403"):
                raise PermissionError(f"Access denied: {uri}") from e
            if code in ("PermanentRedirect", "AuthorizationHeaderMalformed"):
                raise ValueError(f"Wrong region/endpoint for {uri}: {code}") from e
            if code in transient_codes and attempt < 2:
                time.sleep((2 ** attempt) + random.uniform(0, 0.25))
                continue
            raise

        except network_excs:
            if attempt < 2:
                time.sleep((2 ** attempt) + random.uniform(0, 0.25))
                continue
            raise

    raise RuntimeError(f"Failed to HEAD after 3 attempts: {uri}")



from typing import Dict, Any

def exists(uri: str) -> bool:
    """
    Return True if file exists, False if not found. Bubbles other errors (permission, network).
    """
    try:
        head(uri)
        return True
    except FileNotFoundError:
        return False


def get_bytes(
    uri: str,
    *,
    chunk_size: int = 15 * 1024 * 1024,
    max_inline_bytes: int = 100 * 1024 * 1024,
    dest_dir: Optional[str] = None
):
    """
    Download file. Returns bytes if ≤100MB, else streams to temp file and returns path.
    Validates extension, retries on transient errors (3x exponential backoff).
    """
    
    if not _check_extension(uri, SUPPORTED_FILE_EXTENSIONS):
        raise ValueError(f"File extension not allowed: {uri}")
    
    metadata = head(uri)
    file_size = metadata.get("size")
    bucket, key = _parse_s3_uri(uri)
    s3 = _get_s3_client()
    
    max_attempts = 3
    transient_codes = {"500", "503", "RequestTimeout", "SlowDown", "InternalError", "ServiceUnavailable"}
    network_excs = (EndpointConnectionError, ReadTimeoutError, ConnectionClosedError, ConnectTimeoutError)
    
    use_disk = file_size is not None and file_size > max_inline_bytes
    
    for attempt in range(max_attempts):
        try:
            response = s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"]
            
            # Stream to disk for large files
            if use_disk:
                _, ext = os.path.splitext(key)
                tmp_file = tempfile.NamedTemporaryFile(
                    delete=False,
                    dir=dest_dir,
                    prefix="sdk_",
                    suffix=ext or ".bin"
                )
                try:
                    for chunk in body.iter_chunks(chunk_size=chunk_size):
                        if chunk:
                            tmp_file.write(chunk)
                finally:
                    body.close()
                    tmp_file.close()
                return tmp_file.name
            
            # Stream to memory for small files
            else:
                chunks = []
                total_bytes = 0
                
                for chunk in body.iter_chunks(chunk_size=chunk_size):
                    if not chunk:
                        continue
                    
                    total_bytes += len(chunk)
                    
                    # Dynamic spillover if size unknown and grows too large
                    if file_size is None and total_bytes > max_inline_bytes:
                        if dest_dir is None:
                            raise ValueError(f"File exceeds {max_inline_bytes} bytes but no dest_dir for spillover")
                        
                        # Spill to disk
                        _, ext = os.path.splitext(key)
                        tmp_file = tempfile.NamedTemporaryFile(
                            delete=False,
                            dir=dest_dir,
                            prefix="sdk_",
                            suffix=ext or ".bin"
                        )
                        try:
                            for buffered in chunks:
                                tmp_file.write(buffered)
                            tmp_file.write(chunk)
                            for remaining in body.iter_chunks(chunk_size=chunk_size):
                                if remaining:
                                    tmp_file.write(remaining)
                        finally:
                            body.close()
                            tmp_file.close()
                        return tmp_file.name
                    
                    chunks.append(chunk)
                
                body.close()
                return b"".join(chunks)
        
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            
            if error_code in ("NoSuchKey", "404", "NotFound"):
                raise FileNotFoundError(f"File not found: {uri}") from e
            if error_code == "NoSuchBucket":
                raise FileNotFoundError(f"Bucket not found: {uri}") from e
            if error_code in ("AccessDenied", "403"):
                raise PermissionError(f"Access denied: {uri}") from e
            
            if error_code in transient_codes and attempt < max_attempts - 1:
                time.sleep((2 ** attempt) + random.uniform(0, 0.25))
                continue
            raise
        
        except network_excs:
            if attempt < max_attempts - 1:
                time.sleep((2 ** attempt) + random.uniform(0, 0.25))
                continue
            raise
    
    raise RuntimeError(f"Failed to download after {max_attempts} attempts: {uri}")


def get_json(uri: str) -> Dict[str, Any]:
    """
    Download and parse JSON file. Returns dict/list.
    """
    data = get_bytes(uri)
    
    # Handle both bytes and file path
    if isinstance(data, str):
        with open(data, 'rb') as f:
            content = f.read()
    else:
        content = data
    
    try:
        return json.loads(content.decode('utf-8'))
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON in {uri}: {e.msg}", e.doc, e.pos)


def put_bytes(prefix: str, filename: str, data: bytes, content_type: str = None, metadata: Dict[str, str] = None) -> str:
    """
    Write bytes to S3 with atomic write (tmp→final) and multipart for large files.
    Enforces prefix boundaries. Returns final S3 URI.
    """
    
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError("put_bytes() expects bytes or bytearray")
    
    # Enforce filename safety
    if not filename or "/" in filename or "\\" in filename or ".." in filename:
        raise ValueError(f"Invalid filename: {filename}")
    
    # Parse prefix
    bucket, base_key = _parse_s3_uri(prefix)
    if base_key and not base_key.endswith("/"):
        base_key = base_key + "/"
    
    # Extension check
    if not _check_extension(filename, SUPPORTED_FILE_EXTENSIONS):
        raise ValueError(f"File extension not allowed: {filename}")
    
    final_key = f"{base_key}{os.path.basename(filename)}"
    tmp_key = f"{final_key}.tmp-{uuid.uuid4().hex}"
    
    if content_type is None:
        content_type = guess_content_type(filename)
    
    metadata = metadata or {}
    s3 = _get_s3_client()
    
    attempts = 3
    transient_codes = {"500", "503", "SlowDown", "RequestTimeout", "InternalError", "ServiceUnavailable"}
    network_excs = (EndpointConnectionError, ReadTimeoutError, ConnectionClosedError, ConnectTimeoutError)
    
    multipart_threshold = 8 * 1024 * 1024
    part_size = 16 * 1024 * 1024
    
    def _copy_tmp_to_final():
        s3.copy_object(
            Bucket=bucket,
            Key=final_key,
            CopySource={"Bucket": bucket, "Key": tmp_key},
            MetadataDirective="COPY",
        )
    
    def _delete_tmp_silent():
        try:
            s3.delete_object(Bucket=bucket, Key=tmp_key)
        except Exception:
            pass
    
    for attempt in range(attempts):
        upload_id = None
        try:
            # Upload to tmp key
            if len(data) < multipart_threshold:
                s3.put_object(
                    Bucket=bucket,
                    Key=tmp_key,
                    Body=data,
                    ContentType=content_type,
                    Metadata=metadata,
                )
            else:
                # Multipart upload
                create_resp = s3.create_multipart_upload(
                    Bucket=bucket,
                    Key=tmp_key,
                    ContentType=content_type,
                    Metadata=metadata,
                )
                upload_id = create_resp["UploadId"]
                
                parts = []
                stream = io.BytesIO(data)
                part_number = 1
                while True:
                    chunk = stream.read(part_size)
                    if not chunk:
                        break
                    up = s3.upload_part(
                        Bucket=bucket,
                        Key=tmp_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk,
                    )
                    parts.append({"ETag": up["ETag"], "PartNumber": part_number})
                    part_number += 1
                
                s3.complete_multipart_upload(
                    Bucket=bucket,
                    Key=tmp_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts},
                )
                upload_id = None
            
            # Atomic finalize
            _copy_tmp_to_final()
            _delete_tmp_silent()
            
            return f"s3://{bucket}/{final_key}"
        
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if upload_id:
                try:
                    s3.abort_multipart_upload(Bucket=bucket, Key=tmp_key, UploadId=upload_id)
                except Exception:
                    pass
            _delete_tmp_silent()
            
            if code in ("AccessDenied", "403"):
                raise PermissionError(f"Access denied: s3://{bucket}/{final_key}") from e
            if code == "NoSuchBucket":
                raise FileNotFoundError(f"Bucket not found: s3://{bucket}/") from e
            if code in ("PermanentRedirect", "AuthorizationHeaderMalformed"):
                raise ValueError(f"Wrong region/endpoint for bucket {bucket}") from e
            if code in transient_codes and attempt < attempts - 1:
                time.sleep((2 ** attempt) + random.uniform(0, 0.25))
                continue
            raise
        
        except network_excs:
            if upload_id:
                try:
                    s3.abort_multipart_upload(Bucket=bucket, Key=tmp_key, UploadId=upload_id)
                except Exception:
                    pass
            _delete_tmp_silent()
            
            if attempt < attempts - 1:
                time.sleep((2 ** attempt) + random.uniform(0, 0.25))
                continue
            raise
        
        except Exception:
            if upload_id:
                try:
                    s3.abort_multipart_upload(Bucket=bucket, Key=tmp_key, UploadId=upload_id)
                except Exception:
                    pass
            _delete_tmp_silent()
            raise
    
    raise RuntimeError(f"Failed to upload after {attempts} attempts: s3://{bucket}/{final_key}")


def put_json(prefix: str, filename: str, obj: Dict[str, Any], metadata: Dict[str, str] | None = None) -> str:
    """
    Serialize dict to JSON (UTF-8) and write to S3.
    """
    json_bytes = json.dumps(obj, indent=2).encode("utf-8")
    return put_bytes(prefix, filename, json_bytes, "application/json; charset=utf-8", metadata or {})


def delete(uri: str) -> None:
    """
    Delete object from S3/R2. Idempotent for missing keys.
    Retries transient and network errors with backoff + jitter.
    """
    bucket, key = _parse_s3_uri(uri)
    s3 = _get_s3_client()

    transient_codes = {"500", "503", "RequestTimeout", "SlowDown", "InternalError", "ServiceUnavailable"}
    network_excs = (EndpointConnectionError, ReadTimeoutError, ConnectionClosedError)

    for attempt in range(3):
        try:
            # S3 DeleteObject is idempotent and generally returns 204 even if key is missing
            s3.delete_object(Bucket=bucket, Key=key)
            return
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")

            # Treat missing key as success (idempotent semantics)
            if code in ("404", "NoSuchKey", "NotFound"):
                return

            # Config/permissions: surface clearly
            if code in ("NoSuchBucket",):
                raise FileNotFoundError(f"Bucket not found for URI: {uri}") from e
            if code in ("AccessDenied", "403"):
                raise PermissionError(f"Access denied deleting: {uri}") from e

            # Retry transient server-side errors
            if code in transient_codes and attempt < 2:
                time.sleep((2 ** attempt) + random.uniform(0, 0.25))
                continue
            raise

        except network_excs as e:
            if attempt < 2:
                time.sleep((2 ** attempt) + random.uniform(0, 0.25))
                continue
            raise

    raise RuntimeError(f"Failed to delete after 3 attempts: {uri}")



# ============================================================================
# HELPERS
# ============================================================================

def guess_content_type(filename: str) -> str:
    """
    Infer MIME type for common images, videos, audio, and documents.
    Falls back to mimetypes and then application/octet-stream.
    """
    name = os.path.basename(str(filename)).split("?", 1)[0].split("#", 1)[0]
    ext = os.path.splitext(name)[1].lower()
    
    # Use mapping from constants
    if ext in EXTENSION_MIME_TYPES:
        ct = EXTENSION_MIME_TYPES[ext]
    else:
        guessed, _ = mimetypes.guess_type(name)
        ct = guessed or "application/octet-stream"
    
    # Add charset for text-based types
    if ct.startswith("text/") and "charset=" not in ct:
        return f"{ct}; charset=utf-8"
    if ct == "application/json":
        return f"{ct}; charset=utf-8"
    
    return ct



def ensure_within_prefix(prefix: str, filename: str) -> str:
    """
    Build safe s3://.../prefix/filename. Blocks path traversal and slashes in filename.
    """
    if not isinstance(prefix, str) or not isinstance(filename, str):
        raise ValueError("prefix and filename must be strings")

    prefix = prefix.strip()
    filename = filename.strip()

    if not filename:
        raise ValueError("filename cannot be empty")

    # Validate prefix as an S3 URI and get its components
    bucket, base_key = _parse_s3_uri(prefix)

    # Path traversal / absolutes
    if filename.startswith("/") or ".." in filename:
        raise ValueError(f"Path traversal or absolute path not allowed: {filename}")

    # Forbid directory separators in filename (no subdirs)
    if "/" in filename or "\\" in filename:
        raise ValueError(f"Slashes not allowed in filename: {filename}")

    # Forbid control chars and a few dangerous shell chars
    if any(ord(c) < 32 for c in filename) or any(c in filename for c in (";", "|", "\x00")):
        raise ValueError(f"Dangerous characters in filename: {filename}")

    # Basic length guard (S3 key max is ~1024 bytes; keep headroom)
    if len(filename.encode("utf-8")) > 512:
        raise ValueError("filename too long")

    # Normalize base_key to **exactly one** trailing slash (if non-empty)
    if base_key:
        base_key = base_key.rstrip("/") + "/"

    return f"s3://{bucket}/{base_key}{filename}"

# ============================================================================
# PRIVATE HELPERS
# ============================================================================

def _get_s3_client():
    """Get boto3 S3 client (cached singleton pattern would go here in production)."""
    import boto3
    # TODO: Add caching, R2 endpoint configuration from config
    return boto3.client('s3')


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    """
    Parse s3://bucket/key into (bucket, key). Rejects bad URIs.
    MVP-hardening:
      - trims surrounding whitespace
      - rejects backslashes and URL/query fragments (?, #)
    """
    if not isinstance(uri, str):
        raise ValueError(f"Invalid URI (not a string): {uri!r}")

    uri = uri.strip()
    if not uri:
        raise ValueError("Invalid URI: empty string")

    if not uri.startswith("s3://"):
        raise ValueError(f"URI must start with s3://: {uri}")

    # Fast-fail on characters that don't belong in plain S3 URIs
    if any(c in uri for c in ("\\", "?", "#")):
        raise ValueError(f"Invalid S3 URI (contains '\\', '?' or '#'): {uri}")

    path = uri[len("s3://"):]  # remove scheme

    if "/" not in path:
        # MVP: require an object/prefix key (no bare-bucket URIs)
        raise ValueError(f"Invalid S3 URI format (missing key): {uri}")

    bucket, key = path.split("/", 1)
    if not bucket:
        raise ValueError(f"Invalid S3 URI (empty bucket): {uri}")
    if not key:
        # Key present but empty (e.g., s3://bucket/), treat as invalid for MVP
        raise ValueError(f"Invalid S3 URI (empty key): {uri}")

    return bucket, key


def _check_extension(filename: str, allowed_extensions: List[str]) -> bool:
    """
    Enforce safe, allow-listed file extensions (case-insensitive).
    """
    if not filename:
        return False
    
    filename_lower = filename.lower()
    return any(filename_lower.endswith(ext.lower()) for ext in allowed_extensions)

