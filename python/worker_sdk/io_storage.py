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
) -> bytes:
    """
    Download an S3 object (must be ≤ 50 MB) and return its content as bytes.

    What this does
    --------------
    1) Validates the file extension against an allowlist.
    2) Uses HEAD to check object size up front and enforce a 50 MB hard cap.
    3) Streams the body in chunks (no single huge read) and re-enforces the cap while reading.
    4) Retries transient server/network errors up to 3 times with exponential backoff + jitter.
       Non-transient issues (e.g., 403/404) fail fast with clear exceptions.

    When to use it
    --------------
    Use when you need the whole object in memory (e.g., small JSON/config/media)
    and your service guarantees objects are ≤ 50 MB.

    Parameters
    ----------
    uri : str
        S3 URI of the object, e.g. "s3://my-bucket/path/to/file.json".
    chunk_size : int, optional
        Size (in bytes) for each streamed read. Larger chunks reduce network calls
        but increase peak memory per chunk. Default: 15 MiB.

    Returns
    -------
    bytes
        The full object content.

    Raises
    ------
    ValueError
        - File extension not allowed.
        - Object exceeds the 50 MB limit (detected via HEAD or during streaming).
        - URI is malformed (raised by _parse_s3_uri).
    FileNotFoundError
        - The bucket or object key does not exist.
    PermissionError
        - Access is denied to the object or bucket.
    RuntimeError
        - All retry attempts were exhausted without success.
    botocore.exceptions.*
        - Other client/network errors not considered transient.

    Notes
    -----
    - If the server does not provide a size via HEAD, the function still streams
      safely and enforces the 50 MB cap during the read loop.
    - Returned value is always bytes; there is no temp file to clean up.

    Example
    -------
    # Read a small JSON manifest (≤ 50 MB) into memory:
    # data = get_bytes("s3://media/config/manifest.json")
    # manifest = json.loads(data.decode("utf-8"))
    """
    MAX_OBJECT_SIZE_BYTES = 50 * 1024 * 1024  # 50 MB hard cap

    if not _check_extension(uri, SUPPORTED_FILE_EXTENSIONS):
        raise ValueError(f"File extension not allowed: {uri}")

    # Inspect size/type up front
    meta = head(uri)
    size = meta.get("size")

    # Enforce the cap using HEAD; if size is unknown, we also enforce during streaming
    if size is None:
        # Allow unknown here, but double-check in the stream loop
        pass
    elif size > MAX_OBJECT_SIZE_BYTES:
        raise ValueError(f"Object exceeds 50 MB limit ({size} bytes): {uri}")

    bucket, key = _parse_s3_uri(uri)
    s3 = _get_s3_client()

    max_attempts = 3
    transient_codes = {"500", "503", "RequestTimeout", "SlowDown", "InternalError", "ServiceUnavailable"}
    network_excs = (EndpointConnectionError, ReadTimeoutError, ConnectionClosedError, ConnectTimeoutError)

    for attempt in range(max_attempts):
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            body = resp["Body"]

            chunks = []
            total = 0

            for chunk in body.iter_chunks(chunk_size=chunk_size):
                if not chunk:
                    continue
                total += len(chunk)
                # Enforce cap defensively even if HEAD lied or was None
                if total > MAX_OBJECT_SIZE_BYTES:
                    body.close()
                    raise ValueError(f"Object exceeds 50 MB limit while streaming: {uri}")
                chunks.append(chunk)

            body.close()
            return b"".join(chunks)

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")

            if code in ("NoSuchKey", "404", "NotFound"):
                raise FileNotFoundError(f"File not found: {uri}") from e
            if code == "NoSuchBucket":
                raise FileNotFoundError(f"Bucket not found: {uri}") from e
            if code in ("AccessDenied", "403"):
                raise PermissionError(f"Access denied: {uri}") from e

            if code in transient_codes and attempt < max_attempts - 1:
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


def put_bytes(
    prefix: str,
    filename: str,
    data: bytes,
    content_type: str = None,
    metadata: Dict[str, str] = None,
) -> str:
    """
    Upload bytes to S3 using an atomic tmp→final pattern.

    Summary
    -------
    - Validates prefix/filename and file extension.
    - Enforces a hard size cap (1 GiB) to mirror read-path expectations.
    - Writes to a temporary key first, then atomically finalizes with CopyObject.
    - Uses single-part for small payloads and multipart for larger ones.
    - Retries transient server/network errors (3x) with exponential backoff + jitter.

    When to use it
    --------------
    Use to publish a single object (≤ 1 GiB) under a known output prefix, ensuring
    readers never observe partial files.

    Parameters
    ----------
    prefix : str
        Destination S3 prefix (must be a valid s3://bucket/path/…). Example:
        "s3://media/work/abc123/RENDER/t1/"
    filename : str
        Final object name (no slashes). Example: "preview.jpg"
    data : bytes
        The full file content to upload.
    content_type : str, optional
        MIME type to store with the object. If omitted, inferred from filename.
    metadata : Dict[str, str], optional
        User metadata to attach. Keys/values must be strings.

    Returns
    -------
    str
        The final object URI, e.g. "s3://media/work/abc123/RENDER/t1/preview.jpg"

    Raises
    ------
    TypeError
        - `data` is not bytes/bytearray.
    ValueError
        - Invalid filename (slashes, traversal, empty).
        - Disallowed file extension.
        - Invalid/unsupported prefix.
        - Object exceeds 1 GiB size limit.
        - Metadata keys/values are not strings.
    FileNotFoundError
        - Bucket does not exist.
    PermissionError
        - Access denied to write to the bucket/key.
    RuntimeError
        - Exhausted retries without success.
    botocore.exceptions.*
        - Other non-transient client/network errors.

    Notes
    -----
    - Atomicity: readers see either nothing or the complete final object.
    - Overwrite behavior: if the final key already exists, it will be overwritten.
      (Add a pre-write HEAD check if you want "fail-if-exists" semantics.)
    """
    MAX_UPLOAD_BYTES = 1 * 1024 * 1024 * 1024  # 1 GiB

    if not isinstance(data, (bytes, bytearray)):
        raise TypeError("put_bytes() expects bytes or bytearray")

    # Enforce filename safety
    if not filename or "/" in filename or "\\" in filename or ".." in filename:
        raise ValueError(f"Invalid filename: {filename}")

    # Parse and normalize prefix
    bucket, base_key = _parse_s3_uri(prefix)
    if base_key and not base_key.endswith("/"):
        base_key = base_key + "/"

    # Extension allowlist
    if not _check_extension(filename, SUPPORTED_FILE_EXTENSIONS):
        raise ValueError(f"File extension not allowed: {filename}")

    # Size cap (aligns with read-path expectations)
    if len(data) > MAX_UPLOAD_BYTES:
        raise ValueError(f"Object exceeds 1 GiB limit ({len(data)} bytes): s3://{bucket}/{base_key}{filename}")

    final_key = f"{base_key}{os.path.basename(filename)}"
    tmp_key = f"{final_key}.tmp-{uuid.uuid4().hex}"

    if content_type is None:
        content_type = guess_content_type(filename)

    # Basic metadata sanity: keys/values must be strings
    meta = metadata or {}
    for k, v in meta.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise ValueError("Metadata keys and values must be strings")

    s3 = _get_s3_client()

    attempts = 3
    transient_codes = {"500", "503", "SlowDown", "RequestTimeout", "InternalError", "ServiceUnavailable"}
    network_excs = (EndpointConnectionError, ReadTimeoutError, ConnectionClosedError, ConnectTimeoutError)

    # Thresholds for multipart (kept conservative for reliability)
    multipart_threshold = 8 * 1024 * 1024      # 8 MiB
    part_size = 16 * 1024 * 1024               # 16 MiB

    def _copy_tmp_to_final() -> None:
        s3.copy_object(
            Bucket=bucket,
            Key=final_key,
            CopySource={"Bucket": bucket, "Key": tmp_key},
            MetadataDirective="COPY",
        )

    def _delete_tmp_silent() -> None:
        try:
            s3.delete_object(Bucket=bucket, Key=tmp_key)
        except Exception:
            pass

    for attempt in range(attempts):
        upload_id = None
        try:
            # 1) Upload to temporary key
            # --- SINGLE-PART UPLOAD: for payloads < multipart_threshold (fast/simple) ---
            if len(data) < multipart_threshold:
                s3.put_object(
                    Bucket=bucket,
                    Key=tmp_key,
                    Body=data,
                    ContentType=content_type,
                    Metadata=meta,
                )
            else:
                # --- MULTIPART UPLOAD: for larger payloads; improves reliability over flaky networks ---
                create_resp = s3.create_multipart_upload(
                    Bucket=bucket,
                    Key=tmp_key,
                    ContentType=content_type,
                    Metadata=meta,
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

            # 2) Atomic finalize (tmp → final)
            _copy_tmp_to_final()

            # 3) Cleanup temporary object
            _delete_tmp_silent()

            return f"s3://{bucket}/{final_key}"

        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")

            # Abort multipart if needed
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

