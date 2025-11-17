"""
Task and Job persistence for Postgres.

Responsibilities:
- Manage connection pool
- Provide schema bootstrap (idempotent)
- Typed CRUD/upserts for jobs, tasks, and results metadata
- Status validation guards

Notes:
- Uses psycopg (v3) with connection pooling.
- All queries are parameterized. No string interpolation of user data.
- Status sets are defined here; update if you expand workflow states.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    import psycopg
    from psycopg_pool import ConnectionPool
except Exception:  # pragma: no cover - import guard for environments without psycopg installed
    psycopg = None
    ConnectionPool = object  # type: ignore

# ---------------------------------------------------------------------------
# Status validation
# ---------------------------------------------------------------------------

VALID_JOB_STATUSES = {
    "QUEUED",
    "RUNNING",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    "DLQ",
}

VALID_TASK_STATUSES = {
    "QUEUED",
    "STARTED",
    "RETRIED",
    "SUCCEEDED",
    "FAILED",
    "CANCELLED",
    "DLQ",
}


def validate_job_status(status: str) -> bool:
    return isinstance(status, str) and status in VALID_JOB_STATUSES


def validate_task_status(status: str) -> bool:
    return isinstance(status, str) and status in VALID_TASK_STATUSES


# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

@dataclass
class JobRecord:
    job_id: str
    user_id: str
    schema: str
    status: str
    trace_id: Optional[str]
    attrs: Dict[str, Any]


@dataclass
class TaskRecord:
    task_id: str
    job_id: str
    user_id: str
    step: str
    status: str
    retry_count: int
    parent_task_id: Optional[str]
    input_uri: str
    output_prefix: str
    params: Dict[str, Any]
    error_code: Optional[str] = None
    error_message: Optional[str] = None


# ---------------------------------------------------------------------------
# DB Adapter
# ---------------------------------------------------------------------------

class PostgresDB:
    """
    Postgres adapter with simple connection pool and upsert helpers.
    """

    def __init__(
        self,
        dsn: Optional[str] = None,
        min_size: int = 1,
        max_size: int = 10,
        application_name: str = "worker_sdk_io_db",
    ):
        if psycopg is None:
            raise RuntimeError(
                "psycopg is not installed. Add it to requirements and install before using io_db."
            )
        self._dsn = dsn or self._dsn_from_env(application_name)
        self._pool: ConnectionPool = ConnectionPool(self._dsn, min_size=min_size, max_size=max_size)

    @staticmethod
    def _dsn_from_env(application_name: str) -> str:
        host = os.getenv("PGHOST", "127.0.0.1")
        port = os.getenv("PGPORT", "5432")
        user = os.getenv("PGUSER", "postgres")
        password = os.getenv("PGPASSWORD", "")
        database = os.getenv("PGDATABASE", "postgres")
        options = f"-c application_name={application_name}"
        return f"host={host} port={port} user={user} password={password} dbname={database} options='{options}'"

    # -----------------------------------------------------------------------
    # Schema management (idempotent)
    # -----------------------------------------------------------------------
    def migrate(self) -> None:
        """
        Create tables and indexes if they don't exist.
        """
        ddl = [
            """
            CREATE TABLE IF NOT EXISTS jobs (
                job_id          TEXT PRIMARY KEY,
                user_id         TEXT NOT NULL,
                schema          TEXT NOT NULL,
                status          TEXT NOT NULL,
                trace_id        TEXT NULL,
                attrs           JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_jobs_user_created ON jobs (user_id, created_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at);",
            """
            CREATE TABLE IF NOT EXISTS tasks (
                task_id         TEXT PRIMARY KEY,
                job_id          TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
                user_id         TEXT NOT NULL,
                step            TEXT NOT NULL,
                status          TEXT NOT NULL,
                retry_count     INT  NOT NULL DEFAULT 0,
                parent_task_id  TEXT NULL,
                input_uri       TEXT NOT NULL,
                output_prefix   TEXT NOT NULL,
                params          JSONB NOT NULL DEFAULT '{}'::jsonb,
                error_code      TEXT NULL,
                error_message   TEXT NULL,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                started_at      TIMESTAMPTZ NULL,
                finished_at     TIMESTAMPTZ NULL
            );
            """,
            "CREATE INDEX IF NOT EXISTS idx_tasks_job_step ON tasks (job_id, step);",
            "CREATE INDEX IF NOT EXISTS idx_tasks_user_created ON tasks (user_id, created_at DESC);",
            "CREATE INDEX IF NOT EXISTS idx_tasks_status_created ON tasks (status, created_at);",
            "CREATE INDEX IF NOT EXISTS idx_tasks_parent ON tasks (parent_task_id);",
            "CREATE INDEX IF NOT EXISTS idx_tasks_output_prefix ON tasks (output_prefix);",
            # Migration: Add updated_at column to tasks if it doesn't exist
            """
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='tasks' AND column_name='updated_at'
                ) THEN
                    ALTER TABLE tasks ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
                END IF;
            END $$;
            """,
            """
            CREATE TABLE IF NOT EXISTS task_results (
                task_id             TEXT PRIMARY KEY REFERENCES tasks(task_id) ON DELETE CASCADE,
                primary_result_uri  TEXT NULL,
                metrics_uri         TEXT NULL,
                primary_mime        TEXT NULL,
                primary_size_bytes  BIGINT NULL,
                extra               JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """,
        ]
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                for stmt in ddl:
                    cur.execute(stmt)
            conn.commit()

    # -----------------------------------------------------------------------
    # Job operations
    # -----------------------------------------------------------------------
    def create_or_get_job(
        self,
        *,
        job_id: str,
        user_id: str,
        schema: str,
        status: str = "QUEUED",
        trace_id: Optional[str] = None,
        attrs: Optional[Dict[str, Any]] = None,
    ) -> JobRecord:
        if not validate_job_status(status):
            raise ValueError(f"Invalid job status: {status}")
        attrs = attrs or {}
        sql = """
        INSERT INTO jobs (job_id, user_id, schema, status, trace_id, attrs)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (job_id)
        DO UPDATE SET updated_at = NOW()
        RETURNING job_id, user_id, schema, status, trace_id, COALESCE(attrs, '{}'::jsonb);
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (job_id, user_id, schema, status, trace_id, psycopg.types.json.Json(attrs)))
            row = cur.fetchone()
        return JobRecord(
            job_id=row[0],
            user_id=row[1],
            schema=row[2],
            status=row[3],
            trace_id=row[4],
            attrs=row[5] or {},
        )

    def update_job_status(self, *, job_id: str, status: str) -> None:
        if not validate_job_status(status):
            raise ValueError(f"Invalid job status: {status}")
        sql = "UPDATE jobs SET status=%s, updated_at=NOW() WHERE job_id=%s;"
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (status, job_id))
            conn.commit()

    # -----------------------------------------------------------------------
    # Task operations
    # -----------------------------------------------------------------------
    def upsert_task(
        self,
        *,
        task: TaskRecord,
    ) -> TaskRecord:
        if not validate_task_status(task.status):
            raise ValueError(f"Invalid task status: {task.status}")
        sql = """
        INSERT INTO tasks (
            task_id, job_id, user_id, step, status, retry_count, parent_task_id,
            input_uri, output_prefix, params, error_code, error_message
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (task_id) DO UPDATE SET
            status = EXCLUDED.status,
            retry_count = EXCLUDED.retry_count,
            error_code = EXCLUDED.error_code,
            error_message = EXCLUDED.error_message,
            params = EXCLUDED.params,
            updated_at = NOW()
        RETURNING
            task_id, job_id, user_id, step, status, retry_count, parent_task_id,
            input_uri, output_prefix, params, error_code, error_message;
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    task.task_id,
                    task.job_id,
                    task.user_id,
                    task.step,
                    task.status,
                    int(task.retry_count),
                    task.parent_task_id,
                    task.input_uri,
                    task.output_prefix,
                    psycopg.types.json.Json(task.params or {}),
                    task.error_code,
                    task.error_message,
                ),
            )
            row = cur.fetchone()
        return TaskRecord(
            task_id=row[0],
            job_id=row[1],
            user_id=row[2],
            step=row[3],
            status=row[4],
            retry_count=row[5],
            parent_task_id=row[6],
            input_uri=row[7],
            output_prefix=row[8],
            params=row[9] or {},
            error_code=row[10],
            error_message=row[11],
        )

    def set_task_started(self, *, task_id: str) -> None:
        sql = "UPDATE tasks SET status='STARTED', started_at=NOW(), updated_at=NOW() WHERE task_id=%s;"
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (task_id,))
            conn.commit()

    def set_task_succeeded(self, *, task_id: str) -> None:
        sql = "UPDATE tasks SET status='SUCCEEDED', finished_at=NOW(), updated_at=NOW() WHERE task_id=%s;"
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (task_id,))
            conn.commit()

    def set_task_failed(self, *, task_id: str, error_code: Optional[str], error_message: Optional[str]) -> None:
        sql = """
        UPDATE tasks
        SET status='FAILED', error_code=%s, error_message=%s, finished_at=NOW(), updated_at=NOW()
        WHERE task_id=%s;
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (error_code, error_message, task_id))
            conn.commit()

    def increment_retry(self, *, task_id: str) -> None:
        sql = """
        UPDATE tasks
        SET retry_count = retry_count + 1, status='RETRIED', updated_at=NOW()
        WHERE task_id=%s;
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (task_id,))
            conn.commit()

    # -----------------------------------------------------------------------
    # Results
    # -----------------------------------------------------------------------
    def save_task_result(
        self,
        *,
        task_id: str,
        primary_result_uri: Optional[str],
        metrics_uri: Optional[str],
        primary_mime: Optional[str],
        primary_size_bytes: Optional[int],
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        extra = extra or {}
        sql = """
        INSERT INTO task_results (
            task_id, primary_result_uri, metrics_uri, primary_mime, primary_size_bytes, extra
        )
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (task_id) DO UPDATE SET
            primary_result_uri = EXCLUDED.primary_result_uri,
            metrics_uri = EXCLUDED.metrics_uri,
            primary_mime = EXCLUDED.primary_mime,
            primary_size_bytes = EXCLUDED.primary_size_bytes,
            extra = EXCLUDED.extra;
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    task_id,
                    primary_result_uri,
                    metrics_uri,
                    primary_mime,
                    primary_size_bytes,
                    psycopg.types.json.Json(extra),
                ),
            )
            conn.commit()

    # -----------------------------------------------------------------------
    # Reads
    # -----------------------------------------------------------------------
    def get_job(self, *, job_id: str) -> Optional[JobRecord]:
        sql = "SELECT job_id, user_id, schema, status, trace_id, attrs FROM jobs WHERE job_id=%s;"
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (job_id,))
            row = cur.fetchone()
        if not row:
            return None
        return JobRecord(job_id=row[0], user_id=row[1], schema=row[2], status=row[3], trace_id=row[4], attrs=row[5] or {})

    def list_jobs(self, *, user_id: str, status: Optional[str] = None, limit: int = 100) -> List[JobRecord]:
        if status and not validate_job_status(status):
            raise ValueError(f"Invalid job status: {status}")
        params: Tuple[Any, ...]
        if status:
            sql = """
            SELECT job_id, user_id, schema, status, trace_id, attrs
            FROM jobs
            WHERE user_id=%s AND status=%s
            ORDER BY created_at DESC
            LIMIT %s;
            """
            params = (user_id, status, int(limit))
        else:
            sql = """
            SELECT job_id, user_id, schema, status, trace_id, attrs
            FROM jobs
            WHERE user_id=%s
            ORDER BY created_at DESC
            LIMIT %s;
            """
            params = (user_id, int(limit))
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        return [
            JobRecord(job_id=r[0], user_id=r[1], schema=r[2], status=r[3], trace_id=r[4], attrs=r[5] or {}) for r in rows
        ]

    def get_task(self, *, task_id: str) -> Optional[TaskRecord]:
        sql = """
        SELECT task_id, job_id, user_id, step, status, retry_count, parent_task_id,
               input_uri, output_prefix, params, error_code, error_message
        FROM tasks
        WHERE task_id=%s;
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (task_id,))
            row = cur.fetchone()
        if not row:
            return None
        return TaskRecord(
            task_id=row[0],
            job_id=row[1],
            user_id=row[2],
            step=row[3],
            status=row[4],
            retry_count=row[5],
            parent_task_id=row[6],
            input_uri=row[7],
            output_prefix=row[8],
            params=row[9] or {},
            error_code=row[10],
            error_message=row[11],
        )

    def list_tasks(
        self,
        *,
        job_id: str,
        step: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[TaskRecord]:
        params: List[Any] = [job_id]
        wheres = ["job_id=%s"]
        if step:
            wheres.append("step=%s")
            params.append(step)
        if status:
            if not validate_task_status(status):
                raise ValueError(f"Invalid task status: {status}")
            wheres.append("status=%s")
            params.append(status)
        params.append(int(limit))
        where_sql = " AND ".join(wheres)
        sql = f"""
        SELECT task_id, job_id, user_id, step, status, retry_count, parent_task_id,
               input_uri, output_prefix, params, error_code, error_message
        FROM tasks
        WHERE {where_sql}
        ORDER BY created_at DESC
        LIMIT %s;
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
        out: List[TaskRecord] = []
        for r in rows:
            out.append(
                TaskRecord(
                    task_id=r[0],
                    job_id=r[1],
                    user_id=r[2],
                    step=r[3],
                    status=r[4],
                    retry_count=r[5],
                    parent_task_id=r[6],
                    input_uri=r[7],
                    output_prefix=r[8],
                    params=r[9] or {},
                    error_code=r[10],
                    error_message=r[11],
                )
            )
        return out

    def list_children(self, *, parent_task_id: str) -> List[TaskRecord]:
        sql = """
        SELECT task_id, job_id, user_id, step, status, retry_count, parent_task_id,
               input_uri, output_prefix, params, error_code, error_message
        FROM tasks
        WHERE parent_task_id=%s
        ORDER BY created_at ASC;
        """
        with self._pool.connection() as conn, conn.cursor() as cur:
            cur.execute(sql, (parent_task_id,))
            rows = cur.fetchall()
        children: List[TaskRecord] = []
        for r in rows:
            children.append(
                TaskRecord(
                    task_id=r[0],
                    job_id=r[1],
                    user_id=r[2],
                    step=r[3],
                    status=r[4],
                    retry_count=r[5],
                    parent_task_id=r[6],
                    input_uri=r[7],
                    output_prefix=r[8],
                    params=r[9] or {},
                    error_code=r[10],
                    error_message=r[11],
                )
            )
        return children

    # -----------------------------------------------------------------------
    # Pool lifecycle
    # -----------------------------------------------------------------------
    def close(self) -> None:
        try:
            self._pool.close()
        except Exception:
            pass


__all__ = [
    "VALID_JOB_STATUSES",
    "VALID_TASK_STATUSES",
    "validate_job_status",
    "validate_task_status",
    "JobRecord",
    "TaskRecord",
    "PostgresDB",
]

"""
Task DB: atomic claim/lease, set status, increment counters.

TODO: Implement status validators here:
- validate_task_status(status: str) -> bool
  Check against VALID_TASK_STATUSES from contracts before writing to DB
  
- validate_job_status(status: str) -> bool
  Check against VALID_JOB_STATUSES from contracts before writing to DB
"""

