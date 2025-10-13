# Worker SDK

**Purpose**: Run parallel ML pipelines where each step autoscales based on SQS backlog.

## What Goes In
SQS message:
```json
{
  "job_id": "abc123",
  "task_id": "abc123-det-001",
  "input_uri": "s3://bucket/input.jpg",
  "output_prefix": "s3://bucket/jobs/abc123/detection/",
  "params": {"model": "yolo", "threshold": 0.5}
}
```

## What Happens
1. **Runner** long-polls your queue (from config)
2. Validates message, checks idempotency, claims task in DB
3. Calls **your 3 hooks**:
   - `load_pipeline(ctx)` — load model once per pod
   - `process(ctx, msg)` — do the work (detection/analysis/inpaint)
   - `publish_next(ctx, msg, outputs)` — fan-out to next queue (or skip if final step)
4. Writes `result.*` + `metrics.json` to `output_prefix/`
5. Updates DB (DONE), ACKs SQS, next service scales up automatically

## What Comes Out
- **Storage**: `s3://bucket/jobs/abc123/detection/result.json` + `metrics.json`
- **DB**: task status = DONE, job status = DONE (when all tasks finish)
- **Next Queue**: messages for next step (e.g., 12 detections → 12 analysis tasks)

## Your Job (Per Service)
Implement 3 functions in `hooks.py`:

```python
def load_pipeline(ctx):
    # Load model into VRAM once
    return model

def process(ctx, msg):
    # Run inference on msg["input_uri"]
    # Return result data (bytes or dict)
    return {"detections": [...], "metrics": {...}}

def publish_next(ctx, msg, outputs):
    # Return list of next messages (or [] if final step)
    return [{"task_id": "...", "input_uri": "...", ...}, ...]
```

**That's it**. The SDK handles SQS, S3, DB, idempotency, autoscaling, retries.

---

## Repository Structure

```
/python/worker_sdk/       # Core SDK (don't edit in services)
  __init__.py             # Package entry
  runner.py               # Main loop: SQS → validate → hooks → output → DB → ACK
  hooks.py                # Interface you implement: load_pipeline, process, publish_next
  contracts.py            # Message schema validation
  config.py               # Loads env + YAML (queues, models, etc.)
  io_storage.py           # S3/R2 read/write (enforces output_prefix rule)
  io_sqs.py               # SQS client: receive, publish, extend visibility
  io_db.py                # Task DB: atomic claim, status updates
  util_idempotency.py     # Skip duplicate work if result.* exists
  util_timeouts.py        # Heartbeat lease, extend SQS visibility
  util_logging.py         # Structured JSON logs

/python/pyproject.toml    # Pip package metadata
/python/README.md         # Install + usage guide

/helm/worker-chart/       # K8s deployment template
  Chart.yaml              # Helm descriptor
  values.yaml             # Defaults (replicas, resources, queues)
  templates/
    deployment.yaml       # Supports CPU/GPU, volumes
    scaledobject.yaml     # KEDA: scales on SQS backlog (1 msg = 1 replica)
    serviceaccount.yaml   # RBAC
    role.yaml             # Least-privilege permissions
    rolebinding.yaml      # Bind role to service account
    configmap.yaml        # Mounts config/*.yaml
    secret-env.yaml       # Injects AWS/DB/LLM secrets
    probes.yaml           # Readiness + liveness checks

/observability/
  dashboards.json         # Grafana: backlog, latency, GPU, DLQ
  alerts.yaml             # Alerts: stuck backlog, DLQ > 0, OOM, crash loops

/tests/
  contract-checklist.md   # Verify your service obeys the SDK contract
  load-recipe.md          # How to test autoscaling (100–500 msgs)
```

---

## How Runner Knows What to Do

**Queue name**: from `config.py` (env `QUEUE_URL` or `config/queues.yaml`)

**Per message**:
1. Long-poll input queue
2. Validate schema (contracts.py)
3. Idempotency check (util_idempotency.py)
4. Claim task in DB (io_db.py)
5. Call `load_pipeline` (once per pod)
6. Call `process` (your logic)
7. Call `publish_next` (fan-out if needed)
8. Write `result.*` + `metrics.json` (io_storage.py)
9. Update DB → DONE
10. Publish to next queue (io_sqs.py)
11. ACK message

**You never edit runner.py**. You only provide config + hooks.

---

## Hooks: What You Implement

Every service uses these 3 functions:

### 1. `load_pipeline(ctx)` — warm up once
- **GPU step**: load diffusion model → VRAM
- **CPU step**: maybe no-op
- **Runs**: once per pod at startup
- **Returns**: model object (cached for all messages)

### 2. `process(ctx, msg)` — do the work
- **Detection**: run detector → return boxes
- **Analysis**: crop + call LLM → return structured JSON
- **Completion**: inpaint/enhance → return final image bytes
- **Returns**: `{"result": <data>, "metrics": {...}}`
- SDK writes `result.*` + `metrics.json` to `output_prefix/`

### 3. `publish_next(ctx, msg, outputs)` — fan-out
- **Purpose**: create messages for next service
- **Parallelism**: 1 detection → 12 crops → 12 analysis tasks (12 messages)
- **Deterministic IDs**: same input → same `task_id` (safe retries)
- **Final step**: return `[]` (nothing to enqueue)

---

## Autoscaling Flow

```
1. You push 100 messages → SQS input queue
2. KEDA sees backlog = 100, target = 1 → scales to 100 replicas
3. Each pod: long-poll → process 1 message → ACK → scale down
4. After cooldown (120s), scales to 0 if queue empty
```

**Next step starts automatically**: when you `publish_next`, the next service's queue fills → its KEDA scales up.

---

## Local Dev

```bash
# Install SDK
cd python && pip install -e .

# Set env
export QUEUE_URL=https://sqs.us-east-1.amazonaws.com/.../my-queue
export NEXT_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/.../next-queue
export BUCKET=my-bucket
export DB_DSN=postgresql://...

# Implement hooks.py in your service
# Run worker
python -m worker_sdk.runner
```

---

## Deploy

```bash
# Build service image
docker build -t my-service:v1 .

# Deploy with Helm (override values)
helm install my-service helm/worker-chart \
  --set image.repository=my-service \
  --set image.tag=v1 \
  --set queues.input=https://sqs.../detection-queue \
  --set queues.next=https://sqs.../analysis-queue \
  --set resources.limits.nvidia.com/gpu=1
```

KEDA scales automatically. Done.

---

## Observability

- **Logs**: structured JSON (job_id, task_id, latency_ms, gpu_mem_gb)
- **Dashboards**: Grafana panels for backlog, p95, GPU util
- **Alerts**: backlog stuck, DLQ > 0, OOM, crash loops

Import `/observability/*` into your stack.

---

## Testing

1. **Contract check**: `/tests/contract-checklist.md` — verify message schema, outputs, idempotency, DB updates
2. **Load test**: `/tests/load-recipe.md` — inject 100 messages, watch KEDA scale 0 → 100 → 0

---

## Rules

1. **Don't edit runner.py** in services
2. **Write only under `output_prefix/`** (enforced by io_storage.py)
3. **Implement all 3 hooks** (SDK won't run without them)
4. **Deterministic task_ids** (same input → same ID for safe retries)
5. **Use SDK logging** (util_logging.py) for consistent traces

---

## Questions?

Read `/python/README.md` for install + usage details.  
Check `/tests/contract-checklist.md` to validate your service.  
Load-test with `/tests/load-recipe.md`.

