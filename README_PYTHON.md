# Worker SDK — Python

SQS-driven worker framework for autoscaling ML pipelines.

---

## Install

```bash
pip install worker-sdk
# or from source:
cd python && pip install -e .
```

---

## Configuration

The SDK supports multiple configuration files. Set the `WORKER_CONFIG` environment variable to choose which config to use:

```bash
# Use default configuration
python worker.py  # Uses config/default.yaml

# Use vastra configuration  
export WORKER_CONFIG=vastra
python worker.py  # Uses config/vastra.yaml

# Use production configuration
export WORKER_CONFIG=production
python worker.py  # Uses config/production.yaml
```

**Available configs:**
- `default` → `config/default.yaml`
- `vastra` → `config/vastra.yaml`
- `ytbot` → `config/ytbot.yaml`

---

## Usage

### 1. Implement 3 hooks in your service

Create `service/hooks.py`:

```python
from worker_sdk import hooks

class MyServiceHooks(hooks.WorkerHooks):
    def load_pipeline(self, ctx):
        """Load model once per pod. Called at startup."""
        from transformers import AutoModel
        model = AutoModel.from_pretrained("model-name")
        model.to("cuda")
        return model
    
    def process(self, ctx, msg):
        """Do the work. Called per message."""
        # msg = {"job_id": ..., "task_id": ..., "input_uri": ..., "params": ...}
        input_data = ctx.storage.read(msg["input_uri"])
        
        # Your logic here
        result = self.model.predict(input_data, **msg["params"])
        
        return {
            "result": result,  # SDK writes this to result.*
            "metrics": {"latency_ms": 123, "model": "v1"}
        }
    
    def publish_next(self, ctx, msg, outputs):
        """Fan-out to next queue. Return [] if final step."""
        if not outputs["result"]:
            return []
        
        # Create 1 message per detection (parallelism)
        next_msgs = []
        for i, detection in enumerate(outputs["result"]["detections"]):
            next_msgs.append({
                "job_id": msg["job_id"],
                "task_id": f"{msg['task_id']}-analysis-{i:03d}",  # deterministic
                "input_uri": detection["crop_uri"],
                "output_prefix": f"{msg['output_prefix']}/analysis/{i:03d}/",
                "params": {"detection_id": i}
            })
        return next_msgs
```

### 2. Configure queues

`config/queues.yaml`:
```yaml
input_queue: https://sqs.us-east-1.amazonaws.com/.../detection-queue
next_queue: https://sqs.us-east-1.amazonaws.com/.../analysis-queue
```

Or use env vars:
```bash
export QUEUE_URL=https://sqs.../detection-queue
export NEXT_QUEUE_URL=https://sqs.../analysis-queue
export BUCKET=my-bucket
export DB_DSN=postgresql://user:pass@host/db
```

### 3. Run

```bash
python -m worker_sdk.runner --hooks service.hooks.MyServiceHooks
```

Runner will:
- Long-poll SQS
- Validate messages
- Skip duplicates (idempotency)
- Call your hooks
- Write outputs to S3
- Update DB
- Publish next messages
- ACK

---

## Message Schema

**Input** (SQS message body):
```json
{
  "job_id": "abc123",
  "task_id": "abc123-detection-001",
  "input_uri": "s3://bucket/input.jpg",
  "output_prefix": "s3://bucket/jobs/abc123/detection/",
  "params": {"threshold": 0.5}
}
```

**Output** (written to S3):
- `{output_prefix}/result.json` or `result.png` (your data)
- `{output_prefix}/metrics.json` (latency, version, etc.)

**DB**:
- Task status: PENDING → PROCESSING → DONE/FAILED
- Job status: DONE when all tasks complete

---

## Context Object

Your hooks receive `ctx` with:
- `ctx.config` — merged env + YAML
- `ctx.storage` — `read(uri)`, `write(prefix, filename, data)`
- `ctx.sqs` — `publish(queue, msgs)`
- `ctx.db` — `claim_task(id)`, `update_status(id, status)`
- `ctx.logger` — structured logging

---

## Idempotency

SDK checks if `result.*` exists under `output_prefix/` before processing. If found → skip work, ACK immediately.

Safe for retries.

---

## Local Dev

```bash
# Install
pip install -e .

# Set env
export QUEUE_URL=...
export BUCKET=...
export DB_DSN=...

# Run
python -m worker_sdk.runner --hooks myservice.hooks.MyHooks
```

Send test message to SQS → worker processes it → check S3 + DB.

---

## Deploy

See `/helm/worker-chart/` for K8s deployment + KEDA autoscaling.

```bash
helm install my-service ../helm/worker-chart \
  --set image.repository=my-service \
  --set queues.input=$QUEUE_URL \
  --set resources.limits.nvidia.com/gpu=1
```

KEDA scales 0 → N based on SQS backlog.

---

## Rules

1. **Never write outside `output_prefix/`** (SDK enforces this)
2. **Deterministic task_ids** (same input → same ID)
3. **All 3 hooks required** (runner fails without them)
4. **Use ctx.logger** (consistent traces across services)

---

## Example Pipeline

```
Detection → Analysis → Completion

1. Detection service:
   - Reads image from input_uri
   - Runs YOLO → 12 boxes
   - Writes result.json to output_prefix/
   - Publishes 12 messages to analysis queue

2. Analysis service:
   - Each pod reads 1 detection crop
   - Calls LLM → structured output
   - Writes result.json
   - Publishes 1 message to completion queue

3. Completion service:
   - Reads all 12 analysis results
   - Inpaints → final image
   - Writes result.png
   - publish_next returns [] (done)
```

Each step autoscales independently via KEDA.

---

## Testing

1. **Unit test hooks**: mock `ctx`, call `process()`, assert outputs
2. **Integration test**: send real message → SQS, check S3 + DB
3. **Load test**: see `/tests/load-recipe.md`

---

Done. That's the entire SDK.

