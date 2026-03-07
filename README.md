# FSU - FastScan Unified

High-performance S3 event processor that consumes event notifications from AWS SQS, filters events based on configured audit points, and stores them in PostgreSQL for analysis and traceability.

## 📋 Overview

FSU is an asynchronous Python service that:

- ✅ **Consumes** S3 events from AWS SQS (long polling)
- 🎯 **Filters** events according to audit points (bucket + prefix)
- 💾 **Stores** relevant events in PostgreSQL with array support
- 🔄 **Reloads** audit points automatically in real-time (PostgreSQL LISTEN/NOTIFY)
- 📊 **Tracks** processing statistics in real-time
- 🧪 **Load testing** via `tools/sqs_ingest.py` (inject events into SQS, then run the processor)

### Architecture

```
┌─────────────┐
│   AWS SQS   │  Messages with S3 events
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────┐
│          SQS Consumer (aioboto3)            │
│  - Long polling (20s default)               │
│  - Batch processing (10 messages)           │
└──────┬──────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────┐
│              Event Processor                │
│  - Parses SQS messages                      │
│  - Extracts S3 events                       │
└──────┬──────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────┐
│               Audit Matcher                 │
│  - Checks if bucket/prefix matches          │
│  - Supports hierarchical prefixes           │
│  - Returns all matching audit_point_ids     │
└──────┬──────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────┐
│          Database Manager (asyncpg)         │
│  - Batch insert via multi-row INSERT        │
│  - Connection pool                          │
│  - PostgreSQL array support                 │
└──────┬──────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────┐
│               PostgreSQL 17                 │
│  - s3_events table                          │
│  - audit_points table                       │
│  - GIN index on audit_point_ids[]           │
│  - NOTIFY trigger on changes                │
└─────────────────────────────────────────────┘
```

## 🚀 Quick Start

### Prerequisites

- **Python 3.12+**
- **Docker & Docker Compose** (for PostgreSQL)
- **AWS Account** with SQS access (or IAM role)

### Installation

1. **Clone the project** (or extract files)

```bash
cd fsudaemon
```

2. **Create Python virtual environment**

```bash
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or
.\.venv\Scripts\activate  # Windows
```

3. **Install dependencies**

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

4. **Configure environment**

Copy `.env.example` to `.env` and configure variables:

```bash
cp .env.example .env
```

Edit `.env` with your settings:

```ini
# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/000000000000/your_queue_name_here

# PostgreSQL Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=fsu
DB_USER=postgres
DB_PASSWORD=postgres
DB_POOL_MIN_SIZE=5
DB_POOL_MAX_SIZE=20

# Processing Configuration
SQS_BATCH_SIZE=10  # Maximum 10 (AWS SQS limitation)
SQS_WAIT_TIME_SECONDS=20
SQS_VISIBILITY_TIMEOUT=30

# Logging Configuration
LOG_LEVEL=INFO
LOG_FORMAT=json
```

5. **Start PostgreSQL with Docker Compose**

```bash
docker compose up -d
```

6. **Initialize database schema**

```bash
python tools/init_db.py
```

7. **Run the processor**

```bash
python main.py
```

## 🎯 Key Features

### Hierarchical Prefix Matching

An object `/data/2024/dir1/file.txt` will be associated with **all** matching audit points:

- Audit point 1: `bucket=prod-bucket`, `prefix=/data`
- Audit point 2: `bucket=prod-bucket`, `prefix=/data/2024`
- Audit point 3: `bucket=prod-bucket`, `prefix=/data/2024/dir1`

→ The event will be stored with `audit_point_ids = [1, 2, 3]`

This allows you to:

- Query events at different granularity levels
- Avoid database duplicates
- Track the complete monitoring hierarchy

### Dynamic Audit Point Reloading

The system listens to PostgreSQL notifications (LISTEN/NOTIFY) and automatically reloads audit points when modified:

```sql
-- Add a new audit point
INSERT INTO audit_points (bucket, prefix, description)
VALUES ('new-bucket', 'data/', 'New audit point');
-- → Processor reloads automatically!

-- Soft delete an audit point
UPDATE audit_points SET deleted_at = NOW() WHERE id = 5;
-- → Processor reloads automatically!
```

No restart needed! The PostgreSQL trigger `audit_points_change_trigger` automatically notifies the application.

### Optimized Performance

- **asyncpg**: Ultra-fast asynchronous PostgreSQL library
- **Multi-row INSERT**: Optimized batch insertion (much faster than individual INSERTs). `COPY FROM` would be even faster but does not natively support `ON CONFLICT`.
- **Connection pool**: Reuse PostgreSQL connections
- **Long polling SQS**: Reduces AWS API calls
- **GIN index**: Fast queries on PostgreSQL arrays

### Resilience

- **Graceful shutdown**: Handles SIGINT/SIGTERM signals (CTRL+C). Running workers complete their operations, PostgreSQL connections are closed properly, and final statistics are displayed.
- **Error handling**: Exponential backoff after consecutive errors
- **SQS visibility**: Messages reappear if processing fails
- **Health checks**: PostgreSQL connection verification at startup

## 📊 Database Structure

### Table `audit_points`

Audit points defining the S3 buckets and prefixes to monitor.

```sql
CREATE TABLE audit_points (
    id SERIAL PRIMARY KEY,
    bucket VARCHAR(255) NOT NULL,
    prefix VARCHAR(1024) NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    deleted_at TIMESTAMPTZ  -- Soft delete
);
```

**Example data**:

```sql
INSERT INTO audit_points (bucket, prefix, description)
VALUES 
    ('prod-bucket', 'data/', 'Production data files'),
    ('prod-bucket', 'data/2024/', '2024 data subset'),
    ('test-bucket-1', 'logs/', 'Application logs');
```

### Table `s3_events`

S3 events stored with their audit point associations.

```sql
CREATE TABLE s3_events (
    id BIGSERIAL PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    bucket VARCHAR(255) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    size BIGINT DEFAULT 0,
    version_id VARCHAR(255),
    audit_point_ids INTEGER[] NOT NULL,  -- Array of audit point IDs
    received_at TIMESTAMPTZ DEFAULT NOW()
);
```

**Performance indexes**:

- GIN index on `audit_point_ids[]` for fast array queries
- B-tree indexes on `event_time`, `bucket`, `object_key`, `event_name`, `received_at`
- UNIQUE index on `(bucket, object_key, event_time, event_name)` for idempotence / SQS replay without duplicates

### Example Queries

```sql
-- All events for a specific audit point
SELECT * FROM s3_events 
WHERE audit_point_ids @> ARRAY[1]  -- Operator @> = contains
ORDER BY event_time DESC;

-- Statistics per audit point
SELECT 
    COUNT(*) as total_events,
    COUNT(DISTINCT object_key) as unique_objects,
    SUM(size) as total_size_bytes,
    SUM(CASE WHEN event_name LIKE 'ObjectCreated:%' THEN 1 ELSE 0 END) as creates,
    SUM(CASE WHEN event_name LIKE 'ObjectRemoved:%' THEN 1 ELSE 0 END) as deletes
FROM s3_events
WHERE audit_point_ids @> ARRAY[1];
```

## 🧪 Load testing with simulated events

To test the system without production SQS, use the **`tools/sqs_ingest.py`** script. It generates S3 events and sends them to the queue configured in `.env`. Then run `main.py` as usual to consume and process them.

Example: send 1000 messages (10,000 events) for bucket `my-bucket`, then start the processor:

```bash
python tools/sqs_ingest.py --bucket my-bucket --messages 1000
python main.py
```

### LocalStack (mock SQS local)

For a fully local setup (no AWS), use **LocalStack** from the `tools/` directory:

1. **Start LocalStack** with the docker compose in `tools/`:

   ```bash
   cd tools
   docker compose up -d
   ```

2. **Create the SQS queue** (after LocalStack is up). Example with queue name `019cb84a-45e5-786d-9d74-1d5086bbf1b3`:

   ```bash
   curl -X POST "http://localhost:4566/" -H "Content-Type: application/x-www-form-urlencoded" -d "Action=CreateQueue&QueueName=019cb84a-45e5-786d-9d74-1d5086bbf1b3&Version=2012-11-05"
   ```

   Then set in your `.env`:
   - `SQS_QUEUE_URL=http://localhost:4566/000000000000/019cb84a-45e5-786d-9d74-1d5086bbf1b3` (or your LocalStack queue URL, e.g. `http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/...`)
   - `AWS_ENDPOINT_URL=http://localhost:4566` (so the SDK targets LocalStack instead of AWS; omit for real AWS)

3. Run `sqs_ingest.py` to fill the queue, then `main.py` to consume. Ensure audit points exist in the database for the buckets/prefixes used by the generated events.

## 🏗️ Code Architecture

### Main Modules


| File                 | Description                                        |
| -------------------- | -------------------------------------------------- |
| `main.py`            | Entry point, orchestrates async workers            |
| `config.py`          | Centralized configuration with Pydantic Settings   |
| `sqs_consumer.py`    | Async SQS consumer with aioboto3                   |
| `event_processor.py` | Event processing orchestration                     |
| `audit_matcher.py`   | Fast event matching against audit points           |
| `db_manager.py`      | Async PostgreSQL management with asyncpg           |
| `models.py`          | Data models (AuditPoint, S3Event)                  |
| `telemetry.py`       | OpenTelemetry metrics (export via structlog)        |


### Tools (`tools/`)


| File                  | Description                                                   |
| --------------------- | ------------------------------------------------------------- |
| `tools/init_db.py`    | Database schema initialization script                         |
| `tools/changelist.py` | Fetch all events between two audit points (keyset pagination) |
| `tools/sqs_ingest.py` | Manual SQS message ingestion utility                          |
| `tools/db_inject.py`  | Direct DB insertion for benchmarks (no SQS)                   |


### Async Workers

The processor runs 2 workers in parallel:

1. **`process_messages_loop`**: Consumes and processes SQS messages
2. **`listen_audit_points_changes`**: Listens to PostgreSQL notifications to reload audit points

OpenTelemetry metrics are exported to stdout every 60 seconds via the `StructlogMetricExporter`.

## 📈 Statistics and Monitoring

### Structured Logs

The system uses `structlog` for easily parseable structured JSON logs:

```json
{
  "event": "message_processed",
  "events_kept": 8,
  "events_discarded": 2,
  "associations": 15,
  "timestamp": "2026-02-20T10:30:45.123456Z",
  "logger": "event_processor"
}
```

### Periodic Statistics

Every 60 seconds, the system displays:

```json
{
  "event": "processing_stats",
  "messages_received": 1234,
  "messages_processed": 1230,
  "events_kept": 9876,
  "events_discarded": 543,
  "total_associations": 15432,
  "errors": 4
}
```

## ⚙️ Configuration Parameters

### SQS Processing Configuration


| Parameter                | Valid Range | Description                                                                               |
| ------------------------ | ----------- | ----------------------------------------------------------------------------------------- |
| `SQS_BATCH_SIZE`         | **1-10**    | Maximum number of messages to receive per batch. **AWS SQS limitation: cannot exceed 10** |
| `SQS_WAIT_TIME_SECONDS`  | 0-20        | Long polling wait time. Recommended: 20 for reduced API calls                             |
| `SQS_VISIBILITY_TIMEOUT` | 0-43200     | Time a message is hidden after being received (seconds)                                   |


### PostgreSQL Pool Configuration


| Parameter          | Recommended | Description                 |
| ------------------ | ----------- | --------------------------- |
| `DB_POOL_MIN_SIZE` | 5-10        | Minimum connections in pool |
| `DB_POOL_MAX_SIZE` | 20-50       | Maximum connections in pool |


⚠️ **Important**: `SQS_BATCH_SIZE` is limited to **10 messages maximum** by AWS SQS API. Setting a higher value will be ignored or cause errors. This is a hard limit imposed by Amazon SQS `MaxNumberOfMessages` parameter.

## 🛠️ Tools (`tools/`)

### `tools/changelist.py` — Fetch events between two audit points

Iterates over all S3 events belonging to an audit point and received before another audit point was created, using **keyset pagination** on the primary key (1024 events per batch).

```bash
python tools/changelist.py --audit-point-id 1 --audit-point-end 2
```

Add your processing logic inside the `for row in rows` loop (write to file, transform, send to API, etc.).

**Why keyset pagination?** Unlike `OFFSET/LIMIT` which rescans all preceding rows on each page, `AND id > last_id` jumps directly to the right position via the primary key index — performance stays constant regardless of how deep into the result set you are.

### `tools/init_db.py` — Database Schema Initialization

Creates the `audit_points` and `s3_events` tables, indexes, and the PostgreSQL NOTIFY trigger.

```bash
python tools/init_db.py
```

### `tools/sqs_ingest.py` — SQS Load Testing

Generates S3 events and injects them into the configured SQS queue.

```bash
python tools/sqs_ingest.py --bucket my-bucket --messages 1000
```

### `tools/db_inject.py` — Direct DB Benchmark

Inserts S3 events directly into the database, bypassing SQS. Useful for isolating DB insertion performance.

```bash
python tools/db_inject.py --bucket my-bucket --events 10000
python tools/db_inject.py --bucket my-bucket --events 50000 --batch-size 500
```

## ❓ FAQ

### How do I connect to the PostgreSQL database running in Docker?

```bash
docker exec -it fsu-postgres psql -U postgres -d fsu
```

Once connected, useful commands:

```sql
-- List all tables
\dt

-- Show active audit points
SELECT * FROM audit_points WHERE deleted_at IS NULL;

-- Count stored events
SELECT COUNT(*) FROM s3_events;

-- Show the 10 most recent events
SELECT id, event_time, event_name, bucket, object_key, audit_point_ids
FROM s3_events
ORDER BY received_at DESC
LIMIT 10;

-- Exit psql
\q
```

### How do I check that the Docker container is running?

```bash
docker compose ps
docker compose logs -f postgres
```

### How do I install Docker on Linux (RHEL/CentOS/Rocky/AlmaLinux)?

```bash
dnf install -y dnf-plugins-core
dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
dnf install docker-ce docker-ce-cli containerd.io docker-compose-plugin
systemctl start docker
systemctl enable docker

docker run hello-world
```

> `docker-compose-plugin` provides the `docker compose` (V2) command used throughout this documentation.

### How do I start, stop, or reset PostgreSQL?

```bash
# Start PostgreSQL in background
docker compose up -d

# Stop PostgreSQL (data preserved)
docker compose down

# View live logs
docker compose logs -f postgres

# Stop and delete all data (full reset)
docker compose down -v
```

### How do I deploy FSU as a Docker container?

Create a `Dockerfile` at the root of the project:

```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
```

Then add the service to `docker-compose.yml`:

```yaml
services:
  fsu:
    build: .
    container_name: fsu-app
    restart: unless-stopped
    env_file: .env
    depends_on:
      postgres:
        condition: service_healthy
    networks:
      - fsu-network

  postgres:
    # ... (existing configuration)
```

```bash
# Build and start everything
docker compose up -d --build
```

### How do I run multiple instances for multiple SQS queues?

Each instance only differs by `SQS_QUEUE_URL`. All instances share the same database and are automatically notified of audit point changes via PostgreSQL LISTEN/NOTIFY:

```bash
SQS_QUEUE_URL=https://sqs.../queue-A python main.py &
SQS_QUEUE_URL=https://sqs.../queue-B python main.py &
SQS_QUEUE_URL=https://sqs.../queue-C python main.py &
```

### Why are no events being stored when using sqs_ingest or test data?

Generated events must match at least one active audit point (bucket + prefix). Check that audit points exist and match the buckets/prefixes used in your test messages:

```sql
SELECT * FROM audit_points WHERE deleted_at IS NULL;
```

### How do I add a new audit point without restarting FSU?

Simply insert a row in the `audit_points` table. The PostgreSQL trigger automatically notifies all running FSU instances, which reload their in-memory matcher within seconds — no restart needed:

```sql
INSERT INTO audit_points (bucket, prefix, description)
VALUES ('my-bucket', 'data/', 'My new audit point');
```

---

**Built with ❤️ by the Atempo team**
