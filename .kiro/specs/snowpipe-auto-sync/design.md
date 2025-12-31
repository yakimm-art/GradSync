# Design Document: Snowpipe Auto-Sync

## Overview

This design document describes the architecture and implementation for Snowpipe auto-sync functionality in GradSync. The system enables continuous, automatic data ingestion from external district systems (SIS, LMS) into Snowflake, eliminating manual data uploads for IT departments.

The solution leverages Snowflake's native Snowpipe, Streams, and Tasks features to create a serverless, event-driven data pipeline that processes attendance, grades, and student data in near real-time.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        EXTERNAL CLOUD STORAGE                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                      │
│  │ /attendance │  │  /grades    │  │  /students  │                      │
│  │   *.json    │  │   *.json    │  │   *.json    │                      │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                      │
└─────────┼────────────────┼────────────────┼─────────────────────────────┘
          │                │                │
          │  Cloud Event Notifications (SQS/Event Grid/Pub-Sub)
          ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         SNOWFLAKE PLATFORM                              │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    SNOWPIPE (AUTO_INGEST=TRUE)                   │   │
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐        │   │
│  │  │ ATTENDANCE_PIPE│ │  GRADES_PIPE   │ │ STUDENTS_PIPE  │        │   │
│  │  └───────┬────────┘ └───────┬────────┘ └───────┬────────┘        │   │
│  └──────────┼──────────────────┼──────────────────┼─────────────────┘   │
│             ▼                  ▼                  ▼                     │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                      LANDING TABLES (RAW_DATA)                   │   │
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐        │   │
│  │  │ ATTENDANCE_    │ │ GRADE_EVENTS_  │ │ STUDENT_EVENTS_│        │   │
│  │  │ EVENTS_LANDING │ │ LANDING        │ │ LANDING        │        │   │
│  │  └───────┬────────┘ └───────┬────────┘ └───────┬────────┘        │   │
│  └──────────┼──────────────────┼──────────────────┼─────────────────┘   │
│             │                  │                  │                     │
│             ▼                  ▼                  ▼                     │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                         STREAMS (CDC)                            │   │
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐        │   │
│  │  │ ATTENDANCE_    │ │ GRADES_        │ │ STUDENTS_      │        │   │
│  │  │ EVENTS_STREAM  │ │ EVENTS_STREAM  │ │ EVENTS_STREAM  │        │   │
│  │  └───────┬────────┘ └───────┬────────┘ └───────┬────────┘        │   │
│  └──────────┼──────────────────┼──────────────────┼─────────────────┘   │
│             │                  │                  │                     │
│             ▼                  ▼                  ▼                     │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    TASKS (SCHEDULED PROCESSING)                  │   │
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐        │   │
│  │  │ PROCESS_       │ │ PROCESS_       │ │ PROCESS_       │        │   │
│  │  │ ATTENDANCE     │ │ GRADES         │ │ STUDENTS       │        │   │
│  │  └───────┬────────┘ └───────┬────────┘ └───────┬────────┘        │   │
│  └──────────┼──────────────────┼──────────────────┼─────────────────┘   │
│             ▼                  ▼                  ▼                     │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                   NORMALIZED TABLES (RAW_DATA)                   │   │
│  │  ┌────────────────┐ ┌────────────────┐ ┌────────────────┐        │   │
│  │  │   ATTENDANCE   │ │     GRADES     │ │    STUDENTS    │        │   │
│  │  └────────────────┘ └────────────────┘ └────────────────┘        │   │
│  └──────────────────────────────────────────────────────────────────┘   │
│                                │                                        │
│                                ▼                                        │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    DYNAMIC TABLES (ANALYTICS)                    │   │
│  │                      STUDENT_360_VIEW                            │   │
│  └──────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

## Components and Interfaces

### 1. Storage Integration Component

Manages secure connections to external cloud storage providers.

```sql
-- Interface: Storage Integration
CREATE STORAGE INTEGRATION <integration_name>
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = '<S3|AZURE|GCS>'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('<cloud_url>');
```

| Provider | URL Format | Authentication |
|----------|------------|----------------|
| AWS S3 | `s3://bucket/path/` | IAM Role ARN |
| Azure Blob | `azure://account.blob.core.windows.net/container/` | Tenant ID |
| GCS | `gcs://bucket/path/` | Service Account |

### 2. External Stage Component

Points to the cloud storage location where district systems deposit files.

```sql
-- Interface: External Stage
CREATE STAGE <stage_name>
    URL = '<cloud_url>'
    STORAGE_INTEGRATION = <integration_name>
    FILE_FORMAT = <format_name>
    DIRECTORY = (ENABLE = TRUE);
```

### 3. Snowpipe Component

Serverless ingestion service that automatically loads files when they arrive.

```sql
-- Interface: Snowpipe
CREATE PIPE <pipe_name>
    AUTO_INGEST = TRUE
AS
COPY INTO <landing_table>
FROM @<stage>/<path>/
FILE_FORMAT = <format>;
```

### 4. Stream Component

Captures change data from landing tables for downstream processing.

```sql
-- Interface: Stream
CREATE STREAM <stream_name>
    ON TABLE <landing_table>;
```

### 5. Task Component

Scheduled SQL execution that processes stream data into normalized tables.

```sql
-- Interface: Task
CREATE TASK <task_name>
    WAREHOUSE = <warehouse>
    SCHEDULE = '<cron_or_interval>'
    WHEN SYSTEM$STREAM_HAS_DATA('<stream_name>')
AS
<sql_statement>;
```

### 6. Monitoring Component

Views and functions for observing pipeline health.

| Function | Purpose |
|----------|---------|
| `SYSTEM$PIPE_STATUS('<pipe>')` | Get current pipe state |
| `COPY_HISTORY()` | View ingestion history |
| `TASK_HISTORY()` | View task execution history |

## Data Models

### Landing Table Schema: Attendance Events

```sql
CREATE TABLE RAW_DATA.ATTENDANCE_EVENTS_LANDING (
    event_id VARCHAR(50),           -- Unique event identifier
    student_id VARCHAR(20),         -- Student identifier
    event_timestamp TIMESTAMP,      -- When the event occurred
    event_type VARCHAR(20),         -- check_in, check_out_early, no_show, late_arrival
    location VARCHAR(100),          -- School/classroom location
    raw_payload VARIANT,            -- Original JSON for audit
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### Landing Table Schema: Grade Events

```sql
CREATE TABLE RAW_DATA.GRADE_EVENTS_LANDING (
    event_id VARCHAR(50),
    student_id VARCHAR(20),
    course_name VARCHAR(100),
    assignment_name VARCHAR(200),
    score DECIMAL(5,2),
    max_score DECIMAL(5,2),
    grade_date DATE,
    raw_payload VARIANT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### Landing Table Schema: Student Events

```sql
CREATE TABLE RAW_DATA.STUDENT_EVENTS_LANDING (
    event_id VARCHAR(50),
    student_id VARCHAR(20),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    grade_level INT,
    parent_email VARCHAR(100),
    parent_language VARCHAR(20),
    event_type VARCHAR(20),         -- create, update, transfer
    raw_payload VARIANT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### JSON Input Format: Attendance

```json
{
    "event_id": "ATT-2024-001",
    "student_id": "STU001",
    "timestamp": "2024-12-30T08:15:00Z",
    "type": "check_in",
    "location": "Main Entrance"
}
```

### JSON Input Format: Grades

```json
{
    "event_id": "GRD-2024-001",
    "student_id": "STU001",
    "course": "Algebra I",
    "assignment": "Chapter 5 Quiz",
    "score": 85.5,
    "max_score": 100,
    "date": "2024-12-30"
}
```

### JSON Input Format: Students

```json
{
    "event_id": "STU-2024-001",
    "student_id": "STU001",
    "first_name": "John",
    "last_name": "Doe",
    "grade_level": 9,
    "parent_email": "parent@example.com",
    "parent_language": "Spanish",
    "event_type": "create"
}
```

### Event Type Mapping

| Event Type | Attendance Status |
|------------|-------------------|
| check_in | Present |
| check_out_early | Present |
| no_show | Absent |
| late_arrival | Tardy |



## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

### Property 1: Raw Payload Preservation (Round-Trip)

*For any* valid JSON event ingested through Snowpipe, the `raw_payload` VARIANT column SHALL contain the complete original JSON document, allowing reconstruction of the source data.

**Validates: Requirements 2.4**

### Property 2: Event Type to Status Mapping

*For any* attendance event with a valid event_type, the processing task SHALL produce the correct attendance status according to the mapping: check_in→Present, check_out_early→Present, no_show→Absent, late_arrival→Tardy.

**Validates: Requirements 3.3**

### Property 3: Processing Idempotency

*For any* record in the landing table, processing it multiple times (via task re-execution) SHALL NOT create duplicate records in the normalized table.

**Validates: Requirements 3.4**

### Property 4: Malformed JSON Rejection

*For any* file containing malformed JSON, the system SHALL reject the invalid records while successfully processing any valid JSON records in the same batch.

**Validates: Requirements 2.5**

## Error Handling

### Ingestion Errors

| Error Type | Handling Strategy |
|------------|-------------------|
| Malformed JSON | Log to COPY_HISTORY, skip file, continue processing |
| Missing required fields | Insert with NULL values, flag for review |
| Invalid data types | Attempt coercion, log warning if fails |
| File not found | Retry 3 times, then mark as failed |

### Processing Errors

| Error Type | Handling Strategy |
|------------|-------------------|
| Task failure | Retain landing data, retry on next schedule |
| Duplicate key | Use MERGE instead of INSERT to handle upserts |
| Warehouse unavailable | Queue task, execute when warehouse resumes |

### Monitoring Queries

```sql
-- Check Snowpipe status
SELECT SYSTEM$PIPE_STATUS('ATTENDANCE_PIPE');

-- View recent ingestion history
SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ATTENDANCE_EVENTS_LANDING',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
));

-- Check task execution history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'PROCESS_ATTENDANCE_EVENTS',
    SCHEDULED_TIME_RANGE_START => DATEADD(hours, -24, CURRENT_TIMESTAMP())
));
```

## Testing Strategy

### Unit Testing

Unit tests verify specific examples and edge cases:

1. **Template Generation Tests**: Verify cloud provider templates contain required elements
2. **Event Type Mapping Tests**: Test each event type maps to correct status
3. **JSON Schema Validation Tests**: Verify sample files match expected schema

### Property-Based Testing

Property-based tests use the `hypothesis` library (Python) to verify universal properties:

1. **Property 1 Test**: Generate random valid JSON events, ingest, verify raw_payload matches input
2. **Property 2 Test**: Generate random event types, verify mapping produces correct status
3. **Property 3 Test**: Insert same record multiple times, verify only one exists in normalized table
4. **Property 4 Test**: Generate mix of valid/invalid JSON, verify valid records processed, invalid rejected

### Integration Testing

Integration tests verify end-to-end pipeline behavior:

1. **End-to-End Ingestion Test**: Upload file to stage, wait, verify in normalized table
2. **Stream/Task Test**: Insert into landing table, verify task processes correctly
3. **Error Recovery Test**: Simulate task failure, verify data retained for retry

### Test File Samples

Sample JSON files for testing are provided in `test_data/snowpipe_samples/`:

- `attendance_sample.json` - Valid attendance events
- `grades_sample.json` - Valid grade events  
- `students_sample.json` - Valid student events
- `malformed_sample.json` - Invalid JSON for error handling tests
