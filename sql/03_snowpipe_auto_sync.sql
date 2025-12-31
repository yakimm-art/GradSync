-- GradSync: Snowpipe Auto-Sync Configuration
-- For IT Department: Automatic data ingestion from district systems
-- Issue #9: Configure Snowpipe for Auto-Sync
-- Spec: .kiro/specs/snowpipe-auto-sync/

USE DATABASE GRADSYNC_DB;
USE SCHEMA RAW_DATA;

-- ============================================
-- STEP 1: FILE FORMATS
-- ============================================

-- Create file format for JSON (from district APIs)
-- STRIP_OUTER_ARRAY handles arrays of events in a single file
-- IGNORE_UTF8_ERRORS prevents failures on encoding issues
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    IGNORE_UTF8_ERRORS = TRUE;

-- Create file format for CSV uploads
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL', 'null')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

-- ============================================
-- STEP 2: INTERNAL STAGE (Default - No Cloud Setup Required)
-- ============================================

-- Internal stage for manual file uploads and testing
-- Files can be uploaded via PUT command or Snowsight UI
CREATE OR REPLACE STAGE GRADSYNC_STAGE
    FILE_FORMAT = CSV_FORMAT
    DIRECTORY = (ENABLE = TRUE);

-- Internal stage specifically for JSON auto-sync testing
CREATE OR REPLACE STAGE GRADSYNC_JSON_STAGE
    FILE_FORMAT = JSON_FORMAT
    DIRECTORY = (ENABLE = TRUE);

-- ============================================
-- STEP 3: STORAGE INTEGRATION TEMPLATES
-- Choose ONE provider and configure with your credentials
-- Requires ACCOUNTADMIN role to create storage integrations
-- ============================================

-- ============================================
-- OPTION A: AWS S3 STORAGE INTEGRATION
-- ============================================
-- Prerequisites:
--   1. Create an S3 bucket (e.g., s3://your-district-gradsync/)
--   2. Create an IAM role with S3 read access
--   3. Configure trust relationship for Snowflake
--
-- Configuration Steps:
--   1. Replace <YOUR_BUCKET_NAME> with your S3 bucket name
--   2. Replace <YOUR_AWS_ACCOUNT_ID> with your AWS account ID
--   3. Replace <YOUR_IAM_ROLE_NAME> with your IAM role name
--   4. Run the storage integration creation (requires ACCOUNTADMIN)
--   5. Run DESC INTEGRATION to get the AWS_IAM_USER_ARN and AWS_EXTERNAL_ID
--   6. Update your IAM role trust policy with these values
/*
-- Step 1: Create the storage integration (ACCOUNTADMIN required)
CREATE OR REPLACE STORAGE INTEGRATION GRADSYNC_S3_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/<YOUR_IAM_ROLE_NAME>'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<YOUR_BUCKET_NAME>/gradsync/');

-- Step 2: Get the Snowflake AWS user ARN and external ID for trust policy
DESC INTEGRATION GRADSYNC_S3_INTEGRATION;
-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID values

-- Step 3: Create the external stage
CREATE OR REPLACE STAGE GRADSYNC_EXTERNAL_STAGE
    URL = 's3://<YOUR_BUCKET_NAME>/gradsync/'
    STORAGE_INTEGRATION = GRADSYNC_S3_INTEGRATION
    FILE_FORMAT = JSON_FORMAT
    DIRECTORY = (ENABLE = TRUE);

-- Step 4: Validate connectivity
LIST @GRADSYNC_EXTERNAL_STAGE;
*/

-- ============================================
-- OPTION B: AZURE BLOB STORAGE INTEGRATION
-- ============================================
-- Prerequisites:
--   1. Create an Azure Storage Account and container
--   2. Note your Azure Tenant ID
--   3. Configure Azure AD app registration for Snowflake
--
-- Configuration Steps:
--   1. Replace <YOUR_STORAGE_ACCOUNT> with your Azure storage account name
--   2. Replace <YOUR_CONTAINER> with your container name
--   3. Replace <YOUR_TENANT_ID> with your Azure AD tenant ID
--   4. Run the storage integration creation (requires ACCOUNTADMIN)
--   5. Run DESC INTEGRATION to get consent URL
--   6. Grant consent in Azure portal
/*
-- Step 1: Create the storage integration (ACCOUNTADMIN required)
CREATE OR REPLACE STORAGE INTEGRATION GRADSYNC_AZURE_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'AZURE'
    ENABLED = TRUE
    AZURE_TENANT_ID = '<YOUR_TENANT_ID>'
    STORAGE_ALLOWED_LOCATIONS = ('azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/<YOUR_CONTAINER>/gradsync/');

-- Step 2: Get the consent URL
DESC INTEGRATION GRADSYNC_AZURE_INTEGRATION;
-- Open AZURE_CONSENT_URL in browser and grant consent

-- Step 3: Create the external stage
CREATE OR REPLACE STAGE GRADSYNC_EXTERNAL_STAGE
    URL = 'azure://<YOUR_STORAGE_ACCOUNT>.blob.core.windows.net/<YOUR_CONTAINER>/gradsync/'
    STORAGE_INTEGRATION = GRADSYNC_AZURE_INTEGRATION
    FILE_FORMAT = JSON_FORMAT
    DIRECTORY = (ENABLE = TRUE);

-- Step 4: Validate connectivity
LIST @GRADSYNC_EXTERNAL_STAGE;
*/

-- ============================================
-- OPTION C: GOOGLE CLOUD STORAGE INTEGRATION
-- ============================================
-- Prerequisites:
--   1. Create a GCS bucket
--   2. Create a service account with Storage Object Viewer role
--   3. Generate a JSON key file for the service account
--
-- Configuration Steps:
--   1. Replace <YOUR_BUCKET_NAME> with your GCS bucket name
--   2. Run the storage integration creation (requires ACCOUNTADMIN)
--   3. Run DESC INTEGRATION to get the GCS service account
--   4. Grant the Snowflake service account access to your bucket
/*
-- Step 1: Create the storage integration (ACCOUNTADMIN required)
CREATE OR REPLACE STORAGE INTEGRATION GRADSYNC_GCS_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'GCS'
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://<YOUR_BUCKET_NAME>/gradsync/');

-- Step 2: Get the Snowflake GCS service account
DESC INTEGRATION GRADSYNC_GCS_INTEGRATION;
-- Copy STORAGE_GCP_SERVICE_ACCOUNT value

-- Step 3: In GCP Console, grant this service account "Storage Object Viewer" role on your bucket

-- Step 4: Create the external stage
CREATE OR REPLACE STAGE GRADSYNC_EXTERNAL_STAGE
    URL = 'gcs://<YOUR_BUCKET_NAME>/gradsync/'
    STORAGE_INTEGRATION = GRADSYNC_GCS_INTEGRATION
    FILE_FORMAT = JSON_FORMAT
    DIRECTORY = (ENABLE = TRUE);

-- Step 5: Validate connectivity
LIST @GRADSYNC_EXTERNAL_STAGE;
*/

-- ============================================
-- STEP 3.1: VALIDATE STORAGE INTEGRATION
-- Run these after configuring your chosen provider
-- ============================================
/*
-- Check integration status
SHOW INTEGRATIONS LIKE 'GRADSYNC%';

-- Describe integration details
DESC INTEGRATION GRADSYNC_S3_INTEGRATION;  -- or AZURE/GCS variant

-- List files in stage (validates connectivity)
LIST @GRADSYNC_EXTERNAL_STAGE;

-- Test reading a file
SELECT $1 FROM @GRADSYNC_EXTERNAL_STAGE/attendance/ LIMIT 5;
*/

-- ============================================
-- STEP 4: LANDING TABLES FOR AUTO-INGESTION
-- ============================================

-- Landing table for raw attendance events
CREATE OR REPLACE TABLE RAW_DATA.ATTENDANCE_EVENTS_LANDING (
    event_id VARCHAR(50),
    student_id VARCHAR(20),
    event_timestamp TIMESTAMP,
    event_type VARCHAR(20),
    location VARCHAR(100),
    raw_payload VARIANT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Landing table for grade events
CREATE OR REPLACE TABLE RAW_DATA.GRADE_EVENTS_LANDING (
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

-- Landing table for student updates
CREATE OR REPLACE TABLE RAW_DATA.STUDENT_EVENTS_LANDING (
    event_id VARCHAR(50),
    student_id VARCHAR(20),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    grade_level INT,
    parent_email VARCHAR(100),
    parent_language VARCHAR(20),
    event_type VARCHAR(20), -- 'create', 'update', 'transfer'
    raw_payload VARIANT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================
-- STEP 5: SNOWPIPE DEFINITIONS
-- These pipes automatically ingest JSON files when they arrive in the stage
-- Uncomment after configuring external stage (Step 3)
-- For testing with internal stage, use the _INTERNAL variants below
-- ============================================

-- ============================================
-- 5A: SNOWPIPES FOR EXTERNAL STAGE (Production)
-- Uncomment after configuring cloud storage integration
-- ============================================

-- Snowpipe for Attendance Events
-- Ingests JSON files from /attendance/ folder
-- Raw payload stored in VARIANT column for audit (Property 1: Raw Payload Preservation)
/*
CREATE OR REPLACE PIPE ATTENDANCE_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingests attendance events from district system'
AS
COPY INTO RAW_DATA.ATTENDANCE_EVENTS_LANDING (
    event_id, student_id, event_timestamp, event_type, location, raw_payload
)
FROM (
    SELECT 
        $1:event_id::VARCHAR,
        $1:student_id::VARCHAR,
        TRY_TO_TIMESTAMP($1:timestamp::VARCHAR),
        $1:type::VARCHAR,
        $1:location::VARCHAR,
        $1  -- Store complete raw JSON for audit trail
    FROM @GRADSYNC_EXTERNAL_STAGE/attendance/
)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';  -- Continue processing valid files if some are malformed
*/

-- Snowpipe for Grade Events
-- Ingests JSON files from /grades/ folder
-- Handles decimal score parsing with TRY_TO_DECIMAL for robustness
/*
CREATE OR REPLACE PIPE GRADES_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingests grade events from LMS'
AS
COPY INTO RAW_DATA.GRADE_EVENTS_LANDING (
    event_id, student_id, course_name, assignment_name, score, max_score, grade_date, raw_payload
)
FROM (
    SELECT 
        $1:event_id::VARCHAR,
        $1:student_id::VARCHAR,
        $1:course::VARCHAR,
        $1:assignment::VARCHAR,
        TRY_TO_DECIMAL($1:score::VARCHAR, 5, 2),
        TRY_TO_DECIMAL($1:max_score::VARCHAR, 5, 2),
        TRY_TO_DATE($1:date::VARCHAR),
        $1  -- Store complete raw JSON for audit trail
    FROM @GRADSYNC_EXTERNAL_STAGE/grades/
)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';
*/

-- Snowpipe for Student Updates
-- Ingests JSON files from /students/ folder
-- Handles optional parent_language with COALESCE default
/*
CREATE OR REPLACE PIPE STUDENTS_PIPE
    AUTO_INGEST = TRUE
    COMMENT = 'Auto-ingests student roster updates from SIS'
AS
COPY INTO RAW_DATA.STUDENT_EVENTS_LANDING (
    event_id, student_id, first_name, last_name, grade_level, parent_email, parent_language, event_type, raw_payload
)
FROM (
    SELECT 
        $1:event_id::VARCHAR,
        $1:student_id::VARCHAR,
        $1:first_name::VARCHAR,
        $1:last_name::VARCHAR,
        TRY_TO_NUMBER($1:grade_level::VARCHAR),
        $1:parent_email::VARCHAR,
        COALESCE($1:parent_language::VARCHAR, 'English'),  -- Default to English if not specified
        $1:event_type::VARCHAR,
        $1  -- Store complete raw JSON for audit trail
    FROM @GRADSYNC_EXTERNAL_STAGE/students/
)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';
*/

-- ============================================
-- 5B: MANUAL COPY COMMANDS FOR INTERNAL STAGE (Testing)
-- Use these for testing without cloud storage setup
-- Upload files to @GRADSYNC_JSON_STAGE first
-- ============================================

-- Manual copy for attendance (run after uploading test file)
/*
COPY INTO RAW_DATA.ATTENDANCE_EVENTS_LANDING (
    event_id, student_id, event_timestamp, event_type, location, raw_payload
)
FROM (
    SELECT 
        $1:event_id::VARCHAR,
        $1:student_id::VARCHAR,
        TRY_TO_TIMESTAMP($1:timestamp::VARCHAR),
        $1:type::VARCHAR,
        $1:location::VARCHAR,
        $1
    FROM @GRADSYNC_JSON_STAGE/attendance/
)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';
*/

-- Manual copy for grades
/*
COPY INTO RAW_DATA.GRADE_EVENTS_LANDING (
    event_id, student_id, course_name, assignment_name, score, max_score, grade_date, raw_payload
)
FROM (
    SELECT 
        $1:event_id::VARCHAR,
        $1:student_id::VARCHAR,
        $1:course::VARCHAR,
        $1:assignment::VARCHAR,
        TRY_TO_DECIMAL($1:score::VARCHAR, 5, 2),
        TRY_TO_DECIMAL($1:max_score::VARCHAR, 5, 2),
        TRY_TO_DATE($1:date::VARCHAR),
        $1
    FROM @GRADSYNC_JSON_STAGE/grades/
)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';
*/

-- Manual copy for students
/*
COPY INTO RAW_DATA.STUDENT_EVENTS_LANDING (
    event_id, student_id, first_name, last_name, grade_level, parent_email, parent_language, event_type, raw_payload
)
FROM (
    SELECT 
        $1:event_id::VARCHAR,
        $1:student_id::VARCHAR,
        $1:first_name::VARCHAR,
        $1:last_name::VARCHAR,
        TRY_TO_NUMBER($1:grade_level::VARCHAR),
        $1:parent_email::VARCHAR,
        COALESCE($1:parent_language::VARCHAR, 'English'),
        $1:event_type::VARCHAR,
        $1
    FROM @GRADSYNC_JSON_STAGE/students/
)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';
*/

-- ============================================
-- STREAMS + TASKS FOR PROCESSING EVENTS
-- Transforms landing data into normalized tables
-- Uses MERGE for idempotency (Property 3: Processing Idempotency)
-- ============================================

-- ============================================
-- STEP 6: STREAMS (Change Data Capture)
-- Streams track new records in landing tables
-- ============================================

CREATE OR REPLACE STREAM ATTENDANCE_EVENTS_STREAM
    ON TABLE RAW_DATA.ATTENDANCE_EVENTS_LANDING
    APPEND_ONLY = TRUE;  -- Only track inserts, not updates/deletes

CREATE OR REPLACE STREAM GRADES_EVENTS_STREAM
    ON TABLE RAW_DATA.GRADE_EVENTS_LANDING
    APPEND_ONLY = TRUE;

CREATE OR REPLACE STREAM STUDENTS_EVENTS_STREAM
    ON TABLE RAW_DATA.STUDENT_EVENTS_LANDING
    APPEND_ONLY = TRUE;

-- ============================================
-- STEP 7: PROCESSING TASKS
-- Tasks run on schedule when streams have data
-- ============================================

-- Task: Process Attendance Events
-- Maps event_type to attendance status (Property 2: Event Type to Status Mapping)
-- Uses MERGE to prevent duplicates (Property 3: Processing Idempotency)
CREATE OR REPLACE TASK PROCESS_ATTENDANCE_EVENTS
    WAREHOUSE = GRADSYNC_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ATTENDANCE_EVENTS_STREAM')
AS
MERGE INTO RAW_DATA.ATTENDANCE AS target
USING (
    SELECT 
        student_id,
        DATE(event_timestamp) AS attendance_date,
        -- Property 2: Event Type to Status Mapping
        CASE event_type
            WHEN 'check_in' THEN 'Present'
            WHEN 'check_out_early' THEN 'Present'
            WHEN 'no_show' THEN 'Absent'
            WHEN 'late_arrival' THEN 'Tardy'
            ELSE 'Present'  -- Default to Present for unknown types
        END AS status,
        -- Calculate period from timestamp (assuming school starts at 8am)
        GREATEST(1, LEAST(8, HOUR(event_timestamp) - 7)) AS period,
        event_id
    FROM ATTENDANCE_EVENTS_STREAM
    WHERE METADATA$ACTION = 'INSERT'
      AND student_id IS NOT NULL
      AND event_timestamp IS NOT NULL
) AS source
ON target.student_id = source.student_id 
   AND target.attendance_date = source.attendance_date
   AND target.period = source.period
WHEN NOT MATCHED THEN
    INSERT (student_id, attendance_date, status, period)
    VALUES (source.student_id, source.attendance_date, source.status, source.period);

-- Task: Process Grade Events
-- Transforms grade events into normalized GRADES table
CREATE OR REPLACE TASK PROCESS_GRADES_EVENTS
    WAREHOUSE = GRADSYNC_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('GRADES_EVENTS_STREAM')
AS
MERGE INTO RAW_DATA.GRADES AS target
USING (
    SELECT 
        student_id,
        course_name,
        assignment_name,
        score,
        max_score,
        grade_date,
        event_id
    FROM GRADES_EVENTS_STREAM
    WHERE METADATA$ACTION = 'INSERT'
      AND student_id IS NOT NULL
      AND course_name IS NOT NULL
) AS source
ON target.student_id = source.student_id 
   AND target.course_name = source.course_name
   AND target.assignment_name = source.assignment_name
   AND target.grade_date = source.grade_date
WHEN MATCHED THEN
    UPDATE SET 
        score = source.score,
        max_score = source.max_score
WHEN NOT MATCHED THEN
    INSERT (student_id, course_name, assignment_name, score, max_score, grade_date)
    VALUES (source.student_id, source.course_name, source.assignment_name, 
            source.score, source.max_score, source.grade_date);

-- Task: Process Student Events
-- Handles create/update/transfer events for student roster
CREATE OR REPLACE TASK PROCESS_STUDENTS_EVENTS
    WAREHOUSE = GRADSYNC_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('STUDENTS_EVENTS_STREAM')
AS
MERGE INTO RAW_DATA.STUDENTS AS target
USING (
    SELECT 
        student_id,
        first_name,
        last_name,
        grade_level,
        parent_email,
        parent_language,
        event_type,
        CURRENT_DATE() AS enrollment_date
    FROM STUDENTS_EVENTS_STREAM
    WHERE METADATA$ACTION = 'INSERT'
      AND student_id IS NOT NULL
      AND event_type IN ('create', 'update', 'transfer')
) AS source
ON target.student_id = source.student_id
WHEN MATCHED AND source.event_type IN ('update', 'transfer') THEN
    UPDATE SET 
        first_name = COALESCE(source.first_name, target.first_name),
        last_name = COALESCE(source.last_name, target.last_name),
        grade_level = COALESCE(source.grade_level, target.grade_level),
        parent_email = COALESCE(source.parent_email, target.parent_email),
        parent_language = COALESCE(source.parent_language, target.parent_language)
WHEN NOT MATCHED AND source.event_type = 'create' THEN
    INSERT (student_id, first_name, last_name, grade_level, enrollment_date, parent_email, parent_language)
    VALUES (source.student_id, source.first_name, source.last_name, 
            source.grade_level, source.enrollment_date, source.parent_email, source.parent_language);

-- ============================================
-- STEP 8: RESUME TASKS (Run once after setup)
-- Tasks are created in suspended state by default
-- ============================================
/*
-- Resume all processing tasks
ALTER TASK PROCESS_ATTENDANCE_EVENTS RESUME;
ALTER TASK PROCESS_GRADES_EVENTS RESUME;
ALTER TASK PROCESS_STUDENTS_EVENTS RESUME;

-- Verify tasks are running
SHOW TASKS LIKE 'PROCESS_%';
*/

-- ============================================
-- STEP 9: MONITORING VIEWS
-- Views for IT administrators to monitor pipeline health
-- ============================================

-- View: Snowpipe Status Summary
-- Shows current state of all Snowpipes
CREATE OR REPLACE VIEW APP.SNOWPIPE_STATUS AS
SELECT 
    'ATTENDANCE_PIPE' AS pipe_name,
    PARSE_JSON(SYSTEM$PIPE_STATUS('RAW_DATA.ATTENDANCE_PIPE')):executionState::VARCHAR AS status,
    PARSE_JSON(SYSTEM$PIPE_STATUS('RAW_DATA.ATTENDANCE_PIPE')):pendingFileCount::INT AS pending_files,
    PARSE_JSON(SYSTEM$PIPE_STATUS('RAW_DATA.ATTENDANCE_PIPE')):lastIngestedTimestamp::TIMESTAMP AS last_ingestion
UNION ALL
SELECT 
    'GRADES_PIPE',
    PARSE_JSON(SYSTEM$PIPE_STATUS('RAW_DATA.GRADES_PIPE')):executionState::VARCHAR,
    PARSE_JSON(SYSTEM$PIPE_STATUS('RAW_DATA.GRADES_PIPE')):pendingFileCount::INT,
    PARSE_JSON(SYSTEM$PIPE_STATUS('RAW_DATA.GRADES_PIPE')):lastIngestedTimestamp::TIMESTAMP
UNION ALL
SELECT 
    'STUDENTS_PIPE',
    PARSE_JSON(SYSTEM$PIPE_STATUS('RAW_DATA.STUDENTS_PIPE')):executionState::VARCHAR,
    PARSE_JSON(SYSTEM$PIPE_STATUS('RAW_DATA.STUDENTS_PIPE')):pendingFileCount::INT,
    PARSE_JSON(SYSTEM$PIPE_STATUS('RAW_DATA.STUDENTS_PIPE')):lastIngestedTimestamp::TIMESTAMP;

-- View: Ingestion Metrics (Last 24 Hours)
-- Shows file and record counts by pipe
CREATE OR REPLACE VIEW APP.INGESTION_METRICS_24H AS
SELECT 
    pipe_name,
    COUNT(DISTINCT file_name) AS files_processed,
    SUM(row_count) AS records_ingested,
    SUM(CASE WHEN status = 'LOAD_FAILED' THEN 1 ELSE 0 END) AS failed_files,
    MIN(last_load_time) AS first_load,
    MAX(last_load_time) AS last_load
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ATTENDANCE_EVENTS_LANDING',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
GROUP BY pipe_name
UNION ALL
SELECT 
    pipe_name,
    COUNT(DISTINCT file_name),
    SUM(row_count),
    SUM(CASE WHEN status = 'LOAD_FAILED' THEN 1 ELSE 0 END),
    MIN(last_load_time),
    MAX(last_load_time)
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'GRADE_EVENTS_LANDING',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
GROUP BY pipe_name
UNION ALL
SELECT 
    pipe_name,
    COUNT(DISTINCT file_name),
    SUM(row_count),
    SUM(CASE WHEN status = 'LOAD_FAILED' THEN 1 ELSE 0 END),
    MIN(last_load_time),
    MAX(last_load_time)
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'STUDENT_EVENTS_LANDING',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
GROUP BY pipe_name;

-- View: Ingestion Error Log
-- Shows recent ingestion failures with details
CREATE OR REPLACE VIEW APP.INGESTION_ERRORS AS
SELECT 
    pipe_name,
    file_name,
    status,
    first_error_message AS error_message,
    first_error_line_number AS error_line,
    last_load_time AS error_time,
    row_count AS rows_before_error
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ATTENDANCE_EVENTS_LANDING',
    START_TIME => DATEADD(days, -7, CURRENT_TIMESTAMP())
))
WHERE status = 'LOAD_FAILED' OR first_error_message IS NOT NULL
UNION ALL
SELECT 
    pipe_name,
    file_name,
    status,
    first_error_message,
    first_error_line_number,
    last_load_time,
    row_count
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'GRADE_EVENTS_LANDING',
    START_TIME => DATEADD(days, -7, CURRENT_TIMESTAMP())
))
WHERE status = 'LOAD_FAILED' OR first_error_message IS NOT NULL
UNION ALL
SELECT 
    pipe_name,
    file_name,
    status,
    first_error_message,
    first_error_line_number,
    last_load_time,
    row_count
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'STUDENT_EVENTS_LANDING',
    START_TIME => DATEADD(days, -7, CURRENT_TIMESTAMP())
))
WHERE status = 'LOAD_FAILED' OR first_error_message IS NOT NULL
ORDER BY error_time DESC;

-- View: Task Execution History
-- Shows recent task runs and their status
CREATE OR REPLACE VIEW APP.TASK_EXECUTION_HISTORY AS
SELECT 
    name AS task_name,
    state,
    scheduled_time,
    completed_time,
    DATEDIFF('second', scheduled_time, completed_time) AS duration_seconds,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
WHERE name LIKE 'PROCESS_%'
ORDER BY scheduled_time DESC;

-- ============================================
-- STEP 10: STORED PROCEDURE FOR BULK CSV PROCESSING
-- Called by Streamlit after file upload
-- ============================================

CREATE OR REPLACE PROCEDURE RAW_DATA.PROCESS_BULK_UPLOAD(
    data_type VARCHAR,
    batch_id VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    CASE data_type
        WHEN 'students' THEN
            INSERT INTO RAW_DATA.STUDENTS (student_id, first_name, last_name, grade_level, enrollment_date, parent_email)
            SELECT 
                raw_data:student_id::VARCHAR,
                raw_data:first_name::VARCHAR,
                raw_data:last_name::VARCHAR,
                raw_data:grade_level::INT,
                raw_data:enrollment_date::DATE,
                raw_data:parent_email::VARCHAR
            FROM RAW_DATA.BULK_UPLOAD_STAGING
            WHERE upload_batch = :batch_id AND processed = FALSE;
            
        WHEN 'attendance' THEN
            INSERT INTO RAW_DATA.ATTENDANCE (student_id, attendance_date, status, period)
            SELECT 
                raw_data:student_id::VARCHAR,
                raw_data:date::DATE,
                raw_data:status::VARCHAR,
                raw_data:period::INT
            FROM RAW_DATA.BULK_UPLOAD_STAGING
            WHERE upload_batch = :batch_id AND processed = FALSE;
            
        WHEN 'grades' THEN
            INSERT INTO RAW_DATA.GRADES (student_id, course_name, assignment_name, score, max_score, grade_date)
            SELECT 
                raw_data:student_id::VARCHAR,
                raw_data:course::VARCHAR,
                raw_data:assignment::VARCHAR,
                raw_data:score::DECIMAL(5,2),
                raw_data:max_score::DECIMAL(5,2),
                raw_data:date::DATE
            FROM RAW_DATA.BULK_UPLOAD_STAGING
            WHERE upload_batch = :batch_id AND processed = FALSE;
    END CASE;
    
    UPDATE RAW_DATA.BULK_UPLOAD_STAGING 
    SET processed = TRUE 
    WHERE upload_batch = :batch_id;
    
    RETURN 'Processed batch: ' || :batch_id;
END;
$$;

GRANT USAGE ON PROCEDURE RAW_DATA.PROCESS_BULK_UPLOAD(VARCHAR, VARCHAR) TO ROLE PUBLIC;
