-- GradSync: Snowpipe Auto-Sync Configuration
-- For IT Department: Automatic data ingestion from district systems

USE DATABASE GRADSYNC_DB;
USE SCHEMA RAW_DATA;

-- ============================================
-- STAGE FOR EXTERNAL DATA (S3/Azure/GCS)
-- ============================================

-- Create internal stage for file uploads
CREATE OR REPLACE STAGE GRADSYNC_STAGE
    FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Create file format for JSON (from district APIs)
CREATE OR REPLACE FILE FORMAT JSON_FORMAT
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

-- Create file format for CSV uploads
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('', 'NULL', 'null');

-- ============================================
-- SNOWPIPE FOR ATTENDANCE EVENTS
-- Auto-ingests attendance data from district system
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

-- Snowpipe definition (requires external stage setup)
-- Uncomment and configure for your cloud provider
/*
CREATE OR REPLACE PIPE ATTENDANCE_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO RAW_DATA.ATTENDANCE_EVENTS_LANDING (
    event_id, student_id, event_timestamp, event_type, location, raw_payload
)
FROM (
    SELECT 
        $1:event_id::VARCHAR,
        $1:student_id::VARCHAR,
        $1:timestamp::TIMESTAMP,
        $1:type::VARCHAR,
        $1:location::VARCHAR,
        $1
    FROM @GRADSYNC_STAGE/attendance/
)
FILE_FORMAT = JSON_FORMAT;
*/

-- ============================================
-- STREAM + TASK FOR PROCESSING EVENTS
-- Transforms landing data into normalized tables
-- ============================================

CREATE OR REPLACE STREAM ATTENDANCE_EVENTS_STREAM
    ON TABLE RAW_DATA.ATTENDANCE_EVENTS_LANDING;

CREATE OR REPLACE TASK PROCESS_ATTENDANCE_EVENTS
    WAREHOUSE = GRADSYNC_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ATTENDANCE_EVENTS_STREAM')
AS
INSERT INTO RAW_DATA.ATTENDANCE (student_id, attendance_date, status, period)
SELECT 
    student_id,
    DATE(event_timestamp),
    CASE event_type
        WHEN 'check_in' THEN 'Present'
        WHEN 'check_out_early' THEN 'Present'
        WHEN 'no_show' THEN 'Absent'
        WHEN 'late_arrival' THEN 'Tardy'
        ELSE 'Present'
    END,
    HOUR(event_timestamp) - 7 -- Convert to period (assuming school starts at 8am)
FROM ATTENDANCE_EVENTS_STREAM
WHERE METADATA$ACTION = 'INSERT';

-- Resume task (run once after setup)
-- ALTER TASK PROCESS_ATTENDANCE_EVENTS RESUME;

-- ============================================
-- STORED PROCEDURE FOR BULK CSV PROCESSING
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
