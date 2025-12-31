-- GradSync: Auto-Sync End-to-End Test Script
-- Issue #9: Test Snowpipe configuration
-- Run this script to verify the auto-sync pipeline is working

USE DATABASE GRADSYNC_DB;
USE SCHEMA RAW_DATA;

-- ============================================
-- STEP 1: VERIFY PREREQUISITES
-- ============================================

-- Check that landing tables exist
SELECT 'ATTENDANCE_EVENTS_LANDING' AS table_name, COUNT(*) AS row_count FROM ATTENDANCE_EVENTS_LANDING
UNION ALL
SELECT 'GRADE_EVENTS_LANDING', COUNT(*) FROM GRADE_EVENTS_LANDING
UNION ALL
SELECT 'STUDENT_EVENTS_LANDING', COUNT(*) FROM STUDENT_EVENTS_LANDING;

-- Check that streams exist
SHOW STREAMS LIKE '%EVENTS_STREAM';

-- Check that tasks exist
SHOW TASKS LIKE 'PROCESS_%';

-- ============================================
-- STEP 2: UPLOAD TEST FILES TO INTERNAL STAGE
-- Run these commands from SnowSQL or Snowsight
-- ============================================

-- First, create subdirectories in the stage
-- PUT file://test_data/snowpipe_samples/attendance_sample.json @GRADSYNC_JSON_STAGE/attendance/;
-- PUT file://test_data/snowpipe_samples/grades_sample.json @GRADSYNC_JSON_STAGE/grades/;
-- PUT file://test_data/snowpipe_samples/students_sample.json @GRADSYNC_JSON_STAGE/students/;

-- List files in stage to verify upload
LIST @GRADSYNC_JSON_STAGE;

-- ============================================
-- STEP 3: MANUALLY INGEST TEST DATA
-- Since we're using internal stage, run COPY manually
-- ============================================

-- Ingest attendance test data
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

-- Ingest grades test data
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

-- Ingest students test data
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

-- ============================================
-- STEP 4: VERIFY DATA IN LANDING TABLES
-- ============================================

-- Check attendance landing table
SELECT 'Attendance Landing' AS test, COUNT(*) AS records FROM ATTENDANCE_EVENTS_LANDING;
SELECT * FROM ATTENDANCE_EVENTS_LANDING ORDER BY ingested_at DESC LIMIT 10;

-- Check grades landing table
SELECT 'Grades Landing' AS test, COUNT(*) AS records FROM GRADE_EVENTS_LANDING;
SELECT * FROM GRADE_EVENTS_LANDING ORDER BY ingested_at DESC LIMIT 10;

-- Check students landing table
SELECT 'Students Landing' AS test, COUNT(*) AS records FROM STUDENT_EVENTS_LANDING;
SELECT * FROM STUDENT_EVENTS_LANDING ORDER BY ingested_at DESC LIMIT 10;

-- ============================================
-- STEP 5: VERIFY RAW PAYLOAD PRESERVATION
-- Property 1: Raw Payload Preservation (Round-Trip)
-- ============================================

-- Verify raw_payload contains original JSON
SELECT 
    event_id,
    raw_payload:event_id::VARCHAR AS payload_event_id,
    raw_payload:student_id::VARCHAR AS payload_student_id,
    CASE WHEN event_id = raw_payload:event_id::VARCHAR THEN 'PASS' ELSE 'FAIL' END AS round_trip_test
FROM ATTENDANCE_EVENTS_LANDING
LIMIT 5;

-- ============================================
-- STEP 6: CHECK STREAM STATUS
-- ============================================

-- Verify streams have captured the new data
SELECT 
    'ATTENDANCE_EVENTS_STREAM' AS stream_name,
    SYSTEM$STREAM_HAS_DATA('ATTENDANCE_EVENTS_STREAM') AS has_data
UNION ALL
SELECT 
    'GRADES_EVENTS_STREAM',
    SYSTEM$STREAM_HAS_DATA('GRADES_EVENTS_STREAM')
UNION ALL
SELECT 
    'STUDENTS_EVENTS_STREAM',
    SYSTEM$STREAM_HAS_DATA('STUDENTS_EVENTS_STREAM');

-- ============================================
-- STEP 7: MANUALLY EXECUTE TASKS (for testing)
-- In production, tasks run automatically on schedule
-- ============================================

-- Execute attendance processing task
EXECUTE TASK PROCESS_ATTENDANCE_EVENTS;

-- Execute grades processing task
EXECUTE TASK PROCESS_GRADES_EVENTS;

-- Execute students processing task
EXECUTE TASK PROCESS_STUDENTS_EVENTS;

-- ============================================
-- STEP 8: VERIFY DATA IN NORMALIZED TABLES
-- ============================================

-- Check attendance table for new records
SELECT 'Attendance Normalized' AS test, COUNT(*) AS records FROM ATTENDANCE;
SELECT * FROM ATTENDANCE ORDER BY attendance_date DESC LIMIT 10;

-- Verify event type mapping (Property 2)
SELECT 
    l.event_type AS original_type,
    a.status AS mapped_status,
    CASE 
        WHEN l.event_type = 'check_in' AND a.status = 'Present' THEN 'PASS'
        WHEN l.event_type = 'check_out_early' AND a.status = 'Present' THEN 'PASS'
        WHEN l.event_type = 'no_show' AND a.status = 'Absent' THEN 'PASS'
        WHEN l.event_type = 'late_arrival' AND a.status = 'Tardy' THEN 'PASS'
        ELSE 'FAIL'
    END AS mapping_test
FROM ATTENDANCE_EVENTS_LANDING l
JOIN ATTENDANCE a ON l.student_id = a.student_id 
    AND DATE(l.event_timestamp) = a.attendance_date
LIMIT 10;

-- Check grades table for new records
SELECT 'Grades Normalized' AS test, COUNT(*) AS records FROM GRADES;
SELECT * FROM GRADES ORDER BY grade_date DESC LIMIT 10;

-- Check students table for new records
SELECT 'Students Normalized' AS test, COUNT(*) AS records FROM STUDENTS;
SELECT * FROM STUDENTS ORDER BY enrollment_date DESC LIMIT 10;

-- ============================================
-- STEP 9: TEST IDEMPOTENCY (Property 3)
-- Re-running should not create duplicates
-- ============================================

-- Get current counts
SELECT 'Before Re-run' AS test, COUNT(*) AS attendance_count FROM ATTENDANCE;

-- Re-execute task (should not add duplicates due to MERGE)
EXECUTE TASK PROCESS_ATTENDANCE_EVENTS;

-- Verify count unchanged
SELECT 'After Re-run' AS test, COUNT(*) AS attendance_count FROM ATTENDANCE;

-- ============================================
-- STEP 10: CHECK TASK EXECUTION HISTORY
-- ============================================

SELECT 
    name AS task_name,
    state,
    scheduled_time,
    completed_time,
    error_message
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD(hours, -1, CURRENT_TIMESTAMP())
))
WHERE name LIKE 'PROCESS_%'
ORDER BY scheduled_time DESC;

-- ============================================
-- STEP 11: DIAGNOSTIC QUERIES
-- Use these to troubleshoot issues
-- ============================================

-- Check copy history for errors
SELECT 
    file_name,
    status,
    row_count,
    first_error_message,
    last_load_time
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => 'ATTENDANCE_EVENTS_LANDING',
    START_TIME => DATEADD(hours, -24, CURRENT_TIMESTAMP())
))
ORDER BY last_load_time DESC;

-- Check stream offsets
SHOW STREAMS LIKE '%EVENTS_STREAM';

-- Check task schedules
SHOW TASKS LIKE 'PROCESS_%';

-- ============================================
-- TEST SUMMARY
-- ============================================
SELECT '=== AUTO-SYNC TEST COMPLETE ===' AS message;
SELECT 
    'Landing Tables' AS category,
    (SELECT COUNT(*) FROM ATTENDANCE_EVENTS_LANDING) AS attendance,
    (SELECT COUNT(*) FROM GRADE_EVENTS_LANDING) AS grades,
    (SELECT COUNT(*) FROM STUDENT_EVENTS_LANDING) AS students
UNION ALL
SELECT 
    'Normalized Tables',
    (SELECT COUNT(*) FROM ATTENDANCE),
    (SELECT COUNT(*) FROM GRADES),
    (SELECT COUNT(*) FROM STUDENTS);
