-- GradSync: Bulk Upload Processing Procedure
-- Creates the stored procedure to process uploaded CSV data

USE DATABASE GRADSYNC_DB;
USE SCHEMA RAW_DATA;
USE WAREHOUSE GRADSYNC_WH;

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
DECLARE
    rows_processed INT DEFAULT 0;
BEGIN
    CASE data_type
        WHEN 'students' THEN
            INSERT INTO RAW_DATA.STUDENTS (student_id, first_name, last_name, grade_level, enrollment_date, parent_email)
            SELECT 
                raw_data:student_id::VARCHAR,
                raw_data:first_name::VARCHAR,
                raw_data:last_name::VARCHAR,
                raw_data:grade_level::INT,
                TRY_TO_DATE(raw_data:enrollment_date::VARCHAR),
                raw_data:parent_email::VARCHAR
            FROM RAW_DATA.BULK_UPLOAD_STAGING
            WHERE upload_batch = :batch_id AND processed = FALSE;
            
            rows_processed := SQLROWCOUNT;
            
        WHEN 'attendance' THEN
            INSERT INTO RAW_DATA.ATTENDANCE (student_id, attendance_date, status, period)
            SELECT 
                raw_data:student_id::VARCHAR,
                TRY_TO_DATE(raw_data:date::VARCHAR),
                raw_data:status::VARCHAR,
                raw_data:period::INT
            FROM RAW_DATA.BULK_UPLOAD_STAGING
            WHERE upload_batch = :batch_id AND processed = FALSE;
            
            rows_processed := SQLROWCOUNT;
            
        WHEN 'grades' THEN
            INSERT INTO RAW_DATA.GRADES (student_id, course_name, assignment_name, score, max_score, grade_date)
            SELECT 
                raw_data:student_id::VARCHAR,
                raw_data:course::VARCHAR,
                raw_data:assignment::VARCHAR,
                raw_data:score::DECIMAL(5,2),
                raw_data:max_score::DECIMAL(5,2),
                TRY_TO_DATE(raw_data:date::VARCHAR)
            FROM RAW_DATA.BULK_UPLOAD_STAGING
            WHERE upload_batch = :batch_id AND processed = FALSE;
            
            rows_processed := SQLROWCOUNT;
            
        ELSE
            RETURN 'Error: Unknown data type: ' || :data_type;
    END CASE;
    
    -- Mark staging records as processed
    UPDATE RAW_DATA.BULK_UPLOAD_STAGING 
    SET processed = TRUE 
    WHERE upload_batch = :batch_id;
    
    RETURN 'Successfully processed ' || :rows_processed || ' ' || :data_type || ' records (batch: ' || :batch_id || ')';
END;
$$;

-- Grant execute permission
GRANT USAGE ON PROCEDURE RAW_DATA.PROCESS_BULK_UPLOAD(VARCHAR, VARCHAR) TO ROLE PUBLIC;

-- ============================================
-- TEST THE PROCEDURE
-- ============================================

-- Test with sample data
-- INSERT INTO RAW_DATA.BULK_UPLOAD_STAGING (upload_batch, uploaded_by, data_type, raw_data)
-- VALUES ('test123', 'TEST_USER', 'students', PARSE_JSON('{"student_id":"STU999","first_name":"Test","last_name":"User","grade_level":9}'));
-- CALL RAW_DATA.PROCESS_BULK_UPLOAD('students', 'test123');
-- SELECT * FROM RAW_DATA.STUDENTS WHERE student_id = 'STU999';
