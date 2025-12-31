-- GradSync: AWS S3 Snowpipe Setup
-- Run this script in Snowflake as ACCOUNTADMIN
-- After running, you'll need to update the IAM trust policy with the values from DESC INTEGRATION

USE ROLE ACCOUNTADMIN;
USE DATABASE GRADSYNC_DB;
USE SCHEMA RAW_DATA;

-- ============================================
-- STEP 1: Create Storage Integration
-- ============================================

CREATE OR REPLACE STORAGE INTEGRATION GRADSYNC_S3_INTEGRATION
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<YOUR_AWS_ACCOUNT_ID>:role/SnowflakeGradSyncRole'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<YOUR_BUCKET_NAME>/gradsync/');

-- ============================================
-- STEP 2: Get Snowflake's AWS User ARN and External ID
-- IMPORTANT: Copy these values for the next step!
-- ============================================

DESC INTEGRATION GRADSYNC_S3_INTEGRATION;

-- Look for these two values in the output:
--   STORAGE_AWS_IAM_USER_ARN  (e.g., arn:aws:iam::123456789012:user/abc123)
--   STORAGE_AWS_EXTERNAL_ID   (e.g., ABC123_SFCRole=2_abcdefg...)
--
-- You'll need to update the IAM role trust policy with these values.
-- Run this AWS CLI command after getting the values:
--
-- aws iam update-assume-role-policy --role-name SnowflakeGradSyncRole --policy-document '{
--   "Version": "2012-10-17",
--   "Statement": [{
--     "Effect": "Allow",
--     "Principal": {"AWS": "<STORAGE_AWS_IAM_USER_ARN>"},
--     "Action": "sts:AssumeRole",
--     "Condition": {"StringEquals": {"sts:ExternalId": "<STORAGE_AWS_EXTERNAL_ID>"}}
--   }]
-- }'

-- ============================================
-- STEP 3: Create External Stage (run after updating trust policy)
-- ============================================

CREATE OR REPLACE STAGE GRADSYNC_EXTERNAL_STAGE
    URL = 's3://<YOUR_BUCKET_NAME>/gradsync/'
    STORAGE_INTEGRATION = GRADSYNC_S3_INTEGRATION
    FILE_FORMAT = JSON_FORMAT
    DIRECTORY = (ENABLE = TRUE);

-- Verify connectivity
LIST @GRADSYNC_EXTERNAL_STAGE;

-- ============================================
-- STEP 4: Create Snowpipes
-- ============================================

-- Attendance Pipe
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
        $1
    FROM @GRADSYNC_EXTERNAL_STAGE/attendance/
)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';

-- Grades Pipe
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
        $1
    FROM @GRADSYNC_EXTERNAL_STAGE/grades/
)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';

-- Students Pipe
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
        COALESCE($1:parent_language::VARCHAR, 'English'),
        $1:event_type::VARCHAR,
        $1
    FROM @GRADSYNC_EXTERNAL_STAGE/students/
)
FILE_FORMAT = JSON_FORMAT
ON_ERROR = 'CONTINUE';

-- ============================================
-- STEP 5: Get SQS Queue ARN for S3 Event Notifications
-- ============================================

SHOW PIPES;

-- Copy the notification_channel value - you'll need it for S3 event configuration
-- Run this AWS CLI command with the notification_channel value:
--
-- aws s3api put-bucket-notification-configuration \
--   --bucket <YOUR_BUCKET_NAME> \
--   --notification-configuration '{
--     "QueueConfigurations": [{
--       "QueueArn": "<notification_channel from SHOW PIPES>",
--       "Events": ["s3:ObjectCreated:*"],
--       "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "gradsync/"}]}}
--     }]
--   }'

-- ============================================
-- STEP 6: Resume Processing Tasks
-- ============================================

ALTER TASK PROCESS_ATTENDANCE_EVENTS RESUME;
ALTER TASK PROCESS_GRADES_EVENTS RESUME;
ALTER TASK PROCESS_STUDENTS_EVENTS RESUME;

-- Verify everything is running
SHOW PIPES;
SHOW TASKS LIKE 'PROCESS_%';

-- ============================================
-- STEP 7: Test by uploading a file
-- ============================================

-- Run this AWS CLI command to upload a test file:
-- aws s3 cp test_data/snowpipe_samples/attendance_sample.json s3://<YOUR_BUCKET_NAME>/gradsync/attendance/

-- Then check if data arrived:
SELECT * FROM RAW_DATA.ATTENDANCE_EVENTS_LANDING ORDER BY ingested_at DESC LIMIT 10;
