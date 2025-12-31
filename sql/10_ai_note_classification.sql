-- GradSync: AI-Powered Note Classification with Cortex
-- Issue #12: Automatically classify teacher notes into concern categories

USE DATABASE GRADSYNC_DB;
USE SCHEMA APP;
USE WAREHOUSE GRADSYNC_WH;

-- ============================================
-- STEP 1: Add AI Classification Columns to TEACHER_NOTES
-- ============================================

ALTER TABLE APP.TEACHER_NOTES ADD COLUMN IF NOT EXISTS ai_classification VARCHAR(100);
ALTER TABLE APP.TEACHER_NOTES ADD COLUMN IF NOT EXISTS ai_confidence FLOAT;
ALTER TABLE APP.TEACHER_NOTES ADD COLUMN IF NOT EXISTS is_high_risk BOOLEAN DEFAULT FALSE;
ALTER TABLE APP.TEACHER_NOTES ADD COLUMN IF NOT EXISTS reviewed_by VARCHAR(100);
ALTER TABLE APP.TEACHER_NOTES ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMP;

-- ============================================
-- STEP 2: Create Classification Function
-- Uses Cortex CLASSIFY_TEXT to categorize notes
-- ============================================

CREATE OR REPLACE FUNCTION APP.CLASSIFY_NOTE(note_text VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
    SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        note_text,
        ['Academic Struggle', 'Behavioral Concern', 'Social-Emotional Risk', 
         'Attendance Pattern', 'Family Situation', 'Positive Progress']
    )
$$;

-- ============================================
-- STEP 3: Create Helper Function to Extract Classification
-- Returns the top classification label
-- ============================================

CREATE OR REPLACE FUNCTION APP.GET_CLASSIFICATION_LABEL(note_text VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
    SELECT GET(SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        note_text,
        ['Academic Struggle', 'Behavioral Concern', 'Social-Emotional Risk', 
         'Attendance Pattern', 'Family Situation', 'Positive Progress']
    ), 'label')::VARCHAR
$$;

-- ============================================
-- STEP 4: Create Helper Function to Extract Confidence
-- Returns the confidence score (0-1)
-- ============================================

CREATE OR REPLACE FUNCTION APP.GET_CLASSIFICATION_CONFIDENCE(note_text VARCHAR)
RETURNS FLOAT
LANGUAGE SQL
AS
$$
    SELECT GET(SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        note_text,
        ['Academic Struggle', 'Behavioral Concern', 'Social-Emotional Risk', 
         'Attendance Pattern', 'Family Situation', 'Positive Progress']
    ), 'score')::FLOAT
$$;

-- ============================================
-- STEP 5: Create High-Risk Detection Function
-- Flags Social-Emotional Risk and Family Situation as high-risk
-- ============================================

CREATE OR REPLACE FUNCTION APP.IS_HIGH_RISK_NOTE(classification VARCHAR)
RETURNS BOOLEAN
LANGUAGE SQL
AS
$$
    SELECT classification IN ('Social-Emotional Risk', 'Family Situation')
$$;

-- ============================================
-- STEP 6: Create Counselor Alert Queue View
-- Shows all high-risk notes pending review
-- ============================================

CREATE OR REPLACE VIEW APP.COUNSELOR_ALERT_QUEUE AS
SELECT 
    n.note_id,
    n.student_id,
    s.first_name || ' ' || s.last_name as student_name,
    s.grade_level,
    n.note_text,
    n.note_category as teacher_category,
    n.ai_classification,
    n.ai_confidence,
    n.sentiment_score,
    n.is_high_risk,
    n.created_at,
    n.reviewed_by,
    n.reviewed_at,
    CASE 
        WHEN n.reviewed_at IS NOT NULL THEN 'Reviewed'
        WHEN n.is_high_risk THEN 'Pending Review'
        ELSE 'Normal'
    END as review_status,
    DATEDIFF('hour', n.created_at, CURRENT_TIMESTAMP()) as hours_since_created
FROM APP.TEACHER_NOTES n
JOIN RAW_DATA.STUDENTS s ON n.student_id = s.student_id
WHERE n.is_high_risk = TRUE
ORDER BY 
    CASE WHEN n.reviewed_at IS NULL THEN 0 ELSE 1 END,  -- Unreviewed first
    n.created_at DESC;

-- ============================================
-- STEP 7: Create Note Summary View with AI Classification
-- ============================================

CREATE OR REPLACE VIEW APP.NOTES_WITH_AI AS
SELECT 
    n.note_id,
    n.student_id,
    s.first_name || ' ' || s.last_name as student_name,
    s.grade_level,
    n.note_text,
    n.note_category as teacher_category,
    n.ai_classification,
    n.ai_confidence,
    n.sentiment_score,
    n.is_high_risk,
    n.created_at,
    CASE 
        WHEN n.sentiment_score > 0.3 THEN 'Positive'
        WHEN n.sentiment_score < -0.3 THEN 'Negative'
        ELSE 'Neutral'
    END as sentiment_label
FROM APP.TEACHER_NOTES n
JOIN RAW_DATA.STUDENTS s ON n.student_id = s.student_id
ORDER BY n.created_at DESC;

-- ============================================
-- STEP 8: Grant Permissions
-- ============================================

GRANT SELECT ON VIEW APP.COUNSELOR_ALERT_QUEUE TO ROLE PUBLIC;
GRANT SELECT ON VIEW APP.NOTES_WITH_AI TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION APP.CLASSIFY_NOTE(VARCHAR) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION APP.GET_CLASSIFICATION_LABEL(VARCHAR) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION APP.GET_CLASSIFICATION_CONFIDENCE(VARCHAR) TO ROLE PUBLIC;
GRANT USAGE ON FUNCTION APP.IS_HIGH_RISK_NOTE(VARCHAR) TO ROLE PUBLIC;

-- ============================================
-- TEST: Verify Classification Works
-- ============================================
-- SELECT APP.CLASSIFY_NOTE('Student seems withdrawn and mentioned parents are separating');
-- Expected: Family Situation or Social-Emotional Risk

-- SELECT APP.GET_CLASSIFICATION_LABEL('Maria showed great improvement in math today!');
-- Expected: Positive Progress

-- SELECT APP.IS_HIGH_RISK_NOTE('Social-Emotional Risk');
-- Expected: TRUE
