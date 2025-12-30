-- ============================================
-- GradSync: Issue 8 - Test Direct Entry Feature
-- Verify teacher observation with sentiment analysis
-- ============================================

USE DATABASE GRADSYNC_DB;
USE WAREHOUSE GRADSYNC_WH;

-- ============================================
-- 8.1 & 8.2: Test Sentiment Analysis Scenarios
-- ============================================

-- Test 1: Positive sentiment note
SELECT 
    'Positive Test' as test_case,
    'Student showed excellent problem-solving skills today!' as note_text,
    SNOWFLAKE.CORTEX.SENTIMENT('Student showed excellent problem-solving skills today!') as sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT('Student showed excellent problem-solving skills today!') > 0.3 THEN '😊 PASS'
        ELSE '❌ FAIL'
    END as result;

-- Test 2: Neutral sentiment note
SELECT 
    'Neutral Test' as test_case,
    'Completed the assignment.' as note_text,
    SNOWFLAKE.CORTEX.SENTIMENT('Completed the assignment.') as sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT('Completed the assignment.') BETWEEN -0.3 AND 0.3 THEN '😐 PASS'
        ELSE '❌ FAIL'
    END as result;

-- Test 3: Negative sentiment note
SELECT 
    'Negative Test' as test_case,
    'Student was disruptive and refused to participate.' as note_text,
    SNOWFLAKE.CORTEX.SENTIMENT('Student was disruptive and refused to participate.') as sentiment_score,
    CASE 
        WHEN SNOWFLAKE.CORTEX.SENTIMENT('Student was disruptive and refused to participate.') < -0.3 THEN '😟 PASS'
        ELSE '❌ FAIL'
    END as result;

-- ============================================
-- 8.3: Check Write Latency
-- Run this AFTER submitting notes in the app
-- ============================================

SELECT 
    note_id,
    student_id,
    note_category,
    sentiment_score,
    created_at,
    DATEDIFF('millisecond', created_at, CURRENT_TIMESTAMP()) as age_ms,
    CASE 
        WHEN DATEDIFF('millisecond', created_at, CURRENT_TIMESTAMP()) < 5000 THEN '✅ Recent'
        ELSE '📝 Older'
    END as status
FROM APP.TEACHER_NOTES
ORDER BY created_at DESC
LIMIT 10;

-- ============================================
-- 8.4: Verify Recent Notes Display Data
-- ============================================

SELECT 
    n.note_id,
    s.first_name || ' ' || s.last_name as student_name,
    n.note_category,
    n.sentiment_score,
    CASE 
        WHEN n.sentiment_score > 0.3 THEN '😊 Positive'
        WHEN n.sentiment_score < -0.3 THEN '😟 Negative'
        ELSE '😐 Neutral'
    END as sentiment_label,
    n.created_at
FROM APP.TEACHER_NOTES n
JOIN RAW_DATA.STUDENTS s ON n.student_id = s.student_id
ORDER BY n.created_at DESC
LIMIT 5;

-- ============================================
-- Summary Statistics
-- ============================================

SELECT 
    COUNT(*) as total_notes,
    COUNT(DISTINCT student_id) as unique_students,
    ROUND(AVG(sentiment_score), 3) as avg_sentiment,
    SUM(CASE WHEN sentiment_score > 0.3 THEN 1 ELSE 0 END) as positive_notes,
    SUM(CASE WHEN sentiment_score < -0.3 THEN 1 ELSE 0 END) as negative_notes,
    SUM(CASE WHEN sentiment_score BETWEEN -0.3 AND 0.3 THEN 1 ELSE 0 END) as neutral_notes
FROM APP.TEACHER_NOTES;

-- ============================================
-- Insert Test Notes (for verification)
-- ============================================

-- Uncomment to insert test notes:
/*
INSERT INTO APP.TEACHER_NOTES (student_id, teacher_id, note_text, note_category, sentiment_score)
SELECT 
    'STU001',
    'TEACHER001',
    'Student showed excellent problem-solving skills today!',
    'Academic',
    SNOWFLAKE.CORTEX.SENTIMENT('Student showed excellent problem-solving skills today!');

INSERT INTO APP.TEACHER_NOTES (student_id, teacher_id, note_text, note_category, sentiment_score)
SELECT 
    'STU002',
    'TEACHER001',
    'Completed the assignment.',
    'Academic',
    SNOWFLAKE.CORTEX.SENTIMENT('Completed the assignment.');

INSERT INTO APP.TEACHER_NOTES (student_id, teacher_id, note_text, note_category, sentiment_score)
SELECT 
    'STU003',
    'TEACHER001',
    'Student was disruptive and refused to participate.',
    'Behavioral',
    SNOWFLAKE.CORTEX.SENTIMENT('Student was disruptive and refused to participate.');
*/

-- ============================================
-- Note about Hybrid Tables
-- ============================================
-- The current setup uses standard tables because Hybrid Tables
-- require a non-trial Snowflake account.
-- 
-- For production with Hybrid Tables, the table would be:
-- CREATE OR REPLACE HYBRID TABLE APP.TEACHER_NOTES (
--     note_id INT AUTOINCREMENT PRIMARY KEY,
--     student_id VARCHAR(20),
--     teacher_id VARCHAR(20),
--     note_text VARCHAR(2000),
--     note_category VARCHAR(50),
--     sentiment_score FLOAT,
--     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
--     INDEX idx_student (student_id),
--     INDEX idx_created (created_at)
-- );
--
-- Benefits of Hybrid Tables:
-- - <100ms single-row write latency
-- - Row-level locking for concurrent writes
-- - Indexed lookups for fast queries
