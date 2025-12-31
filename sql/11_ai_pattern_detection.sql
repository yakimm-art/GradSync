-- GradSync: AI Pattern Detection Across Multiple Notes
-- Issue #13: Detect hidden patterns from multiple teacher observations

USE DATABASE GRADSYNC_DB;
USE SCHEMA APP;
USE WAREHOUSE GRADSYNC_WH;

-- ============================================
-- STEP 1: Create AI_INSIGHTS Table
-- Stores AI-detected patterns and early warnings
-- ============================================

CREATE TABLE IF NOT EXISTS APP.AI_INSIGHTS (
    insight_id INT AUTOINCREMENT PRIMARY KEY,
    student_id VARCHAR(20),
    insight_type VARCHAR(50),  -- 'pattern', 'early_warning', 'trend'
    insight_text VARCHAR(2000),
    confidence_score FLOAT,
    contributing_note_ids VARIANT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    expires_at TIMESTAMP,
    is_acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP
);

-- ============================================
-- STEP 2: Create View for Student Notes Summary
-- Aggregates notes for pattern analysis
-- ============================================

CREATE OR REPLACE VIEW APP.STUDENT_NOTES_SUMMARY AS
SELECT 
    n.student_id,
    s.first_name || ' ' || s.last_name as student_name,
    s.grade_level,
    COUNT(*) as note_count,
    LISTAGG(n.note_text, ' | ') WITHIN GROUP (ORDER BY n.created_at DESC) as all_notes,
    ARRAY_AGG(n.note_id) as note_ids,
    AVG(n.sentiment_score) as avg_sentiment,
    SUM(CASE WHEN n.is_high_risk THEN 1 ELSE 0 END) as high_risk_count,
    MAX(n.created_at) as last_note_date
FROM APP.TEACHER_NOTES n
JOIN RAW_DATA.STUDENTS s ON n.student_id = s.student_id
WHERE n.created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY n.student_id, s.first_name, s.last_name, s.grade_level
HAVING COUNT(*) >= 2;  -- Only students with 2+ notes

-- ============================================
-- STEP 3: Grant Permissions
-- ============================================

GRANT SELECT ON TABLE APP.AI_INSIGHTS TO ROLE PUBLIC;
GRANT INSERT, UPDATE ON TABLE APP.AI_INSIGHTS TO ROLE PUBLIC;
GRANT SELECT ON VIEW APP.STUDENT_NOTES_SUMMARY TO ROLE PUBLIC;
