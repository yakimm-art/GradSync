-- GradSync: Sentiment Trend Analysis
-- Issue #15: Track sentiment changes over time

USE DATABASE GRADSYNC_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE GRADSYNC_WH;

-- ============================================
-- Sentiment Trend View
-- Shows sentiment over time for each student
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.STUDENT_SENTIMENT_TRENDS AS
SELECT 
    n.student_id,
    s.first_name || ' ' || s.last_name as student_name,
    s.grade_level,
    DATE_TRUNC('day', n.created_at) as note_date,
    AVG(n.sentiment_score) as avg_sentiment,
    COUNT(*) as note_count,
    SUM(CASE WHEN n.is_high_risk THEN 1 ELSE 0 END) as high_risk_count
FROM APP.TEACHER_NOTES n
JOIN RAW_DATA.STUDENTS s ON n.student_id = s.student_id
WHERE n.created_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
GROUP BY n.student_id, s.first_name, s.last_name, s.grade_level, DATE_TRUNC('day', n.created_at)
ORDER BY n.student_id, note_date;

-- ============================================
-- Sentiment Summary View
-- Current vs previous period comparison
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.SENTIMENT_SUMMARY AS
WITH current_sentiment AS (
    SELECT 
        student_id,
        AVG(sentiment_score) as current_avg,
        COUNT(*) as current_count
    FROM APP.TEACHER_NOTES
    WHERE created_at >= DATEADD('day', -14, CURRENT_TIMESTAMP())
    GROUP BY student_id
),
previous_sentiment AS (
    SELECT 
        student_id,
        AVG(sentiment_score) as previous_avg,
        COUNT(*) as previous_count
    FROM APP.TEACHER_NOTES
    WHERE created_at BETWEEN DATEADD('day', -28, CURRENT_TIMESTAMP()) AND DATEADD('day', -14, CURRENT_TIMESTAMP())
    GROUP BY student_id
)
SELECT 
    s.student_id,
    s.first_name || ' ' || s.last_name as student_name,
    s.grade_level,
    COALESCE(c.current_avg, 0) as current_sentiment,
    COALESCE(p.previous_avg, 0) as previous_sentiment,
    COALESCE(c.current_avg, 0) - COALESCE(p.previous_avg, 0) as sentiment_change,
    CASE 
        WHEN COALESCE(c.current_avg, 0) - COALESCE(p.previous_avg, 0) > 0.1 THEN 'Improving'
        WHEN COALESCE(c.current_avg, 0) - COALESCE(p.previous_avg, 0) < -0.1 THEN 'Declining'
        ELSE 'Stable'
    END as trend,
    COALESCE(c.current_count, 0) as recent_note_count
FROM RAW_DATA.STUDENTS s
LEFT JOIN current_sentiment c ON s.student_id = c.student_id
LEFT JOIN previous_sentiment p ON s.student_id = p.student_id
WHERE c.current_count > 0 OR p.previous_count > 0;

GRANT SELECT ON VIEW ANALYTICS.STUDENT_SENTIMENT_TRENDS TO ROLE PUBLIC;
GRANT SELECT ON VIEW ANALYTICS.SENTIMENT_SUMMARY TO ROLE PUBLIC;
