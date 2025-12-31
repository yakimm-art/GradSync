-- GradSync: Predictive Early Warning System
-- Issue #14: Predict students likely to become at-risk

USE DATABASE GRADSYNC_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE GRADSYNC_WH;

-- ============================================
-- EARLY WARNING VIEW
-- Detects students showing warning signs before they become at-risk
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.EARLY_WARNING_STUDENTS AS
WITH current_metrics AS (
    -- Current 2-week metrics
    SELECT 
        s.student_id,
        s.first_name || ' ' || s.last_name as student_name,
        s.grade_level,
        COALESCE(
            (SELECT ROUND(SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)
             FROM RAW_DATA.ATTENDANCE a 
             WHERE a.student_id = s.student_id 
             AND a.attendance_date >= DATEADD('day', -14, CURRENT_DATE())), 100
        ) as current_attendance,
        COALESCE(
            (SELECT ROUND(AVG(score / NULLIF(max_score, 0) * 100), 1)
             FROM RAW_DATA.GRADES g 
             WHERE g.student_id = s.student_id 
             AND g.grade_date >= DATEADD('day', -14, CURRENT_DATE())), 100
        ) as current_grade_avg,
        COALESCE(
            (SELECT AVG(sentiment_score)
             FROM APP.TEACHER_NOTES n 
             WHERE n.student_id = s.student_id 
             AND n.created_at >= DATEADD('day', -14, CURRENT_TIMESTAMP())), 0
        ) as current_sentiment,
        COALESCE(
            (SELECT COUNT(*)
             FROM APP.TEACHER_NOTES n 
             WHERE n.student_id = s.student_id 
             AND n.is_high_risk = TRUE
             AND n.created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())), 0
        ) as high_risk_note_count
    FROM RAW_DATA.STUDENTS s
),
previous_metrics AS (
    -- Previous 2-week metrics (2-4 weeks ago)
    SELECT 
        s.student_id,
        COALESCE(
            (SELECT ROUND(SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1)
             FROM RAW_DATA.ATTENDANCE a 
             WHERE a.student_id = s.student_id 
             AND a.attendance_date BETWEEN DATEADD('day', -28, CURRENT_DATE()) AND DATEADD('day', -14, CURRENT_DATE())), 100
        ) as prev_attendance,
        COALESCE(
            (SELECT ROUND(AVG(score / NULLIF(max_score, 0) * 100), 1)
             FROM RAW_DATA.GRADES g 
             WHERE g.student_id = s.student_id 
             AND g.grade_date BETWEEN DATEADD('day', -28, CURRENT_DATE()) AND DATEADD('day', -14, CURRENT_DATE())), 100
        ) as prev_grade_avg,
        COALESCE(
            (SELECT AVG(sentiment_score)
             FROM APP.TEACHER_NOTES n 
             WHERE n.student_id = s.student_id 
             AND n.created_at BETWEEN DATEADD('day', -28, CURRENT_TIMESTAMP()) AND DATEADD('day', -14, CURRENT_TIMESTAMP())), 0
        ) as prev_sentiment
    FROM RAW_DATA.STUDENTS s
)
SELECT 
    c.student_id,
    c.student_name,
    c.grade_level,
    c.current_attendance,
    c.current_grade_avg,
    c.current_sentiment,
    c.high_risk_note_count,
    
    -- Calculate drops
    ROUND(p.prev_attendance - c.current_attendance, 1) as attendance_drop,
    ROUND(p.prev_grade_avg - c.current_grade_avg, 1) as grade_drop,
    ROUND(p.prev_sentiment - c.current_sentiment, 2) as sentiment_drop,
    
    -- Warning indicators
    CASE WHEN p.prev_attendance - c.current_attendance > 5 THEN TRUE ELSE FALSE END as attendance_warning,
    CASE WHEN p.prev_grade_avg - c.current_grade_avg > 10 THEN TRUE ELSE FALSE END as grade_warning,
    CASE WHEN p.prev_sentiment - c.current_sentiment > 0.3 THEN TRUE ELSE FALSE END as sentiment_warning,
    CASE WHEN c.high_risk_note_count >= 3 THEN TRUE ELSE FALSE END as notes_warning,
    
    -- Calculate early warning score (0-60 points)
    (CASE WHEN p.prev_attendance - c.current_attendance > 5 THEN 15 ELSE 0 END) +
    (CASE WHEN p.prev_grade_avg - c.current_grade_avg > 10 THEN 15 ELSE 0 END) +
    (CASE WHEN p.prev_sentiment - c.current_sentiment > 0.3 THEN 10 ELSE 0 END) +
    (CASE WHEN c.high_risk_note_count >= 3 THEN 20 ELSE 0 END) as early_warning_score,
    
    -- Warning level
    CASE 
        WHEN (CASE WHEN p.prev_attendance - c.current_attendance > 5 THEN 15 ELSE 0 END) +
             (CASE WHEN p.prev_grade_avg - c.current_grade_avg > 10 THEN 15 ELSE 0 END) +
             (CASE WHEN p.prev_sentiment - c.current_sentiment > 0.3 THEN 10 ELSE 0 END) +
             (CASE WHEN c.high_risk_note_count >= 3 THEN 20 ELSE 0 END) >= 30 THEN 'High'
        WHEN (CASE WHEN p.prev_attendance - c.current_attendance > 5 THEN 15 ELSE 0 END) +
             (CASE WHEN p.prev_grade_avg - c.current_grade_avg > 10 THEN 15 ELSE 0 END) +
             (CASE WHEN p.prev_sentiment - c.current_sentiment > 0.3 THEN 10 ELSE 0 END) +
             (CASE WHEN c.high_risk_note_count >= 3 THEN 20 ELSE 0 END) >= 15 THEN 'Medium'
        WHEN (CASE WHEN p.prev_attendance - c.current_attendance > 5 THEN 15 ELSE 0 END) +
             (CASE WHEN p.prev_grade_avg - c.current_grade_avg > 10 THEN 15 ELSE 0 END) +
             (CASE WHEN p.prev_sentiment - c.current_sentiment > 0.3 THEN 10 ELSE 0 END) +
             (CASE WHEN c.high_risk_note_count >= 3 THEN 20 ELSE 0 END) > 0 THEN 'Low'
        ELSE 'None'
    END as warning_level

FROM current_metrics c
JOIN previous_metrics p ON c.student_id = p.student_id
WHERE 
    (p.prev_attendance - c.current_attendance > 5) OR
    (p.prev_grade_avg - c.current_grade_avg > 10) OR
    (p.prev_sentiment - c.current_sentiment > 0.3) OR
    (c.high_risk_note_count >= 3)
ORDER BY early_warning_score DESC;

-- Grant permissions
GRANT SELECT ON VIEW ANALYTICS.EARLY_WARNING_STUDENTS TO ROLE PUBLIC;
