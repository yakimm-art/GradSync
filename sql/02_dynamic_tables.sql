-- GradSync: Dynamic Tables for Analytics
-- These auto-refresh to provide real-time student insights

USE DATABASE GRADSYNC_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE GRADSYNC_WH;

-- ============================================
-- STUDENT 360 VIEW (Core Analytics Table)
-- ============================================

CREATE OR REPLACE DYNAMIC TABLE STUDENT_360_VIEW
    TARGET_LAG = '1 hour'
    WAREHOUSE = GRADSYNC_WH
AS
WITH attendance_summary AS (
    SELECT 
        student_id,
        COUNT(*) as total_days,
        SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) as days_present,
        ROUND(SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) as attendance_rate,
        SUM(CASE WHEN status = 'Tardy' THEN 1 ELSE 0 END) as tardy_count
    FROM RAW_DATA.ATTENDANCE
    WHERE attendance_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY student_id
),
grade_summary AS (
    SELECT 
        student_id,
        ROUND(AVG(score / NULLIF(max_score, 0) * 100), 1) as current_gpa,
        COUNT(DISTINCT course_name) as course_count,
        MIN(score / NULLIF(max_score, 0) * 100) as lowest_grade
    FROM RAW_DATA.GRADES
    WHERE grade_date >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY student_id
),
notes_summary AS (
    SELECT 
        student_id,
        COUNT(*) as note_count,
        AVG(sentiment_score) as avg_sentiment,
        MAX(created_at) as last_note_date
    FROM APP.TEACHER_NOTES
    WHERE created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY student_id
)
SELECT 
    s.student_id,
    s.first_name || ' ' || s.last_name as student_name,
    s.grade_level,
    s.parent_email,
    s.parent_language,
    COALESCE(a.attendance_rate, 100) as attendance_rate,
    COALESCE(a.tardy_count, 0) as tardy_count,
    COALESCE(g.current_gpa, 0) as current_gpa,
    COALESCE(g.lowest_grade, 0) as lowest_grade,
    COALESCE(n.avg_sentiment, 0) as avg_sentiment,
    COALESCE(n.note_count, 0) as note_count,
    n.last_note_date,
    -- Risk Score Calculation (0-100, higher = more at risk)
    ROUND(
        GREATEST(0, LEAST(100,
            (100 - COALESCE(a.attendance_rate, 100)) * 0.4 +
            (100 - COALESCE(g.current_gpa, 100)) * 0.4 +
            (CASE WHEN COALESCE(n.avg_sentiment, 0) < -0.3 THEN 20 ELSE 0 END)
        ))
    , 1) as risk_score,
    CURRENT_TIMESTAMP() as last_updated
FROM RAW_DATA.STUDENTS s
LEFT JOIN attendance_summary a ON s.student_id = a.student_id
LEFT JOIN grade_summary g ON s.student_id = g.student_id
LEFT JOIN notes_summary n ON s.student_id = n.student_id;

-- ============================================
-- AT-RISK STUDENTS VIEW (Filtered for Dashboard)
-- ============================================

CREATE OR REPLACE DYNAMIC TABLE AT_RISK_STUDENTS
    TARGET_LAG = '1 hour'
    WAREHOUSE = GRADSYNC_WH
AS
SELECT 
    *,
    CASE 
        WHEN risk_score >= 70 THEN 'Critical'
        WHEN risk_score >= 50 THEN 'High'
        WHEN risk_score >= 30 THEN 'Moderate'
        ELSE 'Low'
    END as risk_level
FROM STUDENT_360_VIEW
WHERE risk_score >= 30
ORDER BY risk_score DESC;

-- ============================================
-- CLASSROOM HEATMAP DATA
-- ============================================

CREATE OR REPLACE DYNAMIC TABLE CLASSROOM_HEATMAP
    TARGET_LAG = '1 hour'
    WAREHOUSE = GRADSYNC_WH
AS
SELECT 
    grade_level,
    COUNT(*) as total_students,
    SUM(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) as critical_count,
    SUM(CASE WHEN risk_score >= 50 AND risk_score < 70 THEN 1 ELSE 0 END) as high_count,
    SUM(CASE WHEN risk_score >= 30 AND risk_score < 50 THEN 1 ELSE 0 END) as moderate_count,
    ROUND(AVG(attendance_rate), 1) as avg_attendance,
    ROUND(AVG(current_gpa), 1) as avg_gpa
FROM STUDENT_360_VIEW
GROUP BY grade_level
ORDER BY grade_level;

GRANT SELECT ON ALL DYNAMIC TABLES IN SCHEMA ANALYTICS TO ROLE PUBLIC;
