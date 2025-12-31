-- GradSync: Dynamic Tables for Analytics
-- These auto-refresh to provide real-time student insights

USE DATABASE GRADSYNC_DB;
USE SCHEMA ANALYTICS;
USE WAREHOUSE GRADSYNC_WH;

-- ============================================
-- STUDENT 360 VIEW (Core Analytics Table)
-- 4-Factor Explainable Risk Scoring
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
        ROUND(AVG(score / NULLIF(max_score, 0) * 4.0), 2) as current_gpa,  -- GPA on 4.0 scale
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
        MAX(created_at) as last_note_date,
        SUM(CASE WHEN sentiment_score < -0.3 THEN 1 ELSE 0 END) as negative_note_count
    FROM APP.TEACHER_NOTES
    WHERE created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY student_id
),
-- Historical data for trend detection (7 days ago)
attendance_7d_ago AS (
    SELECT 
        student_id,
        ROUND(SUM(CASE WHEN status = 'Present' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) as attendance_rate_7d_ago
    FROM RAW_DATA.ATTENDANCE
    WHERE attendance_date BETWEEN DATEADD('day', -37, CURRENT_DATE()) AND DATEADD('day', -7, CURRENT_DATE())
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
    COALESCE(g.current_gpa, 4.0) as current_gpa,
    COALESCE(g.lowest_grade, 100) as lowest_grade,
    COALESCE(n.avg_sentiment, 0) as avg_sentiment,
    COALESCE(n.note_count, 0) as note_count,
    n.last_note_date,
    
    -- ============================================
    -- 4-FACTOR RISK BREAKDOWN (Explainable AI)
    -- Each factor contributes 0-25 points (max 100)
    -- ============================================
    
    -- Factor 1: Attendance (25% weight)
    -- Low attendance = higher risk
    ROUND((100 - COALESCE(a.attendance_rate, 100)) * 0.25, 1) as attendance_risk_contribution,
    
    -- Factor 2: Academic Performance (25% weight)
    -- Low GPA = higher risk (normalized from 4.0 scale)
    ROUND((100 - (COALESCE(g.current_gpa, 4.0) / 4.0 * 100)) * 0.25, 1) as academic_risk_contribution,
    
    -- Factor 3: Sentiment Analysis (25% weight)
    -- Negative sentiment in teacher notes = higher risk
    ROUND(CASE 
        WHEN COALESCE(n.avg_sentiment, 0) < -0.5 THEN 25  -- Very negative
        WHEN COALESCE(n.avg_sentiment, 0) < -0.3 THEN 20  -- Negative
        WHEN COALESCE(n.avg_sentiment, 0) < 0 THEN 10     -- Slightly negative
        ELSE 0                                             -- Neutral/Positive
    END, 1) as sentiment_risk_contribution,
    
    -- Factor 4: AI-Detected Signals (25% weight)
    -- Combines multiple warning signals
    ROUND(LEAST(25,
        -- Tardy pattern (up to 10 points)
        LEAST(10, COALESCE(a.tardy_count, 0) * 2) +
        -- Multiple negative notes (up to 10 points)
        LEAST(10, COALESCE(n.negative_note_count, 0) * 5) +
        -- Attendance decline trend (up to 5 points)
        CASE WHEN COALESCE(a.attendance_rate, 100) < COALESCE(a7.attendance_rate_7d_ago, 100) - 5 THEN 5 ELSE 0 END
    ), 1) as ai_signal_risk_contribution,
    
    -- Total Risk Score (sum of 4 factors)
    ROUND(
        GREATEST(0, LEAST(100,
            -- Factor 1: Attendance
            (100 - COALESCE(a.attendance_rate, 100)) * 0.25 +
            -- Factor 2: Academic
            (100 - (COALESCE(g.current_gpa, 4.0) / 4.0 * 100)) * 0.25 +
            -- Factor 3: Sentiment
            CASE 
                WHEN COALESCE(n.avg_sentiment, 0) < -0.5 THEN 25
                WHEN COALESCE(n.avg_sentiment, 0) < -0.3 THEN 20
                WHEN COALESCE(n.avg_sentiment, 0) < 0 THEN 10
                ELSE 0
            END +
            -- Factor 4: AI Signals
            LEAST(25,
                LEAST(10, COALESCE(a.tardy_count, 0) * 2) +
                LEAST(10, COALESCE(n.negative_note_count, 0) * 5) +
                CASE WHEN COALESCE(a.attendance_rate, 100) < COALESCE(a7.attendance_rate_7d_ago, 100) - 5 THEN 5 ELSE 0 END
            )
        ))
    , 1) as risk_score,
    
    -- Trend indicators for "New Signal" highlighting
    COALESCE(a7.attendance_rate_7d_ago, 100) as attendance_rate_7d_ago,
    CASE WHEN COALESCE(a.attendance_rate, 100) < COALESCE(a7.attendance_rate_7d_ago, 100) - 5 
         THEN TRUE ELSE FALSE END as attendance_declining,
    
    CURRENT_TIMESTAMP() as last_updated
FROM RAW_DATA.STUDENTS s
LEFT JOIN attendance_summary a ON s.student_id = a.student_id
LEFT JOIN grade_summary g ON s.student_id = g.student_id
LEFT JOIN notes_summary n ON s.student_id = n.student_id
LEFT JOIN attendance_7d_ago a7 ON s.student_id = a7.student_id;

-- ============================================
-- RISK BREAKDOWN VIEW (For UI Display)
-- ============================================

CREATE OR REPLACE VIEW ANALYTICS.RISK_BREAKDOWN AS
SELECT 
    student_id,
    student_name,
    grade_level,
    risk_score,
    attendance_risk_contribution,
    academic_risk_contribution,
    sentiment_risk_contribution,
    ai_signal_risk_contribution,
    -- Percentage contribution of each factor
    ROUND(attendance_risk_contribution / NULLIF(risk_score, 0) * 100, 0) as attendance_pct,
    ROUND(academic_risk_contribution / NULLIF(risk_score, 0) * 100, 0) as academic_pct,
    ROUND(sentiment_risk_contribution / NULLIF(risk_score, 0) * 100, 0) as sentiment_pct,
    ROUND(ai_signal_risk_contribution / NULLIF(risk_score, 0) * 100, 0) as ai_signal_pct,
    -- Human-readable explanation
    CASE 
        WHEN risk_score >= 70 THEN 'Critical Risk'
        WHEN risk_score >= 50 THEN 'High Risk'
        WHEN risk_score >= 30 THEN 'Moderate Risk'
        ELSE 'Low Risk'
    END as risk_level,
    -- Primary risk factor
    CASE 
        WHEN attendance_risk_contribution >= academic_risk_contribution 
             AND attendance_risk_contribution >= sentiment_risk_contribution 
             AND attendance_risk_contribution >= ai_signal_risk_contribution 
        THEN 'Attendance'
        WHEN academic_risk_contribution >= sentiment_risk_contribution 
             AND academic_risk_contribution >= ai_signal_risk_contribution 
        THEN 'Academic Performance'
        WHEN sentiment_risk_contribution >= ai_signal_risk_contribution 
        THEN 'Teacher Observations'
        ELSE 'Multiple Warning Signs'
    END as primary_risk_factor,
    -- New signal flag
    attendance_declining as has_new_signal,
    attendance_rate,
    current_gpa,
    avg_sentiment
FROM ANALYTICS.STUDENT_360_VIEW;

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
GRANT SELECT ON VIEW ANALYTICS.RISK_BREAKDOWN TO ROLE PUBLIC;
