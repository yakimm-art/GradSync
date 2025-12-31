-- GradSync: Intervention Tracking
-- Issue #16: Track success plan interventions and outcomes

USE DATABASE GRADSYNC_DB;
USE SCHEMA APP;

CREATE TABLE IF NOT EXISTS APP.INTERVENTION_LOG (
    log_id INT AUTOINCREMENT PRIMARY KEY,
    student_id VARCHAR(20),
    plan_generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    plan_text VARCHAR(4000),
    risk_score_at_plan FLOAT,
    primary_risk_factor VARCHAR(100),
    counselor_referral BOOLEAN DEFAULT FALSE,
    interventions_completed VARCHAR(2000),
    outcome_notes VARCHAR(1000),
    outcome_logged_at TIMESTAMP,
    created_by VARCHAR(100)
);

GRANT SELECT, INSERT, UPDATE ON TABLE APP.INTERVENTION_LOG TO ROLE PUBLIC;
