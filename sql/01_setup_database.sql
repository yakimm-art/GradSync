-- GradSync: Database Setup
-- Run this first to create the application schema and base tables

USE ROLE ACCOUNTADMIN;

-- Create application database
CREATE DATABASE IF NOT EXISTS GRADSYNC_DB;
USE DATABASE GRADSYNC_DB;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS RAW_DATA;      -- For ingested data
CREATE SCHEMA IF NOT EXISTS ANALYTICS;      -- For Dynamic Tables
CREATE SCHEMA IF NOT EXISTS APP;            -- For Hybrid Tables & app objects

-- Create warehouse
CREATE WAREHOUSE IF NOT EXISTS GRADSYNC_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

USE WAREHOUSE GRADSYNC_WH;

-- ============================================
-- RAW DATA TABLES (Source of Truth)
-- ============================================

CREATE OR REPLACE TABLE RAW_DATA.STUDENTS (
    student_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    grade_level INT,
    enrollment_date DATE,
    parent_email VARCHAR(100),
    parent_language VARCHAR(20) DEFAULT 'English',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE RAW_DATA.ATTENDANCE (
    attendance_id INT AUTOINCREMENT PRIMARY KEY,
    student_id VARCHAR(20),
    attendance_date DATE,
    status VARCHAR(20), -- 'Present', 'Absent', 'Tardy', 'Excused'
    period INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (student_id) REFERENCES RAW_DATA.STUDENTS(student_id)
);

CREATE OR REPLACE TABLE RAW_DATA.GRADES (
    grade_id INT AUTOINCREMENT PRIMARY KEY,
    student_id VARCHAR(20),
    course_name VARCHAR(100),
    assignment_name VARCHAR(200),
    score DECIMAL(5,2),
    max_score DECIMAL(5,2),
    grade_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    FOREIGN KEY (student_id) REFERENCES RAW_DATA.STUDENTS(student_id)
);

-- ============================================
-- HYBRID TABLE FOR TEACHER NOTES (OLTP)
-- Fast single-row inserts for real-time note-taking
-- ============================================

CREATE OR REPLACE HYBRID TABLE APP.TEACHER_NOTES (
    note_id INT AUTOINCREMENT PRIMARY KEY,
    student_id VARCHAR(20),
    teacher_id VARCHAR(20),
    note_text VARCHAR(2000),
    note_category VARCHAR(50), -- 'Academic', 'Behavioral', 'Social', 'Health'
    sentiment_score FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    INDEX idx_student (student_id),
    INDEX idx_teacher (teacher_id)
);

-- ============================================
-- STAGING TABLE FOR BULK UPLOADS
-- ============================================

CREATE OR REPLACE TABLE RAW_DATA.BULK_UPLOAD_STAGING (
    upload_id INT AUTOINCREMENT,
    upload_batch VARCHAR(50),
    uploaded_by VARCHAR(100),
    upload_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    data_type VARCHAR(50), -- 'students', 'attendance', 'grades'
    raw_data VARIANT,
    processed BOOLEAN DEFAULT FALSE
);

GRANT USAGE ON DATABASE GRADSYNC_DB TO ROLE PUBLIC;
GRANT USAGE ON ALL SCHEMAS IN DATABASE GRADSYNC_DB TO ROLE PUBLIC;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA RAW_DATA TO ROLE PUBLIC;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA APP TO ROLE PUBLIC;
