#!/bin/bash
# GradSync GitHub Issues Creation Script
# Run: chmod +x scripts/create-issues.sh && ./scripts/create-issues.sh

export GH_PAGER=cat

echo "🎓 Creating GradSync GitHub Issues..."
echo "======================================"

# Issue 1: Snowflake Account Setup
gh issue create \
  --title "1. Configure Snowflake Account & Enable Required Features" \
  --label "setup" \
  --body "## 🎯 Objective
Set up the Snowflake environment with all required features enabled for GradSync.

## 📋 Prerequisites
- [ ] Snowflake account (Trial or Enterprise)
- [ ] ACCOUNTADMIN role access

## ✅ Tasks

### 1.1 Verify Cortex AI Access
\`\`\`sql
-- Test Cortex is available in your region
SELECT SNOWFLAKE.CORTEX.SENTIMENT('This is a test');
SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', 'Say hello');
\`\`\`

### 1.2 Enable Streamlit in Snowflake
- Navigate to: Admin → Billing & Terms → Snowflake Marketplace
- Accept terms for Streamlit
- Verify: Data Products → Apps → Create Streamlit App (should be available)

### 1.3 Enable Hybrid Tables (Unistore)
\`\`\`sql
-- Check if Hybrid Tables are available
SHOW PARAMETERS LIKE 'ENABLE_HYBRID_TABLES' IN ACCOUNT;
-- If not enabled, contact Snowflake support or use trial account
\`\`\`

### 1.4 Create Warehouse
\`\`\`sql
USE ROLE ACCOUNTADMIN;
CREATE WAREHOUSE IF NOT EXISTS GRADSYNC_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;
\`\`\`

## 🧪 Verification
- [ ] Cortex SENTIMENT returns a number between -1 and 1
- [ ] Cortex COMPLETE returns generated text
- [ ] Can create a Streamlit app in Snowsight
- [ ] Warehouse GRADSYNC_WH exists and can be used

## 📚 Resources
- [Cortex AI Documentation](https://docs.snowflake.com/en/user-guide/snowflake-cortex)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
- [Hybrid Tables](https://docs.snowflake.com/en/user-guide/tables-hybrid)

## ⏱️ Estimated Time: 30 minutes"

echo "✅ Issue 1 created"

# Issue 2: Database Schema Setup
gh issue create \
  --title "2. Create Database Schema & Base Tables" \
  --label "setup,database" \
  --body "## 🎯 Objective
Create the GradSync database structure with all required schemas and tables.

## 📋 Prerequisites
- [ ] Issue #1 completed (Snowflake account configured)

## ✅ Tasks

### 2.1 Run Setup Script
Execute \`sql/01_setup_database.sql\` which creates:

\`\`\`sql
-- Database and Schemas
CREATE DATABASE IF NOT EXISTS GRADSYNC_DB;
CREATE SCHEMA IF NOT EXISTS RAW_DATA;      -- Source data
CREATE SCHEMA IF NOT EXISTS ANALYTICS;      -- Dynamic Tables
CREATE SCHEMA IF NOT EXISTS APP;            -- Hybrid Tables & app objects
\`\`\`

### 2.2 Verify Tables Created

| Schema | Table | Type | Purpose |
|--------|-------|------|---------|
| RAW_DATA | STUDENTS | Standard | Student roster |
| RAW_DATA | ATTENDANCE | Standard | Daily attendance records |
| RAW_DATA | GRADES | Standard | Assignment scores |
| RAW_DATA | BULK_UPLOAD_STAGING | Standard | CSV import staging |
| APP | TEACHER_NOTES | **Hybrid** | Real-time note entry |

### 2.3 Verify Hybrid Table
\`\`\`sql
-- Confirm TEACHER_NOTES is a Hybrid Table
SHOW TABLES LIKE 'TEACHER_NOTES' IN SCHEMA APP;
-- Check 'is_hybrid' column = 'YES'
\`\`\`

### 2.4 Test Write Performance
\`\`\`sql
-- Insert should complete in <100ms
INSERT INTO APP.TEACHER_NOTES (student_id, teacher_id, note_text, note_category, sentiment_score)
VALUES ('TEST001', 'TCH001', 'Test note', 'Academic', 0.5);

-- Verify
SELECT * FROM APP.TEACHER_NOTES WHERE student_id = 'TEST001';

-- Cleanup
DELETE FROM APP.TEACHER_NOTES WHERE student_id = 'TEST001';
\`\`\`

## 🧪 Verification
- [ ] Database GRADSYNC_DB exists
- [ ] All 3 schemas created (RAW_DATA, ANALYTICS, APP)
- [ ] 5 tables created with correct structure
- [ ] TEACHER_NOTES is a Hybrid Table
- [ ] Insert latency is <100ms

## 📁 Files
- \`sql/01_setup_database.sql\`

## ⏱️ Estimated Time: 15 minutes"

echo "✅ Issue 2 created"

# Issue 3: Dynamic Tables
gh issue create \
  --title "3. Create Dynamic Tables for Analytics" \
  --label "database,pipeline" \
  --body "## 🎯 Objective
Create auto-refreshing Dynamic Tables that power the risk analytics dashboard.

## 📋 Prerequisites
- [ ] Issue #2 completed (Database schema created)

## ✅ Tasks

### 3.1 Run Dynamic Tables Script
Execute \`sql/02_dynamic_tables.sql\` which creates:

| Dynamic Table | Purpose | Refresh Lag |
|---------------|---------|-------------|
| STUDENT_360_VIEW | Unified student data with risk score | 1 hour |
| AT_RISK_STUDENTS | Filtered view of students needing attention | 1 hour |
| CLASSROOM_HEATMAP | Aggregate metrics by grade level | 1 hour |

### 3.2 Understand Risk Score Calculation
\`\`\`sql
-- Risk Score Formula (0-100, higher = more at risk)
risk_score = 
    (100 - attendance_rate) * 0.4 +      -- 40% weight
    (100 - current_gpa) * 0.4 +           -- 40% weight
    (negative_sentiment_bonus) * 0.2      -- 20% if avg sentiment < -0.3
\`\`\`

### 3.3 Verify Dynamic Table Status
\`\`\`sql
-- Check refresh status
SELECT name, target_lag, refresh_mode, scheduling_state
FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
WHERE schema_name = 'ANALYTICS';
\`\`\`

### 3.4 Test Risk Level Classification
| Risk Score | Level | Color |
|------------|-------|-------|
| >= 70 | Critical | 🔴 Red |
| >= 50 | High | 🟠 Orange |
| >= 30 | Moderate | 🟡 Yellow |
| < 30 | Low | 🟢 Green |

## 🧪 Verification
- [ ] All 3 Dynamic Tables created
- [ ] Tables show 'SCHEDULED' in scheduling_state
- [ ] Risk score calculation matches formula
- [ ] Risk levels correctly assigned

## 📁 Files
- \`sql/02_dynamic_tables.sql\`

## ⏱️ Estimated Time: 20 minutes"

echo "✅ Issue 3 created"

# Issue 4: Sample Data
gh issue create \
  --title "4. Load Sample Data for Demo" \
  --label "database" \
  --body "## 🎯 Objective
Populate the database with realistic sample data to demonstrate GradSync features.

## 📋 Prerequisites
- [ ] Issue #3 completed (Dynamic Tables created)

## ✅ Tasks

### 4.1 Run Sample Data Script
Execute \`sql/04_sample_data.sql\` which creates:

| Data Type | Records | Notes |
|-----------|---------|-------|
| Students | 10 | Mix of grade levels 9-12 |
| Attendance | ~100 | Last 30 days, varied patterns |
| Grades | ~12 | Mix of good and struggling |
| Teacher Notes | 7 | Pre-analyzed sentiment scores |

### 4.2 Sample Student Profiles

| Student | Risk Profile | Why |
|---------|--------------|-----|
| Emma Johnson (STU001) | Low | Good attendance, good grades, positive notes |
| Liam Martinez (STU002) | High | Many absences, low grades, negative sentiment |
| Ava Rodriguez (STU007) | Critical | 50% attendance, failing grades, concerning notes |
| Sophia Williams (STU003) | Low | Excellent performance |

### 4.3 Verify Data Loaded
\`\`\`sql
-- Check record counts
SELECT 'Students' as table_name, COUNT(*) as rows FROM RAW_DATA.STUDENTS
UNION ALL SELECT 'Attendance', COUNT(*) FROM RAW_DATA.ATTENDANCE
UNION ALL SELECT 'Grades', COUNT(*) FROM RAW_DATA.GRADES
UNION ALL SELECT 'Teacher Notes', COUNT(*) FROM APP.TEACHER_NOTES;
\`\`\`

### 4.4 Trigger Dynamic Table Refresh
\`\`\`sql
-- Force immediate refresh (optional, will auto-refresh within 1 hour)
ALTER DYNAMIC TABLE ANALYTICS.STUDENT_360_VIEW REFRESH;
ALTER DYNAMIC TABLE ANALYTICS.AT_RISK_STUDENTS REFRESH;
ALTER DYNAMIC TABLE ANALYTICS.CLASSROOM_HEATMAP REFRESH;
\`\`\`

### 4.5 Verify Risk Scores
\`\`\`sql
-- Should see students with varying risk levels
SELECT student_name, risk_score, 
    CASE 
        WHEN risk_score >= 70 THEN '🔴 Critical'
        WHEN risk_score >= 50 THEN '🟠 High'
        WHEN risk_score >= 30 THEN '🟡 Moderate'
        ELSE '🟢 Low'
    END as risk_level
FROM ANALYTICS.STUDENT_360_VIEW
ORDER BY risk_score DESC;
\`\`\`

## 🧪 Verification
- [ ] 10 students loaded
- [ ] Attendance records span 30 days
- [ ] At least 3 students show as at-risk (score >= 30)
- [ ] STU007 (Ava) shows as Critical risk
- [ ] Teacher notes have sentiment scores

## 📁 Files
- \`sql/04_sample_data.sql\`

## ⏱️ Estimated Time: 10 minutes"

echo "✅ Issue 4 created"

# Issue 5: Streamlit App Deployment
gh issue create \
  --title "5. Deploy Streamlit App to Snowflake" \
  --label "frontend,setup" \
  --body "## 🎯 Objective
Deploy the GradSync Streamlit application to Snowflake for browser-based access.

## 📋 Prerequisites
- [ ] Issue #4 completed (Sample data loaded)
- [ ] Streamlit in Snowflake enabled (Issue #1)

## ✅ Tasks

### 5.1 Create Streamlit App in Snowsight
1. Navigate to: **Data Products → Apps → + Streamlit App**
2. Configure:
   - **App name:** GRADSYNC_APP
   - **Warehouse:** GRADSYNC_WH
   - **Database:** GRADSYNC_DB
   - **Schema:** APP

### 5.2 Copy Application Code
1. Open the Streamlit editor in Snowsight
2. Replace default code with contents of \`streamlit/gradsync_app.py\`
3. Click **Run** to test

### 5.3 Verify All Pages Load

| Page | What to Check |
|------|---------------|
| 📊 Dashboard | Shows metrics, heatmap, at-risk table |
| 📝 Log Observation | Student dropdown populated, form submits |
| 📤 Bulk Upload | File uploader visible, format examples shown |
| 🎯 Success Plans | Can select student, generates AI plan |
| ⚙️ Data Sync Status | Shows pipeline status |

### 5.4 Test Core Functionality
\`\`\`
1. Dashboard: Verify risk scores match SQL query results
2. Log Observation: Submit a test note, check sentiment displayed
3. Success Plans: Generate plan for Ava Rodriguez (critical risk)
\`\`\`

### 5.5 Grant Access (Optional)
\`\`\`sql
-- Allow other roles to use the app
GRANT USAGE ON STREAMLIT GRADSYNC_DB.APP.GRADSYNC_APP TO ROLE PUBLIC;
\`\`\`

## 🧪 Verification
- [ ] App loads without errors
- [ ] All 5 navigation pages accessible
- [ ] Dashboard shows correct student count
- [ ] Can submit a teacher observation
- [ ] Cortex AI generates Success Plan

## 📁 Files
- \`streamlit/gradsync_app.py\`

## ⏱️ Estimated Time: 30 minutes"

echo "✅ Issue 5 created"

# Issue 6: Cortex AI Integration
gh issue create \
  --title "6. Test & Verify Cortex AI Features" \
  --label "ai" \
  --body "## 🎯 Objective
Verify all three Cortex AI functions work correctly within the application.

## 📋 Prerequisites
- [ ] Issue #5 completed (Streamlit app deployed)

## ✅ Tasks

### 6.1 Test Sentiment Analysis
\`\`\`sql
-- Test various sentiment inputs
SELECT 
    'Emma is doing great! She actively participates.' as note,
    SNOWFLAKE.CORTEX.SENTIMENT('Emma is doing great! She actively participates.') as sentiment
UNION ALL
SELECT 
    'Student seems distracted and tired in class.',
    SNOWFLAKE.CORTEX.SENTIMENT('Student seems distracted and tired in class.')
UNION ALL
SELECT 
    'Completed homework on time.',
    SNOWFLAKE.CORTEX.SENTIMENT('Completed homework on time.');
\`\`\`

**Expected Results:**
| Note Type | Expected Sentiment |
|-----------|-------------------|
| Positive | > 0.3 |
| Negative | < -0.3 |
| Neutral | -0.3 to 0.3 |

### 6.2 Test Success Plan Generation
\`\`\`sql
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'You are an educational advisor. A student named Ava has:
    - 50% attendance rate
    - 2.1 GPA
    - Risk score of 75 (Critical)
    - Recent negative teacher notes about working two jobs
    
    Generate 3-4 specific, actionable interventions for the teacher.'
) as success_plan;
\`\`\`

### 6.3 Test Translation
\`\`\`sql
SELECT SNOWFLAKE.CORTEX.TRANSLATE(
    'Dear Parent, I would like to schedule a meeting to discuss your child progress.',
    'en',
    'es'
) as spanish_translation;
\`\`\`

### 6.4 Verify In-App Integration
1. **Log Observation page:** Submit note, verify sentiment emoji appears
2. **Success Plans page:** Generate plan for at-risk student
3. **Success Plans page:** Test translate button (if parent language is Spanish)

### 6.5 Performance Check
\`\`\`sql
-- Sentiment should complete in <1 second
-- COMPLETE may take 2-5 seconds for longer responses
-- TRANSLATE should complete in <2 seconds
\`\`\`

## 🧪 Verification
- [ ] SENTIMENT returns values in expected ranges
- [ ] COMPLETE generates coherent, relevant Success Plans
- [ ] TRANSLATE produces accurate Spanish translation
- [ ] All functions work within Streamlit app
- [ ] Response times are acceptable for UX

## 📚 Cortex Models Available
| Function | Model | Use Case |
|----------|-------|----------|
| SENTIMENT | Built-in | Analyze teacher notes |
| COMPLETE | mistral-large | Generate Success Plans |
| TRANSLATE | Built-in | Parent communication |

## ⏱️ Estimated Time: 20 minutes"

echo "✅ Issue 6 created"

# Issue 7: Bulk Upload Feature
gh issue create \
  --title "7. Implement & Test Bulk Upload Feature" \
  --label "frontend,pipeline" \
  --body "## 🎯 Objective
Verify the bulk upload feature allows school admins to import CSV/Excel files.

## 📋 Prerequisites
- [ ] Issue #5 completed (Streamlit app deployed)

## ✅ Tasks

### 7.1 Create Test CSV Files

**students_test.csv:**
\`\`\`csv
student_id,first_name,last_name,grade_level,enrollment_date,parent_email
STU011,Alex,Thompson,9,2024-08-15,thompson@email.com
STU012,Maria,Santos,10,2023-08-15,santos@email.com
\`\`\`

**attendance_test.csv:**
\`\`\`csv
student_id,date,status,period
STU011,2024-12-20,Present,1
STU011,2024-12-19,Tardy,1
STU012,2024-12-20,Present,1
\`\`\`

**grades_test.csv:**
\`\`\`csv
student_id,course,assignment,score,max_score,date
STU011,Algebra I,Quiz 1,85,100,2024-12-15
STU012,Biology,Lab Report,92,100,2024-12-18
\`\`\`

### 7.2 Test Upload Flow
1. Navigate to **📤 Bulk Upload** page
2. Select data type (students/attendance/grades)
3. Drag and drop test CSV file
4. Verify preview shows correct data
5. Click **Import Data**
6. Confirm success message

### 7.3 Verify Data Imported
\`\`\`sql
-- Check new students added
SELECT * FROM RAW_DATA.STUDENTS WHERE student_id IN ('STU011', 'STU012');

-- Check staging table processed
SELECT * FROM RAW_DATA.BULK_UPLOAD_STAGING ORDER BY upload_timestamp DESC LIMIT 5;
\`\`\`

### 7.4 Test Error Handling
1. Upload file with missing columns → Should show validation error
2. Upload file with invalid data types → Should show error message
3. Upload empty file → Should show appropriate message

### 7.5 Test Excel Upload (Optional)
- Save test CSV as .xlsx
- Upload via same interface
- Verify parsing works correctly

## 🧪 Verification
- [ ] CSV file uploads successfully
- [ ] Data preview shows correct columns and rows
- [ ] Import completes without errors
- [ ] Data appears in RAW_DATA tables
- [ ] Staging table shows processed = TRUE
- [ ] Error messages display for invalid files

## 📁 Files
- \`streamlit/gradsync_app.py\` (Bulk Upload page section)
- \`sql/01_setup_database.sql\` (BULK_UPLOAD_STAGING table)
- \`sql/03_snowpipe_auto_sync.sql\` (PROCESS_BULK_UPLOAD procedure)

## ⏱️ Estimated Time: 25 minutes"

echo "✅ Issue 7 created"

# Issue 8: Teacher Notes (Hybrid Table)
gh issue create \
  --title "8. Test Direct Entry with Hybrid Tables" \
  --label "frontend,database" \
  --body "## 🎯 Objective
Verify the teacher observation feature provides fast, real-time note entry using Hybrid Tables.

## 📋 Prerequisites
- [ ] Issue #5 completed (Streamlit app deployed)
- [ ] Issue #6 completed (Cortex AI verified)

## ✅ Tasks

### 8.1 Test Note Submission Flow
1. Navigate to **📝 Log Observation** page
2. Select a student from dropdown
3. Choose category (Academic/Behavioral/Social/Health)
4. Enter observation text
5. Click **Save Observation**
6. Verify sentiment emoji and score displayed

### 8.2 Test Various Sentiment Scenarios

| Test Note | Expected Sentiment |
|-----------|-------------------|
| \"Student showed excellent problem-solving skills today!\" | 😊 Positive (> 0.3) |
| \"Completed the assignment.\" | 😐 Neutral (-0.3 to 0.3) |
| \"Student was disruptive and refused to participate.\" | 😟 Negative (< -0.3) |

### 8.3 Verify Hybrid Table Performance
\`\`\`sql
-- Check write latency (should be <100ms)
-- Run this while submitting notes in the app

SELECT 
    note_id,
    created_at,
    DATEDIFF('millisecond', created_at, CURRENT_TIMESTAMP()) as latency_ms
FROM APP.TEACHER_NOTES
ORDER BY created_at DESC
LIMIT 5;
\`\`\`

### 8.4 Verify Recent Notes Display
- After submitting, check \"Recent Observations\" section
- Should show last 5 notes with student name, category, sentiment

### 8.5 Test Concurrent Writes (Optional)
- Open app in two browser tabs
- Submit notes simultaneously
- Verify both notes saved correctly (row-level locking)

## 🧪 Verification
- [ ] Student dropdown populates correctly
- [ ] Note saves without errors
- [ ] Sentiment analysis runs and displays result
- [ ] Write latency is <100ms
- [ ] Recent notes section updates
- [ ] Multiple concurrent writes succeed

## 💡 Why Hybrid Tables?
Standard Snowflake tables are optimized for analytics (OLAP):
- Batch inserts are fast
- Single-row inserts can be slow (100ms+)

Hybrid Tables (Unistore) provide:
- OLTP-optimized storage
- Fast single-row operations (<100ms)
- Row-level locking for concurrency
- Indexed lookups

## ⏱️ Estimated Time: 20 minutes"

echo "✅ Issue 8 created"

# Issue 9: Snowpipe Configuration
gh issue create \
  --title "9. Configure Snowpipe for Auto-Sync (Optional)" \
  --label "pipeline,setup" \
  --body "## 🎯 Objective
Set up Snowpipe for automatic data ingestion from external district systems.

## ⚠️ Note
This is an **optional advanced feature** for IT departments. The app works without it using Bulk Upload.

## 📋 Prerequisites
- [ ] Issue #2 completed (Database schema created)
- [ ] Cloud storage access (S3, Azure Blob, or GCS)

## ✅ Tasks

### 9.1 Review Snowpipe Configuration
The script \`sql/03_snowpipe_auto_sync.sql\` includes:
- Stage definitions for file uploads
- Snowpipe for attendance events
- Stream + Task for processing

### 9.2 Configure External Stage (Choose One)

**AWS S3:**
\`\`\`sql
CREATE OR REPLACE STAGE GRADSYNC_EXTERNAL_STAGE
    URL = 's3://your-bucket/gradsync/'
    CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...');
\`\`\`

**Azure Blob:**
\`\`\`sql
CREATE OR REPLACE STAGE GRADSYNC_EXTERNAL_STAGE
    URL = 'azure://your-account.blob.core.windows.net/gradsync/'
    CREDENTIALS = (AZURE_SAS_TOKEN = '...');
\`\`\`

### 9.3 Enable Snowpipe
\`\`\`sql
-- Uncomment and configure in sql/03_snowpipe_auto_sync.sql
CREATE OR REPLACE PIPE ATTENDANCE_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO RAW_DATA.ATTENDANCE_EVENTS_LANDING
FROM @GRADSYNC_EXTERNAL_STAGE/attendance/
FILE_FORMAT = JSON_FORMAT;
\`\`\`

### 9.4 Configure Cloud Notifications
- **S3:** Set up SQS event notifications
- **Azure:** Configure Event Grid
- **GCS:** Set up Pub/Sub notifications

### 9.5 Enable Processing Task
\`\`\`sql
ALTER TASK PROCESS_ATTENDANCE_EVENTS RESUME;
\`\`\`

### 9.6 Test Auto-Ingestion
1. Upload a JSON file to the stage
2. Wait 1-2 minutes
3. Check landing table for new records
4. Verify processed into ATTENDANCE table

## 🧪 Verification
- [ ] External stage created and accessible
- [ ] Snowpipe status shows 'RUNNING'
- [ ] Test file auto-ingested within 2 minutes
- [ ] Stream captures new records
- [ ] Task processes into normalized table

## 📁 Files
- \`sql/03_snowpipe_auto_sync.sql\`

## 📚 Resources
- [Snowpipe Documentation](https://docs.snowflake.com/en/user-guide/data-load-snowpipe)
- [Streams and Tasks](https://docs.snowflake.com/en/user-guide/streams)

## ⏱️ Estimated Time: 45 minutes (requires cloud setup)"

echo "✅ Issue 9 created"

# Issue 10: Demo Preparation
gh issue create \
  --title "10. Prepare Hackathon Demo" \
  --label "setup" \
  --body "## 🎯 Objective
Prepare a polished demo flow for the hackathon presentation.

## 📋 Prerequisites
- [ ] All previous issues completed (or at minimum #1-6, #8)

## ✅ Tasks

### 10.1 Demo Script (5 minutes)

**Opening (30 sec):**
> \"Most schools have dead data. Grades and attendance sit in tables, but by the time someone notices a student is failing, it's too late. GradSync changes that.\"

**Dashboard Demo (1 min):**
1. Show risk heatmap by grade level
2. Point out Ava Rodriguez (Critical risk)
3. Explain risk score calculation

**AI Features (2 min):**
1. Click on Ava → Generate Success Plan
2. Show Cortex generating personalized interventions
3. Demo one-click translate for Spanish-speaking parent

**Data Entry (1 min):**
1. Log a quick observation for a student
2. Show instant sentiment analysis
3. Mention: \"No drivers, no plugins, just a browser\"

**Closing (30 sec):**
> \"Everything runs inside Snowflake. Student data never leaves. FERPA compliant by design. That's GradSync.\"

### 10.2 Prepare Demo Data
\`\`\`sql
-- Ensure Ava has compelling data for demo
SELECT * FROM ANALYTICS.AT_RISK_STUDENTS 
WHERE student_name LIKE '%Ava%';

-- Verify teacher notes show concerning pattern
SELECT * FROM APP.TEACHER_NOTES 
WHERE student_id = 'STU007';
\`\`\`

### 10.3 Test Full Flow
- [ ] Dashboard loads in <3 seconds
- [ ] Success Plan generates in <10 seconds
- [ ] Note submission completes in <2 seconds
- [ ] No error messages appear

### 10.4 Backup Plan
- Screenshot key screens in case of connectivity issues
- Have SQL queries ready to show data directly
- Prepare talking points if live demo fails

### 10.5 Judging Criteria Alignment

| Criteria | How GradSync Delivers |
|----------|----------------------|
| **Easy to Run** | Browser-only, zero install |
| **AI for Good** | Identifies at-risk students before it's too late |
| **Native App** | 100% Snowflake, writes back data |
| **Explainability** | Shows WHY students are flagged |

## 🧪 Final Checklist
- [ ] Demo flow rehearsed 2-3 times
- [ ] All features working without errors
- [ ] Backup screenshots prepared
- [ ] Timing fits within presentation limit
- [ ] Key talking points memorized

## ⏱️ Estimated Time: 1 hour (rehearsal)"

echo "✅ Issue 10 created"

echo ""
echo "======================================"
echo "🎉 All 10 issues created successfully!"
echo "======================================"
echo ""
echo "View issues: gh issue list"
echo "Open in browser: gh issue list --web"
