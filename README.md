# GradSync

**Closing the gap between data and graduation.**

A Snowflake Native App that proactively identifies at-risk students using AI-driven interventions and provides actionable Success Plans for educators.

## The Problem

Most schools have **"Dead Data."** Grades and attendance sit in Snowflake tables, but by the time a human notices a student is failing, it's often too late to help.

## The Solution

GradSync "syncs" school data with AI-driven interventions. It identifies students at risk due to subtle patterns (e.g., a drop in sentiment in teacher notes combined with a 5% dip in attendance) and provides an immediate **Success Plan** for the educator.

---

## Technical Architecture

### A. The Data Pipeline (The "Sync")

Use **Dynamic Tables** to create a "360-degree Student View."

- **Implementation:** Join `ENROLLMENT`, `ATTENDANCE`, `GRADES`, and `TEACHER_NOTES` (unstructured) into one materialized view
- **Why it wins:** Demonstrates automated, low-maintenance data engineering that "just works" for school districts

### B. The Intelligence Engine (Cortex AI)

Leverage two Snowflake Cortex functions:

| Function | Purpose |
|----------|---------|
| `SNOWFLAKE.CORTEX.SENTIMENT` | Analyze teacher comments. A "Neutral" grade with "Negative" sentiment is a leading indicator of dropout risk |
| `SNOWFLAKE.CORTEX.COMPLETE` | Generate personalized Success Plans using few-shot prompting |

**Example Output:**
> "Recommendation: Schedule a 15-minute check-in; student may be struggling with the new Algebra module. Provide 'Module 3' practice worksheet."

### C. The Frontend (Streamlit in Snowflake)

Keep it "Few Clicks" for non-technical teachers:

- **Risk Heatmap:** Visual representation of the classroom with AI-flagged "Sync Issues"
- **One-Click Outreach:** Button that uses `CORTEX.TRANSLATE` to draft supportive emails to parents in their primary language

---

## Implementation Steps

### Step 1: Data Preparation (SQL)

Create a unified table pulling from your synthetic dataset:

```sql
-- Example: Dynamic Table for 360-degree Student View
CREATE OR REPLACE DYNAMIC TABLE student_360_view
  TARGET_LAG = '1 hour'
  WAREHOUSE = compute_wh
AS
SELECT 
    e.student_id,
    e.student_name,
    a.attendance_rate,
    g.current_gpa,
    t.teacher_notes,
    SNOWFLAKE.CORTEX.SENTIMENT(t.teacher_notes) as note_sentiment
FROM enrollment e
JOIN attendance a ON e.student_id = a.student_id
JOIN grades g ON e.student_id = g.student_id
LEFT JOIN teacher_notes t ON e.student_id = t.student_id;
```

### Step 2: AI Logic (Python/Streamlit)

Call Cortex in your Streamlit app to provide the "Why":

```python
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

# Generate Success Plan using Cortex
def generate_success_plan(student_context):
    prompt = f"""Based on this student data: {student_context}
    Generate a specific, actionable Success Plan for the educator."""
    
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', '{prompt}') as plan
    """).collect()
    
    return result[0]['PLAN']
```

### Step 3: Reliability Check (Data Metric Functions)

Monitor data integrity with DMFs:

```sql
-- Monitor for null student IDs
CREATE OR REPLACE DATA METRIC FUNCTION null_student_id_check(
    ARG_T TABLE(student_id STRING)
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(*) FROM ARG_T WHERE student_id IS NULL
$$;
```

---

## Why This Wins "AI for Good"

| Criteria | How GradSync Delivers |
|----------|----------------------|
| **Ease of Use** | Teacher can see at-risk students and have a drafted email ready in under 3 clicks |
| **Data Sovereignty** | Built entirely inside Snowflake—no student data leaves the secure environment (FERPA compliant) |
| **Explainability** | Not just a "Risk Score" black box—Cortex provides human-readable explanations of why students are flagged |

---

## Three Data Entry Methods

GradSync provides a "Closed Loop" system where data entry, analysis, and action all happen within Snowflake—no external tools required.

| Method | User Type | Implementation | Why It Works |
|--------|-----------|----------------|--------------|
| **Bulk Upload** | School Admin | `st.file_uploader` → Staging Table → Stored Procedure | Teachers drag-and-drop Excel/CSV from Canvas/PowerSchool |
| **Direct Entry** | Individual Teacher | Streamlit Form → Hybrid Table (OLTP) | Sub-100ms writes for real-time note-taking |
| **Auto-Sync** | IT Department | Snowpipe + Streams + Tasks | District database pushes attendance events automatically |

### The "Write-Back" Architecture

GradSync isn't just a dashboard—it's a true Native App that writes data back to Snowflake:

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Bulk Upload    │────▶│  Staging Table   │────▶│  Raw Tables     │
│  (CSV/Excel)    │     │  + Procedure     │     │                 │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
┌─────────────────┐     ┌──────────────────┐              ▼
│  Teacher Note   │────▶│  Hybrid Table    │────▶┌─────────────────┐
│  (Direct Entry) │     │  (OLTP - Fast!)  │     │ Dynamic Tables  │
└─────────────────┘     └──────────────────┘     │ (Auto-Refresh)  │
                                                  └────────┬────────┘
┌─────────────────┐     ┌──────────────────┐              │
│  District DB    │────▶│  Snowpipe        │──────────────┘
│  (Auto-Sync)    │     │  + Stream/Task   │              ▼
└─────────────────┘     └──────────────────┘     ┌─────────────────┐
                                                  │  Cortex AI      │
                                                  │  (Sentiment +   │
                                                  │   Success Plans)│
                                                  └─────────────────┘
```

### Why Hybrid Tables for Teacher Notes?

Standard Snowflake tables are optimized for analytics (OLAP), but teachers need instant feedback when saving notes. Hybrid Tables (Unistore) provide:

- **Fast single-row inserts** (<100ms latency)
- **Row-level locking** for concurrent writes
- **Indexed lookups** by student_id

```sql
CREATE OR REPLACE HYBRID TABLE APP.TEACHER_NOTES (
    note_id INT AUTOINCREMENT PRIMARY KEY,
    student_id VARCHAR(20),
    note_text VARCHAR(2000),
    sentiment_score FLOAT,
    INDEX idx_student (student_id)
);
```

---

## Getting Started

### Prerequisites

- Snowflake account with Cortex AI enabled
- Streamlit in Snowflake access
- Warehouse with appropriate compute resources

### Installation

1. Clone the repository
2. Run SQL scripts in order:
   ```bash
   sql/01_setup_database.sql    # Create schemas & tables
   sql/02_dynamic_tables.sql    # Create analytics views
   sql/03_snowpipe_auto_sync.sql # Configure auto-ingestion
   sql/04_sample_data.sql       # Load demo data
   ```
3. Deploy `streamlit/gradsync_app.py` to Snowflake
4. Access the app and start exploring!

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

Made with ❤️ for educators and students everywhere
