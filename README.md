# GradSync 🎓

**Closing the gap between data and graduation.**

A Snowflake Native App that proactively identifies at-risk students using AI-driven interventions and provides actionable Success Plans for educators.

## The Problem

Most schools have **"Dead Data."** Grades and attendance sit in Snowflake tables, but by the time a human notices a student is failing, it's often too late to help.

## The Solution

GradSync "syncs" school data with AI-driven interventions. It identifies students at risk due to subtle patterns (e.g., a drop in sentiment in teacher notes combined with a 5% dip in attendance) and provides an immediate **Success Plan** for the educator.

---

## Features

### 🏠 Overview Dashboard
- Quick snapshot of student performance metrics
- At-risk student alerts with priority indicators
- Recent activity feed showing teacher observations
- Quick action buttons for common tasks

### 📊 Analytics
- Risk distribution charts by severity level
- Attendance trends by grade level
- GPA distribution visualization
- Performance overview with key metrics

### 📝 Student Observations
- Log notes about student progress and behavior
- AI-powered sentiment analysis (positive/neutral/negative)
- Categorized observations (Academic, Behavioral, Social, Health)
- Real-time feedback with sentiment scores

### 🎯 AI Success Plans
- AI-generated intervention strategies for at-risk students
- Personalized recommendations based on attendance, grades, and sentiment
- Parent communication drafting with multilingual translation
- Support for 8+ languages (Spanish, Chinese, Vietnamese, Korean, Arabic, French, Portuguese, German)

### 📤 Data Import
- Bulk upload via CSV/Excel files
- Support for student rosters, attendance records, and grades
- File preview and validation before import
- Progress tracking during import

### 🔄 Auto-Sync (Snowpipe)
- Automatic data ingestion from district systems
- Support for AWS S3, Azure Blob, and Google Cloud Storage
- Real-time processing via Streams and Tasks
- Landing tables for audit trail preservation

---

## Tech Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Platform** | Snowflake Native App | Single secure environment, no data leaves Snowflake |
| **Frontend** | Streamlit in Snowflake | Browser-based UI, zero installation for teachers |
| **Database** | Snowflake (Standard + Hybrid Tables) | OLAP analytics + OLTP real-time writes |
| **AI/ML** | Snowflake Cortex | Sentiment analysis, text generation, translation |
| **Data Pipeline** | Dynamic Tables + Snowpipe | Auto-refresh analytics, continuous ingestion |
| **Language** | SQL + Python | SQL for data layer, Python for Streamlit app |

### Why This Stack?

**100% Snowflake** - Everything runs inside Snowflake. No external APIs, no data exports, no compliance headaches. Student data stays protected (FERPA).

**Zero Infrastructure** - Teachers open a browser. That's it. No drivers, plugins, or IT tickets.

**Built-in AI** - Cortex functions are SQL-callable. No ML ops, no model hosting, no API keys.

---

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE PLATFORM                      │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Streamlit   │  │   Cortex AI  │  │   Snowpipe   │      │
│  │  (Frontend)  │  │  (ML/NLP)    │  │  (Ingestion) │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                 │                 │              │
│         ▼                 ▼                 ▼              │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Snowflake Data Layer                   │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │   │
│  │  │ Raw Tables  │  │  Dynamic    │  │   Hybrid    │  │   │
│  │  │ (Source)    │  │  Tables     │  │   Tables    │  │   │
│  │  │             │  │ (Analytics) │  │   (OLTP)    │  │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────┘
```

### Data Flow

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

---

## Key Snowflake Features Used

| Feature | What It Does | Why We Use It |
|---------|--------------|---------------|
| **Dynamic Tables** | Auto-refresh materialized views | Student 360 view updates without manual ETL |
| **Hybrid Tables** | OLTP-optimized storage | Sub-100ms teacher note saves |
| **Snowpipe** | Continuous data loading | District systems push data automatically |
| **Streams + Tasks** | Change data capture + scheduling | Process new attendance events in real-time |
| **Cortex SENTIMENT** | NLP sentiment scoring | Detect negative teacher notes as risk signal |
| **Cortex COMPLETE** | LLM text generation | Generate personalized Success Plans |
| **Cortex TRANSLATE** | Multi-language translation | Parent outreach in native language |

---

## Getting Started

### Prerequisites

- Snowflake account with Cortex AI enabled
- Streamlit in Snowflake access
- Warehouse with appropriate compute resources

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/gradsync.git
   cd gradsync
   ```

2. Run SQL scripts in order:
   ```bash
   sql/01_setup_database.sql      # Create schemas & tables
   sql/02_dynamic_tables.sql      # Create analytics views
   sql/03_snowpipe_auto_sync.sql  # Configure auto-ingestion (optional)
   sql/04_sample_data.sql         # Load demo data
   ```

3. Deploy `streamlit/gradsync_app.py` to Snowflake Streamlit

4. Access the app and start exploring!

### Optional: Configure Auto-Sync

For automatic data ingestion from cloud storage:

1. Configure your cloud provider (AWS S3, Azure, or GCS) in `sql/03_snowpipe_auto_sync.sql`
2. For AWS S3 specifically, use `sql/09_aws_s3_setup.sql`
3. Run `sql/08_test_auto_sync.sql` to verify the pipeline

---

## Project Structure

```
gradsync/
├── streamlit/
│   └── gradsync_app.py          # Main Streamlit application
├── sql/
│   ├── 01_setup_database.sql    # Database and table creation
│   ├── 02_dynamic_tables.sql    # Analytics views
│   ├── 03_snowpipe_auto_sync.sql # Snowpipe configuration
│   ├── 04_sample_data.sql       # Demo data
│   ├── 05_bulk_upload_procedure.sql
│   ├── 06_add_parent_language.sql
│   ├── 07_test_direct_entry.sql
│   ├── 08_test_auto_sync.sql    # Auto-sync test script
│   └── 09_aws_s3_setup.sql      # AWS-specific setup
├── test_data/
│   ├── snowpipe_samples/        # JSON test files for Snowpipe
│   └── *.csv                    # CSV test files
├── tests/
│   └── test_snowpipe_properties.py  # Property-based tests
├── aws/
│   └── *.json                   # AWS IAM policy templates
├── openspec/                    # Feature specifications
│   ├── project.md
│   └── specs/
│       ├── risk-analytics/
│       ├── ai-interventions/
│       └── data-entry/
└── README.md
```

---

## Specifications (OpenSpec)

GradSync uses OpenSpec for feature documentation:

### Risk Analytics (`openspec/specs/risk-analytics/`)
- Student 360 View with auto-refreshing Dynamic Tables
- Risk score calculation (0-100 scale)
- Risk level classification (Critical, High, Moderate, Low)
- Grade-level heatmaps and summaries

### AI Interventions (`openspec/specs/ai-interventions/`)
- Sentiment analysis on teacher notes (-1 to +1 scale)
- AI-generated Success Plans with actionable recommendations
- Multilingual parent communication (8+ languages)

### Data Entry (`openspec/specs/data-entry/`)
- Bulk upload via CSV/Excel
- Direct entry with Hybrid Tables (<100ms latency)
- Auto-sync via Snowpipe with Streams and Tasks

### Snowpipe Auto-Sync (`.kiro/specs/snowpipe-auto-sync/`)
- Storage integration templates (AWS, Azure, GCS)
- Landing tables for raw event ingestion
- Event type mapping (check_in→Present, no_show→Absent, etc.)
- Processing idempotency via MERGE statements
- Property-based tests for correctness validation

---

## Testing

### Property Tests

Run the Snowpipe property tests:

```bash
pip install pytest hypothesis
pytest tests/test_snowpipe_properties.py -v
```

Tests validate:
- Raw payload preservation (round-trip)
- Event type to status mapping
- Processing idempotency (no duplicates)
- Malformed JSON rejection

### Manual Testing

Use the test scripts in `sql/`:
- `sql/07_test_direct_entry.sql` - Test teacher note entry
- `sql/08_test_auto_sync.sql` - Test Snowpipe pipeline

---

## Why This Wins "AI for Good"

| Criteria | How GradSync Delivers |
|----------|----------------------|
| **Ease of Use** | Teacher can see at-risk students and have a drafted email ready in under 3 clicks |
| **Data Sovereignty** | Built entirely inside Snowflake—no student data leaves the secure environment (FERPA compliant) |
| **Explainability** | Not just a "Risk Score" black box—Cortex provides human-readable explanations of why students are flagged |

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

Made with ❤️ for educators and students everywhere
