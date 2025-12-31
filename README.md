# GradSync 🎓

**Closing the gap between data and graduation.**

> *AI-powered student success platform built entirely on Snowflake*

A Snowflake Native App that proactively identifies at-risk students using AI-driven analysis and provides actionable intervention plans for educators.

---

## 🎯 The Problem

Most schools have **"Dead Data."** Grades and attendance sit in spreadsheets, legacy databases, and disconnected systems. By the time a human notices a student is failing, it's often too late to help.

**The stats are alarming:**
- 1.2 million students drop out of high school annually in the US
- Early warning signs are often visible in data months before dropout
- Teachers lack tools to spot subtle patterns across multiple data sources

**There is not only one path towards graduation.** Every student faces unique challenges — some struggle with attendance, others with academics, and many deal with social-emotional issues that never show up in a gradebook. The problem isn't just identifying at-risk students; it's syncing the goals of teachers, counselors, parents, and administrators to create a unified support system around each student.

When data lives in silos, so do the people trying to help.

## 💡 The Solution

GradSync brings modern data infrastructure to an old problem. By consolidating school data into Snowflake and applying AI-driven analysis, it empowers educators with real-time insights and actionable interventions.

---

## ✨ Features

### 📊 Dashboard
- **Modern UI** with light/dark mode toggle
- **Real-time metrics** — Total students, at-risk count, attendance rates, GPA averages
- **Colorful stat cards** with gradient backgrounds
- **Quick actions** for common tasks
- **Recent alerts** sidebar
- **Intervention progress** tracking

### 👥 Students Module
Three integrated views in one place:

| Tab | Description |
|-----|-------------|
| **📊 Analytics** | At-risk student list with risk scores, attendance, and GPA |
| **⚡ Early Warnings** | Students showing warning signs before becoming at-risk |
| **📈 Sentiment Trends** | Track how teacher observations change over time |

### 📝 Notes Module
Comprehensive observation and alert system:

| Tab | Description |
|-----|-------------|
| **📝 Add Observation** | Log teacher notes with AI sentiment analysis |
| **🚨 Counselor Alerts** | High-risk notes flagged for counselor review |
| **🧠 AI Insights** | Cross-teacher pattern detection using Cortex AI |

### 🎯 Interventions Module
End-to-end intervention management:

| Tab | Description |
|-----|-------------|
| **🎯 Create Plan** | AI-generated success plans with risk-specific recommendations |
| **📋 Track Progress** | Log completed interventions and outcomes |

**Key Features:**
- 4-factor risk breakdown (Attendance, Academic, Sentiment, AI Signals)
- "Why This Score?" explainability
- Counselor referral recommendations
- Parent message translation (8+ languages)

### 📤 Import Data
Multiple data entry points:
- **File Upload** — CSV/Excel from Canvas, PowerSchool, or Google Sheets
- **Data Type Selection** — Students, Attendance, or Grades
- **Preview & Validate** — See data before importing
- **Progress Tracking** — Real-time import status

---

## 🤖 AI-Powered Features

| Feature | Snowflake Cortex Function | Description |
|---------|---------------------------|-------------|
| **Sentiment Analysis** | `CORTEX.SENTIMENT` | Analyzes teacher notes for emotional tone |
| **Note Classification** | `CORTEX.CLASSIFY_TEXT` | Categorizes notes (Academic, Behavioral, Safety, etc.) |
| **Pattern Detection** | `CORTEX.COMPLETE` | Identifies hidden patterns across multiple observations |
| **Success Plans** | `CORTEX.COMPLETE` | Generates personalized intervention strategies |
| **Translation** | `CORTEX.TRANSLATE` | Parent outreach in 8+ languages |

### Risk Scoring System

```
Risk Score = Attendance Risk + Academic Risk + Sentiment Risk + AI Signal Risk
             (0-25 pts)       (0-25 pts)      (0-25 pts)       (0-25 pts)
```

**Risk Levels:**
- 🟢 **Low** (0-49): Student on track
- 🟡 **Moderate** (50-69): Needs monitoring
- 🔴 **Critical** (70-100): Immediate intervention required

---

## ⚡ Snowflake Features Used

| Feature | How We Use It |
|---------|---------------|
| **Streamlit in Snowflake** | Teacher-friendly UI, zero installation |
| **Dynamic Tables** | Auto-refreshing Student 360 View & Risk Breakdown |
| **Hybrid Tables (Unistore)** | Sub-100ms teacher note saves |
| **Snowpipe** | Auto-ingest from district systems |
| **Streams + Tasks** | Real-time event processing |
| **Cortex SENTIMENT** | Analyze teacher notes for risk signals |
| **Cortex CLASSIFY_TEXT** | Categorize observations automatically |
| **Cortex COMPLETE** | Generate Success Plans & detect patterns |
| **Cortex TRANSLATE** | Parent outreach in native language |

---

## 🏗️ Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE PLATFORM                      │
│                                                            │
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
│                                                            │
│  🔒 All data stays inside Snowflake — FERPA compliant      │
└────────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites
- Snowflake account with Cortex AI enabled
- Streamlit in Snowflake access

### Installation

1. **Clone the repo**
   ```bash
   git clone https://github.com/your-org/gradsync.git
   cd gradsync
   ```

2. **Run SQL scripts in order**
   ```sql
   -- 1. Setup database and tables
   sql/01_setup_database.sql
   
   -- 2. Create Dynamic Tables
   sql/02_dynamic_tables.sql
   
   -- 3. Setup Snowpipe (optional)
   sql/03_snowpipe_auto_sync.sql
   
   -- 4. Load sample data
   sql/04_sample_data.sql
   
   -- 5-14. Additional features (AI classification, patterns, etc.)
   ```

3. **Deploy Streamlit app**
   ```bash
   snow streamlit deploy
   ```

4. **Open the app and explore!**

---

## 📁 Project Structure

```
gradsync/
├── streamlit/
│   └── gradsync_app.py              # Main Streamlit application
├── sql/
│   ├── 00_verify_snowflake_setup.sql
│   ├── 01_setup_database.sql        # Core schema
│   ├── 02_dynamic_tables.sql        # Analytics views
│   ├── 03_snowpipe_auto_sync.sql    # Auto-ingestion
│   ├── 04_sample_data.sql           # Demo data
│   ├── 05_bulk_upload_procedure.sql # CSV processing
│   ├── 06_add_parent_language.sql   # Translation support
│   ├── 07_test_direct_entry.sql
│   ├── 08_test_auto_sync.sql
│   ├── 09_aws_s3_setup.sql          # S3 integration
│   ├── 10_ai_note_classification.sql # Cortex classification
│   ├── 11_ai_pattern_detection.sql  # Cross-teacher patterns
│   ├── 12_early_warning_system.sql  # Predictive alerts
│   ├── 13_sentiment_trends.sql      # Sentiment tracking
│   └── 14_intervention_tracking.sql # Outcome logging
├── tests/
│   ├── test_snowpipe_properties.py  # Property-based tests
│   └── requirements.txt
├── test_data/
│   ├── snowpipe_samples/            # JSON test files
│   ├── students_test.csv
│   ├── grades_test.csv
│   └── attendance_test.csv
├── aws/
│   ├── snowflake-s3-policy.json
│   └── snowflake-trust-policy-initial.json
├── snowflake.yml                    # Deployment config
└── README.md
```

---

## 📊 Sample Data

The demo includes realistic sample data:
- **10 students** across grades 9-12
- **Attendance records** with various statuses
- **Grade entries** across multiple courses
- **Teacher notes** with sentiment examples

All data is synthetic — no real student information.

---

## 🧪 Testing

```bash
cd tests
pip install -r requirements.txt
pytest test_snowpipe_properties.py -v
```

Tests cover:
- ✅ Raw payload preservation
- ✅ Event type mapping
- ✅ Processing idempotency
- ✅ Malformed JSON rejection

---

## 🌍 Supported Languages

Parent communication translation:
- 🇪🇸 Spanish
- 🇨🇳 Chinese
- 🇻🇳 Vietnamese
- 🇰🇷 Korean
- 🇸🇦 Arabic
- 🇫🇷 French
- 🇵🇹 Portuguese
- 🇩🇪 German

---

## 🛡️ Privacy & Compliance

- **FERPA Compliant** — Student data never leaves Snowflake
- **No External APIs** — All AI runs inside Snowflake Cortex
- **Role-Based Access** — Snowflake handles authentication
- **Audit Trail** — Raw payloads preserved in landing tables

---

## 📜 License

MIT License

---

<div align="center">

**Made with ❤️ for educators and students everywhere**

*Because every student deserves someone watching out for them.*

🎓 **GradSync** — Closing the gap between data and graduation.

</div>
