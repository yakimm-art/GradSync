# GradSync 🎓

**Closing the gap between data and graduation.**

> *AI for Good Hackathon Entry — Assisting teachers with attendance and performance insights*

A Snowflake Native App that proactively identifies at-risk students using AI-driven interventions and provides actionable Success Plans for educators.

---

## 🎯 The Problem

Most schools have **"Dead Data."** Grades and attendance sit in spreadsheets, legacy databases, and disconnected systems. By the time a human notices a student is failing, it's often too late to help.

**The stats are alarming:**
- 1.2 million students drop out of high school annually in the US
- Early warning signs are often visible in data months before dropout
- Teachers lack tools to spot subtle patterns across multiple data sources

## 💡 The Solution

GradSync brings modern data infrastructure to an old problem. By consolidating school data into Snowflake and applying AI-driven analysis, it:

1. **Detects risk early** — Combines attendance, grades, AND teacher note sentiment to catch warning signs
2. **Explains why** — Not just a score, but human-readable reasons ("Attendance dropped 15% + negative sentiment in recent notes")
3. **Provides action** — AI-generated Success Plans with specific interventions
4. **Bridges language gaps** — Translates parent outreach into 8+ languages

**A teacher can identify an at-risk student and have a translated parent email ready in under 3 clicks.**

---

## 🏆 Hackathon Alignment

| Requirement | GradSync |
|-------------|----------|
| **AI for Good use-case** | ✅ Education — helping at-risk students graduate |
| **Runs entirely inside Snowflake** | ✅ 100% — no external APIs, all data stays secure |
| **Clearly defined dataset** | ✅ Sample data included (`sql/04_sample_data.sql`) |
| **Snowflake Intelligence adds value** | ✅ Cortex SENTIMENT, COMPLETE, TRANSLATE |
| **Simple for non-technical users** | ✅ Teachers get insights in 2-3 clicks |
| **Privacy-safe** | ✅ FERPA compliant — student data never leaves Snowflake |

---

## ⚡ Snowflake Features Used

| Feature | How We Use It |
|---------|---------------|
| **Streamlit in Snowflake** | Teacher-friendly UI, zero installation |
| **Dynamic Tables** | Auto-refreshing Student 360 View |
| **Hybrid Tables (Unistore)** | Sub-100ms teacher note saves |
| **Snowpipe** | Auto-ingest from district systems |
| **Streams + Tasks** | Real-time event processing |
| **Cortex SENTIMENT** | Analyze teacher notes for risk signals |
| **Cortex COMPLETE** | Generate personalized Success Plans |
| **Cortex TRANSLATE** | Parent outreach in native language |

---

## 🖥️ Demo Walkthrough

### 1. Overview — See At-Risk Students Instantly
![Overview](docs/screenshots/overview.png)
- Key metrics at a glance (total students, critical risk count, avg attendance)
- Priority list of students needing attention
- Recent teacher activity feed

### 2. Analytics — Understand the Patterns
- Risk distribution by severity level
- Attendance trends by grade
- GPA distribution across the school

### 3. Log Observations — AI Analyzes Sentiment
- Teacher writes a note about a student
- Cortex SENTIMENT scores it instantly (-1 to +1)
- Negative sentiment contributes to risk score

### 4. Success Plans — AI Generates Interventions
- Select an at-risk student
- Click "Generate Success Plan"
- Cortex COMPLETE creates specific, actionable recommendations
- Translate parent message to Spanish, Chinese, Vietnamese, etc.

### 5. Import Data — Multiple Entry Points
- **Bulk Upload**: Drag-and-drop CSV/Excel from Canvas or PowerSchool
- **Direct Entry**: Real-time teacher notes via Hybrid Tables
- **Auto-Sync**: Snowpipe ingests from district systems automatically

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

### Data Flow

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Bulk Upload    │────▶│  Staging Table   │────▶│  Raw Tables     │
│  (CSV/Excel)    │     │  + Procedure     │     │                 │
└─────────────────┘     └──────────────────┘     └────────┬────────┘
                                                          │
┌─────────────────┐     ┌──────────────────┐              ▼
│  Teacher Note   │────▶│  Hybrid Table    │────▶┌─────────────────┐
│  (Direct Entry) │     │  (<100ms write)  │     │ Dynamic Tables  │
└─────────────────┘     └──────────────────┘     │ (Auto-Refresh)  │
                                                  └────────┬────────┘
┌─────────────────┐     ┌──────────────────┐              │
│  District DB    │────▶│  Snowpipe        │──────────────┘
│  (Auto-Sync)    │     │  + Stream/Task   │              ▼
└─────────────────┘     └──────────────────┘     ┌─────────────────┐
                                                  │  Cortex AI      │
                                                  │  Risk Detection │
                                                  │  Success Plans  │
                                                  │  Translation    │
                                                  └─────────────────┘
```

---

## 🚀 Quick Start

### Prerequisites
- Snowflake account with Cortex AI enabled
- Streamlit in Snowflake access

### Installation (5 minutes)

1. **Clone the repo**
   ```bash
   git clone https://github.com/your-org/gradsync.git
   cd gradsync
   ```

2. **Run SQL scripts in Snowflake** (in order)
   ```sql
   -- 1. Create database and tables
   -- Run: sql/01_setup_database.sql
   
   -- 2. Create Dynamic Tables for analytics
   -- Run: sql/02_dynamic_tables.sql
   
   -- 3. Load sample data
   -- Run: sql/04_sample_data.sql
   ```

3. **Deploy Streamlit app**
   - Upload `streamlit/gradsync_app.py` to Snowflake Streamlit
   - Set database context to `GRADSYNC_DB`

4. **Open the app and explore!**

---

## 📊 Sample Data

The demo includes realistic sample data:
- **10 students** across grades 9-12
- **100 attendance records** with various statuses
- **12 grade entries** across multiple courses
- **Teacher notes** with positive/negative sentiment examples

All data is synthetic — no real student information.

---

## 🔑 Key Differentiators

### Beyond Dashboards
GradSync isn't just a read-only dashboard. It **writes back** to Snowflake:
- Teacher notes saved to Hybrid Tables in real-time
- Bulk uploads processed into normalized tables
- Auto-sync ingests external data continuously

### Explainable AI
Not a black-box risk score. Teachers see:
- Which factors contributed to the score
- Specific attendance patterns
- Sentiment trends in notes
- AI-generated explanations

### Actionable Output
Every insight leads to action:
- Risk detected → Success Plan generated
- Language barrier → Auto-translate message
- Pattern spotted → Specific intervention recommended

---

## 📁 Project Structure

```
gradsync/
├── streamlit/
│   └── gradsync_app.py          # Main application (1200+ lines)
├── sql/
│   ├── 01_setup_database.sql    # Schema + tables
│   ├── 02_dynamic_tables.sql    # Analytics views
│   ├── 03_snowpipe_auto_sync.sql # Auto-ingestion config
│   ├── 04_sample_data.sql       # Demo data
│   └── ...
├── tests/
│   └── test_snowpipe_properties.py  # Property-based tests
├── test_data/
│   └── snowpipe_samples/        # JSON test files
└── README.md
```

---

## 🧪 Testing

Property-based tests validate correctness:

```bash
pip install pytest hypothesis
pytest tests/test_snowpipe_properties.py -v
```

Tests cover:
- ✅ Raw payload preservation
- ✅ Event type mapping (check_in → Present, no_show → Absent)
- ✅ Processing idempotency (no duplicates)
- ✅ Malformed JSON rejection

---

## 🌍 Real-World Impact

| Metric | Impact |
|--------|--------|
| **Time to identify at-risk student** | Days → Seconds |
| **Languages supported for parent outreach** | 1 → 8+ |
| **Data sources unified** | Fragmented → Single view |
| **Teacher clicks to action** | Many → 3 |

---

## 🛡️ Privacy & Compliance

- **FERPA Compliant** — Student data never leaves Snowflake
- **No External APIs** — All AI runs inside Snowflake Cortex
- **Role-Based Access** — Snowflake handles authentication
- **Audit Trail** — Raw payloads preserved in landing tables

---

## 📜 License

MIT License — see [LICENSE](LICENSE) for details.

---

<div align="center">

**Made with ❤️ for educators and students everywhere**

*Because every student deserves someone watching out for them.*

</div>
