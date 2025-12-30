# Design Document: AI-Powered Risk Detection

## Overview

Enhanced GradSync with advanced Cortex AI capabilities to detect hidden risks in unstructured educational data, provide explainable risk scoring, and enable proactive interventions.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    GRADSYNC AI ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │ Teacher Note │───▶│   CORTEX     │───▶│  Classified  │       │
│  │   Input      │    │ CLASSIFY_TEXT│    │    Note      │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │  Historical  │───▶│   CORTEX     │───▶│   Pattern    │       │
│  │    Notes     │    │   COMPLETE   │    │   Analysis   │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│                              │                                   │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │              MULTI-FACTOR RISK ENGINE                    │    │
│  │  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐        │    │
│  │  │Attendance│ │ Grades  │ │Sentiment│ │AI Signals│       │    │
│  │  │  (25%)   │ │  (25%)  │ │  (25%)  │ │  (25%)  │        │    │
│  │  └─────────┘ └─────────┘ └─────────┘ └─────────┘        │    │
│  └─────────────────────────────────────────────────────────┘    │
│                              │                                   │
│                              ▼                                   │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │  Risk Score  │───▶│  Explainable │───▶│  Success     │       │
│  │  0-100       │    │  Breakdown   │    │  Plan        │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Components

### 1. Note Classification Engine

Uses `CORTEX.CLASSIFY_TEXT` to automatically categorize teacher observations:

```sql
SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
    note_text,
    ['Academic Struggle', 'Behavioral Concern', 'Social-Emotional Risk', 
     'Attendance Pattern', 'Family Situation', 'Positive Progress']
) as ai_classification
FROM APP.TEACHER_NOTES;
```

### 2. Pattern Detection Engine

Uses `CORTEX.COMPLETE` to analyze multiple notes and detect hidden patterns:

```sql
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large',
    'Analyze these teacher notes for a student and identify any concerning patterns:
     Note 1: [note_text_1]
     Note 2: [note_text_2]
     Note 3: [note_text_3]
     
     Identify: 1) Recurring themes 2) Escalating concerns 3) Hidden risks'
) as pattern_analysis;
```

### 3. Multi-Factor Risk Score Algorithm

```
RISK_SCORE = 
    (100 - attendance_rate) * 0.25 +           -- Attendance Factor
    (100 - normalized_gpa) * 0.25 +            -- Academic Factor
    (sentiment_risk) * 0.25 +                   -- Sentiment Factor
    (ai_signal_score) * 0.25                    -- AI-Detected Signals

Where:
- attendance_rate: 0-100 (higher = better)
- normalized_gpa: 0-100 (GPA scaled to percentage)
- sentiment_risk: 0-100 (based on avg sentiment, negative = higher risk)
- ai_signal_score: 0-100 (based on classified note severity)
```

### 4. Early Warning Indicators

| Indicator | Threshold | Weight |
|-----------|-----------|--------|
| Attendance drop | >5% in 2 weeks | +15 risk points |
| Grade decline | >10% in 30 days | +15 risk points |
| Negative sentiment trend | >0.3 drop | +10 risk points |
| Multiple concerning notes | 3+ in 30 days | +20 risk points |
| Late/missing assignments | 3+ in 2 weeks | +10 risk points |

## Data Models

### Enhanced TEACHER_NOTES Table

```sql
ALTER TABLE APP.TEACHER_NOTES ADD COLUMN ai_classification VARCHAR(100);
ALTER TABLE APP.TEACHER_NOTES ADD COLUMN ai_confidence FLOAT;
ALTER TABLE APP.TEACHER_NOTES ADD COLUMN risk_signals VARIANT;
```

### New AI_INSIGHTS Table

```sql
CREATE TABLE APP.AI_INSIGHTS (
    insight_id INT AUTOINCREMENT PRIMARY KEY,
    student_id VARCHAR(20),
    insight_type VARCHAR(50),  -- 'pattern', 'early_warning', 'trend'
    insight_text VARCHAR(2000),
    confidence_score FLOAT,
    contributing_factors VARIANT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
```

### New RISK_BREAKDOWN View

```sql
CREATE VIEW ANALYTICS.RISK_BREAKDOWN AS
SELECT 
    student_id,
    student_name,
    risk_score,
    attendance_contribution,
    grade_contribution,
    sentiment_contribution,
    ai_signal_contribution,
    top_risk_factors,
    ai_explanation
FROM ANALYTICS.STUDENT_360_VIEW;
```

## Cortex AI Functions Used

| Function | Purpose | Example Use |
|----------|---------|-------------|
| `SENTIMENT` | Analyze note tone | Detect negative teacher observations |
| `COMPLETE` | Generate insights | Pattern analysis, Success Plans |
| `CLASSIFY_TEXT` | Categorize notes | Auto-tag concern types |
| `TRANSLATE` | Parent outreach | Multi-language communication |

## UI Components

### 1. Risk Breakdown Card
- Pie chart showing factor contributions
- Color-coded risk indicators
- "Why this score?" expandable section

### 2. AI Insights Panel
- Pattern detection summary
- Early warning alerts
- Trend visualizations

### 3. Watch List Dashboard
- Students predicted to become at-risk
- Recommended actions
- One-click intervention buttons

## Error Handling

- If Cortex AI is unavailable, fall back to rule-based classification
- Cache AI results to reduce API calls
- Show "AI analysis pending" for new notes

## Testing Strategy

1. Test classification accuracy with sample notes
2. Verify risk score calculations match expected values
3. Test translation for Spanish, Chinese, Korean
4. Load test with 1000+ notes
