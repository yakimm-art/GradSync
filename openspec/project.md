# Project Context

## Purpose
GradSync is a Snowflake Native App that proactively identifies at-risk students using AI-driven interventions and provides actionable Success Plans for educators. The goal is to "close the gap between data and graduation" by transforming dead data into timely action.

## Tech Stack
- **Database**: Snowflake (Dynamic Tables, Hybrid Tables, Snowpipe)
- **Frontend**: Streamlit in Snowflake
- **AI**: Snowflake Cortex (SENTIMENT, COMPLETE, TRANSLATE)
- **Data Formats**: CSV, Excel, JSON (for API ingestion)

## Project Conventions

### Code Style
- SQL: Uppercase keywords, snake_case for identifiers
- Python: PEP 8, type hints where practical
- Use `session.sql()` for Snowpark queries in Streamlit

### Architecture Patterns
- **Three Entry Methods**: Bulk Upload, Direct Entry, Auto-Sync
- **Hybrid Tables**: For OLTP workloads (teacher notes)
- **Dynamic Tables**: For analytics with auto-refresh
- **Snowpipe + Streams**: For continuous ingestion

### Testing Strategy
- Sample data in `sql/04_sample_data.sql` for demos
- Data Metric Functions for quality monitoring
- Manual validation via Streamlit UI

### Git Workflow
- Feature branches for new capabilities
- OpenSpec proposals for breaking changes
- SQL scripts numbered for execution order

## Domain Context
- **FERPA Compliance**: Student data must stay within Snowflake
- **Risk Score**: 0-100 scale (higher = more at risk)
- **Sentiment Analysis**: -1 to +1 scale from Cortex
- **Target Users**: Teachers, School Admins, IT Departments

## Important Constraints
- All data processing must occur inside Snowflake (no external APIs)
- Sub-100ms latency required for teacher note entry (Hybrid Tables)
- Dynamic Tables refresh within 1 hour for dashboard updates
- Must work with existing school systems (Canvas, PowerSchool exports)

## External Dependencies
- Snowflake Cortex AI (mistral-large model)
- Streamlit in Snowflake runtime
- School district data exports (CSV/Excel)
