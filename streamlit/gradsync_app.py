"""
GradSync - Streamlit Native App
Closing the gap between data and graduation.

Three Data Entry Methods:
1. Bulk Upload (School Admin) - CSV/Excel file upload
2. Direct Entry (Teacher) - Quick observation logging via Hybrid Tables
3. Auto-Sync (IT) - Snowpipe integration (configured in SQL)
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import uuid
from datetime import datetime

# Initialize session
session = get_active_session()

# Page config
st.set_page_config(
    page_title="GradSync",
    page_icon="🎓",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .risk-critical { background-color: #ff4b4b; color: white; padding: 5px 10px; border-radius: 5px; }
    .risk-high { background-color: #ffa500; color: white; padding: 5px 10px; border-radius: 5px; }
    .risk-moderate { background-color: #ffd700; color: black; padding: 5px 10px; border-radius: 5px; }
    .risk-low { background-color: #90ee90; color: black; padding: 5px 10px; border-radius: 5px; }
    .metric-card { padding: 20px; border-radius: 10px; background: #f0f2f6; margin: 10px 0; }
</style>
""", unsafe_allow_html=True)

# Sidebar navigation
st.sidebar.title("🎓 GradSync")
page = st.sidebar.radio(
    "Navigation",
    ["📊 Dashboard", "📝 Log Observation", "📤 Bulk Upload", "🎯 Success Plans", "⚙️ Data Sync Status"]
)

# ============================================
# HELPER FUNCTIONS
# ============================================

@st.cache_data(ttl=300)
def get_students():
    """Fetch all students for dropdowns"""
    return session.sql("""
        SELECT student_id, first_name || ' ' || last_name as student_name, grade_level
        FROM RAW_DATA.STUDENTS
        ORDER BY last_name, first_name
    """).to_pandas()

@st.cache_data(ttl=60)
def get_at_risk_students():
    """Fetch at-risk students from Dynamic Table"""
    return session.sql("""
        SELECT * FROM ANALYTICS.AT_RISK_STUDENTS
        ORDER BY risk_score DESC
    """).to_pandas()

@st.cache_data(ttl=60)
def get_classroom_heatmap():
    """Fetch classroom summary data"""
    return session.sql("""
        SELECT * FROM ANALYTICS.CLASSROOM_HEATMAP
        ORDER BY grade_level
    """).to_pandas()

def analyze_sentiment(text):
    """Use Cortex to analyze sentiment of teacher note"""
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.SENTIMENT('{text.replace("'", "''")}') as sentiment
    """).collect()
    return float(result[0]['SENTIMENT'])

def generate_success_plan(student_data):
    """Use Cortex to generate personalized success plan"""
    prompt = f"""You are an educational advisor. Based on this student data, generate a specific, 
    actionable Success Plan for the educator. Keep it concise (3-4 bullet points).
    
    Student: {student_data['student_name']}
    Grade Level: {student_data['grade_level']}
    Attendance Rate: {student_data['attendance_rate']}%
    Current GPA: {student_data['current_gpa']}
    Risk Score: {student_data['risk_score']}
    Recent Sentiment: {student_data['avg_sentiment']}
    
    Provide specific interventions the teacher can implement this week."""
    
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', $${prompt}$$) as plan
    """).collect()
    return result[0]['PLAN']

def translate_message(text, target_language):
    """Use Cortex to translate parent communication"""
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.TRANSLATE($${text}$$, 'en', '{target_language}') as translated
    """).collect()
    return result[0]['TRANSLATED']


# ============================================
# PAGE: DASHBOARD
# ============================================

if page == "📊 Dashboard":
    st.title("📊 Student Risk Dashboard")
    st.caption("Real-time insights powered by Dynamic Tables & Cortex AI")
    
    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)
    
    try:
        metrics = session.sql("""
            SELECT 
                COUNT(*) as total_students,
                SUM(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) as critical,
                SUM(CASE WHEN risk_score >= 50 AND risk_score < 70 THEN 1 ELSE 0 END) as high,
                ROUND(AVG(attendance_rate), 1) as avg_attendance
            FROM ANALYTICS.STUDENT_360_VIEW
        """).collect()[0]
        
        col1.metric("Total Students", metrics['TOTAL_STUDENTS'])
        col2.metric("🔴 Critical Risk", metrics['CRITICAL'], delta=None)
        col3.metric("🟠 High Risk", metrics['HIGH'], delta=None)
        col4.metric("Avg Attendance", f"{metrics['AVG_ATTENDANCE']}%")
    except:
        st.info("Run the SQL setup scripts to populate data.")
    
    st.divider()
    
    # Risk Heatmap by Grade
    st.subheader("🗺️ Risk Heatmap by Grade Level")
    try:
        heatmap_df = get_classroom_heatmap()
        if not heatmap_df.empty:
            # Create visual heatmap
            for _, row in heatmap_df.iterrows():
                col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 2])
                col1.write(f"**Grade {row['GRADE_LEVEL']}**")
                col2.write(f"🔴 {row['CRITICAL_COUNT']}")
                col3.write(f"🟠 {row['HIGH_COUNT']}")
                col4.write(f"🟡 {row['MODERATE_COUNT']}")
                col5.progress(row['AVG_ATTENDANCE'] / 100, text=f"Attendance: {row['AVG_ATTENDANCE']}%")
    except Exception as e:
        st.warning(f"Heatmap data not available: {e}")
    
    st.divider()
    
    # At-Risk Students Table
    st.subheader("⚠️ Students Requiring Attention")
    try:
        at_risk_df = get_at_risk_students()
        if not at_risk_df.empty:
            # Add risk level badges
            def risk_badge(level):
                colors = {'Critical': '🔴', 'High': '🟠', 'Moderate': '🟡', 'Low': '🟢'}
                return f"{colors.get(level, '⚪')} {level}"
            
            display_df = at_risk_df[['STUDENT_NAME', 'GRADE_LEVEL', 'ATTENDANCE_RATE', 
                                      'CURRENT_GPA', 'RISK_SCORE', 'RISK_LEVEL']].copy()
            display_df['RISK_LEVEL'] = display_df['RISK_LEVEL'].apply(risk_badge)
            
            st.dataframe(
                display_df,
                column_config={
                    "STUDENT_NAME": "Student",
                    "GRADE_LEVEL": "Grade",
                    "ATTENDANCE_RATE": st.column_config.ProgressColumn("Attendance", min_value=0, max_value=100),
                    "CURRENT_GPA": st.column_config.NumberColumn("GPA", format="%.1f"),
                    "RISK_SCORE": st.column_config.ProgressColumn("Risk Score", min_value=0, max_value=100),
                    "RISK_LEVEL": "Status"
                },
                hide_index=True,
                use_container_width=True
            )
    except Exception as e:
        st.info("No at-risk students found or data not yet loaded.")

# ============================================
# PAGE: LOG OBSERVATION (Direct Entry - Hybrid Table)
# ============================================

elif page == "📝 Log Observation":
    st.title("📝 Log Student Observation")
    st.caption("Quick note entry with instant AI sentiment analysis")
    
    # The "Smart Form" - Few clicks UX
    with st.form("observation_form", clear_on_submit=True):
        col1, col2 = st.columns(2)
        
        with col1:
            # Student selector
            try:
                students_df = get_students()
                student_options = dict(zip(
                    students_df['STUDENT_NAME'], 
                    students_df['STUDENT_ID']
                ))
                selected_student = st.selectbox(
                    "Select Student",
                    options=list(student_options.keys()),
                    placeholder="Choose a student..."
                )
            except:
                selected_student = st.text_input("Student ID")
                student_options = {selected_student: selected_student}
        
        with col2:
            category = st.selectbox(
                "Category",
                ["Academic", "Behavioral", "Social", "Health", "General"]
            )
        
        # Note input with voice-to-text pitch
        note_text = st.text_area(
            "Observation Note",
            placeholder="Enter your observation about the student...",
            height=150,
            help="💡 Future: Voice-to-text with Cortex AI summarization"
        )
        
        # Optional: Show what voice-to-text could look like
        with st.expander("🎤 Voice-to-Text (Coming Soon)"):
            st.info("""
            **Pitch Feature:** Teachers could dictate notes, and Cortex AI would:
            1. Transcribe the audio
            2. Summarize key points
            3. Extract sentiment automatically
            
            *"Student seemed distracted today, wasn't participating in group work..."*
            → AI Summary: "Engagement concerns noted during collaborative activities"
            """)
        
        submitted = st.form_submit_button("💾 Save Observation", use_container_width=True)
        
        if submitted and note_text and selected_student:
            with st.spinner("Analyzing sentiment & saving..."):
                try:
                    # Analyze sentiment using Cortex
                    sentiment = analyze_sentiment(note_text)
                    
                    # Insert into Hybrid Table (fast OLTP write)
                    student_id = student_options.get(selected_student, selected_student)
                    session.sql(f"""
                        INSERT INTO APP.TEACHER_NOTES 
                        (student_id, teacher_id, note_text, note_category, sentiment_score)
                        VALUES (
                            '{student_id}',
                            'CURRENT_USER',
                            $${note_text}$$,
                            '{category}',
                            {sentiment}
                        )
                    """).collect()
                    
                    # Show success with sentiment feedback
                    sentiment_emoji = "😊" if sentiment > 0.3 else "😐" if sentiment > -0.3 else "😟"
                    st.success(f"✅ Observation saved! Sentiment: {sentiment_emoji} ({sentiment:.2f})")
                    
                    # Trigger Dynamic Table awareness
                    st.info("📊 Dashboard will update within 1 hour (Dynamic Table refresh)")
                    
                except Exception as e:
                    st.error(f"Error saving note: {e}")
    
    # Recent notes preview
    st.divider()
    st.subheader("📋 Recent Observations")
    try:
        recent_notes = session.sql("""
            SELECT 
                s.first_name || ' ' || s.last_name as student_name,
                n.note_category,
                n.note_text,
                n.sentiment_score,
                n.created_at
            FROM APP.TEACHER_NOTES n
            JOIN RAW_DATA.STUDENTS s ON n.student_id = s.student_id
            ORDER BY n.created_at DESC
            LIMIT 5
        """).to_pandas()
        
        if not recent_notes.empty:
            for _, note in recent_notes.iterrows():
                sentiment_emoji = "😊" if note['SENTIMENT_SCORE'] > 0.3 else "😐" if note['SENTIMENT_SCORE'] > -0.3 else "😟"
                with st.container():
                    st.markdown(f"**{note['STUDENT_NAME']}** - {note['NOTE_CATEGORY']} {sentiment_emoji}")
                    st.caption(note['NOTE_TEXT'][:200] + "..." if len(note['NOTE_TEXT']) > 200 else note['NOTE_TEXT'])
                    st.caption(f"_{note['CREATED_AT']}_")
                    st.divider()
    except:
        st.info("No recent observations found.")


# ============================================
# PAGE: BULK UPLOAD (School Admin)
# ============================================

elif page == "📤 Bulk Upload":
    st.title("📤 Bulk Data Upload")
    st.caption("Import data from Canvas, PowerSchool, or any CSV/Excel export")
    
    # Data type selector
    data_type = st.selectbox(
        "What type of data are you uploading?",
        ["students", "attendance", "grades"],
        format_func=lambda x: {"students": "📚 Student Roster", "attendance": "📅 Attendance Records", "grades": "📝 Grade Data"}[x]
    )
    
    # Show expected format
    with st.expander("📋 Expected File Format"):
        if data_type == "students":
            st.markdown("""
            | student_id | first_name | last_name | grade_level | enrollment_date | parent_email |
            |------------|------------|-----------|-------------|-----------------|--------------|
            | STU001 | John | Doe | 9 | 2024-08-15 | parent@email.com |
            """)
        elif data_type == "attendance":
            st.markdown("""
            | student_id | date | status | period |
            |------------|------|--------|--------|
            | STU001 | 2024-12-01 | Present | 1 |
            | STU001 | 2024-12-01 | Tardy | 2 |
            """)
        else:
            st.markdown("""
            | student_id | course | assignment | score | max_score | date |
            |------------|--------|------------|-------|-----------|------|
            | STU001 | Algebra I | Quiz 1 | 85 | 100 | 2024-12-01 |
            """)
    
    # File uploader (Streamlit in Snowflake feature!)
    uploaded_file = st.file_uploader(
        "Drag and drop your file here",
        type=['csv', 'xlsx'],
        help="Supports CSV and Excel files exported from Canvas, PowerSchool, etc."
    )
    
    if uploaded_file:
        # Preview the data
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(uploaded_file)
            else:
                df = pd.read_excel(uploaded_file)
            
            st.subheader("📊 Data Preview")
            st.dataframe(df.head(10), use_container_width=True)
            st.caption(f"Total rows: {len(df)}")
            
            # Validation
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Rows to Import", len(df))
            with col2:
                st.metric("Columns Detected", len(df.columns))
            
            # Import button
            if st.button("🚀 Import Data", type="primary", use_container_width=True):
                with st.spinner("Processing upload..."):
                    try:
                        batch_id = str(uuid.uuid4())[:8]
                        
                        # Convert DataFrame to staging table
                        for _, row in df.iterrows():
                            row_json = row.to_json()
                            session.sql(f"""
                                INSERT INTO RAW_DATA.BULK_UPLOAD_STAGING 
                                (upload_batch, uploaded_by, data_type, raw_data)
                                VALUES ('{batch_id}', CURRENT_USER(), '{data_type}', PARSE_JSON($${row_json}$$))
                            """).collect()
                        
                        # Call processing procedure
                        result = session.sql(f"""
                            CALL RAW_DATA.PROCESS_BULK_UPLOAD('{data_type}', '{batch_id}')
                        """).collect()
                        
                        st.success(f"✅ Successfully imported {len(df)} records!")
                        st.balloons()
                        
                        # Clear cache to refresh data
                        st.cache_data.clear()
                        
                    except Exception as e:
                        st.error(f"Import failed: {e}")
                        
        except Exception as e:
            st.error(f"Error reading file: {e}")

# ============================================
# PAGE: SUCCESS PLANS (AI-Generated)
# ============================================

elif page == "🎯 Success Plans":
    st.title("🎯 AI-Powered Success Plans")
    st.caption("Personalized intervention strategies generated by Cortex AI")
    
    try:
        at_risk_df = get_at_risk_students()
        
        if not at_risk_df.empty:
            # Student selector
            selected = st.selectbox(
                "Select a student to generate Success Plan",
                options=at_risk_df['STUDENT_NAME'].tolist(),
                index=0
            )
            
            student_data = at_risk_df[at_risk_df['STUDENT_NAME'] == selected].iloc[0].to_dict()
            
            # Student summary card
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Risk Score", f"{student_data['RISK_SCORE']}")
            col2.metric("Attendance", f"{student_data['ATTENDANCE_RATE']}%")
            col3.metric("GPA", f"{student_data['CURRENT_GPA']:.1f}")
            col4.metric("Sentiment", f"{student_data['AVG_SENTIMENT']:.2f}")
            
            st.divider()
            
            # Generate Success Plan
            if st.button("🤖 Generate Success Plan", type="primary", use_container_width=True):
                with st.spinner("Cortex AI is analyzing student data..."):
                    try:
                        plan = generate_success_plan(student_data)
                        
                        st.subheader("📋 Recommended Success Plan")
                        st.markdown(plan)
                        
                        # One-click outreach
                        st.divider()
                        st.subheader("📧 One-Click Parent Outreach")
                        
                        parent_language = student_data.get('PARENT_LANGUAGE', 'English')
                        
                        default_message = f"""Dear Parent/Guardian,

I wanted to reach out regarding {selected}'s progress in class. I've noticed some areas where additional support could be beneficial, and I'd like to schedule a brief meeting to discuss strategies we can implement together.

Please let me know your availability this week.

Best regards,
[Teacher Name]"""
                        
                        message = st.text_area("Email Message", value=default_message, height=200)
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            if st.button("📤 Send Email (English)"):
                                st.success("✅ Email drafted! (Integration with school email system)")
                        
                        with col2:
                            if parent_language != 'English':
                                if st.button(f"🌐 Translate & Send ({parent_language})"):
                                    translated = translate_message(message, parent_language[:2].lower())
                                    st.text_area("Translated Message", value=translated, height=200)
                                    st.success(f"✅ Translated to {parent_language}!")
                                    
                    except Exception as e:
                        st.error(f"Error generating plan: {e}")
        else:
            st.info("No at-risk students found. Great news! 🎉")
            
    except Exception as e:
        st.warning("Run the SQL setup scripts to enable Success Plans.")

# ============================================
# PAGE: DATA SYNC STATUS (IT Admin)
# ============================================

elif page == "⚙️ Data Sync Status":
    st.title("⚙️ Data Sync Status")
    st.caption("Monitor Snowpipe ingestion and Dynamic Table refresh status")
    
    # Sync method overview
    st.subheader("📡 Active Data Pipelines")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        ### 📤 Bulk Upload
        **Status:** ✅ Active
        
        School admins can upload CSV/Excel files directly through the app.
        
        *Last upload: Just now*
        """)
    
    with col2:
        st.markdown("""
        ### 📝 Direct Entry
        **Status:** ✅ Active
        
        Teachers log observations via Hybrid Tables for instant writes.
        
        *Latency: <100ms*
        """)
    
    with col3:
        st.markdown("""
        ### 🔄 Auto-Sync (Snowpipe)
        **Status:** ⚙️ Configured
        
        IT can enable automatic ingestion from district systems.
        
        *Refresh: Continuous*
        """)
    
    st.divider()
    
    # Dynamic Table status
    st.subheader("📊 Dynamic Table Refresh Status")
    
    try:
        # Check Dynamic Table status
        dt_status = session.sql("""
            SELECT 
                name,
                target_lag,
                refresh_mode,
                scheduling_state
            FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
            WHERE schema_name = 'ANALYTICS'
        """).to_pandas()
        
        if not dt_status.empty:
            st.dataframe(dt_status, use_container_width=True)
        else:
            st.info("Dynamic Tables not yet created. Run SQL setup scripts.")
    except:
        st.info("Dynamic Table status requires ANALYTICS schema setup.")
    
    st.divider()
    
    # Data quality metrics
    st.subheader("🔍 Data Quality Metrics")
    
    try:
        quality = session.sql("""
            SELECT 
                'Students' as table_name,
                COUNT(*) as row_count,
                SUM(CASE WHEN student_id IS NULL THEN 1 ELSE 0 END) as null_ids
            FROM RAW_DATA.STUDENTS
            UNION ALL
            SELECT 
                'Attendance',
                COUNT(*),
                SUM(CASE WHEN student_id IS NULL THEN 1 ELSE 0 END)
            FROM RAW_DATA.ATTENDANCE
            UNION ALL
            SELECT 
                'Grades',
                COUNT(*),
                SUM(CASE WHEN student_id IS NULL THEN 1 ELSE 0 END)
            FROM RAW_DATA.GRADES
            UNION ALL
            SELECT 
                'Teacher Notes',
                COUNT(*),
                SUM(CASE WHEN student_id IS NULL THEN 1 ELSE 0 END)
            FROM APP.TEACHER_NOTES
        """).to_pandas()
        
        st.dataframe(quality, use_container_width=True)
    except:
        st.info("Run SQL setup scripts to view data quality metrics.")
    
    # Snowpipe configuration guide
    with st.expander("🔧 Snowpipe Configuration Guide"):
        st.markdown("""
        ### For IT Administrators
        
        To enable automatic data sync from your district's central database:
        
        1. **Create External Stage** pointing to your S3/Azure/GCS bucket
        2. **Configure Snowpipe** with AUTO_INGEST = TRUE
        3. **Set up event notifications** from your cloud provider
        
        ```sql
        -- Example: Enable the attendance pipe
        ALTER PIPE ATTENDANCE_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;
        ```
        
        See `sql/03_snowpipe_auto_sync.sql` for full configuration.
        """)
