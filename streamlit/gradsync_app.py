"""
GradSync - Streamlit Native App
Closing the gap between data and graduation.
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import uuid

# Initialize session
session = get_active_session()

# Page config
st.set_page_config(
    page_title="GradSync",
    page_icon="🎓",
    layout="wide"
)

# Enhanced Custom CSS
st.markdown("""
<style>
    /* Main header styling */
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 15px;
        color: white;
        margin-bottom: 2rem;
        text-align: center;
    }
    .main-header h1 {
        margin: 0;
        font-size: 2.5rem;
    }
    .main-header p {
        margin: 0.5rem 0 0 0;
        opacity: 0.9;
    }
    
    /* Metric cards */
    .metric-card {
        background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
        padding: 1.5rem;
        border-radius: 12px;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .metric-card.critical {
        background: linear-gradient(135deg, #ff6b6b 0%, #ee5a5a 100%);
        color: white;
    }
    .metric-card.warning {
        background: linear-gradient(135deg, #ffa502 0%, #ff7f50 100%);
        color: white;
    }
    .metric-card.success {
        background: linear-gradient(135deg, #26de81 0%, #20bf6b 100%);
        color: white;
    }
    
    /* Student cards */
    .student-card {
        background: white;
        border-radius: 12px;
        padding: 1rem;
        margin: 0.5rem 0;
        border-left: 4px solid #667eea;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .student-card.high-risk {
        border-left-color: #ff6b6b;
        background: #fff5f5;
    }
    .student-card.medium-risk {
        border-left-color: #ffa502;
        background: #fff9e6;
    }
    
    /* Risk badges */
    .risk-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
    }
    .risk-critical { background: #ff6b6b; color: white; }
    .risk-high { background: #ffa502; color: white; }
    .risk-moderate { background: #ffd93d; color: #333; }
    .risk-low { background: #26de81; color: white; }
    
    /* Sidebar styling */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #1a1a2e 0%, #16213e 100%);
    }
    [data-testid="stSidebar"] .stRadio label {
        color: white !important;
    }
    
    /* Charts container */
    .chart-container {
        background: white;
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
    }
    
    /* Success plan card */
    .plan-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 12px;
        margin: 1rem 0;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
</style>
""", unsafe_allow_html=True)

# Sidebar
with st.sidebar:
    st.markdown("## 🎓 GradSync")
    st.markdown("*Closing the gap between data and graduation*")
    st.divider()
    
    page = st.radio(
        "Navigation",
        ["📊 Dashboard", "📝 Log Observation", "📤 Bulk Upload", "🎯 Success Plans", "⚙️ Settings"],
        label_visibility="collapsed"
    )
    
    st.divider()
    st.markdown("### 📈 Quick Stats")
    try:
        quick_stats = session.sql("""
            SELECT COUNT(*) as total FROM RAW_DATA.STUDENTS
        """).collect()[0]
        st.metric("Students Tracked", quick_stats['TOTAL'])
    except:
        st.metric("Students Tracked", "—")

# ============================================
# HELPER FUNCTIONS
# ============================================

@st.cache_data(ttl=300)
def get_students():
    return session.sql("""
        SELECT student_id, first_name || ' ' || last_name as student_name, grade_level
        FROM RAW_DATA.STUDENTS ORDER BY last_name, first_name
    """).to_pandas()

@st.cache_data(ttl=60)
def get_at_risk_students():
    return session.sql("""
        SELECT * FROM ANALYTICS.AT_RISK_STUDENTS ORDER BY risk_score DESC
    """).to_pandas()

@st.cache_data(ttl=60)
def get_classroom_heatmap():
    return session.sql("""
        SELECT * FROM ANALYTICS.CLASSROOM_HEATMAP ORDER BY grade_level
    """).to_pandas()

@st.cache_data(ttl=60)
def get_all_students_360():
    return session.sql("""
        SELECT * FROM ANALYTICS.STUDENT_360_VIEW ORDER BY risk_score DESC
    """).to_pandas()

def analyze_sentiment(text):
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.SENTIMENT('{text.replace("'", "''")}') as sentiment
    """).collect()
    return float(result[0]['SENTIMENT'])

def generate_success_plan(student_data):
    prompt = f"""You are an educational advisor. Generate a specific, actionable Success Plan (3-4 bullet points).
    
    Student: {student_data['STUDENT_NAME']}
    Grade: {student_data['GRADE_LEVEL']} | Attendance: {student_data['ATTENDANCE_RATE']}%
    GPA: {student_data['CURRENT_GPA']} | Risk Score: {student_data['RISK_SCORE']}
    Sentiment: {student_data['AVG_SENTIMENT']}
    
    Provide interventions the teacher can implement this week."""
    
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', $${prompt}$$) as plan
    """).collect()
    return result[0]['PLAN']

def translate_message(text, target_language):
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.TRANSLATE($${text}$$, 'en', '{target_language}') as translated
    """).collect()
    return result[0]['TRANSLATED']


# ============================================
# PAGE: DASHBOARD
# ============================================

if page == "📊 Dashboard":
    # Header
    st.markdown("""
    <div class="main-header">
        <h1>🎓 Student Risk Dashboard</h1>
        <p>Real-time insights powered by Dynamic Tables & Cortex AI</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Key Metrics Row
    try:
        metrics = session.sql("""
            SELECT 
                COUNT(*) as total_students,
                SUM(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) as critical,
                SUM(CASE WHEN risk_score >= 50 AND risk_score < 70 THEN 1 ELSE 0 END) as high,
                SUM(CASE WHEN risk_score >= 30 AND risk_score < 50 THEN 1 ELSE 0 END) as moderate,
                ROUND(AVG(attendance_rate), 1) as avg_attendance,
                ROUND(AVG(current_gpa), 1) as avg_gpa
            FROM ANALYTICS.STUDENT_360_VIEW
        """).collect()[0]
        
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.metric("📚 Total Students", metrics['TOTAL_STUDENTS'])
        with col2:
            st.metric("🔴 Critical", metrics['CRITICAL'], delta=None, delta_color="inverse")
        with col3:
            st.metric("🟠 High Risk", metrics['HIGH'], delta=None, delta_color="inverse")
        with col4:
            st.metric("📊 Avg Attendance", f"{metrics['AVG_ATTENDANCE']}%")
        with col5:
            st.metric("📈 Avg GPA", f"{metrics['AVG_GPA']}")
            
    except Exception as e:
        st.error(f"Error loading metrics: {e}")
    
    st.divider()
    
    # Two column layout
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader("⚠️ Students Requiring Attention")
        
        try:
            at_risk_df = get_at_risk_students()
            
            if not at_risk_df.empty:
                for _, student in at_risk_df.iterrows():
                    risk_color = "high-risk" if student['RISK_SCORE'] >= 50 else "medium-risk"
                    risk_emoji = "🔴" if student['RISK_SCORE'] >= 70 else "🟠" if student['RISK_SCORE'] >= 50 else "🟡"
                    
                    with st.container():
                        st.markdown(f"""
                        <div class="student-card {risk_color}">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <div>
                                    <strong style="font-size: 1.1rem;">{risk_emoji} {student['STUDENT_NAME']}</strong>
                                    <span style="color: #666; margin-left: 10px;">Grade {student['GRADE_LEVEL']}</span>
                                </div>
                                <div>
                                    <span class="risk-badge risk-{student['RISK_LEVEL'].lower()}">{student['RISK_LEVEL']}</span>
                                </div>
                            </div>
                            <div style="margin-top: 0.5rem; display: flex; gap: 2rem; color: #555;">
                                <span>📊 Risk: <strong>{student['RISK_SCORE']}</strong></span>
                                <span>📅 Attendance: <strong>{student['ATTENDANCE_RATE']}%</strong></span>
                                <span>📚 GPA: <strong>{student['CURRENT_GPA']:.1f}</strong></span>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
            else:
                st.success("🎉 No at-risk students! All students are doing well.")
                
        except Exception as e:
            st.warning(f"Could not load at-risk students: {e}")
    
    with col_right:
        st.subheader("🗺️ Risk by Grade")
        
        try:
            heatmap_df = get_classroom_heatmap()
            
            if not heatmap_df.empty:
                for _, row in heatmap_df.iterrows():
                    grade = int(row['GRADE_LEVEL'])
                    critical = int(row['CRITICAL_COUNT'])
                    high = int(row['HIGH_COUNT'])
                    moderate = int(row['MODERATE_COUNT'])
                    
                    st.markdown(f"**Grade {grade}**")
                    
                    # Visual bar
                    cols = st.columns([1, 1, 1, 2])
                    cols[0].markdown(f"🔴 {critical}")
                    cols[1].markdown(f"🟠 {high}")
                    cols[2].markdown(f"🟡 {moderate}")
                    cols[3].progress(row['AVG_ATTENDANCE'] / 100)
                    
        except Exception as e:
            st.warning(f"Heatmap unavailable: {e}")
        
        st.divider()
        
        # Recent Activity
        st.subheader("📝 Recent Notes")
        try:
            recent = session.sql("""
                SELECT s.first_name, n.note_category, n.sentiment_score
                FROM APP.TEACHER_NOTES n
                JOIN RAW_DATA.STUDENTS s ON n.student_id = s.student_id
                ORDER BY n.created_at DESC LIMIT 3
            """).to_pandas()
            
            for _, note in recent.iterrows():
                emoji = "😊" if note['SENTIMENT_SCORE'] > 0.3 else "😐" if note['SENTIMENT_SCORE'] > -0.3 else "😟"
                st.markdown(f"{emoji} **{note['FIRST_NAME']}** - {note['NOTE_CATEGORY']}")
        except:
            st.info("No recent notes")

# ============================================
# PAGE: LOG OBSERVATION
# ============================================

elif page == "📝 Log Observation":
    st.markdown("""
    <div class="main-header">
        <h1>📝 Log Student Observation</h1>
        <p>Quick note entry with instant AI sentiment analysis</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        with st.form("observation_form", clear_on_submit=True):
            # Student selector
            try:
                students_df = get_students()
                student_options = dict(zip(students_df['STUDENT_NAME'], students_df['STUDENT_ID']))
                selected_student = st.selectbox("👤 Select Student", options=list(student_options.keys()))
            except:
                selected_student = st.text_input("Student ID")
                student_options = {selected_student: selected_student}
            
            category = st.selectbox("📁 Category", ["Academic", "Behavioral", "Social", "Health", "General"])
            
            note_text = st.text_area(
                "📝 Observation Note",
                placeholder="Enter your observation about the student...",
                height=150
            )
            
            submitted = st.form_submit_button("💾 Save & Analyze", use_container_width=True, type="primary")
            
            if submitted and note_text and selected_student:
                with st.spinner("🤖 Analyzing sentiment..."):
                    try:
                        sentiment = analyze_sentiment(note_text)
                        student_id = student_options.get(selected_student, selected_student)
                        
                        session.sql(f"""
                            INSERT INTO APP.TEACHER_NOTES 
                            (student_id, teacher_id, note_text, note_category, sentiment_score)
                            VALUES ('{student_id}', 'CURRENT_USER', $${note_text}$$, '{category}', {sentiment})
                        """).collect()
                        
                        sentiment_emoji = "😊" if sentiment > 0.3 else "😐" if sentiment > -0.3 else "😟"
                        st.success(f"✅ Saved! Sentiment: {sentiment_emoji} ({sentiment:.2f})")
                        st.cache_data.clear()
                        
                    except Exception as e:
                        st.error(f"Error: {e}")
    
    with col2:
        st.markdown("### 💡 Sentiment Guide")
        st.markdown("""
        | Emoji | Meaning | Score |
        |-------|---------|-------|
        | 😊 | Positive | > 0.3 |
        | 😐 | Neutral | -0.3 to 0.3 |
        | 😟 | Negative | < -0.3 |
        """)
        
        st.divider()
        
        st.markdown("### 📋 Recent Notes")
        try:
            recent_notes = session.sql("""
                SELECT s.first_name || ' ' || s.last_name as name, n.note_category, n.sentiment_score, n.created_at
                FROM APP.TEACHER_NOTES n
                JOIN RAW_DATA.STUDENTS s ON n.student_id = s.student_id
                ORDER BY n.created_at DESC LIMIT 5
            """).to_pandas()
            
            for _, note in recent_notes.iterrows():
                emoji = "😊" if note['SENTIMENT_SCORE'] > 0.3 else "😐" if note['SENTIMENT_SCORE'] > -0.3 else "😟"
                st.markdown(f"{emoji} **{note['NAME']}** - {note['NOTE_CATEGORY']}")
        except:
            st.info("No notes yet")


# ============================================
# PAGE: BULK UPLOAD
# ============================================

elif page == "📤 Bulk Upload":
    st.markdown("""
    <div class="main-header">
        <h1>📤 Bulk Data Upload</h1>
        <p>Import data from Canvas, PowerSchool, or any CSV/Excel export</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        data_type = st.selectbox(
            "📁 Data Type",
            ["students", "attendance", "grades"],
            format_func=lambda x: {"students": "📚 Student Roster", "attendance": "📅 Attendance Records", "grades": "📝 Grade Data"}[x]
        )
        
        uploaded_file = st.file_uploader(
            "Drag and drop your file here",
            type=['csv', 'xlsx'],
            help="Supports CSV and Excel files"
        )
        
        if uploaded_file:
            try:
                df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
                
                st.markdown("### 📊 Preview")
                st.dataframe(df.head(10), use_container_width=True)
                
                col_a, col_b = st.columns(2)
                col_a.metric("Rows", len(df))
                col_b.metric("Columns", len(df.columns))
                
                if st.button("🚀 Import Data", type="primary", use_container_width=True):
                    with st.spinner("Importing..."):
                        try:
                            batch_id = str(uuid.uuid4())[:8]
                            for _, row in df.iterrows():
                                row_json = row.to_json()
                                session.sql(f"""
                                    INSERT INTO RAW_DATA.BULK_UPLOAD_STAGING 
                                    (upload_batch, uploaded_by, data_type, raw_data)
                                    VALUES ('{batch_id}', CURRENT_USER(), '{data_type}', PARSE_JSON($${row_json}$$))
                                """).collect()
                            
                            st.success(f"✅ Imported {len(df)} records!")
                            st.balloons()
                            st.cache_data.clear()
                        except Exception as e:
                            st.error(f"Import failed: {e}")
            except Exception as e:
                st.error(f"Error reading file: {e}")
    
    with col2:
        st.markdown("### 📋 Expected Format")
        
        if data_type == "students":
            st.code("""student_id,first_name,last_name,grade_level
STU001,John,Doe,9
STU002,Jane,Smith,10""", language="csv")
        elif data_type == "attendance":
            st.code("""student_id,date,status,period
STU001,2024-12-01,Present,1
STU001,2024-12-01,Tardy,2""", language="csv")
        else:
            st.code("""student_id,course,assignment,score,max_score
STU001,Algebra,Quiz 1,85,100""", language="csv")

# ============================================
# PAGE: SUCCESS PLANS
# ============================================

elif page == "🎯 Success Plans":
    st.markdown("""
    <div class="main-header">
        <h1>🎯 AI-Powered Success Plans</h1>
        <p>Personalized intervention strategies generated by Cortex AI</p>
    </div>
    """, unsafe_allow_html=True)
    
    try:
        at_risk_df = get_at_risk_students()
        
        if not at_risk_df.empty:
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.markdown("### 👤 Select Student")
                selected = st.selectbox(
                    "Student",
                    options=at_risk_df['STUDENT_NAME'].tolist(),
                    label_visibility="collapsed"
                )
                
                student_data = at_risk_df[at_risk_df['STUDENT_NAME'] == selected].iloc[0].to_dict()
                
                # Student card
                risk_color = "#ff6b6b" if student_data['RISK_SCORE'] >= 70 else "#ffa502" if student_data['RISK_SCORE'] >= 50 else "#ffd93d"
                
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, {risk_color}22 0%, {risk_color}11 100%); 
                            border-left: 4px solid {risk_color}; padding: 1rem; border-radius: 8px; margin: 1rem 0;">
                    <h3 style="margin: 0;">{student_data['STUDENT_NAME']}</h3>
                    <p style="color: #666;">Grade {student_data['GRADE_LEVEL']}</p>
                </div>
                """, unsafe_allow_html=True)
                
                st.metric("Risk Score", f"{student_data['RISK_SCORE']}")
                st.metric("Attendance", f"{student_data['ATTENDANCE_RATE']}%")
                st.metric("GPA", f"{student_data['CURRENT_GPA']:.1f}")
            
            with col2:
                if st.button("🤖 Generate Success Plan", type="primary", use_container_width=True):
                    with st.spinner("Cortex AI is analyzing..."):
                        try:
                            plan = generate_success_plan(student_data)
                            
                            st.markdown("""
                            <div class="plan-card">
                                <h3>📋 Recommended Success Plan</h3>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            st.markdown(plan)
                            
                            st.divider()
                            
                            # Parent outreach
                            st.markdown("### 📧 Parent Outreach")
                            
                            parent_lang = student_data.get('PARENT_LANGUAGE', 'English')
                            
                            default_msg = f"""Dear Parent/Guardian,

I wanted to reach out regarding {selected}'s progress. I'd like to schedule a brief meeting to discuss strategies we can implement together.

Best regards,
[Teacher Name]"""
                            
                            message = st.text_area("Message", value=default_msg, height=150)
                            
                            col_a, col_b = st.columns(2)
                            with col_a:
                                if st.button("📤 Send (English)"):
                                    st.success("✅ Email drafted!")
                            with col_b:
                                if parent_lang != 'English' and st.button(f"🌐 Translate ({parent_lang})"):
                                    translated = translate_message(message, parent_lang[:2].lower())
                                    st.text_area("Translated", value=translated, height=150)
                                    
                        except Exception as e:
                            st.error(f"Error: {e}")
        else:
            st.success("🎉 No at-risk students found!")
            
    except Exception as e:
        st.warning(f"Setup required: {e}")

# ============================================
# PAGE: SETTINGS
# ============================================

elif page == "⚙️ Settings":
    st.markdown("""
    <div class="main-header">
        <h1>⚙️ System Status</h1>
        <p>Monitor data pipelines and system health</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Pipeline status
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        ### 📤 Bulk Upload
        <div style="background: #d4edda; padding: 1rem; border-radius: 8px; text-align: center;">
            <span style="font-size: 2rem;">✅</span>
            <p><strong>Active</strong></p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        ### 📝 Direct Entry
        <div style="background: #d4edda; padding: 1rem; border-radius: 8px; text-align: center;">
            <span style="font-size: 2rem;">✅</span>
            <p><strong>Active</strong></p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        ### 🔄 Auto-Sync
        <div style="background: #fff3cd; padding: 1rem; border-radius: 8px; text-align: center;">
            <span style="font-size: 2rem;">⚙️</span>
            <p><strong>Configured</strong></p>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    # Data quality
    st.subheader("📊 Data Quality")
    
    try:
        quality = session.sql("""
            SELECT 'Students' as table_name, COUNT(*) as rows FROM RAW_DATA.STUDENTS
            UNION ALL SELECT 'Attendance', COUNT(*) FROM RAW_DATA.ATTENDANCE
            UNION ALL SELECT 'Grades', COUNT(*) FROM RAW_DATA.GRADES
            UNION ALL SELECT 'Teacher Notes', COUNT(*) FROM APP.TEACHER_NOTES
        """).to_pandas()
        
        cols = st.columns(4)
        for i, (_, row) in enumerate(quality.iterrows()):
            cols[i].metric(row['TABLE_NAME'], f"{row['ROWS']:,} rows")
            
    except Exception as e:
        st.warning(f"Could not load data quality: {e}")
    
    st.divider()
    
    # Dynamic tables
    st.subheader("🔄 Dynamic Table Status")
    
    try:
        dt_status = session.sql("""
            SELECT name, target_lag, scheduling_state
            FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
            WHERE schema_name = 'ANALYTICS'
        """).to_pandas()
        
        if not dt_status.empty:
            st.dataframe(dt_status, use_container_width=True, hide_index=True)
    except:
        st.info("Dynamic Tables info unavailable")
