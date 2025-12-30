"""
GradSync - Streamlit Native App
Closing the gap between data and graduation.
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import uuid
import time

# Initialize session
session = get_active_session()

# Page config
st.set_page_config(
    page_title="GradSync",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Initialize session state
if 'page' not in st.session_state:
    st.session_state.page = "dashboard"

# Dark theme CSS
st.markdown("""
<style>
    /* Main app dark theme */
    .stApp {
        background-color: #0a0a0a;
    }
    
    /* Hide sidebar completely */
    [data-testid="stSidebar"] {
        display: none;
    }
    
    /* Hide default header */
    header[data-testid="stHeader"] {
        display: none;
    }
    
    /* Make all text visible */
    .stApp, .stApp p, .stApp span, .stApp label, .stApp div {
        color: #e0e0e0;
    }
    
    /* Left navigation panel */
    .nav-panel {
        background: #111111;
        border-radius: 12px;
        padding: 1.5rem 1rem;
        height: calc(100vh - 100px);
        position: sticky;
        top: 20px;
    }
    
    /* Logo */
    .nav-logo {
        display: flex;
        align-items: center;
        gap: 12px;
        padding-bottom: 1.5rem;
        margin-bottom: 1.5rem;
        border-bottom: 1px solid #1a1a1a;
    }
    
    .nav-logo-icon {
        font-size: 1.5rem;
    }
    
    .nav-logo-text {
        color: #22c55e;
        font-size: 1.2rem;
        font-weight: 600;
    }
    
    /* Nav section label */
    .nav-section {
        color: #505050;
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        margin: 1.5rem 0 0.75rem 0;
        padding-left: 0.5rem;
    }
    
    /* Nav items */
    .nav-item {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 0.75rem 1rem;
        margin: 4px 0;
        border-radius: 8px;
        color: #808080;
        cursor: pointer;
        transition: all 0.2s ease;
        border: 1px solid transparent;
    }
    
    .nav-item:hover {
        background: rgba(34, 197, 94, 0.08);
        color: #a0a0a0;
    }
    
    .nav-item.active {
        background: rgba(34, 197, 94, 0.12);
        color: #22c55e;
        border-color: rgba(34, 197, 94, 0.3);
    }
    
    .nav-item-icon {
        font-size: 1.1rem;
        width: 24px;
        text-align: center;
    }
    
    .nav-item-text {
        font-size: 0.9rem;
    }
    
    /* User profile */
    .user-profile {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 1rem;
        margin-top: auto;
        border-top: 1px solid #1a1a1a;
        position: absolute;
        bottom: 1rem;
        left: 1rem;
        right: 1rem;
    }
    
    .user-avatar {
        width: 36px;
        height: 36px;
        background: #1a1a1a;
        border-radius: 8px;
        display: flex;
        align-items: center;
        justify-content: center;
        color: #505050;
        font-size: 1.1rem;
    }
    
    .user-name {
        color: #e0e0e0;
        font-size: 0.9rem;
        font-weight: 500;
    }
    
    .user-status {
        color: #22c55e;
        font-size: 0.75rem;
        display: flex;
        align-items: center;
        gap: 6px;
    }
    
    /* Page header */
    .page-header {
        color: #e0e0e0;
        font-size: 1.3rem;
        font-weight: 500;
        margin-bottom: 0.25rem;
    }
    
    .page-subtitle {
        color: #505050;
        font-size: 0.85rem;
        margin-bottom: 1.5rem;
    }
    
    /* Metric cards */
    .metric-box {
        background: #111111;
        border: 1px solid #1a1a1a;
        border-radius: 10px;
        padding: 1.25rem;
    }
    
    .metric-label {
        color: #606060;
        font-size: 0.7rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
    }
    
    .metric-value {
        color: #e0e0e0;
        font-size: 2rem;
        font-weight: 600;
    }
    
    .metric-value.green { color: #22c55e; }
    .metric-value.yellow { color: #eab308; }
    .metric-value.red { color: #ef4444; }
    
    /* Data panel */
    .panel {
        background: #111111;
        border: 1px solid #1a1a1a;
        border-radius: 10px;
        padding: 1.25rem;
        margin-bottom: 1rem;
    }
    
    .panel-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding-bottom: 0.75rem;
        margin-bottom: 0.75rem;
        border-bottom: 1px solid #1a1a1a;
    }
    
    .panel-title {
        color: #808080;
        font-size: 0.85rem;
        font-weight: 500;
    }
    
    .badge {
        font-size: 0.65rem;
        padding: 4px 10px;
        border-radius: 6px;
        text-transform: uppercase;
        font-weight: 500;
    }
    
    .badge-green {
        background: rgba(34, 197, 94, 0.15);
        color: #22c55e;
    }
    
    .badge-red {
        background: rgba(239, 68, 68, 0.15);
        color: #ef4444;
    }
    
    .badge-yellow {
        background: rgba(234, 179, 8, 0.15);
        color: #eab308;
    }
    
    /* Student row */
    .student-row {
        display: flex;
        align-items: center;
        padding: 0.7rem 0;
        border-bottom: 1px solid #151515;
    }
    
    .student-row:last-child {
        border-bottom: none;
    }
    
    .risk-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        margin-right: 12px;
    }
    
    .risk-dot.critical { background: #ef4444; }
    .risk-dot.high { background: #f59e0b; }
    .risk-dot.moderate { background: #eab308; }
    
    .student-name {
        flex: 1;
        color: #d0d0d0;
        font-size: 0.9rem;
    }
    
    .student-info {
        color: #606060;
        font-size: 0.8rem;
        margin-left: 1rem;
    }
    
    /* Activity log */
    .activity-item {
        padding: 0.5rem 0;
        border-bottom: 1px solid #151515;
        color: #808080;
        font-size: 0.85rem;
    }
    
    /* Navigation buttons - dark style */
    [data-testid="column"]:first-child .stButton > button {
        background: transparent !important;
        color: #808080 !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 400 !important;
        padding: 0.75rem 1rem !important;
        text-align: left !important;
        justify-content: flex-start !important;
        transition: all 0.2s ease !important;
    }
    
    [data-testid="column"]:first-child .stButton > button:hover {
        background: rgba(34, 197, 94, 0.1) !important;
        color: #22c55e !important;
    }
    
    [data-testid="column"]:first-child .stButton > button:focus {
        background: rgba(34, 197, 94, 0.15) !important;
        color: #22c55e !important;
        box-shadow: none !important;
    }
    
    /* Action buttons - green style (for forms, etc) */
    [data-testid="column"]:not(:first-child) .stButton > button,
    form .stButton > button {
        background: #22c55e !important;
        color: #000 !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 500 !important;
        padding: 0.6rem 1.5rem !important;
    }
    
    [data-testid="column"]:not(:first-child) .stButton > button:hover,
    form .stButton > button:hover {
        background: #16a34a !important;
    }
    
    /* Form inputs */
    .stTextArea textarea, .stTextInput input, .stSelectbox > div > div {
        background: #0d0d0d !important;
        border: 1px solid #1a1a1a !important;
        border-radius: 8px !important;
        color: #e0e0e0 !important;
    }
    
    .stSelectbox label, .stTextArea label, .stTextInput label {
        color: #808080 !important;
    }
    
    /* Divider */
    hr {
        border: none;
        border-top: 1px solid #1a1a1a;
        margin: 1.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

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
def get_metrics():
    return session.sql("""
        SELECT 
            COUNT(*) as total_students,
            SUM(CASE WHEN risk_score >= 70 THEN 1 ELSE 0 END) as critical,
            SUM(CASE WHEN risk_score >= 50 AND risk_score < 70 THEN 1 ELSE 0 END) as high_risk,
            ROUND(AVG(attendance_rate), 1) as avg_attendance,
            ROUND(AVG(current_gpa), 2) as avg_gpa
        FROM ANALYTICS.STUDENT_360_VIEW
    """).collect()[0]

def analyze_sentiment(text):
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.SENTIMENT('{text.replace("'", "''")}') as sentiment
    """).collect()
    return float(result[0]['SENTIMENT'])

def generate_success_plan(student_data):
    prompt = f"""You are an educational advisor. Generate a specific, actionable Success Plan with 3-4 bullet points.
    
    Student: {student_data['STUDENT_NAME']} | Grade: {student_data['GRADE_LEVEL']}
    Attendance: {student_data['ATTENDANCE_RATE']}% | GPA: {student_data['CURRENT_GPA']}
    Risk Score: {student_data['RISK_SCORE']}
    
    Provide interventions the teacher can implement this week."""
    
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', $${prompt}$$) as plan
    """).collect()
    return result[0]['PLAN']

SUPPORTED_LANGUAGES = {
    'Spanish': 'es', 'Chinese': 'zh', 'Vietnamese': 'vi', 'Korean': 'ko',
    'Arabic': 'ar', 'French': 'fr', 'Portuguese': 'pt', 'German': 'de'
}

def translate_message(text, target_lang):
    lang_code = SUPPORTED_LANGUAGES.get(target_lang, target_lang)
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.TRANSLATE($${text}$$, 'en', '{lang_code}') as translated
    """).collect()
    return result[0]['TRANSLATED']

def get_parent_language(student_id):
    try:
        result = session.sql(f"""
            SELECT COALESCE(parent_language, 'English') as parent_language
            FROM RAW_DATA.STUDENTS WHERE student_id = '{student_id}'
        """).collect()
        return result[0]['PARENT_LANGUAGE'] if result else 'English'
    except:
        return 'English'

# ============================================
# LAYOUT: Left Nav + Main Content
# ============================================

nav_col, main_col = st.columns([1, 4])

# ============================================
# LEFT NAVIGATION PANEL
# ============================================

with nav_col:
    # Logo
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 10px; padding: 0.5rem 0 1.5rem 0; border-bottom: 1px solid #1a1a1a; margin-bottom: 1rem;">
        <span style="font-size: 1.3rem;">🎓</span>
        <span style="color: #22c55e; font-size: 1.1rem; font-weight: 600;">GradSync</span>
    </div>
    """, unsafe_allow_html=True)
    
    # Main navigation
    nav_items = [
        ("🏠", "Overview", "dashboard"),
        ("📊", "Analytics", "analytics"),
        ("📝", "Observations", "observation"),
        ("📤", "Upload", "upload"),
        ("🎯", "Success Plans", "plans"),
    ]
    
    for icon, label, key in nav_items:
        if st.button(f"{icon}  {label}", key=f"nav_{key}", use_container_width=True):
            st.session_state.page = key
            st.rerun()
    
    # System section
    st.markdown('<p style="color: #404040; font-size: 0.7rem; text-transform: uppercase; letter-spacing: 1px; margin: 1.5rem 0 0.5rem 0;">System</p>', unsafe_allow_html=True)
    
    if st.button("⚙️  Settings", key="nav_settings", use_container_width=True):
        st.session_state.page = "settings"
        st.rerun()
    
    # Spacer
    st.markdown("<br><br><br>", unsafe_allow_html=True)
    
    # User profile
    st.markdown("""
    <div style="border-top: 1px solid #1a1a1a; padding-top: 1rem; margin-top: 1rem;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <div style="width: 32px; height: 32px; background: #1a1a1a; border-radius: 6px; display: flex; align-items: center; justify-content: center;">👤</div>
            <div>
                <div style="color: #e0e0e0; font-size: 0.85rem;">Teacher</div>
                <div style="color: #22c55e; font-size: 0.7rem;">● Active Session</div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ============================================
# MAIN CONTENT AREA
# ============================================

with main_col:
    page = st.session_state.page
    
    # ============================================
    # PAGE: DASHBOARD / OVERVIEW
    # ============================================
    
    if page == "dashboard" or page == "analytics":
        st.markdown('<div class="page-header">📊 Student Risk Dashboard</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Real-time insights powered by Dynamic Tables & Cortex AI</div>', unsafe_allow_html=True)
        
        # Metrics
        try:
            metrics = get_metrics()
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">Total Students</div>
                    <div class="metric-value">{metrics['TOTAL_STUDENTS']}</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col2:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">Critical Risk</div>
                    <div class="metric-value red">{metrics['CRITICAL']}</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col3:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">Avg Attendance</div>
                    <div class="metric-value green">{metrics['AVG_ATTENDANCE']}%</div>
                </div>
                """, unsafe_allow_html=True)
            
            with col4:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">Avg GPA</div>
                    <div class="metric-value">{metrics['AVG_GPA']}</div>
                </div>
                """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Error loading metrics: {e}")
        
        st.markdown("<hr>", unsafe_allow_html=True)
        
        # Two columns
        col_left, col_right = st.columns([2, 1])
        
        with col_left:
            st.markdown("""
            <div class="panel">
                <div class="panel-header">
                    <span class="panel-title">Students Requiring Attention</span>
                    <span class="badge badge-red">Priority</span>
                </div>
            """, unsafe_allow_html=True)
            
            try:
                at_risk_df = get_at_risk_students()
                
                if not at_risk_df.empty:
                    for _, student in at_risk_df.head(8).iterrows():
                        risk_class = "critical" if student['RISK_SCORE'] >= 70 else "high" if student['RISK_SCORE'] >= 50 else "moderate"
                        
                        st.markdown(f"""
                        <div class="student-row">
                            <div class="risk-dot {risk_class}"></div>
                            <span class="student-name">{student['STUDENT_NAME']}</span>
                            <span class="student-info">Grade {int(student['GRADE_LEVEL'])}</span>
                            <span class="student-info">Risk: {student['RISK_SCORE']}</span>
                            <span class="student-info">Att: {student['ATTENDANCE_RATE']}%</span>
                            <span class="student-info">GPA: {student['CURRENT_GPA']:.1f}</span>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.success("🎉 All students are on track!")
            except Exception as e:
                st.warning(f"Could not load students: {e}")
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col_right:
            st.markdown("""
            <div class="panel">
                <div class="panel-header">
                    <span class="panel-title">Recent Activity</span>
                    <span class="badge badge-green">Live</span>
                </div>
            """, unsafe_allow_html=True)
            
            try:
                recent = session.sql("""
                    SELECT s.first_name, n.note_category, n.sentiment_score
                    FROM APP.TEACHER_NOTES n
                    JOIN RAW_DATA.STUDENTS s ON n.student_id = s.student_id
                    ORDER BY n.created_at DESC LIMIT 6
                """).to_pandas()
                
                if not recent.empty:
                    for _, note in recent.iterrows():
                        emoji = "😊" if note['SENTIMENT_SCORE'] > 0.3 else "😟" if note['SENTIMENT_SCORE'] < -0.3 else "😐"
                        st.markdown(f'<div class="activity-item">{emoji} {note["FIRST_NAME"]} - {note["NOTE_CATEGORY"]}</div>', unsafe_allow_html=True)
                else:
                    st.caption("No recent activity")
            except:
                st.caption("Waiting for activity...")
            
            st.markdown("</div>", unsafe_allow_html=True)

    # ============================================
    # PAGE: LOG OBSERVATION
    # ============================================
    
    elif page == "observation":
        st.markdown('<div class="page-header">📝 Log Student Observation</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Quick note entry with AI sentiment analysis</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            with st.form("observation_form", clear_on_submit=True):
                try:
                    students_df = get_students()
                    student_options = dict(zip(students_df['STUDENT_NAME'], students_df['STUDENT_ID']))
                    selected_student = st.selectbox("Select Student", options=list(student_options.keys()))
                except:
                    selected_student = st.text_input("Student ID")
                    student_options = {selected_student: selected_student}
                
                category = st.selectbox("Category", ["Academic", "Behavioral", "Social", "Health", "General"])
                note_text = st.text_area("Observation Note", placeholder="Enter your observation...", height=120)
                
                submitted = st.form_submit_button("💾 Save & Analyze", use_container_width=True)
                
                if submitted and note_text and selected_student:
                    with st.spinner("Analyzing with Cortex AI..."):
                        try:
                            start_time = time.time()
                            sentiment = analyze_sentiment(note_text)
                            student_id = student_options.get(selected_student, selected_student)
                            
                            session.sql(f"""
                                INSERT INTO APP.TEACHER_NOTES 
                                (student_id, teacher_id, note_text, note_category, sentiment_score)
                                VALUES ('{student_id}', 'CURRENT_USER', $${note_text}$$, '{category}', {sentiment})
                            """).collect()
                            
                            latency_ms = (time.time() - start_time) * 1000
                            emoji = "😊" if sentiment > 0.3 else "😐" if sentiment > -0.3 else "😟"
                            label = "Positive" if sentiment > 0.3 else "Neutral" if sentiment > -0.3 else "Negative"
                            color = "green" if sentiment > 0.3 else "" if sentiment > -0.3 else "red"
                            
                            st.success("✅ Observation saved!")
                            st.markdown(f"""
                            <div class="metric-box" style="text-align: center; margin-top: 1rem;">
                                <div style="font-size: 2.5rem;">{emoji}</div>
                                <div class="metric-value {color}" style="font-size: 1.2rem;">{label}</div>
                                <div style="color: #606060; font-size: 0.8rem;">Score: {sentiment:.2f} | {latency_ms:.0f}ms</div>
                            </div>
                            """, unsafe_allow_html=True)
                            st.cache_data.clear()
                        except Exception as e:
                            st.error(f"Error: {e}")
        
        with col2:
            st.markdown("""
            <div class="panel">
                <div class="panel-title" style="margin-bottom: 1rem;">Sentiment Guide</div>
                <div class="activity-item">😊 <strong>Positive</strong> - Score > 0.3</div>
                <div class="activity-item">😐 <strong>Neutral</strong> - Score -0.3 to 0.3</div>
                <div class="activity-item">😟 <strong>Negative</strong> - Score < -0.3</div>
            </div>
            """, unsafe_allow_html=True)


    # ============================================
    # PAGE: UPLOAD DATA
    # ============================================
    
    elif page == "upload":
        st.markdown('<div class="page-header">📤 Bulk Data Upload</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Import data from Canvas, PowerSchool, or CSV exports</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            data_type = st.selectbox(
                "Data Type",
                ["students", "attendance", "grades"],
                format_func=lambda x: {"students": "📚 Student Roster", "attendance": "📅 Attendance Records", "grades": "📝 Grade Data"}[x]
            )
            
            uploaded_file = st.file_uploader("Upload File", type=['csv', 'xlsx'])
            
            if uploaded_file:
                try:
                    df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
                    
                    c1, c2 = st.columns(2)
                    c1.metric("Rows", len(df))
                    c2.metric("Columns", len(df.columns))
                    
                    st.dataframe(df.head(10), use_container_width=True)
                    
                    if st.button("🚀 Import Data", use_container_width=True):
                        with st.spinner("Importing..."):
                            try:
                                batch_id = str(uuid.uuid4())[:8]
                                progress = st.progress(0)
                                
                                for i, (_, row) in enumerate(df.iterrows()):
                                    row_json = row.to_json()
                                    session.sql(f"""
                                        INSERT INTO RAW_DATA.BULK_UPLOAD_STAGING 
                                        (upload_batch, uploaded_by, data_type, raw_data)
                                        VALUES ('{batch_id}', CURRENT_USER(), '{data_type}', PARSE_JSON($${row_json}$$))
                                    """).collect()
                                    progress.progress((i + 1) / len(df))
                                
                                st.success(f"✅ Imported {len(df)} records!")
                                st.balloons()
                                st.cache_data.clear()
                            except Exception as e:
                                st.error(f"Import failed: {e}")
                except Exception as e:
                    st.error(f"Error reading file: {e}")
        
        with col2:
            st.markdown('<div class="panel-title">Expected Format</div>', unsafe_allow_html=True)
            if data_type == "students":
                st.code("student_id,first_name,last_name,grade_level\nSTU001,John,Doe,9", language="csv")
            elif data_type == "attendance":
                st.code("student_id,date,status,period\nSTU001,2024-12-01,Present,1", language="csv")
            else:
                st.code("student_id,course,assignment,score,max_score\nSTU001,Algebra,Quiz 1,85,100", language="csv")

    # ============================================
    # PAGE: SUCCESS PLANS
    # ============================================
    
    elif page == "plans":
        st.markdown('<div class="page-header">🎯 AI-Powered Success Plans</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Personalized intervention strategies generated by Cortex AI</div>', unsafe_allow_html=True)
        
        try:
            at_risk_df = get_at_risk_students()
            
            if not at_risk_df.empty:
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    st.markdown('<div class="panel-title">Select Student</div>', unsafe_allow_html=True)
                    selected = st.selectbox("Student", options=at_risk_df['STUDENT_NAME'].tolist(), label_visibility="collapsed")
                    student_data = at_risk_df[at_risk_df['STUDENT_NAME'] == selected].iloc[0].to_dict()
                    
                    color = "red" if student_data['RISK_SCORE'] >= 70 else "yellow" if student_data['RISK_SCORE'] >= 50 else ""
                    
                    st.markdown(f"""
                    <div class="metric-box" style="margin-top: 1rem;">
                        <div class="metric-label">Risk Score</div>
                        <div class="metric-value {color}">{student_data['RISK_SCORE']}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.metric("Attendance", f"{student_data['ATTENDANCE_RATE']}%")
                    st.metric("GPA", f"{student_data['CURRENT_GPA']:.1f}")
                
                with col2:
                    if st.button("🤖 Generate Success Plan", use_container_width=True):
                        with st.spinner("Cortex AI analyzing..."):
                            try:
                                plan = generate_success_plan(student_data)
                                
                                st.markdown("""
                                <div class="panel-header">
                                    <span class="panel-title">Recommended Interventions</span>
                                    <span class="badge badge-green">AI Generated</span>
                                </div>
                                """, unsafe_allow_html=True)
                                
                                st.markdown(plan)
                                
                                st.markdown("<hr>", unsafe_allow_html=True)
                                st.markdown('<div class="panel-title">Parent Communication</div>', unsafe_allow_html=True)
                                
                                student_id = student_data.get('STUDENT_ID', '')
                                parent_lang = get_parent_language(student_id) if student_id else 'English'
                                
                                if parent_lang != 'English':
                                    st.info(f"🌐 Parent's preferred language: **{parent_lang}**")
                                
                                available_langs = ['English'] + list(SUPPORTED_LANGUAGES.keys())
                                selected_lang = st.selectbox("Target Language", options=available_langs,
                                    index=available_langs.index(parent_lang) if parent_lang in available_langs else 0)
                                
                                default_msg = f"Dear Parent/Guardian,\n\nI wanted to reach out regarding {selected}'s progress. I'd like to schedule a meeting to discuss strategies we can implement together.\n\nBest regards,\n[Teacher Name]"
                                message = st.text_area("Message", value=default_msg, height=120)
                                
                                c1, c2 = st.columns(2)
                                with c1:
                                    if st.button("📤 Send (English)", use_container_width=True):
                                        st.success("✅ Email drafted!")
                                with c2:
                                    if selected_lang != 'English':
                                        if st.button(f"🌐 Translate to {selected_lang}", use_container_width=True):
                                            with st.spinner("Translating..."):
                                                translated = translate_message(message, selected_lang)
                                                st.text_area("Translated Message", value=translated, height=120)
                                                st.success(f"✅ Translated!")
                            except Exception as e:
                                st.error(f"Error: {e}")
            else:
                st.success("🎉 All students are performing well!")
        except Exception as e:
            st.warning(f"Setup required: {e}")

    # ============================================
    # PAGE: SETTINGS
    # ============================================
    
    elif page == "settings":
        st.markdown('<div class="page-header">⚙️ System Status</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Monitor data pipelines and system health</div>', unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("""
            <div class="metric-box" style="text-align: center;">
                <div style="font-size: 2rem;">📤</div>
                <div class="metric-label">Bulk Upload</div>
                <div class="badge badge-green" style="margin-top: 0.5rem;">✓ Active</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
            <div class="metric-box" style="text-align: center;">
                <div style="font-size: 2rem;">📝</div>
                <div class="metric-label">Direct Entry</div>
                <div class="badge badge-green" style="margin-top: 0.5rem;">✓ Active</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown("""
            <div class="metric-box" style="text-align: center;">
                <div style="font-size: 2rem;">🔄</div>
                <div class="metric-label">Auto-Sync</div>
                <div class="badge badge-yellow" style="margin-top: 0.5rem;">⚙ Configured</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<hr>", unsafe_allow_html=True)
        st.markdown('<div class="panel-title">Data Overview</div>', unsafe_allow_html=True)
        
        try:
            quality = session.sql("""
                SELECT 'Students' as tbl, COUNT(*) as cnt FROM RAW_DATA.STUDENTS
                UNION ALL SELECT 'Attendance', COUNT(*) FROM RAW_DATA.ATTENDANCE
                UNION ALL SELECT 'Grades', COUNT(*) FROM RAW_DATA.GRADES
                UNION ALL SELECT 'Notes', COUNT(*) FROM APP.TEACHER_NOTES
            """).to_pandas()
            
            cols = st.columns(4)
            for i, (_, row) in enumerate(quality.iterrows()):
                with cols[i]:
                    st.metric(row['TBL'], f"{row['CNT']:,}")
        except Exception as e:
            st.warning(f"Could not load data: {e}")
        
        st.markdown("<hr>", unsafe_allow_html=True)
        st.markdown("""
        <div class="panel" style="text-align: center;">
            <div style="color: #22c55e; font-size: 1.2rem;">🎓 GradSync</div>
            <div style="color: #606060; font-size: 0.85rem; margin-top: 0.5rem;">Closing the gap between data and graduation</div>
            <div style="color: #404040; font-size: 0.75rem; margin-top: 0.25rem;">Powered by Snowflake • Cortex AI • Dynamic Tables</div>
        </div>
        """, unsafe_allow_html=True)
