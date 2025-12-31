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
    
    .badge-blue {
        background: rgba(59, 130, 246, 0.15);
        color: #3b82f6;
    }
    
    /* Welcome banner */
    .welcome-banner {
        background: linear-gradient(135deg, rgba(34, 197, 94, 0.15) 0%, rgba(34, 197, 94, 0.05) 100%);
        border: 1px solid rgba(34, 197, 94, 0.2);
        border-radius: 12px;
        padding: 1.25rem;
        margin-bottom: 1.5rem;
    }
    
    /* Info tooltip */
    .info-tip {
        background: rgba(59, 130, 246, 0.1);
        border: 1px solid rgba(59, 130, 246, 0.2);
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin: 0.5rem 0;
        font-size: 0.85rem;
        color: #93c5fd;
    }
    
    /* Help text */
    .help-text {
        color: #606060;
        font-size: 0.8rem;
        margin-top: 0.25rem;
    }
    
    /* Empty state */
    .empty-state {
        text-align: center;
        padding: 2rem;
        color: #606060;
    }
    
    .empty-state-icon {
        font-size: 2.5rem;
        margin-bottom: 0.75rem;
    }
    
    /* Action card */
    .action-card {
        background: #111111;
        border: 1px solid #1a1a1a;
        border-radius: 10px;
        padding: 1rem;
        text-align: center;
        transition: all 0.2s ease;
        cursor: pointer;
    }
    
    .action-card:hover {
        border-color: rgba(34, 197, 94, 0.3);
        background: rgba(34, 197, 94, 0.05);
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

def classify_note(text):
    """Classify note using Cortex AI into concern categories"""
    try:
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
                $${text}$$,
                ['Academic Struggle', 'Behavioral Concern', 'Safety Threat', 
                 'Social-Emotional Risk', 'Family Situation', 'Positive Progress']
            ) as classification
        """).collect()
        classification = result[0]['CLASSIFICATION']
        import json
        if isinstance(classification, str):
            classification = json.loads(classification)
        return classification.get('label', 'Unknown'), float(classification.get('score', 0.95))
    except Exception as e:
        st.warning(f"Classification error: {e}")
        return None, 0.0

def is_high_risk_category(classification):
    """Check if classification is high-risk (requires counselor review)"""
    return classification in ('Social-Emotional Risk', 'Family Situation', 'Safety Threat')

@st.cache_data(ttl=60)
def get_counselor_alerts():
    """Get high-risk notes pending review"""
    return session.sql("""
        SELECT 
            n.note_id,
            n.student_id,
            s.first_name || ' ' || s.last_name as student_name,
            s.grade_level,
            n.note_text,
            n.note_category as teacher_category,
            n.ai_classification,
            n.ai_confidence,
            n.sentiment_score,
            n.is_high_risk,
            n.created_at,
            n.reviewed_by,
            n.reviewed_at
        FROM GRADSYNC_DB.APP.TEACHER_NOTES n
        JOIN GRADSYNC_DB.RAW_DATA.STUDENTS s ON n.student_id = s.student_id
        WHERE n.is_high_risk = TRUE
        ORDER BY 
            CASE WHEN n.reviewed_at IS NULL THEN 0 ELSE 1 END,
            n.created_at DESC
    """).to_pandas()

@st.cache_data(ttl=120)
def get_early_warning_students():
    """Get students showing early warning signs"""
    try:
        return session.sql("""
            SELECT * FROM GRADSYNC_DB.ANALYTICS.EARLY_WARNING_STUDENTS
            ORDER BY early_warning_score DESC
        """).to_pandas()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_recent_notes():
    """Get recent notes with AI classification"""
    return session.sql("""
        SELECT 
            n.note_id,
            s.first_name || ' ' || s.last_name as student_name,
            n.note_text,
            n.note_category,
            n.ai_classification,
            n.ai_confidence,
            n.sentiment_score,
            n.is_high_risk,
            n.created_at
        FROM GRADSYNC_DB.APP.TEACHER_NOTES n
        JOIN GRADSYNC_DB.RAW_DATA.STUDENTS s ON n.student_id = s.student_id
        ORDER BY n.created_at DESC
        LIMIT 50
    """).to_pandas()

def analyze_student_patterns(student_id, student_name, notes_text):
    """Use Cortex AI to detect patterns across multiple notes"""
    try:
        prompt = f"""You are a school counselor assistant. Review these teacher notes about {student_name} and write a simple summary for educators.

Teacher observations:
{notes_text}

Write 2-3 short sentences that:
- Point out any worrying patterns you see
- Suggest one simple next step

Use plain language a busy teacher can quickly read. If everything looks fine, just say "No concerns - student appears to be doing well."
"""
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', $${prompt}$$) as analysis
        """).collect()
        return result[0]['ANALYSIS']
    except Exception as e:
        return f"Pattern analysis unavailable: {e}"

@st.cache_data(ttl=120)
def get_students_for_pattern_analysis():
    """Get students with multiple notes for pattern analysis"""
    return session.sql("""
        SELECT 
            n.student_id,
            s.first_name || ' ' || s.last_name as student_name,
            s.grade_level,
            COUNT(*) as note_count,
            LISTAGG(n.note_text, ' | ') WITHIN GROUP (ORDER BY n.created_at DESC) as all_notes,
            AVG(n.sentiment_score) as avg_sentiment,
            SUM(CASE WHEN n.is_high_risk THEN 1 ELSE 0 END) as high_risk_count
        FROM GRADSYNC_DB.APP.TEACHER_NOTES n
        JOIN GRADSYNC_DB.RAW_DATA.STUDENTS s ON n.student_id = s.student_id
        WHERE n.created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        GROUP BY n.student_id, s.first_name, s.last_name, s.grade_level
        HAVING COUNT(*) >= 2
        ORDER BY high_risk_count DESC, note_count DESC
    """).to_pandas()

@st.cache_data(ttl=60)
def get_ai_insights():
    """Get stored AI insights"""
    try:
        return session.sql("""
            SELECT i.*, s.first_name || ' ' || s.last_name as student_name
            FROM GRADSYNC_DB.APP.AI_INSIGHTS i
            JOIN GRADSYNC_DB.RAW_DATA.STUDENTS s ON i.student_id = s.student_id
            WHERE i.is_acknowledged = FALSE
            ORDER BY i.created_at DESC
        """).to_pandas()
    except:
        return pd.DataFrame()

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
        ("�",  "Counselor Alerts", "alerts"),
        ("🧠", "AI Insights", "insights"),
        ("⚡", "Early Warnings", "warnings"),
        ("📤", "Import Data", "upload"),
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
    # PAGE: OVERVIEW (Quick Summary)
    # ============================================
    
    if page == "dashboard":
        st.markdown('<div class="page-header">🏠 Welcome to GradSync</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Your student success dashboard</div>', unsafe_allow_html=True)
        
        # Welcome banner with guidance
        st.markdown("""
        <div class="welcome-banner">
            <div style="display: flex; align-items: flex-start; gap: 12px;">
                <span style="font-size: 1.5rem;">👋</span>
                <div>
                    <div style="color: #22c55e; font-weight: 500; margin-bottom: 0.25rem;">Good to see you!</div>
                    <div style="color: #a0a0a0; font-size: 0.85rem;">Here's a quick look at how your students are doing. Students flagged as "at-risk" may need extra support based on their attendance and grades.</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Metrics with explanations
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
                    for _, student in at_risk_df.head(5).iterrows():
                        risk_class = "critical" if student['RISK_SCORE'] >= 70 else "high" if student['RISK_SCORE'] >= 50 else "moderate"
                        
                        st.markdown(f"""
                        <div class="student-row">
                            <div class="risk-dot {risk_class}"></div>
                            <span class="student-name">{student['STUDENT_NAME']}</span>
                            <span class="student-info">Grade {int(student['GRADE_LEVEL'])}</span>
                            <span class="student-info">Risk: {student['RISK_SCORE']}</span>
                        </div>
                        """, unsafe_allow_html=True)
                    st.markdown('<div style="text-align: center; padding-top: 0.5rem;"><a href="#" style="color: #22c55e; font-size: 0.85rem;">View all in Analytics →</a></div>', unsafe_allow_html=True)
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
                    ORDER BY n.created_at DESC LIMIT 5
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
            
            # Quick actions
            st.markdown("""
            <div class="panel" style="margin-top: 1rem;">
                <div class="panel-title" style="margin-bottom: 0.75rem;">Quick Actions</div>
            """, unsafe_allow_html=True)
            
            if st.button("📝 Log Observation", key="quick_obs", use_container_width=True):
                st.session_state.page = "observation"
                st.rerun()
            if st.button("📤 Upload Data", key="quick_upload", use_container_width=True):
                st.session_state.page = "upload"
                st.rerun()
            
            st.markdown("</div>", unsafe_allow_html=True)

    # ============================================
    # PAGE: ANALYTICS (Detailed Charts & Trends)
    # ============================================
    
    elif page == "analytics":
        st.markdown('<div class="page-header">📊 Analytics</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Detailed insights and trends</div>', unsafe_allow_html=True)
        
        # Metrics row
        try:
            metrics = get_metrics()
            
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown(f'<div class="metric-box"><div class="metric-label">Total Students</div><div class="metric-value">{metrics["TOTAL_STUDENTS"]}</div></div>', unsafe_allow_html=True)
            with col2:
                st.markdown(f'<div class="metric-box"><div class="metric-label">Critical Risk</div><div class="metric-value red">{metrics["CRITICAL"]}</div></div>', unsafe_allow_html=True)
            with col3:
                st.markdown(f'<div class="metric-box"><div class="metric-label">High Risk</div><div class="metric-value yellow">{metrics["HIGH_RISK"]}</div></div>', unsafe_allow_html=True)
            with col4:
                st.markdown(f'<div class="metric-box"><div class="metric-label">Avg GPA</div><div class="metric-value">{metrics["AVG_GPA"]}</div></div>', unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Error loading metrics: {e}")
        
        st.markdown("<hr>", unsafe_allow_html=True)
        
        # Full at-risk student list
        st.markdown("""
        <div class="panel">
            <div class="panel-header">
                <span class="panel-title">All At-Risk Students</span>
                <span class="badge badge-red">Requires Action</span>
            </div>
        """, unsafe_allow_html=True)
        
        try:
            at_risk_df = get_at_risk_students()
            
            if not at_risk_df.empty:
                for _, student in at_risk_df.iterrows():
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
        
        st.markdown("<hr>", unsafe_allow_html=True)
        
        # Charts section
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.markdown("""
            <div class="panel">
                <div class="panel-title">Risk Distribution</div>
                <div class="help-text">Number of students by risk level</div>
            """, unsafe_allow_html=True)
            
            try:
                risk_dist = session.sql("""
                    SELECT 
                        CASE 
                            WHEN risk_score >= 70 THEN 'Critical'
                            WHEN risk_score >= 50 THEN 'High'
                            WHEN risk_score >= 30 THEN 'Moderate'
                            ELSE 'Low'
                        END as risk_level,
                        COUNT(*) as count
                    FROM ANALYTICS.STUDENT_360_VIEW
                    GROUP BY risk_level
                """).to_pandas()
                
                if not risk_dist.empty:
                    risk_order = ['Critical', 'High', 'Moderate', 'Low']
                    risk_dist['RISK_LEVEL'] = pd.Categorical(risk_dist['RISK_LEVEL'], categories=risk_order, ordered=True)
                    risk_dist = risk_dist.sort_values('RISK_LEVEL')
                    st.bar_chart(risk_dist.set_index('RISK_LEVEL')['COUNT'], color='#22c55e')
                else:
                    st.caption("No data available")
            except:
                st.caption("Chart unavailable")
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col_right:
            st.markdown("""
            <div class="panel">
                <div class="panel-title">Attendance by Grade</div>
                <div class="help-text">Average attendance rate per grade level</div>
            """, unsafe_allow_html=True)
            
            try:
                att_by_grade = session.sql("""
                    SELECT 
                        CAST(grade_level AS VARCHAR) as grade,
                        ROUND(AVG(attendance_rate), 1) as avg_attendance
                    FROM ANALYTICS.STUDENT_360_VIEW
                    GROUP BY grade_level
                    ORDER BY grade_level
                """).to_pandas()
                
                if not att_by_grade.empty:
                    st.bar_chart(att_by_grade.set_index('GRADE')['AVG_ATTENDANCE'], color='#3b82f6')
                else:
                    st.caption("No data available")
            except:
                st.caption("Chart unavailable")
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        # Second row of charts
        col_left2, col_right2 = st.columns(2)
        
        with col_left2:
            st.markdown("""
            <div class="panel">
                <div class="panel-title">GPA Distribution</div>
                <div class="help-text">Students grouped by GPA range</div>
            """, unsafe_allow_html=True)
            
            try:
                gpa_dist = session.sql("""
                    SELECT 
                        CASE 
                            WHEN current_gpa >= 3.5 THEN 'A (3.5+)'
                            WHEN current_gpa >= 3.0 THEN 'B (3.0-3.5)'
                            WHEN current_gpa >= 2.0 THEN 'C (2.0-3.0)'
                            WHEN current_gpa >= 1.0 THEN 'D (1.0-2.0)'
                            ELSE 'F (<1.0)'
                        END as gpa_range,
                        COUNT(*) as count
                    FROM ANALYTICS.STUDENT_360_VIEW
                    GROUP BY gpa_range
                """).to_pandas()
                
                if not gpa_dist.empty:
                    st.bar_chart(gpa_dist.set_index('GPA_RANGE')['COUNT'], color='#8b5cf6')
                else:
                    st.caption("No data available")
            except:
                st.caption("Chart unavailable")
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col_right2:
            st.markdown("""
            <div class="panel">
                <div class="panel-title">Performance Overview</div>
                <div class="help-text">Key metrics at a glance</div>
            """, unsafe_allow_html=True)
            
            try:
                perf_data = session.sql("""
                    SELECT 
                        ROUND(AVG(attendance_rate), 1) as avg_attendance,
                        ROUND(AVG(current_gpa), 2) as avg_gpa,
                        COUNT(CASE WHEN risk_score >= 50 THEN 1 END) as at_risk_count,
                        COUNT(*) as total
                    FROM ANALYTICS.STUDENT_360_VIEW
                """).collect()[0]
                
                # Display as metrics
                m1, m2 = st.columns(2)
                with m1:
                    att_val = float(perf_data['AVG_ATTENDANCE'])
                    att_color = "green" if att_val >= 90 else "yellow" if att_val >= 80 else "red"
                    st.markdown(f"""
                    <div class="metric-box" style="text-align: center;">
                        <div class="metric-label">Avg Attendance</div>
                        <div class="metric-value {att_color}">{att_val}%</div>
                    </div>
                    """, unsafe_allow_html=True)
                with m2:
                    gpa_val = float(perf_data['AVG_GPA'])
                    st.markdown(f"""
                    <div class="metric-box" style="text-align: center;">
                        <div class="metric-label">Avg GPA</div>
                        <div class="metric-value">{gpa_val}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                at_risk = int(perf_data['AT_RISK_COUNT'])
                total = int(perf_data['TOTAL'])
                on_track = total - at_risk
                
                st.markdown(f"""
                <div style="text-align: center; color: #808080; font-size: 0.85rem;">
                    <span style="color: #22c55e;">●</span> {on_track} on track &nbsp;&nbsp;
                    <span style="color: #ef4444;">●</span> {at_risk} at risk
                </div>
                """, unsafe_allow_html=True)
            except:
                st.caption("Data unavailable")
            
            st.markdown("</div>", unsafe_allow_html=True)

    # ============================================
    # PAGE: LOG OBSERVATION
    # ============================================
    
    elif page == "observation":
        st.markdown('<div class="page-header">📝 Student Observations</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Record notes about student progress and behavior</div>', unsafe_allow_html=True)
        
        # Guidance banner
        st.markdown("""
        <div class="welcome-banner">
            <div style="display: flex; align-items: flex-start; gap: 12px;">
                <span style="font-size: 1.5rem;">💡</span>
                <div>
                    <div style="color: #22c55e; font-weight: 500; margin-bottom: 0.25rem;">How it works</div>
                    <div style="color: #a0a0a0; font-size: 0.85rem;">Write a note about any student. Our AI will automatically analyze the sentiment (positive, neutral, or negative) to help track student wellbeing over time.</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.markdown("""
            <div class="panel">
                <div class="panel-title" style="margin-bottom: 1rem;">✏️ New Observation</div>
            """, unsafe_allow_html=True)
            
            with st.form("observation_form", clear_on_submit=True):
                try:
                    students_df = get_students()
                    student_options = dict(zip(students_df['STUDENT_NAME'], students_df['STUDENT_ID']))
                    selected_student = st.selectbox("Which student?", options=list(student_options.keys()), help="Select the student you want to write about")
                except:
                    selected_student = st.text_input("Student ID")
                    student_options = {selected_student: selected_student}
                
                category = st.selectbox("What type of observation?", ["Academic", "Behavioral", "Social", "Health", "General"], help="This helps organize your notes")
                note_text = st.text_area("Your observation", placeholder="Example: Maria showed great improvement in today's math quiz. She stayed focused and asked good questions.", height=120)
                
                st.markdown('<div class="help-text">💡 Be specific - include what happened, when, and any context that might be helpful.</div>', unsafe_allow_html=True)
                
                submitted = st.form_submit_button("💾 Save Observation", use_container_width=True)
                
                if submitted and note_text and selected_student:
                    with st.spinner("Saving and analyzing with AI..."):
                        try:
                            start_time = time.time()
                            sentiment = analyze_sentiment(note_text)
                            ai_class, ai_conf = classify_note(note_text)
                            is_high_risk = is_high_risk_category(ai_class) if ai_class else False
                            student_id = student_options.get(selected_student, selected_student)
                            ai_class_sql = f"'{ai_class}'" if ai_class else "NULL"
                            
                            session.sql(f"""
                                INSERT INTO GRADSYNC_DB.APP.TEACHER_NOTES 
                                (student_id, teacher_id, note_text, note_category, sentiment_score,
                                 ai_classification, ai_confidence, is_high_risk)
                                VALUES ('{student_id}', 'CURRENT_USER', $${note_text}$$, '{category}', {sentiment},
                                        {ai_class_sql}, {ai_conf}, {is_high_risk})
                            """).collect()
                            
                            latency_ms = (time.time() - start_time) * 1000
                            
                            # Override sentiment display for high-risk content
                            if is_high_risk:
                                emoji = "🚨"
                                label = "Concerning"
                                color = "red"
                            else:
                                emoji = "😊" if sentiment > 0.3 else "😐" if sentiment > -0.3 else "😟"
                                label = "Positive" if sentiment > 0.3 else "Neutral" if sentiment > -0.3 else "Negative"
                                color = "green" if sentiment > 0.3 else "" if sentiment > -0.3 else "red"
                            
                            st.success("✅ Observation saved!")
                            st.markdown(f"""
                            <div class="metric-box" style="text-align: center; margin-top: 1rem;">
                                <div style="font-size: 2.5rem;">{emoji}</div>
                                <div class="metric-value {color}" style="font-size: 1.2rem;">{label}</div>
                                <div style="color: #606060; font-size: 0.8rem;">Sentiment: {sentiment:.2f}</div>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            if ai_class:
                                risk_badge = '⚠️ HIGH RISK' if is_high_risk else ''
                                badge_style = 'background: rgba(239, 68, 68, 0.15); color: #ef4444;' if is_high_risk else 'background: rgba(59, 130, 246, 0.15); color: #3b82f6;'
                                st.markdown(f"""
                                <div class="metric-box" style="text-align: center; margin-top: 0.75rem;">
                                    <div style="color: #808080; font-size: 0.75rem; margin-bottom: 0.5rem;">AI Classification</div>
                                    <div style="{badge_style} padding: 0.5rem 1rem; border-radius: 6px; display: inline-block; font-weight: 500;">
                                        {ai_class} {risk_badge}
                                    </div>
                                    <div style="color: #606060; font-size: 0.75rem; margin-top: 0.5rem;">Confidence: {ai_conf:.0%}</div>
                                </div>
                                """, unsafe_allow_html=True)
                                if is_high_risk:
                                    st.warning("🔔 This note has been flagged for counselor review.")
                            
                            st.caption(f"Processed in {latency_ms:.0f}ms")
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
    # PAGE: COUNSELOR ALERTS
    # ============================================
    
    elif page == "alerts":
        st.markdown('<div class="page-header">🔔 Counselor Alert Queue</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">High-risk notes requiring review</div>', unsafe_allow_html=True)
        
        # Info banner
        st.markdown("""
        <div class="welcome-banner">
            <div style="display: flex; align-items: flex-start; gap: 12px;">
                <span style="font-size: 1.5rem;">⚠️</span>
                <div>
                    <div style="color: #eab308; font-weight: 500; margin-bottom: 0.25rem;">AI-Flagged Notes</div>
                    <div style="color: #a0a0a0; font-size: 0.85rem;">Notes classified as "Social-Emotional Risk" or "Family Situation" are automatically flagged for counselor review.</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        try:
            alerts_df = get_counselor_alerts()
            
            if alerts_df.empty:
                st.markdown("""
                <div class="panel" style="text-align: center; padding: 3rem;">
                    <div style="font-size: 3rem; margin-bottom: 1rem;">✅</div>
                    <div style="color: #22c55e; font-size: 1.2rem; font-weight: 500;">All Clear!</div>
                    <div style="color: #808080; font-size: 0.9rem; margin-top: 0.5rem;">No high-risk notes pending review.</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                # Stats row
                pending = len(alerts_df[alerts_df['REVIEWED_AT'].isna()])
                reviewed = len(alerts_df[alerts_df['REVIEWED_AT'].notna()])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">Pending Review</div>
                        <div class="metric-value red">{pending}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">Reviewed</div>
                        <div class="metric-value green">{reviewed}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">Total Flagged</div>
                        <div class="metric-value">{len(alerts_df)}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                # Filter
                filter_status = st.selectbox("Filter by status", ["All", "Pending Review", "Reviewed"])
                
                if filter_status == "Pending Review":
                    display_df = alerts_df[alerts_df['REVIEWED_AT'].isna()]
                elif filter_status == "Reviewed":
                    display_df = alerts_df[alerts_df['REVIEWED_AT'].notna()]
                else:
                    display_df = alerts_df
                
                # Display alerts
                for _, alert in display_df.iterrows():
                    is_pending = pd.isna(alert['REVIEWED_AT'])
                    border_color = 'rgba(239, 68, 68, 0.3)' if is_pending else 'rgba(34, 197, 94, 0.3)'
                    
                    st.markdown(f"""
                    <div class="panel" style="border-color: {border_color}; margin-bottom: 1rem;">
                        <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 0.75rem;">
                            <div>
                                <span style="font-weight: 500; color: #e0e0e0;">{alert['STUDENT_NAME']}</span>
                                <span style="color: #606060; font-size: 0.85rem;"> • Grade {int(alert['GRADE_LEVEL'])}</span>
                            </div>
                            <div style="background: rgba(239, 68, 68, 0.15); color: #ef4444; padding: 0.25rem 0.75rem; border-radius: 4px; font-size: 0.75rem; font-weight: 500;">
                                {alert['AI_CLASSIFICATION']}
                            </div>
                        </div>
                        <div style="color: #a0a0a0; font-size: 0.9rem; margin-bottom: 0.75rem; line-height: 1.5;">
                            "{alert['NOTE_TEXT'][:200]}{'...' if len(str(alert['NOTE_TEXT'])) > 200 else ''}"
                        </div>
                        <div style="display: flex; justify-content: space-between; align-items: center; color: #606060; font-size: 0.8rem;">
                            <span>Teacher category: {alert['TEACHER_CATEGORY']} • Confidence: {alert['AI_CONFIDENCE']:.0%}</span>
                            <span>{alert['CREATED_AT'].strftime('%b %d, %Y %H:%M') if hasattr(alert['CREATED_AT'], 'strftime') else alert['CREATED_AT']}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if is_pending:
                        if st.button(f"✓ Mark as Reviewed", key=f"review_{alert['NOTE_ID']}", use_container_width=True):
                            session.sql(f"""
                                UPDATE GRADSYNC_DB.APP.TEACHER_NOTES 
                                SET reviewed_by = 'COUNSELOR', reviewed_at = CURRENT_TIMESTAMP()
                                WHERE note_id = {alert['NOTE_ID']}
                            """).collect()
                            st.success("Marked as reviewed!")
                            st.cache_data.clear()
                            st.rerun()
                    else:
                        st.caption(f"✓ Reviewed by {alert['REVIEWED_BY']} on {alert['REVIEWED_AT']}")
                    
                    st.markdown("<br>", unsafe_allow_html=True)
                    
        except Exception as e:
            st.error(f"Error loading alerts: {e}")
            st.info("Run sql/10_ai_note_classification.sql to set up the alert queue.")


    # ============================================
    # PAGE: AI INSIGHTS (Pattern Detection)
    # ============================================
    
    elif page == "insights":
        st.markdown('<div class="page-header">🧠 AI Insights</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">See the bigger picture across teacher observations</div>', unsafe_allow_html=True)
        
        # Info banner
        st.markdown("""
        <div class="welcome-banner">
            <div style="display: flex; align-items: flex-start; gap: 12px;">
                <span style="font-size: 1.5rem;">�<</span>
                <div>
                    <div style="color: #22c55e; font-weight: 500; margin-bottom: 0.25rem;">How it works</div>
                    <div style="color: #a0a0a0; font-size: 0.85rem;">Select a student to see what AI notices when looking at all their notes together.</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        try:
            students_df = get_students_for_pattern_analysis()
            
            if students_df.empty:
                st.markdown("""
                <div class="panel" style="text-align: center; padding: 3rem;">
                    <div style="font-size: 3rem; margin-bottom: 1rem;">📝</div>
                    <div style="color: #808080; font-size: 1.1rem;">Not enough data yet</div>
                    <div style="color: #606060; font-size: 0.9rem; margin-top: 0.5rem;">Pattern detection requires at least 2 notes per student in the last 30 days.</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                # Stats
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">Students with Multiple Notes</div>
                        <div class="metric-value">{len(students_df)}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col2:
                    high_risk_students = len(students_df[students_df['HIGH_RISK_COUNT'] > 0])
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">With High-Risk Notes</div>
                        <div class="metric-value red">{high_risk_students}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    total_notes = int(students_df['NOTE_COUNT'].sum())
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">Total Notes Analyzed</div>
                        <div class="metric-value">{total_notes}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                # Student selector
                st.markdown("""
                <div class="panel">
                    <div class="panel-title" style="margin-bottom: 1rem;">Select a Student</div>
                """, unsafe_allow_html=True)
                
                student_options = dict(zip(
                    students_df['STUDENT_NAME'] + " (" + students_df['NOTE_COUNT'].astype(str) + " notes)",
                    students_df.index
                ))
                
                selected = st.selectbox("Select a student to analyze", options=list(student_options.keys()))
                
                if selected:
                    idx = student_options[selected]
                    student = students_df.iloc[idx]
                    
                    # Show student info
                    st.markdown(f"""
                    <div style="background: #111; border-radius: 8px; padding: 1rem; margin: 1rem 0;">
                        <div style="display: flex; justify-content: space-between;">
                            <div>
                                <span style="color: #e0e0e0; font-weight: 500;">{student['STUDENT_NAME']}</span>
                                <span style="color: #606060;"> • Grade {int(student['GRADE_LEVEL'])}</span>
                            </div>
                            <div style="color: #606060; font-size: 0.85rem;">
                                {int(student['NOTE_COUNT'])} notes • Avg sentiment: {student['AVG_SENTIMENT']:.2f}
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    if st.button("🧠 What does AI see?", type="primary", use_container_width=True):
                        with st.spinner("Analyzing notes..."):
                            analysis = analyze_student_patterns(
                                student['STUDENT_ID'],
                                student['STUDENT_NAME'],
                                student['ALL_NOTES']
                            )
                            
                            # Determine if concerning
                            is_concerning = "no concerning" not in analysis.lower() and "no pattern" not in analysis.lower()
                            
                            if is_concerning:
                                st.markdown(f"""
                                <div class="panel" style="border-color: rgba(239, 68, 68, 0.3); margin-top: 1rem;">
                                    <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 0.75rem;">
                                        <span style="font-size: 1.2rem;">⚠️</span>
                                        <span style="color: #ef4444; font-weight: 500;">Needs Attention</span>
                                    </div>
                                    <div style="color: #a0a0a0; line-height: 1.6;">{analysis}</div>
                                </div>
                                """, unsafe_allow_html=True)
                            else:
                                st.markdown(f"""
                                <div class="panel" style="border-color: rgba(34, 197, 94, 0.3); margin-top: 1rem;">
                                    <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 0.75rem;">
                                        <span style="font-size: 1.2rem;">✅</span>
                                        <span style="color: #22c55e; font-weight: 500;">Looking Good</span>
                                    </div>
                                    <div style="color: #a0a0a0; line-height: 1.6;">{analysis}</div>
                                </div>
                                """, unsafe_allow_html=True)
                    
                    # Show raw notes
                    with st.expander("📋 See all notes"):
                        notes = student['ALL_NOTES'].split(' | ')
                        for i, note in enumerate(notes, 1):
                            st.caption(f"{i}. {note}")
                
                st.markdown("</div>", unsafe_allow_html=True)
                
        except Exception as e:
            st.error(f"Error: {e}")
            st.info("Run sql/11_ai_pattern_detection.sql to set up pattern detection.")


    # ============================================
    # PAGE: EARLY WARNINGS
    # ============================================
    
    elif page == "warnings":
        st.markdown('<div class="page-header">⚡ Early Warnings</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Students showing warning signs before they become at-risk</div>', unsafe_allow_html=True)
        
        st.markdown("""
        <div class="welcome-banner">
            <div style="display: flex; align-items: flex-start; gap: 12px;">
                <span style="font-size: 1.5rem;">🔮</span>
                <div>
                    <div style="color: #eab308; font-weight: 500; margin-bottom: 0.25rem;">Catch problems early</div>
                    <div style="color: #a0a0a0; font-size: 0.85rem;">These students aren't at-risk yet, but show warning signs. Early action can prevent bigger issues.</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        try:
            warnings_df = get_early_warning_students()
            
            if warnings_df.empty:
                st.markdown("""
                <div class="panel" style="text-align: center; padding: 3rem;">
                    <div style="font-size: 3rem; margin-bottom: 1rem;">✅</div>
                    <div style="color: #22c55e; font-size: 1.1rem;">No early warnings</div>
                    <div style="color: #606060; font-size: 0.9rem; margin-top: 0.5rem;">All students are stable right now.</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                # Stats
                high_count = len(warnings_df[warnings_df['WARNING_LEVEL'] == 'High'])
                med_count = len(warnings_df[warnings_df['WARNING_LEVEL'] == 'Medium'])
                low_count = len(warnings_df[warnings_df['WARNING_LEVEL'] == 'Low'])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">High Priority</div>
                        <div class="metric-value red">{high_count}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">Medium Priority</div>
                        <div class="metric-value yellow">{med_count}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">Low Priority</div>
                        <div class="metric-value">{low_count}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                # Warning indicators legend
                st.markdown("""
                <div class="panel" style="padding: 0.75rem 1rem;">
                    <div style="color: #808080; font-size: 0.8rem;">
                        <strong>Warning Signs:</strong> 
                        📉 Attendance dropping • 📚 Grades declining • 😟 Negative sentiment • ⚠️ Multiple concerning notes
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                # List students
                for _, student in warnings_df.iterrows():
                    level = student['WARNING_LEVEL']
                    border_color = 'rgba(239, 68, 68, 0.3)' if level == 'High' else 'rgba(234, 179, 8, 0.3)' if level == 'Medium' else 'rgba(96, 96, 96, 0.3)'
                    level_color = '#ef4444' if level == 'High' else '#eab308' if level == 'Medium' else '#808080'
                    
                    # Build warning badges
                    warnings = []
                    if student['ATTENDANCE_WARNING']:
                        warnings.append(f"📉 Attendance -{student['ATTENDANCE_DROP']:.0f}%")
                    if student['GRADE_WARNING']:
                        warnings.append(f"📚 Grades -{student['GRADE_DROP']:.0f}%")
                    if student['SENTIMENT_WARNING']:
                        warnings.append(f"😟 Sentiment drop")
                    if student['NOTES_WARNING']:
                        warnings.append(f"⚠️ {int(student['HIGH_RISK_NOTE_COUNT'])} concerning notes")
                    
                    st.markdown(f"""
                    <div class="panel" style="border-color: {border_color}; margin-bottom: 1rem;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
                            <div>
                                <span style="font-weight: 500; color: #e0e0e0;">{student['STUDENT_NAME']}</span>
                                <span style="color: #606060; font-size: 0.85rem;"> • Grade {int(student['GRADE_LEVEL'])}</span>
                            </div>
                            <div style="background: {border_color}; color: {level_color}; padding: 0.25rem 0.75rem; border-radius: 4px; font-size: 0.75rem; font-weight: 500;">
                                {level} Priority
                            </div>
                        </div>
                        <div style="display: flex; flex-wrap: wrap; gap: 0.5rem; margin-top: 0.5rem;">
                            {''.join([f'<span style="background: #1a1a1a; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.8rem; color: #a0a0a0;">{w}</span>' for w in warnings])}
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
        except Exception as e:
            st.error(f"Error: {e}")
            st.info("Run sql/12_early_warning_system.sql to set up early warnings.")


    # ============================================
    # PAGE: UPLOAD DATA (Teacher Friendly)
    # ============================================
    
    elif page == "upload":
        st.markdown('<div class="page-header">📤 Upload Data</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Import student records, attendance, or grades from your files</div>', unsafe_allow_html=True)
        
        # Friendly intro
        st.markdown("""
        <div class="panel" style="background: linear-gradient(135deg, rgba(34, 197, 94, 0.1) 0%, rgba(34, 197, 94, 0.05) 100%); border-color: rgba(34, 197, 94, 0.2);">
            <div style="display: flex; align-items: center; gap: 12px;">
                <span style="font-size: 1.5rem;">💡</span>
                <div>
                    <div style="color: #22c55e; font-weight: 500;">Easy Import</div>
                    <div style="color: #808080; font-size: 0.85rem;">Upload CSV files from Excel, Google Sheets, or your school system exports.</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            data_type = st.selectbox(
                "What type of data are you uploading?",
                ["students", "attendance", "grades"],
                format_func=lambda x: {
                    "students": "👥 Student Roster (names, grade levels, contact info)", 
                    "attendance": "📅 Attendance Records (present, absent, tardy)", 
                    "grades": "📝 Grade Data (scores and assignments)"
                }[x]
            )
            
            st.markdown('<div style="color: #808080; font-size: 0.85rem; margin: 0.5rem 0 1rem 0;">Supported formats: CSV, Excel (.xlsx)</div>', unsafe_allow_html=True)
            
            uploaded_file = st.file_uploader("Choose your file", type=['csv', 'xlsx'], label_visibility="collapsed")
            
            if uploaded_file:
                try:
                    df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
                    
                    st.markdown(f"""
                    <div class="panel" style="margin: 1rem 0;">
                        <div style="color: #22c55e; font-weight: 500;">✓ File loaded: {uploaded_file.name}</div>
                        <div style="color: #808080; font-size: 0.85rem; margin-top: 0.25rem;">{len(df)} records found with {len(df.columns)} columns</div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    with st.expander("Preview data", expanded=True):
                        st.dataframe(df.head(10), use_container_width=True)
                        if len(df) > 10:
                            st.caption(f"Showing first 10 of {len(df)} records")
                    
                    if st.button("📥 Import Data", use_container_width=True, type="primary"):
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
                                
                                st.success(f"🎉 Success! {len(df)} records imported.")
                                st.balloons()
                                st.info("💡 Data will appear in reports within a few minutes.")
                                st.cache_data.clear()
                            except Exception as e:
                                st.error("Import failed. Please check your file format.")
                                with st.expander("Technical details"):
                                    st.code(str(e))
                except Exception as e:
                    st.error("Could not read this file. Please make sure it's a valid CSV or Excel file.")
        
        with col2:
            st.markdown("""
            <div class="panel">
                <div class="panel-title" style="margin-bottom: 1rem;">📋 Required Columns</div>
            """, unsafe_allow_html=True)
            
            if data_type == "students":
                st.markdown("""
                <div style="color: #808080; font-size: 0.85rem; margin-bottom: 0.75rem;">
                    • <strong>student_id</strong> - Unique ID<br>
                    • <strong>first_name</strong><br>
                    • <strong>last_name</strong><br>
                    • <strong>grade_level</strong> - (9, 10, etc.)
                </div>
                """, unsafe_allow_html=True)
                st.code("student_id,first_name,last_name,grade_level\nSTU001,John,Doe,9", language="csv")
            elif data_type == "attendance":
                st.markdown("""
                <div style="color: #808080; font-size: 0.85rem; margin-bottom: 0.75rem;">
                    • <strong>student_id</strong><br>
                    • <strong>date</strong> - (YYYY-MM-DD)<br>
                    • <strong>status</strong> - Present/Absent/Tardy<br>
                    • <strong>period</strong> - (optional)
                </div>
                """, unsafe_allow_html=True)
                st.code("student_id,date,status,period\nSTU001,2024-12-30,Present,1", language="csv")
            else:
                st.markdown("""
                <div style="color: #808080; font-size: 0.85rem; margin-bottom: 0.75rem;">
                    • <strong>student_id</strong><br>
                    • <strong>course</strong> - Course name<br>
                    • <strong>assignment</strong><br>
                    • <strong>score</strong> / <strong>max_score</strong>
                </div>
                """, unsafe_allow_html=True)
                st.code("student_id,course,assignment,score,max_score\nSTU001,Algebra I,Quiz 1,85,100", language="csv")
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            st.markdown("""
            <div class="panel" style="margin-top: 1rem;">
                <div class="panel-title" style="margin-bottom: 0.5rem;">💡 Tips</div>
                <div style="color: #606060; font-size: 0.8rem;">
                    • Excel: File → Save As → CSV<br>
                    • Google Sheets: File → Download → CSV<br>
                    • Column names must match exactly
                </div>
            </div>
            """, unsafe_allow_html=True)

    # ============================================
    # PAGE: SUCCESS PLANS
    # ============================================
    
    elif page == "plans":
        st.markdown('<div class="page-header">🎯 Success Plans</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">AI-powered intervention strategies for at-risk students</div>', unsafe_allow_html=True)
        
        # Guidance banner
        st.markdown("""
        <div class="welcome-banner">
            <div style="display: flex; align-items: flex-start; gap: 12px;">
                <span style="font-size: 1.5rem;">🤖</span>
                <div>
                    <div style="color: #22c55e; font-weight: 500; margin-bottom: 0.25rem;">AI-Powered Recommendations</div>
                    <div style="color: #a0a0a0; font-size: 0.85rem;">Select a student below and click "Generate Success Plan" to get personalized intervention strategies based on their attendance, grades, and risk factors.</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        try:
            at_risk_df = get_at_risk_students()
            
            if not at_risk_df.empty:
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    st.markdown("""
                    <div class="panel">
                        <div class="panel-title" style="margin-bottom: 1rem;">👤 Select Student</div>
                    """, unsafe_allow_html=True)
                    
                    selected = st.selectbox("Choose a student", options=at_risk_df['STUDENT_NAME'].tolist(), label_visibility="collapsed", help="Students shown here have been flagged as at-risk")
                    student_data = at_risk_df[at_risk_df['STUDENT_NAME'] == selected].iloc[0].to_dict()
                    
                    color = "red" if student_data['RISK_SCORE'] >= 70 else "yellow" if student_data['RISK_SCORE'] >= 50 else ""
                    risk_label = "Critical" if student_data['RISK_SCORE'] >= 70 else "High" if student_data['RISK_SCORE'] >= 50 else "Moderate"
                    
                    st.markdown(f"""
                    <div class="metric-box" style="margin-top: 1rem; text-align: center;">
                        <div class="metric-label">Risk Level</div>
                        <div class="metric-value {color}" style="font-size: 1.5rem;">{risk_label}</div>
                        <div style="color: #606060; font-size: 0.8rem;">Score: {student_data['RISK_SCORE']}</div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    st.markdown("<br>", unsafe_allow_html=True)
                    
                    c1, c2 = st.columns(2)
                    with c1:
                        st.metric("Attendance", f"{student_data['ATTENDANCE_RATE']}%")
                    with c2:
                        st.metric("GPA", f"{student_data['CURRENT_GPA']:.1f}")
                    
                    st.markdown("</div>", unsafe_allow_html=True)
                    
                    # Risk Breakdown Section
                    st.markdown("""
                    <div class="panel" style="margin-top: 1rem;">
                        <div class="panel-title" style="margin-bottom: 0.75rem;">🔍 Why This Score?</div>
                    """, unsafe_allow_html=True)
                    
                    try:
                        # Get risk breakdown data
                        breakdown = session.sql(f"""
                            SELECT * FROM GRADSYNC_DB.ANALYTICS.RISK_BREAKDOWN 
                            WHERE student_name = '{selected}'
                        """).collect()
                        
                        if breakdown:
                            bd = breakdown[0]
                            att_contrib = float(bd['ATTENDANCE_RISK_CONTRIBUTION'] or 0)
                            acad_contrib = float(bd['ACADEMIC_RISK_CONTRIBUTION'] or 0)
                            sent_contrib = float(bd['SENTIMENT_RISK_CONTRIBUTION'] or 0)
                            ai_contrib = float(bd['AI_SIGNAL_RISK_CONTRIBUTION'] or 0)
                            primary_factor = bd['PRIMARY_RISK_FACTOR']
                            has_new_signal = bd['HAS_NEW_SIGNAL']
                            
                            # Show primary factor
                            st.markdown(f"""
                            <div style="background: rgba(239, 68, 68, 0.1); border-radius: 8px; padding: 0.75rem; margin-bottom: 1rem;">
                                <span style="color: #ef4444; font-weight: 500;">Primary Factor:</span> {primary_factor}
                            </div>
                            """, unsafe_allow_html=True)
                            
                            # New signal badge
                            if has_new_signal:
                                st.markdown("""
                                <div style="background: rgba(234, 179, 8, 0.15); border-radius: 8px; padding: 0.5rem; margin-bottom: 1rem; text-align: center;">
                                    <span style="color: #eab308;">⚠️ NEW: Attendance declining</span>
                                </div>
                                """, unsafe_allow_html=True)
                            
                            # Factor breakdown bars
                            factors = [
                                ("📅 Attendance", att_contrib, "#ef4444"),
                                ("📚 Academic", acad_contrib, "#f59e0b"),
                                ("💬 Sentiment", sent_contrib, "#8b5cf6"),
                                ("🔔 AI Signals", ai_contrib, "#3b82f6")
                            ]
                            
                            for name, value, color in factors:
                                pct = min(100, value * 4)  # Scale to percentage (max 25 per factor)
                                st.markdown(f"""
                                <div style="margin-bottom: 0.5rem;">
                                    <div style="display: flex; justify-content: space-between; font-size: 0.8rem; color: #808080;">
                                        <span>{name}</span>
                                        <span>{value:.1f} pts</span>
                                    </div>
                                    <div style="background: #1a1a1a; border-radius: 4px; height: 8px; overflow: hidden;">
                                        <div style="background: {color}; width: {pct}%; height: 100%;"></div>
                                    </div>
                                </div>
                                """, unsafe_allow_html=True)
                        else:
                            st.caption("Risk breakdown not available - no data for this student")
                    except Exception as e:
                        st.caption(f"Risk breakdown error: {str(e)}")
                    
                    st.markdown("</div>", unsafe_allow_html=True)
                
                with col2:
                    st.markdown("""
                    <div class="panel">
                        <div class="panel-title" style="margin-bottom: 1rem;">📋 Intervention Plan</div>
                    """, unsafe_allow_html=True)
                    
                    if st.button("🤖 Generate Success Plan", use_container_width=True, type="primary"):
                        with st.spinner("AI is analyzing student data and generating recommendations..."):
                            try:
                                plan = generate_success_plan(student_data)
                                
                                st.markdown("""
                                <div style="background: rgba(34, 197, 94, 0.1); border-radius: 8px; padding: 0.75rem; margin-bottom: 1rem;">
                                    <span style="color: #22c55e;">✓ Plan generated successfully</span>
                                </div>
                                """, unsafe_allow_html=True)
                                
                                st.markdown(plan)
                                
                                st.markdown("<hr>", unsafe_allow_html=True)
                                st.markdown('<div class="panel-title" style="margin-bottom: 0.75rem;">📧 Contact Parent</div>', unsafe_allow_html=True)
                                
                                student_id = student_data.get('STUDENT_ID', '')
                                parent_lang = get_parent_language(student_id) if student_id else 'English'
                                
                                if parent_lang != 'English':
                                    st.markdown(f'<div class="info-tip">🌐 This parent prefers communication in <strong>{parent_lang}</strong></div>', unsafe_allow_html=True)
                                
                                available_langs = ['English'] + list(SUPPORTED_LANGUAGES.keys())
                                selected_lang = st.selectbox("Translate message to:", options=available_langs,
                                    index=available_langs.index(parent_lang) if parent_lang in available_langs else 0)
                                
                                default_msg = f"Dear Parent/Guardian,\n\nI wanted to reach out regarding {selected}'s progress. I'd like to schedule a meeting to discuss strategies we can implement together.\n\nBest regards,\n[Teacher Name]"
                                message = st.text_area("Message to parent", value=default_msg, height=120)
                                
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
    # PAGE: AUTO-SYNC STATUS
    # ============================================
    
    elif page == "autosync":
        st.markdown('<div class="page-header">🔄 Data Import Center</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Easily import attendance, grades, and student data into GradSync</div>', unsafe_allow_html=True)
        
        # Friendly intro for teachers
        st.markdown("""
        <div class="panel" style="background: linear-gradient(135deg, rgba(34, 197, 94, 0.1) 0%, rgba(34, 197, 94, 0.05) 100%); border-color: rgba(34, 197, 94, 0.2);">
            <div style="display: flex; align-items: center; gap: 12px;">
                <span style="font-size: 1.5rem;">💡</span>
                <div>
                    <div style="color: #22c55e; font-weight: 500;">Quick Import</div>
                    <div style="color: #808080; font-size: 0.85rem;">Upload data files below to quickly add attendance records, grades, or new students to the system.</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Data Connection Status (simplified)
        st.markdown('<div class="panel-title">📊 Connection Status</div>', unsafe_allow_html=True)
        st.markdown('<div style="color: #606060; font-size: 0.85rem; margin-bottom: 1rem;">Shows whether automatic data feeds from your school systems are active</div>', unsafe_allow_html=True)
        
        col1, col2, col3 = st.columns(3)
        
        def get_pipe_status(pipe_name):
            try:
                result = session.sql(f"SELECT SYSTEM$PIPE_STATUS('RAW_DATA.{pipe_name}') as status").collect()
                import json
                status_json = json.loads(result[0]['STATUS'])
                return {
                    'state': status_json.get('executionState', 'UNKNOWN'),
                    'pending': status_json.get('pendingFileCount', 0),
                    'last_ingestion': status_json.get('lastIngestedTimestamp', 'Never')
                }
            except:
                return {'state': 'NOT_CONFIGURED', 'pending': 0, 'last_ingestion': 'N/A'}
        
        pipes = [
            ('ATTENDANCE_PIPE', 'Attendance', '📅', 'From check-in systems'),
            ('GRADES_PIPE', 'Grades', '📝', 'From gradebook/LMS'),
            ('STUDENTS_PIPE', 'Students', '👥', 'From student info system')
        ]
        
        for i, (pipe_name, label, icon, desc) in enumerate(pipes):
            status = get_pipe_status(pipe_name)
            state = status['state']
            
            if state == 'RUNNING':
                badge_class = 'badge-green'
                badge_text = '✓ Connected'
                status_desc = 'Receiving data automatically'
            elif state == 'PAUSED':
                badge_class = 'badge-yellow'
                badge_text = '⏸ Paused'
                status_desc = 'Temporarily stopped'
            elif state == 'NOT_CONFIGURED':
                badge_class = 'badge-yellow'
                badge_text = 'Manual Only'
                status_desc = 'Use file upload below'
            else:
                badge_class = 'badge-red'
                badge_text = '⚠ Issue'
                status_desc = 'Contact IT support'
            
            with [col1, col2, col3][i]:
                st.markdown(f"""
                <div class="metric-box" style="text-align: center;">
                    <div style="font-size: 2rem;">{icon}</div>
                    <div class="metric-label">{label}</div>
                    <div class="badge {badge_class}" style="margin-top: 0.5rem;">{badge_text}</div>
                    <div style="color: #505050; font-size: 0.75rem; margin-top: 0.5rem;">
                        {status_desc}
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        st.markdown("<hr>", unsafe_allow_html=True)
        
        # Recent Activity (simplified)
        col_left, col_right = st.columns([2, 1])
        
        with col_left:
            st.markdown("""
            <div class="panel">
                <div class="panel-header">
                    <span class="panel-title">📥 Recent Imports (Last 24 Hours)</span>
                    <span class="badge badge-green">Live</span>
                </div>
            """, unsafe_allow_html=True)
            
            try:
                metrics_df = session.sql("""
                    SELECT 
                        'Attendance' as data_type,
                        COUNT(*) as records,
                        MAX(ingested_at) as last_ingestion
                    FROM RAW_DATA.ATTENDANCE_EVENTS_LANDING
                    WHERE ingested_at >= DATEADD(hours, -24, CURRENT_TIMESTAMP())
                    UNION ALL
                    SELECT 'Grades', COUNT(*), MAX(ingested_at)
                    FROM RAW_DATA.GRADE_EVENTS_LANDING
                    WHERE ingested_at >= DATEADD(hours, -24, CURRENT_TIMESTAMP())
                    UNION ALL
                    SELECT 'Students', COUNT(*), MAX(ingested_at)
                    FROM RAW_DATA.STUDENT_EVENTS_LANDING
                    WHERE ingested_at >= DATEADD(hours, -24, CURRENT_TIMESTAMP())
                """).to_pandas()
                
                if not metrics_df.empty and metrics_df['RECORDS'].sum() > 0:
                    for _, row in metrics_df.iterrows():
                        if row['RECORDS'] > 0:
                            icon = "📅" if row['DATA_TYPE'] == 'Attendance' else "📝" if row['DATA_TYPE'] == 'Grades' else "👥"
                            st.markdown(f"""
                            <div class="student-row">
                                <span style="margin-right: 10px;">{icon}</span>
                                <span class="student-name">{row['DATA_TYPE']}</span>
                                <span class="student-info" style="color: #22c55e;">{row['RECORDS']} new records</span>
                            </div>
                            """, unsafe_allow_html=True)
                    if metrics_df['RECORDS'].sum() == 0:
                        st.markdown('<div style="color: #606060; padding: 1rem;">No new data imported today. Use the upload section below to add data.</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div style="color: #606060; padding: 1rem;">No new data imported today. Use the upload section below to add data.</div>', unsafe_allow_html=True)
            except Exception as e:
                st.markdown('<div style="color: #606060; padding: 1rem;">Ready to receive data. Use the upload section below to get started.</div>', unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        with col_right:
            st.markdown("""
            <div class="panel">
                <div class="panel-header">
                    <span class="panel-title">✅ System Health</span>
                </div>
            """, unsafe_allow_html=True)
            
            try:
                tasks_df = session.sql("""
                    SELECT name, state
                    FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
                        SCHEDULED_TIME_RANGE_START => DATEADD(hours, -24, CURRENT_TIMESTAMP())
                    ))
                    WHERE name LIKE 'PROCESS_%'
                    ORDER BY scheduled_time DESC
                    LIMIT 3
                """).to_pandas()
                
                if not tasks_df.empty:
                    succeeded = (tasks_df['STATE'] == 'SUCCEEDED').sum()
                    total = len(tasks_df)
                    if succeeded == total:
                        st.markdown('<div style="color: #22c55e; padding: 0.5rem;">✓ All systems running smoothly</div>', unsafe_allow_html=True)
                    else:
                        st.markdown(f'<div style="color: #eab308; padding: 0.5rem;">⚠ {succeeded}/{total} processes OK</div>', unsafe_allow_html=True)
                else:
                    st.markdown('<div style="color: #22c55e; padding: 0.5rem;">✓ System ready</div>', unsafe_allow_html=True)
            except:
                st.markdown('<div style="color: #22c55e; padding: 0.5rem;">✓ System ready</div>', unsafe_allow_html=True)
            
            st.markdown("</div>", unsafe_allow_html=True)
        
        st.markdown("<hr>", unsafe_allow_html=True)
        
        # Simplified Error Section
        try:
            errors_df = session.sql("""
                SELECT COUNT(*) as error_count
                FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
                    TABLE_NAME => 'ATTENDANCE_EVENTS_LANDING',
                    START_TIME => DATEADD(days, -7, CURRENT_TIMESTAMP())
                ))
                WHERE status = 'LOAD_FAILED'
            """).collect()
            
            if errors_df[0]['ERROR_COUNT'] > 0:
                st.markdown(f"""
                <div class="panel" style="border-color: rgba(239, 68, 68, 0.3);">
                    <div style="display: flex; align-items: center; gap: 12px;">
                        <span style="font-size: 1.5rem;">⚠️</span>
                        <div>
                            <div style="color: #ef4444; font-weight: 500;">{errors_df[0]['ERROR_COUNT']} Import Issues</div>
                            <div style="color: #808080; font-size: 0.85rem;">Some files couldn't be imported. Contact IT if this persists.</div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
        except:
            pass  # No errors to show
        
        # Main Upload Section - Teacher Friendly
        st.markdown('<div class="panel-title">📤 Import Data</div>', unsafe_allow_html=True)
        st.markdown('<div style="color: #606060; font-size: 0.85rem; margin-bottom: 1rem;">Upload a file to add new records to GradSync</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            test_type = st.selectbox(
                "What type of data are you importing?",
                ["attendance", "grades", "students"],
                format_func=lambda x: {
                    "attendance": "📅 Attendance Records (who was present/absent)", 
                    "grades": "📝 Grade Data (scores and assignments)", 
                    "students": "👥 Student Information (new students or updates)"
                }[x]
            )
            
            st.markdown('<div style="color: #808080; font-size: 0.85rem; margin: 0.5rem 0 1rem 0;">Upload a JSON file exported from your school system</div>', unsafe_allow_html=True)
            
            test_file = st.file_uploader("Choose file", type=['json'], label_visibility="collapsed")
            
            if test_file:
                try:
                    import json
                    test_data = json.load(test_file)
                    data_list = test_data if isinstance(test_data, list) else [test_data]
                    
                    st.markdown(f"""
                    <div class="panel" style="margin: 1rem 0;">
                        <div style="color: #22c55e; font-weight: 500;">✓ File loaded successfully</div>
                        <div style="color: #808080; font-size: 0.85rem; margin-top: 0.25rem;">{len(data_list)} record(s) ready to import</div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    with st.expander("Preview data"):
                        st.json(data_list[:3] if len(data_list) > 3 else data_list)
                        if len(data_list) > 3:
                            st.caption(f"...and {len(data_list) - 3} more records")
                    
                    if st.button("📥 Import Data", use_container_width=True, type="primary"):
                        with st.spinner("Importing your data..."):
                            try:
                                records_inserted = 0
                                
                                for record in data_list:
                                    record_json = json.dumps(record).replace("'", "''")
                                    
                                    if test_type == "attendance":
                                        session.sql(f"""
                                            INSERT INTO RAW_DATA.ATTENDANCE_EVENTS_LANDING 
                                            (event_id, student_id, event_timestamp, event_type, location, raw_payload)
                                            SELECT 
                                                $1:event_id::VARCHAR,
                                                $1:student_id::VARCHAR,
                                                TRY_TO_TIMESTAMP($1:timestamp::VARCHAR),
                                                $1:type::VARCHAR,
                                                $1:location::VARCHAR,
                                                PARSE_JSON('{record_json}')
                                            FROM (SELECT PARSE_JSON('{record_json}') as $1)
                                        """).collect()
                                    elif test_type == "grades":
                                        session.sql(f"""
                                            INSERT INTO RAW_DATA.GRADE_EVENTS_LANDING 
                                            (event_id, student_id, course_name, assignment_name, score, max_score, grade_date, raw_payload)
                                            SELECT 
                                                $1:event_id::VARCHAR,
                                                $1:student_id::VARCHAR,
                                                $1:course::VARCHAR,
                                                $1:assignment::VARCHAR,
                                                TRY_TO_DECIMAL($1:score::VARCHAR, 5, 2),
                                                TRY_TO_DECIMAL($1:max_score::VARCHAR, 5, 2),
                                                TRY_TO_DATE($1:date::VARCHAR),
                                                PARSE_JSON('{record_json}')
                                            FROM (SELECT PARSE_JSON('{record_json}') as $1)
                                        """).collect()
                                    else:  # students
                                        session.sql(f"""
                                            INSERT INTO RAW_DATA.STUDENT_EVENTS_LANDING 
                                            (event_id, student_id, first_name, last_name, grade_level, parent_email, parent_language, event_type, raw_payload)
                                            SELECT 
                                                $1:event_id::VARCHAR,
                                                $1:student_id::VARCHAR,
                                                $1:first_name::VARCHAR,
                                                $1:last_name::VARCHAR,
                                                TRY_TO_NUMBER($1:grade_level::VARCHAR),
                                                $1:parent_email::VARCHAR,
                                                COALESCE($1:parent_language::VARCHAR, 'English'),
                                                $1:event_type::VARCHAR,
                                                PARSE_JSON('{record_json}')
                                            FROM (SELECT PARSE_JSON('{record_json}') as $1)
                                        """).collect()
                                    records_inserted += 1
                                
                                st.success(f"🎉 Success! {records_inserted} records imported.")
                                st.balloons()
                                st.info("💡 Data will appear in reports within a few minutes.")
                            except Exception as e:
                                st.error(f"Import failed. Please check your file format and try again.")
                                with st.expander("Technical details"):
                                    st.code(str(e))
                except Exception as e:
                    st.error("This file doesn't appear to be valid JSON. Please check the format.")
        
        with col2:
            st.markdown("""
            <div class="panel">
                <div class="panel-title" style="margin-bottom: 1rem;">📋 File Format Help</div>
            """, unsafe_allow_html=True)
            
            if test_type == "attendance":
                st.markdown("""
                <div style="color: #808080; font-size: 0.85rem; margin-bottom: 0.75rem;">
                    <strong>Required fields:</strong><br>
                    • student_id - Student's ID<br>
                    • timestamp - Date and time<br>
                    • type - check_in, late_arrival, or no_show
                </div>
                """, unsafe_allow_html=True)
                st.code('''[{
  "event_id": "ATT-001",
  "student_id": "STU001",
  "timestamp": "2024-12-30T08:15:00Z",
  "type": "check_in",
  "location": "Main Entrance"
}]''', language="json")
            elif test_type == "grades":
                st.markdown("""
                <div style="color: #808080; font-size: 0.85rem; margin-bottom: 0.75rem;">
                    <strong>Required fields:</strong><br>
                    • student_id - Student's ID<br>
                    • course - Course name<br>
                    • score / max_score - Points earned
                </div>
                """, unsafe_allow_html=True)
                st.code('''[{
  "event_id": "GRD-001",
  "student_id": "STU001",
  "course": "Algebra I",
  "assignment": "Quiz 1",
  "score": 85.5,
  "max_score": 100,
  "date": "2024-12-30"
}]''', language="json")
            else:
                st.markdown("""
                <div style="color: #808080; font-size: 0.85rem; margin-bottom: 0.75rem;">
                    <strong>Required fields:</strong><br>
                    • student_id - Unique ID<br>
                    • first_name / last_name<br>
                    • grade_level - Grade number
                </div>
                """, unsafe_allow_html=True)
                st.code('''[{
  "event_id": "STU-001",
  "student_id": "STU005",
  "first_name": "Maria",
  "last_name": "Garcia",
  "grade_level": 10,
  "parent_email": "parent@email.com",
  "event_type": "create"
}]''', language="json")
            
            st.markdown("</div>", unsafe_allow_html=True)
            
            # Help section
            st.markdown("""
            <div class="panel" style="margin-top: 1rem;">
                <div class="panel-title" style="margin-bottom: 0.5rem;">❓ Need Help?</div>
                <div style="color: #606060; font-size: 0.8rem;">
                    Contact your IT department for help exporting data from your school systems.
                </div>
            </div>
            """, unsafe_allow_html=True)

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
