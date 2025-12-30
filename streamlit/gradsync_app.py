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
    layout="wide",
    initial_sidebar_state="expanded"
)

# Modern CSS with glassmorphism and animations
st.markdown("""
<style>
    /* Import Google Font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    * {
        font-family: 'Inter', sans-serif;
    }
    
    /* Hide default Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    header {visibility: hidden;}
    
    /* Sidebar styling - Dark modern theme */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0f0f23 0%, #1a1a3e 50%, #0f0f23 100%);
        border-right: 1px solid rgba(255,255,255,0.1);
    }
    
    [data-testid="stSidebar"] > div:first-child {
        padding-top: 2rem;
    }
    
    /* Navigation buttons */
    .nav-button {
        display: flex;
        align-items: center;
        gap: 12px;
        padding: 14px 20px;
        margin: 8px 0;
        border-radius: 12px;
        cursor: pointer;
        transition: all 0.3s ease;
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.05);
        color: rgba(255,255,255,0.7);
        text-decoration: none;
    }
    
    .nav-button:hover {
        background: rgba(99, 102, 241, 0.2);
        border-color: rgba(99, 102, 241, 0.3);
        color: white;
        transform: translateX(5px);
    }
    
    .nav-button.active {
        background: linear-gradient(135deg, rgba(99, 102, 241, 0.3) 0%, rgba(168, 85, 247, 0.3) 100%);
        border-color: rgba(99, 102, 241, 0.5);
        color: white;
        box-shadow: 0 4px 15px rgba(99, 102, 241, 0.2);
    }
    
    .nav-icon {
        font-size: 1.3rem;
    }
    
    .nav-text {
        font-weight: 500;
        font-size: 0.95rem;
    }
    
    /* Logo section */
    .logo-section {
        text-align: center;
        padding: 1.5rem 1rem 2rem 1rem;
        border-bottom: 1px solid rgba(255,255,255,0.1);
        margin-bottom: 1.5rem;
    }
    
    .logo-text {
        font-size: 1.8rem;
        font-weight: 700;
        background: linear-gradient(135deg, #6366f1 0%, #a855f7 50%, #ec4899 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin: 0;
    }
    
    .logo-subtitle {
        color: rgba(255,255,255,0.5);
        font-size: 0.75rem;
        margin-top: 4px;
        letter-spacing: 1px;
    }
    
    /* Main content area */
    .main-content {
        padding: 0 1rem;
    }
    
    /* Hero header with gradient */
    .hero-header {
        background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 50%, #a855f7 100%);
        border-radius: 20px;
        padding: 2.5rem;
        margin-bottom: 2rem;
        position: relative;
        overflow: hidden;
        box-shadow: 0 20px 40px rgba(99, 102, 241, 0.3);
    }
    
    .hero-header::before {
        content: '';
        position: absolute;
        top: -50%;
        right: -50%;
        width: 100%;
        height: 200%;
        background: radial-gradient(circle, rgba(255,255,255,0.1) 0%, transparent 70%);
    }
    
    .hero-header h1 {
        color: white;
        font-size: 2.2rem;
        font-weight: 700;
        margin: 0;
        position: relative;
        z-index: 1;
    }
    
    .hero-header p {
        color: rgba(255,255,255,0.85);
        font-size: 1rem;
        margin: 0.5rem 0 0 0;
        position: relative;
        z-index: 1;
    }
    
    /* Glassmorphism cards */
    .glass-card {
        background: rgba(255, 255, 255, 0.95);
        backdrop-filter: blur(10px);
        border-radius: 16px;
        padding: 1.5rem;
        border: 1px solid rgba(255,255,255,0.2);
        box-shadow: 0 8px 32px rgba(0,0,0,0.08);
        transition: all 0.3s ease;
    }
    
    .glass-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 12px 40px rgba(0,0,0,0.12);
    }
    
    /* Metric cards with gradients */
    .metric-card {
        background: linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
        border-radius: 16px;
        padding: 1.5rem;
        text-align: center;
        border: 1px solid #e2e8f0;
        transition: all 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 10px 30px rgba(0,0,0,0.1);
    }
    
    .metric-card.danger {
        background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
        border-color: #fecaca;
    }
    
    .metric-card.warning {
        background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%);
        border-color: #fde68a;
    }
    
    .metric-card.success {
        background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%);
        border-color: #bbf7d0;
    }
    
    .metric-card.info {
        background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
        border-color: #bfdbfe;
    }
    
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: #1e293b;
        line-height: 1;
    }
    
    .metric-label {
        font-size: 0.85rem;
        color: #64748b;
        margin-top: 0.5rem;
        font-weight: 500;
    }
    
    /* Student cards */
    .student-card {
        background: white;
        border-radius: 16px;
        padding: 1.25rem;
        margin: 0.75rem 0;
        border-left: 4px solid #6366f1;
        box-shadow: 0 4px 15px rgba(0,0,0,0.05);
        transition: all 0.3s ease;
    }
    
    .student-card:hover {
        transform: translateX(5px);
        box-shadow: 0 8px 25px rgba(0,0,0,0.1);
    }
    
    .student-card.critical {
        border-left-color: #ef4444;
        background: linear-gradient(90deg, #fef2f2 0%, white 20%);
    }
    
    .student-card.high {
        border-left-color: #f97316;
        background: linear-gradient(90deg, #fff7ed 0%, white 20%);
    }
    
    .student-card.moderate {
        border-left-color: #eab308;
        background: linear-gradient(90deg, #fefce8 0%, white 20%);
    }
    
    .student-name {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1e293b;
    }
    
    .student-grade {
        color: #64748b;
        font-size: 0.85rem;
    }
    
    .student-stats {
        display: flex;
        gap: 1.5rem;
        margin-top: 0.75rem;
        flex-wrap: wrap;
    }
    
    .stat-item {
        display: flex;
        align-items: center;
        gap: 0.4rem;
        font-size: 0.85rem;
        color: #475569;
    }
    
    /* Risk badges */
    .risk-badge {
        display: inline-flex;
        align-items: center;
        padding: 0.35rem 0.85rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .risk-badge.critical {
        background: #ef4444;
        color: white;
    }
    
    .risk-badge.high {
        background: #f97316;
        color: white;
    }
    
    .risk-badge.moderate {
        background: #eab308;
        color: #1e293b;
    }
    
    /* Section headers */
    .section-header {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin: 1.5rem 0 1rem 0;
    }
    
    .section-header h2 {
        font-size: 1.25rem;
        font-weight: 600;
        color: #1e293b;
        margin: 0;
    }
    
    .section-icon {
        font-size: 1.5rem;
    }
    
    /* Quick stats in sidebar */
    .sidebar-stats {
        background: rgba(255,255,255,0.05);
        border-radius: 12px;
        padding: 1rem;
        margin-top: 1.5rem;
    }
    
    .sidebar-stat {
        display: flex;
        justify-content: space-between;
        padding: 0.5rem 0;
        border-bottom: 1px solid rgba(255,255,255,0.05);
    }
    
    .sidebar-stat:last-child {
        border-bottom: none;
    }
    
    .sidebar-stat-label {
        color: rgba(255,255,255,0.6);
        font-size: 0.8rem;
    }
    
    .sidebar-stat-value {
        color: white;
        font-weight: 600;
        font-size: 0.9rem;
    }
    
    /* Form styling */
    .stTextArea textarea {
        border-radius: 12px !important;
        border: 2px solid #e2e8f0 !important;
    }
    
    .stTextArea textarea:focus {
        border-color: #6366f1 !important;
        box-shadow: 0 0 0 3px rgba(99, 102, 241, 0.1) !important;
    }
    
    .stSelectbox > div > div {
        border-radius: 12px !important;
    }
    
    /* Button styling */
    .stButton > button {
        border-radius: 12px !important;
        font-weight: 600 !important;
        padding: 0.75rem 2rem !important;
        transition: all 0.3s ease !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 20px rgba(99, 102, 241, 0.3) !important;
    }
    
    /* Divider */
    .custom-divider {
        height: 1px;
        background: linear-gradient(90deg, transparent, #e2e8f0, transparent);
        margin: 2rem 0;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# SIDEBAR NAVIGATION
# ============================================

with st.sidebar:
    # Logo
    st.markdown("""
    <div class="logo-section">
        <p class="logo-text">🎓 GradSync</p>
        <p class="logo-subtitle">STUDENT SUCCESS PLATFORM</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Navigation
    pages = {
        "📊 Dashboard": "dashboard",
        "📝 Log Observation": "observation",
        "📤 Upload Data": "upload",
        "🎯 Success Plans": "plans",
        "⚙️ Settings": "settings"
    }
    
    page = st.radio("Navigation", list(pages.keys()), label_visibility="collapsed")
    
    # Quick stats
    st.markdown("""<div class="sidebar-stats">""", unsafe_allow_html=True)
    
    try:
        stats = session.sql("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN risk_score >= 50 THEN 1 ELSE 0 END) as at_risk
            FROM ANALYTICS.STUDENT_360_VIEW
        """).collect()[0]
        
        st.markdown(f"""
        <div class="sidebar-stat">
            <span class="sidebar-stat-label">Total Students</span>
            <span class="sidebar-stat-value">{stats['TOTAL']}</span>
        </div>
        <div class="sidebar-stat">
            <span class="sidebar-stat-label">At Risk</span>
            <span class="sidebar-stat-value" style="color: #f97316;">{stats['AT_RISK']}</span>
        </div>
        """, unsafe_allow_html=True)
    except:
        st.markdown("""
        <div class="sidebar-stat">
            <span class="sidebar-stat-label">Status</span>
            <span class="sidebar-stat-value">Loading...</span>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("""</div>""", unsafe_allow_html=True)
    
    # Footer
    st.markdown("""
    <div style="position: absolute; bottom: 20px; left: 20px; right: 20px; text-align: center;">
        <p style="color: rgba(255,255,255,0.3); font-size: 0.7rem;">
            Powered by Snowflake Cortex AI
        </p>
    </div>
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
def get_classroom_heatmap():
    return session.sql("""
        SELECT * FROM ANALYTICS.CLASSROOM_HEATMAP ORDER BY grade_level
    """).to_pandas()

def analyze_sentiment(text):
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.SENTIMENT('{text.replace("'", "''")}') as sentiment
    """).collect()
    return float(result[0]['SENTIMENT'])

def generate_success_plan(student_data):
    prompt = f"""You are an educational advisor. Generate a specific, actionable Success Plan with 3-4 bullet points.
    
    Student: {student_data['STUDENT_NAME']} | Grade: {student_data['GRADE_LEVEL']}
    Attendance: {student_data['ATTENDANCE_RATE']}% | GPA: {student_data['CURRENT_GPA']}
    Risk Score: {student_data['RISK_SCORE']} | Sentiment: {student_data['AVG_SENTIMENT']}
    
    Provide interventions the teacher can implement this week."""
    
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', $${prompt}$$) as plan
    """).collect()
    return result[0]['PLAN']

# Language code mapping for Cortex TRANSLATE
SUPPORTED_LANGUAGES = {
    'Spanish': 'es',
    'Chinese': 'zh',
    'Vietnamese': 'vi',
    'Korean': 'ko',
    'Arabic': 'ar',
    'Tagalog': 'tl',
    'Russian': 'ru',
    'French': 'fr',
    'Portuguese': 'pt',
    'Haitian Creole': 'ht',
    'German': 'de',
    'Japanese': 'ja',
    'Hindi': 'hi',
    'Urdu': 'ur',
    'Punjabi': 'pa'
}

def get_language_code(language_name):
    """Convert language name to ISO code for Cortex TRANSLATE"""
    return SUPPORTED_LANGUAGES.get(language_name, 'en')

def get_parent_language(student_id):
    """Get parent's preferred language from student records"""
    try:
        result = session.sql(f"""
            SELECT COALESCE(parent_language, 'English') as parent_language
            FROM RAW_DATA.STUDENTS
            WHERE student_id = '{student_id}'
        """).collect()
        if result:
            return result[0]['PARENT_LANGUAGE']
        return 'English'
    except:
        return 'English'

def translate_message(text, target_language):
    """Translate text to target language using Cortex TRANSLATE"""
    # Convert language name to code if needed
    lang_code = get_language_code(target_language) if target_language in SUPPORTED_LANGUAGES else target_language
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.TRANSLATE($${text}$$, 'en', '{lang_code}') as translated
    """).collect()
    return result[0]['TRANSLATED']


# ============================================
# PAGE: DASHBOARD
# ============================================

if page == "📊 Dashboard":
    # Hero Header
    st.markdown("""
    <div class="hero-header">
        <h1>📊 Student Risk Dashboard</h1>
        <p>Real-time insights powered by Dynamic Tables & Cortex AI</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Metrics Row
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
            st.markdown(f"""
            <div class="metric-card info">
                <div class="metric-value">{metrics['TOTAL_STUDENTS']}</div>
                <div class="metric-label">📚 Total Students</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="metric-card danger">
                <div class="metric-value">{metrics['CRITICAL']}</div>
                <div class="metric-label">🔴 Critical Risk</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="metric-card warning">
                <div class="metric-value">{metrics['HIGH']}</div>
                <div class="metric-label">🟠 High Risk</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="metric-card success">
                <div class="metric-value">{metrics['AVG_ATTENDANCE']}%</div>
                <div class="metric-label">📊 Avg Attendance</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col5:
            st.markdown(f"""
            <div class="metric-card info">
                <div class="metric-value">{metrics['AVG_GPA']}</div>
                <div class="metric-label">📈 Avg GPA</div>
            </div>
            """, unsafe_allow_html=True)
            
    except Exception as e:
        st.error(f"Error loading metrics: {e}")
    
    st.markdown('<div class="custom-divider"></div>', unsafe_allow_html=True)
    
    # Two column layout
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.markdown("""
        <div class="section-header">
            <span class="section-icon">⚠️</span>
            <h2>Students Requiring Attention</h2>
        </div>
        """, unsafe_allow_html=True)
        
        try:
            at_risk_df = get_at_risk_students()
            
            if not at_risk_df.empty:
                for _, student in at_risk_df.iterrows():
                    risk_class = "critical" if student['RISK_SCORE'] >= 70 else "high" if student['RISK_SCORE'] >= 50 else "moderate"
                    risk_emoji = "🔴" if student['RISK_SCORE'] >= 70 else "🟠" if student['RISK_SCORE'] >= 50 else "🟡"
                    
                    st.markdown(f"""
                    <div class="student-card {risk_class}">
                        <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                            <div>
                                <span class="student-name">{risk_emoji} {student['STUDENT_NAME']}</span>
                                <span class="student-grade"> • Grade {int(student['GRADE_LEVEL'])}</span>
                            </div>
                            <span class="risk-badge {risk_class}">{student['RISK_LEVEL']}</span>
                        </div>
                        <div class="student-stats">
                            <span class="stat-item">📊 Risk: <strong>{student['RISK_SCORE']}</strong></span>
                            <span class="stat-item">📅 Attendance: <strong>{student['ATTENDANCE_RATE']}%</strong></span>
                            <span class="stat-item">📚 GPA: <strong>{student['CURRENT_GPA']:.1f}</strong></span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            else:
                st.success("🎉 All students are doing well!")
                
        except Exception as e:
            st.warning(f"Could not load students: {e}")
    
    with col_right:
        st.markdown("""
        <div class="section-header">
            <span class="section-icon">🗺️</span>
            <h2>Risk by Grade</h2>
        </div>
        """, unsafe_allow_html=True)
        
        try:
            heatmap_df = get_classroom_heatmap()
            
            if not heatmap_df.empty:
                for _, row in heatmap_df.iterrows():
                    st.markdown(f"""
                    <div class="glass-card" style="margin-bottom: 0.75rem; padding: 1rem;">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <strong>Grade {int(row['GRADE_LEVEL'])}</strong>
                            <span style="font-size: 0.85rem; color: #64748b;">{row['AVG_ATTENDANCE']}% avg</span>
                        </div>
                        <div style="display: flex; gap: 0.75rem; margin-top: 0.5rem;">
                            <span style="color: #ef4444;">🔴 {int(row['CRITICAL_COUNT'])}</span>
                            <span style="color: #f97316;">🟠 {int(row['HIGH_COUNT'])}</span>
                            <span style="color: #eab308;">🟡 {int(row['MODERATE_COUNT'])}</span>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
        except Exception as e:
            st.warning(f"Heatmap unavailable: {e}")
        
        st.markdown('<div class="custom-divider"></div>', unsafe_allow_html=True)
        
        st.markdown("""
        <div class="section-header">
            <span class="section-icon">📝</span>
            <h2>Recent Activity</h2>
        </div>
        """, unsafe_allow_html=True)
        
        try:
            recent = session.sql("""
                SELECT s.first_name, n.note_category, n.sentiment_score
                FROM APP.TEACHER_NOTES n
                JOIN RAW_DATA.STUDENTS s ON n.student_id = s.student_id
                ORDER BY n.created_at DESC LIMIT 4
            """).to_pandas()
            
            for _, note in recent.iterrows():
                emoji = "😊" if note['SENTIMENT_SCORE'] > 0.3 else "😐" if note['SENTIMENT_SCORE'] > -0.3 else "😟"
                st.markdown(f"""
                <div style="padding: 0.5rem 0; border-bottom: 1px solid #f1f5f9;">
                    {emoji} <strong>{note['FIRST_NAME']}</strong> - {note['NOTE_CATEGORY']}
                </div>
                """, unsafe_allow_html=True)
        except:
            st.info("No recent activity")

# ============================================
# PAGE: LOG OBSERVATION
# ============================================

elif page == "📝 Log Observation":
    st.markdown("""
    <div class="hero-header">
        <h1>📝 Log Student Observation</h1>
        <p>Quick note entry with instant AI sentiment analysis</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("""
        <div class="glass-card">
        """, unsafe_allow_html=True)
        
        with st.form("observation_form", clear_on_submit=True):
            try:
                students_df = get_students()
                student_options = dict(zip(students_df['STUDENT_NAME'], students_df['STUDENT_ID']))
                selected_student = st.selectbox("👤 Select Student", options=list(student_options.keys()))
            except:
                selected_student = st.text_input("Student ID")
                student_options = {selected_student: selected_student}
            
            col_a, col_b = st.columns(2)
            with col_a:
                category = st.selectbox("📁 Category", ["Academic", "Behavioral", "Social", "Health", "General"])
            
            note_text = st.text_area(
                "📝 Observation Note",
                placeholder="Enter your observation about the student...",
                height=150
            )
            
            submitted = st.form_submit_button("💾 Save & Analyze Sentiment", use_container_width=True, type="primary")
            
            if submitted and note_text and selected_student:
                with st.spinner("🤖 Analyzing with Cortex AI..."):
                    try:
                        sentiment = analyze_sentiment(note_text)
                        student_id = student_options.get(selected_student, selected_student)
                        
                        session.sql(f"""
                            INSERT INTO APP.TEACHER_NOTES 
                            (student_id, teacher_id, note_text, note_category, sentiment_score)
                            VALUES ('{student_id}', 'CURRENT_USER', $${note_text}$$, '{category}', {sentiment})
                        """).collect()
                        
                        sentiment_emoji = "😊" if sentiment > 0.3 else "😐" if sentiment > -0.3 else "😟"
                        sentiment_label = "Positive" if sentiment > 0.3 else "Neutral" if sentiment > -0.3 else "Negative"
                        
                        st.success(f"✅ Observation saved!")
                        st.markdown(f"""
                        <div class="glass-card" style="margin-top: 1rem; text-align: center;">
                            <span style="font-size: 3rem;">{sentiment_emoji}</span>
                            <p style="margin: 0.5rem 0 0 0; font-weight: 600;">{sentiment_label} Sentiment</p>
                            <p style="color: #64748b; font-size: 0.85rem;">Score: {sentiment:.2f}</p>
                        </div>
                        """, unsafe_allow_html=True)
                        st.cache_data.clear()
                        
                    except Exception as e:
                        st.error(f"Error: {e}")
        
        st.markdown("</div>", unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="glass-card">
            <h3 style="margin-top: 0;">💡 Sentiment Guide</h3>
            <div style="padding: 1rem 0;">
                <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem;">
                    <span style="font-size: 2rem;">😊</span>
                    <div>
                        <strong>Positive</strong>
                        <p style="margin: 0; color: #64748b; font-size: 0.85rem;">Score > 0.3</p>
                    </div>
                </div>
                <div style="display: flex; align-items: center; gap: 1rem; margin-bottom: 1rem;">
                    <span style="font-size: 2rem;">😐</span>
                    <div>
                        <strong>Neutral</strong>
                        <p style="margin: 0; color: #64748b; font-size: 0.85rem;">-0.3 to 0.3</p>
                    </div>
                </div>
                <div style="display: flex; align-items: center; gap: 1rem;">
                    <span style="font-size: 2rem;">😟</span>
                    <div>
                        <strong>Negative</strong>
                        <p style="margin: 0; color: #64748b; font-size: 0.85rem;">Score < -0.3</p>
                    </div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown('<div class="custom-divider"></div>', unsafe_allow_html=True)
        
        st.markdown("""
        <div class="section-header">
            <span class="section-icon">📋</span>
            <h2>Recent Notes</h2>
        </div>
        """, unsafe_allow_html=True)
        
        try:
            recent_notes = session.sql("""
                SELECT s.first_name || ' ' || s.last_name as name, n.note_category, n.sentiment_score
                FROM APP.TEACHER_NOTES n
                JOIN RAW_DATA.STUDENTS s ON n.student_id = s.student_id
                ORDER BY n.created_at DESC LIMIT 5
            """).to_pandas()
            
            for _, note in recent_notes.iterrows():
                emoji = "😊" if note['SENTIMENT_SCORE'] > 0.3 else "😐" if note['SENTIMENT_SCORE'] > -0.3 else "😟"
                st.markdown(f"""
                <div style="padding: 0.75rem; background: #f8fafc; border-radius: 8px; margin-bottom: 0.5rem;">
                    {emoji} <strong>{note['NAME']}</strong><br>
                    <span style="color: #64748b; font-size: 0.85rem;">{note['NOTE_CATEGORY']}</span>
                </div>
                """, unsafe_allow_html=True)
        except:
            st.info("No notes yet")


# ============================================
# PAGE: UPLOAD DATA
# ============================================

elif page == "📤 Upload Data":
    st.markdown("""
    <div class="hero-header">
        <h1>📤 Bulk Data Upload</h1>
        <p>Import data from Canvas, PowerSchool, or any CSV/Excel export</p>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        
        data_type = st.selectbox(
            "📁 Select Data Type",
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
                
                st.markdown("### 📊 Data Preview")
                st.dataframe(df.head(10), use_container_width=True)
                
                col_a, col_b, col_c = st.columns(3)
                col_a.metric("📄 Rows", len(df))
                col_b.metric("📊 Columns", len(df.columns))
                col_c.metric("📁 File", uploaded_file.name[:15] + "...")
                
                if st.button("🚀 Import Data", type="primary", use_container_width=True):
                    with st.spinner("Importing data..."):
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
                            
                            st.success(f"✅ Successfully imported {len(df)} records!")
                            st.balloons()
                            st.cache_data.clear()
                        except Exception as e:
                            st.error(f"Import failed: {e}")
            except Exception as e:
                st.error(f"Error reading file: {e}")
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="glass-card">
            <h3 style="margin-top: 0;">📋 Expected Format</h3>
        """, unsafe_allow_html=True)
        
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
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        st.markdown("""
        <div class="glass-card" style="margin-top: 1rem;">
            <h3 style="margin-top: 0;">💡 Tips</h3>
            <ul style="color: #64748b; font-size: 0.9rem;">
                <li>Export from Canvas or PowerSchool as CSV</li>
                <li>Ensure column names match expected format</li>
                <li>Remove any empty rows before upload</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)

# ============================================
# PAGE: SUCCESS PLANS
# ============================================

elif page == "🎯 Success Plans":
    st.markdown("""
    <div class="hero-header">
        <h1>🎯 AI-Powered Success Plans</h1>
        <p>Personalized intervention strategies generated by Cortex AI</p>
    </div>
    """, unsafe_allow_html=True)
    
    try:
        at_risk_df = get_at_risk_students()
        
        if not at_risk_df.empty:
            col1, col2 = st.columns([1, 2])
            
            with col1:
                st.markdown('<div class="glass-card">', unsafe_allow_html=True)
                st.markdown("### 👤 Select Student")
                
                selected = st.selectbox(
                    "Student",
                    options=at_risk_df['STUDENT_NAME'].tolist(),
                    label_visibility="collapsed"
                )
                
                student_data = at_risk_df[at_risk_df['STUDENT_NAME'] == selected].iloc[0].to_dict()
                
                risk_color = "#ef4444" if student_data['RISK_SCORE'] >= 70 else "#f97316" if student_data['RISK_SCORE'] >= 50 else "#eab308"
                
                st.markdown(f"""
                <div style="background: linear-gradient(135deg, {risk_color}15 0%, {risk_color}05 100%); 
                            border: 2px solid {risk_color}; padding: 1.25rem; border-radius: 12px; margin: 1rem 0;">
                    <h3 style="margin: 0; color: #1e293b;">{student_data['STUDENT_NAME']}</h3>
                    <p style="color: #64748b; margin: 0.25rem 0 0 0;">Grade {int(student_data['GRADE_LEVEL'])}</p>
                </div>
                """, unsafe_allow_html=True)
                
                st.metric("🎯 Risk Score", f"{student_data['RISK_SCORE']}")
                st.metric("📅 Attendance", f"{student_data['ATTENDANCE_RATE']}%")
                st.metric("📚 GPA", f"{student_data['CURRENT_GPA']:.1f}")
                st.metric("💭 Sentiment", f"{student_data['AVG_SENTIMENT']:.2f}")
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="glass-card">', unsafe_allow_html=True)
                
                if st.button("🤖 Generate Success Plan with Cortex AI", type="primary", use_container_width=True):
                    with st.spinner("🧠 Cortex AI is analyzing student data..."):
                        try:
                            plan = generate_success_plan(student_data)
                            
                            st.markdown("""
                            <div style="background: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%); 
                                        color: white; padding: 1.5rem; border-radius: 12px; margin: 1rem 0;">
                                <h3 style="margin: 0 0 0.5rem 0;">📋 Recommended Success Plan</h3>
                                <p style="margin: 0; opacity: 0.9; font-size: 0.9rem;">Generated by Snowflake Cortex AI</p>
                            </div>
                            """, unsafe_allow_html=True)
                            
                            st.markdown(plan)
                            
                            st.markdown('<div class="custom-divider"></div>', unsafe_allow_html=True)
                            
                            st.markdown("### 📧 Parent Outreach")
                            
                            # Get student ID for language lookup
                            student_id = student_data.get('STUDENT_ID', '')
                            
                            # Auto-detect parent language from student records
                            parent_lang = get_parent_language(student_id) if student_id else 'English'
                            
                            # Show detected language notification
                            if parent_lang != 'English':
                                st.info(f"🌐 Parent's preferred language detected: **{parent_lang}**")
                            
                            # Language selection dropdown (allows override)
                            available_languages = ['English'] + list(SUPPORTED_LANGUAGES.keys())
                            default_idx = available_languages.index(parent_lang) if parent_lang in available_languages else 0
                            selected_lang = st.selectbox(
                                "📌 Target Language",
                                options=available_languages,
                                index=default_idx,
                                help="Auto-detected from student records. You can override if needed."
                            )
                            
                            default_msg = f"""Dear Parent/Guardian,

I wanted to reach out regarding {selected}'s progress in class. I've noticed some areas where additional support could be beneficial, and I'd like to schedule a brief meeting to discuss strategies we can implement together.

Please let me know your availability this week.

Best regards,
[Teacher Name]"""
                            
                            message = st.text_area("✉️ Email Message (English)", value=default_msg, height=150)
                            
                            col_a, col_b = st.columns(2)
                            with col_a:
                                if st.button("📤 Send Email (English)", use_container_width=True):
                                    st.success("✅ Email drafted and ready to send!")
                            with col_b:
                                translate_disabled = selected_lang == 'English'
                                btn_label = f"🌐 Translate to {selected_lang}" if not translate_disabled else "🌐 Select a Language"
                                if st.button(btn_label, use_container_width=True, disabled=translate_disabled):
                                    with st.spinner(f"Translating to {selected_lang}..."):
                                        try:
                                            translated = translate_message(message, selected_lang)
                                            
                                            # Side-by-side comparison
                                            st.markdown("---")
                                            st.markdown("### 📝 Side-by-Side Comparison")
                                            
                                            compare_col1, compare_col2 = st.columns(2)
                                            with compare_col1:
                                                st.markdown("**🇺🇸 English (Original)**")
                                                st.text_area("eng", value=message, height=200, disabled=True, label_visibility="collapsed")
                                            with compare_col2:
                                                st.markdown(f"**🌍 {selected_lang} (Translated)**")
                                                st.text_area("trans", value=translated, height=200, label_visibility="collapsed")
                                            
                                            st.success(f"✅ Successfully translated to {selected_lang}!")
                                            
                                            if st.button(f"📤 Send Translated Email ({selected_lang})", type="primary"):
                                                st.success(f"✅ Translated email drafted in {selected_lang}!")
                                        except Exception as e:
                                            st.error(f"Translation error: {e}")
                                            
                        except Exception as e:
                            st.error(f"Error generating plan: {e}")
                
                st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.success("🎉 No at-risk students found! All students are performing well.")
            
    except Exception as e:
        st.warning(f"Please run the setup scripts first: {e}")

# ============================================
# PAGE: SETTINGS
# ============================================

elif page == "⚙️ Settings":
    st.markdown("""
    <div class="hero-header">
        <h1>⚙️ System Status & Settings</h1>
        <p>Monitor data pipelines and system health</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Pipeline Status Cards
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">📡</span>
        <h2>Data Pipeline Status</h2>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        <div class="metric-card success" style="text-align: center;">
            <span style="font-size: 2.5rem;">📤</span>
            <h3 style="margin: 0.5rem 0;">Bulk Upload</h3>
            <span class="risk-badge" style="background: #22c55e;">✓ ACTIVE</span>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="metric-card success" style="text-align: center;">
            <span style="font-size: 2.5rem;">📝</span>
            <h3 style="margin: 0.5rem 0;">Direct Entry</h3>
            <span class="risk-badge" style="background: #22c55e;">✓ ACTIVE</span>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="metric-card warning" style="text-align: center;">
            <span style="font-size: 2.5rem;">🔄</span>
            <h3 style="margin: 0.5rem 0;">Auto-Sync</h3>
            <span class="risk-badge" style="background: #f97316;">⚙ CONFIGURED</span>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown('<div class="custom-divider"></div>', unsafe_allow_html=True)
    
    # Data Quality
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">📊</span>
        <h2>Data Quality Overview</h2>
    </div>
    """, unsafe_allow_html=True)
    
    try:
        quality = session.sql("""
            SELECT 'Students' as tbl, COUNT(*) as cnt FROM RAW_DATA.STUDENTS
            UNION ALL SELECT 'Attendance', COUNT(*) FROM RAW_DATA.ATTENDANCE
            UNION ALL SELECT 'Grades', COUNT(*) FROM RAW_DATA.GRADES
            UNION ALL SELECT 'Teacher Notes', COUNT(*) FROM APP.TEACHER_NOTES
        """).to_pandas()
        
        cols = st.columns(4)
        icons = ['👥', '📅', '📝', '💬']
        for i, (_, row) in enumerate(quality.iterrows()):
            with cols[i]:
                st.markdown(f"""
                <div class="metric-card">
                    <div class="metric-value">{row['CNT']:,}</div>
                    <div class="metric-label">{icons[i]} {row['TBL']}</div>
                </div>
                """, unsafe_allow_html=True)
                
    except Exception as e:
        st.warning(f"Could not load data quality: {e}")
    
    st.markdown('<div class="custom-divider"></div>', unsafe_allow_html=True)
    
    # Dynamic Tables Status
    st.markdown("""
    <div class="section-header">
        <span class="section-icon">🔄</span>
        <h2>Dynamic Table Status</h2>
    </div>
    """, unsafe_allow_html=True)
    
    try:
        dt_status = session.sql("""
            SELECT name, target_lag, scheduling_state
            FROM INFORMATION_SCHEMA.DYNAMIC_TABLES
            WHERE schema_name = 'ANALYTICS'
        """).to_pandas()
        
        if not dt_status.empty:
            st.dataframe(
                dt_status,
                column_config={
                    "NAME": "Table Name",
                    "TARGET_LAG": "Refresh Interval",
                    "SCHEDULING_STATE": "Status"
                },
                use_container_width=True,
                hide_index=True
            )
    except:
        st.info("Dynamic Tables info unavailable")
    
    st.markdown('<div class="custom-divider"></div>', unsafe_allow_html=True)
    
    # About
    st.markdown("""
    <div class="glass-card" style="text-align: center;">
        <h3>🎓 GradSync</h3>
        <p style="color: #64748b;">Closing the gap between data and graduation</p>
        <p style="font-size: 0.85rem; color: #94a3b8;">
            Powered by Snowflake Native App • Cortex AI • Dynamic Tables
        </p>
    </div>
    """, unsafe_allow_html=True)
