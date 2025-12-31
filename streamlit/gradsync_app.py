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
if 'theme' not in st.session_state:
    st.session_state.theme = "light"
if 'show_guide' not in st.session_state:
    st.session_state.show_guide = False
if 'guide_step' not in st.session_state:
    st.session_state.guide_step = 0

# Theme-aware CSS
def get_theme_css():
    if st.session_state.theme == "dark":
        return """
        <style>
            :root {
                --bg-primary: #0f0f23;
                --bg-secondary: #1a1a2e;
                --bg-card: #16213e;
                --text-primary: #eaeaea;
                --text-secondary: #a0a0a0;
                --text-muted: #606060;
                --accent: #6c5ce7;
                --accent-light: rgba(108, 92, 231, 0.15);
                --success: #00d26a;
                --warning: #ffc048;
                --danger: #ff6b6b;
                --border: #2a2a4a;
                --nav-bg: #1a1a2e;
            }
            
            /* Dark mode button styling */
            .stButton > button {
                background: #2a2a4a !important;
                color: #eaeaea !important;
                border: 1px solid #3a3a5a !important;
            }
            .stButton > button:hover {
                background: #6c5ce7 !important;
                border-color: #6c5ce7 !important;
            }
        </style>
        """
    else:
        return """
        <style>
            :root {
                --bg-primary: #f5f7fb;
                --bg-secondary: #ffffff;
                --bg-card: #ffffff;
                --text-primary: #2d3436;
                --text-secondary: #636e72;
                --text-muted: #b2bec3;
                --accent: #6c5ce7;
                --accent-light: rgba(108, 92, 231, 0.1);
                --success: #00b894;
                --warning: #fdcb6e;
                --danger: #e17055;
                --border: #e9ecef;
                --nav-bg: #6c5ce7;
            }
            
            /* Light mode button styling */
            .stButton > button {
                background: #ffffff !important;
                color: #2d3436 !important;
                border: 1px solid #e9ecef !important;
            }
            .stButton > button:hover {
                background: #6c5ce7 !important;
                color: #ffffff !important;
                border-color: #6c5ce7 !important;
            }
            
            /* Light mode inputs */
            .stTextArea textarea, .stTextInput input, .stSelectbox > div > div {
                background: #ffffff !important;
                color: #2d3436 !important;
                border: 1px solid #e9ecef !important;
            }
            
            /* Light mode file uploader */
            [data-testid="stFileUploader"] {
                background: #ffffff !important;
            }
            [data-testid="stFileUploader"] > div {
                background: #ffffff !important;
            }
            [data-testid="stFileUploader"] section {
                background: #f8f9fa !important;
                border: 2px dashed #e9ecef !important;
            }
            [data-testid="stFileUploader"] section > div {
                color: #636e72 !important;
            }
            [data-testid="stFileUploader"] button {
                background: #6c5ce7 !important;
                color: #ffffff !important;
            }
            
            /* Light mode expander */
            .streamlit-expanderHeader {
                background: #ffffff !important;
                color: #2d3436 !important;
            }
            [data-testid="stExpander"] {
                background: #ffffff !important;
                border: 1px solid #e9ecef !important;
                border-radius: 8px !important;
            }
            [data-testid="stExpander"] > div {
                background: #ffffff !important;
            }
            [data-testid="stExpander"] details {
                background: #ffffff !important;
            }
            [data-testid="stExpander"] summary {
                background: #ffffff !important;
                color: #2d3436 !important;
            }
            [data-testid="stExpander"] [data-testid="stMarkdownContainer"] {
                color: #2d3436 !important;
            }
            [data-testid="stExpander"] p, [data-testid="stExpander"] li {
                color: #2d3436 !important;
            }
        </style>
        """

st.markdown(get_theme_css(), unsafe_allow_html=True)

# Main CSS (theme-aware using CSS variables)
st.markdown("""
<style>
    /* Hide Streamlit defaults */
    [data-testid="stSidebar"] { display: none; }
    header[data-testid="stHeader"] { display: none; }
    
    /* Main app */
    .stApp {
        background-color: var(--bg-primary);
    }
    
    .stApp, .stApp p, .stApp span, .stApp label, .stApp div {
        color: var(--text-primary);
    }
    
    /* Modern card style */
    .card {
        background: var(--bg-card);
        border-radius: 16px;
        padding: 1.5rem;
        box-shadow: 0 2px 12px rgba(0,0,0,0.08);
        border: 1px solid var(--border);
        margin-bottom: 1rem;
    }
    
    .card-header {
        font-size: 0.85rem;
        color: var(--text-secondary);
        margin-bottom: 0.5rem;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .card-value {
        font-size: 2.5rem;
        font-weight: 700;
        color: var(--text-primary);
    }
    
    .card-value.success { color: var(--success); }
    .card-value.warning { color: var(--warning); }
    .card-value.danger { color: var(--danger); }
    .card-value.accent { color: var(--accent); }
    
    /* Stat cards with colored backgrounds */
    .stat-card {
        border-radius: 16px;
        padding: 1.25rem;
        color: white;
        position: relative;
        overflow: visible;
        min-height: 180px;
        display: flex;
        flex-direction: column;
        justify-content: space-between;
    }
    
    .stat-card.purple { background: linear-gradient(135deg, #6c5ce7 0%, #a29bfe 100%); }
    .stat-card.orange { background: linear-gradient(135deg, #f39c12 0%, #f1c40f 100%); }
    .stat-card.pink { background: linear-gradient(135deg, #e84393 0%, #fd79a8 100%); }
    .stat-card.green { background: linear-gradient(135deg, #00b894 0%, #55efc4 100%); }
    
    .stat-card-icon {
        font-size: 2rem;
    }
    
    .stat-card-content {
        margin-top: auto;
    }
    
    .stat-card-value {
        font-size: 2rem;
        font-weight: 700;
        line-height: 1.2;
    }
    
    .stat-card-label {
        font-size: 0.85rem;
        opacity: 0.9;
        margin-top: 0.25rem;
    }
    
    /* Progress ring */
    .progress-ring {
        width: 80px;
        height: 80px;
        border-radius: 50%;
        background: conic-gradient(var(--success) var(--progress), var(--border) 0);
        display: flex;
        align-items: center;
        justify-content: center;
        position: relative;
    }
    
    .progress-ring-inner {
        width: 60px;
        height: 60px;
        border-radius: 50%;
        background: var(--bg-card);
        display: flex;
        align-items: center;
        justify-content: center;
        font-weight: 700;
        font-size: 1rem;
        color: var(--text-primary);
    }
    
    /* Student card */
    .student-card {
        background: var(--bg-card);
        border-radius: 12px;
        padding: 1rem;
        border: 1px solid var(--border);
        display: flex;
        align-items: center;
        gap: 1rem;
        margin-bottom: 0.75rem;
        transition: all 0.2s ease;
    }
    
    .student-card:hover {
        box-shadow: 0 4px 12px rgba(108, 92, 231, 0.15);
        border-color: var(--accent);
    }
    
    .student-avatar {
        width: 48px;
        height: 48px;
        border-radius: 12px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.25rem;
    }
    
    .student-avatar.critical { background: rgba(231, 76, 60, 0.15); }
    .student-avatar.warning { background: rgba(241, 196, 15, 0.15); }
    .student-avatar.good { background: rgba(0, 184, 148, 0.15); }
    
    .student-info { flex: 1; }
    .student-name { font-weight: 600; color: var(--text-primary); }
    .student-meta { font-size: 0.8rem; color: var(--text-secondary); }
    
    .student-score {
        font-size: 1.25rem;
        font-weight: 700;
        padding: 0.5rem 1rem;
        border-radius: 8px;
    }
    
    .student-score.critical { background: rgba(231, 76, 60, 0.15); color: #e74c3c; }
    .student-score.warning { background: rgba(241, 196, 15, 0.15); color: #f39c12; }
    .student-score.good { background: rgba(0, 184, 148, 0.15); color: #00b894; }
    
    /* Navigation */
    .nav-container {
        background: var(--nav-bg);
        border-radius: 20px;
        padding: 1.5rem 1rem;
        min-height: calc(100vh - 2rem);
    }
    
    .nav-logo {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 0.5rem;
        margin-bottom: 2rem;
    }
    
    .nav-logo-icon { font-size: 1.5rem; }
    .nav-logo-text { color: white; font-size: 1.2rem; font-weight: 700; }
    
    /* Welcome section */
    .welcome-section {
        margin-bottom: 2rem;
    }
    
    .welcome-title {
        font-size: 1.75rem;
        font-weight: 700;
        color: var(--text-primary);
        margin-bottom: 0.25rem;
    }
    
    .welcome-subtitle {
        color: var(--text-secondary);
        font-size: 0.95rem;
    }
    
    /* Section title */
    .section-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--text-primary);
        margin-bottom: 1rem;
    }
    
    /* Badge */
    .badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 500;
    }
    
    .badge-success { background: rgba(0, 184, 148, 0.15); color: var(--success); }
    .badge-warning { background: rgba(253, 203, 110, 0.3); color: #e67e22; }
    .badge-danger { background: rgba(231, 76, 60, 0.15); color: var(--danger); }
    .badge-accent { background: var(--accent-light); color: var(--accent); }
    
    /* Quick action buttons */
    .quick-action {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1rem;
        text-align: center;
        cursor: pointer;
        transition: all 0.2s ease;
    }
    
    .quick-action:hover {
        border-color: var(--accent);
        box-shadow: 0 4px 12px rgba(108, 92, 231, 0.1);
    }
    
    .quick-action-icon { font-size: 1.5rem; margin-bottom: 0.5rem; }
    .quick-action-label { font-size: 0.85rem; color: var(--text-secondary); }
    
    /* Theme toggle */
    .theme-toggle {
        background: rgba(255,255,255,0.1);
        border-radius: 20px;
        padding: 0.5rem 1rem;
        display: inline-flex;
        align-items: center;
        gap: 8px;
        cursor: pointer;
        color: white;
        font-size: 0.85rem;
    }
    
    /* Activity chart placeholder */
    .chart-container {
        background: var(--bg-card);
        border-radius: 16px;
        padding: 1.5rem;
        border: 1px solid var(--border);
    }
    
    /* Info tip */
    .info-tip {
        background: var(--accent-light);
        border: 1px solid rgba(108, 92, 231, 0.2);
        border-radius: 12px;
        padding: 1rem;
        color: var(--accent);
        font-size: 0.9rem;
    }
    
    /* Panel */
    .panel {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: 12px;
        padding: 1.25rem;
        margin-bottom: 1rem;
    }
    
    .panel-title {
        color: var(--text-secondary);
        font-size: 0.85rem;
        font-weight: 600;
        margin-bottom: 0.75rem;
    }
    
    /* Form styling */
    .stTextArea textarea, .stTextInput input, .stSelectbox > div > div {
        background: var(--bg-secondary) !important;
        border: 1px solid var(--border) !important;
        border-radius: 10px !important;
        color: var(--text-primary) !important;
    }
    
    /* Button styling */
    .stButton > button {
        border-radius: 10px !important;
        font-weight: 500 !important;
        transition: all 0.2s ease !important;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        background: var(--bg-secondary);
        padding: 0.5rem;
        border-radius: 12px;
    }
    
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px;
        color: var(--text-secondary);
    }
    
    .stTabs [aria-selected="true"] {
        background: var(--accent) !important;
        color: white !important;
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

@st.cache_data(ttl=120)
def get_sentiment_trends(student_id):
    """Get sentiment history for a student"""
    try:
        return session.sql(f"""
            SELECT 
                DATE_TRUNC('day', created_at) as note_date,
                AVG(sentiment_score) as avg_sentiment,
                COUNT(*) as note_count
            FROM GRADSYNC_DB.APP.TEACHER_NOTES
            WHERE student_id = '{student_id}'
            AND created_at >= DATEADD('day', -90, CURRENT_TIMESTAMP())
            GROUP BY DATE_TRUNC('day', created_at)
            ORDER BY note_date
        """).to_pandas()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=120)
def get_sentiment_summary():
    """Get sentiment summary for all students"""
    try:
        return session.sql("""
            WITH current_sentiment AS (
                SELECT student_id, AVG(sentiment_score) as current_avg, COUNT(*) as note_count
                FROM GRADSYNC_DB.APP.TEACHER_NOTES
                WHERE created_at >= DATEADD('day', -14, CURRENT_TIMESTAMP())
                GROUP BY student_id
            ),
            previous_sentiment AS (
                SELECT student_id, AVG(sentiment_score) as previous_avg
                FROM GRADSYNC_DB.APP.TEACHER_NOTES
                WHERE created_at BETWEEN DATEADD('day', -28, CURRENT_TIMESTAMP()) AND DATEADD('day', -14, CURRENT_TIMESTAMP())
                GROUP BY student_id
            )
            SELECT 
                s.student_id,
                s.first_name || ' ' || s.last_name as student_name,
                s.grade_level,
                COALESCE(c.current_avg, 0) as current_sentiment,
                COALESCE(p.previous_avg, 0) as previous_sentiment,
                COALESCE(c.current_avg, 0) - COALESCE(p.previous_avg, 0) as sentiment_change,
                CASE 
                    WHEN COALESCE(c.current_avg, 0) - COALESCE(p.previous_avg, 0) > 0.1 THEN 'Improving'
                    WHEN COALESCE(c.current_avg, 0) - COALESCE(p.previous_avg, 0) < -0.1 THEN 'Declining'
                    ELSE 'Stable'
                END as trend,
                COALESCE(c.note_count, 0) as recent_note_count
            FROM GRADSYNC_DB.RAW_DATA.STUDENTS s
            LEFT JOIN current_sentiment c ON s.student_id = c.student_id
            LEFT JOIN previous_sentiment p ON s.student_id = p.student_id
            WHERE c.note_count > 0
            ORDER BY sentiment_change ASC
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

def generate_success_plan(student_data, risk_breakdown=None, recent_notes=None):
    """Generate enhanced AI Success Plan"""
    risk_info = ""
    if risk_breakdown:
        risk_info = f"\nRisk: Attendance {risk_breakdown.get('attendance', 0):.0f}, Academic {risk_breakdown.get('academic', 0):.0f}\nPrimary: {risk_breakdown.get('primary_factor', 'Unknown')}"
    notes_info = f"\nNotes: {recent_notes[:200]}" if recent_notes else ""
    needs_counselor = risk_breakdown and risk_breakdown.get('primary_factor') in ['Social-Emotional Risk', 'Family Situation']
    
    prompt = f"""Create a 4-point Success Plan:
Student: {student_data['STUDENT_NAME']} | Grade {student_data['GRADE_LEVEL']}
Attendance: {student_data['ATTENDANCE_RATE']}% | GPA: {student_data['CURRENT_GPA']:.1f} | Risk: {student_data['RISK_SCORE']}{risk_info}{notes_info}

4 actions (1 sentence each): 1.This Week 2.Academic 3.Connection 4.Family
{"Recommend counselor." if needs_counselor else ""}"""

    try:
        result = session.sql(f"""SELECT SNOWFLAKE.CORTEX.COMPLETE('mistral-large', $${prompt}$$) as plan""").collect()
        return result[0]['PLAN'], needs_counselor
    except Exception as e:
        return f"Error: {e}", False
    
def get_student_risk_breakdown(student_id):
    try:
        result = session.sql(f"""
            SELECT attendance_risk_contribution, academic_risk_contribution,
                   sentiment_risk_contribution, ai_signal_risk_contribution, primary_risk_factor
            FROM GRADSYNC_DB.ANALYTICS.RISK_BREAKDOWN WHERE student_id = '{student_id}'
        """).collect()
        if result:
            return {
                'attendance': float(result[0]['ATTENDANCE_RISK_CONTRIBUTION'] or 0),
                'academic': float(result[0]['ACADEMIC_RISK_CONTRIBUTION'] or 0),
                'sentiment': float(result[0]['SENTIMENT_RISK_CONTRIBUTION'] or 0),
                'ai_signals': float(result[0]['AI_SIGNAL_RISK_CONTRIBUTION'] or 0),
                'primary_factor': result[0]['PRIMARY_RISK_FACTOR']
            }
    except:
        pass
    return None

def get_recent_notes_summary(student_id):
    try:
        result = session.sql(f"""
            SELECT LISTAGG(note_text, ' | ') as notes FROM GRADSYNC_DB.APP.TEACHER_NOTES
            WHERE student_id = '{student_id}' AND created_at >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        """).collect()
        return result[0]['NOTES'] if result and result[0]['NOTES'] else None
    except:
        return None

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
# INTERVENTION TRACKING FUNCTIONS
# ============================================


def ensure_intervention_table():
    """Create intervention table if it doesn't exist"""
    try:
        session.sql("""
            CREATE TABLE IF NOT EXISTS APP.INTERVENTION_LOG (
                log_id INT AUTOINCREMENT PRIMARY KEY,
                student_id VARCHAR(20),
                plan_generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
                plan_text VARCHAR(4000),
                risk_score_at_plan FLOAT,
                primary_risk_factor VARCHAR(100),
                counselor_referral BOOLEAN DEFAULT FALSE,
                interventions_completed VARCHAR(2000),
                outcome_notes VARCHAR(1000),
                outcome_logged_at TIMESTAMP,
                created_by VARCHAR(100)
            )
        """).collect()
        return True
    except:
        return False


def log_intervention(student_id, plan_text, risk_score, primary_factor, counselor_referral):
    """Log a new intervention plan"""
    try:
        ensure_intervention_table()
        session.sql(f"""
            INSERT INTO APP.INTERVENTION_LOG 
            (student_id, plan_text, risk_score_at_plan, primary_risk_factor, counselor_referral, created_by)
            VALUES ('{student_id}', $${plan_text}$$, {risk_score}, '{primary_factor}', {counselor_referral}, CURRENT_USER())
        """).collect()
        return True
    except Exception as e:
        st.error(f"Failed to log intervention: {e}")
        return False

def update_intervention_outcome(log_id, interventions_completed, outcome_notes):
    """Update intervention with completed actions and outcomes"""
    try:
        session.sql(f"""
            UPDATE APP.INTERVENTION_LOG 
            SET interventions_completed = $${interventions_completed}$$,
                outcome_notes = $${outcome_notes}$$,
                outcome_logged_at = CURRENT_TIMESTAMP()
            WHERE log_id = {log_id}
        """).collect()
        return True
    except Exception as e:
        st.error(f"Failed to update outcome: {e}")
        return False

@st.cache_data(ttl=60)
def get_intervention_history(student_id=None):
    """Get intervention history, optionally filtered by student"""
    try:
        ensure_intervention_table()
        where_clause = f"WHERE i.student_id = '{student_id}'" if student_id else ""
        return session.sql(f"""
            SELECT 
                i.log_id,
                i.student_id,
                s.first_name || ' ' || s.last_name as student_name,
                s.grade_level,
                i.plan_generated_at,
                i.plan_text,
                i.risk_score_at_plan,
                i.primary_risk_factor,
                i.counselor_referral,
                i.interventions_completed,
                i.outcome_notes,
                i.outcome_logged_at,
                CASE WHEN i.outcome_logged_at IS NOT NULL THEN 'Completed' ELSE 'In Progress' END as status
            FROM APP.INTERVENTION_LOG i
            JOIN RAW_DATA.STUDENTS s ON i.student_id = s.student_id
            {where_clause}
            ORDER BY i.plan_generated_at DESC
        """).to_pandas()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_intervention_stats():
    """Get intervention statistics"""
    try:
        ensure_intervention_table()
        return session.sql("""
            SELECT 
                COUNT(*) as total_plans,
                SUM(CASE WHEN outcome_logged_at IS NOT NULL THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN outcome_logged_at IS NULL THEN 1 ELSE 0 END) as in_progress,
                SUM(CASE WHEN counselor_referral THEN 1 ELSE 0 END) as counselor_referrals,
                ROUND(AVG(risk_score_at_plan), 1) as avg_risk_score
            FROM APP.INTERVENTION_LOG
        """).collect()[0]
    except:
        return None

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
    <div style="display: flex; align-items: center; gap: 10px; padding: 1rem 0.5rem; margin-bottom: 1rem; border-bottom: 1px solid var(--border);">
        <span style="font-size: 1.5rem;">🎓</span>
        <span style="color: var(--accent); font-size: 1.2rem; font-weight: 700;">GradSync</span>
    </div>
    """, unsafe_allow_html=True)
    
    # Theme toggle
    theme_label = "🌙 Dark" if st.session_state.theme == "light" else "☀️ Light"
    if st.button(theme_label, key="theme_toggle", use_container_width=True):
        st.session_state.theme = "dark" if st.session_state.theme == "light" else "light"
        st.rerun()
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Navigation items
    nav_items = [
        ("🏠", "Dashboard", "dashboard"),
        ("👥", "Students", "students"),
        ("📝", "Notes", "notes"),
        ("🎯", "Interventions", "interventions"),
        ("📤", "Import Data", "upload"),
    ]
    
    for icon, label, key in nav_items:
        if st.button(f"{icon}  {label}", key=f"nav_{key}", use_container_width=True):
            st.session_state.page = key
            st.rerun()
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    if st.button("⚙️  Settings", key="nav_settings", use_container_width=True):
        st.session_state.page = "settings"
        st.rerun()
    
    if st.button("❓  Help Guide", key="nav_help", use_container_width=True):
        st.session_state.show_guide = True
        st.rerun()


# ============================================
# MAIN CONTENT AREA
# ============================================

with main_col:
    page = st.session_state.page
    
    # ============================================
    # HELP GUIDE - SIMPLE MODAL
    # ============================================
    
    def show_help_guide():
        """Show help guide as a simple step-by-step modal"""
        if not st.session_state.show_guide:
            return
        
        guide_content = [
            {
                "icon": "🎓",
                "title": "Welcome to GradSync!",
                "desc": "Your AI-powered student success platform",
                "items": [
                    "Identify at-risk students early",
                    "AI analyzes attendance, grades & observations",
                    "Get actionable intervention plans"
                ]
            },
            {
                "icon": "📊",
                "title": "Your Dashboard",
                "desc": "Everything you need at a glance",
                "items": [
                    "Colored cards show key metrics",
                    "Student list sorted by risk level",
                    "Quick actions for common tasks"
                ]
            },
            {
                "icon": "📍",
                "title": "Navigation",
                "desc": "Find what you need quickly",
                "items": [
                    "� Studeonts — Analytics & warnings",
                    "📝 Notes — Log observations",
                    "🎯 Interventions — Success plans",
                    "📤 Import — Upload data"
                ]
            },
            {
                "icon": "🚀",
                "title": "You're Ready!",
                "desc": "Start helping your students succeed",
                "items": [
                    "Check dashboard daily",
                    "Log observations regularly",
                    "Create plans for at-risk students"
                ]
            }
        ]
        
        step = st.session_state.guide_step
        current = guide_content[step]
        total = len(guide_content)
        
        # Modern card design with shadow
        st.markdown(f"""
        <div style="background: linear-gradient(135deg, #6c5ce7 0%, #a29bfe 100%); border-radius: 20px; padding: 2rem; margin-bottom: 1.5rem; color: white; box-shadow: 0 10px 40px rgba(108, 92, 231, 0.3);">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 1.5rem;">
                <span style="background: rgba(255,255,255,0.2); padding: 0.4rem 0.8rem; border-radius: 20px; font-size: 0.85rem;">📚 Getting Started</span>
                <span style="background: rgba(255,255,255,0.2); padding: 0.4rem 0.8rem; border-radius: 20px; font-size: 0.85rem;">Step {step + 1} of {total}</span>
            </div>
            <div style="text-align: center; margin-bottom: 1.5rem;">
                <div style="font-size: 4rem; margin-bottom: 0.75rem;">{current['icon']}</div>
                <div style="font-size: 1.75rem; font-weight: 700; margin-bottom: 0.5rem;">{current['title']}</div>
                <div style="font-size: 1rem; opacity: 0.9;">{current['desc']}</div>
            </div>
            <div style="background: rgba(255,255,255,0.15); border-radius: 16px; padding: 1.25rem;">
        """, unsafe_allow_html=True)
        
        for item in current['items']:
            st.markdown(f"""
            <div style="display: flex; align-items: center; gap: 10px; padding: 0.5rem 0; border-bottom: 1px solid rgba(255,255,255,0.1);">
                <span style="color: #a29bfe; font-size: 1.1rem;">✓</span>
                <span style="color: white; font-size: 0.95rem;">{item}</span>
            </div>
            """, unsafe_allow_html=True)
        
        # Progress dots
        dots_html = '<div style="display: flex; justify-content: center; gap: 10px; margin-top: 1.5rem;">'
        for i in range(total):
            bg = "white" if i == step else "rgba(255,255,255,0.3)"
            size = "12px" if i == step else "8px"
            dots_html += f'<div style="width: {size}; height: {size}; border-radius: 50%; background: {bg}; transition: all 0.3s;"></div>'
        dots_html += '</div>'
        
        st.markdown(f"""
            </div>
            {dots_html}
        </div>
        """, unsafe_allow_html=True)
        
        # Navigation buttons
        c1, c2, c3 = st.columns(3)
        with c1:
            if step > 0:
                if st.button("← Back", key="help_back", use_container_width=True):
                    st.session_state.guide_step -= 1
                    st.rerun()
        with c2:
            if st.button("✕ Close Guide", key="help_skip", use_container_width=True):
                st.session_state.show_guide = False
                st.session_state.guide_step = 0
                st.rerun()
        with c3:
            if step < total - 1:
                if st.button("Next →", key="help_next", use_container_width=True):
                    st.session_state.guide_step += 1
                    st.rerun()
            else:
                if st.button("✓ Start Using!", key="help_done", use_container_width=True):
                    st.session_state.show_guide = False
                    st.session_state.guide_step = 0
                    st.rerun()
        
        return True
    
    # ============================================
    # PAGE: DASHBOARD (Modern Design)
    # ============================================
    
    if page == "dashboard":
        # Show guide if active (at top of page) - only when explicitly triggered
        if st.session_state.get('show_guide', False):
            show_help_guide()
            st.markdown("<hr>", unsafe_allow_html=True)
        
        # Welcome section with help button
        col_welcome, col_help = st.columns([4, 1])
        
        with col_welcome:
            st.markdown("""
            <div class="welcome-section">
                <div class="welcome-title">Hello, Teacher! 👋</div>
                <div class="welcome-subtitle">Here's what's happening with your students today</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col_help:
            if st.button("❓ Take a Tour", use_container_width=True):
                st.session_state.show_guide = True
                st.session_state.guide_step = 0
                st.rerun()
        
        # Main layout: Left content + Right sidebar
        main_left, main_right = st.columns([2, 1])
        
        with main_left:
            # Metrics row with colored stat cards
            try:
                metrics = get_metrics()
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.markdown(f"""
                    <div class="stat-card purple">
                        <div class="stat-card-icon">👥</div>
                        <div class="stat-card-content">
                            <div class="stat-card-value">{metrics['TOTAL_STUDENTS']}</div>
                            <div class="stat-card-label">Students</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col2:
                    st.markdown(f"""
                    <div class="stat-card orange">
                        <div class="stat-card-icon">⚠️</div>
                        <div class="stat-card-content">
                            <div class="stat-card-value">{metrics['CRITICAL']}</div>
                            <div class="stat-card-label">At Risk</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col3:
                    st.markdown(f"""
                    <div class="stat-card green">
                        <div class="stat-card-icon">📊</div>
                        <div class="stat-card-content">
                            <div class="stat-card-value">{metrics['AVG_ATTENDANCE']}%</div>
                            <div class="stat-card-label">Attendance</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                with col4:
                    st.markdown(f"""
                    <div class="stat-card pink">
                        <div class="stat-card-icon">📚</div>
                        <div class="stat-card-content">
                            <div class="stat-card-value">{metrics['AVG_GPA']}</div>
                            <div class="stat-card-label">Avg GPA</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Error loading metrics: {e}")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Students needing attention
            st.markdown('<div class="section-title">Students Needing Attention</div>', unsafe_allow_html=True)
            
            try:
                at_risk_df = get_at_risk_students()
                
                if not at_risk_df.empty:
                    for _, student in at_risk_df.head(4).iterrows():
                        risk_class = "critical" if student['RISK_SCORE'] >= 70 else "warning" if student['RISK_SCORE'] >= 50 else "good"
                        risk_icon = "🔴" if student['RISK_SCORE'] >= 70 else "🟡" if student['RISK_SCORE'] >= 50 else "🟢"
                        
                        st.markdown(f"""
                        <div class="student-card">
                            <div class="student-avatar {risk_class}">{risk_icon}</div>
                            <div class="student-info">
                                <div class="student-name">{student['STUDENT_NAME']}</div>
                                <div class="student-meta">Grade {int(student['GRADE_LEVEL'])} · Attendance: {student['ATTENDANCE_RATE']}%</div>
                            </div>
                            <div class="student-score {risk_class}">{int(student['RISK_SCORE'])}</div>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.success("🎉 All students are doing great!")
            except Exception as e:
                st.info("Loading student data...")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Activity chart placeholder
            st.markdown('<div class="section-title">Weekly Activity</div>', unsafe_allow_html=True)
            st.markdown("""
            <div class="chart-container">
                <div style="text-align: center; padding: 2rem; color: var(--text-muted);">
                    📈 Activity trends will appear here as data accumulates
                </div>
            </div>
            """, unsafe_allow_html=True)
        
        with main_right:
            # Quick actions
            st.markdown('<div class="section-title">Quick Actions</div>', unsafe_allow_html=True)
            
            col1, col2 = st.columns(2)
            with col1:
                if st.button("📝 Add Note", use_container_width=True):
                    st.session_state.page = "notes"
                    st.rerun()
            with col2:
                if st.button("🎯 New Plan", use_container_width=True):
                    st.session_state.page = "interventions"
                    st.rerun()
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Recent alerts
            st.markdown('<div class="section-title">Recent Alerts</div>', unsafe_allow_html=True)
            
            try:
                alerts = get_counselor_alerts()
                if not alerts.empty:
                    for _, alert in alerts.head(3).iterrows():
                        st.markdown(f"""
                        <div class="card" style="padding: 1rem;">
                            <div style="display: flex; justify-content: space-between; align-items: center;">
                                <span style="font-weight: 500;">{alert['STUDENT_NAME']}</span>
                                <span class="badge badge-danger">Alert</span>
                            </div>
                            <div style="font-size: 0.8rem; color: var(--text-secondary); margin-top: 0.5rem;">
                                {alert['AI_CLASSIFICATION']}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div class="card" style="text-align: center; padding: 1.5rem;">
                        <div style="font-size: 2rem; margin-bottom: 0.5rem;">✅</div>
                        <div style="color: var(--text-secondary);">No pending alerts</div>
                    </div>
                    """, unsafe_allow_html=True)
            except:
                st.markdown("""
                <div class="card" style="text-align: center; padding: 1.5rem;">
                    <div style="color: var(--text-secondary);">Alerts will appear here</div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Intervention stats
            st.markdown('<div class="section-title">Intervention Progress</div>', unsafe_allow_html=True)
            
            try:
                stats = get_intervention_stats()
                if stats and stats['TOTAL_PLANS'] > 0:
                    completed_pct = int((stats['COMPLETED'] / stats['TOTAL_PLANS']) * 100) if stats['TOTAL_PLANS'] > 0 else 0
                    st.markdown(f"""
                    <div class="card">
                        <div style="display: flex; align-items: center; gap: 1rem;">
                            <div class="progress-ring" style="--progress: {completed_pct}%;">
                                <div class="progress-ring-inner">{completed_pct}%</div>
                            </div>
                            <div>
                                <div style="font-weight: 600; font-size: 1.1rem;">{stats['COMPLETED']}/{stats['TOTAL_PLANS']}</div>
                                <div style="color: var(--text-secondary); font-size: 0.85rem;">Plans Completed</div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div class="card" style="text-align: center; padding: 1.5rem;">
                        <div style="color: var(--text-secondary);">Create your first intervention plan</div>
                    </div>
                    """, unsafe_allow_html=True)
            except:
                pass
        
    # ============================================
    # PAGE: STUDENTS (Analytics + Early Warnings + Sentiment)
    # ============================================
    
    elif page == "students":
        st.markdown('<div class="page-header">👥 Students</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Student analytics, early warnings, and sentiment trends</div>', unsafe_allow_html=True)
        
        # Tabs for sub-sections
        tab1, tab2, tab3 = st.tabs(["📊 Analytics", "⚡ Early Warnings", "📈 Sentiment Trends"])
        
        with tab1:
            # ANALYTICS CONTENT
            try:
                metrics = get_metrics()
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.markdown(f"""
                    <div class="stat-card purple">
                        <div class="stat-card-icon">👥</div>
                        <div class="stat-card-content">
                            <div class="stat-card-value">{metrics['TOTAL_STUDENTS']}</div>
                            <div class="stat-card-label">Students</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.markdown(f"""
                    <div class="stat-card orange">
                        <div class="stat-card-icon">🔴</div>
                        <div class="stat-card-content">
                            <div class="stat-card-value">{metrics['CRITICAL']}</div>
                            <div class="stat-card-label">Critical</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    st.markdown(f"""
                    <div class="stat-card pink">
                        <div class="stat-card-icon">🟡</div>
                        <div class="stat-card-content">
                            <div class="stat-card-value">{metrics['HIGH_RISK']}</div>
                            <div class="stat-card-label">High Risk</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                with col4:
                    st.markdown(f"""
                    <div class="stat-card green">
                        <div class="stat-card-icon">📚</div>
                        <div class="stat-card-content">
                            <div class="stat-card-value">{metrics['AVG_GPA']}</div>
                            <div class="stat-card-label">Avg GPA</div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            except Exception as e:
                st.error(f"Error loading metrics: {e}")
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # At-risk student list
            st.markdown('<div class="section-title">At-Risk Students</div>', unsafe_allow_html=True)
            try:
                at_risk_df = get_at_risk_students()
                if not at_risk_df.empty:
                    for _, student in at_risk_df.iterrows():
                        risk_class = "critical" if student['RISK_SCORE'] >= 70 else "warning" if student['RISK_SCORE'] >= 50 else "good"
                        risk_icon = "🔴" if student['RISK_SCORE'] >= 70 else "🟡" if student['RISK_SCORE'] >= 50 else "🟢"
                        
                        st.markdown(f"""
                        <div class="student-card">
                            <div class="student-avatar {risk_class}">{risk_icon}</div>
                            <div class="student-info">
                                <div class="student-name">{student['STUDENT_NAME']}</div>
                                <div class="student-meta">Grade {int(student['GRADE_LEVEL'])} · Attendance: {student['ATTENDANCE_RATE']}% · GPA: {student['CURRENT_GPA']:.1f}</div>
                            </div>
                            <div class="student-score {risk_class}">{int(student['RISK_SCORE'])}</div>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.success("🎉 No at-risk students!")
            except Exception as e:
                st.warning(f"Could not load data: {e}")
        
        with tab2:
            # EARLY WARNINGS CONTENT
            st.markdown("""
            <div class="info-tip">
                ⚡ Students showing warning signs before becoming at-risk. Early intervention can prevent escalation.
            </div>
            """, unsafe_allow_html=True)
            
            try:
                warning_df = get_early_warning_students()
                if not warning_df.empty:
                    for _, student in warning_df.iterrows():
                        indicators = []
                        if student.get('ATTENDANCE_DECLINING'): indicators.append("📉 Attendance declining")
                        if student.get('GRADES_DROPPING'): indicators.append("📚 Grades dropping")
                        if student.get('NEGATIVE_SENTIMENT'): indicators.append("😟 Negative sentiment")
                        
                        st.markdown(f"""
                        <div class="student-card">
                            <div class="student-avatar warning">⚠️</div>
                            <div class="student-info">
                                <div class="student-name">{student['STUDENT_NAME']}</div>
                                <div class="student-meta">Grade {int(student['GRADE_LEVEL'])} · {' · '.join(indicators) if indicators else 'Multiple indicators'}</div>
                            </div>
                            <div class="student-score warning">{student['EARLY_WARNING_SCORE']:.0f}</div>
                        </div>
                        """, unsafe_allow_html=True)
                else:
                    st.success("🎉 No early warning signs detected!")
            except Exception as e:
                st.info("Early warning system ready - data will appear as patterns emerge.")
        
        with tab3:
            # SENTIMENT TRENDS CONTENT
            st.markdown("""
            <div class="info-tip">
                📈 Track how teacher observations about students change over time.
            </div>
            """, unsafe_allow_html=True)
            
            try:
                sentiment_df = get_sentiment_summary()
                if not sentiment_df.empty:
                    # Show declining sentiment first
                    declining = sentiment_df[sentiment_df['TREND'] == 'Declining']
                    if not declining.empty:
                        st.markdown('<div class="section-title" style="color: var(--danger);">⚠️ Declining Sentiment</div>', unsafe_allow_html=True)
                        for _, student in declining.iterrows():
                            st.markdown(f"""
                            <div class="student-card">
                                <div class="student-avatar critical">📉</div>
                                <div class="student-info">
                                    <div class="student-name">{student['STUDENT_NAME']}</div>
                                    <div class="student-meta">Sentiment declining</div>
                                </div>
                                <div class="student-score critical">{student['SENTIMENT_CHANGE']:.2f}</div>
                            </div>
                            """, unsafe_allow_html=True)
                    
                    improving = sentiment_df[sentiment_df['TREND'] == 'Improving']
                    if not improving.empty:
                        st.markdown('<div class="section-title" style="color: var(--success); margin-top: 1rem;">✨ Improving Sentiment</div>', unsafe_allow_html=True)
                        for _, student in improving.iterrows():
                            st.markdown(f"""
                            <div class="student-card">
                                <div class="student-avatar good">📈</div>
                                <div class="student-info">
                                    <div class="student-name">{student['STUDENT_NAME']}</div>
                                    <div class="student-meta">Sentiment improving</div>
                                </div>
                                <div class="student-score good">+{student['SENTIMENT_CHANGE']:.2f}</div>
                            </div>
                            """, unsafe_allow_html=True)
                else:
                    st.info("Add teacher observations to see sentiment trends.")
            except Exception as e:
                st.info("Sentiment tracking ready - add observations to see trends.")

    # ============================================
    # PAGE: NOTES (Observations + Alerts + AI Insights)
    # ============================================
    
    elif page == "notes":
        st.markdown('<div class="page-header">📝 Notes & Alerts</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Teacher observations, counselor alerts, and AI insights</div>', unsafe_allow_html=True)
        
        # Tabs for sub-sections
        tab1, tab2, tab3 = st.tabs(["📝 Add Observation", "🚨 Counselor Alerts", "🧠 AI Insights"])
        
        with tab1:
            # OBSERVATION ENTRY CONTENT
            st.markdown("""
            <div class="welcome-banner">
                <div style="display: flex; align-items: flex-start; gap: 12px;">
                    <span style="font-size: 1.5rem;">💡</span>
                    <div>
                        <div style="color: #22c55e; font-weight: 500; margin-bottom: 0.25rem;">Quick Tip</div>
                        <div style="color: #a0a0a0; font-size: 0.85rem;">Your observations help identify students who may need support. Notes are analyzed by AI to detect patterns and flag concerns.</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            try:
                students_df = get_students()
                
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    selected_student = st.selectbox("Select Student", options=students_df['STUDENT_NAME'].tolist(), help="Choose the student you want to write about")
                    student_row = students_df[students_df['STUDENT_NAME'] == selected_student].iloc[0]
                    student_id = student_row['STUDENT_ID']
                    
                    category = st.selectbox("Category", ["Classroom Behavior", "Academic Performance", "Social Interaction", "Attendance Pattern", "Parent Communication", "Positive Recognition", "Concern"])
                
                with col2:
                    note_text = st.text_area("Your Observation", height=150, placeholder="What did you notice about this student today?", help="Be specific - include what you observed, when, and any context")
                    
                    if st.button("💾 Save Observation", type="primary", use_container_width=True):
                        if note_text.strip():
                            try:
                                sentiment = analyze_sentiment(note_text)
                                classification, confidence = classify_note(note_text)
                                is_high_risk = is_high_risk_category(classification) if classification else False
                                
                                note_id = str(uuid.uuid4())[:8]
                                
                                class_value = f"'{classification}'" if classification else "NULL"
                                conf_value = confidence if confidence else "NULL"
                                
                                session.sql(f"""
                                    INSERT INTO APP.TEACHER_NOTES (note_id, student_id, note_text, note_category, sentiment_score, ai_classification, ai_confidence, is_high_risk)
                                    VALUES ('{note_id}', '{student_id}', $${note_text}$$, '{category}', {sentiment}, {class_value}, {conf_value}, {is_high_risk})
                                """).collect()
                                
                                st.success("✅ Observation saved!")
                                if is_high_risk:
                                    st.warning(f"⚠️ Flagged for counselor review: {classification}")
                                
                                st.cache_data.clear()
                            except Exception as e:
                                st.error(f"Error saving: {e}")
                        else:
                            st.warning("Please enter your observation.")
                
                # Recent notes
                st.markdown("<hr>", unsafe_allow_html=True)
                st.markdown('<div class="panel-title">Recent Observations</div>', unsafe_allow_html=True)
                
                recent = get_recent_notes()
                if not recent.empty:
                    for _, note in recent.head(5).iterrows():
                        sentiment_color = "#22c55e" if note['SENTIMENT_SCORE'] > 0 else "#ef4444" if note['SENTIMENT_SCORE'] < 0 else "#808080"
                        st.markdown(f"""
                        <div class="panel" style="margin-bottom: 0.5rem;">
                            <div style="display: flex; justify-content: space-between;">
                                <span style="color: #e0e0e0;">{note['STUDENT_NAME']}</span>
                                <span style="color: {sentiment_color}; font-size: 0.8rem;">{note['AI_CLASSIFICATION'] or note['NOTE_CATEGORY']}</span>
                            </div>
                            <div style="color: #808080; font-size: 0.85rem; margin-top: 0.25rem;">{note['NOTE_TEXT'][:100]}...</div>
                        </div>
                        """, unsafe_allow_html=True)
            except Exception as e:
                st.warning(f"Setup required: {e}")
        
        with tab2:
            # COUNSELOR ALERTS CONTENT
            st.markdown("""
            <div class="info-tip">
                🚨 Notes flagged as high-risk by AI that may need counselor review.
            </div>
            """, unsafe_allow_html=True)
            
            try:
                alerts_df = get_counselor_alerts()
                if not alerts_df.empty:
                    pending = alerts_df[alerts_df['REVIEWED_AT'].isna()]
                    reviewed = alerts_df[alerts_df['REVIEWED_AT'].notna()]
                    
                    if not pending.empty:
                        st.markdown(f'<div class="panel-title" style="color: #ef4444;">⚠️ Pending Review ({len(pending)})</div>', unsafe_allow_html=True)
                        for _, alert in pending.iterrows():
                            st.markdown(f"""
                            <div class="panel" style="border-color: rgba(239, 68, 68, 0.3); margin-bottom: 0.75rem;">
                                <div style="display: flex; justify-content: space-between; align-items: center;">
                                    <div>
                                        <span style="color: #e0e0e0; font-weight: 500;">{alert['STUDENT_NAME']}</span>
                                        <span style="color: #606060; margin-left: 8px;">Grade {int(alert['GRADE_LEVEL'])}</span>
                                    </div>
                                    <span class="badge badge-red">{alert['AI_CLASSIFICATION']}</span>
                                </div>
                                <div style="color: #a0a0a0; font-size: 0.9rem; margin-top: 0.5rem;">{alert['NOTE_TEXT']}</div>
                            </div>
                            """, unsafe_allow_html=True)
                    else:
                        st.success("✅ All alerts reviewed!")
                else:
                    st.info("No high-risk alerts at this time.")
            except Exception as e:
                st.info("Counselor alerts ready - high-risk notes will appear here.")
        
        with tab3:
            # AI INSIGHTS CONTENT
            st.markdown("""
            <div class="info-tip">
                🧠 AI-detected patterns across multiple teacher observations.
            </div>
            """, unsafe_allow_html=True)
            
            try:
                # Students with multiple notes for pattern analysis
                pattern_df = get_students_for_pattern_analysis()
                if not pattern_df.empty:
                    st.markdown('<div class="panel-title">Students with Multiple Observations</div>', unsafe_allow_html=True)
                    
                    for _, student in pattern_df.head(5).iterrows():
                        with st.expander(f"🔍 {student['STUDENT_NAME']} ({student['NOTE_COUNT']} notes)"):
                            if st.button(f"Analyze Patterns", key=f"analyze_{student['STUDENT_ID']}"):
                                with st.spinner("AI analyzing patterns..."):
                                    analysis = analyze_student_patterns(
                                        student['STUDENT_ID'],
                                        student['STUDENT_NAME'],
                                        student['ALL_NOTES'][:2000]
                                    )
                                    st.markdown(analysis)
                else:
                    st.info("Add more observations to enable pattern detection.")
                
                # Stored AI insights
                insights_df = get_ai_insights()
                if not insights_df.empty:
                    st.markdown("<hr>", unsafe_allow_html=True)
                    st.markdown('<div class="panel-title">Recent AI Insights</div>', unsafe_allow_html=True)
                    for _, insight in insights_df.head(5).iterrows():
                        st.markdown(f"""
                        <div class="panel" style="margin-bottom: 0.5rem;">
                            <div style="color: #e0e0e0;">{insight['STUDENT_NAME']}</div>
                            <div style="color: #808080; font-size: 0.85rem;">{insight.get('INSIGHT_TEXT', 'Pattern detected')}</div>
                        </div>
                        """, unsafe_allow_html=True)
            except Exception as e:
                st.info("AI insights ready - patterns will appear as data accumulates.")

    # ============================================
    # PAGE: INTERVENTIONS (Success Plans + Tracking)
    # ============================================
    
    elif page == "interventions":
        st.markdown('<div class="page-header">🎯 Interventions</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Create success plans and track intervention outcomes</div>', unsafe_allow_html=True)
        
        # Tabs for sub-sections
        tab1, tab2 = st.tabs(["🎯 Create Plan", "📋 Track Progress"])
        
        with tab1:
            # SUCCESS PLANS CONTENT
            st.markdown("""
            <div class="welcome-banner">
                <div style="display: flex; align-items: flex-start; gap: 12px;">
                    <span style="font-size: 1.5rem;">🤖</span>
                    <div>
                        <div style="color: #22c55e; font-weight: 500; margin-bottom: 0.25rem;">AI-Powered Recommendations</div>
                        <div style="color: #a0a0a0; font-size: 0.85rem;">Select a student and generate personalized intervention strategies based on their data.</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            try:
                at_risk_df = get_at_risk_students()
                
                if not at_risk_df.empty:
                    col1, col2 = st.columns([1, 2])
                    
                    with col1:
                        selected = st.selectbox("Select Student", options=at_risk_df['STUDENT_NAME'].tolist(), help="Students shown here have been flagged as at-risk")
                        student_data = at_risk_df[at_risk_df['STUDENT_NAME'] == selected].iloc[0].to_dict()
                        
                        color = "red" if student_data['RISK_SCORE'] >= 70 else "yellow" if student_data['RISK_SCORE'] >= 50 else ""
                        risk_label = "Critical" if student_data['RISK_SCORE'] >= 70 else "High" if student_data['RISK_SCORE'] >= 50 else "Moderate"
                        
                        st.markdown(f"""
                        <div class="metric-box" style="text-align: center;">
                            <div class="metric-label">Risk Level</div>
                            <div class="metric-value {color}" style="font-size: 1.5rem;">{risk_label}</div>
                            <div style="color: #606060; font-size: 0.8rem;">Score: {student_data['RISK_SCORE']}</div>
                        </div>
                        """, unsafe_allow_html=True)
                        
                        c1, c2 = st.columns(2)
                        with c1:
                            st.metric("Attendance", f"{student_data['ATTENDANCE_RATE']}%")
                        with c2:
                            st.metric("GPA", f"{student_data['CURRENT_GPA']:.1f}")
                    
                    with col2:
                        if st.button("🤖 Generate Success Plan", use_container_width=True, type="primary"):
                            with st.spinner("AI is analyzing student data..."):
                                try:
                                    student_id = student_data.get('STUDENT_ID', '')
                                    risk_breakdown = get_student_risk_breakdown(student_id) if student_id else None
                                    recent_notes = get_recent_notes_summary(student_id) if student_id else None
                                    
                                    plan, needs_counselor = generate_success_plan(student_data, risk_breakdown, recent_notes)
                                    
                                    # Log the intervention
                                    primary_factor = risk_breakdown.get('primary_factor', 'Unknown') if risk_breakdown else 'Unknown'
                                    if student_id:
                                        log_intervention(student_id, plan, student_data['RISK_SCORE'], primary_factor, needs_counselor)
                                    
                                    st.success("✅ Plan generated and logged!")
                                    
                                    if needs_counselor:
                                        st.warning("⚠️ Consider counselor referral for this student.")
                                    
                                    st.markdown(plan)
                                except Exception as e:
                                    st.error(f"Error: {e}")
                else:
                    st.success("🎉 All students are performing well!")
            except Exception as e:
                st.warning(f"Setup required: {e}")
        
        with tab2:
            # INTERVENTION TRACKING CONTENT
            stats = get_intervention_stats()
            if stats:
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.markdown(f'<div class="metric-box"><div class="metric-label">Total Plans</div><div class="metric-value">{stats["TOTAL_PLANS"]}</div></div>', unsafe_allow_html=True)
                with col2:
                    st.markdown(f'<div class="metric-box"><div class="metric-label">In Progress</div><div class="metric-value yellow">{stats["IN_PROGRESS"]}</div></div>', unsafe_allow_html=True)
                with col3:
                    st.markdown(f'<div class="metric-box"><div class="metric-label">Completed</div><div class="metric-value green">{stats["COMPLETED"]}</div></div>', unsafe_allow_html=True)
                with col4:
                    st.markdown(f'<div class="metric-box"><div class="metric-label">Counselor Referrals</div><div class="metric-value">{stats["COUNSELOR_REFERRALS"]}</div></div>', unsafe_allow_html=True)
            
            st.markdown("<hr>", unsafe_allow_html=True)
            
            filter_status = st.selectbox("Filter", ["All", "In Progress", "Completed"])
            
            history_df = get_intervention_history()
            
            if not history_df.empty:
                if filter_status == "In Progress":
                    history_df = history_df[history_df['STATUS'] == 'In Progress']
                elif filter_status == "Completed":
                    history_df = history_df[history_df['STATUS'] == 'Completed']
                
                for _, row in history_df.iterrows():
                    counselor_text = " | 🚨 Counselor Referral" if row['COUNSELOR_REFERRAL'] else ''
                    created_date = row['PLAN_GENERATED_AT'].strftime('%b %d, %Y') if pd.notna(row['PLAN_GENERATED_AT']) else 'N/A'
                    
                    with st.container():
                        col_info, col_status = st.columns([3, 1])
                        with col_info:
                            st.markdown(f"**{row['STUDENT_NAME']}** · Grade {int(row['GRADE_LEVEL'])}{counselor_text}")
                            st.caption(f"Risk: {row['RISK_SCORE_AT_PLAN']} | Factor: {row['PRIMARY_RISK_FACTOR']} | Created: {created_date}")
                        with col_status:
                            if row['STATUS'] == 'In Progress':
                                st.warning("In Progress")
                            else:
                                st.success("Completed")
                    
                    with st.expander(f"📄 View Plan & Log Outcome"):
                        st.markdown("**Plan:**")
                        st.markdown(row['PLAN_TEXT'] if row['PLAN_TEXT'] else "No plan text")
                        
                        if row['STATUS'] == 'In Progress':
                            st.markdown("---")
                            interventions = st.text_area("Interventions completed", key=f"int_{row['LOG_ID']}", height=60)
                            outcome = st.text_area("Outcome notes", key=f"out_{row['LOG_ID']}", height=60)
                            
                            if st.button("✅ Mark Complete", key=f"done_{row['LOG_ID']}", type="primary"):
                                if interventions and outcome:
                                    if update_intervention_outcome(row['LOG_ID'], interventions, outcome):
                                        st.success("Saved!")
                                        st.cache_data.clear()
                                        st.rerun()
                                else:
                                    st.warning("Fill in both fields.")
                        else:
                            if row['INTERVENTIONS_COMPLETED']:
                                st.markdown("**Completed:**")
                                st.markdown(row['INTERVENTIONS_COMPLETED'])
                            if row['OUTCOME_NOTES']:
                                st.markdown("**Outcome:**")
                                st.markdown(row['OUTCOME_NOTES'])
            else:
                st.info("No intervention plans yet. Create one in the 'Create Plan' tab.")

    # ============================================
    # PAGE: IMPORT DATA
    # ============================================
    
    elif page == "upload":
        st.markdown('<div class="page-header">📤 Data Management</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Import, export, and manage your student data</div>', unsafe_allow_html=True)
        
        # Tabs for Import, Export, and Templates
        tab_import, tab_export, tab_templates = st.tabs(["📥 Import Data", "📤 Export Data", "📋 CSV Templates"])
        
        with tab_import:
            col1, col2 = st.columns([3, 2])
            
            with col1:
                # Upload area card
                st.markdown("""
                <div class="card" style="text-align: center; padding: 2rem; border: 2px dashed var(--border);">
                    <div style="font-size: 3rem; margin-bottom: 0.75rem;">📁</div>
                    <div style="font-weight: 600; color: var(--text-primary);">Upload Your Data</div>
                    <div style="color: var(--text-secondary); font-size: 0.85rem;">CSV or Excel files</div>
                </div>
                """, unsafe_allow_html=True)
                
                data_type = st.selectbox(
                    "Data Type",
                    ["students", "attendance", "grades"],
                    format_func=lambda x: {"students": "👥 Student Roster", "attendance": "📅 Attendance", "grades": "📝 Grades"}[x]
                )
                
            uploaded_file = st.file_uploader("Choose your file", type=['csv', 'xlsx'])
            
            if uploaded_file:
                try:
                    df = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
                    
                    st.success(f"✓ File loaded: {uploaded_file.name} ({len(df)} records)")
                    
                    with st.expander("Preview data", expanded=True):
                        st.dataframe(df.head(10), use_container_width=True)
                    
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
                                
                                st.success(f"🎉 {len(df)} records imported!")
                                st.balloons()
                                st.cache_data.clear()
                            except Exception as e:
                                st.error(f"Import failed: {e}")
                except Exception as e:
                    st.error("Could not read file. Check format.")
        
            with col2:
                # Required columns card
                st.markdown("""
                <div class="card">
                    <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 1rem;">
                        <span style="font-size: 1.2rem;">📋</span>
                        <span style="font-weight: 600; color: var(--text-primary);">Required Columns</span>
                    </div>
                """, unsafe_allow_html=True)
                
                if data_type == "students":
                    cols_info = [("student_id", "Unique ID"), ("first_name", "First name"), ("last_name", "Last name"), ("grade_level", "9-12")]
                elif data_type == "attendance":
                    cols_info = [("student_id", "Student ID"), ("date", "YYYY-MM-DD"), ("status", "Present/Absent/Tardy")]
                else:
                    cols_info = [("student_id", "Student ID"), ("subject", "e.g., Math"), ("assignment", "Name"), ("score", "Points")]
                
                for col_name, col_desc in cols_info:
                    st.markdown(f"""
                    <div style="display: flex; justify-content: space-between; padding: 0.4rem 0; border-bottom: 1px solid var(--border);">
                        <code style="background: var(--accent-light); color: var(--accent); padding: 0.15rem 0.4rem; border-radius: 4px; font-size: 0.75rem;">{col_name}</code>
                        <span style="color: var(--text-secondary); font-size: 0.8rem;">{col_desc}</span>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("""
                </div>
                <div class="card" style="margin-top: 1rem;">
                    <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 0.5rem;">
                        <span style="font-size: 1.2rem;">💡</span>
                        <span style="font-weight: 600; color: var(--text-primary);">Tips</span>
                    </div>
                    <div style="color: var(--text-secondary); font-size: 0.8rem; line-height: 1.5;">
                        • Get templates from "CSV Templates" tab<br>
                        • Save Excel as CSV before upload<br>
                        • Keep student_id consistent
                    </div>
                </div>
                """, unsafe_allow_html=True)
        
        with tab_export:
            # Export header
            st.markdown("""
            <div class="card" style="background: linear-gradient(135deg, rgba(108, 92, 231, 0.1) 0%, rgba(108, 92, 231, 0.05) 100%); border-color: rgba(108, 92, 231, 0.2);">
                <div style="display: flex; align-items: center; gap: 12px;">
                    <span style="font-size: 1.5rem;">📤</span>
                    <div>
                        <div style="color: var(--accent); font-weight: 600;">Export Your Data</div>
                        <div style="color: var(--text-secondary); font-size: 0.85rem;">Download reports for sharing or backup</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            export_type = st.selectbox(
                "Select data to export",
                ["at_risk_students", "all_students", "intervention_history", "teacher_notes"],
                format_func=lambda x: {
                    "at_risk_students": "⚠️ At-Risk Students Report",
                    "all_students": "👥 All Students Data",
                    "intervention_history": "🎯 Intervention History",
                    "teacher_notes": "📝 Teacher Observations"
                }[x]
            )
            
            try:
                if export_type == "at_risk_students":
                    export_df = get_at_risk_students()
                    filename = "at_risk_students_report.csv"
                elif export_type == "all_students":
                    export_df = session.sql("""
                        SELECT student_id, first_name, last_name, grade_level, 
                               COALESCE(parent_language, 'English') as parent_language
                        FROM RAW_DATA.STUDENTS ORDER BY last_name, first_name
                    """).to_pandas()
                    filename = "all_students.csv"
                elif export_type == "intervention_history":
                    export_df = get_intervention_history()
                    filename = "intervention_history.csv"
                else:
                    export_df = get_recent_notes()
                    filename = "teacher_observations.csv"
                
                if not export_df.empty:
                    st.markdown(f"""
                    <div class="card" style="background: linear-gradient(135deg, rgba(34, 197, 94, 0.1) 0%, rgba(34, 197, 94, 0.05) 100%); border-color: rgba(34, 197, 94, 0.3);">
                        <div style="display: flex; align-items: center; gap: 12px;">
                            <span style="font-size: 1.5rem;">✅</span>
                            <div>
                                <div style="color: #22c55e; font-weight: 600;">Ready to Download</div>
                                <div style="color: var(--text-secondary); font-size: 0.85rem;">{len(export_df)} records · {filename}</div>
                            </div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    with st.expander("📊 Preview Data"):
                        st.dataframe(export_df.head(10), use_container_width=True)
                    
                    csv_data = export_df.to_csv(index=False)
                    st.download_button(
                        label="⬇️ Download CSV",
                        data=csv_data,
                        file_name=filename,
                        mime="text/csv",
                        use_container_width=True,
                        type="primary"
                    )
                else:
                    st.info("No data available for export.")
            except Exception as e:
                st.warning(f"Could not load data: {e}")
        
        with tab_templates:
            # Header
            st.markdown("""
            <div class="card" style="background: linear-gradient(135deg, rgba(59, 130, 246, 0.1) 0%, rgba(59, 130, 246, 0.05) 100%); border-color: rgba(59, 130, 246, 0.2);">
                <div style="display: flex; align-items: center; gap: 12px;">
                    <span style="font-size: 1.5rem;">📋</span>
                    <div>
                        <div style="color: #3b82f6; font-weight: 600;">CSV Templates</div>
                        <div style="color: var(--text-secondary); font-size: 0.85rem;">Download → Fill in Excel → Upload in Import tab</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Template cards in 3 columns
            t_col1, t_col2, t_col3 = st.columns(3)
            
            with t_col1:
                st.markdown("""
                <div class="card" style="text-align: center;">
                    <div style="font-size: 2.5rem; margin-bottom: 0.5rem;">👥</div>
                    <div style="font-weight: 600; color: var(--text-primary);">Students</div>
                    <div style="color: var(--text-secondary); font-size: 0.8rem; margin-bottom: 1rem;">Add new students</div>
                </div>
                """, unsafe_allow_html=True)
                
                students_template = pd.DataFrame({
                    'student_id': ['STU001', 'STU002', 'STU003'],
                    'first_name': ['John', 'Maria', 'James'],
                    'last_name': ['Smith', 'Garcia', 'Johnson'],
                    'grade_level': [9, 10, 11],
                    'parent_language': ['English', 'Spanish', 'English']
                })
                
                st.download_button(
                    label="⬇️ Download",
                    data=students_template.to_csv(index=False),
                    file_name="students_template.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with t_col2:
                st.markdown("""
                <div class="card" style="text-align: center;">
                    <div style="font-size: 2.5rem; margin-bottom: 0.5rem;">📅</div>
                    <div style="font-weight: 600; color: var(--text-primary);">Attendance</div>
                    <div style="color: var(--text-secondary); font-size: 0.8rem; margin-bottom: 1rem;">Daily records</div>
                </div>
                """, unsafe_allow_html=True)
                
                attendance_template = pd.DataFrame({
                    'student_id': ['STU001', 'STU001', 'STU002'],
                    'date': ['2025-01-06', '2025-01-07', '2025-01-06'],
                    'status': ['Present', 'Absent', 'Tardy']
                })
                
                st.download_button(
                    label="⬇️ Download",
                    data=attendance_template.to_csv(index=False),
                    file_name="attendance_template.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with t_col3:
                st.markdown("""
                <div class="card" style="text-align: center;">
                    <div style="font-size: 2.5rem; margin-bottom: 0.5rem;">📝</div>
                    <div style="font-weight: 600; color: var(--text-primary);">Grades</div>
                    <div style="color: var(--text-secondary); font-size: 0.8rem; margin-bottom: 1rem;">Assignment scores</div>
                </div>
                """, unsafe_allow_html=True)
                
                grades_template = pd.DataFrame({
                    'student_id': ['STU001', 'STU001', 'STU002'],
                    'subject': ['Math', 'English', 'Math'],
                    'assignment': ['Quiz 1', 'Essay 1', 'Quiz 1'],
                    'score': [85, 92, 78],
                    'max_score': [100, 100, 100]
                })
                
                st.download_button(
                    label="⬇️ Download",
                    data=grades_template.to_csv(index=False),
                    file_name="grades_template.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            # Quick start guide
            st.markdown("""
            <div class="card">
                <div style="display: flex; align-items: center; gap: 8px; margin-bottom: 1rem;">
                    <span style="font-size: 1.2rem;">🚀</span>
                    <span style="font-weight: 600; color: var(--text-primary);">Quick Start</span>
                </div>
                <div style="display: flex; justify-content: space-between; text-align: center;">
                    <div style="flex: 1;">
                        <div style="background: var(--accent); color: white; width: 28px; height: 28px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-weight: 600; font-size: 0.85rem;">1</div>
                        <div style="color: var(--text-secondary); font-size: 0.75rem; margin-top: 0.25rem;">Download</div>
                    </div>
                    <div style="flex: 1;">
                        <div style="background: var(--accent); color: white; width: 28px; height: 28px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-weight: 600; font-size: 0.85rem;">2</div>
                        <div style="color: var(--text-secondary); font-size: 0.75rem; margin-top: 0.25rem;">Open Excel</div>
                    </div>
                    <div style="flex: 1;">
                        <div style="background: var(--accent); color: white; width: 28px; height: 28px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-weight: 600; font-size: 0.85rem;">3</div>
                        <div style="color: var(--text-secondary); font-size: 0.75rem; margin-top: 0.25rem;">Fill Data</div>
                    </div>
                    <div style="flex: 1;">
                        <div style="background: var(--accent); color: white; width: 28px; height: 28px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-weight: 600; font-size: 0.85rem;">4</div>
                        <div style="color: var(--text-secondary); font-size: 0.75rem; margin-top: 0.25rem;">Save CSV</div>
                    </div>
                    <div style="flex: 1;">
                        <div style="background: var(--accent); color: white; width: 28px; height: 28px; border-radius: 50%; display: inline-flex; align-items: center; justify-content: center; font-weight: 600; font-size: 0.85rem;">5</div>
                        <div style="color: var(--text-secondary); font-size: 0.75rem; margin-top: 0.25rem;">Upload</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    # ============================================
    # LEGACY PAGES (redirect to new consolidated pages)
    # ============================================
    
    elif page == "analytics":
        st.session_state.page = "students"
        st.rerun()
    
    elif page == "observation":
        st.session_state.page = "notes"
        st.rerun()
    
    elif page == "alerts":
        st.session_state.page = "notes"
        st.rerun()
    
    elif page == "insights":
        st.session_state.page = "notes"
        st.rerun()
    
    elif page == "warnings":
        st.session_state.page = "students"
        st.rerun()
    
    elif page == "sentiment":
        st.session_state.page = "students"
        st.rerun()
    
    elif page == "plans":
        st.session_state.page = "interventions"
        st.rerun()

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
    # PAGE: SENTIMENT TRENDS
    # ============================================
    
    elif page == "sentiment":
        st.markdown('<div class="page-header">📈 Sentiment Trends</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Track how teacher observations change over time</div>', unsafe_allow_html=True)
        
        try:
            summary_df = get_sentiment_summary()
            
            if summary_df.empty:
                st.markdown("""
                <div class="panel" style="text-align: center; padding: 3rem;">
                    <div style="font-size: 3rem; margin-bottom: 1rem;">📝</div>
                    <div style="color: #808080; font-size: 1.1rem;">No sentiment data yet</div>
                    <div style="color: #606060; font-size: 0.9rem; margin-top: 0.5rem;">Add teacher observations to see trends.</div>
                </div>
                """, unsafe_allow_html=True)
            else:
                # Stats
                improving = len(summary_df[summary_df['TREND'] == 'Improving'])
                declining = len(summary_df[summary_df['TREND'] == 'Declining'])
                stable = len(summary_df[summary_df['TREND'] == 'Stable'])
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">📈 Improving</div>
                        <div class="metric-value green">{improving}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col2:
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">➡️ Stable</div>
                        <div class="metric-value">{stable}</div>
                    </div>
                    """, unsafe_allow_html=True)
                with col3:
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">📉 Declining</div>
                        <div class="metric-value red">{declining}</div>
                    </div>
                    """, unsafe_allow_html=True)
                
                st.markdown("<br>", unsafe_allow_html=True)
                
                # Show declining students first
                if declining > 0:
                    st.markdown("""
                    <div class="panel" style="border-color: rgba(239, 68, 68, 0.3);">
                        <div class="panel-title" style="color: #ef4444; margin-bottom: 0.75rem;">⚠️ Declining Sentiment</div>
                    """, unsafe_allow_html=True)
                    
                    for _, student in summary_df[summary_df['TREND'] == 'Declining'].iterrows():
                        change = student['SENTIMENT_CHANGE']
                        st.markdown(f"""
                        <div style="display: flex; justify-content: space-between; padding: 0.5rem 0; border-bottom: 1px solid #1a1a1a;">
                            <span style="color: #e0e0e0;">{student['STUDENT_NAME']}</span>
                            <span style="color: #ef4444;">{change:+.2f} ↓</span>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    st.markdown("</div>", unsafe_allow_html=True)
                
                # Show improving students
                if improving > 0:
                    st.markdown("""
                    <div class="panel" style="border-color: rgba(34, 197, 94, 0.3); margin-top: 1rem;">
                        <div class="panel-title" style="color: #22c55e; margin-bottom: 0.75rem;">🎉 Improving Sentiment</div>
                    """, unsafe_allow_html=True)
                    
                    for _, student in summary_df[summary_df['TREND'] == 'Improving'].iterrows():
                        change = student['SENTIMENT_CHANGE']
                        st.markdown(f"""
                        <div style="display: flex; justify-content: space-between; padding: 0.5rem 0; border-bottom: 1px solid #1a1a1a;">
                            <span style="color: #e0e0e0;">{student['STUDENT_NAME']}</span>
                            <span style="color: #22c55e;">{change:+.2f} ↑</span>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    st.markdown("</div>", unsafe_allow_html=True)
                
                # Student detail view
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown("""
                <div class="panel">
                    <div class="panel-title" style="margin-bottom: 1rem;">📊 Student Sentiment History</div>
                """, unsafe_allow_html=True)
                
                student_options = dict(zip(summary_df['STUDENT_NAME'], summary_df['STUDENT_ID']))
                selected = st.selectbox("Select student", options=list(student_options.keys()))
                
                if selected:
                    student_id = student_options[selected]
                    trend_df = get_sentiment_trends(student_id)
                    
                    if not trend_df.empty:
                        # Simple line chart
                        st.line_chart(trend_df.set_index('NOTE_DATE')['AVG_SENTIMENT'])
                        
                        # Current status
                        student_data = summary_df[summary_df['STUDENT_ID'] == student_id].iloc[0]
                        trend = student_data['TREND']
                        trend_color = '#22c55e' if trend == 'Improving' else '#ef4444' if trend == 'Declining' else '#808080'
                        trend_icon = '📈' if trend == 'Improving' else '📉' if trend == 'Declining' else '➡️'
                        
                        st.markdown(f"""
                        <div style="text-align: center; margin-top: 1rem;">
                            <span style="color: {trend_color}; font-size: 1.2rem; font-weight: 500;">{trend_icon} {trend}</span>
                            <div style="color: #606060; font-size: 0.85rem; margin-top: 0.25rem;">
                                Current: {student_data['CURRENT_SENTIMENT']:.2f} | Previous: {student_data['PREVIOUS_SENTIMENT']:.2f}
                            </div>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.caption("No trend data available for this student.")
                
                st.markdown("</div>", unsafe_allow_html=True)
                
        except Exception as e:
            st.error(f"Error: {e}")


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
                        with st.spinner("AI is analyzing student data..."):
                            try:
                                # Get risk breakdown and notes for enhanced plan
                                student_id = student_data.get('STUDENT_ID', '')
                                risk_breakdown = get_student_risk_breakdown(student_id) if student_id else None
                                recent_notes = get_recent_notes_summary(student_id) if student_id else None
                                
                                plan, needs_counselor = generate_success_plan(student_data, risk_breakdown, recent_notes)
                                
                                # Log the intervention
                                primary_factor = risk_breakdown.get('primary_factor', 'Unknown') if risk_breakdown else 'Unknown'
                                if student_id:
                                    log_intervention(student_id, plan, student_data['RISK_SCORE'], primary_factor, needs_counselor)
                                
                                st.markdown("""
                                <div style="background: rgba(34, 197, 94, 0.1); border-radius: 8px; padding: 0.75rem; margin-bottom: 1rem;">
                                    <span style="color: #22c55e;">✓ Plan generated and logged</span>
                                </div>
                                """, unsafe_allow_html=True)
                                
                                if needs_counselor:
                                    st.warning("⚠️ Social-emotional concerns detected. Consider counselor referral.")
                                
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
    # PAGE: INTERVENTION TRACKING
    # ============================================
    
    elif page == "interventions":
        st.markdown('<div class="page-header">📋 Intervention Tracking</div>', unsafe_allow_html=True)
        st.markdown('<div class="page-subtitle">Track and manage student intervention plans and outcomes</div>', unsafe_allow_html=True)
        
        # Guidance banner
        st.markdown("""
        <div class="welcome-banner">
            <div style="display: flex; align-items: flex-start; gap: 12px;">
                <span style="font-size: 1.5rem;">📊</span>
                <div>
                    <div style="color: #22c55e; font-weight: 500; margin-bottom: 0.25rem;">Track Your Progress</div>
                    <div style="color: #a0a0a0; font-size: 0.85rem;">View all intervention plans you've created, mark completed actions, and log outcomes to measure effectiveness.</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Stats overview
        stats = get_intervention_stats()
        if stats:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">Total Plans</div>
                    <div class="metric-value">{stats['TOTAL_PLANS']}</div>
                </div>
                """, unsafe_allow_html=True)
            with col2:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">In Progress</div>
                    <div class="metric-value yellow">{stats['IN_PROGRESS']}</div>
                </div>
                """, unsafe_allow_html=True)
            with col3:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">Completed</div>
                    <div class="metric-value green">{stats['COMPLETED']}</div>
                </div>
                """, unsafe_allow_html=True)
            with col4:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">Counselor Referrals</div>
                    <div class="metric-value">{stats['COUNSELOR_REFERRALS']}</div>
                </div>
                """, unsafe_allow_html=True)
        
        st.markdown("<hr>", unsafe_allow_html=True)
        
        # Filter options
        col_filter, col_spacer = st.columns([1, 3])
        with col_filter:
            filter_status = st.selectbox("Filter by status", ["All", "In Progress", "Completed"])
        
        # Get intervention history
        history_df = get_intervention_history()
        
        if not history_df.empty:
            # Apply filter
            if filter_status == "In Progress":
                history_df = history_df[history_df['STATUS'] == 'In Progress']
            elif filter_status == "Completed":
                history_df = history_df[history_df['STATUS'] == 'Completed']
            
            for _, row in history_df.iterrows():
                status_badge = "badge-yellow" if row['STATUS'] == 'In Progress' else "badge-green"
                counselor_text = " | 🚨 Counselor Referral" if row['COUNSELOR_REFERRAL'] else ''
                created_date = row['PLAN_GENERATED_AT'].strftime('%b %d, %Y') if pd.notna(row['PLAN_GENERATED_AT']) else 'N/A'
                
                with st.container():
                    col_info, col_status = st.columns([3, 1])
                    with col_info:
                        st.markdown(f"**{row['STUDENT_NAME']}** · Grade {int(row['GRADE_LEVEL'])}{counselor_text}")
                        st.caption(f"Risk: {row['RISK_SCORE_AT_PLAN']} | Factor: {row['PRIMARY_RISK_FACTOR']} | Created: {created_date}")
                    with col_status:
                        if row['STATUS'] == 'In Progress':
                            st.warning("In Progress")
                        else:
                            st.success("Completed")
                
                with st.expander(f"📄 View Plan & Log Outcome", expanded=False):
                    st.markdown("**Intervention Plan:**")
                    st.markdown(row['PLAN_TEXT'] if row['PLAN_TEXT'] else "No plan text available")
                    
                    if row['STATUS'] == 'In Progress':
                        st.markdown("<hr>", unsafe_allow_html=True)
                        st.markdown("**Log Outcome:**")
                        
                        interventions = st.text_area(
                            "What interventions were completed?",
                            key=f"interventions_{row['LOG_ID']}",
                            placeholder="e.g., Met with student weekly, contacted parent, arranged tutoring...",
                            height=80
                        )
                        
                        outcome = st.text_area(
                            "Outcome notes",
                            key=f"outcome_{row['LOG_ID']}",
                            placeholder="e.g., Student attendance improved by 15%, GPA increased...",
                            height=80
                        )
                        
                        if st.button("✅ Mark Complete", key=f"complete_{row['LOG_ID']}", type="primary"):
                            if interventions and outcome:
                                if update_intervention_outcome(row['LOG_ID'], interventions, outcome):
                                    st.success("Outcome logged successfully!")
                                    st.cache_data.clear()
                                    st.rerun()
                            else:
                                st.warning("Please fill in both fields before marking complete.")
                    else:
                        if row['INTERVENTIONS_COMPLETED']:
                            st.markdown("<hr>", unsafe_allow_html=True)
                            st.markdown("**Completed Interventions:**")
                            st.markdown(row['INTERVENTIONS_COMPLETED'])
                        if row['OUTCOME_NOTES']:
                            st.markdown("**Outcome:**")
                            st.markdown(row['OUTCOME_NOTES'])
                        if pd.notna(row['OUTCOME_LOGGED_AT']):
                            st.caption(f"Completed on: {row['OUTCOME_LOGGED_AT'].strftime('%b %d, %Y')}")
        else:
            st.markdown("""
            <div class="empty-state">
                <div class="empty-state-icon">📋</div>
                <div>No intervention plans yet</div>
                <div style="font-size: 0.85rem; margin-top: 0.5rem;">Generate a Success Plan from the Success Plans page to start tracking interventions.</div>
            </div>
            """, unsafe_allow_html=True)

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
