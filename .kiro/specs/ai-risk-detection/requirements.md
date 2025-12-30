# Requirements Document

## Introduction

Enhanced AI-powered risk detection for GradSync that analyzes unstructured data (teacher notes, assignment submissions, behavioral reports) to identify hidden academic and social-emotional risks before they escalate. This feature demonstrates "AI for Good" in education by using Cortex AI to spot patterns humans might miss.

## Glossary

- **Risk Signal**: An indicator from unstructured data that suggests a student may need intervention
- **Cortex AI**: Snowflake's built-in AI functions (SENTIMENT, COMPLETE, CLASSIFY_TEXT)
- **Multi-Factor Risk Score**: Aggregate score combining attendance, grades, sentiment, and AI-detected signals
- **Early Warning System**: Automated detection of at-risk students before traditional metrics show problems

## Requirements

### Requirement 1: AI-Powered Note Classification

**User Story:** As a school administrator, I want the system to automatically classify teacher notes by concern type, so that I can quickly identify students with specific issues (academic, behavioral, social-emotional, attendance).

#### Acceptance Criteria

1. WHEN a teacher submits an observation note, THE system SHALL use Cortex AI to classify it into categories: Academic Struggle, Behavioral Concern, Social-Emotional Risk, Attendance Pattern, Family Situation, Positive Progress
2. WHEN displaying classified notes, THE system SHALL show the AI-detected category alongside the teacher's manual category
3. WHEN a note is classified as high-risk (Social-Emotional, Family Situation), THE system SHALL flag it for counselor review

### Requirement 2: Hidden Pattern Detection

**User Story:** As a counselor, I want the AI to detect hidden patterns across multiple notes for a student, so that I can identify concerning trends that individual teachers might not see.

#### Acceptance Criteria

1. WHEN a student has 3+ notes within 30 days, THE system SHALL use Cortex COMPLETE to generate a pattern analysis summary
2. WHEN the pattern analysis detects escalating concerns, THE system SHALL increase the student's risk score
3. WHEN displaying the student profile, THE system SHALL show "AI Insights" with detected patterns

### Requirement 3: Explainable Risk Scoring

**User Story:** As a teacher, I want to understand WHY a student is flagged as at-risk, so that I can address the specific issues in my interventions.

#### Acceptance Criteria

1. WHEN displaying a student's risk score, THE system SHALL show a breakdown of contributing factors with percentages
2. WHEN Cortex AI contributes to the risk score, THE system SHALL provide a human-readable explanation
3. WHEN a risk factor changes significantly, THE system SHALL highlight it as a "New Signal"

### Requirement 4: Predictive Early Warning

**User Story:** As a principal, I want the system to predict which students are likely to become at-risk in the next 30 days, so that we can intervene proactively.

#### Acceptance Criteria

1. WHEN analyzing student data, THE system SHALL use Cortex COMPLETE to identify students showing early warning signs
2. WHEN a student shows 2+ early warning indicators, THE system SHALL add them to a "Watch List"
3. WHEN displaying the Watch List, THE system SHALL show predicted risk factors and recommended actions

### Requirement 5: Assignment Submission Analysis

**User Story:** As a teacher, I want the system to analyze patterns in assignment submissions (late, missing, declining quality), so that I can identify struggling students earlier.

#### Acceptance Criteria

1. WHEN a student has 3+ late or missing assignments in 2 weeks, THE system SHALL flag this as an attendance/engagement risk
2. WHEN grade trends show a 10%+ decline over 30 days, THE system SHALL trigger an early warning
3. WHEN displaying assignment patterns, THE system SHALL show a visual trend line

### Requirement 6: AI-Generated Intervention Recommendations

**User Story:** As a teacher, I want specific, actionable intervention recommendations based on the AI's analysis, so that I know exactly what steps to take.

#### Acceptance Criteria

1. WHEN generating a Success Plan, THE system SHALL tailor recommendations to the specific risk factors detected
2. WHEN a student has social-emotional risks, THE system SHALL recommend counselor involvement
3. WHEN a student has academic risks, THE system SHALL suggest specific tutoring resources or study strategies

### Requirement 7: Sentiment Trend Analysis

**User Story:** As a counselor, I want to see how a student's sentiment in teacher notes has changed over time, so that I can identify students whose situation is deteriorating.

#### Acceptance Criteria

1. WHEN displaying a student profile, THE system SHALL show a sentiment trend chart over the last 90 days
2. WHEN sentiment shows a downward trend of 0.3+ points, THE system SHALL flag this as a concern
3. WHEN sentiment improves after intervention, THE system SHALL highlight this as a success indicator

### Requirement 8: Multi-Language Support for Parent Communication

**User Story:** As a teacher, I want to communicate with parents in their preferred language, so that language barriers don't prevent family engagement.

#### Acceptance Criteria

1. WHEN generating parent outreach, THE system SHALL detect the parent's preferred language from student records
2. WHEN the parent language is not English, THE system SHALL offer one-click translation using Cortex TRANSLATE
3. WHEN displaying translated messages, THE system SHALL show both English and translated versions
