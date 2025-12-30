# Implementation Tasks

## Phase 1: Enhanced Risk Scoring (Priority: High)

- [ ] 1.1 Update risk score calculation to include AI signals
  - Modify STUDENT_360_VIEW Dynamic Table
  - Add ai_signal_contribution column
  - Implement 4-factor weighted scoring
  - _Requirements: 3.1, 3.2_

- [ ] 1.2 Create Risk Breakdown component
  - Add pie chart showing factor contributions
  - Show percentage breakdown (Attendance 25%, Grades 25%, Sentiment 25%, AI 25%)
  - Add "Why this score?" explanation section
  - _Requirements: 3.1, 3.2, 3.3_

- [ ] 1.3 Add risk factor highlighting
  - Highlight factors that changed recently
  - Show "New Signal" badge for recent changes
  - Color-code by severity
  - _Requirements: 3.3_

## Phase 2: Note Classification (Priority: High)

- [ ] 2.1 Implement Cortex CLASSIFY_TEXT for notes
  - Create classification function
  - Categories: Academic, Behavioral, Social-Emotional, Attendance, Family, Positive
  - Store classification and confidence score
  - _Requirements: 1.1, 1.2_

- [ ] 2.2 Update teacher note submission flow
  - Auto-classify on submission
  - Show AI classification alongside manual category
  - Flag high-risk classifications for counselor
  - _Requirements: 1.1, 1.2, 1.3_

- [ ] 2.3 Create counselor alert system
  - Filter notes by AI classification
  - Priority queue for Social-Emotional and Family concerns
  - One-click acknowledge/assign
  - _Requirements: 1.3_

## Phase 3: Pattern Detection (Priority: Medium)

- [ ] 3.1 Create pattern analysis function
  - Aggregate notes per student (last 30 days)
  - Use Cortex COMPLETE for pattern detection
  - Store insights in AI_INSIGHTS table
  - _Requirements: 2.1, 2.2_

- [ ] 3.2 Build AI Insights panel
  - Show pattern summary on student profile
  - Display detected themes and concerns
  - Link to contributing notes
  - _Requirements: 2.3_

- [ ] 3.3 Implement escalation detection
  - Compare current vs previous period sentiment
  - Flag escalating concerns
  - Adjust risk score based on trends
  - _Requirements: 2.2_

## Phase 4: Early Warning System (Priority: Medium)

- [ ] 4.1 Create early warning indicators
  - Attendance drop detection (>5% in 2 weeks)
  - Grade decline detection (>10% in 30 days)
  - Sentiment trend analysis
  - _Requirements: 4.1, 4.2_

- [ ] 4.2 Build Watch List dashboard
  - Students with 2+ early warning indicators
  - Predicted risk factors
  - Recommended actions
  - _Requirements: 4.2, 4.3_

- [ ] 4.3 Implement proactive alerts
  - Daily scan for new early warnings
  - Email/notification to assigned teachers
  - Track intervention status
  - _Requirements: 4.1_

## Phase 5: Sentiment Trends (Priority: Medium)

- [ ] 5.1 Create sentiment trend analysis
  - Calculate rolling 90-day sentiment average
  - Detect downward trends (>0.3 drop)
  - Store trend data for visualization
  - _Requirements: 7.1, 7.2_

- [ ] 5.2 Build sentiment trend chart
  - Line chart showing sentiment over time
  - Highlight intervention points
  - Show improvement indicators
  - _Requirements: 7.1, 7.3_

## Phase 6: Enhanced Success Plans (Priority: High)

- [ ] 6.1 Improve Success Plan generation
  - Include AI-detected risk factors in prompt
  - Tailor recommendations to specific concerns
  - Add counselor referral for social-emotional risks
  - _Requirements: 6.1, 6.2, 6.3_

- [ ] 6.2 Add intervention tracking
  - Log when Success Plans are generated
  - Track which recommendations were implemented
  - Measure outcome improvements
  - _Requirements: 6.1_

## Phase 7: Multi-Language Support (Priority: Low)

- [ ] 7.1 Enhance parent communication
  - Auto-detect parent language from records
  - One-click translation button
  - Show side-by-side English/translated
  - _Requirements: 8.1, 8.2, 8.3_

- [ ] 7.2 Add language preference management
  - Allow parents to set preferred language
  - Store in STUDENTS table
  - Default to English if not set
  - _Requirements: 8.1_

## Demo Preparation

- [ ] 8.1 Create demo script showing AI in action
  - Show note classification in real-time
  - Demonstrate pattern detection
  - Explain risk score breakdown
  - Generate Success Plan with AI insights

- [ ] 8.2 Prepare Technical Architecture slide
  - Multi-factor risk scoring diagram
  - Cortex AI integration points
  - Data flow visualization
