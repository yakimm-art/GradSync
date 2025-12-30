# Risk Analytics Capabilities

## Purpose
GradSync uses Dynamic Tables and Cortex AI to calculate student risk scores and provide real-time insights.

## Requirements

### Requirement: Student 360 View
The system SHALL maintain a unified view of each student combining attendance, grades, and teacher notes.

#### Scenario: Dynamic Table refresh
- **WHEN** source data changes in RAW_DATA tables
- **THEN** STUDENT_360_VIEW Dynamic Table refreshes within 1 hour
- **AND** risk scores are recalculated automatically

#### Scenario: Risk score calculation
- **WHEN** student data is aggregated
- **THEN** risk score is computed as weighted combination of attendance rate, GPA, and negative sentiment indicator
- **AND** score ranges from 0 (low risk) to 100 (critical risk)

### Requirement: At-Risk Student Identification
The system SHALL flag students with risk scores above threshold levels.

#### Scenario: Risk level classification
- **WHEN** risk score is calculated
- **THEN** students are classified as Critical (>=70), High (>=50), Moderate (>=30), or Low (<30)

#### Scenario: Dashboard filtering
- **WHEN** educator views the dashboard
- **THEN** at-risk students (score >= 30) are displayed sorted by risk score descending

### Requirement: Classroom Heatmap
The system SHALL provide aggregate risk metrics by grade level.

#### Scenario: Grade-level summary
- **WHEN** educator views the heatmap
- **THEN** each grade level shows total student count, count by risk level, average attendance rate, and average GPA
