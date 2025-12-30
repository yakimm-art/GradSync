# Data Entry Capabilities

## Purpose
GradSync provides three methods for data ingestion, each optimized for different user types and use cases.

## Requirements

### Requirement: Bulk Upload
School administrators SHALL be able to upload CSV or Excel files containing student data through a drag-and-drop interface.

#### Scenario: CSV file upload
- **WHEN** admin uploads a valid CSV file with student roster data
- **THEN** the system stages the data and processes it into RAW_DATA tables
- **AND** displays a preview of imported records

#### Scenario: Excel file upload
- **WHEN** admin uploads an Excel file exported from Canvas or PowerSchool
- **THEN** the system parses the file and maps columns to expected schema
- **AND** reports the number of successfully imported rows

#### Scenario: Invalid file format
- **WHEN** admin uploads a file with missing required columns
- **THEN** the system displays a validation error
- **AND** shows the expected file format

### Requirement: Direct Entry via Hybrid Tables
Teachers SHALL be able to log student observations with sub-100ms write latency using Hybrid Tables.

#### Scenario: Save teacher observation
- **WHEN** teacher submits an observation note for a student
- **THEN** the note is written to APP.TEACHER_NOTES Hybrid Table
- **AND** Cortex sentiment analysis runs on the note text
- **AND** the sentiment score is stored with the note

#### Scenario: Real-time feedback
- **WHEN** teacher saves a note
- **THEN** the UI displays the sentiment result immediately
- **AND** confirms the save completed

### Requirement: Auto-Sync via Snowpipe
IT departments SHALL be able to configure automatic data ingestion from district systems using Snowpipe.

#### Scenario: Attendance event ingestion
- **WHEN** district system pushes attendance events to the configured stage
- **THEN** Snowpipe automatically loads data into landing tables
- **AND** Stream + Task processes events into normalized ATTENDANCE table

#### Scenario: Continuous refresh
- **WHEN** new data arrives in the external stage
- **THEN** ingestion occurs within minutes without manual intervention
