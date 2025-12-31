# Requirements Document

## Introduction

This document specifies the requirements for configuring Snowpipe auto-sync functionality in GradSync. Snowpipe enables automatic, continuous data ingestion from external district systems (SIS, LMS) into Snowflake without manual intervention. This is an optional advanced feature designed for IT departments that want to automate data flow from their existing systems.

## Glossary

- **Snowpipe**: A Snowflake feature that enables continuous, serverless data loading from files in cloud storage
- **Stage**: A Snowflake object that points to a location where data files are stored (internal or external)
- **Storage Integration**: A Snowflake object that stores credentials for accessing external cloud storage
- **Stream**: A Snowflake object that records data manipulation language (DML) changes to a table
- **Task**: A Snowflake object that executes SQL statements on a schedule or when triggered
- **Landing Table**: A staging table where raw ingested data is initially stored before processing
- **SIS**: Student Information System - the district's primary student data source
- **LMS**: Learning Management System - systems like Canvas or Blackboard that manage grades

## Requirements

### Requirement 1

**User Story:** As an IT administrator, I want to configure external cloud storage stages, so that Snowpipe can automatically ingest files from our district's cloud storage.

#### Acceptance Criteria

1. WHEN an IT administrator selects AWS S3 as the cloud provider THEN the system SHALL provide a template for creating an S3 storage integration and external stage
2. WHEN an IT administrator selects Azure Blob Storage as the cloud provider THEN the system SHALL provide a template for creating an Azure storage integration and external stage
3. WHEN an IT administrator selects Google Cloud Storage as the cloud provider THEN the system SHALL provide a template for creating a GCS storage integration and external stage
4. WHEN a storage integration is created THEN the system SHALL validate connectivity to the external storage location
5. IF the storage integration credentials are invalid THEN the system SHALL display a clear error message indicating the authentication failure

### Requirement 2

**User Story:** As an IT administrator, I want to set up Snowpipe for automatic data ingestion, so that attendance, grades, and student data flow into GradSync without manual uploads.

#### Acceptance Criteria

1. WHEN Snowpipe is configured for attendance events THEN the system SHALL automatically ingest JSON files from the attendance folder within 2 minutes of file arrival
2. WHEN Snowpipe is configured for grade events THEN the system SHALL automatically ingest JSON files from the grades folder within 2 minutes of file arrival
3. WHEN Snowpipe is configured for student updates THEN the system SHALL automatically ingest JSON files from the students folder within 2 minutes of file arrival
4. WHEN a file is ingested THEN the system SHALL store the raw payload in a VARIANT column for audit purposes
5. IF a file contains malformed JSON THEN the system SHALL log the error and continue processing valid files

### Requirement 3

**User Story:** As an IT administrator, I want streams and tasks to process landing table data, so that raw ingested data is transformed into normalized tables automatically.

#### Acceptance Criteria

1. WHEN new records appear in the attendance landing table THEN the stream SHALL capture the changes within 1 minute
2. WHEN the attendance stream has data THEN the processing task SHALL transform and insert records into the normalized ATTENDANCE table
3. WHEN the processing task runs THEN the system SHALL map event types to attendance statuses (check_in→Present, no_show→Absent, late_arrival→Tardy)
4. WHEN the processing task completes THEN the system SHALL mark processed records to prevent duplicate processing
5. IF the processing task fails THEN the system SHALL retain the original landing table data for retry

### Requirement 4

**User Story:** As an IT administrator, I want to monitor Snowpipe status and ingestion metrics, so that I can ensure data is flowing correctly and troubleshoot issues.

#### Acceptance Criteria

1. WHEN an IT administrator views the auto-sync status THEN the system SHALL display the current state of each Snowpipe (RUNNING, PAUSED, STOPPED_ON_ERROR)
2. WHEN an IT administrator views ingestion metrics THEN the system SHALL show the count of files processed in the last 24 hours
3. WHEN an IT administrator views ingestion metrics THEN the system SHALL show the count of records ingested in the last 24 hours
4. WHEN an IT administrator views error logs THEN the system SHALL display any ingestion failures with file names and error messages
5. WHEN a Snowpipe encounters repeated failures THEN the system SHALL provide a notification mechanism for alerting administrators

### Requirement 5

**User Story:** As an IT administrator, I want to test the auto-sync pipeline end-to-end, so that I can verify the configuration before going live.

#### Acceptance Criteria

1. WHEN an IT administrator uploads a test JSON file to the stage THEN the system SHALL provide sample test files for each data type (attendance, grades, students)
2. WHEN a test file is uploaded THEN the system SHALL ingest and process the file within 5 minutes
3. WHEN the test completes THEN the system SHALL display a verification report showing records ingested and any errors
4. WHEN the test succeeds THEN the system SHALL confirm data appears in the normalized tables
5. IF the test fails THEN the system SHALL provide diagnostic information to help identify the issue
