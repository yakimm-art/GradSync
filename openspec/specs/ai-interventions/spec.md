# AI Intervention Capabilities

## Purpose
GradSync uses Snowflake Cortex AI to analyze student data and generate actionable interventions.

## Requirements

### Requirement: Sentiment Analysis
The system SHALL analyze teacher notes using Cortex SENTIMENT function.

#### Scenario: Note sentiment scoring
- **WHEN** teacher saves an observation note
- **THEN** SNOWFLAKE.CORTEX.SENTIMENT analyzes the text
- **AND** returns a score from -1 (negative) to +1 (positive)
- **AND** score is stored with the note record

#### Scenario: Sentiment as risk indicator
- **WHEN** average sentiment for a student falls below -0.3
- **THEN** 20 points are added to their risk score

### Requirement: AI-Generated Success Plans
The system SHALL generate personalized intervention strategies using Cortex COMPLETE.

#### Scenario: Success plan generation
- **WHEN** educator requests a Success Plan for an at-risk student
- **THEN** Cortex generates 3-4 specific, actionable recommendations
- **AND** recommendations are based on student's attendance, grades, and sentiment data

#### Scenario: Contextual recommendations
- **WHEN** generating a Success Plan
- **THEN** the AI considers current risk score, attendance patterns, grade trends, and recent teacher note sentiment

### Requirement: Multilingual Parent Communication
The system SHALL translate parent outreach messages using Cortex TRANSLATE.

#### Scenario: Message translation
- **WHEN** educator clicks Translate and Send for a parent message
- **THEN** the message is translated to the parent's preferred language
- **AND** the translated text is displayed for review

#### Scenario: Language detection
- **WHEN** student record includes parent_language field
- **THEN** translation option is shown for non-English languages
