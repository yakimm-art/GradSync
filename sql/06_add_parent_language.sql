-- ============================================
-- GradSync: Add Parent Language Support
-- Issue 7: Multi-Language Parent Communication
-- ============================================

USE DATABASE GRADSYNC;
USE SCHEMA RAW_DATA;

-- Add parent_language column to STUDENTS table if not exists
ALTER TABLE RAW_DATA.STUDENTS ADD COLUMN IF NOT EXISTS parent_language VARCHAR(50) DEFAULT 'English';

-- Update sample data with diverse languages for testing
UPDATE RAW_DATA.STUDENTS SET parent_language = 'Spanish' WHERE student_id = 'STU001';
UPDATE RAW_DATA.STUDENTS SET parent_language = 'Chinese' WHERE student_id = 'STU002';
UPDATE RAW_DATA.STUDENTS SET parent_language = 'Vietnamese' WHERE student_id = 'STU003';
UPDATE RAW_DATA.STUDENTS SET parent_language = 'Korean' WHERE student_id = 'STU004';
UPDATE RAW_DATA.STUDENTS SET parent_language = 'English' WHERE student_id = 'STU005';

-- Verify the changes
SELECT student_id, first_name, last_name, parent_language 
FROM RAW_DATA.STUDENTS 
ORDER BY student_id;

-- Show supported languages reference
SELECT 'Supported Languages for Cortex TRANSLATE:' as info;
SELECT 'Spanish (es), Chinese (zh), Vietnamese (vi), Korean (ko), Arabic (ar)' as languages_1;
SELECT 'Tagalog (tl), Russian (ru), French (fr), Portuguese (pt), German (de)' as languages_2;
SELECT 'Japanese (ja), Hindi (hi), Urdu (ur), Punjabi (pa), Haitian Creole (ht)' as languages_3;
