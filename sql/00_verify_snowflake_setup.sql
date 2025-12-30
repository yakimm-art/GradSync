-- ============================================
-- GradSync: Snowflake Environment Verification
-- Issue #1: Configure Snowflake Account
-- ============================================
-- Run this script in Snowsight to verify your environment is ready.
-- Copy each section and run separately to check results.

USE ROLE ACCOUNTADMIN;

-- ============================================
-- 1.1 VERIFY CORTEX AI ACCESS
-- ============================================
-- These should return results without errors.
-- If you get "function not found", Cortex may not be enabled in your region.

-- Test Sentiment Analysis (should return a number between -1 and 1)
SELECT SNOWFLAKE.CORTEX.SENTIMENT('This student is doing great work!') AS sentiment_test;
-- Expected: ~0.8 (positive)

SELECT SNOWFLAKE.CORTEX.SENTIMENT('The student is struggling and seems disengaged.') AS sentiment_test;
-- Expected: ~-0.6 (negative)

-- Test Text Generation (should return generated text)
SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'mistral-large', 
    'In one sentence, explain why early intervention helps at-risk students.'
) AS complete_test;
-- Expected: A coherent sentence about early intervention

-- Test Translation (should return Spanish text)
SELECT SNOWFLAKE.CORTEX.TRANSLATE(
    'Your child is making excellent progress in class.',
    'en',
    'es'
) AS translate_test;
-- Expected: Spanish translation

-- ============================================
-- 1.2 CHECK HYBRID TABLES AVAILABILITY
-- ============================================
-- Hybrid Tables require specific account configuration.

SHOW PARAMETERS LIKE 'ENABLE_HYBRID_TABLES' IN ACCOUNT;
-- If empty or FALSE, Hybrid Tables may not be available.
-- Options:
--   1. Use a Snowflake Trial account (Hybrid Tables enabled by default)
--   2. Contact Snowflake support to enable
--   3. Use standard tables (slower writes, but functional)

-- ============================================
-- 1.3 CREATE WAREHOUSE
-- ============================================

CREATE WAREHOUSE IF NOT EXISTS GRADSYNC_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for GradSync Native App';

-- Verify warehouse created
SHOW WAREHOUSES LIKE 'GRADSYNC_WH';

-- Test warehouse works
USE WAREHOUSE GRADSYNC_WH;
SELECT CURRENT_WAREHOUSE();
-- Expected: GRADSYNC_WH

-- ============================================
-- 1.4 VERIFY STREAMLIT ACCESS
-- ============================================
-- Streamlit verification must be done in the UI:
-- 1. Go to Snowsight
-- 2. Navigate to: Data Products → Apps
-- 3. Click "+ Streamlit App"
-- 4. If the option exists, Streamlit is enabled

-- Check if Streamlit objects can be created
SELECT SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2024_02');
-- This shows feature bundles; Streamlit should be available

-- ============================================
-- SUMMARY: VERIFICATION CHECKLIST
-- ============================================
-- Run this to get a summary of your environment status:

SELECT 
    'Cortex SENTIMENT' AS feature,
    CASE 
        WHEN TRY_TO_NUMBER(SNOWFLAKE.CORTEX.SENTIMENT('test')) IS NOT NULL 
        THEN '✅ Available' 
        ELSE '❌ Not Available' 
    END AS status
UNION ALL
SELECT 
    'Warehouse GRADSYNC_WH',
    CASE 
        WHEN (SELECT COUNT(*) FROM INFORMATION_SCHEMA.WAREHOUSES WHERE WAREHOUSE_NAME = 'GRADSYNC_WH') > 0 
        THEN '✅ Created' 
        ELSE '❌ Not Found' 
    END;

-- ============================================
-- NEXT STEPS
-- ============================================
-- If all checks pass:
--   1. Mark Issue #1 tasks as complete
--   2. Proceed to Issue #2: Create Database Schema
--   3. Run: sql/01_setup_database.sql
