-- GradSync: Sample Data for Demo
-- Run after setup to populate test data

USE DATABASE GRADSYNC_DB;
USE WAREHOUSE GRADSYNC_WH;

-- ============================================
-- SAMPLE STUDENTS
-- ============================================

INSERT INTO RAW_DATA.STUDENTS (student_id, first_name, last_name, grade_level, enrollment_date, parent_email, parent_language)
VALUES
    ('STU001', 'Emma', 'Johnson', 9, '2024-08-15', 'johnson.parent@email.com', 'English'),
    ('STU002', 'Liam', 'Martinez', 9, '2024-08-15', 'martinez.familia@email.com', 'Spanish'),
    ('STU003', 'Sophia', 'Williams', 10, '2023-08-15', 'williams.home@email.com', 'English'),
    ('STU004', 'Noah', 'Brown', 10, '2023-08-15', 'brown.parents@email.com', 'English'),
    ('STU005', 'Olivia', 'Garcia', 11, '2022-08-15', 'garcia.casa@email.com', 'Spanish'),
    ('STU006', 'James', 'Davis', 11, '2022-08-15', 'davis.family@email.com', 'English'),
    ('STU007', 'Ava', 'Rodriguez', 12, '2021-08-15', 'rodriguez.padres@email.com', 'Spanish'),
    ('STU008', 'William', 'Wilson', 12, '2021-08-15', 'wilson.home@email.com', 'English'),
    ('STU009', 'Isabella', 'Chen', 9, '2024-08-15', 'chen.family@email.com', 'Chinese'),
    ('STU010', 'Benjamin', 'Kim', 10, '2023-08-15', 'kim.parents@email.com', 'Korean');

-- ============================================
-- SAMPLE ATTENDANCE (Last 30 days)
-- ============================================

-- Good attendance students
INSERT INTO RAW_DATA.ATTENDANCE (student_id, attendance_date, status, period)
SELECT 
    'STU001',
    DATEADD('day', -seq4(), CURRENT_DATE()),
    'Present',
    1
FROM TABLE(GENERATOR(ROWCOUNT => 20));

INSERT INTO RAW_DATA.ATTENDANCE (student_id, attendance_date, status, period)
SELECT 
    'STU003',
    DATEADD('day', -seq4(), CURRENT_DATE()),
    'Present',
    1
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- At-risk attendance (STU002 - many absences)
INSERT INTO RAW_DATA.ATTENDANCE (student_id, attendance_date, status, period)
SELECT 
    'STU002',
    DATEADD('day', -seq4(), CURRENT_DATE()),
    CASE WHEN seq4() % 3 = 0 THEN 'Absent' ELSE 'Present' END,
    1
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- At-risk attendance (STU005 - tardies)
INSERT INTO RAW_DATA.ATTENDANCE (student_id, attendance_date, status, period)
SELECT 
    'STU005',
    DATEADD('day', -seq4(), CURRENT_DATE()),
    CASE WHEN seq4() % 2 = 0 THEN 'Tardy' ELSE 'Present' END,
    1
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- Critical risk (STU007 - many absences)
INSERT INTO RAW_DATA.ATTENDANCE (student_id, attendance_date, status, period)
SELECT 
    'STU007',
    DATEADD('day', -seq4(), CURRENT_DATE()),
    CASE WHEN seq4() % 2 = 0 THEN 'Absent' ELSE 'Present' END,
    1
FROM TABLE(GENERATOR(ROWCOUNT => 20));

-- ============================================
-- SAMPLE GRADES
-- ============================================

-- Good grades
INSERT INTO RAW_DATA.GRADES (student_id, course_name, assignment_name, score, max_score, grade_date)
VALUES
    ('STU001', 'Algebra I', 'Quiz 1', 92, 100, CURRENT_DATE() - 5),
    ('STU001', 'Algebra I', 'Homework 1', 88, 100, CURRENT_DATE() - 10),
    ('STU001', 'English 9', 'Essay 1', 85, 100, CURRENT_DATE() - 7),
    ('STU003', 'Geometry', 'Test 1', 95, 100, CURRENT_DATE() - 3),
    ('STU003', 'Biology', 'Lab Report', 90, 100, CURRENT_DATE() - 8);

-- Struggling grades (at-risk)
INSERT INTO RAW_DATA.GRADES (student_id, course_name, assignment_name, score, max_score, grade_date)
VALUES
    ('STU002', 'Algebra I', 'Quiz 1', 58, 100, CURRENT_DATE() - 5),
    ('STU002', 'Algebra I', 'Homework 1', 45, 100, CURRENT_DATE() - 10),
    ('STU002', 'English 9', 'Essay 1', 62, 100, CURRENT_DATE() - 7),
    ('STU005', 'US History', 'Test 1', 55, 100, CURRENT_DATE() - 3),
    ('STU005', 'Chemistry', 'Lab Report', 48, 100, CURRENT_DATE() - 8),
    ('STU007', 'AP Calculus', 'Quiz 1', 42, 100, CURRENT_DATE() - 5),
    ('STU007', 'AP Physics', 'Test 1', 38, 100, CURRENT_DATE() - 3);

-- ============================================
-- SAMPLE TEACHER NOTES (with sentiment)
-- ============================================

INSERT INTO APP.TEACHER_NOTES (student_id, teacher_id, note_text, note_category, sentiment_score)
VALUES
    ('STU001', 'TCH001', 'Emma is doing great! She actively participates in class discussions and helps other students.', 'Academic', 0.85),
    ('STU002', 'TCH001', 'Liam seems distracted lately. He has been missing assignments and appears tired in class.', 'Behavioral', -0.65),
    ('STU002', 'TCH002', 'Concerned about Liam - he mentioned family issues at home. May need counselor referral.', 'Social', -0.72),
    ('STU005', 'TCH003', 'Olivia has been arriving late consistently. When present, she struggles to focus.', 'Behavioral', -0.45),
    ('STU007', 'TCH004', 'Ava is at serious risk of not graduating. Multiple missing assignments and poor test scores.', 'Academic', -0.88),
    ('STU007', 'TCH005', 'Had a conversation with Ava - she is working two jobs to help family. Needs support.', 'Social', -0.55),
    ('STU003', 'TCH002', 'Sophia is excelling! She completed the extra credit project with exceptional quality.', 'Academic', 0.92);

-- Verify data loaded
SELECT 'Students' as table_name, COUNT(*) as row_count FROM RAW_DATA.STUDENTS
UNION ALL
SELECT 'Attendance', COUNT(*) FROM RAW_DATA.ATTENDANCE
UNION ALL
SELECT 'Grades', COUNT(*) FROM RAW_DATA.GRADES
UNION ALL
SELECT 'Teacher Notes', COUNT(*) FROM APP.TEACHER_NOTES;
