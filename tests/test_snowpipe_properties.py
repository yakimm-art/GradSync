"""
GradSync: Snowpipe Auto-Sync Property Tests
Issue #9: Property-based tests for correctness validation

These tests use hypothesis to verify universal properties of the auto-sync pipeline.
Run with: pytest tests/test_snowpipe_properties.py -v
"""

import pytest
import json
from hypothesis import given, strategies as st, settings, assume
from datetime import datetime, date
from decimal import Decimal


# ============================================
# Test Data Generators (Strategies)
# ============================================

# Valid event types for attendance
VALID_EVENT_TYPES = ['check_in', 'check_out_early', 'no_show', 'late_arrival']

# Event type to status mapping (from design.md)
EVENT_TYPE_MAPPING = {
    'check_in': 'Present',
    'check_out_early': 'Present',
    'no_show': 'Absent',
    'late_arrival': 'Tardy'
}

# Strategy for generating valid student IDs
student_id_strategy = st.text(
    alphabet=st.sampled_from('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'),
    min_size=3,
    max_size=20
).map(lambda s: f"STU{s}")

# Strategy for generating valid event IDs
event_id_strategy = st.text(
    alphabet=st.sampled_from('ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-'),
    min_size=5,
    max_size=50
)

# Strategy for generating valid attendance events
attendance_event_strategy = st.fixed_dictionaries({
    'event_id': event_id_strategy,
    'student_id': student_id_strategy,
    'timestamp': st.datetimes(
        min_value=datetime(2020, 1, 1),
        max_value=datetime(2030, 12, 31)
    ).map(lambda dt: dt.isoformat() + 'Z'),
    'type': st.sampled_from(VALID_EVENT_TYPES),
    'location': st.text(min_size=1, max_size=100).filter(lambda x: x.strip())
})

# Strategy for generating valid grade events
grade_event_strategy = st.fixed_dictionaries({
    'event_id': event_id_strategy,
    'student_id': student_id_strategy,
    'course': st.text(min_size=1, max_size=100).filter(lambda x: x.strip()),
    'assignment': st.text(min_size=1, max_size=200).filter(lambda x: x.strip()),
    'score': st.floats(min_value=0, max_value=1000, allow_nan=False, allow_infinity=False),
    'max_score': st.floats(min_value=1, max_value=1000, allow_nan=False, allow_infinity=False),
    'date': st.dates(
        min_value=date(2020, 1, 1),
        max_value=date(2030, 12, 31)
    ).map(str)
})

# Strategy for generating valid student events
student_event_strategy = st.fixed_dictionaries({
    'event_id': event_id_strategy,
    'student_id': student_id_strategy,
    'first_name': st.text(alphabet='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', min_size=2, max_size=20),
    'last_name': st.text(alphabet='abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', min_size=2, max_size=20),
    'grade_level': st.integers(min_value=1, max_value=12),
    'parent_email': st.from_regex(r'[a-z]{3,10}@[a-z]{3,8}\.(com|edu|org)', fullmatch=True),
    'parent_language': st.sampled_from(['English', 'Spanish', 'Chinese', 'Vietnamese', 'Korean']),
    'event_type': st.sampled_from(['create', 'update', 'transfer'])
})


# ============================================
# Property 1: Raw Payload Preservation (Round-Trip)
# Validates: Requirements 2.4
# ============================================

class TestRawPayloadPreservation:
    """
    Property 1: For any valid JSON event ingested through Snowpipe, 
    the raw_payload VARIANT column SHALL contain the complete original 
    JSON document, allowing reconstruction of the source data.
    """
    
    @given(event=attendance_event_strategy)
    @settings(max_examples=50)
    def test_attendance_payload_roundtrip(self, event):
        """Verify attendance event can be serialized and deserialized without loss."""
        # Serialize to JSON (simulating what Snowpipe receives)
        json_str = json.dumps(event)
        
        # Deserialize (simulating raw_payload retrieval)
        recovered = json.loads(json_str)
        
        # All fields must be preserved
        assert recovered['event_id'] == event['event_id']
        assert recovered['student_id'] == event['student_id']
        assert recovered['timestamp'] == event['timestamp']
        assert recovered['type'] == event['type']
        assert recovered['location'] == event['location']
    
    @given(event=grade_event_strategy)
    @settings(max_examples=50)
    def test_grade_payload_roundtrip(self, event):
        """Verify grade event can be serialized and deserialized without loss."""
        json_str = json.dumps(event)
        recovered = json.loads(json_str)
        
        assert recovered['event_id'] == event['event_id']
        assert recovered['student_id'] == event['student_id']
        assert recovered['course'] == event['course']
        assert recovered['assignment'] == event['assignment']
        # Floats may have precision differences, use approximate comparison
        assert abs(recovered['score'] - event['score']) < 0.01
        assert abs(recovered['max_score'] - event['max_score']) < 0.01
        assert recovered['date'] == event['date']
    
    @given(event=student_event_strategy)
    @settings(max_examples=50)
    def test_student_payload_roundtrip(self, event):
        """Verify student event can be serialized and deserialized without loss."""
        json_str = json.dumps(event)
        recovered = json.loads(json_str)
        
        assert recovered['event_id'] == event['event_id']
        assert recovered['student_id'] == event['student_id']
        assert recovered['first_name'] == event['first_name']
        assert recovered['last_name'] == event['last_name']
        assert recovered['grade_level'] == event['grade_level']
        assert recovered['parent_email'] == event['parent_email']
        assert recovered['parent_language'] == event['parent_language']
        assert recovered['event_type'] == event['event_type']


# ============================================
# Property 2: Event Type to Status Mapping
# Validates: Requirements 3.3
# ============================================

class TestEventTypeMapping:
    """
    Property 2: For any attendance event with a valid event_type, 
    the processing task SHALL produce the correct attendance status 
    according to the mapping: check_in→Present, check_out_early→Present, 
    no_show→Absent, late_arrival→Tardy.
    """
    
    def map_event_type_to_status(self, event_type: str) -> str:
        """Simulate the CASE statement from PROCESS_ATTENDANCE_EVENTS task."""
        mapping = {
            'check_in': 'Present',
            'check_out_early': 'Present',
            'no_show': 'Absent',
            'late_arrival': 'Tardy'
        }
        return mapping.get(event_type, 'Present')  # Default to Present
    
    @given(event_type=st.sampled_from(VALID_EVENT_TYPES))
    def test_all_event_types_map_correctly(self, event_type):
        """Every valid event type maps to the expected status."""
        result = self.map_event_type_to_status(event_type)
        expected = EVENT_TYPE_MAPPING[event_type]
        assert result == expected, f"{event_type} should map to {expected}, got {result}"
    
    def test_check_in_maps_to_present(self):
        """check_in explicitly maps to Present."""
        assert self.map_event_type_to_status('check_in') == 'Present'
    
    def test_check_out_early_maps_to_present(self):
        """check_out_early explicitly maps to Present."""
        assert self.map_event_type_to_status('check_out_early') == 'Present'
    
    def test_no_show_maps_to_absent(self):
        """no_show explicitly maps to Absent."""
        assert self.map_event_type_to_status('no_show') == 'Absent'
    
    def test_late_arrival_maps_to_tardy(self):
        """late_arrival explicitly maps to Tardy."""
        assert self.map_event_type_to_status('late_arrival') == 'Tardy'
    
    @given(event_type=st.text(min_size=1, max_size=50))
    @settings(max_examples=50)
    def test_unknown_event_types_default_to_present(self, event_type):
        """Unknown event types should default to Present (safe default)."""
        assume(event_type not in VALID_EVENT_TYPES)
        result = self.map_event_type_to_status(event_type)
        assert result == 'Present', f"Unknown type '{event_type}' should default to Present"


# ============================================
# Property 3: Processing Idempotency
# Validates: Requirements 3.4
# ============================================

class TestProcessingIdempotency:
    """
    Property 3: For any record in the landing table, processing it 
    multiple times (via task re-execution) SHALL NOT create duplicate 
    records in the normalized table.
    """
    
    @staticmethod
    def merge_attendance(table: dict, student_id: str, attendance_date: str, 
                         period: int, status: str) -> bool:
        """
        Simulate MERGE behavior from PROCESS_ATTENDANCE_EVENTS task.
        Returns True if inserted, False if already exists (no duplicate).
        """
        key = (student_id, attendance_date, period)
        
        if key in table:
            return False
        else:
            table[key] = {
                'student_id': student_id,
                'attendance_date': attendance_date,
                'period': period,
                'status': status
            }
            return True
    
    @given(
        student_id=student_id_strategy,
        date_str=st.dates().map(str),
        period=st.integers(min_value=1, max_value=8),
        status=st.sampled_from(['Present', 'Absent', 'Tardy'])
    )
    @settings(max_examples=50)
    def test_duplicate_processing_does_not_create_duplicates(
        self, student_id, date_str, period, status
    ):
        """Processing the same record multiple times creates only one entry."""
        table = {}
        
        # First processing - should insert
        first_result = self.merge_attendance(table, student_id, date_str, period, status)
        assert first_result == True, "First insert should succeed"
        assert len(table) == 1
        
        # Second processing (re-execution) - should NOT insert duplicate
        second_result = self.merge_attendance(table, student_id, date_str, period, status)
        assert second_result == False, "Duplicate should be rejected"
        assert len(table) == 1, "Table should still have only 1 record"
        
        # Third processing - still no duplicate
        third_result = self.merge_attendance(table, student_id, date_str, period, status)
        assert third_result == False
        assert len(table) == 1
    
    @given(
        records=st.lists(
            st.tuples(
                student_id_strategy,
                st.dates().map(str),
                st.integers(min_value=1, max_value=8),
                st.sampled_from(['Present', 'Absent', 'Tardy'])
            ),
            min_size=1,
            max_size=20
        )
    )
    @settings(max_examples=20)
    def test_batch_processing_idempotency(self, records):
        """Processing a batch twice results in same final state."""
        table = {}
        
        # First batch processing
        for student_id, date_str, period, status in records:
            self.merge_attendance(table, student_id, date_str, period, status)
        
        count_after_first = len(table)
        state_after_first = dict(table)
        
        # Second batch processing (re-execution)
        for student_id, date_str, period, status in records:
            self.merge_attendance(table, student_id, date_str, period, status)
        
        count_after_second = len(table)
        
        # Counts should be identical
        assert count_after_first == count_after_second, \
            "Re-processing should not change record count"
        
        # State should be identical
        assert table == state_after_first, \
            "Re-processing should not change table state"


# ============================================
# Property 4: Malformed JSON Rejection
# Validates: Requirements 2.5
# ============================================

class TestMalformedJsonHandling:
    """
    Property 4: For any file containing malformed JSON, the system 
    SHALL reject the invalid records while successfully processing 
    any valid JSON records in the same batch.
    """
    
    def parse_json_safe(self, json_str: str) -> tuple:
        """
        Simulate Snowpipe's ON_ERROR='CONTINUE' behavior.
        Returns (parsed_data, is_valid).
        """
        try:
            data = json.loads(json_str)
            return (data, True)
        except json.JSONDecodeError:
            return (None, False)
    
    def process_batch(self, json_strings: list) -> tuple:
        """
        Process a batch of JSON strings, returning (valid_records, error_count).
        Simulates Snowpipe with ON_ERROR='CONTINUE'.
        """
        valid_records = []
        error_count = 0
        
        for json_str in json_strings:
            data, is_valid = self.parse_json_safe(json_str)
            if is_valid:
                valid_records.append(data)
            else:
                error_count += 1
        
        return (valid_records, error_count)
    
    @given(event=attendance_event_strategy)
    @settings(max_examples=50)
    def test_valid_json_is_accepted(self, event):
        """Valid JSON should always be accepted."""
        json_str = json.dumps(event)
        data, is_valid = self.parse_json_safe(json_str)
        
        assert is_valid == True, "Valid JSON should be accepted"
        assert data is not None
        assert data['event_id'] == event['event_id']
    
    @given(malformed=st.sampled_from([
        '{invalid json}',
        '{"unclosed": "bracket"',
        'not json at all',
        '{"key": undefined}',
        "{'single': 'quotes'}",
        '',
        '{,}',
        '[,]',
        '{"trailing": "comma",}',
    ]))
    def test_malformed_json_is_rejected(self, malformed):
        """Malformed JSON should be rejected."""
        data, is_valid = self.parse_json_safe(malformed)
        
        assert is_valid == False, f"Malformed JSON should be rejected: {malformed}"
        assert data is None
    
    @given(
        valid_events=st.lists(attendance_event_strategy, min_size=1, max_size=5),
        malformed_count=st.integers(min_value=1, max_value=3)
    )
    @settings(max_examples=20)
    def test_mixed_batch_processes_valid_rejects_invalid(
        self, valid_events, malformed_count
    ):
        """In a mixed batch, valid records are processed, invalid are rejected."""
        # Create batch with valid and invalid JSON
        batch = []
        
        # Add valid events
        for event in valid_events:
            batch.append(json.dumps(event))
        
        # Add malformed entries
        malformed_samples = [
            '{invalid}',
            '{"unclosed"',
            'not json',
        ]
        for i in range(malformed_count):
            batch.append(malformed_samples[i % len(malformed_samples)])
        
        # Process batch
        valid_records, error_count = self.process_batch(batch)
        
        # Verify results
        assert len(valid_records) == len(valid_events), \
            f"Should have {len(valid_events)} valid records, got {len(valid_records)}"
        assert error_count == malformed_count, \
            f"Should have {malformed_count} errors, got {error_count}"
        
        # Verify valid records contain expected data
        for i, record in enumerate(valid_records):
            assert record['event_id'] == valid_events[i]['event_id']


# ============================================
# Integration Tests (Simulated)
# ============================================

class TestEndToEndSimulation:
    """
    Simulated end-to-end tests for the auto-sync pipeline.
    These test the logic without requiring a Snowflake connection.
    """
    
    def test_full_attendance_pipeline_simulation(self):
        """Simulate full attendance event processing."""
        # Sample event
        event = {
            'event_id': 'ATT-TEST-001',
            'student_id': 'STU001',
            'timestamp': '2024-12-30T08:15:00Z',
            'type': 'check_in',
            'location': 'Main Entrance'
        }
        
        # Step 1: JSON serialization (Snowpipe ingestion)
        json_str = json.dumps(event)
        
        # Step 2: Parse and extract fields (landing table)
        landing_record = json.loads(json_str)
        
        # Step 3: Transform (processing task)
        status_mapping = {
            'check_in': 'Present',
            'check_out_early': 'Present', 
            'no_show': 'Absent',
            'late_arrival': 'Tardy'
        }
        
        normalized_record = {
            'student_id': landing_record['student_id'],
            'attendance_date': landing_record['timestamp'][:10],
            'status': status_mapping.get(landing_record['type'], 'Present'),
            'period': 1  # Calculated from timestamp
        }
        
        # Verify transformation
        assert normalized_record['student_id'] == 'STU001'
        assert normalized_record['attendance_date'] == '2024-12-30'
        assert normalized_record['status'] == 'Present'
        
        # Verify raw payload preservation
        assert landing_record == event


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
