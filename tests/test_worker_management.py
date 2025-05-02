#!/usr/bin/env python3
"""
Test script for worker management module
Tests the parallel processing of files using the database
"""

import os
import sys
import tempfile
import shutil
import time
import unittest
from typing import Dict, Any, List

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.db_utils import get_database
from src.core.file_discovery import scan_directory
from src.core.worker_management import (
    process_files_parallel,
    process_single_file,
    process_single_file_thread_safe,
    estimate_completion_time,
    interrupt_processing,
    SafeQueue
)

# Mock processing function for testing
def mock_process_file(file_path: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Mock processing function that simulates finding entities"""
    # Simulate processing delay
    delay = settings.get('delay', 0.01)
    time.sleep(delay)
    
    # Read file to simulate processing
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Generate fake entities based on content
    entities = []
    if 'test' in content.lower():
        entities.append({
            'entity_type': 'TEST_ENTITY',
            'text': 'test',
            'start': content.lower().find('test'),
            'end': content.lower().find('test') + 4,
            'score': 0.95
        })
    
    # Add a fake SSN for every file
    entities.append({
        'entity_type': 'SSN',
        'text': '123-45-6789',
        'start': 0,
        'end': 11,
        'score': 0.99
    })
    
    # Add a credit card if specified in settings
    if settings.get('add_cc', False):
        entities.append({
            'entity_type': 'CREDIT_CARD',
            'text': '4111-1111-1111-1111',
            'start': 20,
            'end': 39,
            'score': 0.98
        })
    
    return {
        'file_path': file_path,
        'entities': entities,
        'file_size': os.path.getsize(file_path)
    }

class TestWorkerManagement(unittest.TestCase):
    """Test cases for worker management module"""
    
    def setUp(self):
        """Set up test environment"""
        from tests.test_file_discovery import create_test_directory
        
        # Create temporary test directory
        self.temp_dir = tempfile.mkdtemp()
        create_test_directory(self.temp_dir, num_files=20)
        
        # Create test database
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self.db = get_database(self.db_path)
        
        # Create a job
        self.job_id = self.db.create_job("test_worker_management")
        
        # Scan directory to register files
        scan_directory(self.temp_dir, self.db, self.job_id)
    
    def tearDown(self):
        """Clean up test environment"""
        self.db.close()
        shutil.rmtree(self.temp_dir)
    
    def test_parallel_processing(self):
        """Test thread-safe parallel processing of files"""
        # Process files with mock processor
        stats = process_files_parallel(
            self.db,
            self.job_id,
            mock_process_file,
            max_workers=2,
            batch_size=5
        )
        
        # Check processing stats
        self.assertEqual(stats['job_id'], self.job_id)
        self.assertGreater(stats['total_processed'], 0)
        
        # Verify job was marked as completed
        job = self.db.get_job(self.job_id)
        self.assertEqual(job['status'], 'completed')
        
        # Check entity storage
        job_data = self.db.export_to_json(self.job_id)
        
        # All files should be completed
        completed_files = sum(1 for f in job_data['results'] if f['status'] == 'completed')
        self.assertEqual(completed_files, job['total_files'])
        
        # Check for SSN entities
        ssn_count = 0
        for result_file in job_data['results']:
            if result_file['status'] == 'completed':
                # Count SSN entities
                for entity in result_file['entities']:
                    if entity['entity_type'] == 'SSN':
                        ssn_count += 1
        
        # Should have SSN entities
        self.assertGreater(ssn_count, 0)
    
    def test_processing_with_settings(self):
        """Test processing with custom settings"""
        # Process files with settings
        settings = {
            'delay': 0.02,  # Longer delay
            'add_cc': True  # Add credit card entities
        }
        
        stats = process_files_parallel(
            self.db,
            self.job_id,
            mock_process_file,
            max_workers=2,
            batch_size=5,
            settings=settings
        )
        
        # Check processing stats
        self.assertGreater(stats['total_processed'], 0)
        
        # Check for credit card entities
        job_data = self.db.export_to_json(self.job_id)
        
        # Count credit card entities
        cc_count = 0
        for result_file in job_data['results']:
            for entity in result_file.get('entities', []):
                if entity.get('entity_type') == 'CREDIT_CARD':
                    cc_count += 1
        
        # Should have credit card entities
        self.assertGreater(cc_count, 0)
    
    def test_limited_processing(self):
        """Test processing with a file limit"""
        # Process only a subset of files
        max_files = 5
        
        stats = process_files_parallel(
            self.db,
            self.job_id,
            mock_process_file,
            max_workers=2,
            batch_size=3,
            max_files=max_files
        )
        
        # Check processing stats
        self.assertEqual(stats['total_processed'], max_files)
        
        # Job should be marked as interrupted since not all files were processed
        job = self.db.get_job(self.job_id)
        self.assertEqual(job['status'], 'interrupted')
        
        # Count completed files
        job_data = self.db.export_to_json(self.job_id)
        completed_files = sum(1 for f in job_data['results'] if f['status'] == 'completed')
        self.assertEqual(completed_files, max_files)
    
    def test_thread_safe_queue(self):
        """Test the thread-safe queue implementation"""
        queue = SafeQueue()
        
        # Add some processed items
        for _ in range(10):
            queue.add_processed()
        
        # Add some errors
        for _ in range(3):
            queue.add_error()
        
        # Check stats
        processed, errors = queue.get_stats()
        self.assertEqual(processed, 10)
        self.assertEqual(errors, 3)
    
    def test_estimate_completion_time(self):
        """Test estimation of completion time"""
        # Process some files
        max_files = 5
        process_files_parallel(
            self.db,
            self.job_id,
            mock_process_file,
            max_workers=1,
            batch_size=2,
            max_files=max_files
        )
        
        # Update job status back to running for testing
        self.db.update_job_status(self.job_id, 'running')
        
        # Get estimate
        estimate = estimate_completion_time(self.db, self.job_id)
        
        # Check estimate fields
        self.assertEqual(estimate['status'], 'running')
        self.assertEqual(estimate['processed_files'], max_files)
        self.assertGreater(estimate['total_files'], max_files)
        self.assertGreater(estimate['remaining_files'], 0)
        self.assertGreater(estimate['percent_complete'], 0)
        self.assertLess(estimate['percent_complete'], 100)
    
    def test_interrupt_processing(self):
        """Test interruption of processing"""
        # Mark some files as processing
        pending_files = self.db.get_pending_files(self.job_id, limit=5)
        for file_id, _ in pending_files:
            self.db.mark_file_processing(file_id)
        
        # Update job status to running
        self.db.update_job_status(self.job_id, 'running')
        
        # Interrupt processing
        success = interrupt_processing(self.db, self.job_id)
        
        # Should succeed
        self.assertTrue(success)
        
        # Job should be marked as interrupted
        job = self.db.get_job(self.job_id)
        self.assertEqual(job['status'], 'interrupted')
        
        # All files should be pending or completed (none processing)
        job_data = self.db.export_to_json(self.job_id)
        processing_files = sum(1 for f in job_data['results'] if f['status'] == 'processing')
        self.assertEqual(processing_files, 0)

def manual_test():
    """Run a manual test for interactive exploration"""
    from tests.test_file_discovery import create_test_directory
    
    # Create temporary test directory
    temp_dir = tempfile.mkdtemp()
    try:
        # Create test files
        create_test_directory(temp_dir, num_files=50)
        
        # Create test database
        db_path = os.path.join(temp_dir, "test.db")
        db = get_database(db_path)
        
        # Create a job
        job_id = db.create_job("test_worker_management")
        
        # Scan directory
        total, new = scan_directory(temp_dir, db, job_id)
        print(f"Registered {new} files for processing")
        
        # Process files
        print("Starting parallel processing...")
        start_time = time.time()
        stats = process_files_parallel(
            db,
            job_id,
            mock_process_file,
            max_workers=4,
            batch_size=10,
            settings={'add_cc': True}
        )
        elapsed = time.time() - start_time
        
        # Print results
        print(f"\nProcessing complete in {elapsed:.2f} seconds:")
        print(f"Files processed: {stats['total_processed']}")
        print(f"Errors: {stats['total_errors']}")
        print(f"Files per second: {stats['files_per_second']:.2f}")
        
        # Get job statistics
        job_stats = db.get_job_statistics(job_id)
        print("\nEntity counts:")
        for entity_type, count in job_stats.get('entity_types', {}).items():
            print(f"  {entity_type}: {count}")
        
        # Close database
        db.close()
        
        print(f"\nTest database at: {db_path}")
        print(f"Test files at: {temp_dir}")
        print("Remember to delete these directories when done testing.")
        
    except Exception as e:
        print(f"Error in manual test: {e}")
        shutil.rmtree(temp_dir)

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--manual":
        manual_test()
    else:
        unittest.main() 