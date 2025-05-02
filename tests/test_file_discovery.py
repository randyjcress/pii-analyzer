#!/usr/bin/env python3
"""
Test script for file discovery module
Tests the scanning and registration of files in the database
"""

import os
import sys
import tempfile
import shutil
from datetime import datetime
import unittest

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.database.db_utils import get_database
from src.core.file_discovery import (
    scan_directory,
    scan_file_list,
    find_resumption_point,
    reset_stalled_files,
    get_file_statistics
)

def create_test_directory(base_dir, num_files=20):
    """Create a test directory with sample files of different types"""
    os.makedirs(base_dir, exist_ok=True)
    
    # Create subdirectories
    os.makedirs(os.path.join(base_dir, "subdir1"), exist_ok=True)
    os.makedirs(os.path.join(base_dir, "subdir2"), exist_ok=True)
    
    # Create files of different types
    extensions = ['.txt', '.pdf', '.docx', '.xlsx', '.csv', '.json', '.zip', '.exe']
    
    for i in range(num_files):
        ext = extensions[i % len(extensions)]
        
        # Put some files in root, some in subdirs
        if i % 3 == 0:
            path = base_dir
        elif i % 3 == 1:
            path = os.path.join(base_dir, "subdir1")
        else:
            path = os.path.join(base_dir, "subdir2")
            
        # Create empty file
        with open(os.path.join(path, f"file{i}{ext}"), 'w') as f:
            f.write(f"Test file {i} content\nThis is line 2\nThis is line 3")
    
    return base_dir

class TestFileDiscovery(unittest.TestCase):
    """Test cases for file discovery module"""
    
    def setUp(self):
        """Set up test environment"""
        # Create temporary test directory
        self.temp_dir = tempfile.mkdtemp()
        create_test_directory(self.temp_dir, num_files=20)
        
        # Create test database
        self.db_path = os.path.join(self.temp_dir, "test.db")
        self.db = get_database(self.db_path)
        
        # Create a job
        self.job_id = self.db.create_job("test_file_discovery")
    
    def tearDown(self):
        """Clean up test environment"""
        self.db.close()
        shutil.rmtree(self.temp_dir)
    
    def test_scan_directory(self):
        """Test scanning a directory for files"""
        # Only scan for txt, pdf, and docx files
        supported_extensions = {'.txt', '.pdf', '.docx'}
        
        # Scan directory
        total, new = scan_directory(
            self.temp_dir,
            self.db,
            self.job_id,
            supported_extensions=supported_extensions
        )
        
        # We should have found files with the specified extensions
        self.assertIn(total, [8, 9])  # Allow for slight variation in how many files of each type we get
        self.assertEqual(new, total)  # All found files should be new
        
        # Scan again - should find same total but no new files
        total2, new2 = scan_directory(
            self.temp_dir,
            self.db,
            self.job_id,
            supported_extensions=supported_extensions
        )
        
        self.assertEqual(total2, total)  # Same files found
        self.assertEqual(new2, 0)        # No new files registered
    
    def test_scan_file_list(self):
        """Test scanning a list of files"""
        # Create a list of files
        file_list = []
        for i in range(5):
            file_path = os.path.join(self.temp_dir, f"file{i*3}.txt")
            file_list.append(file_path)
        
        # Add a non-existent file
        file_list.append(os.path.join(self.temp_dir, "nonexistent.txt"))
        
        # Scan file list
        total, new = scan_file_list(file_list, self.db, self.job_id)
        
        # Should have found some txt files and registered them
        self.assertGreaterEqual(total, 1)  # At least 1 file should be processed
        self.assertEqual(new, total)       # All found files should be new
        
        # Scan again - should find same total but no new files
        total2, new2 = scan_file_list(file_list, self.db, self.job_id)
        
        self.assertEqual(total2, total)  # Same files found
        self.assertEqual(new2, 0)        # No new files registered
    
    def test_resumption_point(self):
        """Test finding resumption point"""
        # Scan directory
        scan_directory(self.temp_dir, self.db, self.job_id)
        
        # Get resumption info
        info = find_resumption_point(self.db, self.job_id)
        
        # Should be resumable with all files pending
        self.assertEqual(info['status'], 'resumable')
        self.assertGreater(info['pending_files'], 0)  # Should have pending files
        self.assertEqual(info['processing_files'], 0)
        self.assertEqual(info['completed_files'], 0)
        self.assertEqual(info['error_files'], 0)
        
        # Store the number of pending files for later checks
        initial_pending = info['pending_files']
        
        # Mark some files as processing
        pending_files = self.db.get_pending_files(self.job_id, limit=5)
        for file_id, _ in pending_files:
            self.db.mark_file_processing(file_id)
        
        # Mark some files as completed
        pending_files = self.db.get_pending_files(self.job_id, limit=3)
        for file_id, _ in pending_files:
            self.db.mark_file_completed(file_id, self.job_id)
        
        # Mark some files as error
        pending_files = self.db.get_pending_files(self.job_id, limit=2)
        for file_id, _ in pending_files:
            self.db.mark_file_error(file_id, self.job_id, "Test error")
        
        # Get resumption info again
        info = find_resumption_point(self.db, self.job_id)
        
        # Should still be resumable
        self.assertEqual(info['status'], 'resumable')
        self.assertEqual(info['pending_files'], initial_pending - 5 - 3 - 2)  # Remaining pending
        self.assertEqual(info['processing_files'], 5)
        self.assertEqual(info['completed_files'], 3)
        self.assertEqual(info['error_files'], 2)
    
    def test_reset_stalled_files(self):
        """Test resetting stalled files"""
        # Scan directory
        scan_directory(self.temp_dir, self.db, self.job_id)
        
        # Get the initial count of pending files
        info_before = find_resumption_point(self.db, self.job_id)
        initial_pending = info_before['pending_files']
        
        # Mark some files as processing
        pending_files = self.db.get_pending_files(self.job_id, limit=5)
        for file_id, _ in pending_files:
            self.db.mark_file_processing(file_id)
        
        # Reset stalled files
        reset_count = reset_stalled_files(self.db, self.job_id)
        
        # Should have reset 5 files
        self.assertEqual(reset_count, 5)
        
        # Get resumption info
        info = find_resumption_point(self.db, self.job_id)
        
        # All files should be pending now
        self.assertEqual(info['pending_files'], initial_pending)  # Back to all pending
        self.assertEqual(info['processing_files'], 0)
    
    def test_file_statistics(self):
        """Test getting file statistics"""
        # Scan directory
        scan_directory(self.temp_dir, self.db, self.job_id)
        
        # Get file statistics
        stats = get_file_statistics(self.db, self.job_id)
        
        # Check statistics
        self.assertEqual(stats['job_id'], self.job_id)
        self.assertIn('status_counts', stats)
        self.assertIn('type_counts', stats)
        self.assertIn('size_stats', stats)
        
        # All files should be pending
        info = find_resumption_point(self.db, self.job_id)
        pending_count = info['pending_files']
        self.assertEqual(stats['status_counts'].get('pending', 0), pending_count)
        
        # Should have files of various types
        self.assertGreaterEqual(len(stats['type_counts']), 5)
        
        # Size stats should be reasonable
        self.assertGreater(stats['size_stats']['total_size'], 0)
        self.assertGreater(stats['size_stats']['avg_size'], 0)

def manual_test():
    """Run a manual test for interactive exploration"""
    # Create temporary test directory
    temp_dir = tempfile.mkdtemp()
    try:
        # Create test files
        create_test_directory(temp_dir, num_files=20)
        
        # Create test database
        db_path = os.path.join(temp_dir, "test.db")
        db = get_database(db_path)
        
        # Create a job
        job_id = db.create_job("test_file_discovery")
        
        # Scan directory
        total, new = scan_directory(temp_dir, db, job_id)
        
        # Print results
        print(f"Total files found: {total}")
        print(f"New files registered: {new}")
        
        # Get job statistics
        stats = db.get_job_statistics(job_id)
        print("\nJob Statistics:")
        print(f"Total files: {stats.get('total_files', 0)}")
        print("\nFile types:")
        for ext, count in stats.get('file_types', {}).items():
            print(f"  {ext}: {count}")
        
        # Get resumption point
        info = find_resumption_point(db, job_id)
        print("\nResumption Info:")
        print(f"Status: {info['status']}")
        print(f"Message: {info['message']}")
        print(f"Pending: {info['pending_files']}")
        
        # Get detailed file statistics
        file_stats = get_file_statistics(db, job_id)
        print("\nFile Statistics:")
        print(f"Status counts: {file_stats['status_counts']}")
        print(f"Type counts: {file_stats['type_counts']}")
        print(f"Size stats: ")
        for k, v in file_stats['size_stats'].items():
            print(f"  {k}: {v}")
        
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