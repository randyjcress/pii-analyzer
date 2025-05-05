#!/usr/bin/env python3
"""
Unit tests for inspect_db.py focusing on error file reset functionality
"""

import unittest
import os
import sqlite3
import tempfile
import sys
from datetime import datetime

# Add the parent directory to the path so we can import inspect_db
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import inspect_db

class TestInspectDb(unittest.TestCase):
    """
    Tests for inspect_db.py
    """
    
    def setUp(self):
        """
        Set up a test database with sample data
        """
        # Create a temporary database file
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.db_path = self.temp_db.name
        self.temp_db.close()
        
        # Create a connection to the database
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        
        # Create the necessary tables
        self.cursor.executescript("""
        CREATE TABLE jobs (
            job_id INTEGER PRIMARY KEY,
            name TEXT,
            status TEXT,
            start_time TEXT,
            last_updated TEXT,
            total_files INTEGER DEFAULT 0,
            processed_files INTEGER DEFAULT 0,
            error_files INTEGER DEFAULT 0
        );
        
        CREATE TABLE job_metadata (
            job_id INTEGER,
            key TEXT,
            value TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(job_id)
        );
        
        CREATE TABLE files (
            file_id INTEGER PRIMARY KEY,
            job_id INTEGER,
            file_path TEXT,
            file_type TEXT,
            file_size INTEGER,
            status TEXT,
            process_start TEXT,
            process_end TEXT,
            error_message TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(job_id)
        );
        
        CREATE TABLE results (
            file_id INTEGER PRIMARY KEY,
            processing_time REAL,
            entity_count INTEGER,
            metadata TEXT,
            FOREIGN KEY(file_id) REFERENCES files(file_id)
        );
        """)
        
        # Insert a sample job
        self.cursor.execute("""
        INSERT INTO jobs (job_id, name, status, total_files, processed_files, error_files)
        VALUES (1, 'Test Job', 'running', 10, 5, 5)
        """)
        
        # Insert sample files with different error conditions
        test_files = [
            # Normal error file that should be reset
            (1, 1, '/path/to/normal.txt', 'txt', 1000, 'error', '2025-01-01 12:00:00', '2025-01-01 12:01:00', 'Test error'),
            
            # Error file that's too small (should be excluded)
            (2, 1, '/path/to/small.txt', 'txt', 50, 'error', '2025-01-01 12:00:00', '2025-01-01 12:01:00', 'File is empty'),
            
            # Temp file (should be excluded)
            (3, 1, '/path/to/~$temp.docx', 'docx', 1000, 'error', '2025-01-01 12:00:00', '2025-01-01 12:01:00', 'Temporary file'),
            
            # Another normal error file
            (4, 1, '/path/to/another.pdf', 'pdf', 2000, 'error', '2025-01-01 12:00:00', '2025-01-01 12:01:00', 'PDF error'),
            
            # Zero-byte file (should be excluded)
            (5, 1, '/path/to/zero.txt', 'txt', 0, 'error', '2025-01-01 12:00:00', '2025-01-01 12:01:00', 'Zero bytes')
        ]
        
        self.cursor.executemany("""
        INSERT INTO files (file_id, job_id, file_path, file_type, file_size, status, process_start, process_end, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, test_files)
        
        self.conn.commit()
    
    def tearDown(self):
        """
        Clean up after tests
        """
        self.conn.close()
        os.unlink(self.db_path)
    
    def test_reset_error_files(self):
        """
        Test that reset_error_files works correctly
        """
        # Call the function
        inspect_db.reset_error_files(self.conn)
        
        # Check that the correct files were reset
        self.cursor.execute("SELECT file_id, status FROM files ORDER BY file_id")
        files = self.cursor.fetchall()
        
        # Files 1 and 4 should be reset to pending, the rest should still be error
        expected_statuses = {
            1: 'pending',  # Normal file - should be reset
            2: 'error',    # Small file - should remain error
            3: 'error',    # Temp file - should remain error
            4: 'pending',  # Normal file - should be reset
            5: 'error'     # Zero-byte file - should remain error
        }
        
        for file_id, status in files:
            self.assertEqual(status, expected_statuses[file_id], 
                            f"File {file_id} has status {status}, expected {expected_statuses[file_id]}")
        
        # Check that reset files have cleared process_start, process_end, and error_message
        self.cursor.execute("""
        SELECT process_start, process_end, error_message 
        FROM files 
        WHERE file_id = 1
        """)
        file_1 = self.cursor.fetchone()
        self.assertIsNone(file_1[0], "process_start should be NULL")
        self.assertIsNone(file_1[1], "process_end should be NULL")
        self.assertIsNone(file_1[2], "error_message should be NULL")
        
        # Check that the job's error_files count was updated correctly
        self.cursor.execute("SELECT error_files FROM jobs WHERE job_id = 1")
        error_count = self.cursor.fetchone()[0]
        self.assertEqual(error_count, 3, "error_files count should be 3 (5 original - 2 reset)")
    
    def test_reset_error_files_custom_size(self):
        """
        Test reset_error_files with a custom minimum file size
        """
        # Modify the min_file_size to allow smaller files
        original_min_size = inspect_db.min_file_size
        inspect_db.min_file_size = 10
        
        try:
            # Call the function with the modified min size
            inspect_db.reset_error_files(self.conn)
            
            # Check that the correct files were reset
            self.cursor.execute("SELECT file_id, status FROM files ORDER BY file_id")
            files = self.cursor.fetchall()
            
            # Files 1, 2, and 4 should be reset to pending, 3 and 5 should still be error
            expected_statuses = {
                1: 'pending',  # Normal file - should be reset
                2: 'pending',  # Small file (50 bytes) - should be reset with lower threshold
                3: 'error',    # Temp file - should remain error
                4: 'pending',  # Normal file - should be reset
                5: 'error'     # Zero-byte file - should remain error
            }
            
            for file_id, status in files:
                self.assertEqual(status, expected_statuses[file_id], 
                                f"File {file_id} has status {status}, expected {expected_statuses[file_id]}")
            
        finally:
            # Restore the original min_file_size
            inspect_db.min_file_size = original_min_size

if __name__ == '__main__':
    unittest.main() 