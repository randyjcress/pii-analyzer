#!/usr/bin/env python3
"""
Test script for error analysis JSON output
"""

import os
import sys
import json
import sqlite3
import tempfile
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Import our modules
import inspect_db

def create_test_db():
    """Create a test database with sample error files"""
    # Create a temporary database file
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    db_path = temp_db.name
    temp_db.close()
    
    # Create a connection to the database
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Create the necessary tables
    cursor.executescript("""
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
    cursor.execute("""
    INSERT INTO jobs (job_id, name, status, total_files, processed_files, error_files)
    VALUES (1, 'Test Job', 'running', 10, 5, 5)
    """)
    
    # Insert sample files with different error conditions
    test_files = [
        # Temp file
        (1, 1, '/path/to/~$temp.docx', 'docx', 1000, 'error', '2025-01-01 12:00:00', 
         '2025-01-01 12:01:00', 'Temporary file error'),
        
        # Empty file
        (2, 1, '/path/to/empty.pdf', 'pdf', 0, 'error', '2025-01-01 12:00:00', 
         '2025-01-01 12:01:00', 'Empty file or zero bytes'),
        
        # Missing file
        (3, 1, '/path/to/missing.txt', 'txt', 1000, 'error', '2025-01-01 12:00:00', 
         '2025-01-01 12:01:00', 'File not found at path'),
        
        # Permission error
        (4, 1, '/path/to/noaccess.xlsx', 'xlsx', 2000, 'error', '2025-01-01 12:00:00', 
         '2025-01-01 12:01:00', 'Permission denied'),
        
        # Format error
        (5, 1, '/path/to/corrupt.docx', 'docx', 3000, 'error', '2025-01-01 12:00:00', 
         '2025-01-01 12:01:00', 'Invalid format or corrupt file')
    ]
    
    cursor.executemany("""
    INSERT INTO files (file_id, job_id, file_path, file_type, file_size, status, process_start, process_end, error_message)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, test_files)
    
    conn.commit()
    
    return db_path, conn

def main():
    """Test both text and JSON output formats"""
    try:
        # Create test database
        print("Creating test database...")
        db_path, conn = create_test_db()
        
        print(f"Test database created at: {db_path}")
        
        # Test text output
        print("\n=== TESTING TEXT OUTPUT ===")
        text_result = inspect_db.analyze_error_files(conn, output_format='text')
        print(f"Text output returned: {text_result}")
        
        # Test JSON output
        print("\n=== TESTING JSON OUTPUT ===")
        json_result = inspect_db.analyze_error_files(conn, output_format='json')
        print(f"JSON output structure: {list(json_result.keys())}")
        print(f"Total errors: {json_result['total_errors']}")
        print(f"Categories: {len(json_result['categories'])}")
        print(f"Extensions: {len(json_result['extensions'])}")
        print(f"Sample categories: {len(json_result['samples'])}")
        
        # Print the JSON nicely formatted
        print("\nJSON output contents:")
        print(json.dumps(json_result, indent=2))
        
        # Clean up
        conn.close()
        os.unlink(db_path)
        print("\nClean up complete")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 