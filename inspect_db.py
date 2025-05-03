#!/usr/bin/env python3
"""
Database Inspector Utility for PII Analyzer
Shows job status, metadata, and file counts to help debug resumption issues
"""

import os
import sqlite3
import json
import argparse
from datetime import datetime

def inspect_database(db_path):
    """Inspect database and print relevant information"""
    if not os.path.exists(db_path):
        print(f"Error: Database file {db_path} not found")
        return
        
    # Connect to the database
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get jobs
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs")
    jobs = cursor.fetchall()

    print("\n=== JOBS ===")
    for job in jobs:
        print(f"Job ID: {job['job_id']}")
        print(f"Name: {job.get('name', 'Unknown')}")
        print(f"Status: {job.get('status', 'Unknown')}")
        
        # Handle datetime objects safely
        for col in ['start_time', 'last_updated']:
            if col in job.keys():
                val = job[col]
                if isinstance(val, datetime):
                    print(f"{col}: {val.isoformat()}")
                else:
                    print(f"{col}: {val}")
        
        print(f"Total Files: {job.get('total_files', 0)}")
        print(f"Processed Files: {job.get('processed_files', 0)}")
        print(f"Error Files: {job.get('error_files', 0)}")
        print("---")

    # Get job metadata
    print("\n=== JOB METADATA ===")
    cursor.execute("SELECT * FROM job_metadata")
    metadata = cursor.fetchall()
    for m in metadata:
        print(f"Job ID: {m['job_id']}, Key: {m['key']}, Value: {m['value']}")

    # Get file counts
    print("\n=== FILE COUNTS BY STATUS ===")
    cursor.execute("SELECT job_id, status, COUNT(*) as count FROM files GROUP BY job_id, status")
    for row in cursor.fetchall():
        print(f"Job ID: {row['job_id']}, Status: {row['status']}, Count: {row['count']}")
        
    # Get file counts by type
    print("\n=== FILE COUNTS BY TYPE ===")
    cursor.execute("SELECT job_id, file_type, COUNT(*) as count FROM files GROUP BY job_id, file_type")
    for row in cursor.fetchall():
        print(f"Job ID: {row['job_id']}, Type: {row['file_type']}, Count: {row['count']}")
    
    # Get overall file count
    cursor.execute("SELECT COUNT(*) as count FROM files")
    total_files = cursor.fetchone()['count']
    print(f"\nTotal files in database: {total_files}")
    
    # Check for directory metadata to help with resumption
    print("\n=== DIRECTORY METADATA ===")
    cursor.execute("SELECT j.job_id, m.value FROM jobs j JOIN job_metadata m ON j.job_id = m.job_id WHERE m.key = 'directory'")
    directories = cursor.fetchall()
    if directories:
        for d in directories:
            print(f"Job ID: {d['job_id']}, Directory: {d['value']}")
    else:
        print("No directory metadata found - this may be why resumption is failing")
        
        # Suggest a fix
        print("\n=== SUGGESTED FIX ===")
        print("To add directory metadata for resumption, run:")
        job_ids = [job['job_id'] for job in jobs]
        if job_ids:
            for job_id in job_ids:
                print(f"INSERT INTO job_metadata (job_id, key, value) VALUES ({job_id}, 'directory', '/CoWS/');")
        
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Inspect PII Analyzer database")
    parser.add_argument('--db-path', type=str, default='pii_results.db',
                      help='Path to database file')
    args = parser.parse_args()
    
    inspect_database(args.db_path)

if __name__ == "__main__":
    main() 