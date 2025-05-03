#!/usr/bin/env python3
"""
Database Inspector Utility for PII Analyzer
Shows job status, metadata, and file counts to help debug resumption issues
"""

import os
import sqlite3
import json
import argparse
from datetime import datetime, timedelta
from collections import defaultdict

def inspect_database(db_path, show_processing_speed=False, time_window=30):
    """Inspect database and print relevant information"""
    if not os.path.exists(db_path):
        print(f"Error: Database file {db_path} not found")
        return
        
    # Connect to the database with datetime detection
    conn = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row

    # Get jobs
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM jobs")
    jobs = cursor.fetchall()

    print("\n=== JOBS ===")
    for job in jobs:
        print(f"Job ID: {job['job_id']}")
        
        # Safely access columns that might not exist
        name = job['name'] if 'name' in job.keys() else 'Unknown'
        status = job['status'] if 'status' in job.keys() else 'Unknown'
        
        print(f"Name: {name}")
        print(f"Status: {status}")
        
        # Handle datetime objects safely
        for col in ['start_time', 'last_updated']:
            if col in job.keys():
                val = job[col]
                if isinstance(val, datetime):
                    print(f"{col}: {val.isoformat()}")
                else:
                    print(f"{col}: {val}")
        
        # Safely access other columns
        total_files = job['total_files'] if 'total_files' in job.keys() else 0
        processed_files = job['processed_files'] if 'processed_files' in job.keys() else 0
        error_files = job['error_files'] if 'error_files' in job.keys() else 0
        
        print(f"Total Files: {total_files}")
        print(f"Processed Files: {processed_files}")
        print(f"Error Files: {error_files}")
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
                print(f"sqlite3 {db_path} \"INSERT INTO job_metadata (job_id, key, value) VALUES ({job_id}, 'directory', '/CoWS/');\"")
    
    # Processing speed statistics
    if show_processing_speed:
        print("\n=== PROCESSING SPEED STATISTICS ===")
        for job in jobs:
            job_id = job['job_id']
            print(f"Job ID: {job_id}")
            
            # Get completed files with processing times
            cursor.execute("""
            SELECT f.file_id, f.file_path, f.file_size, f.process_start, f.process_end, 
                   r.processing_time, r.entity_count
            FROM files f
            LEFT JOIN results r ON f.file_id = r.file_id
            WHERE f.job_id = ? AND f.status = 'completed'
            ORDER BY f.process_end DESC
            """, (job_id,))
            
            completed_files = cursor.fetchall()
            
            if not completed_files:
                print("  No completed files found with timing information")
                continue
                
            # Statistics for the last window minutes
            now = datetime.now()
            window_start = now - timedelta(minutes=time_window)
            
            # Track files processed by time intervals
            files_by_minute = defaultdict(int)
            files_by_hour = defaultdict(int)
            processing_times = []
            file_sizes = []
            file_types = defaultdict(int)
            entity_counts = []
            
            # Recent files (within time window)
            recent_files = []
            recent_processing_times = []
            
            # Count of files with valid timestamps
            valid_timestamp_count = 0
            timestamp_format_issues = 0
            
            for file in completed_files:
                # Extract processing time
                proc_time = file['processing_time'] if file['processing_time'] else 0
                processing_times.append(proc_time)
                
                # Get file size
                file_size = file['file_size'] if file['file_size'] else 0
                file_sizes.append(file_size)
                
                # Get entity count if available
                if file['entity_count']:
                    entity_counts.append(file['entity_count'])
                
                # Get file extension for type stats
                file_path = file['file_path']
                file_ext = os.path.splitext(file_path)[1].lower()
                file_types[file_ext] += 1
                
                # Process time-based statistics
                process_end = file['process_end']
                
                # Try to handle different timestamp formats
                end_time = None
                if process_end:
                    if isinstance(process_end, datetime):
                        end_time = process_end
                    elif isinstance(process_end, str):
                        # Try to parse string timestamps
                        try:
                            # Try ISO format
                            end_time = datetime.fromisoformat(process_end)
                        except ValueError:
                            try:
                                # Try common SQLite format
                                end_time = datetime.strptime(process_end, '%Y-%m-%d %H:%M:%S.%f')
                            except ValueError:
                                try:
                                    # Try without microseconds
                                    end_time = datetime.strptime(process_end, '%Y-%m-%d %H:%M:%S')
                                except ValueError:
                                    timestamp_format_issues += 1
                
                if end_time:
                    valid_timestamp_count += 1
                    # Add to minute & hour counters
                    minute_key = end_time.strftime("%Y-%m-%d %H:%M")
                    hour_key = end_time.strftime("%Y-%m-%d %H")
                    
                    files_by_minute[minute_key] += 1
                    files_by_hour[hour_key] += 1
                    
                    # Check if within time window
                    if end_time >= window_start:
                        recent_files.append(file)
                        recent_processing_times.append(proc_time)
            
            # Calculate statistics
            if processing_times:
                avg_processing_time = sum(processing_times) / len(processing_times)
                max_processing_time = max(processing_times)
                min_processing_time = min(processing_times)
                
                print(f"  Overall Processing Statistics:")
                print(f"    Total files processed: {len(completed_files)}")
                print(f"    Average processing time: {avg_processing_time:.2f} seconds")
                print(f"    Min/Max processing time: {min_processing_time:.2f}s / {max_processing_time:.2f}s")
                
                if file_sizes:
                    avg_file_size = sum(file_sizes) / len(file_sizes)
                    print(f"    Average file size: {avg_file_size:.2f} bytes")
                
                if entity_counts:
                    avg_entities = sum(entity_counts) / len(entity_counts)
                    max_entities = max(entity_counts)
                    print(f"    Average entities per file: {avg_entities:.2f}")
                    print(f"    Maximum entities in a file: {max_entities}")
                
                # Print timestamp validation info
                print(f"    Files with valid timestamps: {valid_timestamp_count}/{len(completed_files)}")
                if timestamp_format_issues > 0:
                    print(f"    Files with timestamp format issues: {timestamp_format_issues}")
            
            # Recent processing statistics
            if recent_files:
                print(f"\n  Last {time_window} minutes:")
                print(f"    Files processed: {len(recent_files)}")
                files_per_hour = (len(recent_files) / time_window) * 60
                print(f"    Processing rate: {files_per_hour:.2f} files/hour")
                
                if recent_processing_times:
                    avg_recent_time = sum(recent_processing_times) / len(recent_processing_times)
                    print(f"    Average recent processing time: {avg_recent_time:.2f} seconds")
            
            # Show hourly processing rates for the last 24 hours
            print("\n  Hourly processing rates:")
            if files_by_hour:
                # Sort hours and get the last 24
                sorted_hours = sorted(files_by_hour.items())[-24:]
                for hour, count in sorted_hours:
                    print(f"    {hour}: {count} files ({count/1.0:.2f} files/hour)")
            else:
                print("    No hourly data available")
                
                # Let's manually check some process_end values to debug
                cursor.execute("""
                SELECT process_end FROM files 
                WHERE job_id = ? AND status = 'completed'
                LIMIT 5
                """, (job_id,))
                sample_timestamps = [row['process_end'] for row in cursor.fetchall()]
                print("\n  Sample process_end values for debugging:")
                for i, ts in enumerate(sample_timestamps):
                    print(f"    Sample {i+1}: {ts} (type: {type(ts).__name__})")
    
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Inspect PII Analyzer database")
    parser.add_argument('--db-path', type=str, default='pii_results.db',
                      help='Path to database file')
    parser.add_argument('--show-speed', action='store_true',
                      help='Show processing speed statistics')
    parser.add_argument('--time-window', type=int, default=30,
                      help='Time window in minutes for recent statistics (default: 30)')
    args = parser.parse_args()
    
    inspect_database(args.db_path, args.show_speed, args.time_window)

if __name__ == "__main__":
    main() 