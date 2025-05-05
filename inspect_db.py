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

def inspect_database(db_path, show_processing_speed=False, time_window=30, review_errors=False, reset_errors=False):
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
    
    # Reset error files if requested
    if reset_errors:
        reset_error_files(conn)
    
    # Analyze error files if requested
    if review_errors:
        analyze_error_files(conn)
    
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

def reset_error_files(conn):
    """
    Reset files with error status back to pending so they can be reprocessed
    
    Args:
        conn: SQLite database connection
    """
    print("\n=== RESETTING ERROR FILES ===")
    
    cursor = conn.cursor()
    
    # Get count of error files
    cursor.execute("SELECT COUNT(*) as count FROM files WHERE status = 'error'")
    error_count = cursor.fetchone()['count']
    print(f"Total error files to reset: {error_count}")
    
    if error_count == 0:
        print("No error files to reset")
        return
    
    # Options to exclude certain files
    exclude_temp_files = True  # Skip resetting temporary Office files (starting with ~$)
    exclude_small_files = True  # Skip resetting small/empty files
    min_file_size = 100  # Minimum file size in bytes to consider for reset (files smaller than this will be skipped)
    
    # Optional: Allow filtering by job ID
    # job_id = None  # Set to a specific job ID to only reset errors for that job
    
    # Build the query conditions
    conditions = ["status = 'error'"]
    excluded_counts = {}
    
    # Exclude temporary files if requested
    if exclude_temp_files:
        # Check how many temp files exist
        cursor.execute("""
        SELECT COUNT(*) as count FROM files 
        WHERE status = 'error' AND file_path LIKE '%/~$%'
        """)
        temp_count = cursor.fetchone()['count']
        excluded_counts["temp_files"] = temp_count
        conditions.append("file_path NOT LIKE '%/~$%'")
    
    # Exclude small files if requested
    if exclude_small_files:
        # Check how many small files exist
        cursor.execute("""
        SELECT COUNT(*) as count FROM files 
        WHERE status = 'error' AND (file_size IS NULL OR file_size <= ?)
        """, (min_file_size,))
        small_count = cursor.fetchone()['count']
        excluded_counts["small_files"] = small_count
        conditions.append(f"(file_size > {min_file_size})")
    
    # Combine all conditions
    where_clause = " AND ".join(conditions)
    
    # Print exclusion info
    for category, count in excluded_counts.items():
        print(f"Excluding {count} {category.replace('_', ' ')}")
    
    # Reset filtered error files
    reset_query = f"""
    UPDATE files
    SET 
        status = 'pending',
        process_start = NULL,
        process_end = NULL,
        error_message = NULL
    WHERE {where_clause}
    """
    
    cursor.execute(reset_query)
    reset_count = cursor.rowcount
    
    # Commit changes
    conn.commit()
    
    # Update job stats in jobs table
    cursor.execute("""
    UPDATE jobs
    SET 
        error_files = error_files - ?,
        processed_files = processed_files - ?
    WHERE job_id IN (SELECT DISTINCT job_id FROM files WHERE status = 'pending')
    """, (reset_count, reset_count))
    
    conn.commit()
    
    total_excluded = sum(excluded_counts.values())
    print(f"Successfully reset {reset_count} files from 'error' to 'pending' status")
    print(f"Excluded {total_excluded} files ({', '.join([f'{count} {cat.replace("_", " ")}' for cat, count in excluded_counts.items()])})")
    print(f"Files will be reprocessed on the next run of process_files.py")

def analyze_error_files(conn):
    """
    Analyze files with error status to categorize common error patterns
    
    Args:
        conn: SQLite database connection
    """
    print("\n=== ERROR FILE ANALYSIS ===")
    
    cursor = conn.cursor()
    
    # Get count of error files
    cursor.execute("SELECT COUNT(*) as count FROM files WHERE status = 'error'")
    error_count = cursor.fetchone()['count']
    print(f"Total error files: {error_count}")
    
    if error_count == 0:
        print("No error files to analyze")
        return
    
    # Query error files with their error messages
    cursor.execute("""
    SELECT f.file_id, f.file_path, f.file_size, f.error_message, f.job_id
    FROM files f
    WHERE f.status = 'error'
    ORDER BY f.file_path
    """)
    
    error_files = cursor.fetchall()
    
    # Error categories to track
    categories = {
        'temp_files': 0,          # Temporary files (starting with ~$)
        'empty_files': 0,         # Empty or zero-byte files
        'missing_files': 0,       # Files not found at path
        'permission_errors': 0,   # Permission denied errors
        'format_errors': 0,       # File format not recognized or corrupt
        'tika_errors': 0,         # Tika server errors
        'ocr_errors': 0,          # OCR processing errors
        'timeout_errors': 0,      # Timeouts
        'extraction_errors': 0,   # Text extraction errors
        'other': 0                # Uncategorized errors
    }
    
    # File extension statistics for error files
    ext_stats = defaultdict(int)
    
    # Error message patterns for categorization
    temp_file_pattern = "~$"
    empty_file_patterns = ["empty", "no output file", "zero", "0 bytes"]
    missing_file_patterns = ["not found", "no such file", "does not exist"]
    permission_patterns = ["permission denied", "access denied"]
    format_patterns = ["unsupported", "invalid format", "format error", "corrupt"]
    tika_patterns = ["tika", "connection refused", "service unavailable"]
    ocr_patterns = ["ocr", "tesseract", "recognition failed"]
    timeout_patterns = ["timeout", "timed out", "time limit"]
    extraction_patterns = ["extraction failed", "could not extract", "no text"]
    
    # Sample error messages for each category
    error_samples = {k: [] for k in categories.keys()}
    max_samples = 5  # number of sample error messages to store per category
    
    for file in error_files:
        file_path = file['file_path']
        file_size = file['file_size'] or 0
        error_msg = file['error_message'] or ""
        
        # Get file extension for stats
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        ext_stats[ext] += 1
        
        # Check for temporary Office files
        if os.path.basename(file_path).startswith("~$"):
            categories['temp_files'] += 1
            if len(error_samples['temp_files']) < max_samples:
                error_samples['temp_files'].append((file_path, error_msg))
            continue
        
        # Categorize based on error message
        error_lower = error_msg.lower()
        
        if file_size == 0 or any(p in error_lower for p in empty_file_patterns):
            categories['empty_files'] += 1
            if len(error_samples['empty_files']) < max_samples:
                error_samples['empty_files'].append((file_path, error_msg))
        elif any(p in error_lower for p in missing_file_patterns):
            categories['missing_files'] += 1
            if len(error_samples['missing_files']) < max_samples:
                error_samples['missing_files'].append((file_path, error_msg))
        elif any(p in error_lower for p in permission_patterns):
            categories['permission_errors'] += 1
            if len(error_samples['permission_errors']) < max_samples:
                error_samples['permission_errors'].append((file_path, error_msg))
        elif any(p in error_lower for p in format_patterns):
            categories['format_errors'] += 1
            if len(error_samples['format_errors']) < max_samples:
                error_samples['format_errors'].append((file_path, error_msg))
        elif any(p in error_lower for p in tika_patterns):
            categories['tika_errors'] += 1
            if len(error_samples['tika_errors']) < max_samples:
                error_samples['tika_errors'].append((file_path, error_msg))
        elif any(p in error_lower for p in ocr_patterns):
            categories['ocr_errors'] += 1
            if len(error_samples['ocr_errors']) < max_samples:
                error_samples['ocr_errors'].append((file_path, error_msg))
        elif any(p in error_lower for p in timeout_patterns):
            categories['timeout_errors'] += 1
            if len(error_samples['timeout_errors']) < max_samples:
                error_samples['timeout_errors'].append((file_path, error_msg))
        elif any(p in error_lower for p in extraction_patterns):
            categories['extraction_errors'] += 1
            if len(error_samples['extraction_errors']) < max_samples:
                error_samples['extraction_errors'].append((file_path, error_msg))
        else:
            categories['other'] += 1
            if len(error_samples['other']) < max_samples:
                error_samples['other'].append((file_path, error_msg))
    
    # Print error category statistics
    print("\nError Categories:")
    for category, count in categories.items():
        if count > 0:
            percentage = (count / error_count) * 100
            print(f"  {category.replace('_', ' ').title()}: {count} ({percentage:.1f}%)")
    
    # Print file extension statistics for errors
    print("\nFile Extensions with Errors:")
    total_ext = sum(ext_stats.values())
    for ext, count in sorted(ext_stats.items(), key=lambda x: x[1], reverse=True)[:20]:  # Top 20
        if count > 0:
            percentage = (count / total_ext) * 100
            print(f"  {ext or '(no extension)'}: {count} ({percentage:.1f}%)")
    
    # Print sample error messages for each category
    print("\nSample Error Messages by Category:")
    for category, samples in error_samples.items():
        if samples:
            print(f"\n  {category.replace('_', ' ').title()}:")
            for i, (file_path, error_msg) in enumerate(samples[:max_samples]):
                # Truncate long paths and messages
                trunc_path = (file_path[:60] + '...') if len(file_path) > 60 else file_path
                trunc_msg = (error_msg[:100] + '...') if len(error_msg) > 100 else error_msg
                print(f"    Sample {i+1}: {trunc_path}")
                print(f"      Error: {trunc_msg}")

def main():
    parser = argparse.ArgumentParser(description="Inspect PII Analyzer database")
    parser.add_argument('--db-path', type=str, default='pii_results.db',
                      help='Path to database file')
    parser.add_argument('--show-speed', action='store_true',
                      help='Show processing speed statistics')
    parser.add_argument('--time-window', type=int, default=30,
                      help='Time window in minutes for recent statistics (default: 30)')
    parser.add_argument('--review-errors', action='store_true',
                      help='Analyze error files to identify patterns and categorize errors')
    parser.add_argument('--reset-errors', action='store_true',
                      help='Reset error files back to pending status so they can be reprocessed')
    parser.add_argument('--min-size', type=int, default=100,
                      help='Minimum file size in bytes to reset when using --reset-errors (default: 100)')
    args = parser.parse_args()
    
    # If min-size is provided, modify the global variable
    if args.reset_errors and hasattr(args, 'min_size'):
        # This is a bit of a hack, but it works for simple script
        global min_file_size
        min_file_size = args.min_size
    
    inspect_database(args.db_path, args.show_speed, args.time_window, args.review_errors, args.reset_errors)

if __name__ == "__main__":
    main() 