#!/usr/bin/env python3
"""
PII Database Management Utilities

This script provides command-line tools for working with PII analysis databases,
including exporting to JSON, database maintenance, and reporting.
"""

import os
import sys
import argparse
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))))

from src.database.db_utils import get_database
from src.database.db_reporting import load_pii_data_from_db, get_file_type_statistics, get_entity_statistics

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pii_db_tools')

def export_to_json(db_path: str, output_file: str, job_id: Optional[int] = None, 
                  threshold: float = 0.0, pretty: bool = False) -> bool:
    """
    Export database contents to JSON format.
    
    Args:
        db_path: Path to database file
        output_file: Path to save the JSON output
        job_id: Specific job ID to export (most recent if None)
        threshold: Confidence threshold for entities
        pretty: Whether to format the JSON for human readability
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Get database connection
        db = get_database(db_path)
        
        # Get job ID if not provided
        if job_id is None:
            jobs = db.get_all_jobs()
            if not jobs:
                logger.error(f"No jobs found in database: {db_path}")
                return False
            job_id = jobs[0]['job_id']
        
        # Verify job exists
        job = db.get_job(job_id)
        if not job:
            logger.error(f"Job ID {job_id} not found in database: {db_path}")
            return False
        
        # Load the data
        data = load_pii_data_from_db(db_path, job_id, threshold)
        
        # Write to file
        with open(output_file, 'w') as f:
            if pretty:
                json.dump(data, f, indent=2)
            else:
                json.dump(data, f)
        
        logger.info(f"Exported job {job_id} to {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Error exporting database to JSON: {e}")
        return False

def list_jobs(db_path: str, detailed: bool = False) -> bool:
    """
    List all jobs in the database.
    
    Args:
        db_path: Path to database file
        detailed: Whether to show detailed information
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Get database connection
        db = get_database(db_path)
        
        # Get all jobs
        jobs = db.get_all_jobs()
        if not jobs:
            logger.info(f"No jobs found in database: {db_path}")
            return True
        
        # Print job information
        print(f"\nJobs in database: {db_path}")
        print(f"{'ID':<4} {'Status':<12} {'Files':<8} {'Start Time':<26} {'Name'}")
        print("-" * 80)
        
        for job in jobs:
            job_id = job['job_id']
            status = job.get('status', 'unknown')
            name = job.get('name', 'Unnamed')
            start_time = job.get('start_time', 'unknown')
            processed = job.get('processed_files', 0)
            total = job.get('total_files', 0)
            
            print(f"{job_id:<4} {status:<12} {processed}/{total:<6} {start_time:<26} {name}")
            
            # Show detailed information if requested
            if detailed:
                # Get file statistics for this job
                status_counts = {}
                cursor = db.conn.cursor()
                cursor.execute("""
                SELECT status, COUNT(*) as count
                FROM files
                WHERE job_id = ?
                GROUP BY status
                """, (job_id,))
                
                for row in cursor.fetchall():
                    status_counts[row['status']] = row['count']
                
                # Show file status counts
                print(f"  File status:")
                for status, count in status_counts.items():
                    print(f"    {status}: {count}")
                
                # Show metadata if available
                if job.get('metadata'):
                    print(f"  Metadata:")
                    for key, value in job['metadata'].items():
                        print(f"    {key}: {value}")
                
                print()
        
        return True
        
    except Exception as e:
        logger.error(f"Error listing jobs: {e}")
        return False

def show_job_status(db_path: str, job_id: Optional[int] = None) -> bool:
    """
    Show detailed status for a specific job.
    
    Args:
        db_path: Path to database file
        job_id: Specific job ID to show (most recent if None)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Get database connection
        db = get_database(db_path)
        
        # Get job ID if not provided
        if job_id is None:
            jobs = db.get_all_jobs()
            if not jobs:
                logger.error(f"No jobs found in database: {db_path}")
                return False
            job_id = jobs[0]['job_id']
        
        # Verify job exists
        job = db.get_job(job_id)
        if not job:
            logger.error(f"Job ID {job_id} not found in database: {db_path}")
            return False
        
        # Print job information
        print(f"\nJob {job_id}: {job.get('name', 'Unnamed')}")
        print(f"Status: {job.get('status', 'unknown')}")
        print(f"Started: {job.get('start_time', 'unknown')}")
        print(f"Last updated: {job.get('last_updated', 'unknown')}")
        
        # Get file statistics
        cursor = db.conn.cursor()
        cursor.execute("""
        SELECT status, COUNT(*) as count
        FROM files
        WHERE job_id = ?
        GROUP BY status
        """, (job_id,))
        
        status_counts = {}
        for row in cursor.fetchall():
            status_counts[row['status']] = row['count']
        
        # Show file status counts
        print(f"\nFile Status:")
        for status, count in status_counts.items():
            print(f"  {status}: {count}")
        
        # Get file type statistics
        cursor.execute("""
        SELECT file_type, COUNT(*) as count
        FROM files
        WHERE job_id = ?
        GROUP BY file_type
        ORDER BY count DESC
        """, (job_id,))
        
        file_types = {}
        for row in cursor.fetchall():
            file_types[row['file_type']] = row['count']
        
        # Show file type counts
        print(f"\nFile Types:")
        for file_type, count in file_types.items():
            print(f"  {file_type}: {count}")
        
        # Get entity statistics if entities exist
        entity_counts = get_entity_statistics(db_path, job_id)
        
        if entity_counts:
            print(f"\nTop Entity Types:")
            for entity_type, count in sorted(entity_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
                print(f"  {entity_type}: {count}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error showing job status: {e}")
        return False

def clean_stalled_files(db_path: str, job_id: Optional[int] = None) -> bool:
    """
    Reset any stalled files (stuck in 'processing' state) to 'pending'.
    
    Args:
        db_path: Path to database file
        job_id: Specific job ID to clean (most recent if None)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Get database connection
        db = get_database(db_path)
        
        # Get job ID if not provided
        if job_id is None:
            jobs = db.get_all_jobs()
            if not jobs:
                logger.error(f"No jobs found in database: {db_path}")
                return False
            job_id = jobs[0]['job_id']
        
        # Get stalled files
        cursor = db.conn.cursor()
        cursor.execute("""
        SELECT file_id, file_path
        FROM files
        WHERE job_id = ? AND status = 'processing'
        """, (job_id,))
        
        stalled_files = list(cursor.fetchall())
        
        if not stalled_files:
            logger.info(f"No stalled files found for job {job_id}")
            return True
        
        # Reset stalled files
        with db.conn:
            for file in stalled_files:
                cursor.execute("""
                UPDATE files
                SET status = 'pending', error_message = NULL, process_start = NULL
                WHERE file_id = ?
                """, (file['file_id'],))
        
        logger.info(f"Reset {len(stalled_files)} stalled files for job {job_id}")
        for file in stalled_files:
            logger.info(f"  - {file['file_path']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error cleaning stalled files: {e}")
        return False

def main():
    """Main function for the CLI."""
    parser = argparse.ArgumentParser(
        description="PII Database Management Utilities",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all jobs in the database
  python src/database/db_tools.py --db-path results.db --list-jobs
  
  # Show detailed status for a job
  python src/database/db_tools.py --db-path results.db --status
  
  # Export a job to JSON
  python src/database/db_tools.py --db-path results.db --export results.json
  
  # Clean up stalled files
  python src/database/db_tools.py --db-path results.db --cleanup
"""
    )
    
    # Database specification
    parser.add_argument("--db-path", "-d", type=str, required=True,
                       help="Path to the database file")
    
    # Actions (mutually exclusive)
    action_group = parser.add_mutually_exclusive_group(required=True)
    action_group.add_argument("--list-jobs", "-l", action="store_true",
                             help="List all jobs in the database")
    action_group.add_argument("--status", "-s", action="store_true",
                             help="Show detailed status for a job")
    action_group.add_argument("--export", "-e", type=str, metavar="FILE",
                             help="Export job to JSON file")
    action_group.add_argument("--cleanup", "-c", action="store_true",
                             help="Clean up stalled files")
    
    # Additional options
    parser.add_argument("--job-id", "-j", type=int,
                       help="Specific job ID to work with (default: most recent)")
    parser.add_argument("--threshold", "-t", type=float, default=0.0,
                       help="Confidence threshold for entities when exporting")
    parser.add_argument("--pretty", "-p", action="store_true",
                       help="Format JSON output for readability")
    parser.add_argument("--detailed", action="store_true",
                       help="Show detailed information when listing jobs")
    
    args = parser.parse_args()
    
    # Execute the requested action
    if args.list_jobs:
        list_jobs(args.db_path, args.detailed)
    elif args.status:
        show_job_status(args.db_path, args.job_id)
    elif args.export:
        export_to_json(args.db_path, args.export, args.job_id, args.threshold, args.pretty)
    elif args.cleanup:
        clean_stalled_files(args.db_path, args.job_id)

if __name__ == "__main__":
    main() 