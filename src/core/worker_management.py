#!/usr/bin/env python3
"""
Worker Management Module for PII Analyzer
Provides functions to manage parallel processing of files
using the database for tracking and coordination
"""

import concurrent.futures
import time
import logging
import multiprocessing
import queue
import threading
import os
from typing import Callable, List, Dict, Any, Optional, Tuple

from src.database.db_utils import get_database, PIIDatabase

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('worker_management')

# Thread-local storage for database connections
thread_local = threading.local()

def get_thread_db(db_path: str) -> PIIDatabase:
    """
    Get a thread-local database connection.
    SQLite connections can only be used in the thread they were created in.
    
    Args:
        db_path: Path to the database file
        
    Returns:
        Thread-specific database connection
    """
    if not hasattr(thread_local, "db"):
        thread_local.db = get_database(db_path)
    return thread_local.db

class SafeQueue:
    """Thread-safe queue to track results and communicate between threads."""
    
    def __init__(self):
        self.queue = queue.Queue()
        self.processed = 0
        self.errors = 0
        self.lock = threading.Lock()
    
    def add_processed(self):
        """Increment processed count."""
        with self.lock:
            self.processed += 1
    
    def add_error(self):
        """Increment error count."""
        with self.lock:
            self.errors += 1
    
    def get_stats(self) -> Tuple[int, int]:
        """Get current statistics."""
        with self.lock:
            return self.processed, self.errors

def process_files_parallel(
    db: PIIDatabase,
    job_id: int,
    processing_func: Callable[[str, Dict[str, Any]], Dict[str, Any]],
    max_workers: Optional[int] = None,
    batch_size: int = 10,
    max_files: Optional[int] = None,
    settings: Optional[Dict[str, Any]] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None
) -> Dict[str, Any]:
    """
    Process files in parallel using database to track progress.
    
    Args:
        db: Database connection
        job_id: Job ID to process
        processing_func: Function that processes a single file
        max_workers: Maximum number of worker processes (None for auto)
        batch_size: Number of files to fetch at once
        max_files: Maximum number of files to process (None for all)
        settings: Additional settings to pass to processing function
        progress_callback: Optional callback function to report progress
        
    Returns:
        Dictionary with processing statistics
    """
    if not max_workers:
        # Auto-determine based on CPU count
        max_workers = max(1, multiprocessing.cpu_count() - 1)
    
    if settings is None:
        settings = {}
    
    # Get the database path for worker processes
    db_path = db.db_path
    
    start_time = time.time()
    stats_queue = SafeQueue()
    files_remaining = True
    processed_count = 0
    
    logger.info(f"Starting parallel processing with {max_workers} worker processes")
    
    # Create a process pool with fixed number of workers
    # Use ProcessPoolExecutor instead of ThreadPoolExecutor for true parallelism
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        while files_remaining and (max_files is None or processed_count < max_files):
            # Get batch of pending files
            limit = min(batch_size, max_files - processed_count if max_files else batch_size)
            pending_files = db.get_pending_files(job_id, limit=limit)
            
            if not pending_files:
                files_remaining = False
                break
            
            # Log batch information
            logger.info(f"Processing batch of {len(pending_files)} files")
            
            # Submit jobs to process pool
            futures = []
            for file_id, file_path in pending_files:
                # Mark file as processing
                if db.mark_file_processing(file_id):
                    futures.append(
                        executor.submit(
                            process_single_file_process_safe,
                            file_id,
                            file_path,
                            db_path,
                            job_id,
                            settings
                        )
                    )
            
            # Wait for the batch to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    
                    if result.get('success', False):
                        # Update the database with results
                        db.store_file_results(
                            result['file_id'], 
                            result['processing_time'], 
                            result.get('entities', []), 
                            result.get('metadata', {})
                        )
                        db.mark_file_completed(result['file_id'], job_id)
                        stats_queue.add_processed()
                    else:
                        # Mark as error
                        db.mark_file_error(result['file_id'], job_id, result.get('error_message', 'Unknown error'))
                        stats_queue.add_error()
                    
                    # Call progress callback if provided
                    if progress_callback:
                        progress_callback({
                            'type': 'file_completed' if result.get('success', False) else 'file_error',
                            'file_id': result.get('file_id'),
                            'file_path': result.get('file_path'),
                            'entities': result.get('entities', []),
                            'error': result.get('error_message') if not result.get('success', False) else None
                        })
                    
                    # Check progress
                    processed, errors = stats_queue.get_stats()
                    if processed % 10 == 0 and processed > 0:
                        elapsed = time.time() - start_time
                        rate = processed / elapsed if elapsed > 0 else 0
                        logger.info(f"Processed {processed} files in {elapsed:.2f}s ({rate:.2f} files/sec)")
                        
                except Exception as e:
                    logger.error(f"Worker process error: {e}")
                    
                    # Call progress callback for errors if provided
                    if progress_callback:
                        progress_callback({
                            'type': 'file_error',
                            'error': str(e)
                        })
            
            # Update processed count
            processed_count, error_count = stats_queue.get_stats()
    
    # Update job status
    elapsed = time.time() - start_time
    processed_count, error_count = stats_queue.get_stats()
    rate = processed_count / elapsed if elapsed > 0 else 0
    
    if not files_remaining:
        db.update_job_status(job_id, 'completed')
        logger.info(f"Job completed: processed {processed_count} files in {elapsed:.2f}s ({rate:.2f} files/sec)")
    else:
        db.update_job_status(job_id, 'interrupted')
        logger.info(f"Job interrupted: processed {processed_count} files in {elapsed:.2f}s ({rate:.2f} files/sec)")
    
    return {
        'job_id': job_id,
        'total_processed': processed_count,
        'total_errors': error_count,
        'elapsed_time': elapsed,
        'files_per_second': rate
    }

def process_single_file_process_safe(
    file_id: int,
    file_path: str,
    db_path: str,
    job_id: int,
    settings: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process a single file in a separate process with its own database connection.
    
    Args:
        file_id: ID of the file to process
        file_path: Path to the file
        db_path: Path to the database
        job_id: Job ID this file belongs to
        settings: Additional settings to pass to processing function
        
    Returns:
        Dictionary with processing results
    """
    from src.core.pii_analyzer_adapter import analyze_file
    
    # Set process name for better monitoring
    try:
        import setproctitle
        setproctitle.setproctitle(f"pii-worker-{os.path.basename(file_path)}")
    except ImportError:
        pass
    
    start_time = time.time()
    
    try:
        # Process the file
        result = analyze_file(file_path, settings)
        
        # Add file ID for tracking
        result['file_id'] = file_id
        
        return result
    except Exception as e:
        # Return error information
        return {
            'file_id': file_id,
            'file_path': file_path,
            'success': False,
            'error_message': f"Process error: {str(e)}",
            'entities': [],
            'processing_time': time.time() - start_time
        }

def process_single_file_thread_safe(
    file_id: int,
    file_path: str,
    db_path: str,
    job_id: int,
    processing_func: Callable[[str, Dict[str, Any]], Dict[str, Any]],
    settings: Dict[str, Any],
    stats_queue: SafeQueue
) -> Dict[str, Any]:
    """
    Process a single file with thread-safe database handling.
    
    Args:
        file_id: ID of the file to process
        file_path: Path to the file
        db_path: Path to the database
        job_id: Job ID this file belongs to
        processing_func: Function to perform the actual processing
        settings: Additional settings to pass to processing function
        stats_queue: Queue to track statistics
        
    Returns:
        Dictionary with processing results
    """
    # Get thread-local database connection
    db = get_thread_db(db_path)
    
    start_time = time.time()
    
    try:
        # Process the file
        result = processing_func(file_path, settings)
        
        # Extract entities
        entities = result.get('entities', [])
        
        # Extract metadata
        metadata = result.get('metadata', {})
        
        # Store results in database
        processing_time = time.time() - start_time
        db.store_file_results(file_id, processing_time, entities, metadata)
        
        # Mark file as completed
        db.mark_file_completed(file_id, job_id)
        
        # Update statistics
        stats_queue.add_processed()
        
        # Add file info to result for progress reporting
        result['file_id'] = file_id
        result['processing_time'] = processing_time
        
        return result
    except Exception as e:
        # Mark as error and update statistics
        db.mark_file_error(file_id, job_id, str(e))
        stats_queue.add_error()
        
        # Log and re-raise the exception
        logger.error(f"Error processing file {file_path}: {e}")
        raise

def process_single_file(
    file_id: int,
    file_path: str,
    db: PIIDatabase,
    job_id: int,
    processing_func: Callable[[str, Dict[str, Any]], Dict[str, Any]],
    settings: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process a single file and store results in database.
    This version should only be used for single-threaded processing.
    
    Args:
        file_id: ID of the file to process
        file_path: Path to the file
        db: Database connection
        job_id: Job ID this file belongs to
        processing_func: Function to perform the actual processing
        settings: Additional settings to pass to processing function
        
    Returns:
        Dictionary with processing results
    """
    start_time = time.time()
    
    try:
        # Process the file
        result = processing_func(file_path, settings)
        
        # Extract entities
        entities = result.get('entities', [])
        
        # Store results in database
        processing_time = time.time() - start_time
        db.store_file_results(file_id, processing_time, entities)
        
        # Mark file as completed
        db.mark_file_completed(file_id, job_id)
        
        return result
    except Exception as e:
        # Log and re-raise the exception
        logger.error(f"Error processing file {file_path}: {e}")
        raise

def estimate_completion_time(
    db: PIIDatabase,
    job_id: int
) -> Dict[str, Any]:
    """
    Estimate remaining time for job completion based on
    current processing rate.
    
    Args:
        db: Database connection
        job_id: Job ID to estimate
        
    Returns:
        Dictionary with estimation information
    """
    # Get job information
    job = db.get_job(job_id)
    if not job:
        return {
            'status': 'error',
            'message': f"Job {job_id} not found"
        }
    
    # Get file counts
    total_files = job.get('total_files', 0)
    processed_files = job.get('processed_files', 0)
    error_files = job.get('error_files', 0)
    
    # Calculate remaining files
    remaining_files = total_files - processed_files - error_files
    
    # Check if job is still running
    if job.get('status') != 'running':
        return {
            'status': job.get('status'),
            'total_files': total_files,
            'processed_files': processed_files,
            'error_files': error_files,
            'remaining_files': remaining_files,
            'message': f"Job is not running, status is {job.get('status')}"
        }
    
    # Calculate processing rate
    start_time = job.get('start_time')
    last_updated = job.get('last_updated')
    
    if not start_time or not last_updated or processed_files == 0:
        return {
            'status': 'running',
            'total_files': total_files,
            'processed_files': processed_files,
            'error_files': error_files,
            'remaining_files': remaining_files,
            'message': "Insufficient data to estimate completion time"
        }
    
    # Convert timestamps to time objects
    if isinstance(start_time, str):
        from datetime import datetime
        start_time = datetime.fromisoformat(start_time)
    if isinstance(last_updated, str):
        from datetime import datetime
        last_updated = datetime.fromisoformat(last_updated)
    
    # Calculate elapsed time in seconds
    elapsed_seconds = (last_updated - start_time).total_seconds()
    
    # Calculate processing rate (files per second)
    if elapsed_seconds > 0:
        rate = processed_files / elapsed_seconds
    else:
        rate = 0
    
    # Estimate remaining time
    if rate > 0:
        estimated_seconds = remaining_files / rate
        estimated_minutes = estimated_seconds / 60
        estimated_hours = estimated_minutes / 60
    else:
        estimated_seconds = float('inf')
        estimated_minutes = float('inf')
        estimated_hours = float('inf')
    
    # Format estimated time
    if estimated_hours > 24:
        estimated_time = f"{estimated_hours/24:.1f} days"
    elif estimated_hours > 1:
        estimated_time = f"{estimated_hours:.1f} hours"
    elif estimated_minutes > 1:
        estimated_time = f"{estimated_minutes:.1f} minutes"
    else:
        estimated_time = f"{estimated_seconds:.1f} seconds"
    
    return {
        'status': 'running',
        'total_files': total_files,
        'processed_files': processed_files,
        'error_files': error_files,
        'remaining_files': remaining_files,
        'elapsed_time': elapsed_seconds,
        'processing_rate': rate,
        'estimated_remaining_seconds': estimated_seconds,
        'estimated_remaining_time': estimated_time,
        'percent_complete': (processed_files + error_files) / total_files * 100 if total_files > 0 else 0
    }

def interrupt_processing(
    db: PIIDatabase,
    job_id: int
) -> bool:
    """
    Mark a job as interrupted and reset processing files to pending.
    
    Args:
        db: Database connection
        job_id: Job ID to interrupt
        
    Returns:
        True if job was interrupted, False otherwise
    """
    # Get job information
    job = db.get_job(job_id)
    if not job or job.get('status') != 'running':
        return False
    
    # Reset processing files to pending
    from src.core.file_discovery import reset_stalled_files
    reset_count = reset_stalled_files(db, job_id)
    
    # Mark job as interrupted
    success = db.update_job_status(job_id, 'interrupted')
    
    logger.info(f"Job {job_id} interrupted, reset {reset_count} files to pending")
    
    return success 