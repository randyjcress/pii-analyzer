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
import psutil
import setproctitle
import math
from typing import Callable, List, Dict, Any, Optional, Tuple

from src.database.db_utils import get_database, PIIDatabase

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('worker_management')

# Thread-local storage for database connections
thread_local = threading.local()

# Global OCR semaphore to limit OCR processes
# Will be initialized during process_files_parallel
OCR_SEMAPHORE = None

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

def calculate_optimal_workers() -> int:
    """
    Calculate the optimal number of worker processes based on system resources.
    Considers CPU cores, memory, and whether running on an NFS/networked storage.
    
    Returns:
        Optimal number of worker processes
    """
    try:
        # Get CPU cores and memory
        cpu_count = psutil.cpu_count(logical=True)
        memory_gb = psutil.virtual_memory().total / (1024 * 1024 * 1024)
        
        # Check if we're accessing network storage
        # This is a simplified check - might need adjustment
        is_network_storage = False
        for part in psutil.disk_partitions(all=True):
            if 'nfs' in part.fstype.lower() or 'cifs' in part.fstype.lower():
                is_network_storage = True
                break
        
        # For 96-core high-memory systems, optimize for maximum parallelism
        if cpu_count >= 96:
            # Use 90% of available cores on these high-end systems
            base_workers = int(cpu_count * 0.9)
            
            # Calculate workers based on memory (assume ~500MB per worker)
            max_by_memory = int((memory_gb * 0.9) / 0.5)
            
            # Ensure we use at least 350 workers on high-end systems
            optimal_workers = min(max(350, base_workers), max_by_memory)
            
            logger.info(f"High-end system detected. Using {optimal_workers} workers (CPU: {cpu_count}, Memory: {memory_gb:.1f}GB)")
            return optimal_workers
        
        # For 32-64 core systems
        elif cpu_count >= 32:
            # For 32+ CPU systems, ensure we can use at least 256 workers
            # on systems with sufficient memory
            base_workers = max(2, int(cpu_count * 0.9))
            max_by_memory = int((memory_gb * 0.9) / 0.5)
            min_workers_high_end = min(256, int(memory_gb / 2))
            optimal_workers = max(min(base_workers, max_by_memory), min_workers_high_end)
        
        # Standard calculation for smaller systems
        else:
            # Calculate base worker count based on CPU
            # Use 90% of available cores
            base_workers = max(2, int(cpu_count * 0.9))
            
            # Adjust based on memory - each worker might use ~500MB
            # Allow up to 90% of system memory for workers
            max_by_memory = int((memory_gb * 0.9) / 0.5)
            
            # Take the minimum to avoid oversubscription
            optimal_workers = min(base_workers, max_by_memory)
        
        logger.info(f"Calculated optimal workers: {optimal_workers} (CPU: {cpu_count}, Memory: {memory_gb:.1f}GB)")
        return optimal_workers
    
    except Exception as e:
        logger.warning(f"Error calculating optimal workers: {e}, using fallback value")
        # Fallback to a more aggressive value based on CPU count if available
        if 'cpu_count' in locals():
            return max(32, int(cpu_count * 0.8))
        # Conservative fallback if CPU count isn't available
        return 32

def process_files_parallel(
    db: PIIDatabase,
    job_id: int,
    processing_func: Callable[[str, Dict[str, Any]], Dict[str, Any]],
    max_workers: Optional[int] = None,
    batch_size: int = 100,  # Increased from 10 to 100
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
        # Auto-determine based on system resources
        max_workers = calculate_optimal_workers()
    
    if settings is None:
        settings = {}
    
    # Get the database path for worker processes
    db_path = db.db_path
    
    # Set process title for main process
    setproctitle.setproctitle(f"pii-main-{os.getpid()}")
    
    start_time = time.time()
    stats_queue = SafeQueue()
    files_remaining = True
    processed_count = 0
    error_count = 0
    
    # Print system info
    cpu_count = psutil.cpu_count(logical=True)
    memory_gb = psutil.virtual_memory().total / (1024 * 1024 * 1024)
    logger.info(f"System info: {cpu_count} CPU cores, {memory_gb:.1f}GB memory")
    logger.info(f"Starting parallel processing with {max_workers} worker processes and batch size {batch_size}")
    
    # Create a process pool with fixed number of workers
    # Use ProcessPoolExecutor for true parallelism
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
            for i, (file_id, file_path) in enumerate(pending_files):
                # Mark file as processing
                if db.mark_file_processing(file_id):
                    # Assign a worker ID for tracking
                    worker_settings = settings.copy()
                    worker_settings['worker_id'] = i
                    
                    futures.append(
                        executor.submit(
                            process_single_file_process_safe,
                            file_id,
                            file_path,
                            db_path,
                            job_id,
                            worker_settings
                        )
                    )
            
            # Wait for the batch to complete
            batch_start_time = time.time()
            batch_files_processed = 0
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    batch_files_processed += 1
                    
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
                        processed_count += 1
                    else:
                        # Mark as error
                        db.mark_file_error(result['file_id'], job_id, result.get('error_message', 'Unknown error'))
                        stats_queue.add_error()
                        error_count += 1
                    
                    # Call progress callback if provided
                    if progress_callback:
                        progress_callback({
                            'type': 'file_completed' if result.get('success', False) else 'file_error',
                            'file_id': result.get('file_id'),
                            'file_path': result.get('file_path'),
                            'entities': result.get('entities', []),
                            'error': result.get('error_message') if not result.get('success', False) else None
                        })
                    
                    # Check progress more frequently
                    total_processed = processed_count + error_count
                    if total_processed % 5 == 0 and total_processed > 0:
                        elapsed = time.time() - start_time
                        rate = total_processed / elapsed if elapsed > 0 else 0
                        logger.info(f"Processed {total_processed} files in {elapsed:.2f}s ({rate:.2f} files/sec)")
                        
                except Exception as e:
                    logger.error(f"Worker process error: {e}")
                    error_count += 1
                    
                    # Call progress callback for errors if provided
                    if progress_callback:
                        progress_callback({
                            'type': 'file_error',
                            'error': str(e)
                        })
            
            # Log batch statistics
            batch_elapsed = time.time() - batch_start_time
            batch_rate = batch_files_processed / batch_elapsed if batch_elapsed > 0 else 0
            logger.info(f"Batch completed: {batch_files_processed} files in {batch_elapsed:.2f}s ({batch_rate:.2f} files/sec)")
            
            # Check for resource exhaustion
            mem = psutil.virtual_memory()
            if mem.percent > 90:
                logger.warning(f"Memory pressure detected ({mem.percent}% used), reducing batch size")
                batch_size = max(50, batch_size // 2)  # Maintain minimum batch size of 50
    
    # Update job status
    elapsed = time.time() - start_time
    rate = processed_count / elapsed if elapsed > 0 else 0
    
    if not files_remaining:
        db.update_job_status(job_id, 'completed')
        logger.info(f"Job completed: processed {processed_count} files ({error_count} errors) in {elapsed:.2f}s ({rate:.2f} files/sec)")
    else:
        db.update_job_status(job_id, 'interrupted')
        logger.info(f"Job interrupted: processed {processed_count} files ({error_count} errors) in {elapsed:.2f}s ({rate:.2f} files/sec)")
    
    return {
        'processed': processed_count,
        'errors': error_count,
        'elapsed': elapsed,
        'rate': rate,
        'status': 'completed' if not files_remaining else 'interrupted'
    }

def process_single_file_process_safe(
    file_id: int,
    file_path: str,
    db_path: str,
    job_id: int,
    settings: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process a single file safely in a separate process.
    This function is the entry point for ProcessPoolExecutor workers.
    
    Args:
        file_id: ID of the file to process
        file_path: Path to the file
        db_path: Path to the database
        job_id: ID of the current job
        settings: Processing settings
        
    Returns:
        Processing result dictionary
    """
    try:
        # Import the pii_analyzer_adapter in the worker process
        from src.core.pii_analyzer_adapter import analyze_file
        
        # Process the file
        start_time = time.time()
        
        # Set process title for identifying in monitoring tools
        setproctitle.setproctitle(f"pii-worker-{settings.get('worker_id', os.getpid())}")
        
        result = analyze_file(file_path, settings)
        
        # Add file ID and path to result for tracking
        result['file_id'] = file_id
        result['file_path'] = file_path
        
        # Add timing data
        processing_time = time.time() - start_time
        result['processing_time'] = processing_time
        
        return result
    
    except Exception as e:
        # Catch any exception and return a standardized error result
        logger.error(f"Error in worker process for file {file_path}: {str(e)}")
        return {
            'file_id': file_id,
            'file_path': file_path,
            'success': False,
            'error_message': f"Worker process exception: {str(e)}",
            'processing_time': time.time() - start_time if 'start_time' in locals() else 0
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
    Process a single file safely in a worker thread.
    This function is the entry point for ThreadPoolExecutor workers.
    
    Args:
        file_id: ID of the file to process
        file_path: Path to the file
        db_path: Path to the database
        job_id: ID of the current job
        processing_func: Function to process the file
        settings: Processing settings
        stats_queue: Queue for tracking statistics
        
    Returns:
        Processing result
    """
    # Get thread-local database connection
    db = get_thread_db(db_path)
    
    try:
        # Process the file
        result = process_single_file(file_id, file_path, db, job_id, processing_func, settings)
        
        # Update statistics
        if result.get('success', False):
            stats_queue.add_processed()
        else:
            stats_queue.add_error()
        
        return result
    
    except Exception as e:
        # Log the error
        logger.error(f"Error processing file {file_path}: {e}")
        
        # Mark as error in the database
        db.mark_file_error(file_id, job_id, str(e))
        
        # Update statistics
        stats_queue.add_error()
        
        # Return error result
        return {
            'file_id': file_id,
            'file_path': file_path,
            'success': False,
            'error_message': str(e)
        }

def process_single_file(
    file_id: int,
    file_path: str,
    db: PIIDatabase,
    job_id: int,
    processing_func: Callable[[str, Dict[str, Any]], Dict[str, Any]],
    settings: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process a single file, handling database updates.
    
    Args:
        file_id: ID of the file to process
        file_path: Path to the file
        db: Database connection
        job_id: ID of the current job
        processing_func: Function to process the file
        settings: Processing settings
        
    Returns:
        Processing result dictionary
    """
    # Mark file as processing
    if not db.mark_file_processing(file_id):
        return {
            'file_id': file_id,
            'file_path': file_path,
            'success': False,
            'error_message': "Could not mark file as processing"
        }
    
    try:
        # Measure processing time
        start_time = time.time()
        
        # Process the file
        result = processing_func(file_path, settings)
        
        # Add file ID to result
        result['file_id'] = file_id
        result['file_path'] = file_path
        
        # Calculate processing time
        processing_time = time.time() - start_time
        result['processing_time'] = processing_time
        
        # Update the database
        if result.get('success', False):
            # Store entities and mark as completed
            db.store_file_results(
                file_id, 
                processing_time, 
                result.get('entities', []), 
                result.get('metadata', {})
            )
            db.mark_file_completed(file_id, job_id)
        else:
            # Mark as error
            db.mark_file_error(
                file_id, 
                job_id, 
                result.get('error_message', 'Unknown error')
            )
        
        return result
    
    except Exception as e:
        # Log the error
        logger.error(f"Error processing file {file_path}: {e}")
        
        # Mark as error in the database
        db.mark_file_error(file_id, job_id, str(e))
        
        # Return error result
        return {
            'file_id': file_id,
            'file_path': file_path,
            'success': False,
            'error_message': str(e)
        }

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