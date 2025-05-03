#!/usr/bin/env python3
"""
File Discovery Module for PII Analyzer
Provides functions to scan directories and register files in the database
for resumable processing
"""

import os
import time
import logging
from typing import List, Tuple, Set, Dict, Any, Optional, Callable
from pathlib import Path

from src.database.db_utils import PIIDatabase

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('file_discovery')

# Default supported file extensions
DEFAULT_SUPPORTED_EXTENSIONS = {
    '.txt', '.pdf', '.docx', '.doc', '.rtf',  # Text documents
    '.xlsx', '.xls', '.csv', '.tsv',          # Spreadsheets
    '.pptx', '.ppt',                          # Presentations
    '.json', '.xml', '.html', '.htm',         # Structured data
    '.eml', '.msg',                           # Email files
    '.md', '.markdown'                        # Markdown
}

def get_file_type(file_path: str) -> str:
    """
    Get the file type (extension) from a file path.
    
    Args:
        file_path: Path to the file
        
    Returns:
        File extension (lowercase) including the dot
    """
    _, ext = os.path.splitext(file_path)
    return ext.lower()

def is_supported_file(file_path: str, supported_extensions: Set[str]) -> bool:
    """
    Check if a file is supported based on its extension.
    
    Args:
        file_path: Path to the file
        supported_extensions: Set of supported file extensions
        
    Returns:
        True if file type is supported, False otherwise
    """
    file_type = get_file_type(file_path)
    
    # Handle extensions with or without dots
    if file_type in supported_extensions:
        return True
    
    # Try without the dot if extension has a dot
    if file_type.startswith('.') and file_type[1:] in supported_extensions:
        return True
    
    # Try with a dot if supported_extensions have dots but file_type doesn't
    if not file_type.startswith('.') and f'.{file_type}' in supported_extensions:
        return True
        
    return False

def scan_directory(
    directory_path: str, 
    db: PIIDatabase, 
    job_id: int,
    supported_extensions: Optional[Set[str]] = None,
    max_files: Optional[int] = None,
    skip_registration: bool = False,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None
) -> Tuple[int, int]:
    """
    Scan directory for files and register them in the database.
    
    Args:
        directory_path: Directory to scan
        db: Database connection
        job_id: Job ID to register files under
        supported_extensions: Set of allowed file extensions (None for all)
        max_files: Maximum number of files to register
        skip_registration: If True, only count files but don't try to register them
        progress_callback: Optional callback for progress updates
        
    Returns:
        Tuple of (total files found, newly registered files)
    """
    logger.info(f"Scanning directory: {directory_path}")
    
    if not os.path.isdir(directory_path):
        logger.error(f"Directory not found: {directory_path}")
        return 0, 0
    
    if supported_extensions is None:
        from ..utils.file_utils import get_supported_extensions
        supported_extensions = set(get_supported_extensions().keys())
        logger.info(f"Using default supported extensions: {supported_extensions}")
    
    total_files = 0
    new_files = 0
    start_time = time.time()
    last_update_time = start_time
    
    try:
        for root, _, files in os.walk(directory_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                
                # Check if we've reached the maximum file limit
                if max_files is not None and new_files >= max_files:
                    logger.info(f"Reached maximum file limit of {max_files}, stopping scan")
                    return total_files, new_files
                
                # Skip unsupported file types
                if not is_supported_file(file_path, supported_extensions):
                    continue
                    
                total_files += 1
                
                # Update progress more frequently
                current_time = time.time()
                if progress_callback and (total_files % 100 == 0 or current_time - last_update_time > 0.5):
                    progress_callback({
                        'type': 'scan_progress',
                        'total_files': total_files,
                        'new_files': new_files,
                        'current_file': file_path,
                        'elapsed': current_time - start_time
                    })
                    last_update_time = current_time
                
                # Skip registration if requested (for use with reset database)
                if skip_registration:
                    continue
                
                # Register file in database
                try:
                    file_size = os.path.getsize(file_path)
                    modified_time = os.path.getmtime(file_path)
                    file_type = get_file_type(file_path)
                    
                    if db.register_file(job_id, file_path, file_size, file_type, modified_time):
                        new_files += 1
                        
                        # Log progress every 1000 files
                        if new_files % 1000 == 0:
                            elapsed = time.time() - start_time
                            logger.info(f"Registered {new_files} files so far (total found: {total_files}, elapsed: {elapsed:.2f}s)")
                except OSError as e:
                    logger.error(f"Error accessing file {file_path}: {e}")
    
        elapsed = time.time() - start_time
        logger.info(f"Directory scan complete: found {total_files} files, "
                    f"registered {new_files} new files in {elapsed:.2f} seconds")
        
        # Final progress update
        if progress_callback:
            progress_callback({
                'type': 'scan_complete',
                'total_files': total_files,
                'new_files': new_files,
                'elapsed': elapsed
            })
        
        return total_files, new_files
    
    except Exception as e:
        logger.error(f"Error scanning directory {directory_path}: {e}")
        raise

def scan_file_list(
    file_list: List[str], 
    db: PIIDatabase, 
    job_id: int,
    supported_extensions: Optional[Set[str]] = None
) -> Tuple[int, int]:
    """
    Scan a list of files and register them in the database.
    
    Args:
        file_list: List of file paths to scan
        db: Database connection
        job_id: Current job ID
        supported_extensions: Set of supported file extensions
        
    Returns:
        Tuple of (total_files_processed, new_files_registered)
    """
    if supported_extensions is None:
        supported_extensions = DEFAULT_SUPPORTED_EXTENSIONS
    
    total_files = 0
    new_files = 0
    
    logger.info(f"Processing list of {len(file_list)} files")
    
    for file_path in file_list:
        if not os.path.isfile(file_path):
            logger.warning(f"File not found or not a regular file: {file_path}")
            continue
            
        if not is_supported_file(file_path, supported_extensions):
            continue
            
        total_files += 1
        
        try:
            file_size = os.path.getsize(file_path)
            modified_time = os.path.getmtime(file_path)
            file_type = get_file_type(file_path)
            
            if db.register_file(job_id, file_path, file_size, file_type, modified_time):
                new_files += 1
        except OSError as e:
            logger.error(f"Error accessing file {file_path}: {e}")
    
    logger.info(f"File list processing complete: processed {total_files} files, "
                f"registered {new_files} new files")
    
    return total_files, new_files

def find_resumption_point(db: PIIDatabase, job_id: int) -> Dict[str, Any]:
    """
    Find the point to resume processing from a previous run.
    
    Args:
        db: Database connection
        job_id: Job ID to resume
        
    Returns:
        Dictionary with resumption information:
        - status: 'resumable', 'completed', or 'not_found'
        - total_files: Total files in job
        - pending_files: Number of pending files
        - processing_files: Number of files marked as processing
        - completed_files: Number of completed files
        - error_files: Number of files with errors
    """
    # Get job information
    job = db.get_job(job_id)
    if not job:
        return {
            'status': 'not_found',
            'job_id': job_id,
            'message': f"Job {job_id} not found"
        }
    
    # Job already completed
    if job['status'] == 'completed':
        return {
            'status': 'completed',
            'job_id': job_id,
            'total_files': job['total_files'],
            'completed_files': job['processed_files'],
            'error_files': job['error_files'],
            'message': f"Job {job_id} is already completed"
        }
    
    # Count files by status
    cursor = db.conn.cursor()
    cursor.execute("""
    SELECT status, COUNT(*) as count FROM files
    WHERE job_id = ?
    GROUP BY status
    """, (job_id,))
    
    status_counts = {row['status']: row['count'] for row in cursor.fetchall()}
    
    # Calculate total counts
    pending_files = status_counts.get('pending', 0)
    processing_files = status_counts.get('processing', 0)
    completed_files = status_counts.get('completed', 0)
    error_files = status_counts.get('error', 0)
    
    # Determine if job is resumable
    is_resumable = pending_files > 0 or processing_files > 0
    
    result = {
        'status': 'resumable' if is_resumable else 'not_resumable',
        'job_id': job_id,
        'total_files': job['total_files'],
        'pending_files': pending_files,
        'processing_files': processing_files,
        'completed_files': completed_files,
        'error_files': error_files
    }
    
    if is_resumable:
        result['message'] = f"Job {job_id} can be resumed: {pending_files} pending, {processing_files} processing"
    else:
        result['message'] = f"Job {job_id} cannot be resumed: no pending or processing files"
    
    return result

def reset_stalled_files(db: PIIDatabase, job_id: int) -> int:
    """
    Reset files that were left in 'processing' state due to
    program interruption.
    
    Args:
        db: Database connection
        job_id: Job ID to update
        
    Returns:
        Number of files reset to 'pending'
    """
    try:
        with db.conn:
            cursor = db.conn.cursor()
            cursor.execute("""
            UPDATE files SET status = 'pending', process_start = NULL
            WHERE job_id = ? AND status = 'processing'
            """, (job_id,))
            
            reset_count = cursor.rowcount
            
            if reset_count > 0:
                logger.info(f"Reset {reset_count} stalled files to 'pending' status")
            
            return reset_count
    except Exception as e:
        logger.error(f"Error resetting stalled files: {e}")
        return 0

def get_file_statistics(db: PIIDatabase, job_id: int) -> Dict[str, Any]:
    """
    Get detailed statistics about files in a job.
    
    Args:
        db: Database connection
        job_id: Job ID to query
        
    Returns:
        Dictionary with file statistics
    """
    try:
        cursor = db.conn.cursor()
        
        # Get file counts by status
        cursor.execute("""
        SELECT status, COUNT(*) as count FROM files
        WHERE job_id = ?
        GROUP BY status
        """, (job_id,))
        status_counts = {row['status']: row['count'] for row in cursor.fetchall()}
        
        # Get file counts by type
        cursor.execute("""
        SELECT file_type, COUNT(*) as count FROM files
        WHERE job_id = ?
        GROUP BY file_type
        """, (job_id,))
        type_counts = {row['file_type']: row['count'] for row in cursor.fetchall()}
        
        # Get size statistics
        cursor.execute("""
        SELECT 
            MIN(file_size) as min_size,
            MAX(file_size) as max_size,
            AVG(file_size) as avg_size,
            SUM(file_size) as total_size
        FROM files
        WHERE job_id = ?
        """, (job_id,))
        size_stats = dict(cursor.fetchone())
        
        # Get directory counts
        cursor.execute("""
        SELECT COUNT(DISTINCT substr(file_path, 1, instr(file_path, '/'))) as dir_count
        FROM files
        WHERE job_id = ?
        """, (job_id,))
        dir_count = cursor.fetchone()['dir_count']
        
        return {
            'job_id': job_id,
            'status_counts': status_counts,
            'type_counts': type_counts,
            'size_stats': size_stats,
            'directory_count': dir_count
        }
    except Exception as e:
        logger.error(f"Error getting file statistics: {e}")
        return {} 