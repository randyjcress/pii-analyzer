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

def is_supported_file(file_path: str, extensions: Set[str]) -> bool:
    """
    Check if a file is supported based on its extension.
    
    Args:
        file_path: Path to the file
        extensions: Set of supported file extensions
        
    Returns:
        True if file type is supported, False otherwise
    """
    file_type = get_file_type(file_path)
    
    # Handle extensions with or without dots
    if file_type in extensions:
        return True
    
    # Try without the dot if extension has a dot
    if file_type.startswith('.') and file_type[1:] in extensions:
        return True
    
    # Try with a dot if extensions have dots but file_type doesn't
    if not file_type.startswith('.') and f'.{file_type}' in extensions:
        return True
        
    return False

def scan_directory(
    db: PIIDatabase, 
    job_id: int,
    directory_path: str, 
    extensions: Optional[Set[str]] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None
) -> Dict[str, int]:
    """
    Scan directory for files and register them in the database.
    
    Args:
        db: Database connection
        job_id: Job ID to register files under
        directory_path: Directory to scan
        extensions: Set of allowed file extensions (None for defaults)
        progress_callback: Optional callback for progress updates
        
    Returns:
        Dictionary with scan statistics
    """
    logger.info(f"Scanning directory: {directory_path}")
    
    if not os.path.isdir(directory_path):
        logger.error(f"Directory not found: {directory_path}")
        return {'added': 0, 'removed': 0, 'total': 0}
    
    if extensions is None:
        extensions = DEFAULT_SUPPORTED_EXTENSIONS
    
    # Track statistics
    stats = {
        'files_scanned': 0,
        'files_added': 0,
        'files_removed': 0,
        'files_total': 0
    }
    
    # Track all file paths found
    found_files = set()
    
    # Start timing scan
    start_time = time.time()
    last_update_time = start_time
    
    # Scan directory
    try:
        for root, _, files in os.walk(directory_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                
                # Update scanned count
                stats['files_scanned'] += 1
                
                # Check if it's a supported file type
                if not is_supported_file(file_path, extensions):
                    continue
                
                # Add to found file set
                found_files.add(file_path)
                
                # Call progress callback periodically
                current_time = time.time()
                if progress_callback and (stats['files_scanned'] % 100 == 0 or 
                                          current_time - last_update_time > 1.0):
                    progress_callback({
                        'type': 'progress',
                        'files_scanned': stats['files_scanned']
                    })
                    last_update_time = current_time
                
                # Get file information
                try:
                    file_size = os.path.getsize(file_path)
                    modified_time = os.path.getmtime(file_path)
                    file_type = get_file_type(file_path)
                    
                    # Register file in database
                    if db.register_file(job_id, file_path, file_size, file_type, modified_time):
                        stats['files_added'] += 1
                except OSError as e:
                    logger.error(f"Error accessing file {file_path}: {e}")
    
        # Check for removed files
        removed_count = db.mark_missing_files(job_id, found_files)
        stats['files_removed'] = removed_count
        
        # Get total file count
        stats['files_total'] = db.get_file_count_for_job(job_id)
        
        # Log completion
        elapsed = time.time() - start_time
        logger.info(f"Directory scan complete: found {len(found_files)} files, "
                    f"added {stats['files_added']} new files, removed {stats['files_removed']} "
                    f"files in {elapsed:.2f} seconds")
        
        # Final progress update
        if progress_callback:
            progress_callback({
                'type': 'completed',
                'files_added': stats['files_added'],
                'files_removed': stats['files_removed'],
                'files_total': stats['files_total']
            })
        
        return {
            'added': stats['files_added'],
            'removed': stats['files_removed'],
            'total': stats['files_total']
        }
    
    except Exception as e:
        logger.error(f"Error scanning directory {directory_path}: {e}")
        if progress_callback:
            progress_callback({
                'type': 'error',
                'error': str(e)
            })
        return {'added': 0, 'removed': 0, 'total': 0}

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

def find_resumption_point(
    db: PIIDatabase, 
    directory: str,
    job_id: Optional[int] = None
) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    """
    Find a job that can be resumed for a directory.
    
    Args:
        db: Database connection
        directory: Directory path
        job_id: Specific job ID to check (optional)
        
    Returns:
        Tuple of (job_id, job_info) or (None, None) if no resumable job
    """
    if job_id:
        # Check specific job
        job = db.get_job(job_id)
        if job and job.get('directory') == directory:
            return job_id, job
        return None, None
    
    # Find jobs for this directory
    jobs = db.get_jobs_for_directory(directory)
    
    if not jobs:
        return None, None
    
    # Return the most recent job
    return jobs[0]['job_id'], jobs[0]

def reset_stalled_files(db: PIIDatabase, job_id: int) -> int:
    """
    Reset stalled files to pending status.
    
    Args:
        db: Database connection
        job_id: Job ID to process
        
    Returns:
        Number of reset files
    """
    # Reset processing files to pending
    reset_count = db.reset_processing_files(job_id)
    
    if reset_count > 0:
        logger.info(f"Reset {reset_count} stalled files to 'pending' status")
    
    return reset_count

def get_file_statistics(db: PIIDatabase, job_id: int) -> Dict[str, Any]:
    """
    Get statistics about files in a job.
    
    Args:
        db: Database connection
        job_id: Job ID to query
        
    Returns:
        Dictionary with statistics
    """
    # Get status counts
    status_counts = db.get_file_status_counts(job_id)
    
    # Extract counts for common statuses
    pending = status_counts.get('pending', 0)
    processing = status_counts.get('processing', 0)
    completed = status_counts.get('completed', 0)
    error = status_counts.get('error', 0)
    
    return {
        'pending': pending,
        'processing': processing,
        'completed': completed,
        'error': error,
        'total': pending + processing + completed + error
    } 