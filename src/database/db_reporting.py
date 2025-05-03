#!/usr/bin/env python3
"""
Database Reporting Utilities for PII Analysis

Provides functions for accessing PII analysis data directly from the database
rather than through intermediate JSON files. These utilities support the
classification and reporting scripts.
"""

import os
import json
from typing import Dict, List, Set, Optional, Any, Tuple
from collections import defaultdict

from .db_utils import get_database

def get_file_processing_stats(db_path: str, job_id: Optional[int] = None) -> Dict[str, int]:
    """
    Get statistics about file processing status for a job.
    
    Args:
        db_path: Path to the database file
        job_id: Specific job ID to analyze (most recent if None)
        
    Returns:
        Dictionary with counts of files in different states
    """
    # Connect to database
    db = get_database(db_path)
    
    # Get job ID if not provided
    if job_id is None:
        jobs = db.get_all_jobs()
        if not jobs:
            return {
                'total_registered': 0,
                'pending': 0,
                'processing': 0,
                'completed': 0,
                'error': 0
            }
        job_id = jobs[0]['job_id']  # Get most recent job
    
    # Get job information
    job = db.get_job(job_id)
    if not job:
        return {
            'total_registered': 0,
            'pending': 0,
            'processing': 0,
            'completed': 0,
            'error': 0
        }
    
    # Query for file status counts
    cursor = db.conn.cursor()
    cursor.execute("""
    SELECT status, COUNT(*) as count FROM files
    WHERE job_id = ?
    GROUP BY status
    """, (job_id,))
    
    status_counts = {row['status']: row['count'] for row in cursor.fetchall()}
    
    # Calculate total files registered
    total_registered = job.get('total_files', 0)
    
    # Get counts by status
    pending_count = status_counts.get('pending', 0)
    processing_count = status_counts.get('processing', 0)
    completed_count = status_counts.get('completed', 0)
    error_count = status_counts.get('error', 0)
    
    return {
        'total_registered': total_registered,
        'pending': pending_count,
        'processing': processing_count,
        'completed': completed_count,
        'error': error_count
    }

def load_pii_data_from_db(db_path: str, job_id: Optional[int] = None, threshold: float = 0.7) -> Dict[str, Any]:
    """
    Load PII analysis data directly from the database.
    
    Args:
        db_path: Path to the database file
        job_id: Specific job ID to load (most recent if None)
        threshold: Confidence threshold for filtering entities
        
    Returns:
        Dictionary with structured PII data, resembling the format of JSON exports
    """
    # Connect to database
    db = get_database(db_path)
    
    # Get job ID if not provided
    if job_id is None:
        jobs = db.get_all_jobs()
        if not jobs:
            raise ValueError(f"No jobs found in database: {db_path}")
        job_id = jobs[0]['job_id']  # Get most recent job
    else:
        # Verify job exists
        job = db.get_job(job_id)
        if not job:
            raise ValueError(f"Job ID {job_id} not found in database: {db_path}")
    
    # Get job information
    job = db.get_job(job_id)
    
    # Get file processing status statistics
    processing_stats = get_file_processing_stats(db_path, job_id)
    
    # Get all completed files for this job
    files = db.get_completed_files(job_id)
    total_completed = len(files)
    
    # Structure to hold results
    results = []
    
    # Get file results with entities
    for file_data in files:
        file_id = file_data['file_id']
        file_path = file_data['file_path']
        
        # Get result record
        result = db.get_result_by_file_id(file_id)
        if not result:
            continue
            
        result_id = result['result_id']
        
        # Get entities
        entities = db.get_entities_by_result_id(result_id)
        
        # Filter entities by threshold
        filtered_entities = [
            {
                'entity_type': entity['entity_type'],
                'text': entity['text'],
                'score': entity['score'],
                'start_index': entity['start_index'],
                'end_index': entity['end_index']
            }
            for entity in entities if entity['score'] >= threshold
        ]
        
        # Create file result object (similar to JSON structure)
        file_result = {
            'file_path': file_path,
            'file_size': file_data.get('file_size', 0),
            'file_type': file_data.get('file_type', ''),
            'entity_count': len(filtered_entities),
            'extraction_method': result.get('extraction_method', 'unknown'),
            'processing_time': result.get('processing_time', 0),
            'entities': filtered_entities
        }
        
        results.append(file_result)
    
    # Create complete data structure (similar to JSON export)
    pii_data = {
        'job_id': job_id,
        'job_name': job.get('name', ''),
        'start_time': job.get('start_time', ''),
        'end_time': job.get('last_updated', ''),
        'total_registered': processing_stats['total_registered'],
        'total_completed': total_completed,
        'pending_files': processing_stats['pending'],
        'processing_files': processing_stats['processing'],
        'error_files': processing_stats['error'],
        'total_files': total_completed,  # For backward compatibility
        'processed_files': job.get('processed_files', 0),  # For backward compatibility
        'results': results
    }
    
    return pii_data

def convert_db_to_json_format(db_path: str, output_path: str, job_id: Optional[int] = None, threshold: float = 0.7) -> str:
    """
    Convert database contents to a JSON file compatible with the original format.
    
    Args:
        db_path: Path to the database file
        output_path: Path to save the JSON output
        job_id: Specific job ID to export (most recent if None)
        threshold: Confidence threshold for filtering entities
        
    Returns:
        Path to the created JSON file
    """
    # Load data from database
    pii_data = load_pii_data_from_db(db_path, job_id, threshold)
    
    # Save to JSON file
    with open(output_path, 'w') as f:
        json.dump(pii_data, f, indent=2)
    
    return output_path

def get_file_type_statistics(db_path: str, job_id: Optional[int] = None) -> Dict[str, int]:
    """
    Get statistics on file types from the database.
    
    Args:
        db_path: Path to the database file
        job_id: Specific job ID to analyze (most recent if None)
        
    Returns:
        Dictionary mapping file extensions to counts
    """
    # Connect to database
    db = get_database(db_path)
    
    # Get job ID if not provided
    if job_id is None:
        jobs = db.get_all_jobs()
        if not jobs:
            return {}
        job_id = jobs[0]['job_id']  # Get most recent job
    
    # Get file type statistics
    files = db.get_files_by_job_id(job_id)
    
    # Count by file type
    file_types = defaultdict(int)
    for file_data in files:
        file_type = file_data.get('file_type', '')
        if file_type:
            file_types[file_type] += 1
    
    return dict(file_types)

def get_entity_statistics(db_path: str, job_id: Optional[int] = None, threshold: float = 0.7) -> Dict[str, int]:
    """
    Get statistics on entity types from the database.
    
    Args:
        db_path: Path to the database file
        job_id: Specific job ID to analyze (most recent if None)
        threshold: Confidence threshold for counting entities
        
    Returns:
        Dictionary mapping entity types to counts
    """
    # Connect to database
    db = get_database(db_path)
    
    # Get job ID if not provided
    if job_id is None:
        jobs = db.get_all_jobs()
        if not jobs:
            return {}
        job_id = jobs[0]['job_id']  # Get most recent job
    
    # Get entity statistics using SQL aggregation
    entity_counts = db.get_entity_counts_by_type(job_id, threshold)
    
    return entity_counts 