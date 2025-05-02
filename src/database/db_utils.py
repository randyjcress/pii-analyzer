#!/usr/bin/env python3
"""
PII Analyzer Database Utilities
Provides SQLite database functionality for persistent storage and resumable processing
"""

import os
import json
import sqlite3
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple, Set

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('pii_database')

# Schema version for future upgrades
SCHEMA_VERSION = 1

class PIIDatabase:
    """Manages SQLite database operations for the PII Analyzer."""
    
    def __init__(self, db_path: str):
        """
        Initialize database connection.
        
        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = db_path
        self.conn = None
        self._initialize()
    
    def _initialize(self):
        """Initialize connection and create schema if needed."""
        exists = os.path.exists(self.db_path)
        try:
            # Connect with foreign key support
            self.conn = sqlite3.connect(self.db_path)
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.conn.row_factory = sqlite3.Row
            
            if not exists:
                logger.info(f"Creating new database at {self.db_path}")
                self._create_schema()
            else:
                logger.info(f"Connected to existing database at {self.db_path}")
                self._verify_schema()
        except sqlite3.Error as e:
            logger.error(f"Database initialization error: {e}")
            raise
    
    def _create_schema(self):
        """Create database schema tables and indexes."""
        try:
            with self.conn:
                # Create metadata table for schema version tracking
                self.conn.execute("""
                CREATE TABLE metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """)
                self.conn.execute("INSERT INTO metadata (key, value) VALUES (?, ?)",
                                 ("schema_version", str(SCHEMA_VERSION)))
                
                # Jobs table for tracking overall job status
                self.conn.execute("""
                CREATE TABLE jobs (
                    job_id INTEGER PRIMARY KEY,
                    name TEXT,
                    start_time TIMESTAMP,
                    last_updated TIMESTAMP,
                    status TEXT,
                    command_line TEXT,
                    total_files INTEGER DEFAULT 0,
                    processed_files INTEGER DEFAULT 0,
                    error_files INTEGER DEFAULT 0,
                    settings TEXT
                )
                """)
                
                # Job metadata table for custom properties
                self.conn.execute("""
                CREATE TABLE job_metadata (
                    metadata_id INTEGER PRIMARY KEY,
                    job_id INTEGER,
                    key TEXT,
                    value TEXT,
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
                    UNIQUE (job_id, key)
                )
                """)
                
                # Files table for tracking individual file status
                self.conn.execute("""
                CREATE TABLE files (
                    file_id INTEGER PRIMARY KEY,
                    job_id INTEGER,
                    file_path TEXT UNIQUE,
                    file_size INTEGER,
                    file_type TEXT,
                    modified_time TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    process_start TIMESTAMP,
                    process_end TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
                )
                """)
                
                # Results table for overall file results
                self.conn.execute("""
                CREATE TABLE results (
                    result_id INTEGER PRIMARY KEY,
                    file_id INTEGER,
                    entity_count INTEGER DEFAULT 0,
                    processing_time FLOAT,
                    metadata TEXT,
                    FOREIGN KEY (file_id) REFERENCES files(file_id)
                )
                """)
                
                # Entities table for individual PII entities
                self.conn.execute("""
                CREATE TABLE entities (
                    entity_id INTEGER PRIMARY KEY,
                    result_id INTEGER,
                    entity_type TEXT,
                    text TEXT,
                    start_index INTEGER,
                    end_index INTEGER,
                    score FLOAT,
                    FOREIGN KEY (result_id) REFERENCES results(result_id)
                )
                """)
                
                # Create indexes for performance
                self.conn.execute("CREATE INDEX idx_files_status ON files(status)")
                self.conn.execute("CREATE INDEX idx_files_job_id ON files(job_id)")
                self.conn.execute("CREATE INDEX idx_results_file_id ON results(file_id)")
                self.conn.execute("CREATE INDEX idx_entities_result_id ON entities(result_id)")
                self.conn.execute("CREATE INDEX idx_entities_type ON entities(entity_type)")
                self.conn.execute("CREATE INDEX idx_job_metadata_key ON job_metadata(key)")
                
                logger.info("Database schema created successfully")
        except sqlite3.Error as e:
            logger.error(f"Schema creation error: {e}")
            raise
    
    def _verify_schema(self):
        """Verify schema version and upgrade if necessary."""
        try:
            cursor = self.conn.cursor()
            # Check if metadata table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'")
            if not cursor.fetchone():
                raise Exception("Invalid database schema: metadata table missing")
            
            # Check schema version
            cursor.execute("SELECT value FROM metadata WHERE key='schema_version'")
            result = cursor.fetchone()
            if not result:
                raise Exception("Schema version not found in metadata")
            
            version = int(result[0])
            if version < SCHEMA_VERSION:
                logger.info(f"Upgrading schema from version {version} to {SCHEMA_VERSION}")
                self._upgrade_schema(version)
            elif version > SCHEMA_VERSION:
                logger.warning(f"Database schema version ({version}) is newer than expected ({SCHEMA_VERSION})")
        except Exception as e:
            logger.error(f"Schema verification error: {e}")
            raise
    
    def _upgrade_schema(self, current_version: int):
        """
        Upgrade schema from current version to latest version.
        
        Args:
            current_version: Current schema version
        """
        # Placeholder for future schema upgrades
        # Example: if current_version == 1: 
        #             upgrade from 1 to 2...
        pass
    
    def close(self):
        """Close the database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    # ---- Job Management ----
    
    def create_job(self, command_line: str = "", name: str = "", 
                  settings: Dict[str, Any] = None, 
                  metadata: Dict[str, Any] = None) -> int:
        """
        Create a new job in the database.
        
        Args:
            command_line: Command line used to start the job
            name: Name for the job
            settings: Dictionary of job settings
            metadata: Dictionary of additional metadata
            
        Returns:
            job_id: ID of the created job
        """
        try:
            now = datetime.now()
            settings_json = json.dumps(settings) if settings else None
            
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    INSERT INTO jobs (name, start_time, last_updated, status, command_line, settings)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (name, now, now, 'created', command_line, settings_json)
                )
                job_id = cursor.lastrowid
                
                # Store additional metadata if provided
                if metadata:
                    for key, value in metadata.items():
                        cursor.execute(
                            """
                            INSERT INTO job_metadata (job_id, key, value)
                            VALUES (?, ?, ?)
                            """,
                            (job_id, key, str(value))
                        )
                
                logger.info(f"Created new job with ID {job_id}")
                return job_id
        except sqlite3.Error as e:
            logger.error(f"Error creating job: {e}")
            raise
    
    def update_job_status(self, job_id: int, status: str, 
                          processed_files: Optional[int] = None, 
                          error_files: Optional[int] = None) -> bool:
        """
        Update job status and counters.
        
        Args:
            job_id: ID of the job to update
            status: New status ('running', 'completed', 'interrupted', 'error')
            processed_files: Number of processed files (if known)
            error_files: Number of error files (if known)
            
        Returns:
            bool: Success of the operation
        """
        try:
            now = datetime.now()
            with self.conn:
                cursor = self.conn.cursor()
                
                # Build update parameters
                update_params = {"last_updated": now, "status": status}
                if processed_files is not None:
                    update_params["processed_files"] = processed_files
                if error_files is not None:
                    update_params["error_files"] = error_files
                
                # Build SQL query
                fields = ", ".join([f"{k} = ?" for k in update_params.keys()])
                values = list(update_params.values())
                values.append(job_id)
                
                cursor.execute(f"UPDATE jobs SET {fields} WHERE job_id = ?", values)
                
                return cursor.rowcount > 0
        except sqlite3.Error as e:
            logger.error(f"Error updating job {job_id}: {e}")
            return False
    
    def get_latest_job(self) -> Optional[Dict[str, Any]]:
        """
        Get the latest job from database.
        
        Returns:
            Dict containing job information or None if no jobs
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM jobs ORDER BY job_id DESC LIMIT 1")
            result = cursor.fetchone()
            
            if result:
                return dict(result)
            return None
        except sqlite3.Error as e:
            logger.error(f"Error getting latest job: {e}")
            return None
    
    def get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        """
        Get job information by ID.
        
        Args:
            job_id: Job ID to retrieve
            
        Returns:
            Dict containing job information or None if not found
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
            result = cursor.fetchone()
            
            if result:
                return dict(result)
            return None
        except sqlite3.Error as e:
            logger.error(f"Error getting job {job_id}: {e}")
            return None
    
    # ---- File Management ----
    
    def register_file(self, job_id: int, file_path: str, file_size: int, 
                     file_type: str, modified_time: float) -> bool:
        """
        Register a file for processing in the database.
        
        Args:
            job_id: Job ID this file belongs to
            file_path: Full path to the file
            file_size: Size of the file in bytes
            file_type: File extension/type
            modified_time: File modification timestamp
            
        Returns:
            bool: True if file was registered, False if already exists
        """
        try:
            with self.conn:
                cursor = self.conn.cursor()
                
                # Check if file already exists for this job
                cursor.execute(
                    "SELECT file_id FROM files WHERE job_id = ? AND file_path = ?", 
                    (job_id, file_path)
                )
                
                if cursor.fetchone():
                    # File already registered
                    return False
                
                # Convert timestamp to datetime
                mod_time = datetime.fromtimestamp(modified_time)
                
                # Register the file
                cursor.execute("""
                INSERT INTO files (job_id, file_path, file_size, file_type, modified_time, status)
                VALUES (?, ?, ?, ?, ?, 'pending')
                """, (job_id, file_path, file_size, file_type, mod_time))
                
                # Update job total_files count
                cursor.execute("""
                UPDATE jobs SET total_files = total_files + 1, last_updated = ?
                WHERE job_id = ?
                """, (datetime.now(), job_id))
                
                return True
        except sqlite3.Error as e:
            logger.error(f"Error registering file {file_path}: {e}")
            return False
    
    def get_pending_files(self, job_id: int, limit: int = 100) -> List[Tuple[int, str]]:
        """
        Get list of pending files for processing.
        
        Args:
            job_id: Job ID to get files for
            limit: Maximum number of files to return
            
        Returns:
            List of (file_id, file_path) tuples
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT file_id, file_path FROM files 
            WHERE job_id = ? AND status = 'pending'
            LIMIT ?
            """, (job_id, limit))
            
            return [(row['file_id'], row['file_path']) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error getting pending files for job {job_id}: {e}")
            return []
    
    def mark_file_processing(self, file_id: int) -> bool:
        """
        Mark a file as currently being processed.
        
        Args:
            file_id: ID of the file to update
            
        Returns:
            bool: Success of the operation
        """
        try:
            with self.conn:
                now = datetime.now()
                self.conn.execute("""
                UPDATE files SET status = 'processing', process_start = ?
                WHERE file_id = ? AND status = 'pending'
                """, (now, file_id))
                
                return self.conn.total_changes > 0
        except sqlite3.Error as e:
            logger.error(f"Error marking file {file_id} as processing: {e}")
            return False
    
    def mark_file_completed(self, file_id: int, job_id: int) -> bool:
        """
        Mark a file as successfully processed.
        
        Args:
            file_id: ID of the file to update
            job_id: Job ID this file belongs to
            
        Returns:
            bool: Success of the operation
        """
        try:
            with self.conn:
                now = datetime.now()
                self.conn.execute("""
                UPDATE files SET status = 'completed', process_end = ?
                WHERE file_id = ?
                """, (now, file_id))
                
                # Update job processed count
                self.conn.execute("""
                UPDATE jobs SET processed_files = processed_files + 1, last_updated = ?
                WHERE job_id = ?
                """, (now, job_id))
                
                return self.conn.total_changes > 0
        except sqlite3.Error as e:
            logger.error(f"Error marking file {file_id} as completed: {e}")
            return False
    
    def mark_file_error(self, file_id: int, job_id: int, error_message: str) -> bool:
        """
        Mark a file as failed with error.
        
        Args:
            file_id: ID of the file to update
            job_id: Job ID this file belongs to
            error_message: Error message to store
            
        Returns:
            bool: Success of the operation
        """
        try:
            with self.conn:
                now = datetime.now()
                self.conn.execute("""
                UPDATE files SET status = 'error', process_end = ?, error_message = ?
                WHERE file_id = ?
                """, (now, error_message, file_id))
                
                # Update job error count
                self.conn.execute("""
                UPDATE jobs SET error_files = error_files + 1, last_updated = ?
                WHERE job_id = ?
                """, (now, job_id))
                
                return self.conn.total_changes > 0
        except sqlite3.Error as e:
            logger.error(f"Error marking file {file_id} as error: {e}")
            return False
    
    # ---- Result Storage ----
    
    def store_file_results(self, file_id: int, processing_time: float, 
                          entities: List[Dict[str, Any]], 
                          metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Store processing results for a file.
        
        Args:
            file_id: ID of the processed file
            processing_time: Time taken to process in seconds
            entities: List of entity dictionaries with detection results
            metadata: Additional metadata about the processing
            
        Returns:
            bool: Success of the operation
        """
        try:
            with self.conn:
                cursor = self.conn.cursor()
                
                # Create result record
                entity_count = len(entities)

                # Convert metadata to JSON if provided
                metadata_json = json.dumps(metadata) if metadata else None
                
                cursor.execute("""
                INSERT INTO results (file_id, entity_count, processing_time, metadata)
                VALUES (?, ?, ?, ?)
                """, (file_id, entity_count, processing_time, metadata_json))
                
                result_id = cursor.lastrowid
                
                # Store individual entities
                if entities:
                    for entity in entities:
                        cursor.execute("""
                        INSERT INTO entities (
                            result_id, entity_type, text, start_index, 
                            end_index, score
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """, (
                            result_id,
                            entity.get('entity_type', ''),
                            entity.get('text', ''),
                            entity.get('start', 0),
                            entity.get('end', 0),
                            entity.get('score', 0.0)
                        ))
                
                return True
        except sqlite3.Error as e:
            logger.error(f"Error storing results for file {file_id}: {e}")
            return False
    
    # ---- Query Functions ----
    
    def get_file_entity_types(self, file_id: int) -> List[str]:
        """
        Get the set of entity types detected in a file.
        
        Args:
            file_id: ID of the file to query
            
        Returns:
            List of unique entity types
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT DISTINCT e.entity_type FROM entities e
            JOIN results r ON e.result_id = r.result_id
            WHERE r.file_id = ?
            """, (file_id,))
            
            return [row[0] for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error getting entity types for file {file_id}: {e}")
            return []
    
    def get_job_statistics(self, job_id: int) -> Dict[str, Any]:
        """
        Get statistics for a job including entity counts by type.
        
        Args:
            job_id: ID of the job to query
            
        Returns:
            Dict with statistics
        """
        try:
            cursor = self.conn.cursor()
            
            # Get job details
            cursor.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
            job = cursor.fetchone()
            if not job:
                return {}
            
            # Get file type counts
            cursor.execute("""
            SELECT file_type, COUNT(*) as count FROM files
            WHERE job_id = ?
            GROUP BY file_type
            """, (job_id,))
            file_types = {row['file_type']: row['count'] for row in cursor.fetchall()}
            
            # Get entity type counts
            cursor.execute("""
            SELECT e.entity_type, COUNT(*) as count FROM entities e
            JOIN results r ON e.result_id = r.result_id
            JOIN files f ON r.file_id = f.file_id
            WHERE f.job_id = ?
            GROUP BY e.entity_type
            """, (job_id,))
            entity_types = {row['entity_type']: row['count'] for row in cursor.fetchall()}
            
            # Calculate average processing time
            cursor.execute("""
            SELECT AVG(r.processing_time) as avg_time FROM results r
            JOIN files f ON r.file_id = f.file_id
            WHERE f.job_id = ? AND f.status = 'completed'
            """, (job_id,))
            avg_time = cursor.fetchone()['avg_time'] or 0
            
            # Build statistics dict
            stats = dict(job)
            stats['file_types'] = file_types
            stats['entity_types'] = entity_types
            stats['avg_processing_time'] = avg_time
            
            return stats
        except sqlite3.Error as e:
            logger.error(f"Error getting statistics for job {job_id}: {e}")
            return {}
    
    def export_to_json(self, job_id: int, include_entities: bool = True) -> Dict[str, Any]:
        """
        Export job results in the traditional JSON format.
        
        Args:
            job_id: ID of the job to export
            include_entities: Whether to include detailed entity data
            
        Returns:
            Dict with results in the original JSON format
        """
        try:
            cursor = self.conn.cursor()
            
            # Get job details
            cursor.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
            job = cursor.fetchone()
            if not job:
                return {}
            
            # Build results structure
            results = {
                "job_id": job_id,
                "command_line": job['command_line'],
                "start_time": job['start_time'],
                "end_time": job['last_updated'],
                "status": job['status'],
                "total_files": job['total_files'],
                "processed_files": job['processed_files'],
                "error_files": job['error_files'],
                "results": []
            }
            
            # Get all processed files
            cursor.execute("""
            SELECT f.*, r.entity_count, r.processing_time, r.result_id, r.metadata 
            FROM files f
            LEFT JOIN results r ON f.file_id = r.file_id
            WHERE f.job_id = ?
            """, (job_id,))
            
            files = cursor.fetchall()
            for file in files:
                file_result = {
                    "file_path": file['file_path'],
                    "file_type": file['file_type'],
                    "file_size": file['file_size'],
                    "status": file['status'],
                    "processing_time": file['processing_time'] if file['processing_time'] else 0,
                    "entities": []
                }
                
                # Parse and add metadata if available
                if file['metadata']:
                    try:
                        metadata = json.loads(file['metadata'])
                        file_result['metadata'] = metadata
                    except json.JSONDecodeError:
                        pass
                
                if include_entities and file['result_id']:
                    # Get entities for this file
                    cursor.execute("""
                    SELECT * FROM entities
                    WHERE result_id = ?
                    """, (file['result_id'],))
                    
                    entities = cursor.fetchall()
                    for entity in entities:
                        file_result['entities'].append({
                            "entity_type": entity['entity_type'],
                            "text": entity['text'],
                            "start": entity['start_index'],
                            "end": entity['end_index'],
                            "score": entity['score']
                        })
                
                results['results'].append(file_result)
            
            return results
        except sqlite3.Error as e:
            logger.error(f"Error exporting results for job {job_id}: {e}")
            return {}

    def get_jobs_by_metadata(self, key: str, value: str) -> List[Dict[str, Any]]:
        """
        Get jobs by a metadata key-value pair.
        
        Args:
            key: Metadata key
            value: Metadata value
            
        Returns:
            List of job dictionaries
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT j.* 
                FROM jobs j
                JOIN job_metadata m ON j.job_id = m.job_id
                WHERE m.key = ? AND m.value = ?
                ORDER BY j.job_id DESC
                """,
                (key, str(value))
            )
            
            jobs = []
            for row in cursor.fetchall():
                job = dict(row)
                
                # Get metadata for this job
                metadata_cursor = self.conn.cursor()
                metadata_cursor.execute(
                    "SELECT key, value FROM job_metadata WHERE job_id = ?",
                    (job['job_id'],)
                )
                
                metadata = {}
                for meta_row in metadata_cursor.fetchall():
                    metadata[meta_row[0]] = meta_row[1]
                
                job['metadata'] = metadata
                
                jobs.append(job)
                
            return jobs
        except sqlite3.Error as e:
            logger.error(f"Error getting jobs by metadata: {e}")
            return []

    def get_all_jobs(self) -> List[Dict[str, Any]]:
        """
        Get all jobs in the database, ordered by newest first.
        
        Returns:
            List of job dictionaries
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT * FROM jobs
                ORDER BY job_id DESC
                """
            )
            
            jobs = []
            for row in cursor.fetchall():
                job = dict(row)
                
                # Get metadata for this job
                metadata_cursor = self.conn.cursor()
                metadata_cursor.execute(
                    "SELECT key, value FROM job_metadata WHERE job_id = ?",
                    (job['job_id'],)
                )
                
                metadata = {}
                for meta_row in metadata_cursor.fetchall():
                    metadata[meta_row[0]] = meta_row[1]
                
                job['metadata'] = metadata
                
                jobs.append(job)
                
            return jobs
        except sqlite3.Error as e:
            logger.error(f"Error getting all jobs: {e}")
            return []

    def get_entity_counts_by_type(self, job_id: int, threshold: float = 0.0) -> Dict[str, int]:
        """
        Get counts of entity types for a job.
        
        Args:
            job_id: Job ID to get entity counts for
            threshold: Minimum confidence score to include
            
        Returns:
            Dictionary of entity types and their counts
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT e.entity_type, COUNT(*) as count
            FROM entities e
            JOIN results r ON e.result_id = r.result_id
            JOIN files f ON r.file_id = f.file_id
            WHERE f.job_id = ? AND f.status = 'completed' AND e.score >= ?
            GROUP BY e.entity_type
            ORDER BY count DESC
            """, (job_id, threshold))
            
            entity_counts = {}
            for row in cursor.fetchall():
                entity_counts[row['entity_type']] = row['count']
                
            return entity_counts
        except sqlite3.Error as e:
            logger.error(f"Error getting entity counts for job {job_id}: {e}")
            return {}
    
    def get_completed_files(self, job_id: int) -> List[Dict[str, Any]]:
        """
        Get all completed files for a job.
        
        Args:
            job_id: Job ID to get files for
            
        Returns:
            List of completed file dictionaries
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT * FROM files
            WHERE job_id = ? AND status = 'completed'
            """, (job_id,))
            
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error getting completed files for job {job_id}: {e}")
            return []
    
    def get_result_by_file_id(self, file_id: int) -> Optional[Dict[str, Any]]:
        """
        Get the result record for a file.
        
        Args:
            file_id: File ID to get result for
            
        Returns:
            Result dictionary or None if not found
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT * FROM results
            WHERE file_id = ?
            """, (file_id,))
            
            row = cursor.fetchone()
            return dict(row) if row else None
        except sqlite3.Error as e:
            logger.error(f"Error getting result for file {file_id}: {e}")
            return None
    
    def get_entities_by_result_id(self, result_id: int) -> List[Dict[str, Any]]:
        """
        Get all entities for a result.
        
        Args:
            result_id: Result ID to get entities for
            
        Returns:
            List of entity dictionaries
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT * FROM entities
            WHERE result_id = ?
            """, (result_id,))
            
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error getting entities for result {result_id}: {e}")
            return []
    
    def get_files_by_job_id(self, job_id: int) -> List[Dict[str, Any]]:
        """
        Get all files for a job.
        
        Args:
            job_id: Job ID to get files for
            
        Returns:
            List of file dictionaries
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT * FROM files
            WHERE job_id = ?
            """, (job_id,))
            
            return [dict(row) for row in cursor.fetchall()]
        except sqlite3.Error as e:
            logger.error(f"Error getting files for job {job_id}: {e}")
            return []
    
    def get_file_results_with_entities(self, file_ids: List[int]) -> List[Dict[str, Any]]:
        """
        Get results with entities for multiple files.
        Useful for batch processing in reporting.
        
        Args:
            file_ids: List of file IDs to get results for
            
        Returns:
            List of file results with entities
        """
        if not file_ids:
            return []
            
        try:
            cursor = self.conn.cursor()
            
            # Prepare placeholders for the IN clause
            placeholders = ','.join(['?'] * len(file_ids))
            
            # Get file and result data
            cursor.execute(f"""
            SELECT f.*, r.result_id, r.entity_count, r.processing_time, r.metadata
            FROM files f
            LEFT JOIN results r ON f.file_id = r.file_id
            WHERE f.file_id IN ({placeholders})
            """, file_ids)
            
            files = []
            for file_row in cursor.fetchall():
                file_data = dict(file_row)
                
                # Get entities if result exists
                if file_data['result_id']:
                    entity_cursor = self.conn.cursor()
                    entity_cursor.execute("""
                    SELECT * FROM entities
                    WHERE result_id = ?
                    """, (file_data['result_id'],))
                    
                    entities = [dict(entity_row) for entity_row in entity_cursor.fetchall()]
                    file_data['entities'] = entities
                else:
                    file_data['entities'] = []
                
                files.append(file_data)
            
            return files
        except sqlite3.Error as e:
            logger.error(f"Error getting file results with entities: {e}")
            return []


# Factory function to get a database instance
def get_database(db_path: str = 'pii_analysis.db') -> PIIDatabase:
    """
    Get a database instance with the given path.
    
    Args:
        db_path: Path to the database file
        
    Returns:
        PIIDatabase instance
    """
    return PIIDatabase(db_path) 