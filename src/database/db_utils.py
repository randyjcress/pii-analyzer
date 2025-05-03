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
SCHEMA_VERSION = 2

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
            self.conn = sqlite3.connect(self.db_path, detect_types=sqlite3.PARSE_DECLTYPES)
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
                    file_path TEXT,
                    file_size INTEGER,
                    file_type TEXT,
                    modified_time TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    process_start TIMESTAMP,
                    process_end TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
                    UNIQUE (job_id, file_path)
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

            # Check if results table has metadata column
            cursor.execute("PRAGMA table_info(results)")
            columns = [row['name'] for row in cursor.fetchall()]
            if 'metadata' not in columns:
                logger.info("Adding metadata column to results table")
                cursor.execute("ALTER TABLE results ADD COLUMN metadata TEXT")
                self.conn.commit()
                
        except Exception as e:
            logger.error(f"Schema verification error: {e}")
            raise
    
    def _upgrade_schema(self, current_version: int):
        """
        Upgrade schema from current version to latest version.
        
        Args:
            current_version: Current schema version
        """
        with self.conn:
            cursor = self.conn.cursor()
            
            if current_version == 1 and SCHEMA_VERSION >= 2:
                logger.info("Upgrading schema from version 1 to 2")
                
                # Create a new files table with the correct constraints
                cursor.execute("""
                CREATE TABLE files_new (
                    file_id INTEGER PRIMARY KEY,
                    job_id INTEGER,
                    file_path TEXT,
                    file_size INTEGER,
                    file_type TEXT,
                    modified_time TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    error_message TEXT,
                    process_start TIMESTAMP,
                    process_end TIMESTAMP,
                    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
                    UNIQUE (job_id, file_path)
                )
                """)
                
                # Copy data from old table
                cursor.execute("""
                INSERT INTO files_new 
                SELECT * FROM files
                """)
                
                # Drop old table and rename new one
                cursor.execute("DROP TABLE files")
                cursor.execute("ALTER TABLE files_new RENAME TO files")
                
                # Recreate indexes
                cursor.execute("CREATE INDEX idx_files_status ON files(status)")
                cursor.execute("CREATE INDEX idx_files_job_id ON files(job_id)")
                
                # Update schema version
                cursor.execute("UPDATE metadata SET value = ? WHERE key = 'schema_version'", 
                            (str(SCHEMA_VERSION),))
                
                logger.info("Schema upgrade to version 2 completed successfully")
    
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
        Create a new job in the database
        
        Args:
            command_line: The command line that started the job
            name: Job name for display
            settings: Dictionary of job settings
            metadata: Dictionary of job metadata
            
        Returns:
            ID of the new job
        """
        try:
            now = datetime.now()  # Use datetime object directly, not string
            
            # Ensure metadata is a dictionary
            if metadata is None:
                metadata = {}
            
            # Convert settings to JSON if present
            settings_json = json.dumps(settings) if settings else None
            
            # Create the job
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute("""
                INSERT INTO jobs (
                    name, start_time, last_updated, status, command_line, settings
                ) VALUES (?, ?, ?, 'created', ?, ?)
                """, (name, now, now, command_line, settings_json))
                
                job_id = cursor.lastrowid
                
                # Store metadata
                for key, value in metadata.items():
                    cursor.execute("""
                    INSERT INTO job_metadata (job_id, key, value)
                    VALUES (?, ?, ?)
                    """, (job_id, key, str(value)))
                
                # Make sure directory is always set if provided in name
                if 'directory' not in metadata and name.startswith('PII Analysis - '):
                    # Extract directory from name
                    directory = name[len('PII Analysis - '):]
                    if directory and directory != '.':
                        cursor.execute("""
                        INSERT INTO job_metadata (job_id, key, value)
                        VALUES (?, ?, ?)
                        """, (job_id, 'directory', directory))
                
                logger.info(f"Created new job with ID {job_id}")
                return job_id
        except sqlite3.Error as e:
            logger.error(f"Error creating job: {e}")
            return -1
    
    def update_job_status(self, job_id: int, status: str, 
                          processed_files: Optional[int] = None, 
                          error_files: Optional[int] = None) -> bool:
        """
        Update job status and counters
        
        Args:
            job_id: ID of the job to update
            status: New status value
            processed_files: Number of processed files (None to keep current)
            error_files: Number of error files (None to keep current)
            
        Returns:
            bool: Success of the operation
        """
        try:
            with self.conn:
                # Build update SQL dynamically based on which values are provided
                sql = "UPDATE jobs SET status = ?, last_updated = ?"
                params = [status, datetime.now()]
                
                if processed_files is not None:
                    sql += ", processed_files = ?"
                    params.append(processed_files)
                
                if error_files is not None:
                    sql += ", error_files = ?"
                    params.append(error_files)
                
                sql += " WHERE job_id = ?"
                params.append(job_id)
                
                # Execute update
                self.conn.execute(sql, params)
                
                return self.conn.total_changes > 0
        except sqlite3.Error as e:
            logger.error(f"Error updating job status: {e}")
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
    
    def get_job_status(self, job_id: int) -> Optional[str]:
        """
        Get the status of a job.
        
        Args:
            job_id: Job ID to get status for
            
        Returns:
            Job status or None if job not found
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT status FROM jobs
            WHERE job_id = ?
            """, (job_id,))
            
            result = cursor.fetchone()
            return result['status'] if result else None
            
        except sqlite3.Error as e:
            logger.error(f"Error getting job status for job {job_id}: {e}")
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

    def clear_files_for_job(self, job_id: int) -> int:
        """
        Clear all files for a job to allow for a forced restart.
        This deletes all files, results, and entities associated with the job.
        
        Args:
            job_id: Job ID to clear files for
            
        Returns:
            Number of files deleted
        """
        try:
            with self.conn:
                cursor = self.conn.cursor()
                
                # Get all file IDs for the job
                cursor.execute("SELECT file_id FROM files WHERE job_id = ?", (job_id,))
                file_ids = [row['file_id'] for row in cursor.fetchall()]
                
                # Get all result IDs for these files
                if file_ids:
                    placeholders = ', '.join(['?'] * len(file_ids))
                    cursor.execute(f"SELECT result_id FROM results WHERE file_id IN ({placeholders})", file_ids)
                    result_ids = [row['result_id'] for row in cursor.fetchall()]
                    
                    # Delete entities for these results
                    if result_ids:
                        entity_placeholders = ', '.join(['?'] * len(result_ids))
                        cursor.execute(f"DELETE FROM entities WHERE result_id IN ({entity_placeholders})", result_ids)
                    
                    # Delete results for these files
                    cursor.execute(f"DELETE FROM results WHERE file_id IN ({placeholders})", file_ids)
                
                # Count how many files we'll delete
                cursor.execute("SELECT COUNT(*) as count FROM files WHERE job_id = ?", (job_id,))
                count = cursor.fetchone()['count']
                
                # Delete all files for this job
                cursor.execute("DELETE FROM files WHERE job_id = ?", (job_id,))
                
                # Reset job counters
                cursor.execute("""
                UPDATE jobs 
                SET total_files = 0, processed_files = 0, error_files = 0, status = 'running', last_updated = ?
                WHERE job_id = ?
                """, (datetime.now(), job_id))
                
                logger.info(f"Cleared {count} files for job {job_id} (force restart)")
                return count
                
        except sqlite3.Error as e:
            logger.error(f"Error clearing files for job {job_id}: {e}")
            return 0

    def reset_all_files(self) -> int:
        """
        Reset all files in the database to 'pending' status without deleting them.
        Also resets all job counters.
        
        Returns:
            Number of files reset
        """
        try:
            with self.conn:
                cursor = self.conn.cursor()
                
                # Reset all files to pending
                cursor.execute("""
                UPDATE files 
                SET status = 'pending', process_start = NULL, process_end = NULL, error_message = NULL
                """)
                
                reset_count = cursor.rowcount
                
                # Get all job IDs
                cursor.execute("SELECT job_id FROM jobs")
                job_ids = [row['job_id'] for row in cursor.fetchall()]
                
                # Reset job counters for all jobs
                for job_id in job_ids:
                    cursor.execute("""
                    UPDATE jobs 
                    SET processed_files = 0, error_files = 0, status = 'created', last_updated = ?
                    WHERE job_id = ?
                    """, (datetime.now(), job_id))
                
                logger.info(f"Reset {reset_count} files to 'pending' status across all jobs")
                
                # Also delete all results and entities (they'll be recreated during processing)
                # First get all result IDs
                cursor.execute("SELECT result_id FROM results")
                result_ids = [row['result_id'] for row in cursor.fetchall()]
                
                # Delete all entities
                if result_ids:
                    cursor.execute("DELETE FROM entities")
                
                # Delete all results
                cursor.execute("DELETE FROM results")
                
                return reset_count
                
        except sqlite3.Error as e:
            logger.error(f"Error resetting all files: {e}")
            return 0

    def get_jobs_for_directory(self, directory: str) -> List[Dict[str, Any]]:
        """
        Get jobs for a specific directory path, sorted by most recent first.
        
        Args:
            directory: Directory path to find jobs for
            
        Returns:
            List of job dictionaries, sorted by most recent first
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT j.* FROM jobs j
            JOIN job_metadata m ON j.job_id = m.job_id
            WHERE m.key = 'directory' AND m.value = ?
            ORDER BY j.start_time DESC
            """, (directory,))
            
            jobs = []
            for row in cursor.fetchall():
                job = dict(row)
                
                # Get metadata for this job
                cursor.execute("""
                SELECT key, value FROM job_metadata
                WHERE job_id = ?
                """, (job['job_id'],))
                
                metadata = {row['key']: row['value'] for row in cursor.fetchall()}
                job['metadata'] = metadata
                job['directory'] = metadata.get('directory', '')
                
                jobs.append(job)
                
            return jobs
            
        except sqlite3.Error as e:
            logger.error(f"Error getting jobs for directory {directory}: {e}")
            return []

    def mark_missing_files(self, job_id: int, found_files: Set[str]) -> int:
        """
        Mark files as missing (error) if they are in the database but not in the found_files set.
        
        Args:
            job_id: Job ID to check files for
            found_files: Set of file paths that were found during scanning
            
        Returns:
            Number of files marked as missing
        """
        try:
            # Get all file paths for this job
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT file_id, file_path FROM files
            WHERE job_id = ?
            """, (job_id,))
            
            db_files = {row['file_path']: row['file_id'] for row in cursor.fetchall()}
            
            # Find files that are in the database but not in found_files
            missing_files = set(db_files.keys()) - found_files
            
            # Mark missing files as error
            if missing_files:
                with self.conn:
                    for file_path in missing_files:
                        file_id = db_files[file_path]
                        self.conn.execute("""
                        UPDATE files
                        SET status = 'error', error_message = 'File no longer exists'
                        WHERE file_id = ?
                        """, (file_id,))
                        
                return len(missing_files)
            return 0
            
        except sqlite3.Error as e:
            logger.error(f"Error marking missing files for job {job_id}: {e}")
            return 0

    def get_file_count_for_job(self, job_id: int) -> int:
        """
        Get the total number of files for a job.
        
        Args:
            job_id: Job ID to get count for
            
        Returns:
            Total number of files
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT COUNT(*) as count FROM files
            WHERE job_id = ?
            """, (job_id,))
            
            result = cursor.fetchone()
            return result['count'] if result else 0
            
        except sqlite3.Error as e:
            logger.error(f"Error getting file count for job {job_id}: {e}")
            return 0

    def get_completed_count_for_job(self, job_id: int) -> int:
        """
        Get the number of completed files for a job.
        
        Args:
            job_id: Job ID to get count for
            
        Returns:
            Number of completed files
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT COUNT(*) as count FROM files
            WHERE job_id = ? AND status = 'completed'
            """, (job_id,))
            
            result = cursor.fetchone()
            return result['count'] if result else 0
            
        except sqlite3.Error as e:
            logger.error(f"Error getting completed count for job {job_id}: {e}")
            return 0

    def get_file_status_counts(self, job_id: int) -> Dict[str, int]:
        """
        Get counts of files by status for a job.
        
        Args:
            job_id: Job ID to get counts for
            
        Returns:
            Dictionary mapping status to count
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
            SELECT status, COUNT(*) as count FROM files
            WHERE job_id = ?
            GROUP BY status
            """, (job_id,))
            
            return {row['status']: row['count'] for row in cursor.fetchall()}
            
        except sqlite3.Error as e:
            logger.error(f"Error getting file status counts for job {job_id}: {e}")
            return {}

    def reset_processing_files(self, job_id: int) -> int:
        """
        Reset files in 'processing' status to 'pending'.
        
        Args:
            job_id: Job ID to reset files for
            
        Returns:
            Number of files reset
        """
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute("""
                UPDATE files
                SET status = 'pending', process_start = NULL
                WHERE job_id = ? AND status = 'processing'
                """, (job_id,))
                
                return cursor.rowcount
                
        except sqlite3.Error as e:
            logger.error(f"Error resetting processing files for job {job_id}: {e}")
            return 0

    def create_job(self, directory: str, name: str = "", settings: Dict[str, Any] = None) -> int:
        """
        Create a new job for the given directory.
        
        Args:
            directory: Directory to process
            name: Optional name for the job
            settings: Optional settings for the job
            
        Returns:
            Job ID of the newly created job
        """
        timestamp = datetime.now()
        if not name:
            # Use the directory name as the job name if not provided
            name = f"PII Analysis - {os.path.basename(directory)}"
            
        try:
            with self.conn:
                cursor = self.conn.cursor()
                
                # Insert job record
                cursor.execute("""
                INSERT INTO jobs (name, start_time, last_updated, status, settings)
                VALUES (?, ?, ?, ?, ?)
                """, (
                    name,
                    timestamp,
                    timestamp,
                    'created',
                    json.dumps(settings) if settings else None
                ))
                
                job_id = cursor.lastrowid
                
                # Store directory as metadata
                cursor.execute("""
                INSERT INTO job_metadata (job_id, key, value)
                VALUES (?, ?, ?)
                """, (job_id, 'directory', directory))
                
                logger.info(f"Created new job {job_id} for directory: {directory}")
                return job_id
                
        except sqlite3.Error as e:
            logger.error(f"Error creating job for directory {directory}: {e}")
            raise


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