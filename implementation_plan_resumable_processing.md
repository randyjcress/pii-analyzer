# Implementation Plan: Resumable Processing for PII Analyzer

## Overview

This document outlines the plan to enhance the PII Analyzer with resumable processing capabilities for extremely large document sets (hundreds of thousands of files). The implementation will use SQLite as the persistent storage mechanism to track processing state and store results, enabling the system to resume analysis after interruption without duplicating work.

## Goals

- Enable processing of very large document sets (100,000+ files)
- Provide ability to resume processing after interruption
- Maintain processing state and results in a persistent SQLite database
- Refactor dependent scripts to work with the new storage mechanism
- Ensure backward compatibility where possible

## Implementation Checklist

### Phase 1: Database Design and Setup ✅

- [x] **1.1 Design database schema**
  - [x] Create `files` table to track all discovered files
  - [x] Create `results` table to store PII detection results
  - [x] Create `jobs` table to track overall job status and metadata
  - [x] Create `entities` table to store individual entity detections

- [x] **1.2 Implement database initialization module**
  - [x] Create `db_utils.py` module
  - [x] Implement database connection and initialization functions
  - [x] Write schema creation/upgrade functions
  - [x] Create indexes for performance optimization

- [x] **1.3 Design file tracking mechanism**
  - [x] Implement function to scan and register files without processing
  - [x] Add file status tracking (pending, processing, completed, error)
  - [x] Store file metadata (size, modification time, path)

#### Phase 1 Testing ✅
- [x] Test database creation and connection
- [x] Verify schema creation with all tables and indexes
- [x] Test job creation and status updates
- [x] Test file registration and status updates
- [x] Validate result storage and retrieval
- [x] Verify JSON export functionality

### Phase 2: Modify Core Processing Logic ✅

- [x] **2.1 Update file discovery process** ✅
  - [x] Modify directory scanning to register files in database
  - [x] Implement file filtering based on processing status
  - [x] Create resumption point detection

#### Phase 2.1 Testing ✅
- [x] Test file discovery and registration with sample directories
- [x] Verify file classification by type
- [x] Test handling of duplicate files
- [x] Validate resumption detection logic
- [x] Benchmark scanning performance with large directories (1,000+ files)

- [x] **2.2 Adapt parallel processing** ✅
  - [x] Modify worker assignment to pull from database
  - [x] Update progress tracking to use database
  - [x] Implement atomic status updates during processing
  - [x] Add thread-safe database handling

#### Phase 2.2 Testing ✅
- [x] Test worker assignment with sample database files
- [x] Verify atomic status updates with concurrent workers
- [x] Test worker scaling based on available resources
- [x] Simulate interruption during processing
- [x] Validate progress tracking accuracy

- [x] **2.3 Implement result storage** ✅
  - [x] Create functions to store entity results in database
  - [x] Modify result collection to save incrementally
  - [x] Add transaction handling for reliability

#### Phase 2.3 Testing ✅
- [x] Test entity result storage with various entity types
- [x] Verify transaction rollback on errors
- [x] Test incremental result saving
- [x] Validate entity retrieval by file and type
- [x] Benchmark storage performance with large entity sets

### Phase 3: Command-Line Interface Enhancements ✅

- [x] **3.1 Add resumption options** ✅
  - [x] Add `--resume` flag to continue from last point
  - [x] Add `--force-restart` to clear previous progress
  - [x] Add `--reprocess-errors` to retry failed files

#### Phase 3.1 Testing ✅
- [x] Test resumption from last known point
- [x] Verify forced restart clears previous state correctly
- [x] Test reprocessing of error files
- [x] Validate command-line argument parsing
- [x] Test integration with existing CLI options

- [x] **3.2 Add database management options** ✅
  - [x] Implement `--db-path` to specify database location
  - [x] Add `--export` to generate legacy JSON format
  - [x] Create `--status` option to view processing statistics
  - [x] Add `--job-id` option to specify which job to work with

#### Phase 3.2 Testing ✅
- [x] Test custom database path specification
- [x] Verify JSON export matches legacy format
- [x] Test status reporting with various job states
- [x] Validate database file handling
- [x] Test cross-platform path handling

- [x] **3.3 Update progress display** ✅
  - [x] Enhance progress bar to show overall job status
  - [x] Add estimated time remaining based on processing history
  - [x] Implement detailed status reporting

#### Phase 3.3 Testing ✅
- [x] Test progress display with various job sizes
- [x] Verify time estimation accuracy
- [x] Test terminal output formatting
- [x] Validate performance impact of progress tracking
- [x] Test interrupt handling during progress display

### Phase 4: Refactor Dependent Scripts

- [ ] **4.1 Modify strict_nc_breach_pii.py**
  - [ ] Update to accept database path as input
  - [ ] Implement direct database query capabilities
  - [ ] Maintain JSON input option for backward compatibility

#### Phase 4.1 Testing
- [ ] Test breach script with database input
- [ ] Verify backward compatibility with JSON
- [ ] Validate report generation with database source
- [ ] Test large dataset reporting performance
- [ ] Verify classification accuracy compared to previous version

- [ ] **4.2 Modify unc_data_classification.py**
  - [ ] Update to accept database path as input
  - [ ] Implement direct database query capabilities
  - [ ] Maintain JSON input option for backward compatibility

#### Phase 4.2 Testing
- [ ] Test classification script with database input
- [ ] Verify backward compatibility with JSON
- [ ] Validate classification accuracy with database source
- [ ] Test performance with large datasets
- [ ] Verify report generation compared to previous version

- [ ] **4.3 Create database utility script**
  - [ ] Implement functions to export database to JSON
  - [ ] Add database maintenance and cleanup functions
  - [ ] Create reporting and statistics functions

#### Phase 4.3 Testing
- [ ] Test JSON export functionality
- [ ] Verify database maintenance operations
- [ ] Test report generation with various datasets
- [ ] Validate statistics accuracy
- [ ] Test command-line interface of utility script

### Phase 5: Testing and Validation

- [ ] **5.1 Create unit tests**
  - [ ] Test database operations and transactions
  - [ ] Validate resumption logic
  - [ ] Test error handling and recovery

- [ ] **5.2 Implement integration tests**
  - [ ] Test full workflow with interruption and resumption
  - [ ] Validate results against previous implementation
  - [ ] Test performance with large file sets

- [ ] **5.3 Real-world testing**
  - [ ] Test with progressively larger document sets
  - [ ] Validate memory usage during extended runs
  - [ ] Measure and optimize performance

## Detailed Database Schema

### `jobs` Table
```sql
CREATE TABLE jobs (
    job_id INTEGER PRIMARY KEY,
    name TEXT,
    start_time TIMESTAMP,
    last_updated TIMESTAMP,
    status TEXT, -- 'running', 'completed', 'interrupted', 'error'
    command_line TEXT,
    total_files INTEGER,
    processed_files INTEGER,
    error_files INTEGER,
    settings TEXT -- Stores configuration options as JSON
);
```

### `job_metadata` Table
```sql
CREATE TABLE job_metadata (
    metadata_id INTEGER PRIMARY KEY,
    job_id INTEGER,
    key TEXT,
    value TEXT,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id),
    UNIQUE (job_id, key)
);
```

### `files` Table
```sql
CREATE TABLE files (
    file_id INTEGER PRIMARY KEY,
    job_id INTEGER,
    file_path TEXT UNIQUE,
    file_size INTEGER,
    file_type TEXT,
    modified_time TIMESTAMP,
    status TEXT, -- 'pending', 'processing', 'completed', 'error'
    error_message TEXT,
    process_start TIMESTAMP,
    process_end TIMESTAMP,
    FOREIGN KEY (job_id) REFERENCES jobs(job_id)
);
```

### `results` Table
```sql
CREATE TABLE results (
    result_id INTEGER PRIMARY KEY,
    file_id INTEGER,
    entity_count INTEGER,
    processing_time FLOAT,
    FOREIGN KEY (file_id) REFERENCES files(file_id)
);
```

### `entities` Table
```sql
CREATE TABLE entities (
    entity_id INTEGER PRIMARY KEY,
    result_id INTEGER,
    entity_type TEXT,
    text TEXT,
    start_index INTEGER,
    end_index INTEGER,
    score FLOAT,
    FOREIGN KEY (result_id) REFERENCES results(result_id)
);
```

## Current Implementation Status

### Completed
- [x] **Phase 1: Database Design and Setup** - The database module `db_utils.py` has been implemented with all required functionality:
  - SQLite database connection and initialization
  - Schema creation and version management
  - Job management functions
  - File tracking and status updates
  - Result storage
  - Query capabilities
  - Export to JSON

- [x] **Phase 2.1: File Discovery Process** - The file discovery module `file_discovery.py` has been implemented with:
  - Directory scanning and file registration
  - File filtering by type/extension
  - Resumption point detection
  - File status reset functionality
  - File statistics generation

- [x] **Phase 2.2-2.3: Parallel Processing and Result Storage** - The worker management module `worker_management.py` has been implemented with:
  - Thread-safe database handling
  - Batched file processing
  - Progress tracking
  - Atomic status updates
  - Interruption handling
  - Result storage in database

- [x] **Phase 3: Command-Line Interface** - A comprehensive CLI script `process_files.py` has been implemented with:
  - Resumable processing capabilities
  - Database specification options
  - Export functionality
  - Status reporting
  - Progress display
  - Interruption handling

### In Progress
- [ ] **Phase 4: Refactor Dependent Scripts** - This phase will focus on updating dependent scripts to use the database directly:
  - Update strict_nc_breach_pii.py to query database directly
  - Update unc_data_classification.py to work with database
  - Create utility scripts for database management

## Next Steps: Refactoring Dependent Scripts

The next phase is to update the existing PII analysis tools to work directly with the database storage. This will help to:

1. Eliminate the need for intermediate JSON files
2. Enable more efficient querying of the data
3. Support analysis of very large datasets that wouldn't fit in memory
4. Maintain backward compatibility with JSON for existing workflows

### Implementation Plan for `strict_nc_breach_pii.py`

To modify the North Carolina breach analysis script:

1. Update the input handling to accept a database path
2. Add database query functions to replace JSON loading
3. Implement result batching for efficient memory usage with large datasets
4. Use SQL queries for filtering and aggregation where possible
5. Maintain the existing JSON input option for backward compatibility

```python
# Example code for using database directly
def load_from_database(db_path, job_id=None):
    """Load PII data directly from database."""
    db = get_database(db_path)
    
    # Get the latest job if none specified
    if job_id is None:
        jobs = db.get_all_jobs()
        if not jobs:
            raise ValueError("No jobs found in database")
        job_id = jobs[0]['job_id']
    
    # Get job information
    job = db.get_job(job_id)
    if not job:
        raise ValueError(f"Job {job_id} not found")
    
    # Query for PII entities in batches
    ssn_files = []
    cc_files = []
    
    # Use SQL to efficiently get counts
    entity_counts = db.get_entity_counts_by_type(job_id)
    
    # Process files in batches to avoid memory issues
    files = db.get_completed_files(job_id)
    
    for file_batch in batched(files, 1000):
        batch_results = db.get_file_results_with_entities(file_batch)
        for file_result in batch_results:
            process_file_for_breach_analysis(file_result, ssn_files, cc_files)
    
    return {
        'job_info': job,
        'ssn_files': ssn_files,
        'cc_files': cc_files,
        'entity_counts': entity_counts
    }
```

## Timeline Estimate (Updated)

- Phase 1: ✅ Completed
- Phase 2: ✅ Completed
- Phase 3: ✅ Completed
- Phase 4: 4-6 days (including testing)
- Phase 5: 5-7 days

Total remaining time: 9-13 days 