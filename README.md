# PII Analysis System with Resumable Processing

A comprehensive system for analyzing files for Personally Identifiable Information (PII) with support for resumable processing of large document sets.

## Overview

This project enhances the PII Analyzer with persistent storage and resumable processing capabilities, allowing it to handle extremely large document sets (100,000+ files) efficiently. The system uses SQLite as the storage mechanism to track processing state and store results, enabling it to resume analysis after interruption without duplicating work.

## Features

- **Resumable Processing**: Continue processing from where you left off if interrupted
- **Parallel Processing**: Efficient multi-threaded processing with thread-safe database access
- **Persistent Storage**: SQLite database for storing all processing results
- **Progress Tracking**: Real-time progress tracking with estimated completion time
- **File Classification**: Automatic file type detection and classification
- **Detailed Reporting**: Comprehensive statistics on processed files and found entities
- **Export Capabilities**: Export results to JSON format for compatibility with other tools
- **Command-Line Interface**: Robust CLI with many customization options

## Getting Started

### Prerequisites

- Python 3.6+
- Required Python packages:
  - sqlite3 (standard library)
  - concurrent.futures (standard library)
  - argparse (standard library)

### Installation

Clone the repository and navigate to the project directory:

```bash
git clone https://github.com/yourusername/pii-analysis.git
cd pii-analysis
```

### Basic Usage

Process a directory for PII:

```bash
python src/process_files.py /path/to/documents --db-path results.db
```

Resume processing after interruption:

```bash
python src/process_files.py /path/to/documents --db-path results.db --resume
```

Process with 8 worker threads:

```bash
python src/process_files.py /path/to/documents --workers 8
```

Export results to JSON:

```bash
python src/process_files.py --db-path results.db --export results.json
```

Show job status:

```bash
python src/process_files.py --db-path results.db --status
```

For more options:

```bash
python src/process_files.py --help
```

## Architecture

The system consists of several key components:

1. **Database Utilities** (`src/database/db_utils.py`): 
   - SQLite database connection and management
   - Schema creation and version management
   - Query and transaction functionality

2. **File Discovery** (`src/core/file_discovery.py`):
   - Directory scanning and file registration
   - File filtering by type/extension
   - Resumption point detection
   - Status management

3. **Worker Management** (`src/core/worker_management.py`):
   - Thread-safe parallel processing
   - Progress tracking and reporting
   - Result storage coordination

4. **Command-Line Interface** (`src/process_files.py`):
   - User-facing command-line tool
   - Job management and control
   - Status reporting and export functionality

## Database Schema

The SQLite database schema includes the following tables:

- `jobs`: Job metadata and overall status
- `job_metadata`: Additional metadata for jobs
- `files`: Individual file information and processing status
- `results`: Processing results for each file
- `entities`: Individual PII entities found in files

## Advanced Usage

### Processing Specific File Types

Limit processing to specific file extensions:

```bash
python src/process_files.py /path/to/documents --extensions txt,pdf,docx
```

### Limiting Processing

Process only a specific number of files:

```bash
python src/process_files.py /path/to/documents --max-files 1000
```

### Batch Size Control

Adjust the batch size for processing:

```bash
python src/process_files.py /path/to/documents --batch-size 50
```

## Implementation Details

The system is designed with the following principles:

1. **Efficiency**: Minimizes repeated work through persistent tracking
2. **Scalability**: Handles extremely large document sets through batched processing
3. **Reliability**: Tolerates interruptions and system crashes
4. **Thread-safety**: Uses thread-local storage for database connections
5. **Atomicity**: Ensures database operations are atomic even with multiple workers

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

* North Carolina Breach Notification Requirements
* UNC Data Classification Framework 