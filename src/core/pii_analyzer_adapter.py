#!/usr/bin/env python3
"""
PII Analyzer Adapter Module for Resumable Processing
Adapts the existing PII analyzer functionality to work with the resumable processing system
"""

import os
import time
import logging
import subprocess
import tempfile
import json
import psutil
import setproctitle
import resource
import sys
from typing import Dict, Any, List, Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('pii_analyzer_adapter')

# Global OCR semaphore to limit concurrent OCR processes across the system
# This will be initialized by the main process
OCR_SEMAPHORE = None

def analyze_file(
    file_path: str,
    settings: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Process a single file using the PII analyzer
    
    Args:
        file_path: Path to the file to process
        settings: Dictionary of processing settings:
                  - threshold: Confidence threshold (0-1)
                  - force_ocr: Force OCR for text extraction
                  - ocr_dpi: DPI for OCR
                  - ocr_threads: Number of OCR threads
                  - max_pages: Maximum pages per PDF
                  - entities: List of entity types to detect
                  - debug: Show detailed debug information
                  - worker_id: Worker ID for tracking
        
    Returns:
        Dictionary with processing results
    """
    # Set process title for better monitoring
    worker_id = settings.get('worker_id', os.getpid())
    setproctitle.setproctitle(f"pii-worker-{worker_id}")
    
    # Set resource limits to prevent runaway processes
    # Don't set RLIMIT_AS on macOS as it can cause issues
    if sys.platform != 'darwin':
        # Limit virtual memory to 4GB per process on Linux only
        resource.setrlimit(resource.RLIMIT_AS, (4 * 1024 * 1024 * 1024, -1))
    
    # Record memory usage at start
    start_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
    
    # Record start time for overall processing
    start_time = time.time()
    
    # Track timing of individual components
    timings = {
        'setup': 0,
        'execution': 0,
        'result_processing': 0,
        'cleanup': 0,
        'file_io': 0,
        'ocr': 0
    }
    
    # Extract settings with defaults
    threshold = settings.get('threshold', 0.7)
    force_ocr = settings.get('force_ocr', False)
    ocr_dpi = settings.get('ocr_dpi', 300)
    ocr_threads = settings.get('ocr_threads', 0)
    max_pages = settings.get('max_pages')
    entities = settings.get('entities')
    debug = settings.get('debug', False)
    
    # Check if file exists and is accessible
    if not os.path.exists(file_path):
        return {
            'file_path': file_path,
            'success': False,
            'error_message': f"File not found: {file_path}",
            'entities': [],
            'file_size': 0,
            'processing_time': time.time() - start_time,
            'timings': timings,
            'memory_usage_mb': 0
        }
    
    # Check file size first to skip very large files
    try:
        file_size = os.path.getsize(file_path)
        if file_size > 100 * 1024 * 1024:  # 100MB
            return {
                'file_path': file_path,
                'success': False,
                'error_message': f"File too large: {file_size/1024/1024:.2f}MB (max 100MB)",
                'entities': [],
                'file_size': file_size,
                'processing_time': time.time() - start_time,
                'timings': timings,
                'memory_usage_mb': 0
            }
    except OSError as e:
        return {
            'file_path': file_path,
            'success': False,
            'error_message': f"File access error: {str(e)}",
            'entities': [],
            'file_size': 0,
            'processing_time': time.time() - start_time,
            'timings': timings,
            'memory_usage_mb': 0
        }
    
    # Start timing setup phase
    setup_start = time.time()
    
    # Create a temporary output file
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_file:
        temp_output = tmp_file.name
    
    # Record setup time
    timings['setup'] = time.time() - setup_start
    
    try:
        # Build command for the PII analyzer
        cmd = ["python", "-m", "src.cli", "analyze", 
               "-i", file_path, 
               "-o", temp_output, 
               "-f", "json", 
               "-t", str(threshold)]
        
        # Add optional parameters
        if entities:
            cmd.extend(["-e", ",".join(entities)])
        if force_ocr:
            cmd.append("--ocr")
        if ocr_dpi != 300:
            cmd.extend(["--ocr-dpi", str(ocr_dpi)])
        
        # Always set OCR threads to 1 for better system-wide parallelism
        cmd.extend(["--ocr-threads", "1"])
        
        if max_pages is not None:
            cmd.extend(["--max-pages", str(max_pages)])
        
        # Log the command if in debug mode
        if debug:
            logger.debug(f"Worker {worker_id} running command: {' '.join(cmd)}")
        
        # Start timing execution phase
        execution_start = time.time()
        
        # Run the CLI tool with full output capture and timeout
        process = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=300  # 5 minute timeout per file
        )
        
        # Record execution time
        timings['execution'] = time.time() - execution_start
        
        # Start timing result processing phase
        result_processing_start = time.time()
        
        # Check if process returned an error
        if process.returncode != 0:
            error_msg = process.stderr or "Unknown error (no stderr)"
            if debug:
                logger.error(f"Worker {worker_id} command failed: {error_msg}")
            
            # Record result processing time
            timings['result_processing'] = time.time() - result_processing_start
            
            # Calculate memory usage
            end_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            memory_delta = end_memory - start_memory
            
            # Return minimal result with error info and performance metrics
            return {
                'file_path': file_path,
                'success': False,
                'error_message': error_msg,
                'entities': [],
                'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                'processing_time': time.time() - start_time,
                'timings': timings,
                'memory_usage_mb': memory_delta
            }
        
        # Check if output file exists and has content
        if not os.path.exists(temp_output) or os.path.getsize(temp_output) == 0:
            error_msg = "No output file created or file is empty"
            if debug:
                logger.error(f"Worker {worker_id}: {error_msg}")
            
            # Record result processing time
            timings['result_processing'] = time.time() - result_processing_start
            
            # Calculate memory usage
            end_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            memory_delta = end_memory - start_memory
            
            # Return minimal result with error info and performance metrics
            return {
                'file_path': file_path,
                'success': False,
                'error_message': error_msg,
                'entities': [],
                'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                'processing_time': time.time() - start_time,
                'timings': timings,
                'memory_usage_mb': memory_delta
            }
        
        # Try to read the JSON output
        try:
            with open(temp_output, 'r') as f:
                result_data = json.load(f)
            
            # Record result processing time
            timings['result_processing'] = time.time() - result_processing_start
            
            # Calculate overall processing time
            processing_time = time.time() - start_time
            
            # Calculate memory usage
            end_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            memory_delta = end_memory - start_memory
            
            # Gather more detailed metrics if available
            metadata = result_data.get('metadata', {})
            metadata.update({
                'process_stats': {
                    'timings': timings,
                    'memory_usage_mb': memory_delta,
                    'pid': os.getpid(),
                    'worker_id': worker_id
                }
            })
            
            # Build consistent result format with performance metrics
            result = {
                'file_path': file_path,
                'success': True,
                'entities': result_data.get('entities', []),
                'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                'processing_time': processing_time,
                'text_length': result_data.get('text_length', 0),
                'metadata': metadata,
                'timings': timings,
                'memory_usage_mb': memory_delta
            }
            
            return result
            
        except Exception as e:
            error_msg = f"Error processing results: {str(e)}"
            logger.error(f"Worker {worker_id}: {error_msg}")
            
            # Record result processing time
            timings['result_processing'] = time.time() - result_processing_start
            
            # Calculate memory usage
            end_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            memory_delta = end_memory - start_memory
            
            # Return minimal result with error info and performance metrics
            return {
                'file_path': file_path,
                'success': False,
                'error_message': error_msg,
                'entities': [],
                'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                'processing_time': time.time() - start_time,
                'timings': timings,
                'memory_usage_mb': memory_delta
            }
    
    except subprocess.TimeoutExpired:
        error_msg = "Processing timeout (exceeded 5 minutes)"
        logger.error(f"Worker {worker_id}: {error_msg} for {file_path}")
        
        # Calculate memory usage
        end_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
        memory_delta = end_memory - start_memory
        
        # Return minimal result with error info and performance metrics
        return {
            'file_path': file_path,
            'success': False,
            'error_message': error_msg,
            'entities': [],
            'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
            'processing_time': time.time() - start_time,
            'timings': timings,
            'memory_usage_mb': memory_delta
        }
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"Worker {worker_id}: {error_msg}")
        
        # Calculate memory usage
        try:
            end_memory = psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
            memory_delta = end_memory - start_memory
        except:
            memory_delta = 0
        
        # Return minimal result with error info and performance metrics
        return {
            'file_path': file_path,
            'success': False,
            'error_message': error_msg,
            'entities': [],
            'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
            'processing_time': time.time() - start_time,
            'timings': timings,
            'memory_usage_mb': memory_delta
        }
    
    finally:
        # Start timing cleanup phase
        cleanup_start = time.time()
        
        # Clean up the temporary output file
        try:
            if os.path.exists(temp_output):
                os.unlink(temp_output)
        except:
            pass
        
        # Record cleanup time
        timings['cleanup'] = time.time() - cleanup_start 