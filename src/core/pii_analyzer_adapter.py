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
from typing import Dict, Any, List, Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('pii_analyzer_adapter')

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
        
    Returns:
        Dictionary with processing results
    """
    start_time = time.time()
    
    # Extract settings with defaults
    threshold = settings.get('threshold', 0.7)
    force_ocr = settings.get('force_ocr', False)
    ocr_dpi = settings.get('ocr_dpi', 300)
    ocr_threads = settings.get('ocr_threads', 0)
    max_pages = settings.get('max_pages')
    entities = settings.get('entities')
    debug = settings.get('debug', False)
    
    # Create a temporary output file
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_file:
        temp_output = tmp_file.name
    
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
        if ocr_threads > 0:
            cmd.extend(["--ocr-threads", str(ocr_threads)])
        if max_pages is not None:
            cmd.extend(["--max-pages", str(max_pages)])
        
        if debug:
            logger.debug(f"Running command: {' '.join(cmd)}")
        
        # Run the CLI tool with full output capture
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        # Check if process returned an error
        if process.returncode != 0:
            error_msg = process.stderr or "Unknown error (no stderr)"
            if debug:
                logger.error(f"Command failed: {error_msg}")
            
            # Return minimal result with error info
            return {
                'file_path': file_path,
                'success': False,
                'error_message': error_msg,
                'entities': [],
                'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                'processing_time': time.time() - start_time
            }
        
        # Check if output file exists and has content
        if not os.path.exists(temp_output) or os.path.getsize(temp_output) == 0:
            error_msg = "No output file created or file is empty"
            if debug:
                logger.error(error_msg)
            
            # Return minimal result with error info
            return {
                'file_path': file_path,
                'success': False,
                'error_message': error_msg,
                'entities': [],
                'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                'processing_time': time.time() - start_time
            }
        
        # Try to read the JSON output
        try:
            with open(temp_output, 'r') as f:
                result_data = json.load(f)
            
            # Calculate processing time
            processing_time = time.time() - start_time
            
            # Build consistent result format
            result = {
                'file_path': file_path,
                'success': True,
                'entities': result_data.get('entities', []),
                'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                'processing_time': processing_time,
                'text_length': result_data.get('text_length', 0),
                'metadata': result_data.get('metadata', {})
            }
            
            return result
            
        except json.JSONDecodeError as e:
            # Try to read raw file content for debugging
            try:
                with open(temp_output, 'r') as f:
                    content = f.read()
                error_msg = f"Invalid JSON: {str(e)}. Content: {content[:100]}..."
            except Exception:
                error_msg = f"Invalid JSON: {str(e)}"
                
            if debug:
                logger.error(f"JSON decode error: {error_msg}")
            
            # Return minimal result with error info
            return {
                'file_path': file_path,
                'success': False,
                'error_message': error_msg,
                'entities': [],
                'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                'processing_time': time.time() - start_time
            }
            
    except Exception as e:
        error_msg = f"Exception: {str(e)}"
        if debug:
            logger.error(f"Exception: {error_msg}")
        
        # Return minimal result with error info
        return {
            'file_path': file_path,
            'success': False,
            'error_message': error_msg,
            'entities': [],
            'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
            'processing_time': time.time() - start_time
        }
    
    finally:
        # Clean up temp file
        if os.path.exists(temp_output):
            try:
                os.remove(temp_output)
            except:
                pass 