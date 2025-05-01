#!/usr/bin/env python3
"""
Test script to diagnose why DOCX files are failing to process.
This samples a few DOCX files from the error list and tries to extract text from them.
"""

import os
import sys
import json
import traceback
from typing import List, Set, Tuple

# Import the extractor directly from the project
try:
    from src.extractors.extractor_factory import ExtractorFactory
    from src.utils.logger import app_logger as logger
    logger.setLevel("DEBUG")  # Set logger to debug level
except ImportError:
    print("Error: Could not import PII analyzer modules.")
    print("Make sure you're running this script from the project root directory.")
    sys.exit(1)

def get_error_docx_files(json_file: str, sample_size: int = 5) -> List[str]:
    """Get a sample of DOCX files that failed processing."""
    # Load the analysis results
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    # Extract all the successfully processed files
    successful_files = set()
    for result in data.get('results', []):
        file_path = result.get('file_path', '')
        if file_path:
            successful_files.add(file_path)
    
    # Find all docx files
    docx_files = []
    for root, _, files in os.walk('docs'):
        for file in files:
            if file.lower().endswith('.docx'):
                docx_files.append(os.path.join(root, file))
    
    # Find docx files that weren't successfully processed
    error_docx_files = [f for f in docx_files if f not in successful_files]
    
    return error_docx_files[:sample_size]

def test_extract_docx(file_path: str) -> Tuple[bool, str, dict]:
    """Try to extract text from a DOCX file and return details."""
    try:
        print(f"Testing extraction for: {file_path}")
        
        # Initialize extractor
        extractor = ExtractorFactory()
        
        # Extract text
        print("  Extracting text...")
        text, metadata = extractor.extract_text(file_path)
        
        # Check if text was extracted
        if not text:
            return False, "No text extracted (empty result)", metadata
        
        # Get text length
        text_length = len(text)
        print(f"  Successfully extracted {text_length} characters")
        
        # Success
        return True, f"Successfully extracted {text_length} characters", metadata
    
    except Exception as e:
        # Capture the full exception details
        exc_type, exc_value, exc_traceback = sys.exc_info()
        error_details = ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback))
        return False, f"Error: {str(e)}\n{error_details}", {}

def main():
    """Main function to test DOCX extraction."""
    # Get a sample of DOCX files that failed
    sample_files = get_error_docx_files('full_pii_analysis.json', sample_size=5)
    
    if not sample_files:
        print("No error DOCX files found for testing.")
        return
    
    print(f"Testing {len(sample_files)} DOCX files that failed processing:\n")
    
    # Test each file
    results = []
    for file_path in sample_files:
        success, message, metadata = test_extract_docx(file_path)
        results.append({
            "file": file_path,
            "success": success,
            "message": message,
            "metadata": metadata
        })
        print(f"  Result: {'✓' if success else '✗'} {message}\n")
    
    # Print summary
    print("\nSummary:")
    print(f"  Total files tested: {len(results)}")
    print(f"  Successful extractions: {sum(1 for r in results if r['success'])}")
    print(f"  Failed extractions: {sum(1 for r in results if not r['success'])}")
    
    # Print error patterns
    if any(not r['success'] for r in results):
        print("\nError patterns:")
        error_messages = [r['message'] for r in results if not r['success']]
        unique_errors = set()
        
        for error in error_messages:
            # Try to extract just the main error message (first line)
            main_error = error.split('\n')[0]
            unique_errors.add(main_error)
        
        for error in unique_errors:
            print(f"  - {error}")

if __name__ == "__main__":
    main() 