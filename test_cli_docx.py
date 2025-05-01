#!/usr/bin/env python3
"""
Test script to diagnose why DOCX files are failing in the enhanced_cli.py script.
This script simulates the exact command sequence used in enhanced_cli.py.
"""

import os
import sys
import json
import subprocess
import tempfile
from typing import List, Dict, Tuple

def test_docx_with_cli(file_path: str) -> Tuple[bool, str, Dict]:
    """Test a DOCX file using the same CLI command that enhanced_cli.py uses."""
    print(f"Testing CLI extraction for: {file_path}")
    
    # Create a temporary output file for results
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp:
        temp_output = tmp.name
    
    try:
        # Build the same command that enhanced_cli.py uses
        cmd = [
            "python", "-m", "src.cli", "analyze", 
            "-i", file_path, 
            "-o", temp_output, 
            "-f", "json", 
            "-t", "0.7"
        ]
        
        # Run the command with full output capture
        print("  Running command:", " ".join(cmd))
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        # Check process result
        if process.returncode != 0:
            return False, f"Command failed with code {process.returncode}: {process.stderr}", {}
            
        # Check if output file was created
        if not os.path.exists(temp_output):
            return False, "No output file was created", {}
            
        # Check file size
        file_size = os.path.getsize(temp_output)
        if file_size == 0:
            return False, "Output file is empty", {}
            
        # Load and check the result
        try:
            with open(temp_output, 'r') as f:
                result = json.load(f)
                
            text_length = result.get('text_length', 0)
            entities = result.get('entities', [])
            
            return True, f"Success - extracted {text_length} chars, found {len(entities)} entities", result
            
        except json.JSONDecodeError:
            with open(temp_output, 'r') as f:
                content = f.read()
            return False, f"Invalid JSON in output file: {content[:100]}...", {}
            
    except Exception as e:
        return False, f"Error: {str(e)}", {}
        
    finally:
        # Clean up temp file
        if os.path.exists(temp_output):
            os.remove(temp_output)

def main():
    """Test a sample of DOCX files."""
    # Sample some DOCX files that previously failed
    sample_files = [
        "docs/extracted/karlynd/FHAP Training Plan 2021.docx",
        "docs/extracted/karlynd/5.25.23 Staff Meeting Agenda.docx",
        "docs/extracted/karlynd/Confidentiality Form.docx",
        "docs/extracted/karlynd/Time Management.docx",
        "docs/extracted/karlynd/Staff Meeting 3-2.docx"
    ]
    
    # Verify the files exist
    existing_files = [f for f in sample_files if os.path.exists(f)]
    if not existing_files:
        print("None of the sample files exist.")
        return
    
    print(f"Testing {len(existing_files)} DOCX files with CLI command:\n")
    
    # Test each file
    results = []
    for file_path in existing_files:
        success, message, data = test_docx_with_cli(file_path)
        results.append({
            "file": file_path,
            "success": success,
            "message": message
        })
        print(f"  Result: {'✓' if success else '✗'} {message}\n")
    
    # Print summary
    print("\nSummary:")
    print(f"  Total files tested: {len(results)}")
    print(f"  Successful CLI extractions: {sum(1 for r in results if r['success'])}")
    print(f"  Failed CLI extractions: {sum(1 for r in results if not r['success'])}")
    
    # Print error patterns
    if any(not r['success'] for r in results):
        print("\nError patterns:")
        error_messages = [r['message'] for r in results if not r['success']]
        for i, error in enumerate(set(error_messages)):
            print(f"  {i+1}. {error}")

if __name__ == "__main__":
    main() 