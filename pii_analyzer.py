#!/usr/bin/env python3
"""
PII Analyzer - Main Entry Point
A tool for detecting and analyzing personally identifiable information in documents.
Uses parallel processing by default for improved performance on large document collections.
"""

import sys
import os
import argparse
from fix_enhanced_cli import main as original_sequential_main
from pii_analyzer_parallel import main as parallel_main

def sequential_main():
    """Wrapper around the original sequential main function to add the --sequential option."""
    # Check if this is a help request
    if '--help' in sys.argv or '-h' in sys.argv:
        # Show our custom help message with the sequential option
        parser = argparse.ArgumentParser(description="PII Analysis with Automatic Parallel Processing")
        # Copy all arguments from the original parser
        parser.add_argument("-i", "--input", required=True, help="Input file or directory")
        parser.add_argument("-o", "--output", help="Output JSON file for results")
        parser.add_argument("-e", "--entities", help="Comma-separated list of entities to detect")
        parser.add_argument("-t", "--threshold", type=float, default=0.7, help="Confidence threshold (0-1)")
        parser.add_argument("--ocr", action="store_true", help="Force OCR for text extraction")
        parser.add_argument("--ocr-dpi", type=int, default=300, help="DPI for OCR")
        parser.add_argument("--ocr-threads", type=int, default=0, help="Number of OCR threads (0=auto)")
        parser.add_argument("--max-pages", type=int, help="Maximum pages per PDF")
        parser.add_argument("--sample", type=int, help="Analyze only a sample of files")
        parser.add_argument("--debug", action="store_true", help="Show detailed debug information")
        parser.add_argument("--test-docx", action="store_true", help="Test DOCX files only")
        # Add our new option
        parser.add_argument("--sequential", action="store_true", 
                        help="Force sequential processing for directories (ignored for single files)")
        
        parser.print_help()
        return 0
    
    # For normal execution, just call the original function
    if '--debug' in sys.argv:
        print("[DEBUG] Using sequential processing mode")
    return original_sequential_main()

def main():
    """
    Main entry point for PII Analyzer. Automatically uses parallel processing
    for directories and sequential processing for single files.
    """
    # Create a parser just to extract the input path and sequential flag
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("-i", "--input", help="Input file or directory")
    parser.add_argument("--sequential", action="store_true", help="Force sequential processing even for directories")
    parser.add_argument("--debug", action="store_true", help="Show debug information")
    
    # Parse known args without showing errors for other args
    args, _ = parser.parse_known_args()
    
    if not args.input:
        # If no input specified, use sequential_main to show the full help message
        return sequential_main()
    
    # Determine if we should use parallel processing
    debug = args.debug or '--debug' in sys.argv
    
    if os.path.isdir(args.input) and not args.sequential:
        # Process directory in parallel
        if debug:
            print(f"[DEBUG] Input '{args.input}' is a directory, using parallel processing mode")
        return parallel_main()
    else:
        # Process single file or forced sequential mode
        if debug:
            if os.path.isfile(args.input):
                print(f"[DEBUG] Input '{args.input}' is a file, using sequential processing mode")
            elif args.sequential:
                print(f"[DEBUG] Sequential mode forced, using sequential processing mode")
            else:
                print(f"[DEBUG] Input '{args.input}' not found, using sequential processing mode")
        return sequential_main()

if __name__ == "__main__":
    sys.exit(main()) 