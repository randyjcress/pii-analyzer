#!/usr/bin/env python3
"""
Test script for error analysis parsing
"""

import os
import sys
import json
import io
from contextlib import redirect_stdout

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import PII analysis modules
from src.database.db_utils import get_database
import inspect_db

def parse_error_analysis_output(output: str) -> dict:
    """Parse the text output from error analysis into structured data"""
    lines = output.split('\n')
    result = {
        'total_errors': 0,
        'categories': [],
        'extensions': [],
        'samples': {}
    }
    
    current_section = None
    current_category = None
    
    # Debug information
    debug_info = {
        'sections_found': [],
        'category_lines_found': 0,
        'extension_lines_found': 0,
        'sample_categories_found': 0,
        'raw_lines': []
    }
    
    for i, line in enumerate(lines):
        debug_info['raw_lines'].append(f"{i+1}: {line}")
        
        line = line.strip()
        
        if not line:
            continue
            
        if "Total error files:" in line:
            parts = line.split(": ")
            if len(parts) == 2:
                try:
                    result['total_errors'] = int(parts[1])
                    print(f"Found total errors: {parts[1]}")
                except ValueError:
                    print(f"Error parsing total errors from: {line}")
        
        elif "Error Categories:" in line:
            current_section = "categories"
            debug_info['sections_found'].append(f"categories at line {i+1}")
        
        elif "File Extensions with Errors:" in line:
            current_section = "extensions"
            debug_info['sections_found'].append(f"extensions at line {i+1}")
        
        elif "Sample Error Messages by Category:" in line:
            current_section = "samples"
            debug_info['sections_found'].append(f"samples at line {i+1}")
        
        elif current_section == "categories" and line.startswith("  "):
            # Parse category lines like "  category_name: 123 (45.6%)"
            print(f"Processing category line: {line}")
            try:
                # Split by colon to get name and value parts
                parts = line.split(":", 1)
                if len(parts) == 2:
                    category_name = parts[0].strip()
                    value_part = parts[1].strip()
                    
                    # Extract count and percentage
                    count_parts = value_part.split(" (")
                    if len(count_parts) == 2:
                        count = int(count_parts[0].strip())
                        percentage = float(count_parts[1].strip().rstrip("%)"))
                        result['categories'].append({
                            'name': category_name,
                            'count': count,
                            'percentage': percentage
                        })
                        debug_info['category_lines_found'] += 1
                        print(f"Added category: {category_name} - {count} ({percentage}%)")
            except (ValueError, IndexError) as e:
                print(f"Error parsing category line '{line}': {e}")
        
        elif current_section == "extensions" and line.startswith("  "):
            # Parse extension lines like "  .ext: 123 (45.6%)"
            print(f"Processing extension line: {line}")
            try:
                # Split by colon to get extension and value parts
                parts = line.split(":", 1)
                if len(parts) == 2:
                    extension = parts[0].strip()
                    value_part = parts[1].strip()
                    
                    # Extract count and percentage
                    count_parts = value_part.split(" (")
                    if len(count_parts) == 2:
                        count = int(count_parts[0].strip())
                        percentage = float(count_parts[1].strip().rstrip("%)"))
                        result['extensions'].append({
                            'extension': extension,
                            'count': count,
                            'percentage': percentage
                        })
                        debug_info['extension_lines_found'] += 1
                        print(f"Added extension: {extension} - {count} ({percentage}%)")
            except (ValueError, IndexError) as e:
                print(f"Error parsing extension line '{line}': {e}")
        
        elif current_section == "samples" and line.startswith("  ") and not line.startswith("    "):
            # New category in samples
            if line.endswith(":"):
                current_category = line.strip().rstrip(":")
                result['samples'][current_category] = []
                debug_info['sample_categories_found'] += 1
                print(f"Found sample category: {current_category}")
        
        elif current_section == "samples" and current_category and line.startswith("    Sample"):
            # Sample file path
            file_parts = line.replace("Sample", "").split(": ", 1)
            if len(file_parts) > 1:
                file_path = file_parts[1].strip()
                result['samples'][current_category].append({
                    'file_path': file_path,
                    'error': None
                })
                print(f"Added sample file path: {file_path}")
        
        elif current_section == "samples" and current_category and line.startswith("      Error:"):
            # Error message for the last sample
            error_parts = line.split(": ", 1)
            if len(error_parts) > 1:
                error_msg = error_parts[1].strip()
                if result['samples'][current_category]:
                    result['samples'][current_category][-1]['error'] = error_msg
                    print(f"Added error message: {error_msg}")
    
    # Ensure we have at least empty arrays/objects for all keys
    result.setdefault('categories', [])
    result.setdefault('extensions', [])
    result.setdefault('samples', {})
    
    # Add debug information
    result['_debug'] = debug_info
    
    return result

def main():
    """Run the script"""
    if len(sys.argv) < 2:
        print("Usage: python error_analysis_test.py <database_path>")
        sys.exit(1)
    
    db_path = sys.argv[1]
    
    try:
        # Connect to database
        print(f"Connecting to database: {db_path}")
        db = get_database(db_path)
        
        # Create a custom connection with row factory
        conn = db.conn
        
        # Capture stdout to get the text output
        print("Running analyze_error_files...")
        with io.StringIO() as buf, redirect_stdout(buf):
            inspect_db.analyze_error_files(conn)
            output = buf.getvalue()
        
        # Print raw output
        print("\n-------- RAW OUTPUT --------")
        print(output)
        print("----------------------------\n")
        
        # Parse the output and print the result
        print("Parsing output...")
        result = parse_error_analysis_output(output)
        
        # Print result
        print("\n-------- PARSED RESULT --------")
        print(f"Total errors: {result['total_errors']}")
        print(f"Categories found: {len(result['categories'])}")
        print(f"Extensions found: {len(result['extensions'])}")
        print(f"Sample categories found: {len(result['samples'])}")
        print("-------------------------------\n")
        
        # Print debug information
        print("\n-------- DEBUG INFO --------")
        print(f"Sections found: {result['_debug']['sections_found']}")
        print(f"Category lines found: {result['_debug']['category_lines_found']}")
        print(f"Extension lines found: {result['_debug']['extension_lines_found']}")
        print(f"Sample categories found: {result['_debug']['sample_categories_found']}")
        print("---------------------------\n")
        
        # Save result to file
        with open('error_analysis_result.json', 'w') as f:
            json.dump(result, f, indent=2)
        
        print("Result saved to error_analysis_result.json")
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 