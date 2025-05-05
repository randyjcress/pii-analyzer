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
        
        # Get JSON output directly
        print("Getting JSON output directly...")
        json_data = inspect_db.analyze_error_files(conn, output_format='json')
        
        # Also get text output for display and comparison
        print("Getting text output for comparison...")
        with io.StringIO() as buf, redirect_stdout(buf):
            inspect_db.analyze_error_files(conn, output_format='text')
            text_output = buf.getvalue()
        
        # Print raw output
        print("\n-------- RAW TEXT OUTPUT --------")
        print(text_output)
        print("----------------------------\n")
        
        # Print result
        print("\n-------- JSON RESULT --------")
        print(f"Total errors: {json_data['total_errors']}")
        print(f"Categories found: {len(json_data['categories'])}")
        print(f"Extensions found: {len(json_data['extensions'])}")
        print(f"Sample categories found: {len(json_data['samples'])}")
        print("-------------------------------\n")
        
        # Print debug information
        print("\n-------- DEBUG INFO --------")
        # Print some category details
        for category in json_data['categories'][:3]:  # First 3 categories
            print(f"Category: {category['name']}, Count: {category['count']}, Percentage: {category['percentage']}%")
        
        # Print some extension details
        for ext in json_data['extensions'][:3]:  # First 3 extensions
            print(f"Extension: {ext['extension']}, Count: {ext['count']}, Percentage: {ext['percentage']}%")
        print("---------------------------\n")
        
        # Save result to file
        with open('error_analysis_result.json', 'w') as f:
            json.dump(json_data, f, indent=2)
        
        print("Result saved to error_analysis_result.json")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 