#!/usr/bin/env python3
"""
PII Analysis Dashboard

A Flask web application to visualize PII analysis progress and results
"""

import os
import json
import time
import logging
import argparse
from datetime import datetime
import threading
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from flask import Flask, render_template, jsonify, request, abort, send_from_directory

# Add project root to path
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import PII analysis modules
from src.database.db_utils import get_database
from src.database.db_reporting import (
    get_file_processing_stats,
    get_processing_time_stats,
    get_file_type_statistics,
    get_entity_statistics
)
from strict_nc_breach_pii import (
    analyze_pii_database,
    generate_executive_summary,
    ENTITY_DISPLAY_NAMES
)
import inspect_db

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('pii_dashboard')

# Initialize Flask app
app = Flask(__name__, 
            static_folder=os.path.join(os.path.dirname(__file__), 'static'),
            template_folder=os.path.join(os.path.dirname(__file__), 'templates'))

# Global cache for dashboard data
cache = {
    'last_update': 0,
    'dashboard_data': {},
    'job_id': None,
    'high_risk_files': {},
    'db_path': None,
    'refresh_interval': 30  # seconds
}

def load_dashboard_data(db_path: str, job_id: Optional[int] = None, force_refresh: bool = False) -> Dict[str, Any]:
    """
    Load all data needed for the dashboard from the database.
    
    Args:
        db_path: Path to the database file
        job_id: Specific job ID to analyze (most recent if None)
        force_refresh: Whether to force a refresh of the cache
        
    Returns:
        Dictionary with dashboard data
    """
    current_time = time.time()
    
    # Use cached data if it's recent enough and job_id matches
    if (not force_refresh and 
        cache['dashboard_data'] and 
        current_time - cache['last_update'] < cache['refresh_interval'] and
        cache['job_id'] == job_id and
        cache['db_path'] == db_path):
        return cache['dashboard_data']
    
    try:
        # Connect to database
        db = get_database(db_path)
        
        # Get job ID if not provided
        if job_id is None:
            jobs = db.get_all_jobs()
            if not jobs:
                return {
                    'error': f"No jobs found in database: {db_path}",
                    'status': 'error'
                }
            job_id = jobs[0]['job_id']  # Get most recent job
            
        # Store job_id and db_path in cache
        cache['job_id'] = job_id
        cache['db_path'] = db_path
        
        # Get job information
        job = db.get_job(job_id)
        if not job:
            return {
                'error': f"Job ID {job_id} not found in database: {db_path}",
                'status': 'error'
            }
        
        # Get file processing statistics
        processing_stats = get_file_processing_stats(db_path, job_id)
        
        # Get time statistics and estimates
        time_stats = get_processing_time_stats(db_path, job_id)
        
        # Get file type statistics
        file_types = get_file_type_statistics(db_path, job_id)
        
        # Get entity type statistics
        entity_stats = get_entity_statistics(db_path, job_id)
        
        # Format entity stats for display
        entity_display = []
        for entity_type, count in entity_stats.items():
            entity_display.append({
                'type': entity_type,
                'display_name': ENTITY_DISPLAY_NAMES.get(entity_type, entity_type),
                'count': count
            })
        
        # Sort by count descending
        entity_display.sort(key=lambda x: x['count'], reverse=True)
        
        # Calculate progress percentage
        total_files = processing_stats.get('total_registered', 0)
        completed = processing_stats.get('completed', 0) + processing_stats.get('error', 0)
        progress_percent = (completed / total_files * 100) if total_files > 0 else 0
        
        # Calculate error rate
        error_files = processing_stats.get('error', 0)
        error_rate = (error_files / total_files * 100) if total_files > 0 else 0
        
        # Get high risk files data - run breach notification analysis
        # Note: This is a potentially expensive operation, so we'll cache it
        if force_refresh or 'high_risk_files' not in cache or not cache['high_risk_files']:
            try:
                high_risk_files = analyze_pii_database(db_path, job_id)
                cache['high_risk_files'] = high_risk_files
                
                # Generate executive summary
                executive_summary = generate_executive_summary(
                    high_risk_files,
                    db_path=db_path,
                    job_id=job_id
                )
            except Exception as e:
                logger.error(f"Error analyzing PII database: {e}")
                high_risk_files = {}
                executive_summary = f"Error generating executive summary: {e}"
        else:
            high_risk_files = cache['high_risk_files']
            
            # Generate executive summary
            executive_summary = generate_executive_summary(
                high_risk_files,
                db_path=db_path,
                job_id=job_id
            )
        
        # Count number of high risk files
        high_risk_count = len(high_risk_files)
        
        # Assemble dashboard data
        dashboard_data = {
            'status': 'success',
            'job': {
                'id': job_id,
                'name': job.get('name', 'Unnamed Job'),
                'status': job.get('status', 'Unknown'),
                'start_time': job.get('start_time', ''),
                'last_updated': job.get('last_updated', '')
            },
            'processing': {
                'total_files': total_files,
                'completed': processing_stats.get('completed', 0),
                'pending': processing_stats.get('pending', 0),
                'processing': processing_stats.get('processing', 0),
                'error': error_files,
                'progress_percent': round(progress_percent, 1),
                'error_rate': round(error_rate, 1)
            },
            'time': {
                'elapsed': time_stats.get('elapsed_time_formatted', '0:00:00'),
                'elapsed_seconds': time_stats.get('elapsed_time_seconds', 0),
                'files_per_hour': time_stats.get('files_per_hour', 0),
                'estimated_completion': time_stats.get('estimated_completion_time', 'Unknown')
            },
            'file_types': [{'type': k, 'count': v} for k, v in sorted(file_types.items(), key=lambda x: x[1], reverse=True)],
            'entity_types': entity_display,
            'high_risk': {
                'count': high_risk_count,
                'files': list(high_risk_files.keys())[:50],  # Limit to 50 for display
                'has_more': len(high_risk_files) > 50
            },
            'executive_summary': executive_summary,
            'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Update cache
        cache['dashboard_data'] = dashboard_data
        cache['last_update'] = current_time
        
        return dashboard_data
        
    except Exception as e:
        logger.error(f"Error loading dashboard data: {e}")
        return {
            'error': str(e),
            'status': 'error'
        }

@app.route('/')
def index():
    """Render the main dashboard page"""
    return render_template('index.html')

@app.route('/api/dashboard')
def api_dashboard():
    """API endpoint for dashboard data"""
    db_path = request.args.get('db_path', os.environ.get('PII_DB_PATH', 'pii_results.db'))
    job_id = request.args.get('job_id')
    force_refresh = request.args.get('refresh', '0') == '1'
    
    # Convert job_id to integer if provided
    if job_id is not None:
        try:
            job_id = int(job_id)
        except ValueError:
            return jsonify({'error': 'Invalid job_id parameter'}), 400
    
    # Load dashboard data
    data = load_dashboard_data(db_path, job_id, force_refresh)
    
    return jsonify(data)

@app.route('/api/jobs')
def api_jobs():
    """API endpoint to get available jobs"""
    db_path = request.args.get('db_path', os.environ.get('PII_DB_PATH', 'pii_results.db'))
    
    try:
        # Connect to database
        db = get_database(db_path)
        
        # Get all jobs
        jobs = db.get_all_jobs()
        
        return jsonify({
            'status': 'success',
            'jobs': jobs
        })
    except Exception as e:
        logger.error(f"Error loading jobs: {e}")
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500

@app.route('/api/error_analysis')
def api_error_analysis():
    """API endpoint to analyze error files"""
    db_path = request.args.get('db_path', os.environ.get('PII_DB_PATH', 'pii_results.db'))
    
    try:
        # Connect to database
        db = get_database(db_path)
        
        # Create a custom connection with row factory
        conn = db.conn
        
        # Capture stdout to get the text output
        import io
        from contextlib import redirect_stdout
        
        with io.StringIO() as buf, redirect_stdout(buf):
            inspect_db.analyze_error_files(conn)
            output = buf.getvalue()
        
        # Parse the output to extract key information
        error_data = parse_error_analysis_output(output)
        
        # Debug logging
        logger.info(f"Error analysis completed for {db_path}")
        logger.debug(f"Error data structure: {error_data}")
        
        return jsonify({
            'status': 'success',
            'error_analysis': error_data,
            'raw_output': output
        })
    except Exception as e:
        logger.error(f"Error analyzing errors: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500

@app.route('/api/error_analysis/debug')
def api_error_analysis_debug():
    """Debug endpoint to get raw error analysis output"""
    db_path = request.args.get('db_path', os.environ.get('PII_DB_PATH', 'pii_results.db'))
    
    try:
        # Connect to database
        db = get_database(db_path)
        
        # Create a custom connection with row factory
        conn = db.conn
        
        # Capture stdout to get the text output
        import io
        from contextlib import redirect_stdout
        
        with io.StringIO() as buf, redirect_stdout(buf):
            inspect_db.analyze_error_files(conn)
            output = buf.getvalue()
        
        return jsonify({
            'status': 'success',
            'raw_output': output,
            'db_path': db_path
        })
    except Exception as e:
        logger.error(f"Error in debug endpoint: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500

def parse_error_analysis_output(output: str) -> Dict[str, Any]:
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
    
    for line in lines:
        line = line.strip()
        
        if not line:
            continue
            
        if "Total error files:" in line:
            parts = line.split(": ")
            if len(parts) == 2:
                try:
                    result['total_errors'] = int(parts[1])
                except ValueError:
                    logger.warning(f"Could not parse total errors from: {line}")
                    result['total_errors'] = 0
        
        elif "Error Categories:" in line:
            current_section = "categories"
        
        elif "File Extensions with Errors:" in line:
            current_section = "extensions"
        
        elif "Sample Error Messages by Category:" in line:
            current_section = "samples"
        
        elif current_section == "categories" and line.startswith("  "):
            # Parse category lines
            parts = line.split(": ")
            if len(parts) == 2:
                category_parts = parts[1].split(" (")
                if len(category_parts) == 2:
                    try:
                        count = int(category_parts[0])
                        percentage = float(category_parts[1].rstrip("%)"))
                        result['categories'].append({
                            'name': parts[0].strip(),
                            'count': count,
                            'percentage': percentage
                        })
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error parsing category line '{line}': {e}")
        
        elif current_section == "extensions" and line.startswith("  "):
            # Parse extension lines
            parts = line.split(": ")
            if len(parts) == 2:
                ext_parts = parts[1].split(" (")
                if len(ext_parts) == 2:
                    try:
                        count_part = ext_parts[0].split(" ")[0]
                        count = int(count_part)
                        percentage = float(ext_parts[1].rstrip("%)"))
                        result['extensions'].append({
                            'extension': parts[0].strip(),
                            'count': count,
                            'percentage': percentage
                        })
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Error parsing extension line '{line}': {e}")
        
        elif current_section == "samples" and line.startswith("  ") and not line.startswith("    "):
            # New category in samples
            if line.endswith(":"):
                current_category = line.strip().rstrip(":")
                result['samples'][current_category] = []
        
        elif current_section == "samples" and current_category and line.startswith("    Sample"):
            # Sample file path
            file_parts = line.replace("Sample", "").split(": ", 1)
            if len(file_parts) > 1:
                file_path = file_parts[1].strip()
                result['samples'][current_category].append({
                    'file_path': file_path,
                    'error': None
                })
        
        elif current_section == "samples" and current_category and line.startswith("      Error:"):
            # Error message for the last sample
            error_parts = line.split(": ", 1)
            if len(error_parts) > 1:
                error_msg = error_parts[1].strip()
                if result['samples'][current_category]:
                    result['samples'][current_category][-1]['error'] = error_msg
    
    # Ensure we have at least empty arrays/objects for all keys
    result.setdefault('categories', [])
    result.setdefault('extensions', [])
    result.setdefault('samples', {})
    
    return result

@app.route('/api/config')
def api_config():
    """API endpoint to get server configuration"""
    db_path = os.environ.get('PII_DB_PATH', 'pii_results.db')
    
    return jsonify({
        'status': 'success',
        'db_path': db_path,
        'server_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

@app.route('/static/<path:path>')
def send_static(path):
    """Serve static files"""
    return send_from_directory(app.static_folder, path)

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='PII Analysis Dashboard')
    parser.add_argument('--db-path', type=str, help='Path to the PII database file')
    parser.add_argument('--port', type=int, default=5000, help='Port to run the dashboard on')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    return parser.parse_args()

def main():
    """Run the Flask application"""
    args = parse_args()
    
    # Set default database path from arguments, environment, or use default
    if args.db_path:
        os.environ['PII_DB_PATH'] = args.db_path
    else:
        os.environ.setdefault('PII_DB_PATH', 'pii_results.db')
    
    # Get port from arguments, environment, or use default
    port = args.port or int(os.environ.get('PORT', 5000))
    
    # Set debug mode
    debug = args.debug or (os.environ.get('FLASK_ENV') == 'development')
    
    # Log startup information
    logger.info(f"Starting PII Analysis Dashboard on port {port}")
    logger.info(f"Using database: {os.environ.get('PII_DB_PATH')}")
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=port, debug=debug)

if __name__ == '__main__':
    main() 