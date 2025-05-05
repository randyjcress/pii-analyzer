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
import secrets
from datetime import datetime
import threading
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from flask import Flask, render_template, jsonify, request, abort, send_from_directory, redirect, url_for, session

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

# Configuration
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE='Lax',
    PERMANENT_SESSION_LIFETIME=86400,  # 24 hours
)

# Global variables
password_required = False
dashboard_password = None

# Global cache for dashboard data
cache = {
    'last_update': 0,
    'dashboard_data': {},
    'job_id': None,
    'high_risk_files': {},
    'db_path': None,
    'refresh_interval': 30  # seconds
}

# Check if user is authenticated
def is_authenticated():
    """Check if the user is authenticated"""
    # If no password is required, always return True
    if not password_required:
        return True
    
    # Otherwise, check if the user is logged in
    return session.get('authenticated', False)

# Authentication routes
@app.route('/login', methods=['GET', 'POST'])
def login():
    """Handle login requests"""
    # If no password is required, redirect to dashboard
    if not password_required:
        return redirect(url_for('index'))
    
    error = None
    
    # Handle login form submission
    if request.method == 'POST':
        if request.form.get('password') == dashboard_password:
            session['authenticated'] = True
            session.permanent = True
            return redirect(url_for('index'))
        else:
            error = 'Invalid password'
    
    # Render login page
    return render_template('login.html', error=error)

@app.route('/logout')
def logout():
    """Handle logout requests"""
    session.pop('authenticated', None)
    return redirect(url_for('login'))

# Middleware to check authentication
@app.before_request
def check_auth():
    """Check authentication before processing requests"""
    # Skip authentication for static files and login page
    if request.path.startswith('/static/') or request.path == '/login':
        return None
    
    # If authentication is required and user is not authenticated, redirect to login
    if password_required and not is_authenticated():
        # Allow API calls with password in header
        if request.path.startswith('/api/'):
            auth_header = request.headers.get('Authorization')
            if auth_header and auth_header == f"Bearer {dashboard_password}":
                return None
            return jsonify({'error': 'Authentication required', 'status': 'error'}), 401
        
        return redirect(url_for('login'))
    
    return None

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
    return render_template('index.html', password_required=password_required)

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
        logger.info(f"Running error analysis on database: {db_path}")
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
        logger.info(f"Found {error_data['total_errors']} total errors")
        logger.info(f"Parsed {len(error_data['categories'])} categories and {len(error_data['extensions'])} extensions")
        
        # Log each category and extension for debugging
        for category in error_data['categories']:
            logger.debug(f"Category: {category['name']}, Count: {category['count']}, Percentage: {category['percentage']}")
        
        for extension in error_data['extensions']:
            logger.debug(f"Extension: {extension['extension']}, Count: {extension['count']}, Percentage: {extension['percentage']}")
        
        # Validate the parsed data
        if error_data['total_errors'] > 0 and (not error_data['categories'] or not error_data['extensions']):
            logger.warning(f"Parsing issue: Found {error_data['total_errors']} errors but no categories or extensions")
            # Add raw output for debugging in case of issues
            error_data['_debug'] = {
                'raw_output': output,
                'parsing_warning': 'Found errors but no categories or extensions'
            }
        
        return jsonify({
            'status': 'success',
            'error_analysis': error_data,
            'meta': {
                'database': db_path,
                'timestamp': datetime.now().isoformat(),
                'categories_count': len(error_data['categories']),
                'extensions_count': len(error_data['extensions']),
                'samples_count': len(error_data['samples'])
            }
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
        
        # Parse the output and include parsing details
        error_data = parse_error_analysis_output(output)
        
        # Add parsing markers for debugging
        parsed_lines = []
        for i, line in enumerate(output.split('\n')):
            if line.strip():
                line_type = "unknown"
                if "Total error files:" in line:
                    line_type = "total_count"
                elif "Error Categories:" in line:
                    line_type = "category_header"
                elif "File Extensions with Errors:" in line:
                    line_type = "extension_header"
                elif "Sample Error Messages by Category:" in line:
                    line_type = "samples_header"
                elif line.strip().startswith("  ") and not line.strip().startswith("    "):
                    if ":" in line:
                        if any(c in line for c in ["(", ")"]):
                            if "File Extensions with Errors:" in output.split('\n')[0:i]:
                                line_type = "extension_line"
                            else:
                                line_type = "category_line"
                        elif line.strip().endswith(":"):
                            line_type = "sample_category"
                
                parsed_lines.append({"line": line, "type": line_type, "line_num": i+1})
        
        return jsonify({
            'status': 'success',
            'raw_output': output,
            'db_path': db_path,
            'parsed_data': error_data,
            'parsing_details': parsed_lines
        })
    except Exception as e:
        logger.error(f"Error in debug endpoint: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500

@app.route('/api/error_analysis_direct')
def api_error_analysis_direct():
    """Direct test endpoint to analyze error files and return parsed data"""
    db_path = request.args.get('db_path', os.environ.get('PII_DB_PATH', 'pii_results.db'))
    
    try:
        # Connect to database
        logger.info(f"Direct error analysis test on database: {db_path}")
        db = get_database(db_path)
        conn = db.conn
        
        # Capture stdout to get the text output
        import io
        from contextlib import redirect_stdout
        
        with io.StringIO() as buf, redirect_stdout(buf):
            inspect_db.analyze_error_files(conn)
            output = buf.getvalue()
        
        # Parse the output
        error_data = parse_error_analysis_output(output)
        
        # Add raw output for debugging
        error_data['_raw_output'] = output
        
        # Log detail of what we're parsing
        logger.info(f"Parsed error data with {len(error_data['categories'])} categories and {len(error_data['extensions'])} extensions")
        
        # Debug logging for categories
        for category in error_data['categories']:
            logger.info(f"Category: {category['name']}, Count: {category['count']}")
        
        # Debug logging for extensions
        for extension in error_data['extensions']:
            logger.info(f"Extension: {extension['extension']}, Count: {extension['count']}")
        
        return jsonify({
            'status': 'success',
            'error_analysis': error_data
        })
    except Exception as e:
        logger.error(f"Error in direct error analysis: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500

@app.route('/error_analysis_test')
def error_analysis_test_page():
    """Test page to display error analysis results directly"""
    db_path = request.args.get('db_path', os.environ.get('PII_DB_PATH', 'pii_results.db'))
    
    try:
        # Connect to database and run analysis
        db = get_database(db_path)
        conn = db.conn
        
        # Capture stdout
        import io
        from contextlib import redirect_stdout
        
        with io.StringIO() as buf, redirect_stdout(buf):
            inspect_db.analyze_error_files(conn)
            output = buf.getvalue()
        
        # Parse the output
        error_data = parse_error_analysis_output(output)
        
        # Build HTML directly
        html = "<html><head><title>Error Analysis Test</title>"
        html += "<link href='https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css' rel='stylesheet'>"
        html += "<script src='https://cdn.jsdelivr.net/npm/chart.js@3.7.0/dist/chart.min.js'></script>"
        html += "</head><body>"
        html += "<div class='container mt-4'>"
        
        # Header
        html += "<h1>Error Analysis Test</h1>"
        html += f"<p>Database: {db_path}</p>"
        
        # Total errors
        html += f"<div class='alert alert-info'>Total error files: {error_data['total_errors']}</div>"
        
        # Categories
        html += "<h2>Error Categories</h2>"
        if error_data['categories']:
            html += "<div class='row'>"
            html += "<div class='col-md-6'>"
            html += "<canvas id='categoriesChart' width='400' height='300'></canvas>"
            html += "</div>"
            html += "<div class='col-md-6'>"
            html += "<table class='table table-striped'>"
            html += "<thead><tr><th>Category</th><th>Count</th><th>Percentage</th></tr></thead>"
            html += "<tbody>"
            for category in error_data['categories']:
                html += f"<tr><td>{category['name']}</td><td>{category['count']}</td><td>{category['percentage']}%</td></tr>"
            html += "</tbody></table>"
            html += "</div>"
            html += "</div>"
        else:
            html += "<div class='alert alert-warning'>No category data available</div>"
        
        # Extensions
        html += "<h2 class='mt-4'>File Extensions with Errors</h2>"
        if error_data['extensions']:
            html += "<div class='row'>"
            html += "<div class='col-md-6'>"
            html += "<canvas id='extensionsChart' width='400' height='300'></canvas>"
            html += "</div>"
            html += "<div class='col-md-6'>"
            html += "<table class='table table-striped'>"
            html += "<thead><tr><th>Extension</th><th>Count</th><th>Percentage</th></tr></thead>"
            html += "<tbody>"
            for ext in error_data['extensions']:
                html += f"<tr><td>{ext['extension']}</td><td>{ext['count']}</td><td>{ext['percentage']}%</td></tr>"
            html += "</tbody></table>"
            html += "</div>"
            html += "</div>"
        else:
            html += "<div class='alert alert-warning'>No extension data available</div>"
        
        # Samples
        html += "<h2 class='mt-4'>Error Samples</h2>"
        if error_data['samples']:
            for category, samples in error_data['samples'].items():
                html += f"<h3 class='mt-3'>{category}</h3>"
                html += "<ul class='list-group'>"
                for sample in samples[:3]:  # Limit to 3 samples per category
                    html += "<li class='list-group-item'>"
                    html += f"<div><strong>File:</strong> {sample['file_path']}</div>"
                    html += f"<div class='text-danger'>Error: {sample['error'] or 'No error message'}</div>"
                    html += "</li>"
                html += "</ul>"
        else:
            html += "<div class='alert alert-warning'>No error samples available</div>"
        
        # Raw output section
        html += "<h2 class='mt-4'>Raw Output</h2>"
        html += "<pre class='bg-light p-3'>" + output + "</pre>"
        
        # JavaScript for charts
        html += """
        <script>
        document.addEventListener('DOMContentLoaded', function() {
            // Categories chart
            if (document.getElementById('categoriesChart')) {
                const categoryLabels = [];
                const categoryData = [];
                const categoryColors = [
                    '#4e73df', '#1cc88a', '#36b9cc', '#f6c23e', '#e74a3b', 
                    '#6f42c1', '#5a5c69', '#858796', '#f8f9fc', '#d1d3e2'
                ];
        """
        
        # Add category data
        if error_data['categories']:
            for i, category in enumerate(error_data['categories']):
                html += f"categoryLabels.push('{category['name']}');\n"
                html += f"categoryData.push({category['count']});\n"
        
        html += """
                new Chart(document.getElementById('categoriesChart'), {
                    type: 'pie',
                    data: {
                        labels: categoryLabels,
                        datasets: [{
                            data: categoryData,
                            backgroundColor: categoryColors
                        }]
                    },
                    options: {
                        responsive: true,
                        plugins: {
                            legend: {
                                position: 'right'
                            }
                        }
                    }
                });
            }
            
            // Extensions chart
            if (document.getElementById('extensionsChart')) {
                const extLabels = [];
                const extData = [];
                const extColors = [
                    '#4e73df', '#1cc88a', '#36b9cc', '#f6c23e', '#e74a3b', 
                    '#6f42c1', '#5a5c69', '#858796', '#f8f9fc', '#d1d3e2'
                ];
        """
        
        # Add extension data
        if error_data['extensions']:
            for i, ext in enumerate(sorted(error_data['extensions'], key=lambda x: x['count'], reverse=True)[:10]):
                html += f"extLabels.push('{ext['extension']}');\n"
                html += f"extData.push({ext['count']});\n"
        
        html += """
                new Chart(document.getElementById('extensionsChart'), {
                    type: 'bar',
                    data: {
                        labels: extLabels,
                        datasets: [{
                            label: 'Error Count',
                            data: extData,
                            backgroundColor: extColors
                        }]
                    },
                    options: {
                        indexAxis: 'y',
                        responsive: true
                    }
                });
            }
        });
        </script>
        """
        
        html += "</div></body></html>"
        
        return html
        
    except Exception as e:
        logger.error(f"Error in test page: {str(e)}", exc_info=True)
        return f"<h1>Error</h1><p>{str(e)}</p>"

@app.route('/api/config')
def api_config():
    """API endpoint to get server configuration"""
    db_path = os.environ.get('PII_DB_PATH', 'pii_results.db')
    
    return jsonify({
        'status': 'success',
        'db_path': db_path,
        'auth_required': password_required,
        'server_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    })

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
            # Parse category lines like "  category_name: 123 (45.6%)"
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
                        logger.debug(f"Parsed category: {category_name}, count: {count}, percentage: {percentage}")
            except (ValueError, IndexError) as e:
                logger.warning(f"Error parsing category line '{line}': {e}")
        
        elif current_section == "extensions" and line.startswith("  "):
            # Parse extension lines like "  .ext: 123 (45.6%)"
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
                        logger.debug(f"Parsed extension: {extension}, count: {count}, percentage: {percentage}")
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

@app.route('/static/<path:path>')
def send_static(path):
    """Serve static files"""
    return send_from_directory(app.static_folder, path)

@app.route('/api/test_error_analysis')
def api_test_error_analysis():
    """Test endpoint to run error analysis parsing directly on a database file"""
    db_path = request.args.get('db_path', os.environ.get('PII_DB_PATH', 'pii_results.db'))
    
    try:
        # Connect to database
        logger.info(f"Testing error analysis on database: {db_path}")
        db = get_database(db_path)
        
        # Create a custom connection with row factory
        conn = db.conn
        
        # Capture stdout to get the text output
        import io
        from contextlib import redirect_stdout
        
        with io.StringIO() as buf, redirect_stdout(buf):
            inspect_db.analyze_error_files(conn)
            raw_output = buf.getvalue()
        
        # Parse the output
        parsed_data = parse_error_analysis_output(raw_output)
        
        # Add line-by-line parsing information for debugging
        lines = raw_output.split('\n')
        lines_with_type = []
        
        for i, line in enumerate(lines):
            line_type = "unknown"
            if not line.strip():
                line_type = "blank"
            elif "Total error files:" in line:
                line_type = "total_count"
            elif "Error Categories:" in line:
                line_type = "category_header"
            elif "File Extensions with Errors:" in line:
                line_type = "extension_header"
            elif "Sample Error Messages by Category:" in line:
                line_type = "samples_header"
            elif line.strip().startswith("  "):
                if ":" in line:
                    # Is this a category or extension line?
                    value_part = line.split(":", 1)[1].strip() if ":" in line else ""
                    if " (" in value_part and ")" in value_part:
                        # Look at surrounding context to determine type
                        context_before = "\n".join(lines[max(0, i-10):i])
                        if "Error Categories:" in context_before and "File Extensions with Errors:" not in context_before:
                            line_type = "category_line"
                        elif "File Extensions with Errors:" in context_before:
                            line_type = "extension_line"
                elif line.strip().endswith(":") and not line.strip().startswith("    "):
                    line_type = "sample_category"
            
            lines_with_type.append({
                "line_num": i+1,
                "content": line,
                "type": line_type
            })
        
        # Return detailed debugging information
        return jsonify({
            'status': 'success',
            'db_path': db_path,
            'raw_output': raw_output,
            'parsed_data': parsed_data,
            'total_lines': len(lines),
            'parsing_details': lines_with_type,
            'categories_count': len(parsed_data['categories']),
            'extensions_count': len(parsed_data['extensions']),
            'samples_count': len(parsed_data['samples'])
        })
    except Exception as e:
        logger.error(f"Error testing error analysis: {str(e)}", exc_info=True)
        return jsonify({
            'error': str(e),
            'status': 'error'
        }), 500

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='PII Analysis Dashboard')
    parser.add_argument('--db-path', type=str, help='Path to the PII database file')
    parser.add_argument('--port', type=int, default=5000, help='Port to run the dashboard on')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    parser.add_argument('--password', type=str, help='Set a password for dashboard access')
    return parser.parse_args()

def main():
    """Run the Flask application"""
    global dashboard_password, password_required
    
    args = parse_args()
    
    # Set password if provided
    if args.password:
        dashboard_password = args.password
        password_required = True
        logger.info("Password protection enabled")
    
    # Generate a secret key for sessions
    app.secret_key = secrets.token_hex(16)
    
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