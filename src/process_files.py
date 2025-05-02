#!/usr/bin/env python3
"""
Command-line tool for PII Analysis with resumable processing
"""

import os
import sys
import argparse
import logging
import time
from typing import Dict, Any, List, Optional

# Add the project root to path if needed
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Add Rich for better progress visualization
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.table import Table

from src.database.db_utils import get_database
from src.core.file_discovery import (
    scan_directory,
    find_resumption_point,
    reset_stalled_files,
    get_file_statistics
)
from src.core.worker_management import (
    process_files_parallel,
    estimate_completion_time
)
from src.core.pii_analyzer_adapter import analyze_file

# Initialize rich console
console = Console()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('pii_analyzer.log')
    ]
)
logger = logging.getLogger('pii_analyzer')

# Default supported file extensions
DEFAULT_EXTENSIONS = {
    '.txt', '.pdf', '.docx', '.doc', '.rtf',
    '.xlsx', '.xls', '.csv', '.tsv',
    '.pptx', '.ppt',
    '.json', '.xml', '.html', '.htm',
    '.md', '.log'
}

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='PII Analyzer with resumable processing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process a directory and store results in a database
  python src/process_files.py /path/to/documents --db-path results.db
  
  # Resume processing from a previous run
  python src/process_files.py /path/to/documents --db-path results.db --resume
  
  # Process with 8 worker threads
  python src/process_files.py /path/to/documents --workers 8
  
  # Export results to JSON
  python src/process_files.py --db-path results.db --export results.json
  
  # Show job status
  python src/process_files.py --db-path results.db --status
"""
    )
    
    # Basic arguments
    parser.add_argument('directory', nargs='?', help='Directory to scan for files')
    parser.add_argument('--db-path', type=str, default='pii_results.db',
                        help='Path to database file')
    
    # Processing control
    parser.add_argument('--resume', action='store_true',
                        help='Resume processing from last point')
    parser.add_argument('--force-restart', action='store_true',
                        help='Force restart of processing')
    parser.add_argument('--workers', type=int, default=None,
                        help='Number of worker threads (default: auto)')
    parser.add_argument('--batch-size', type=int, default=10,
                        help='Number of files to process in a batch')
    parser.add_argument('--max-files', type=int, default=None,
                        help='Maximum number of files to process')
    
    # File filtering
    parser.add_argument('--extensions', type=str, default=None,
                        help='Comma-separated list of file extensions to process')
    
    # PII analyzer options
    parser.add_argument('--threshold', type=float, default=0.7,
                        help='Confidence threshold (0-1)')
    parser.add_argument('--entities', type=str, default=None,
                        help='Comma-separated list of entities to detect (default: all)')
    parser.add_argument('--ocr', action='store_true',
                        help='Force OCR for text extraction')
    parser.add_argument('--ocr-dpi', type=int, default=300,
                        help='DPI for OCR')
    parser.add_argument('--ocr-threads', type=int, default=0,
                        help='Number of OCR threads (0=auto)')
    parser.add_argument('--max-pages', type=int, default=None,
                        help='Maximum pages to process per PDF')
    
    # Output options
    parser.add_argument('--export', type=str, default=None,
                        help='Export results to JSON file')
    parser.add_argument('--job-id', type=int, default=None,
                        help='Specific job ID to export or show status for')
    parser.add_argument('--status', action='store_true',
                        help='Show job status and exit')
    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose output')
    parser.add_argument('--debug', action='store_true',
                        help='Enable debug mode with detailed logging')
    
    return parser.parse_args()

def show_status(db_path: str, job_id: Optional[int] = None):
    """
    Show status of jobs in the database
    
    Args:
        db_path: Path to the database
        job_id: Specific job ID to show (None for all jobs)
    """
    # Connect to database
    db = get_database(db_path)
    
    # Get job IDs
    if job_id is None:
        # Get all jobs
        jobs = db.get_all_jobs()
        if not jobs:
            console.print("[yellow]No jobs found in the database.[/yellow]")
            return
    else:
        # Get specific job
        job = db.get_job(job_id)
        if not job:
            console.print(f"[bold red]Job {job_id} not found.[/bold red]")
            return
        jobs = [job]
    
    # Display job information
    for job in jobs:
        job_id = job['job_id']
        status = job.get('status', 'unknown')
        status_color = {
            'completed': 'green',
            'running': 'blue',
            'interrupted': 'yellow',
            'error': 'red',
        }.get(status, 'white')
        
        console.print(f"\n[bold]Job {job_id}:[/bold] {job.get('name', 'Unnamed')}")
        console.print(f"Status: [{status_color}]{status}[/{status_color}]")
        console.print(f"Started: {job.get('start_time', 'unknown')}")
        console.print(f"Last updated: {job.get('last_updated', 'unknown')}")
        
        # Get file statistics
        stats = get_file_statistics(db, job_id)
        
        # Create table for file status
        status_table = Table(title="File Status", show_header=True, header_style="bold")
        status_table.add_column("Status", style="cyan")
        status_table.add_column("Count", justify="right", style="green")
        
        # Add rows
        for status, count in stats.get('status_counts', {}).items():
            status_table.add_row(status, str(count))
        
        # If no status counts, add an empty row
        if not stats.get('status_counts'):
            status_table.add_row("No files", "0")
            
        console.print(status_table)
        
        # Create table for file types
        type_table = Table(title="File Types", show_header=True, header_style="bold")
        type_table.add_column("Extension", style="cyan")
        type_table.add_column("Count", justify="right", style="green")
        
        # Add rows sorted by count (descending)
        sorted_types = sorted(stats.get('type_counts', {}).items(), key=lambda x: x[1], reverse=True)
        for ext, count in sorted_types:
            type_table.add_row(ext, str(count))
            
        # If no type counts, add an empty row
        if not sorted_types:
            type_table.add_row("No files", "0")
            
        console.print(type_table)
        
        # Size statistics
        size_stats = stats.get('size_stats', {})
        total_size = size_stats.get('total_size', 0)
        avg_size = size_stats.get('avg_size', 0)
        max_size = size_stats.get('max_size', 0)
        
        size_table = Table(title="Size Statistics", show_header=True, header_style="bold")
        size_table.add_column("Metric", style="cyan")
        size_table.add_column("Value", justify="right", style="green")
        
        if total_size is not None:
            size_table.add_row("Total size", f"{total_size / (1024*1024):.2f} MB")
        else:
            size_table.add_row("Total size", "0.00 MB")
            
        if avg_size is not None:
            size_table.add_row("Average size", f"{avg_size / 1024:.2f} KB")
        else:
            size_table.add_row("Average size", "0.00 KB")
            
        if max_size is not None:
            size_table.add_row("Largest file", f"{max_size / 1024:.2f} KB")
        else:
            size_table.add_row("Largest file", "0.00 KB")
            
        console.print(size_table)
        
        # If job is running, show estimated completion time
        if job.get('status') == 'running':
            estimate = estimate_completion_time(db, job_id)
            
            progress_table = Table(title="Estimated Completion", show_header=True, header_style="bold")
            progress_table.add_column("Metric", style="cyan")
            progress_table.add_column("Value", justify="right", style="green")
            
            progress_table.add_row("Progress", f"{estimate.get('percent_complete', 0):.1f}%")
            progress_table.add_row("Processing rate", f"{estimate.get('processing_rate', 0):.2f} files/sec")
            progress_table.add_row("Est. time remaining", estimate.get('estimated_remaining_time', 'unknown'))
            
            console.print(progress_table)
    
    # Close database
    db.close()

def export_to_json(db_path: str, output_path: str, job_id: Optional[int] = None):
    """
    Export results to JSON file
    
    Args:
        db_path: Path to the database
        output_path: Path to output JSON file
        job_id: Specific job ID to export (None for latest job)
    """
    # Connect to database
    db = get_database(db_path)
    
    # Get job ID if not specified
    if job_id is None:
        jobs = db.get_all_jobs()
        if not jobs:
            console.print("[yellow]No jobs found in the database.[/yellow]")
            return
        # Use the most recent job
        job_id = jobs[0]['job_id']
    
    # Export to JSON
    import json
    data = db.export_to_json(job_id)
    
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    # Get job info for summary
    job = db.get_job(job_id)
    job_stats = db.get_job_statistics(job_id)
    
    # Create summary display
    console.print(f"\n[bold green]Export Complete![/bold green]")
    console.print(f"Exported job [bold blue]{job_id}[/bold blue] to [bold cyan]{output_path}[/bold cyan]")
    
    # Show entity counts if available
    if job_stats.get('entity_types'):
        entity_table = Table(title="PII Entities Exported", show_header=True, header_style="bold")
        entity_table.add_column("Entity Type", style="cyan")
        entity_table.add_column("Count", justify="right", style="green")
        
        for entity_type, count in sorted(job_stats.get('entity_types', {}).items(), 
                                        key=lambda x: x[1], reverse=True):
            entity_table.add_row(entity_type, str(count))
        
        console.print(entity_table)
    
    file_table = Table(title="Export Summary", show_header=True, header_style="bold")
    file_table.add_column("Metric", style="cyan")
    file_table.add_column("Value", justify="right", style="green")
    
    file_table.add_row("Total Files", str(job.get('total_files', 0)))
    file_table.add_row("Processed Files", str(job.get('processed_files', 0)))
    file_table.add_row("Error Files", str(job.get('error_files', 0)))
    file_table.add_row("JSON File Size", f"{os.path.getsize(output_path) / (1024 * 1024):.2f} MB")
    
    console.print(file_table)
    
    # Close database
    db.close()

def process_directory(args):
    """
    Process directory with the specified options
    
    Args:
        args: Command-line arguments
    """
    # Configure logging - adjust based on verbose mode
    if args.verbose:
        # Keep console logging in verbose mode
        pass
    else:
        # In progress bar mode, redirect logs to file only
        for handler in logging.root.handlers[:]:
            if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
                logging.root.removeHandler(handler)
    
    # Connect to database
    db = get_database(args.db_path)
    
    # Parse extensions
    if args.extensions:
        extensions = set('.' + ext.lower().lstrip('.') for ext in args.extensions.split(','))
    else:
        extensions = DEFAULT_EXTENSIONS
    
    # Parse entities if provided
    entity_list = None
    if args.entities:
        entity_list = [e.strip() for e in args.entities.split(',')]
    
    # Prepare analyzer settings
    analyzer_settings = {
        'threshold': args.threshold,
        'entities': entity_list,
        'force_ocr': args.ocr,
        'ocr_dpi': args.ocr_dpi,
        'ocr_threads': args.ocr_threads,
        'max_pages': args.max_pages,
        'debug': args.debug
    }
    
    # Check for resumable job
    if args.resume:
        # Look for an existing job for this directory
        existing_jobs = db.get_jobs_by_metadata('directory', args.directory)
        
        if existing_jobs:
            job_id = existing_jobs[0]['job_id']
            
            # Check if job can be resumed
            info = find_resumption_point(db, job_id)
            
            if info['status'] == 'resumable':
                logger.info(f"Resuming job {job_id}: {info['message']}")
                
                # Reset any stalled files
                reset_count = reset_stalled_files(db, job_id)
                if reset_count > 0:
                    logger.info(f"Reset {reset_count} stalled files to pending status")
            else:
                # Create new job
                job_id = db.create_job(
                    name=f"PII Analysis - {os.path.basename(args.directory)}",
                    metadata={'directory': args.directory}
                )
                logger.info(f"Created new job {job_id} (previous job cannot be resumed: {info['message']})")
                
                # Scan directory
                total, new = scan_directory(
                    args.directory,
                    db,
                    job_id,
                    supported_extensions=extensions
                )
                logger.info(f"Scanned directory: found {total} files, registered {new} new files")
        else:
            # No existing job, create new one
            job_id = db.create_job(
                name=f"PII Analysis - {os.path.basename(args.directory)}",
                metadata={'directory': args.directory}
            )
            logger.info(f"Created new job {job_id} (no existing jobs found)")
            
            # Scan directory
            total, new = scan_directory(
                args.directory,
                db,
                job_id,
                supported_extensions=extensions
            )
            logger.info(f"Scanned directory: found {total} files, registered {new} new files")
    else:
        # Create new job
        job_id = db.create_job(
            name=f"PII Analysis - {os.path.basename(args.directory)}",
            metadata={'directory': args.directory}
        )
        logger.info(f"Created new job {job_id}")
        
        # Scan directory
        total, new = scan_directory(
            args.directory,
            db,
            job_id,
            supported_extensions=extensions
        )
        logger.info(f"Scanned directory: found {total} files, registered {new} new files")
    
    # Update job status to running
    db.update_job_status(job_id, 'running')
    
    # Get job information for progress bar
    job_info = db.get_job(job_id)
    total_files = job_info.get('total_files', 0)
    pending_files = find_resumption_point(db, job_id).get('pending_files', 0)
    
    # Process files
    if args.verbose:
        logger.info(f"Starting processing with {args.workers or 'auto'} workers, batch size {args.batch_size}")
        
        try:
            # Replace mock_process_file with actual PII processing function
            stats = process_files_parallel(
                db,
                job_id,
                analyze_file,
                max_workers=args.workers,
                batch_size=args.batch_size,
                max_files=args.max_files,
                settings=analyzer_settings
            )
            
            # Show results
            logger.info(f"Processing complete: processed {stats['total_processed']} files in {stats['elapsed_time']:.2f}s")
            logger.info(f"Processing rate: {stats['files_per_second']:.2f} files/sec")
            
            # Show job statistics
            if args.verbose:
                show_status(args.db_path, job_id)
        
        except KeyboardInterrupt:
            logger.info("Processing interrupted by user")
            db.update_job_status(job_id, 'interrupted')
    else:
        # Use Rich progress display when not in verbose mode
        console.print(f"[bold]Processing [cyan]{args.directory}[/cyan] with {args.workers or 'auto'} workers[/bold]")
        console.print(f"Found {total_files} files, {pending_files} pending to process")
        
        # Create tracking variables for progress
        processed_count = 0
        error_count = 0
        
        # Process with Rich progress bar
        try:
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                console=console
            ) as progress:
                # Create task for overall progress
                task = progress.add_task("[green]Processing files...", total=pending_files)
                
                # Custom callback to update progress
                def progress_callback(state):
                    nonlocal processed_count, error_count
                    
                    if state.get('type') == 'file_completed':
                        processed_count += 1
                        progress.update(task, completed=processed_count, 
                                       description=f"[green]Processed: [cyan]{processed_count}[/cyan] files")
                    elif state.get('type') == 'file_error':
                        error_count += 1
                        # Don't update progress bar for errors
                
                # Process files with progress callback
                stats = process_files_parallel(
                    db,
                    job_id,
                    analyze_file,
                    max_workers=args.workers,
                    batch_size=args.batch_size,
                    max_files=args.max_files,
                    settings=analyzer_settings,
                    progress_callback=progress_callback
                )
            
            # Show summary after completion
            console.print(f"\n[bold green]Processing complete![/bold green]")
            console.print(f"Processed {stats['total_processed']} files in {stats['elapsed_time']:.2f}s")
            console.print(f"Processing rate: {stats['files_per_second']:.2f} files/sec")
            
            # Show entity counts
            job_stats = db.get_job_statistics(job_id)
            if job_stats.get('entity_types'):
                console.print("\n[bold]PII Entities Found:[/bold]")
                for entity_type, count in job_stats.get('entity_types', {}).items():
                    console.print(f"  {entity_type}: {count}")
        
        except KeyboardInterrupt:
            console.print("\n[bold red]Processing interrupted by user[/bold red]")
            db.update_job_status(job_id, 'interrupted')
    
    # Close database
    db.close()

def main():
    """Main entry point"""
    # Parse arguments
    args = parse_args()
    
    # Handle --status option
    if args.status:
        show_status(args.db_path, args.job_id)
        return
    
    # Handle --export option
    if args.export:
        export_to_json(args.db_path, args.export, args.job_id)
        return
    
    # Make sure directory is specified
    if not args.directory:
        console.print("[bold red]Error:[/bold red] Directory must be specified unless using --status or --export")
        console.print("Run with --help for usage information")
        return
    
    # Verify directory exists
    if not os.path.isdir(args.directory):
        console.print(f"[bold red]Error:[/bold red] Directory not found: {args.directory}")
        return
    
    # Process directory
    process_directory(args)

if __name__ == "__main__":
    main() 