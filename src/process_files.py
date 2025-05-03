#!/usr/bin/env python3
"""
Command-line tool for PII Analysis with resumable processing
"""

import os
import sys
import argparse
import logging
import time
import glob
import re
import multiprocessing
import threading
import signal
import psutil
from typing import Dict, Any, List, Optional

# Add the project root to path if needed
sys.path.append(os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

# Add Rich for better progress visualization
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.table import Table
from rich.prompt import Prompt
from rich.panel import Panel
from rich.live import Live

from src.database.db_utils import get_database
from src.core.file_discovery import (
    scan_directory,
    find_resumption_point,
    reset_stalled_files,
    get_file_statistics
)
from src.core.worker_management import (
    process_files_parallel,
    estimate_completion_time,
    calculate_optimal_workers
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
  
  # Resume a specific job by ID
  python src/process_files.py /path/to/documents --db-path results.db --resume --job-id 123
  
  # Resume without rescanning the directory
  python src/process_files.py /path/to/documents --db-path results.db --resume --skip-scan
  
  # Process with 8 worker processes
  python src/process_files.py /path/to/documents --workers 8
  
  # Export results to JSON
  python src/process_files.py --db-path results.db --export results.json
  
  # Show job status
  python src/process_files.py --db-path results.db --status
  
  # List all jobs for a directory
  python src/process_files.py --db-path results.db --list-jobs /path/to/documents
  
  # Run in detached mode (continue even if terminal closes)
  python src/process_files.py /path/to/documents --detach
  
  # Follow output of a detached process
  python src/process_files.py --follow <pid>
  
  # List all detached processes
  python src/process_files.py --list-detached
  
  # Reset all files in database to pending status (keeps file records)
  python src/process_files.py --db-path results.db --reset-db
"""
    )
    
    # Basic arguments
    parser.add_argument('directory', nargs='?', help='Directory to scan for files')
    parser.add_argument('--db-path', type=str, default='pii_results.db',
                        help='Path to database file')
    
    # Processing control
    parser.add_argument('--resume', action='store_true',
                        help='Resume processing from last point')
    parser.add_argument('--job-id', type=int, default=None,
                        help='Specific job ID to process, resume, export or show status for')
    parser.add_argument('--force-restart', action='store_true',
                        help='Force restart of processing')
    parser.add_argument('--reset-db', action='store_true',
                        help='Reset all files to pending status (keeps file records)')
    parser.add_argument('--skip-scan', action='store_true',
                        help='Skip directory scanning when resuming (use existing files in database)')
    parser.add_argument('--workers', type=int, default=None,
                        help='Number of worker processes (default: auto)')
    parser.add_argument('--batch-size', type=int, default=10,
                        help='Number of files to process in a batch')
    parser.add_argument('--max-files', type=int, default=None,
                        help='Maximum number of files to process')
    parser.add_argument('--detach', action='store_true',
                        help='Detach from terminal and run in the background (survive SSH disconnection)')
    parser.add_argument('--follow', type=str, metavar='PID_OR_TIMESTAMP',
                        help='Follow logs of a detached process (by PID or timestamp)')
    parser.add_argument('--list-detached', action='store_true',
                        help='List all detached processes')
    parser.add_argument('--list-jobs', type=str, metavar='DIRECTORY', 
                        help='List all jobs for a specific directory')
    
    # File filtering
    parser.add_argument('--extensions', type=str, default=None,
                        help='Comma-separated list of file extensions to process')
    parser.add_argument('--file-size-limit', type=int, default=100,
                        help='Maximum file size in MB to process (default: 100MB)')
    
    # PII analyzer options
    parser.add_argument('--threshold', type=float, default=0.7,
                        help='Confidence threshold (0-1)')
    parser.add_argument('--entities', type=str, default=None,
                        help='Comma-separated list of entities to detect (default: all)')
    parser.add_argument('--ocr', action='store_true',
                        help='Force OCR for text extraction')
    parser.add_argument('--ocr-dpi', type=int, default=300,
                        help='DPI for OCR')
    parser.add_argument('--ocr-threads', type=int, default=1,
                        help='Number of OCR threads per file (default: 1)')
    parser.add_argument('--max-ocr', type=int, default=None, 
                        help='Maximum number of concurrent OCR processes (default: auto)')
    parser.add_argument('--max-pages', type=int, default=None,
                        help='Maximum pages to process per PDF')
    
    # Performance monitoring
    parser.add_argument('--monitor', action='store_true',
                        help='Show real-time performance monitoring')
    parser.add_argument('--profile', action='store_true',
                        help='Run with performance profiling and show report at end')
    
    # Output options
    parser.add_argument('--export', type=str, default=None,
                        help='Export results to JSON file')
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
            'unknown': 'magenta'
        }.get(status, 'white')
        
        # Create a table for job details
        table = Table(title=f"Job {job_id} - {job.get('directory', 'unknown')}")
        table.add_column("Property", style="cyan")
        table.add_column("Value")
        
        # Add rows
        table.add_row("Status", f"[{status_color}]{status}[/{status_color}]")
        table.add_row("Start Time", job.get('start_time', 'unknown'))
        table.add_row("Last Update", job.get('last_update', 'unknown'))
        
        if 'file_count' in job:
            table.add_row("Total Files", str(job.get('file_count', 0)))
        
        # Get file statistics
        stats = get_file_statistics(db, job_id)
        completed = stats.get('completed', 0)
        pending = stats.get('pending', 0)
        processing = stats.get('processing', 0)
        error = stats.get('error', 0)
        total = completed + pending + processing + error
        
        table.add_row("Files Completed", f"[green]{completed}[/green] ({completed/total*100:.1f}% of {total})")
        table.add_row("Files Pending", f"[blue]{pending}[/blue] ({pending/total*100:.1f}% of {total})")
        table.add_row("Files Processing", f"[yellow]{processing}[/yellow] ({processing/total*100:.1f}% of {total})")
        table.add_row("Files Error", f"[red]{error}[/red] ({error/total*100:.1f}% of {total})")
        
        # Estimate completion
        if status == 'running' and completed > 0 and pending > 0:
            estimate = estimate_completion_time(db, job_id)
            remaining_time = estimate.get('remaining_seconds', 0)
            remaining_hours = remaining_time // 3600
            remaining_minutes = (remaining_time % 3600) // 60
            remaining_seconds = remaining_time % 60
            
            estimated_completion = estimate.get('estimated_completion', 'unknown')
            rate = estimate.get('files_per_second', 0)
            
            table.add_row("Processing Rate", f"{rate:.2f} files/second")
            table.add_row("Time Remaining", f"{remaining_hours:.0f}h {remaining_minutes:.0f}m {remaining_seconds:.0f}s")
            table.add_row("Estimated Completion", estimated_completion)
        
        console.print(table)
        
        # Add a separator
        console.print("")

def process_directory(args):
    """
    Process a directory using the PII analyzer
    
    Args:
        args: Parsed command-line arguments
    """
    if not args.directory:
        console.print("[bold red]Error:[/bold red] Directory is required")
        return
    
    # Expand directory path
    directory = os.path.abspath(args.directory)
    
    # Check if directory exists
    if not os.path.isdir(directory):
        console.print(f"[bold red]Error:[/bold red] Directory not found: {directory}")
        return
    
    # Connect to database
    db = get_database(args.db_path)
    
    # Get extensions to process
    extensions = None
    if args.extensions:
        extensions = set('.' + ext.strip().lstrip('.') for ext in args.extensions.split(','))
    else:
        extensions = DEFAULT_EXTENSIONS
    
    # Process new job or resume existing job
    if args.resume:
        # Resume existing job
        job_id, job_info = find_resumption_point(db, directory, args.job_id)
        if not job_id:
            if args.job_id:
                console.print(f"[bold red]Error:[/bold red] Job ID {args.job_id} not found or not associated with {directory}")
            else:
                console.print(f"[bold red]Error:[/bold red] No existing job found for {directory}")
            return
        
        # If job is already completed
        if job_info.get('status') == 'completed' and not args.force_restart:
            console.print(f"[bold green]Job {job_id} for {directory} is already completed.[/bold green]")
            return
        
        # Reset stalled files
        reset_stalled_files(db, job_id)
        
        # Display job information
        console.print(f"[bold blue]Resuming job {job_id} for {directory}[/bold blue]")
        
        # Scan directory if needed
        if not args.skip_scan:
            console.print(f"Scanning directory {directory} for files...")
            
            # Define callback for progress updates
            def scan_progress_callback(state):
                if state['type'] == 'progress':
                    files_scanned = state.get('files_scanned', 0)
                    console.print(f"Scanned {files_scanned} files...")
                elif state['type'] == 'completed':
                    files_added = state.get('files_added', 0)
                    files_removed = state.get('files_removed', 0)
                    files_total = state.get('files_total', 0)
                    console.print(f"Scan completed. Added {files_added} files, removed {files_removed} files, total {files_total} files.")
            
            # Scan directory and update database
            result = scan_directory(
                db, 
                job_id, 
                directory, 
                extensions=extensions, 
                progress_callback=scan_progress_callback
            )
            
            console.print(f"Added {result['added']} new files, removed {result['removed']} missing files")
        else:
            console.print("[yellow]Skipping directory scan as requested[/yellow]")
    else:
        # Start new job
        console.print(f"[bold blue]Starting new job for {directory}[/bold blue]")
        
        # Create job in database
        job_id = db.create_job(directory)
        
        # Scan directory for files
        console.print(f"Scanning directory {directory} for files...")
        
        # Define callback for progress updates
        def scan_progress_callback(state):
            if state['type'] == 'progress':
                files_scanned = state.get('files_scanned', 0)
                console.print(f"Scanned {files_scanned} files...")
            elif state['type'] == 'completed':
                files_added = state.get('files_added', 0)
                files_total = state.get('files_total', 0)
                console.print(f"Scan completed. Added {files_added} files, total {files_total} files.")
        
        # Scan directory and add files to database
        result = scan_directory(
            db, 
            job_id, 
            directory, 
            extensions=extensions, 
            progress_callback=scan_progress_callback
        )
        
        console.print(f"Added {result['added']} files to the database")
    
    # Get stats before processing
    stats = get_file_statistics(db, job_id)
    pending_count = stats.get('pending', 0)
    
    if pending_count == 0:
        console.print("[yellow]No pending files to process.[/yellow]")
        return
    
    # Prepare PII analyzer settings
    settings = {
        'threshold': args.threshold,
        'force_ocr': args.ocr,
        'ocr_dpi': args.ocr_dpi,
        'ocr_threads': args.ocr_threads,
        'max_ocr': args.max_ocr,
        'max_pages': args.max_pages,
        'debug': args.debug,
        'file_size_limit': args.file_size_limit * 1024 * 1024,  # Convert to bytes
    }
    
    if args.entities:
        settings['entities'] = args.entities.split(',')
    
    # Update job status
    db.update_job_status(job_id, 'running')
    
    # Determine number of workers
    max_workers = args.workers
    if max_workers is None:
        max_workers = calculate_optimal_workers()
        console.print(f"Automatically selected {max_workers} worker processes based on system resources")
    
    console.print(f"[bold blue]Starting processing with {max_workers} worker processes...[/bold blue]")
    
    # Set up signal handling for graceful termination
    def signal_handler(sig, frame):
        console.print("\n[yellow]Interrupting processing...[/yellow]")
        db.update_job_status(job_id, 'interrupted')
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    
    # Get total file count for progress tracking
    total_files = db.get_file_count_for_job(job_id)
    completed_files = stats.get('completed', 0)
    
    # Monitor worker processes if requested
    if args.monitor:
        # Start monitoring in a separate thread
        monitor_stop_event = threading.Event()
        monitor_thread = threading.Thread(
            target=monitor_performance,
            args=(monitor_stop_event, max_workers)
        )
        monitor_thread.daemon = True
        monitor_thread.start()
    
    # Process files with progress display
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        progress_task = progress.add_task(
            f"Processed: {completed_files} files", 
            total=total_files,
            completed=completed_files
        )
        
        last_update_time = time.time()
        last_completed = completed_files
        
        # Define progress callback
        def progress_callback(state):
            nonlocal last_update_time, last_completed
            
            if state['type'] == 'file_completed':
                # Increment completed count
                completed_count = db.get_completed_count_for_job(job_id)
                progress.update(progress_task, completed=completed_count, 
                                description=f"Processed: {completed_count} files")
                
                # Calculate processing rate every 10 files
                current_time = time.time()
                if completed_count % 10 == 0:
                    elapsed = current_time - last_update_time
                    files_processed = completed_count - last_completed
                    
                    if elapsed > 0 and files_processed > 0:
                        rate = files_processed / elapsed
                        progress.console.print(f"Processing rate: {rate:.2f} files/second")
                        
                        last_update_time = current_time
                        last_completed = completed_count
            
            elif state['type'] == 'file_error':
                # Log the error
                file_path = state.get('file_path', 'unknown')
                error = state.get('error', 'Unknown error')
                logger.error(f"Error processing file {file_path}: {error}")
        
        # Process files in parallel
        result = process_files_parallel(
            db,
            job_id,
            analyze_file,
            max_workers=max_workers,
            batch_size=args.batch_size,
            max_files=args.max_files,
            settings=settings,
            progress_callback=progress_callback
        )
        
        # Update job status
        if result['status'] == 'completed':
            db.update_job_status(job_id, 'completed')
        else:
            db.update_job_status(job_id, 'interrupted')
    
    # Stop monitoring if active
    if args.monitor:
        monitor_stop_event.set()
        monitor_thread.join(timeout=1.0)
    
    # Display final statistics
    stats = get_file_statistics(db, job_id)
    completed = stats.get('completed', 0)
    error = stats.get('error', 0)
    total = db.get_file_count_for_job(job_id)
    elapsed = result['elapsed']
    rate = result['rate']
    
    console.print(f"\n[bold green]Processing completed![/bold green]")
    console.print(f"Processed {completed} files with {error} errors in {elapsed:.2f} seconds")
    console.print(f"Average processing rate: {rate:.2f} files/second")
    console.print(f"Job status: {db.get_job_status(job_id)}")

def monitor_performance(stop_event, worker_count):
    """
    Monitor system performance metrics and display in console
    
    Args:
        stop_event: Event to signal when monitoring should stop
        worker_count: Number of worker processes to expect
    """
    try:
        # Initialize console for monitoring
        monitor_console = Console()
        
        with Live(Panel("[bold]Starting performance monitoring...[/bold]"), 
                  console=monitor_console, refresh_per_second=1, transient=True) as live:
            while not stop_event.is_set():
                # Get CPU and memory info
                cpu_percent = psutil.cpu_percent(interval=None)
                memory = psutil.virtual_memory()
                
                # Get process info
                process_info = []
                worker_processes = []
                
                for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
                    try:
                        # Track worker processes
                        if "pii-worker" in proc.info['name'] or "python" in proc.info['name']:
                            worker_processes.append(proc)
                            
                            # Only collect detailed info for top processes
                            if proc.info['cpu_percent'] > 1.0:
                                process_info.append(proc.info)
                    except:
                        continue
                
                # Sort process info by CPU usage
                process_info.sort(key=lambda x: x['cpu_percent'], reverse=True)
                
                # Create status panel
                content = f"[bold]System Performance:[/bold]\n"
                content += f"CPU: {cpu_percent:.1f}% | Memory: {memory.percent:.1f}% ({memory.used/1024/1024/1024:.1f} GB)\n"
                content += f"Workers running: {len(worker_processes)}/{worker_count}\n\n"
                
                if process_info:
                    content += "[bold]Top Processes:[/bold]\n"
                    for proc in process_info[:5]:  # Show top 5 processes
                        content += f"PID {proc['pid']}: {proc['name']} - CPU: {proc['cpu_percent']:.1f}%, Mem: {proc['memory_percent']:.1f}%\n"
                
                live.update(Panel(content, title="Performance Monitor"))
                
                # Sleep briefly
                time.sleep(1)
    except Exception as e:
        logger.error(f"Error in performance monitor: {e}")
        return

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

def list_jobs_for_directory(db_path: str, directory: str):
    """
    List all jobs for a specific directory
    
    Args:
        db_path: Path to the database
        directory: Directory to list jobs for
    """
    # Connect to database
    db = get_database(db_path)
    
    # Get jobs for directory
    jobs = db.get_jobs_for_directory(directory)
    
    if not jobs:
        console.print(f"[yellow]No jobs found for directory: {directory}[/yellow]")
        return
    
    # Create table for jobs
    jobs_table = Table(title=f"Jobs for directory: {directory}", show_header=True, header_style="bold")
    jobs_table.add_column("Job ID", style="cyan", justify="right")
    jobs_table.add_column("Name", style="green")
    jobs_table.add_column("Start Time", style="blue")
    jobs_table.add_column("Status", style="magenta")
    jobs_table.add_column("Files", justify="right")
    
    for job in jobs:
        job_id = job['job_id']
        status = job.get('status', 'unknown')
        status_color = {
            'completed': 'green',
            'running': 'blue',
            'interrupted': 'yellow',
            'error': 'red',
            'unknown': 'magenta'
        }.get(status, 'white')
        
        total_files = db.get_file_count_for_job(job_id)
        completed_files = db.get_completed_count_for_job(job_id)
        
        jobs_table.add_row(
            str(job_id),
            job.get('name', 'Unnamed'),
            job.get('start_time', 'unknown'),
            f"[{status_color}]{status}[/{status_color}]",
            f"{completed_files}/{total_files}"
        )
    
    console.print(jobs_table)
    console.print("\nTo resume a specific job, use: --resume --job-id <JOB_ID>")

def follow_process(pid_or_timestamp: str):
    """
    Follow the log output of a running detached process
    
    Args:
        pid_or_timestamp: Process ID or timestamp of the process to follow
    """
    console.print(f"[yellow]Follow process feature is not implemented in this version.[/yellow]")

def list_detached_processes():
    """
    List all detached PII analysis processes
    """
    console.print(f"[yellow]Detached processes feature is not implemented in this version.[/yellow]")

def detach_process(args):
    """
    Detach the current process to run in the background
    
    Args:
        args: Command line arguments
    """
    console.print(f"[yellow]Detach process feature is not implemented in this version.[/yellow]")

def reset_database(db_path: str):
    """
    Reset all files in the database to pending status.
    
    Args:
        db_path: Path to the database file
    """
    try:
        # Connect to database
        db = get_database(db_path)
        
        # Reset all files
        reset_count = db.reset_all_files()
        
        # Display results
        console.print(f"[bold green]Database Reset Complete[/bold green]")
        console.print(f"Reset {reset_count} files to pending status")
        
        return reset_count
        
    except Exception as e:
        console.print(f"[bold red]Error resetting database:[/bold red] {str(e)}")
        return 0

def main():
    """Main entry point for the application"""
    # Parse command-line arguments
    args = parse_args()
    
    # Set logging level based on verbosity
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.verbose:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.getLogger().setLevel(logging.WARNING)
    
    try:
        # Apply database reset if requested
        if args.reset_db:
            reset_database(args.db_path)
            console.print(f"[green]Database reset completed for {args.db_path}[/green]")
            return
        
        # Show job status if requested
        if args.status:
            show_status(args.db_path, args.job_id)
            return
        
        # Export results if requested
        if args.export:
            export_to_json(args.db_path, args.export, args.job_id)
            return
        
        # List jobs for directory if requested
        if args.list_jobs:
            list_jobs_for_directory(args.db_path, args.list_jobs)
            return
        
        # List detached processes if requested
        if args.list_detached:
            list_detached_processes()
            return
        
        # Follow detached process if requested
        if args.follow:
            follow_process(args.follow)
            return
        
        # Run in detached mode if requested
        if args.detach:
            detach_process(args)
            return
        
        # Process directory
        if args.directory:
            process_directory(args)
        else:
            console.print("[bold red]Error:[/bold red] No operation specified")
            console.print("Use --help for usage information")
    
    except Exception as e:
        console.print(f"[bold red]Error:[/bold red] {str(e)}")
        if args.debug:
            import traceback
            console.print(traceback.format_exc())
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 