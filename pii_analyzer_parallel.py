#!/usr/bin/env python3
"""
Multi-threaded PII Analyzer
A parallel version of the PII analyzer that processes multiple files simultaneously
for improved performance on large document collections.
"""

import os
import sys
import time
import argparse
import json
import psutil
from typing import Dict, List, Optional, Tuple
import concurrent.futures
from threading import Lock

from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.table import Table

# Import the functions from fix_enhanced_cli
from fix_enhanced_cli import (
    scan_directory, 
    analyze_single_file, 
    display_results
)

# Initialize rich console
console = Console()

def determine_optimal_worker_count() -> int:
    """Determine the optimal number of worker threads based on system resources.
    
    Returns:
        int: Optimal number of worker threads
    """
    try:
        # Get CPU info
        cpu_count = os.cpu_count() or 1
        
        # Check available memory
        available_memory_gb = psutil.virtual_memory().available / (1024 * 1024 * 1024)
        
        # Each worker can use significant memory, so we need to consider memory constraints
        # Conservative estimate of 1GB per worker to be safe
        memory_based_worker_limit = max(1, int(available_memory_gb / 1.0))
        
        # Balance CPU cores and memory constraints
        optimal_workers = min(cpu_count, memory_based_worker_limit)
        
        # If we have a lot of cores, we might not want to use all of them
        # to avoid system unresponsiveness
        if cpu_count > 4:
            # Use 75% of available cores, but at least 4
            cpu_limited_workers = max(4, int(cpu_count * 0.75))
            optimal_workers = min(optimal_workers, cpu_limited_workers)
        
        console.print(f"[dim]Optimal worker calculation: CPU cores={cpu_count}, "
                     f"Memory-based limit={memory_based_worker_limit}, "
                     f"Selected={optimal_workers}[/dim]")
        
        return optimal_workers
        
    except Exception as e:
        # Fallback to a simple calculation if psutil fails
        console.print(f"[dim]Error determining optimal workers: {e}. Using CPU count / 2.[/dim]")
        return max(1, (os.cpu_count() or 2) // 2)

def analyze_files_parallel(
    files: List[str],
    output_path: Optional[str] = None,
    entities: Optional[List[str]] = None,
    threshold: float = 0.7,
    force_ocr: bool = False,
    ocr_dpi: int = 300,
    ocr_threads: int = 0, 
    max_pages: Optional[int] = None,
    sample_size: Optional[int] = None,
    debug: bool = False,
    max_workers: int = 0
) -> Dict:
    """Analyze files in parallel with progress bar and detailed error logging.
    
    Args:
        files: List of file paths to analyze
        output_path: Path to write JSON output
        entities: List of entity types to detect
        threshold: Confidence threshold (0-1)
        force_ocr: Force OCR for text extraction
        ocr_dpi: DPI for OCR
        ocr_threads: Number of OCR threads (0=auto)
        max_pages: Maximum pages per PDF
        sample_size: Analyze only a sample of files
        debug: Show detailed debug information
        max_workers: Maximum number of worker threads (0=auto)
        
    Returns:
        Dict: Results and statistics
    """
    total_files = len(files)
    
    if sample_size and sample_size < total_files:
        console.print(f"Using sample of [bold]{sample_size}[/bold] files out of {total_files}")
        files = files[:sample_size]
        total_files = sample_size
    
    # Determine optimal number of workers
    if max_workers <= 0:
        max_workers = determine_optimal_worker_count()
    
    # Adjust if we have fewer files than workers
    max_workers = min(max_workers, total_files)
    
    console.print(f"[bold]Processing {total_files} files using {max_workers} parallel workers[/bold]")
    
    # Track statistics
    results = {
        "total_files": total_files,
        "processed_files": 0,
        "total_entities": 0,
        "entity_counts": {},
        "file_stats": [],
        "errors": [],
        "total_time": 0,
        "file_type_stats": {
            "success": {},
            "error": {}
        }
    }
    
    # Create output directory if needed
    if output_path and os.path.dirname(output_path) and not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))
    
    # Process files with parallel workers
    overall_start_time = time.time()
    all_results = []
    
    # Create locks for thread-safe updating of the results dictionary
    results_lock = Lock()
    
    # Function to process a single file that will be executed by workers
    def process_file(file_path: str, file_idx: int) -> Tuple[bool, Dict, str, int]:
        file_ext = os.path.splitext(file_path)[1].lower()
        
        # Process the file using the existing function
        success, result_data, error_msg = analyze_single_file(
            file_path=file_path,
            threshold=threshold,
            force_ocr=force_ocr,
            ocr_dpi=ocr_dpi,
            ocr_threads=ocr_threads,
            max_pages=max_pages,
            entities=entities,
            debug=debug
        )
        
        return success, result_data, error_msg, file_idx
    
    # Create a progress display
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        task = progress.add_task("[green]Processing files...", total=total_files)
        
        # Submit all files to the thread pool
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Keep track of futures
            future_to_file = {
                executor.submit(process_file, file_path, i): file_path 
                for i, file_path in enumerate(files)
            }
            
            # Process files as they complete
            for future in concurrent.futures.as_completed(future_to_file):
                file_path = future_to_file[future]
                file_name = os.path.basename(file_path)
                file_ext = os.path.splitext(file_path)[1].lower()
                
                try:
                    success, result_data, error_msg, file_idx = future.result()
                    
                    # Update progress display
                    progress.update(task, description=f"[green]Processed: [cyan]{file_name[:30]}[/cyan]")
                    progress.update(task, advance=1)
                    
                    # Thread-safe update of results
                    with results_lock:
                        # Update file type statistics
                        if success:
                            if file_ext not in results["file_type_stats"]["success"]:
                                results["file_type_stats"]["success"][file_ext] = 0
                            results["file_type_stats"]["success"][file_ext] += 1
                        else:
                            if file_ext not in results["file_type_stats"]["error"]:
                                results["file_type_stats"]["error"][file_ext] = 0
                            results["file_type_stats"]["error"][file_ext] += 1
                        
                        # Handle successful processing
                        if success:
                            # Update statistics
                            results["processed_files"] += 1
                            entities_count = len(result_data.get("entities", []))
                            results["total_entities"] += entities_count
                            
                            # Count entity types
                            for entity in result_data.get("entities", []):
                                entity_type = entity['entity_type']
                                results["entity_counts"][entity_type] = results["entity_counts"].get(entity_type, 0) + 1
                            
                            # Record file stats
                            file_stats = {
                                "file_path": file_path,
                                "text_length": result_data.get("text_length", 0),
                                "entity_count": entities_count,
                                "extraction_method": result_data.get("metadata", {}).get("extraction_method", "unknown"),
                                "total_time": result_data.get("processing_time", 0)
                            }
                            results["file_stats"].append(file_stats)
                            
                            # Add to all results
                            all_results.append(result_data)
                        
                        # Handle errors
                        else:
                            results["errors"].append({
                                "file": file_path,
                                "error": error_msg
                            })
                            
                except Exception as e:
                    # Handle any unexpected errors
                    with results_lock:
                        error_msg = f"Unexpected error processing {file_path}: {str(e)}"
                        results["errors"].append({
                            "file": file_path,
                            "error": error_msg
                        })
                        
                        if file_ext not in results["file_type_stats"]["error"]:
                            results["file_type_stats"]["error"][file_ext] = 0
                        results["file_type_stats"]["error"][file_ext] += 1
                    
                    progress.update(task, advance=1)
                    
                    # Log detailed error if in debug mode
                    if debug:
                        import traceback
                        console.print(f"[dim red]{traceback.format_exc()}[/dim red]")
    
    # Final timing
    results["total_time"] = time.time() - overall_start_time
    
    # Write combined results if requested
    if output_path:
        combined_results = {
            "files_analyzed": total_files,
            "files_processed": results["processed_files"],
            "total_entities": results["total_entities"],
            "entity_type_counts": results["entity_counts"],
            "processing_time": results["total_time"],
            "file_type_stats": results["file_type_stats"],
            "workers_used": max_workers,
            "results": all_results
        }
        
        with open(output_path, 'w') as f:
            json.dump(combined_results, f, indent=2)
        
        console.print(f"Results written to [bold blue]{output_path}[/bold blue]")
    
    return results

def main():
    parser = argparse.ArgumentParser(description="Parallel PII Analysis with Multi-threading")
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
    parser.add_argument("--workers", type=int, default=0, help="Number of parallel workers (0=auto)")
    parser.add_argument("--test-docx", action="store_true", help="Test DOCX files only")
    
    args = parser.parse_args()
    
    # Parse entity list if provided
    entity_list = None
    if args.entities:
        entity_list = [e.strip() for e in args.entities.split(",")]
    
    try:
        # Process a single file
        if os.path.isfile(args.input):
            console.print(f"Processing single file: [bold blue]{args.input}[/bold blue]")
            success, result, error = analyze_single_file(
                file_path=args.input,
                threshold=args.threshold,
                force_ocr=args.ocr,
                ocr_dpi=args.ocr_dpi,
                ocr_threads=args.ocr_threads,
                max_pages=args.max_pages,
                entities=entity_list,
                debug=args.debug
            )
            
            if success:
                console.print(f"[green]Successfully analyzed file. Found {len(result.get('entities', []))} PII entities.[/green]")
                if args.output:
                    with open(args.output, 'w') as f:
                        json.dump(result, f, indent=2)
                    console.print(f"Results written to [bold blue]{args.output}[/bold blue]")
            else:
                console.print(f"[bold red]Failed to analyze file:[/bold red] {error}")
        
        # Process a directory
        else:
            # Scan directory for files
            all_files = scan_directory(args.input)
            
            # Filter for DOCX files if test-docx flag is set
            if args.test_docx:
                docx_files = [f for f in all_files if f.lower().endswith('.docx')]
                console.print(f"Found [bold]{len(docx_files)}[/bold] DOCX files to process")
                files_to_process = docx_files
            else:
                console.print(f"Found [bold]{len(all_files)}[/bold] files to process")
                files_to_process = all_files
            
            # Show file types
            file_types = {}
            for file in files_to_process:
                ext = os.path.splitext(file)[1].lower()
                file_types[ext] = file_types.get(ext, 0) + 1
            
            console.print("\n[bold]File types:[/bold]")
            for ext, count in sorted(file_types.items(), key=lambda x: x[1], reverse=True):
                console.print(f"  {ext}: {count}")
            
            # If no files to process, exit
            if not files_to_process:
                console.print("[bold yellow]No files to process.[/bold yellow]")
                return
            
            # Analyze files in parallel
            results = analyze_files_parallel(
                files=files_to_process,
                output_path=args.output,
                entities=entity_list,
                threshold=args.threshold,
                force_ocr=args.ocr,
                ocr_dpi=args.ocr_dpi,
                ocr_threads=args.ocr_threads,
                max_pages=args.max_pages,
                sample_size=args.sample,
                debug=args.debug,
                max_workers=args.workers
            )
            
            # Display results
            display_results(results)
    
    except KeyboardInterrupt:
        console.print("\n[bold red]Process interrupted by user[/bold red]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[bold red]Error: {str(e)}[/bold red]")
        if args.debug:
            import traceback
            console.print(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main() 