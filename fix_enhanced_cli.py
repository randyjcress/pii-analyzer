#!/usr/bin/env python3
"""
A fixed version of the enhanced CLI for PII analysis with better debugging for DOCX files.
This version adds more detailed error logging and fixes issues with subprocess output handling.
"""

import os
import sys
import time
import subprocess
import argparse
from typing import Dict, List, Optional, Tuple
import json
import tempfile

from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.table import Table

# Initialize rich console
console = Console()

def scan_directory(directory_path: str, extensions: Optional[List[str]] = None) -> List[str]:
    """Scan directory for supported files and return their paths."""
    if extensions is None:
        extensions = ["docx", "xlsx", "csv", "rtf", "pdf", "jpg", "jpeg", "png", "tiff", "tif", "txt"]
    
    console.print(f"Scanning directory: [bold blue]{directory_path}[/bold blue]")
    
    # Use find command to get all files with the supported extensions
    all_files = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            if any(file.lower().endswith(f".{ext}") for ext in extensions):
                all_files.append(os.path.join(root, file))
    
    return all_files

def analyze_single_file(
    file_path: str,
    threshold: float = 0.7,
    force_ocr: bool = False,
    ocr_dpi: int = 300,
    ocr_threads: int = 0,
    max_pages: Optional[int] = None,
    entities: Optional[List[str]] = None,
    debug: bool = False
) -> Tuple[bool, Dict, str]:
    """Analyze a single file with detailed debugging."""
    start_time = time.time()
    
    # Create a temporary output file using a context manager
    with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp_file:
        temp_output = tmp_file.name
    
    try:
        # Build command for the PII analyzer
        cmd = ["python", "-m", "src.cli", "analyze", 
               "-i", file_path, 
               "-o", temp_output, 
               "-f", "json", 
               "-t", str(threshold)]
        
        # Add optional parameters
        if entities:
            cmd.extend(["-e", ",".join(entities)])
        if force_ocr:
            cmd.append("--ocr")
        if ocr_dpi != 300:
            cmd.extend(["--ocr-dpi", str(ocr_dpi)])
        if ocr_threads > 0:
            cmd.extend(["--ocr-threads", str(ocr_threads)])
        if max_pages is not None:
            cmd.extend(["--max-pages", str(max_pages)])
        
        if debug:
            console.print(f"  Running command: {' '.join(cmd)}")
        
        # Run the CLI tool with full output capture
        process = subprocess.run(cmd, capture_output=True, text=True)
        
        # Debugging info if requested
        if debug and process.stdout:
            console.print(f"  Command stdout: {process.stdout}")
        
        # Check if process returned an error
        if process.returncode != 0:
            error_msg = process.stderr or "Unknown error (no stderr)"
            if debug:
                console.print(f"  [bold red]Command failed:[/bold red] {error_msg}")
            return False, {}, error_msg
        
        # Check if output file exists and has content
        if not os.path.exists(temp_output):
            if debug:
                console.print(f"  [bold red]No output file created[/bold red]")
            return False, {}, "No output file created"
        
        if os.path.getsize(temp_output) == 0:
            if debug:
                console.print(f"  [bold red]Output file is empty[/bold red]")
            return False, {}, "Output file is empty"
        
        # Try to read the JSON output
        try:
            with open(temp_output, 'r') as f:
                result_data = json.load(f)
            
            # Calculate processing time
            processing_time = time.time() - start_time
            result_data["processing_time"] = processing_time
            
            return True, result_data, ""
            
        except json.JSONDecodeError as e:
            # Try to read raw file content for debugging
            try:
                with open(temp_output, 'r') as f:
                    content = f.read()
                error_msg = f"Invalid JSON: {str(e)}. Content: {content[:100]}..."
            except Exception:
                error_msg = f"Invalid JSON: {str(e)}"
                
            if debug:
                console.print(f"  [bold red]JSON decode error:[/bold red] {error_msg}")
            return False, {}, error_msg
            
    except Exception as e:
        error_msg = f"Exception: {str(e)}"
        if debug:
            console.print(f"  [bold red]Exception:[/bold red] {error_msg}")
        return False, {}, error_msg
    
    finally:
        # Clean up temp file
        if os.path.exists(temp_output):
            os.remove(temp_output)

def analyze_files(
    files: List[str],
    output_path: Optional[str] = None,
    entities: Optional[List[str]] = None,
    threshold: float = 0.7,
    force_ocr: bool = False,
    ocr_dpi: int = 300,
    ocr_threads: int = 0,
    max_pages: Optional[int] = None,
    sample_size: Optional[int] = None,
    debug: bool = False
) -> Dict:
    """Analyze files with progress bar and detailed error logging."""
    total_files = len(files)
    
    if sample_size and sample_size < total_files:
        console.print(f"Using sample of [bold]{sample_size}[/bold] files out of {total_files}")
        files = files[:sample_size]
        total_files = sample_size
    
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
    
    # Process each file with progress bar
    overall_start_time = time.time()
    
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console
    ) as progress:
        task = progress.add_task("[green]Processing files...", total=total_files)
        
        all_results = []
        
        for idx, file_path in enumerate(files):
            file_name = os.path.basename(file_path)
            file_ext = os.path.splitext(file_path)[1].lower()
            
            progress.update(task, description=f"[green]Processing: [cyan]{file_name[:30]}...[/cyan]")
            
            # Process the file
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
            
            # Update statistics based on file type
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
            
            progress.update(task, advance=1)
    
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
            "results": all_results
        }
        
        with open(output_path, 'w') as f:
            json.dump(combined_results, f, indent=2)
        
        console.print(f"Results written to [bold blue]{output_path}[/bold blue]")
    
    return results

def display_results(results: Dict):
    """Display analysis results with file type statistics."""
    console.print("\n[bold green]PII Analysis Results[/bold green]")
    console.print(f"Total files: {results['total_files']}")
    console.print(f"Processed files: {results['processed_files']}")
    console.print(f"Failed files: {len(results['errors'])}")
    console.print(f"Total PII entities found: {results['total_entities']}")
    console.print(f"Total processing time: {results['total_time']:.2f} seconds")
    console.print(f"Avg time per file: {results['total_time'] / max(results['processed_files'], 1):.2f} seconds")
    
    # Show file type statistics
    console.print("\n[bold cyan]File Type Statistics[/bold cyan]")
    type_table = Table(show_header=True)
    type_table.add_column("File Type")
    type_table.add_column("Success")
    type_table.add_column("Error")
    type_table.add_column("Success Rate", justify="right")
    
    # Combine stats
    all_types = set(list(results["file_type_stats"]["success"].keys()) + 
                   list(results["file_type_stats"]["error"].keys()))
    
    for ext in sorted(all_types):
        success_count = results["file_type_stats"]["success"].get(ext, 0)
        error_count = results["file_type_stats"]["error"].get(ext, 0)
        total = success_count + error_count
        success_rate = success_count / total * 100 if total > 0 else 0
        
        type_table.add_row(
            ext,
            str(success_count),
            str(error_count),
            f"{success_rate:.1f}%"
        )
    
    console.print(type_table)
    
    # Entity types table
    console.print("\n[bold cyan]PII Entity Types Detected[/bold cyan]")
    table = Table(show_header=True)
    table.add_column("Entity Type")
    table.add_column("Count")
    table.add_column("Percentage", justify="right")
    
    for entity_type, count in sorted(results["entity_counts"].items(), key=lambda x: x[1], reverse=True):
        percentage = count / max(results["total_entities"], 1) * 100
        table.add_row(
            entity_type,
            str(count),
            f"{percentage:.1f}%"
        )
    
    console.print(table)
    
    # Display errors if any
    if results["errors"]:
        console.print(f"\n[bold red]Errors ({len(results['errors'])})[/bold red]")
        
        # Count error patterns
        error_patterns = {}
        for error in results["errors"]:
            # Get first line of error message as the pattern
            msg = error["error"].split('\n')[0] if error["error"] else "Unknown error"
            error_patterns[msg] = error_patterns.get(msg, 0) + 1
        
        # Show error patterns
        console.print("Error patterns:")
        for msg, count in sorted(error_patterns.items(), key=lambda x: x[1], reverse=True):
            console.print(f"  - {msg} ({count} occurrences)")
        
        # Show sample errors
        console.print("\nSample errors (first 5):")
        for error in results["errors"][:5]:
            console.print(f"  [red]{error['file']}[/red]: {error['error']}")
        
        if len(results["errors"]) > 5:
            console.print(f"  [dim]...and {len(results['errors']) - 5} more errors[/dim]")
    
    # Show file stats for slowest files
    if results["file_stats"]:
        console.print("\n[bold magenta]Slowest Files[/bold magenta]")
        stats_table = Table(show_header=True)
        stats_table.add_column("File")
        stats_table.add_column("Text Length")
        stats_table.add_column("Entities")
        stats_table.add_column("Method")
        stats_table.add_column("Time (s)")
        
        for stat in sorted(results["file_stats"], key=lambda x: x["total_time"], reverse=True)[:10]:
            stats_table.add_row(
                os.path.basename(stat["file_path"]),
                str(stat["text_length"]),
                str(stat["entity_count"]),
                stat["extraction_method"],
                f"{stat['total_time']:.2f}"
            )
        
        console.print(stats_table)

def main():
    parser = argparse.ArgumentParser(description="Fixed Enhanced PII Analysis with Progress Tracking")
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
            
            # Analyze files
            results = analyze_files(
                files=files_to_process,
                output_path=args.output,
                entities=entity_list,
                threshold=args.threshold,
                force_ocr=args.ocr,
                ocr_dpi=args.ocr_dpi,
                ocr_threads=args.ocr_threads,
                max_pages=args.max_pages,
                sample_size=args.sample,
                debug=args.debug
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