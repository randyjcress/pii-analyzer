import json
import os
import sys
import time
from typing import Dict, List, Optional, Tuple

import click
from rich.console import Console
from rich.progress import Progress, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.table import Table

from .analyzers.presidio_analyzer import PresidioAnalyzer
from .anonymizers.presidio_anonymizer import PresidioAnonymizer
from .extractors.extractor_factory import ExtractorFactory
from .utils.file_utils import (find_files, get_output_path, is_supported_format,
                              is_valid_file)
from .utils.logger import app_logger as logger, setup_logger

# Initialize rich console
console = Console()

# Set up CLI
@click.group()
@click.option(
    "--verbose", "-v", is_flag=True, 
    help="Enable verbose logging"
)
@click.option(
    "--log-file", 
    type=str, 
    help="Log file path"
)
def cli(verbose: bool, log_file: Optional[str]):
    """PII Analyzer CLI for extracting and anonymizing PII."""
    # Configure logging
    log_level = "DEBUG" if verbose else "INFO"
    
    if log_file:
        setup_logger(
            "pii_analyzer", 
            log_file=log_file, 
            level=getattr(sys.modules["logging"], log_level)
        )
    else:
        logger.setLevel(getattr(sys.modules["logging"], log_level))
        
    if verbose:
        click.echo("Verbose logging enabled")

@cli.command()
@click.option(
    "--input", "-i", 
    required=True,
    type=str, 
    help="Input file or directory"
)
@click.option(
    "--output", "-o", 
    type=str, 
    help="Output file or directory (optional)"
)
@click.option(
    "--format", "-f", 
    type=click.Choice(["json", "text"]), 
    default="json", 
    help="Output format"
)
@click.option(
    "--entities", "-e", 
    type=str,
    help="Comma-separated list of entities to detect (default: all)"
)
@click.option(
    "--threshold", "-t", 
    type=float, 
    default=0.7, 
    help="Confidence threshold (0-1)"
)
@click.option(
    "--ocr", "-c", 
    is_flag=True, 
    help="Force OCR for text extraction"
)
@click.option(
    "--ocr-dpi", 
    type=int, 
    default=300, 
    help="DPI for OCR (higher values give better quality but slower processing)"
)
@click.option(
    "--ocr-threads", 
    type=int, 
    default=0, 
    help="Number of OCR processing threads (0=auto)"
)
@click.option(
    "--max-pages", 
    type=int, 
    default=None, 
    help="Maximum pages to process per PDF (None=all)"
)
@click.option(
    "--sample", 
    type=int, 
    default=None, 
    help="Process only a sample of files when analyzing directories"
)
@click.option(
    "--summary", 
    is_flag=True, 
    help="Show summary statistics after processing"
)
def analyze(
    input: str, 
    output: Optional[str], 
    format: str, 
    entities: Optional[str], 
    threshold: float, 
    ocr: bool,
    ocr_dpi: int,
    ocr_threads: int,
    max_pages: Optional[int],
    sample: Optional[int],
    summary: bool
):
    """Analyze file(s) for PII entities."""
    # Set up entity list if specified
    entity_list = None
    if entities:
        entity_list = [e.strip() for e in entities.split(",")]
        
    # Process single file
    if os.path.isfile(input):
        _analyze_file(
            file_path=input,
            output_path=output,
            output_format=format,
            entities=entity_list,
            threshold=threshold,
            force_ocr=ocr,
            ocr_dpi=ocr_dpi,
            ocr_threads=ocr_threads,
            max_pages=max_pages
        )
    
    # Process directory
    elif os.path.isdir(input):
        _analyze_directory(
            directory=input,
            output_path=output,
            output_format=format,
            entities=entity_list,
            threshold=threshold,
            force_ocr=ocr,
            ocr_dpi=ocr_dpi,
            ocr_threads=ocr_threads,
            max_pages=max_pages,
            sample_size=sample,
            show_summary=summary
        )
    
    else:
        logger.error(f"Input path does not exist: {input}")
        sys.exit(1)

@cli.command()
@click.option(
    "--input", "-i", 
    required=True,
    type=str, 
    help="Input file or directory"
)
@click.option(
    "--output", "-o", 
    type=str, 
    help="Output file or directory"
)
@click.option(
    "--format", "-f", 
    type=click.Choice(["json", "text"]), 
    default="text", 
    help="Output format"
)
@click.option(
    "--entities", "-e", 
    type=str,
    help="Comma-separated list of entities to detect (default: all)"
)
@click.option(
    "--threshold", "-t", 
    type=float, 
    default=0.7, 
    help="Confidence threshold (0-1)"
)
@click.option(
    "--anonymize", "-a", 
    type=click.Choice(["replace", "redact", "mask", "hash", "encrypt"]), 
    default="replace", 
    help="Anonymization method"
)
@click.option(
    "--ocr", "-c", 
    is_flag=True, 
    help="Force OCR for text extraction"
)
@click.option(
    "--ocr-dpi", 
    type=int, 
    default=300, 
    help="DPI for OCR (higher values give better quality but slower processing)"
)
@click.option(
    "--ocr-threads", 
    type=int, 
    default=0, 
    help="Number of OCR processing threads (0=auto)"
)
@click.option(
    "--max-pages", 
    type=int, 
    default=None, 
    help="Maximum pages to process per PDF (None=all)"
)
def redact(
    input: str, 
    output: Optional[str], 
    format: str, 
    entities: Optional[str], 
    threshold: float, 
    anonymize: str, 
    ocr: bool,
    ocr_dpi: int,
    ocr_threads: int,
    max_pages: Optional[int]
):
    """Redact PII entities from file(s)."""
    # Set up entity list if specified
    entity_list = None
    if entities:
        entity_list = [e.strip() for e in entities.split(",")]
    
    # Process single file
    if os.path.isfile(input):
        _redact_file(
            file_path=input,
            output_path=output,
            output_format=format,
            entities=entity_list,
            threshold=threshold,
            anonymize_method=anonymize,
            force_ocr=ocr,
            ocr_dpi=ocr_dpi,
            ocr_threads=ocr_threads,
            max_pages=max_pages
        )
    
    # Process directory
    elif os.path.isdir(input):
        _redact_directory(
            directory=input,
            output_path=output,
            output_format=format,
            entities=entity_list,
            threshold=threshold,
            anonymize_method=anonymize,
            force_ocr=ocr,
            ocr_dpi=ocr_dpi,
            ocr_threads=ocr_threads,
            max_pages=max_pages
        )
    
    else:
        logger.error(f"Input path does not exist: {input}")
        sys.exit(1)

@cli.command()
@click.option(
    "--port", "-p", 
    type=int, 
    default=5000, 
    help="Port to run server on"
)
def serve(port: int):
    """Run as API server (future development)."""
    click.echo("API server not implemented yet. Coming in future version.")
    sys.exit(0)

def _analyze_file(
    file_path: str, 
    output_path: Optional[str], 
    output_format: str, 
    entities: Optional[List[str]], 
    threshold: float, 
    force_ocr: bool,
    ocr_dpi: int = 300,
    ocr_threads: int = 0,
    max_pages: Optional[int] = None
) -> None:
    """Analyze a single file for PII entities.
    
    Args:
        file_path: Path to input file
        output_path: Path to output file/directory
        output_format: Output format (json, text)
        entities: List of entity types to detect
        threshold: Confidence threshold
        force_ocr: Whether to force OCR for text extraction
        ocr_dpi: DPI for OCR (higher = better quality but slower)
        ocr_threads: Number of OCR processing threads (0=auto)
        max_pages: Maximum pages to process per PDF (None=all)
    """
    if not is_valid_file(file_path):
        logger.error(f"Input file not found or not readable: {file_path}")
        return
        
    if not is_supported_format(file_path):
        logger.error(f"Unsupported file format: {file_path}")
        return
    
    try:
        # Track timing
        start_time = time.time()
        
        # Show processing info
        console.print(f"[bold blue]Processing:[/bold blue] {file_path}")
        
        # Extract text from file
        console.print("Extracting text...", end="")
        extraction_start = time.time()
        extractor = ExtractorFactory(
            ocr_dpi=ocr_dpi,
            ocr_threads=ocr_threads
        )
        text, metadata = extractor.extract_text(
            file_path, 
            force_ocr=force_ocr,
            max_pages=max_pages
        )
        extraction_time = time.time() - extraction_start
        console.print(f" [green]Done[/green] ({extraction_time:.2f}s)")
        
        if not text:
            console.print("[bold red]No text extracted[/bold red]")
            logger.warning(f"No text extracted from {file_path}")
            return
        
        # Analyze text for PII
        console.print("Analyzing for PII...", end="")
        analysis_start = time.time()
        analyzer = PresidioAnalyzer(score_threshold=threshold)
        detected_entities = analyzer.analyze_text(
            text=text,
            entities=entities
        )
        analysis_time = time.time() - analysis_start
        console.print(f" [green]Done[/green] ({analysis_time:.2f}s)")
        
        # Timing information
        total_time = time.time() - start_time
        
        # Summary
        console.print(f"[bold green]Found {len(detected_entities)} PII entities[/bold green]")
        console.print(f"Text length: {len(text)} characters")
        console.print(f"Extraction method: {metadata.get('extraction_method', 'unknown')}")
        console.print(f"Total processing time: {total_time:.2f}s")
            
        # Prepare results
        results = {
            "file_path": file_path,
            "entities": detected_entities,
            "metadata": metadata,
            "text_length": len(text),
            "processing_time": {
                "total": total_time,
                "extraction": extraction_time,
                "analysis": analysis_time
            }
        }
        
        # If no output path is specified, just print to stdout instead of writing to a file
        if output_path is None:
            if output_format == "json":
                print(json.dumps(results, indent=2))
            else:
                print(f"File: {file_path}")
                print(f"Text length: {len(text)}")
                print(f"Extraction method: {metadata.get('extraction_method', 'unknown')}")
                print(f"Entities found: {len(detected_entities)}")
                print()
                
                for entity in detected_entities:
                    print(f"Type: {entity['entity_type']}")
                    print(f"Text: {entity['text']}")
                    print(f"Score: {entity['score']:.2f}")
                    print(f"Position: {entity['start']}-{entity['end']}")
                    print()
            
            logger.info(f"Analysis results printed to stdout")
            return
        
        # Output to file
        if output_format == "json":
            output_file = get_output_path(file_path, output_path, "json")
            with open(output_file, "w") as f:
                json.dump(results, f, indent=2)
            logger.info(f"Analysis results written to {output_file}")
            
        else:  # text format
            output_file = get_output_path(file_path, output_path, "txt")
            with open(output_file, "w") as f:
                f.write(f"File: {file_path}\n")
                f.write(f"Text length: {len(text)}\n")
                f.write(f"Extraction method: {metadata.get('extraction_method', 'unknown')}\n")
                f.write(f"Entities found: {len(detected_entities)}\n\n")
                
                for entity in detected_entities:
                    f.write(f"Type: {entity['entity_type']}\n")
                    f.write(f"Text: {entity['text']}\n")
                    f.write(f"Score: {entity['score']:.2f}\n")
                    f.write(f"Position: {entity['start']}-{entity['end']}\n\n")
                    
            logger.info(f"Analysis results written to {output_file}")
            
    except Exception as e:
        logger.error(f"Error analyzing {file_path}: {e}")
        console.print(f"[bold red]Error:[/bold red] {str(e)}")

def _analyze_directory(
    directory: str, 
    output_path: Optional[str], 
    output_format: str, 
    entities: Optional[List[str]], 
    threshold: float, 
    force_ocr: bool,
    ocr_dpi: int = 300,
    ocr_threads: int = 0,
    max_pages: Optional[int] = None,
    sample_size: Optional[int] = None,
    show_summary: bool = False
) -> None:
    """Analyze all files in a directory for PII entities with progress tracking.
    
    Args:
        directory: Path to input directory
        output_path: Path to output directory
        output_format: Output format (json, text)
        entities: List of entity types to detect
        threshold: Confidence threshold
        force_ocr: Whether to force OCR for text extraction
        ocr_dpi: DPI for OCR (higher = better quality but slower)
        ocr_threads: Number of OCR processing threads (0=auto)
        max_pages: Maximum pages to process per PDF (None=all)
        sample_size: Maximum number of files to process (None=all)
        show_summary: Whether to show summary statistics after processing
    """
    # Find all supported files
    supported_extensions = list(
        set(ext for ext in ["docx", "xlsx", "csv", "rtf", "pdf", "jpg", "jpeg", "png", "tiff", "tif", "txt"])
    )
    
    console.print(f"Scanning directory: [bold blue]{directory}[/bold blue]")
    files = find_files(directory, extensions=supported_extensions)
    
    # Apply sample limit if specified
    if sample_size and len(files) > sample_size:
        console.print(f"Limiting to sample of [bold]{sample_size}[/bold] files out of {len(files)}")
        files = files[:sample_size]
    
    if not files:
        console.print("[bold yellow]No supported files found[/bold yellow]")
        logger.warning(f"No supported files found in {directory}")
        return
        
    # Show file type statistics
    file_types = {}
    for file in files:
        ext = os.path.splitext(file)[1].lower()
        file_types[ext] = file_types.get(ext, 0) + 1
    
    console.print(f"Found [bold]{len(files)}[/bold] supported files")
    console.print("[bold]File types:[/bold]")
    for ext, count in sorted(file_types.items(), key=lambda x: x[1], reverse=True):
        console.print(f"  {ext}: {count}")
    
    # Track directory-wide statistics
    stats = {
        "total_files": len(files),
        "processed_files": 0,
        "total_entities": 0,
        "entity_counts": {},
        "file_stats": [],
        "errors": [],
        "total_time": 0,
        "extraction_time": 0,
        "analysis_time": 0
    }
    
    # For directory analysis with an output file, we'll create a single summary file
    if output_path is not None:
        if os.path.isdir(output_path):
            # If output_path is a directory, create a summary file in that directory
            output_dir = output_path
            output_file = os.path.join(output_dir, f"pii_analysis_summary_{os.path.basename(directory)}")
            if output_format == "json":
                output_file += ".json"
            else:
                output_file += ".txt"
        else:
            # Use the provided output path directly
            output_file = output_path
        
        # Initialize the output file
        if output_format == "json":
            # For JSON, we'll build a list of results and write at the end
            all_results = []
        else:  # text format
            # For text, open file and write header
            with open(output_file, "w") as f:
                f.write(f"PII Analysis Summary for Directory: {directory}\n")
                f.write(f"Files analyzed: {len(files)}\n")
                f.write(f"Analysis timestamp: {os.path.basename(directory)}\n")
                f.write("-" * 80 + "\n\n")
        
        # Initialize extractor once for all files
        extractor = ExtractorFactory(
            ocr_dpi=ocr_dpi,
            ocr_threads=ocr_threads
        )
        
        start_time = time.time()
        
        # Process each file with progress bar
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            task = progress.add_task("[green]Processing files...", total=len(files))
            
            for idx, file_path in enumerate(files):
                file_name = os.path.basename(file_path)
                progress.update(task, description=f"[green]Processing: [cyan]{file_name[:30]}...[/cyan]")
                
                try:
                    # Extract text
                    extraction_start = time.time()
                    text, metadata = extractor.extract_text(file_path, force_ocr=force_ocr, max_pages=max_pages)
                    extraction_time = time.time() - extraction_start
                    stats["extraction_time"] += extraction_time
                    
                    if not text:
                        stats["errors"].append({
                            "file": file_path,
                            "error": "No text extracted"
                        })
                        progress.update(task, advance=1)
                        continue
                    
                    # Analyze text for PII
                    analysis_start = time.time()
                    analyzer = PresidioAnalyzer(score_threshold=threshold)
                    detected_entities = analyzer.analyze_text(
                        text=text,
                        entities=entities
                    )
                    analysis_time = time.time() - analysis_start
                    stats["analysis_time"] += analysis_time
                    
                    # Update statistics
                    stats["total_entities"] += len(detected_entities)
                    stats["processed_files"] += 1
                    
                    # Count entity types
                    for entity in detected_entities:
                        entity_type = entity['entity_type']
                        stats["entity_counts"][entity_type] = stats["entity_counts"].get(entity_type, 0) + 1
                    
                    # Build results
                    file_processing_time = extraction_time + analysis_time
                    results = {
                        "file_path": file_path,
                        "entities": detected_entities,
                        "metadata": metadata,
                        "text_length": len(text),
                        "processing_time": {
                            "extraction": extraction_time,
                            "analysis": analysis_time,
                            "total": file_processing_time
                        }
                    }
                    
                    # Record file stats
                    file_stats = {
                        "file_path": file_path,
                        "text_length": len(text),
                        "entity_count": len(detected_entities),
                        "extraction_method": metadata.get("extraction_method", "unknown"),
                        "extraction_time": extraction_time,
                        "analysis_time": analysis_time,
                        "total_time": file_processing_time
                    }
                    stats["file_stats"].append(file_stats)
                    
                    # Append to JSON results or write to text file
                    if output_format == "json":
                        all_results.append(results)
                    else:  # text format
                        with open(output_file, "a") as f:
                            f.write(f"File: {file_path}\n")
                            f.write(f"Text length: {len(text)}\n")
                            f.write(f"Extraction method: {metadata.get('extraction_method', 'unknown')}\n")
                            f.write(f"Entities found: {len(detected_entities)}\n\n")
                            
                            for entity in detected_entities:
                                f.write(f"Type: {entity['entity_type']}\n")
                                f.write(f"Text: {entity['text']}\n")
                                f.write(f"Score: {entity['score']:.2f}\n")
                                f.write(f"Position: {entity['start']}-{entity['end']}\n\n")
                            
                            f.write("-" * 80 + "\n\n")
                    
                except Exception as e:
                    stats["errors"].append({
                        "file": file_path,
                        "error": str(e)
                    })
                    logger.error(f"Error processing {file_path}: {e}")
                
                progress.update(task, advance=1)
        
        # Calculate total processing time
        stats["total_time"] = time.time() - start_time
        
        # If JSON format, write all results to the output file
        if output_format == "json":
            summary = {
                "directory": directory,
                "files_analyzed": len(files),
                "files_processed": stats["processed_files"],
                "total_entities": stats["total_entities"],
                "entity_type_counts": stats["entity_counts"],
                "processing_time": {
                    "total": stats["total_time"],
                    "extraction": stats["extraction_time"],
                    "analysis": stats["analysis_time"]
                },
                "results": all_results
            }
            
            with open(output_file, "w") as f:
                json.dump(summary, f, indent=2)
        
        # Show summary if requested
        if show_summary:
            _display_analysis_summary(stats)
        
        console.print(f"Analysis complete. Found [bold]{stats['total_entities']}[/bold] PII entities in [bold]{stats['processed_files']}/{len(files)}[/bold] files.")
        console.print(f"Results written to [bold blue]{output_file}[/bold blue]")
        logger.info(f"Analysis complete. Found {stats['total_entities']} PII entities in {stats['processed_files']} files.")
        
    else:
        # No output path specified, process each file individually
        for file_path in files:
            _analyze_file(
                file_path=file_path,
                output_path=None,  # Print to stdout
                output_format=output_format,
                entities=entities,
                threshold=threshold,
                force_ocr=force_ocr,
                ocr_dpi=ocr_dpi,
                ocr_threads=ocr_threads,
                max_pages=max_pages
            )

def _display_analysis_summary(stats: Dict):
    """Display summary statistics for directory analysis."""
    console.print("\n[bold green]PII Analysis Summary[/bold green]")
    console.print(f"Total files: {stats['total_files']}")
    console.print(f"Processed files: {stats['processed_files']}")
    console.print(f"Failed files: {len(stats['errors'])}")
    
    # Timing information
    console.print(f"\n[bold yellow]Timing Information[/bold yellow]")
    console.print(f"Total time: {stats['total_time']:.2f} seconds")
    console.print(f"Avg time per file: {stats['total_time'] / max(stats['processed_files'], 1):.2f} seconds")
    console.print(f"Text extraction time: {stats['extraction_time']:.2f} seconds ({stats['extraction_time'] / stats['total_time'] * 100:.1f}%)")
    console.print(f"PII analysis time: {stats['analysis_time']:.2f} seconds ({stats['analysis_time'] / stats['total_time'] * 100:.1f}%)")
    
    # Entity types table
    console.print("\n[bold cyan]PII Entity Types Detected[/bold cyan]")
    table = Table(show_header=True)
    table.add_column("Entity Type")
    table.add_column("Count")
    table.add_column("Percentage", justify="right")
    
    for entity_type, count in sorted(stats["entity_counts"].items(), key=lambda x: x[1], reverse=True):
        percentage = count / max(stats["total_entities"], 1) * 100
        table.add_row(
            entity_type,
            str(count),
            f"{percentage:.1f}%"
        )
    
    console.print(table)
    
    # Display errors if any
    if stats["errors"]:
        console.print(f"\n[bold red]Errors ({len(stats['errors'])})[/bold red]")
        for error in stats["errors"][:10]:  # Show only first 10 errors
            console.print(f"[red]{error['file']}[/red]: {error['error']}")
        
        if len(stats["errors"]) > 10:
            console.print(f"[dim]...and {len(stats['errors']) - 10} more errors[/dim]")
    
    # Show file stats for slowest files
    if stats["file_stats"]:
        console.print("\n[bold magenta]Slowest Files[/bold magenta]")
        stats_table = Table(show_header=True)
        stats_table.add_column("File")
        stats_table.add_column("Text Length")
        stats_table.add_column("Entities")
        stats_table.add_column("Method")
        stats_table.add_column("Time (s)")
        
        for stat in sorted(stats["file_stats"], key=lambda x: x["total_time"], reverse=True)[:10]:
            stats_table.add_row(
                os.path.basename(stat["file_path"]),
                str(stat["text_length"]),
                str(stat["entity_count"]),
                stat["extraction_method"],
                f"{stat['total_time']:.2f}"
            )
        
        console.print(stats_table)

def _redact_file(
    file_path: str, 
    output_path: Optional[str], 
    output_format: str, 
    entities: Optional[List[str]], 
    threshold: float, 
    anonymize_method: str, 
    force_ocr: bool,
    ocr_dpi: int = 300,
    ocr_threads: int = 0,
    max_pages: Optional[int] = None
) -> None:
    """Redact PII entities from a file.
    
    Args:
        file_path: Path to input file
        output_path: Path to output file/directory
        output_format: Output format (json, text)
        entities: List of entity types to detect
        threshold: Confidence threshold
        anonymize_method: Anonymization method
        force_ocr: Whether to force OCR for text extraction
        ocr_dpi: DPI for OCR (higher = better quality but slower)
        ocr_threads: Number of OCR processing threads (0=auto)
        max_pages: Maximum pages to process per PDF (None=all)
    """
    if not is_valid_file(file_path):
        logger.error(f"Input file not found or not readable: {file_path}")
        return
        
    if not is_supported_format(file_path):
        logger.error(f"Unsupported file format: {file_path}")
        return
    
    try:
        # Extract text from file
        extractor = ExtractorFactory(
            ocr_dpi=ocr_dpi,
            ocr_threads=ocr_threads
        )
        text, metadata = extractor.extract_text(file_path, force_ocr=force_ocr, max_pages=max_pages)
        
        if not text:
            logger.warning(f"No text extracted from {file_path}")
            return
            
        # Analyze text for PII
        analyzer = PresidioAnalyzer(score_threshold=threshold)
        detected_entities = analyzer.analyze_text(
            text=text,
            entities=entities
        )
        
        if not detected_entities:
            logger.info(f"No PII entities found in {file_path}")
            return
            
        # Anonymize text
        anonymizer = PresidioAnonymizer()
        anonymized_text = anonymizer.anonymize_text(
            text=text,
            entities=detected_entities,
            anonymize_method=anonymize_method
        )
        
        # If no output path is specified, just print to stdout
        if output_path is None:
            if output_format == "json":
                results = {
                    "file_path": file_path,
                    "original_text_length": len(text),
                    "anonymized_text_length": len(anonymized_text),
                    "entities_redacted": len(detected_entities),
                    "anonymize_method": anonymize_method,
                    "anonymized_text": anonymized_text
                }
                print(json.dumps(results, indent=2))
            else:
                print(f"File: {file_path}")
                print(f"Original text length: {len(text)}")
                print(f"Anonymized text length: {len(anonymized_text)}")
                print(f"Entities redacted: {len(detected_entities)}")
                print(f"Anonymization method: {anonymize_method}")
                print()
                print(anonymized_text)
            
            logger.info(f"Anonymization results printed to stdout")
            return
        
        # Output to file
        if output_format == "json":
            output_file = get_output_path(file_path, output_path, "json")
            results = {
                "file_path": file_path,
                "original_text_length": len(text),
                "anonymized_text_length": len(anonymized_text),
                "entities_redacted": len(detected_entities),
                "anonymize_method": anonymize_method,
                "anonymized_text": anonymized_text
            }
            with open(output_file, "w") as f:
                json.dump(results, f, indent=2)
            logger.info(f"Anonymization results written to {output_file}")
            
        else:  # text format
            output_file = get_output_path(file_path, output_path, "txt")
            with open(output_file, "w") as f:
                f.write(anonymized_text)
            logger.info(f"Anonymized text written to {output_file}")
            
    except Exception as e:
        logger.error(f"Error redacting {file_path}: {e}")

def _redact_directory(
    directory: str, 
    output_path: Optional[str], 
    output_format: str, 
    entities: Optional[List[str]], 
    threshold: float, 
    anonymize_method: str, 
    force_ocr: bool,
    ocr_dpi: int = 300,
    ocr_threads: int = 0,
    max_pages: Optional[int] = None
) -> None:
    """Redact PII entities from all files in a directory.
    
    Args:
        directory: Path to input directory
        output_path: Path to output directory
        output_format: Output format (json, text)
        entities: List of entity types to detect
        threshold: Confidence threshold
        anonymize_method: Anonymization method
        force_ocr: Whether to force OCR for text extraction
        ocr_dpi: DPI for OCR (higher = better quality but slower)
        ocr_threads: Number of OCR processing threads (0=auto)
        max_pages: Maximum pages to process per PDF (None=all)
    """
    # Find all supported files
    supported_extensions = ["docx", "xlsx", "csv", "rtf", "pdf", "jpg", "jpeg", "png", "tiff", "tif", "txt"]
    files = find_files(directory, extensions=supported_extensions)
    
    if not files:
        logger.warning(f"No supported files found in {directory}")
        return
        
    logger.info(f"Found {len(files)} supported files in {directory}")
    
    # Create output directory if needed
    if output_path is not None and not os.path.exists(output_path):
        try:
            os.makedirs(output_path)
            logger.info(f"Created output directory: {output_path}")
        except Exception as e:
            logger.error(f"Error creating output directory {output_path}: {e}")
            return
    
    # Process each file
    for idx, file_path in enumerate(files):
        try:
            # Create file-specific output path
            file_output_path = None
            if output_path is not None:
                rel_path = os.path.relpath(file_path, directory)
                file_output_path = os.path.join(output_path, rel_path)
                
                # Create subdirectories if needed
                output_dir = os.path.dirname(file_output_path)
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
            
            # Redact file
            _redact_file(
                file_path=file_path,
                output_path=file_output_path,
                output_format=output_format,
                entities=entities,
                threshold=threshold,
                anonymize_method=anonymize_method,
                force_ocr=force_ocr,
                ocr_dpi=ocr_dpi,
                ocr_threads=ocr_threads,
                max_pages=max_pages
            )
            
            # Show progress
            if (idx + 1) % 10 == 0 or idx == len(files) - 1:
                logger.info(f"Processed {idx + 1}/{len(files)} files ({(idx + 1) / len(files) * 100:.1f}%)")
            
        except Exception as e:
            logger.error(f"Error redacting {file_path}: {e}")
    
    logger.info(f"Redaction complete for {len(files)} files")

if __name__ == "__main__":
    cli() 