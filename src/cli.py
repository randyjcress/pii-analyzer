import json
import os
import sys
from typing import Dict, List, Optional, Tuple

import click

from .analyzers.presidio_analyzer import PresidioAnalyzer
from .anonymizers.presidio_anonymizer import PresidioAnonymizer
from .extractors.extractor_factory import ExtractorFactory
from .utils.file_utils import (find_files, get_output_path, is_supported_format,
                              is_valid_file)
from .utils.logger import app_logger as logger, setup_logger

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
        logger = setup_logger(
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
def analyze(
    input: str, 
    output: Optional[str], 
    format: str, 
    entities: Optional[str], 
    threshold: float, 
    ocr: bool
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
            force_ocr=ocr
        )
    
    # Process directory
    elif os.path.isdir(input):
        _analyze_directory(
            directory=input,
            output_path=output,
            output_format=format,
            entities=entity_list,
            threshold=threshold,
            force_ocr=ocr
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
def redact(
    input: str, 
    output: Optional[str], 
    format: str, 
    entities: Optional[str], 
    threshold: float, 
    anonymize: str, 
    ocr: bool
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
            force_ocr=ocr
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
            force_ocr=ocr
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
    force_ocr: bool
) -> None:
    """Analyze a single file for PII entities.
    
    Args:
        file_path: Path to input file
        output_path: Path to output file/directory
        output_format: Output format (json, text)
        entities: List of entity types to detect
        threshold: Confidence threshold
        force_ocr: Whether to force OCR for text extraction
    """
    if not is_valid_file(file_path):
        logger.error(f"Input file not found or not readable: {file_path}")
        return
        
    if not is_supported_format(file_path):
        logger.error(f"Unsupported file format: {file_path}")
        return
    
    try:
        # Extract text from file
        extractor = ExtractorFactory()
        text, metadata = extractor.extract_text(file_path, force_ocr=force_ocr)
        
        if not text:
            logger.warning(f"No text extracted from {file_path}")
            return
            
        # Analyze text for PII
        analyzer = PresidioAnalyzer(score_threshold=threshold)
        detected_entities = analyzer.analyze_text(
            text=text,
            entities=entities
        )
        
        # Prepare results
        results = {
            "file_path": file_path,
            "entities": detected_entities,
            "metadata": metadata,
            "text_length": len(text)
        }
        
        # Output results
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

def _analyze_directory(
    directory: str, 
    output_path: Optional[str], 
    output_format: str, 
    entities: Optional[List[str]], 
    threshold: float, 
    force_ocr: bool
) -> None:
    """Analyze all files in a directory for PII entities.
    
    Args:
        directory: Path to input directory
        output_path: Path to output directory
        output_format: Output format (json, text)
        entities: List of entity types to detect
        threshold: Confidence threshold
        force_ocr: Whether to force OCR for text extraction
    """
    # Find all supported files
    supported_extensions = list(
        set(ext for ext in ["docx", "xlsx", "csv", "rtf", "pdf", "jpg", "jpeg", "png", "tiff", "tif", "txt"])
    )
    files = find_files(directory, extensions=supported_extensions)
    
    if not files:
        logger.warning(f"No supported files found in {directory}")
        return
        
    logger.info(f"Found {len(files)} supported files in {directory}")
    
    # Process each file
    for file_path in files:
        _analyze_file(
            file_path=file_path,
            output_path=output_path,
            output_format=output_format,
            entities=entities,
            threshold=threshold,
            force_ocr=force_ocr
        )

def _redact_file(
    file_path: str, 
    output_path: Optional[str], 
    output_format: str, 
    entities: Optional[List[str]], 
    threshold: float, 
    anonymize_method: str, 
    force_ocr: bool
) -> None:
    """Redact PII from a file.
    
    Args:
        file_path: Path to input file
        output_path: Path to output file/directory
        output_format: Output format (json, text)
        entities: List of entity types to detect
        threshold: Confidence threshold
        anonymize_method: Anonymization method
        force_ocr: Whether to force OCR for text extraction
    """
    if not is_valid_file(file_path):
        logger.error(f"Input file not found or not readable: {file_path}")
        return
        
    if not is_supported_format(file_path):
        logger.error(f"Unsupported file format: {file_path}")
        return
    
    try:
        # Extract text from file
        extractor = ExtractorFactory()
        text, metadata = extractor.extract_text(file_path, force_ocr=force_ocr)
        
        if not text:
            logger.warning(f"No text extracted from {file_path}")
            return
            
        # Analyze text for PII
        analyzer = PresidioAnalyzer(score_threshold=threshold)
        detected_entities = analyzer.analyze_text(
            text=text,
            entities=entities
        )
        
        # Anonymize text
        anonymizer = PresidioAnonymizer(default_method=anonymize_method)
        anonymized_text, anonymize_metadata = anonymizer.anonymize_text(
            text=text,
            entities=detected_entities
        )
        
        # Prepare results
        results = {
            "file_path": file_path,
            "original_text_length": len(text),
            "anonymized_text_length": len(anonymized_text),
            "entities": detected_entities,
            "anonymization": anonymize_metadata,
            "extraction_metadata": metadata
        }
        
        # Output results
        if output_format == "json":
            # Output structured JSON with original and anonymized text
            output_file = get_output_path(file_path, output_path, "json")
            results["original_text"] = text
            results["anonymized_text"] = anonymized_text
            
            with open(output_file, "w") as f:
                json.dump(results, f, indent=2)
                
            logger.info(f"Redaction results written to {output_file}")
            
        else:  # text format
            # Output just the anonymized text
            output_file = get_output_path(file_path, output_path, "txt")
            
            with open(output_file, "w") as f:
                f.write(anonymized_text)
                
            logger.info(f"Redacted text written to {output_file}")
            
    except Exception as e:
        logger.error(f"Error redacting {file_path}: {e}")

def _redact_directory(
    directory: str, 
    output_path: Optional[str], 
    output_format: str, 
    entities: Optional[List[str]], 
    threshold: float, 
    anonymize_method: str, 
    force_ocr: bool
) -> None:
    """Redact PII from all files in a directory.
    
    Args:
        directory: Path to input directory
        output_path: Path to output directory
        output_format: Output format (json, text)
        entities: List of entity types to detect
        threshold: Confidence threshold
        anonymize_method: Anonymization method
        force_ocr: Whether to force OCR for text extraction
    """
    # Find all supported files
    supported_extensions = list(
        set(ext for ext in ["docx", "xlsx", "csv", "rtf", "pdf", "jpg", "jpeg", "png", "tiff", "tif", "txt"])
    )
    files = find_files(directory, extensions=supported_extensions)
    
    if not files:
        logger.warning(f"No supported files found in {directory}")
        return
        
    logger.info(f"Found {len(files)} supported files in {directory}")
    
    # Process each file
    for file_path in files:
        _redact_file(
            file_path=file_path,
            output_path=output_path,
            output_format=output_format,
            entities=entities,
            threshold=threshold,
            anonymize_method=anonymize_method,
            force_ocr=force_ocr
        )

if __name__ == "__main__":
    cli() 