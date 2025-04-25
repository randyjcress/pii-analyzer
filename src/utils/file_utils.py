import os
import pathlib
from typing import List, Optional, Tuple, Union

from .logger import app_logger as logger

def is_valid_file(file_path: str) -> bool:
    """Check if file exists and is accessible.
    
    Args:
        file_path: Path to file to check
        
    Returns:
        bool: True if file exists and is accessible
    """
    return os.path.isfile(file_path) and os.access(file_path, os.R_OK)

def get_file_extension(file_path: str) -> str:
    """Get file extension.
    
    Args:
        file_path: Path to file
        
    Returns:
        str: File extension (lowercase, without dot)
    """
    return os.path.splitext(file_path)[1].lower().lstrip('.')

def get_supported_extensions() -> dict:
    """Get mapping of supported file extensions to extraction methods.
    
    Returns:
        dict: Mapping of extensions to extraction methods
    """
    return {
        'docx': 'tika',
        'xlsx': 'tika',
        'csv': 'tika',
        'rtf': 'tika',
        'pdf': 'tika_or_ocr',
        'jpg': 'ocr',
        'jpeg': 'ocr',
        'png': 'ocr',
        'tiff': 'ocr',
        'tif': 'ocr',
        'txt': 'tika'
    }

def is_supported_format(file_path: str) -> bool:
    """Check if file format is supported.
    
    Args:
        file_path: Path to file
        
    Returns:
        bool: True if file format is supported
    """
    extension = get_file_extension(file_path)
    return extension in get_supported_extensions()

def get_extraction_method(file_path: str) -> Optional[str]:
    """Get appropriate extraction method for file.
    
    Args:
        file_path: Path to file
        
    Returns:
        str: Extraction method ('tika', 'ocr', or 'tika_or_ocr')
        None: If file format is not supported
    """
    if not is_supported_format(file_path):
        logger.warning(f"Unsupported file format: {file_path}")
        return None
        
    extension = get_file_extension(file_path)
    return get_supported_extensions().get(extension)

def find_files(
    directory: str, 
    extensions: Optional[List[str]] = None, 
    recursive: bool = True
) -> List[str]:
    """Find files in directory with specified extensions.
    
    Args:
        directory: Directory to search
        extensions: List of file extensions to include (without dot)
        recursive: Whether to search recursively
        
    Returns:
        List[str]: List of file paths
    """
    if not os.path.isdir(directory):
        logger.error(f"Directory not found: {directory}")
        return []
        
    files = []
    
    if extensions:
        extensions = [ext.lower().lstrip('.') for ext in extensions]
    
    for root, _, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(root, filename)
            
            if extensions and get_file_extension(file_path) not in extensions:
                continue
                
            files.append(file_path)
            
        if not recursive:
            break
            
    return files

def ensure_directory(directory: str) -> None:
    """Ensure directory exists, create if it doesn't.
    
    Args:
        directory: Directory path
    """
    pathlib.Path(directory).mkdir(parents=True, exist_ok=True)

def get_output_path(
    input_path: str, 
    output_path: str, 
    output_extension: Optional[str] = None
) -> str:
    """Generate output file path based on input and output paths.
    
    Args:
        input_path: Input file path
        output_path: Output file/directory path
        output_extension: Extension for output file (without dot)
        
    Returns:
        str: Output file path
    """
    if os.path.isdir(output_path) or not output_path:
        # If output_path is a directory or empty, construct file path from input name
        input_filename = os.path.basename(input_path)
        input_basename = os.path.splitext(input_filename)[0]
        
        output_ext = f".{output_extension}" if output_extension else ".txt"
        output_filename = f"{input_basename}{output_ext}"
        
        if not output_path:
            output_dir = os.path.dirname(input_path)
        else:
            output_dir = output_path
            ensure_directory(output_dir)
            
        return os.path.join(output_dir, output_filename)
    
    # If output_path specifies a file path, return it directly
    output_dir = os.path.dirname(output_path)
    if output_dir:
        ensure_directory(output_dir)
        
    return output_path 