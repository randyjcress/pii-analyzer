import logging
import os
import sys
from logging.handlers import RotatingFileHandler

def setup_logger(name, log_file=None, level=logging.INFO, console_output=True):
    """Set up a logger with file and console handlers.
    
    Args:
        name: Name of the logger
        log_file: Path to log file (optional)
        level: Logging level
        console_output: Whether to output logs to console
        
    Returns:
        Logger instance
    """
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    simple_formatter = logging.Formatter('%(levelname)s: %(message)s')
    
    # Clear existing handlers
    if logger.hasHandlers():
        logger.handlers.clear()
    
    # Add file handler if log_file is provided
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5
        )
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)
    
    # Add console handler if requested
    if console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(simple_formatter)
        logger.addHandler(console_handler)
    
    return logger

# Default application logger
app_logger = setup_logger(
    'pii_analyzer', 
    os.path.join(os.getcwd(), 'logs', 'pii_analyzer.log') 
    if os.environ.get('LOG_TO_FILE', 'False').lower() == 'true' 
    else None
) 