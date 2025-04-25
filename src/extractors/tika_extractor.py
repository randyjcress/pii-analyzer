import os
from typing import Dict, Optional, Tuple

import requests
import tika
from tika import parser

from ..utils.logger import app_logger as logger

# Default Tika server settings
DEFAULT_TIKA_SERVER = "http://localhost:9998"

# Initialize Tika
tika.initVM()

class TikaExtractor:
    """Text extraction using Apache Tika."""
    
    def __init__(self, tika_server: Optional[str] = None):
        """Initialize Tika extractor.
        
        Args:
            tika_server: Tika server URL (default: from env or http://localhost:9998)
        """
        self.tika_server = tika_server or os.environ.get("TIKA_SERVER_ENDPOINT", DEFAULT_TIKA_SERVER)
        
    def is_tika_available(self) -> bool:
        """Check if Tika server is available.
        
        Returns:
            bool: True if Tika server is available
        """
        try:
            response = requests.get(f"{self.tika_server}/tika", timeout=5)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Tika server not available: {e}")
            return False
    
    def extract_text(self, file_path: str) -> Tuple[str, Dict]:
        """Extract text from file using Tika.
        
        Args:
            file_path: Path to file
            
        Returns:
            Tuple[str, Dict]: Extracted text and metadata
            
        Raises:
            ConnectionError: If Tika server is not available
            ValueError: If file does not exist
        """
        if not os.path.exists(file_path):
            raise ValueError(f"File not found: {file_path}")
            
        if not self.is_tika_available():
            raise ConnectionError(f"Tika server not available at {self.tika_server}")
        
        logger.info(f"Extracting text from {file_path} using Tika")
        
        try:
            parsed = parser.from_file(file_path, serverEndpoint=self.tika_server)
            
            if not parsed:
                logger.warning(f"Tika returned empty result for {file_path}")
                return "", {}
                
            text = parsed.get("content", "")
            metadata = parsed.get("metadata", {})
            
            # Clean up text
            if text:
                text = text.strip()
                
            logger.info(f"Successfully extracted {len(text)} characters from {file_path}")
            return text, metadata
            
        except Exception as e:
            logger.error(f"Error extracting text from {file_path}: {e}")
            raise
            
    def extract_with_ocr_check(self, file_path: str) -> Tuple[str, Dict, bool]:
        """Extract text with check for OCR need.
        
        Args:
            file_path: Path to file
            
        Returns:
            Tuple[str, Dict, bool]: Extracted text, metadata, and whether OCR is needed
        """
        try:
            text, metadata = self.extract_text(file_path)
            
            # If file is PDF and text content is missing or very short, may need OCR
            is_pdf = file_path.lower().endswith('.pdf')
            needs_ocr = is_pdf and (not text or len(text.strip()) < 50)
            
            if needs_ocr:
                logger.info(f"Text content minimal, may need OCR for {file_path}")
                
            return text, metadata, needs_ocr
            
        except Exception as e:
            logger.error(f"Error in extract_with_ocr_check: {e}")
            return "", {}, True  # Suggest OCR as fallback 