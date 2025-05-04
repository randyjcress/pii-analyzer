import os
from typing import Dict, Optional, Tuple, List

import requests
import tika
from tika import parser

from ..utils.logger import app_logger as logger
from .tika_load_balancer import TikaLoadBalancer

# Default Tika server settings
DEFAULT_TIKA_SERVER = "http://localhost:9998"

# Initialize Tika
tika.initVM()

# Global load balancer instance
_load_balancer = None

def get_load_balancer(tika_servers: Optional[List[str]] = None) -> TikaLoadBalancer:
    """Get the global load balancer instance.
    
    Args:
        tika_servers: Optional list of Tika server URLs to initialize the load balancer
        
    Returns:
        TikaLoadBalancer instance
    """
    global _load_balancer
    if _load_balancer is None:
        _load_balancer = TikaLoadBalancer(tika_servers)
    return _load_balancer

class TikaExtractor:
    """Text extraction using Apache Tika."""
    
    def __init__(self, tika_server: Optional[str] = None, use_load_balancer: bool = True):
        """Initialize Tika extractor.
        
        Args:
            tika_server: Tika server URL (default: from env or http://localhost:9998)
            use_load_balancer: Whether to use load balancing across multiple Tika instances
        """
        self.use_load_balancer = use_load_balancer
        
        if use_load_balancer:
            # Initialize using a list of servers if provided
            tika_servers = None
            if tika_server:
                # If a single server is provided, check for comma-separated list
                if "," in tika_server:
                    tika_servers = [s.strip() for s in tika_server.split(",")]
                else:
                    tika_servers = [tika_server]
            
            self.load_balancer = get_load_balancer(tika_servers)
            # Set a default server for fallback
            self.tika_server = tika_server or os.environ.get("TIKA_SERVER_ENDPOINT", DEFAULT_TIKA_SERVER)
        else:
            # Traditional single-server mode
            self.tika_server = tika_server or os.environ.get("TIKA_SERVER_ENDPOINT", DEFAULT_TIKA_SERVER)
            self.load_balancer = None
        
    def is_tika_available(self) -> bool:
        """Check if Tika server is available.
        
        Returns:
            bool: True if at least one Tika server is available
        """
        if self.use_load_balancer:
            # Check if at least one server is available
            available_servers = self.load_balancer.get_available_servers()
            if not available_servers:
                # Force a health check on all servers
                self.load_balancer.check_all_servers()
                available_servers = self.load_balancer.get_available_servers()
            
            return len(available_servers) > 0
        else:
            # Traditional single-server check
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
            ConnectionError: If no Tika server is available
            ValueError: If file does not exist
        """
        if not os.path.exists(file_path):
            raise ValueError(f"File not found: {file_path}")
        
        if self.use_load_balancer:
            # Get a server from the load balancer
            server = self.load_balancer.get_server()
            if not server:
                raise ConnectionError("No Tika servers available")
            
            logger.info(f"Extracting text from {file_path} using Tika server: {server}")
            
            try:
                parsed = parser.from_file(file_path, serverEndpoint=server)
                
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
                # Mark the server as having an error
                self.load_balancer.mark_server_error(server)
                logger.error(f"Error extracting text from {file_path} using {server}: {e}")
                
                # Try another server if available
                another_server = self.load_balancer.get_server()
                if another_server:
                    logger.info(f"Retrying with alternate Tika server: {another_server}")
                    try:
                        parsed = parser.from_file(file_path, serverEndpoint=another_server)
                        
                        if not parsed:
                            logger.warning(f"Tika returned empty result for {file_path}")
                            return "", {}
                            
                        text = parsed.get("content", "")
                        metadata = parsed.get("metadata", {})
                        
                        # Clean up text
                        if text:
                            text = text.strip()
                            
                        logger.info(f"Successfully extracted {len(text)} characters from {file_path} with alternate server")
                        return text, metadata
                    except Exception as e2:
                        self.load_balancer.mark_server_error(another_server)
                        logger.error(f"Error with alternate Tika server: {e2}")
                        raise
                else:
                    raise
        else:
            # Traditional single-server mode
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
            
    def get_stats(self) -> Dict:
        """Get Tika server statistics if using load balancer.
        
        Returns:
            Dict with Tika server stats or empty dict if not using load balancer
        """
        if self.use_load_balancer and self.load_balancer:
            return self.load_balancer.get_stats()
        return {"mode": "single_server", "server": self.tika_server} 