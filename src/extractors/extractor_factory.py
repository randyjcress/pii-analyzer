from typing import Optional, Tuple, Dict, Union, List

from ..utils.file_utils import get_extraction_method, is_supported_format
from ..utils.logger import app_logger as logger
from .tika_extractor import TikaExtractor
from .ocr_extractor import OCRExtractor

class ExtractorFactory:
    """Factory for creating appropriate text extractors."""
    
    def __init__(self, 
                 tika_server: Optional[str] = None,
                 tika_servers: Optional[List[str]] = None,
                 use_load_balancer: bool = True,
                 tesseract_cmd: Optional[str] = None,
                 ocr_lang: str = 'eng',
                 ocr_dpi: int = 300,
                 ocr_threads: int = 0,
                 ocr_oem: int = 3,
                 ocr_psm: int = 6):
        """Initialize extractor factory.
        
        Args:
            tika_server: Tika server URL or comma-separated list of URLs
            tika_servers: List of Tika server URLs (alternative to tika_server)
            use_load_balancer: Whether to use load balancing across multiple Tika instances
            tesseract_cmd: Path to tesseract executable
            ocr_lang: OCR language
            ocr_dpi: DPI for PDF rendering
            ocr_threads: Number of threads for OCR processing (0=auto)
            ocr_oem: OCR Engine Mode (3=default, 1=neural nets LSTM only)
            ocr_psm: Page Segmentation Mode (6=default, 3=auto page segmentation)
        """
        # If tika_servers is provided, it takes precedence over tika_server
        if tika_servers:
            # Convert list to comma-separated string for TikaExtractor
            tika_server = ",".join(tika_servers)
            use_load_balancer = True
            
        self.tika_extractor = TikaExtractor(
            tika_server=tika_server,
            use_load_balancer=use_load_balancer
        )
        
        self.ocr_extractor = OCRExtractor(
            tesseract_cmd=tesseract_cmd,
            lang=ocr_lang,
            dpi=ocr_dpi,
            threads=ocr_threads,
            oem=ocr_oem,
            psm=ocr_psm
        )
        
    def get_extractor(self, file_path: str, force_ocr: bool = False) -> str:
        """Get appropriate extractor type for file.
        
        Args:
            file_path: Path to file
            force_ocr: Whether to force OCR extraction
            
        Returns:
            str: Extractor type ('tika', 'ocr', 'tika_with_ocr_fallback')
            
        Raises:
            ValueError: If file format is not supported
        """
        if not is_supported_format(file_path):
            raise ValueError(f"Unsupported file format: {file_path}")
            
        if force_ocr:
            return 'ocr'
            
        extraction_method = get_extraction_method(file_path)
        
        if extraction_method == 'tika':
            return 'tika'
        elif extraction_method == 'ocr':
            return 'ocr'
        elif extraction_method == 'tika_or_ocr':
            return 'tika_with_ocr_fallback'
        else:
            raise ValueError(f"Unknown extraction method: {extraction_method}")
            
    def extract_text(self, 
                    file_path: str, 
                    force_ocr: bool = False,
                    max_pages: Optional[int] = None) -> Tuple[str, Dict]:
        """Extract text from file using appropriate extractor.
        
        Args:
            file_path: Path to file
            force_ocr: Whether to force OCR extraction
            max_pages: Maximum number of pages to process for PDFs (None=all)
            
        Returns:
            Tuple[str, Dict]: Extracted text and metadata
        """
        extractor_type = self.get_extractor(file_path, force_ocr=force_ocr)
        
        try:
            # Extract using appropriate method
            if extractor_type == 'tika':
                text, metadata = self.tika_extractor.extract_text(file_path)
                metadata['extraction_method'] = 'tika'
                return text, metadata
                
            elif extractor_type == 'ocr':
                # Handle image files
                if file_path.lower().endswith(('.jpg', '.jpeg', '.png', '.tiff', '.tif')):
                    text = self.ocr_extractor.extract_from_image_file(file_path)
                    metadata = {'extraction_method': 'ocr'}
                    return text, metadata
                # Handle PDF files
                else:
                    text, metadata = self.ocr_extractor.extract_from_pdf(file_path, max_pages=max_pages)
                    metadata['extraction_method'] = 'ocr'
                    return text, metadata
                    
            elif extractor_type == 'tika_with_ocr_fallback':
                # Try Tika first, fall back to OCR if needed
                text, metadata, needs_ocr = self.tika_extractor.extract_with_ocr_check(file_path)
                
                if needs_ocr:
                    logger.info(f"Falling back to OCR for {file_path}")
                    text, ocr_metadata = self.ocr_extractor.extract_from_pdf(file_path, max_pages=max_pages)
                    metadata.update(ocr_metadata)
                    metadata['extraction_method'] = 'tika_with_ocr_fallback'
                else:
                    metadata['extraction_method'] = 'tika'
                    
                return text, metadata
                
            else:
                raise ValueError(f"Unsupported extractor type: {extractor_type}")
                
        except Exception as e:
            logger.error(f"Error extracting text from {file_path}: {e}")
            raise
            
    def get_tika_stats(self) -> Dict:
        """Get statistics from the Tika load balancer.
        
        Returns:
            Dict with Tika server statistics
        """
        return self.tika_extractor.get_stats() 