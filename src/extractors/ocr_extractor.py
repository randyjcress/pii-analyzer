import os
import tempfile
from typing import Dict, List, Optional, Tuple
import concurrent.futures
import threading

import pytesseract
from pdf2image import convert_from_path
from PIL import Image

from ..utils.logger import app_logger as logger

class OCRExtractor:
    """Text extraction using OCR for image-based files."""
    
    def __init__(self, 
                 tesseract_cmd: Optional[str] = None, 
                 lang: str = 'eng',
                 dpi: int = 300,
                 oem: int = 3,  # OCR Engine Mode (0-3)
                 psm: int = 6,  # Page Segmentation Mode (0-13)
                 threads: int = 0):  # Number of threads (0=auto)
        """Initialize OCR extractor.
        
        Args:
            tesseract_cmd: Path to tesseract executable
            lang: OCR language (e.g., 'eng', 'eng+fra')
            dpi: DPI for PDF rendering (higher values = better quality but slower)
            oem: OCR Engine Mode (3=default, 1=neural nets LSTM only)
            psm: Page Segmentation Mode (6=default, 3=auto page segmentation)
            threads: Number of threads to use (0=auto based on CPU cores)
        """
        if tesseract_cmd:
            pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
        
        # Find tesseract automatically if not specified
        if not tesseract_cmd:
            try:
                from shutil import which
                auto_tesseract = which('tesseract')
                if auto_tesseract:
                    pytesseract.pytesseract.tesseract_cmd = auto_tesseract
                    logger.info(f"Automatically found tesseract at: {auto_tesseract}")
            except Exception as e:
                logger.warning(f"Could not auto-detect tesseract: {e}")
        
        self.lang = lang
        self.dpi = dpi
        self.oem = oem
        self.psm = psm
        self.threads = threads if threads > 0 else os.cpu_count() or 1
        
        # Configure threading lock for tesseract (which is not thread-safe)
        self._tesseract_lock = threading.Lock()
        
        logger.info(f"OCR Extractor initialized with DPI={dpi}, lang={lang}, threads={self.threads}")
        
    def _extract_text_from_image(self, image: Image.Image) -> str:
        """Extract text from a single image using OCR.
        
        Args:
            image: PIL Image to extract text from
            
        Returns:
            str: Extracted text
        """
        # Configure tesseract options
        config = f'--oem {self.oem} --psm {self.psm}'
        
        try:
            # Use lock to protect tesseract calls which might not be thread-safe
            with self._tesseract_lock:
                text = pytesseract.image_to_string(image, lang=self.lang, config=config)
            return text
        except Exception as e:
            logger.error(f"OCR extraction error: {e}")
            return ""
            
    def extract_from_image_file(self, image_path: str) -> str:
        """Extract text from an image file using OCR.
        
        Args:
            image_path: Path to image file
            
        Returns:
            str: Extracted text
            
        Raises:
            ValueError: If file does not exist or is not readable
        """
        if not os.path.exists(image_path):
            raise ValueError(f"Image file not found: {image_path}")
            
        logger.info(f"Extracting text from image {image_path} using OCR")
        
        try:
            with Image.open(image_path) as img:
                return self._extract_text_from_image(img)
        except Exception as e:
            logger.error(f"Error extracting text from image {image_path}: {e}")
            return ""
            
    def _process_image(self, img: Image.Image, page_num: int, total_pages: int) -> str:
        """Process a single image from a PDF
        
        Args:
            img: PIL Image to process
            page_num: Page number (1-based)
            total_pages: Total number of pages
            
        Returns:
            str: Extracted text with page marker
        """
        logger.debug(f"Processing page {page_num}/{total_pages}")
        text = self._extract_text_from_image(img)
        return f"\n\n--- PAGE {page_num} ---\n\n{text}"
            
    def extract_from_pdf(self, pdf_path: str, max_pages: Optional[int] = None) -> Tuple[str, Dict]:
        """Extract text from a PDF file using OCR.
        
        Args:
            pdf_path: Path to PDF file
            max_pages: Maximum number of pages to process (None=all)
            
        Returns:
            Tuple[str, Dict]: Extracted text and metadata
            
        Raises:
            ValueError: If file does not exist or is not readable
        """
        if not os.path.exists(pdf_path):
            raise ValueError(f"PDF file not found: {pdf_path}")
            
        logger.info(f"Extracting text from PDF {pdf_path} using OCR")
        
        try:
            # Create temp directory for images
            with tempfile.TemporaryDirectory() as temp_dir:
                # Convert PDF to images with higher quality settings
                logger.info(f"Converting PDF to images with DPI={self.dpi}")
                images = convert_from_path(
                    pdf_path, 
                    dpi=self.dpi,
                    output_folder=temp_dir,
                    fmt="jpeg",
                    thread_count=self.threads
                )
                
                # Limit pages if requested
                if max_pages and max_pages > 0 and max_pages < len(images):
                    logger.info(f"Limiting OCR to first {max_pages} of {len(images)} pages")
                    images = images[:max_pages]
                
                total_pages = len(images)
                logger.info(f"Processing {total_pages} pages with OCR using {self.threads} threads")
                
                # Use multi-threading to process pages in parallel
                all_text = []
                if self.threads > 1 and total_pages > 1:
                    # Process pages in parallel
                    with concurrent.futures.ThreadPoolExecutor(max_workers=self.threads) as executor:
                        # Submit all page processing jobs
                        future_to_page = {
                            executor.submit(self._process_image, img, i+1, total_pages): i 
                            for i, img in enumerate(images)
                        }
                        
                        # Collect results as they complete
                        page_texts = [None] * total_pages
                        for future in concurrent.futures.as_completed(future_to_page):
                            page_idx = future_to_page[future]
                            try:
                                page_texts[page_idx] = future.result()
                            except Exception as e:
                                logger.error(f"Error processing page {page_idx+1}: {e}")
                                page_texts[page_idx] = f"\n\n--- PAGE {page_idx+1} ---\n\n[OCR ERROR]"
                        
                        all_text = page_texts
                else:
                    # Process pages sequentially
                    for i, img in enumerate(images):
                        page_text = self._process_image(img, i+1, total_pages)
                        all_text.append(page_text)
                    
                # Combine text from all pages
                full_text = "\n\n".join(text for text in all_text if text.strip())
                
                # Basic metadata
                metadata = {
                    "Pages": total_pages,
                    "ProcessedPages": len(all_text),
                    "OCR": True,
                    "DPI": self.dpi,
                    "Language": self.lang,
                    "ContentType": "application/pdf"
                }
                
                logger.info(f"Successfully extracted {len(full_text)} characters from {pdf_path}")
                return full_text, metadata
                
        except Exception as e:
            logger.error(f"Error extracting text from PDF {pdf_path}: {e}")
            return "", {"Error": str(e)} 