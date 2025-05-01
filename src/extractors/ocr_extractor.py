import os
import tempfile
import psutil
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
            threads: Number of threads to use (0=auto based on CPU cores and system resources)
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
        
        # Determine optimal thread count if auto mode is requested
        if threads <= 0:
            self.threads = self._determine_optimal_threads()
        else:
            self.threads = threads
        
        # Configure threading lock for tesseract (which is not thread-safe)
        self._tesseract_lock = threading.Lock()
        
        logger.info(f"OCR Extractor initialized with DPI={dpi}, lang={lang}, threads={self.threads}")
        
    def _determine_optimal_threads(self) -> int:
        """Determine the optimal number of threads based on system resources.
        
        Returns:
            int: Optimal number of threads to use
        """
        try:
            # Get CPU info
            cpu_count = os.cpu_count() or 1
            
            # Check available memory
            available_memory_gb = psutil.virtual_memory().available / (1024 * 1024 * 1024)
            
            # Tesseract is memory intensive, so we need to consider memory constraints
            # Each OCR thread can use approximately 200-300MB of memory
            # We'll use a conservative estimate of 500MB per thread to be safe
            memory_based_thread_limit = max(1, int(available_memory_gb / 0.5))
            
            # Balance CPU cores and memory constraints
            optimal_threads = min(cpu_count, memory_based_thread_limit)
            
            # If we have a lot of cores, we might not want to use all of them
            # to avoid system unresponsiveness
            if cpu_count > 8:
                # Use 75% of available cores, but at least 6
                cpu_limited_threads = max(6, int(cpu_count * 0.75))
                optimal_threads = min(optimal_threads, cpu_limited_threads)
            
            logger.debug(f"Optimal thread calculation: CPU cores={cpu_count}, "
                         f"Memory-based limit={memory_based_thread_limit}, "
                         f"Selected={optimal_threads}")
            
            return optimal_threads
            
        except Exception as e:
            # Fallback to a simple calculation if psutil fails
            logger.warning(f"Error determining optimal threads: {e}. Using CPU count.")
            return os.cpu_count() or 1
    
    def _calculate_threads_for_file(self, file_size_bytes: int, num_pages: int = 1) -> int:
        """Calculate optimal thread count for a specific file based on its size.
        
        For very large files, we might want to reduce the number of threads to 
        avoid memory issues.
        
        Args:
            file_size_bytes: Size of the file in bytes
            num_pages: Number of pages in the document
            
        Returns:
            int: Recommended number of threads
        """
        # Base thread count from the instance
        base_threads = self.threads
        
        # For small files, a single thread might be more efficient
        if file_size_bytes < 1024 * 1024:  # Less than 1MB
            return 1
        
        # For very large files, reduce threads to avoid memory pressure
        file_size_mb = file_size_bytes / (1024 * 1024)
        
        # Very large files (>100MB) or many pages might need fewer threads
        if file_size_mb > 100 or num_pages > 50:
            # Reduce threads proportionally to size, but use at least 2 threads
            size_factor = min(1.0, 100 / file_size_mb)
            return max(2, int(base_threads * size_factor))
            
        # For very high page counts, we need to consider total processing time
        if num_pages > 100:
            # Ensure we have at least one thread for every 50 pages
            return max(base_threads, num_pages // 50)
            
        return base_threads
        
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
            # Get file size to optimize thread count
            file_size = os.path.getsize(pdf_path)
            
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
                total_pdf_pages = len(images)
                if max_pages and max_pages > 0 and max_pages < total_pdf_pages:
                    logger.info(f"Limiting OCR to first {max_pages} of {total_pdf_pages} pages")
                    images = images[:max_pages]
                
                total_pages = len(images)
                
                # Calculate optimal thread count for this specific file
                optimal_threads = self._calculate_threads_for_file(file_size, total_pages)
                logger.info(f"Processing {total_pages} pages with OCR using {optimal_threads} threads")
                
                # Use multi-threading to process pages in parallel
                all_text = []
                if optimal_threads > 1 and total_pages > 1:
                    # Process pages in parallel
                    with concurrent.futures.ThreadPoolExecutor(max_workers=optimal_threads) as executor:
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
                    "Pages": total_pdf_pages,
                    "ProcessedPages": len(all_text),
                    "OCR": True,
                    "DPI": self.dpi,
                    "Language": self.lang,
                    "ContentType": "application/pdf",
                    "ThreadsUsed": optimal_threads
                }
                
                logger.info(f"Successfully extracted {len(full_text)} characters from {pdf_path}")
                return full_text, metadata
                
        except Exception as e:
            logger.error(f"Error extracting text from PDF {pdf_path}: {e}")
            return "", {"Error": str(e)} 