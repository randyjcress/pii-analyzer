import os
import tempfile
from typing import Dict, List, Optional, Tuple

import pytesseract
from pdf2image import convert_from_path
from PIL import Image

from ..utils.logger import app_logger as logger

class OCRExtractor:
    """Text extraction using OCR for image-based files."""
    
    def __init__(self, 
                 tesseract_cmd: Optional[str] = None, 
                 lang: str = 'eng',
                 dpi: int = 300):
        """Initialize OCR extractor.
        
        Args:
            tesseract_cmd: Path to tesseract executable
            lang: OCR language
            dpi: DPI for PDF rendering
        """
        if tesseract_cmd:
            pytesseract.pytesseract.tesseract_cmd = tesseract_cmd
            
        self.lang = lang
        self.dpi = dpi
        
    def _extract_text_from_image(self, image: Image.Image) -> str:
        """Extract text from a single image using OCR.
        
        Args:
            image: PIL Image to extract text from
            
        Returns:
            str: Extracted text
        """
        try:
            text = pytesseract.image_to_string(image, lang=self.lang)
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
            
    def extract_from_pdf(self, pdf_path: str) -> Tuple[str, Dict]:
        """Extract text from a PDF file using OCR.
        
        Args:
            pdf_path: Path to PDF file
            
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
                # Convert PDF to images
                images = convert_from_path(
                    pdf_path, 
                    dpi=self.dpi,
                    output_folder=temp_dir,
                    fmt="jpeg"
                )
                
                # Extract text from each page
                all_text = []
                for i, img in enumerate(images):
                    logger.debug(f"Processing page {i+1}/{len(images)} of {pdf_path}")
                    text = self._extract_text_from_image(img)
                    all_text.append(text)
                    
                # Combine text from all pages
                full_text = "\n\n".join(text for text in all_text if text.strip())
                
                # Basic metadata
                metadata = {
                    "Pages": len(images),
                    "OCR": True,
                    "ContentType": "application/pdf"
                }
                
                logger.info(f"Successfully extracted {len(full_text)} characters from {pdf_path}")
                return full_text, metadata
                
        except Exception as e:
            logger.error(f"Error extracting text from PDF {pdf_path}: {e}")
            return "", {"Error": str(e)} 