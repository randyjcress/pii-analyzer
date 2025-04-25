import os
import pytest
from unittest.mock import MagicMock, patch

from src.extractors.tika_extractor import TikaExtractor
from src.extractors.ocr_extractor import OCRExtractor
from src.extractors.extractor_factory import ExtractorFactory

# Sample file paths for testing
SAMPLE_TEXT_FILE = os.path.join("tests", "sample_files", "text_samples", "sample_text.txt")

class TestTikaExtractor:
    """Tests for TikaExtractor class."""
    
    @patch('src.extractors.tika_extractor.parser')
    @patch('src.extractors.tika_extractor.requests.get')
    def test_extract_text(self, mock_requests_get, mock_parser):
        """Test text extraction from a file."""
        # Mock the Tika server response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_requests_get.return_value = mock_response
        
        # Mock the parser response
        mock_parsed = {
            "content": "Sample text content",
            "metadata": {"Content-Type": "text/plain"}
        }
        mock_parser.from_file.return_value = mock_parsed
        
        # Create extractor and extract text
        extractor = TikaExtractor()
        text, metadata = extractor.extract_text(SAMPLE_TEXT_FILE)
        
        # Verify the results
        assert text == "Sample text content"
        assert metadata == {"Content-Type": "text/plain"}
        
        # Verify the parser was called with the correct arguments
        mock_parser.from_file.assert_called_once_with(
            SAMPLE_TEXT_FILE, 
            serverEndpoint="http://localhost:9998"
        )
    
    @patch('src.extractors.tika_extractor.requests.get')
    def test_tika_not_available(self, mock_requests_get):
        """Test behavior when Tika server is not available."""
        # Mock a failed connection to Tika
        mock_requests_get.side_effect = Exception("Connection error")
        
        extractor = TikaExtractor()
        assert not extractor.is_tika_available()
        
        # Test that attempting to extract text raises ConnectionError
        with pytest.raises(ConnectionError):
            extractor.extract_text(SAMPLE_TEXT_FILE)
    
    @patch('src.extractors.tika_extractor.parser')
    @patch('src.extractors.tika_extractor.requests.get')
    def test_extract_with_ocr_check_no_ocr_needed(self, mock_requests_get, mock_parser):
        """Test OCR check functionality when OCR is not needed."""
        # Mock the Tika server response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_requests_get.return_value = mock_response
        
        # Mock the parser response with sufficient text
        mock_parsed = {
            "content": "Plenty of text content that doesn't need OCR",
            "metadata": {"Content-Type": "text/plain"}
        }
        mock_parser.from_file.return_value = mock_parsed
        
        # Create extractor and extract text with OCR check
        extractor = TikaExtractor()
        text, metadata, needs_ocr = extractor.extract_with_ocr_check(SAMPLE_TEXT_FILE)
        
        # Verify the results
        assert text == "Plenty of text content that doesn't need OCR"
        assert metadata == {"Content-Type": "text/plain"}
        assert not needs_ocr

class TestOCRExtractor:
    """Tests for OCRExtractor class."""
    
    @patch('src.extractors.ocr_extractor.pytesseract.image_to_string')
    @patch('src.extractors.ocr_extractor.Image.open')
    def test_extract_from_image_file(self, mock_image_open, mock_image_to_string):
        """Test OCR extraction from an image file."""
        # Mock the image and OCR result
        mock_image = MagicMock()
        mock_image_open.return_value.__enter__.return_value = mock_image
        mock_image_to_string.return_value = "OCR extracted text"
        
        # Create extractor and extract text
        extractor = OCRExtractor()
        text = extractor.extract_from_image_file(SAMPLE_TEXT_FILE)
        
        # Verify the results
        assert text == "OCR extracted text"
        mock_image_to_string.assert_called_once_with(mock_image, lang='eng')
    
    @patch('src.extractors.ocr_extractor.convert_from_path')
    @patch('src.extractors.ocr_extractor.pytesseract.image_to_string')
    def test_extract_from_pdf(self, mock_image_to_string, mock_convert_from_path):
        """Test OCR extraction from a PDF file."""
        # Mock the PDF conversion and OCR results
        mock_image1 = MagicMock()
        mock_image2 = MagicMock()
        mock_convert_from_path.return_value = [mock_image1, mock_image2]
        
        # Set up the mock to return different text for each image
        mock_image_to_string.side_effect = ["Page 1 text", "Page 2 text"]
        
        # Create extractor and extract text
        extractor = OCRExtractor()
        text, metadata = extractor.extract_from_pdf(SAMPLE_TEXT_FILE)
        
        # Verify the results
        assert text == "Page 1 text\n\nPage 2 text"
        assert metadata == {
            "Pages": 2,
            "OCR": True,
            "ContentType": "application/pdf"
        }
        assert mock_image_to_string.call_count == 2

class TestExtractorFactory:
    """Tests for ExtractorFactory class."""
    
    def test_get_extractor_for_text_file(self):
        """Test getting the correct extractor for a text file."""
        factory = ExtractorFactory()
        
        # Test with a txt file (should use Tika)
        extractor_type = factory.get_extractor("test.txt")
        assert extractor_type == "tika"
        
        # Test with an image file (should use OCR)
        extractor_type = factory.get_extractor("test.jpg")
        assert extractor_type == "ocr"
        
        # Test with a PDF file (should use Tika with OCR fallback)
        extractor_type = factory.get_extractor("test.pdf")
        assert extractor_type == "tika_with_ocr_fallback"
        
        # Test with force_ocr=True
        extractor_type = factory.get_extractor("test.txt", force_ocr=True)
        assert extractor_type == "ocr"
    
    def test_get_extractor_unsupported_format(self):
        """Test behavior with an unsupported file format."""
        factory = ExtractorFactory()
        
        # Test with an unsupported file extension
        with pytest.raises(ValueError):
            factory.get_extractor("test.xyz")
    
    @patch('src.extractors.extractor_factory.TikaExtractor.extract_text')
    def test_extract_text_with_tika(self, mock_tika_extract):
        """Test extraction using Tika."""
        # Mock the Tika extractor
        mock_tika_extract.return_value = ("Tika extracted text", {"Content-Type": "text/plain"})
        
        # Create factory and extract text
        factory = ExtractorFactory()
        text, metadata = factory.extract_text("test.txt")
        
        # Verify the results
        assert text == "Tika extracted text"
        assert metadata["extraction_method"] == "tika"
    
    @patch('src.extractors.extractor_factory.OCRExtractor.extract_from_image_file')
    def test_extract_text_with_ocr_image(self, mock_ocr_extract):
        """Test extraction using OCR for an image."""
        # Mock the OCR extractor
        mock_ocr_extract.return_value = "OCR extracted text"
        
        # Create factory and extract text
        factory = ExtractorFactory()
        text, metadata = factory.extract_text("test.jpg")
        
        # Verify the results
        assert text == "OCR extracted text"
        assert metadata["extraction_method"] == "ocr" 