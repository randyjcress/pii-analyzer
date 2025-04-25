import pytest
from unittest.mock import MagicMock, patch

from src.anonymizers.presidio_anonymizer import PresidioAnonymizer

# Sample entities for testing
SAMPLE_ENTITIES = [
    {
        "entity_type": "PERSON",
        "start": 0,
        "end": 10,
        "score": 0.85,
        "text": "John Smith"
    },
    {
        "entity_type": "EMAIL_ADDRESS",
        "start": 20,
        "end": 42,
        "score": 0.95,
        "text": "john.smith@example.com"
    },
    {
        "entity_type": "PHONE_NUMBER",
        "start": 50,
        "end": 62,
        "score": 0.9,
        "text": "555-123-4567"
    }
]

# Sample text for testing
SAMPLE_TEXT = "John Smith has email john.smith@example.com and phone 555-123-4567."

class MockAnonymizerResult:
    """Mock class for Presidio AnonymizerResult."""
    
    def __init__(self, text):
        self.text = text
        self.items = []

class MockAnonymizedEntity:
    """Mock class for Presidio AnonymizedEntity."""
    
    def __init__(self, entity_type, start, end, operator):
        self.entity_type = entity_type
        self.start = start
        self.end = end
        self.operator = operator

class TestPresidioAnonymizer:
    """Tests for PresidioAnonymizer class."""
    
    def test_initialization(self):
        """Test anonymizer initialization."""
        # Test with default method
        anonymizer = PresidioAnonymizer()
        assert anonymizer.default_method == "replace"
        
        # Test with custom method
        anonymizer = PresidioAnonymizer(default_method="mask")
        assert anonymizer.default_method == "mask"
        
        # Test with invalid method
        with pytest.raises(ValueError):
            PresidioAnonymizer(default_method="invalid_method")
    
    def test_convert_to_recognizer_results(self):
        """Test conversion of entity dictionaries to RecognizerResult objects."""
        anonymizer = PresidioAnonymizer()
        results = anonymizer._convert_to_recognizer_results(SAMPLE_ENTITIES)
        
        # Verify the conversion
        assert len(results) == 3
        assert results[0].entity_type == "PERSON"
        assert results[0].start == 0
        assert results[0].end == 10
        assert results[0].score == 0.85
        
        assert results[1].entity_type == "EMAIL_ADDRESS"
        assert results[2].entity_type == "PHONE_NUMBER"
    
    @patch('src.anonymizers.presidio_anonymizer.AnonymizerEngine.anonymize')
    def test_anonymize_text(self, mock_anonymize):
        """Test text anonymization."""
        # Set up mock result
        mock_result = MockAnonymizerResult("XXXXX XXXXX has email <EMAIL_ADDRESS> and phone <PHONE_NUMBER>.")
        
        # Add anonymized items
        person_item = MockAnonymizedEntity("PERSON", 0, 10, "replace")
        email_item = MockAnonymizedEntity("EMAIL_ADDRESS", 20, 42, "replace")
        phone_item = MockAnonymizedEntity("PHONE_NUMBER", 50, 62, "replace")
        
        mock_result.items = [person_item, email_item, phone_item]
        mock_anonymize.return_value = mock_result
        
        # Create anonymizer and anonymize text
        anonymizer = PresidioAnonymizer()
        anonymized_text, metadata = anonymizer.anonymize_text(
            SAMPLE_TEXT,
            SAMPLE_ENTITIES
        )
        
        # Verify the result
        assert anonymized_text == "XXXXX XXXXX has email <EMAIL_ADDRESS> and phone <PHONE_NUMBER>."
        assert metadata["anonymized_count"] == 3
        assert len(metadata["details"]) == 3
        
        # Verify the details
        details = metadata["details"]
        assert details[0]["entity_type"] == "PERSON"
        assert details[1]["entity_type"] == "EMAIL_ADDRESS"
        assert details[2]["entity_type"] == "PHONE_NUMBER"
    
    @patch('src.anonymizers.presidio_anonymizer.AnonymizerEngine.anonymize')
    def test_anonymize_text_with_custom_method(self, mock_anonymize):
        """Test text anonymization with custom method."""
        # Set up mock result
        mock_result = MockAnonymizerResult("**** ***** has email ********************** and phone ************.")
        
        # Add anonymized items
        person_item = MockAnonymizedEntity("PERSON", 0, 10, "mask")
        email_item = MockAnonymizedEntity("EMAIL_ADDRESS", 20, 42, "mask")
        phone_item = MockAnonymizedEntity("PHONE_NUMBER", 50, 62, "mask")
        
        mock_result.items = [person_item, email_item, phone_item]
        mock_anonymize.return_value = mock_result
        
        # Create anonymizer and anonymize text with custom method
        anonymizer = PresidioAnonymizer()
        anonymized_text, metadata = anonymizer.anonymize_text(
            SAMPLE_TEXT,
            SAMPLE_ENTITIES,
            method="mask"
        )
        
        # Verify the result
        assert anonymized_text == "**** ***** has email ********************** and phone ************."
        assert metadata["anonymized_count"] == 3
    
    @patch('src.anonymizers.presidio_anonymizer.AnonymizerEngine.anonymize')
    def test_anonymize_text_with_custom_operators(self, mock_anonymize):
        """Test text anonymization with custom operators per entity type."""
        # Set up mock result
        mock_result = MockAnonymizerResult("XXXXX XXXXX has email ********************** and phone <PHONE_NUMBER>.")
        
        # Add anonymized items
        person_item = MockAnonymizedEntity("PERSON", 0, 10, "replace")
        email_item = MockAnonymizedEntity("EMAIL_ADDRESS", 20, 42, "mask")
        phone_item = MockAnonymizedEntity("PHONE_NUMBER", 50, 62, "redact")
        
        mock_result.items = [person_item, email_item, phone_item]
        mock_anonymize.return_value = mock_result
        
        # Create anonymizer and anonymize text with custom operators
        anonymizer = PresidioAnonymizer()
        
        # Define custom operators
        custom_operators = {
            "PERSON": {"method": "replace"},
            "EMAIL_ADDRESS": {"method": "mask"},
            "PHONE_NUMBER": {"method": "redact"}
        }
        
        anonymized_text, metadata = anonymizer.anonymize_text(
            SAMPLE_TEXT,
            SAMPLE_ENTITIES,
            operators=custom_operators
        )
        
        # Verify the result
        assert anonymized_text == "XXXXX XXXXX has email ********************** and phone <PHONE_NUMBER>."
        assert metadata["anonymized_count"] == 3
    
    def test_anonymize_text_with_empty_text(self):
        """Test behavior with empty text."""
        anonymizer = PresidioAnonymizer()
        anonymized_text, metadata = anonymizer.anonymize_text("", SAMPLE_ENTITIES)
        
        # Verify that the text was returned as-is with empty metadata
        assert anonymized_text == ""
        assert metadata == {}
    
    def test_anonymize_text_with_no_entities(self):
        """Test behavior with no entities to anonymize."""
        anonymizer = PresidioAnonymizer()
        anonymized_text, metadata = anonymizer.anonymize_text(SAMPLE_TEXT, [])
        
        # Verify that the text was returned as-is with empty metadata
        assert anonymized_text == SAMPLE_TEXT
        assert metadata == {}
    
    @patch('src.anonymizers.presidio_anonymizer.AnonymizerEngine.anonymize')
    def test_anonymize_with_error(self, mock_anonymize):
        """Test behavior when an error occurs during anonymization."""
        # Set up the mock to raise an exception
        mock_anonymize.side_effect = Exception("Anonymization error")
        
        # Create anonymizer and anonymize text (should handle the exception)
        anonymizer = PresidioAnonymizer()
        anonymized_text, metadata = anonymizer.anonymize_text(
            SAMPLE_TEXT,
            SAMPLE_ENTITIES
        )
        
        # Verify that the text was returned as-is with error metadata
        assert anonymized_text == SAMPLE_TEXT
        assert "error" in metadata
    
    @patch('src.anonymizers.presidio_anonymizer.PresidioAnonymizer.anonymize_text')
    def test_anonymize_batch(self, mock_anonymize_text):
        """Test batch anonymization."""
        # Set up mock results for two texts
        mock_anonymize_text.side_effect = [
            ("XXXXX XXXXX", {"anonymized_count": 1}),
            ("<EMAIL_ADDRESS>", {"anonymized_count": 1})
        ]
        
        # Create anonymizer and anonymize batch
        anonymizer = PresidioAnonymizer()
        results = anonymizer.anonymize_batch(
            texts=["John Smith", "john.smith@example.com"],
            batch_entities=[
                [{"entity_type": "PERSON", "start": 0, "end": 10, "score": 0.85}],
                [{"entity_type": "EMAIL_ADDRESS", "start": 0, "end": 22, "score": 0.95}]
            ]
        )
        
        # Verify the results
        assert len(results) == 2
        assert results[0][0] == "XXXXX XXXXX"
        assert results[1][0] == "<EMAIL_ADDRESS>"
        
        # Verify that anonymize_text was called twice with correct arguments
        assert mock_anonymize_text.call_count == 2
    
    def test_anonymize_batch_with_mismatched_lengths(self):
        """Test batch anonymization with mismatched lengths."""
        anonymizer = PresidioAnonymizer()
        results = anonymizer.anonymize_batch(
            texts=["Text 1", "Text 2", "Text 3"],
            batch_entities=[
                [{"entity_type": "PERSON", "start": 0, "end": 5, "score": 0.85}],
                [{"entity_type": "EMAIL_ADDRESS", "start": 0, "end": 10, "score": 0.95}]
            ]
        )
        
        # Verify that error metadata was returned for each text
        assert len(results) == 3
        for text, metadata in results:
            assert "error" in metadata 