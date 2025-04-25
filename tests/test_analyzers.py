import pytest
from unittest.mock import MagicMock, patch

from src.analyzers.presidio_analyzer import PresidioAnalyzer

# Sample text for testing
SAMPLE_TEXT = """
John Smith lives at 123 Main St, Anytown CA 90210.
His email is john.smith@example.com and his phone number is 555-123-4567.
His credit card is 4111-1111-1111-1111 and his SSN is 123-45-6789.
"""

# Sample analyzer results for mocking
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
        "start": 58,
        "end": 80,
        "score": 0.95,
        "text": "john.smith@example.com"
    },
    {
        "entity_type": "PHONE_NUMBER",
        "start": 104,
        "end": 116,
        "score": 0.9,
        "text": "555-123-4567"
    },
    {
        "entity_type": "CREDIT_CARD",
        "start": 136,
        "end": 155,
        "score": 0.98,
        "text": "4111-1111-1111-1111"
    },
    {
        "entity_type": "US_SSN",
        "start": 170,
        "end": 180,
        "score": 0.95,
        "text": "123-45-6789"
    }
]

class MockRecognizerResult:
    """Mock class for Presidio AnalyzerResult."""
    
    def __init__(self, entity_type, start, end, score):
        self.entity_type = entity_type
        self.start = start
        self.end = end
        self.score = score

class TestPresidioAnalyzer:
    """Tests for PresidioAnalyzer class."""
    
    @patch('src.analyzers.presidio_analyzer.RecognizerRegistry')
    @patch('src.analyzers.presidio_analyzer.NlpEngineProvider')
    @patch('src.analyzers.presidio_analyzer.spacy.load')
    def test_initialization(self, mock_spacy_load, mock_nlp_provider, mock_registry):
        """Test analyzer initialization."""
        # Mock the supported entities
        mock_registry_instance = mock_registry.return_value
        mock_registry_instance.get_supported_entities.return_value = [
            "PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "CREDIT_CARD", "US_SSN"
        ]
        
        # Create analyzer
        analyzer = PresidioAnalyzer()
        
        # Verify the spaCy model was loaded
        mock_spacy_load.assert_called_once_with("en_core_web_lg")
        
        # Verify the registry was set up properly
        mock_registry_instance.load_predefined_recognizers.assert_called_once_with(
            languages=["en"]
        )
    
    @patch('src.analyzers.presidio_analyzer.AnalyzerEngine.analyze')
    @patch('src.analyzers.presidio_analyzer.RecognizerRegistry')
    @patch('src.analyzers.presidio_analyzer.NlpEngineProvider')
    @patch('src.analyzers.presidio_analyzer.spacy.load')
    def test_analyze_text(self, mock_spacy_load, mock_nlp_provider, mock_registry, mock_analyze):
        """Test text analysis for PII entities."""
        # Set up mock results
        mock_results = []
        for entity in SAMPLE_ENTITIES:
            mock_result = MockRecognizerResult(
                entity_type=entity["entity_type"],
                start=entity["start"],
                end=entity["end"],
                score=entity["score"]
            )
            mock_results.append(mock_result)
        
        mock_analyze.return_value = mock_results
        
        # Create analyzer and analyze text
        analyzer = PresidioAnalyzer()
        results = analyzer.analyze_text(SAMPLE_TEXT)
        
        # Verify the analyze method was called with the correct arguments
        mock_analyze.assert_called_once_with(
            text=SAMPLE_TEXT,
            entities=None,
            language="en",
            score_threshold=0.7
        )
        
        # Verify the results
        assert len(results) == 5
        assert results[0]["entity_type"] == "PERSON"
        assert results[1]["entity_type"] == "EMAIL_ADDRESS"
        
        # Verify all fields are present in each result
        for i, entity in enumerate(SAMPLE_ENTITIES):
            assert results[i]["entity_type"] == entity["entity_type"]
            assert results[i]["start"] == entity["start"]
            assert results[i]["end"] == entity["end"]
            assert results[i]["score"] == entity["score"]
    
    @patch('src.analyzers.presidio_analyzer.AnalyzerEngine.analyze')
    @patch('src.analyzers.presidio_analyzer.RecognizerRegistry')
    @patch('src.analyzers.presidio_analyzer.NlpEngineProvider')
    @patch('src.analyzers.presidio_analyzer.spacy.load')
    def test_analyze_text_with_specific_entities(self, mock_spacy_load, mock_nlp_provider, 
                                               mock_registry, mock_analyze):
        """Test text analysis with specific entity types."""
        # Create analyzer and analyze text with specific entities
        analyzer = PresidioAnalyzer()
        analyzer.analyze_text(
            SAMPLE_TEXT,
            entities=["PERSON", "EMAIL_ADDRESS"]
        )
        
        # Verify the analyze method was called with the specified entities
        mock_analyze.assert_called_once_with(
            text=SAMPLE_TEXT,
            entities=["PERSON", "EMAIL_ADDRESS"],
            language="en",
            score_threshold=0.7
        )
    
    @patch('src.analyzers.presidio_analyzer.BatchAnalyzerEngine.analyze_dict')
    @patch('src.analyzers.presidio_analyzer.RecognizerRegistry')
    @patch('src.analyzers.presidio_analyzer.NlpEngineProvider')
    @patch('src.analyzers.presidio_analyzer.spacy.load')
    def test_analyze_batch(self, mock_spacy_load, mock_nlp_provider, 
                         mock_registry, mock_analyze_dict):
        """Test batch text analysis."""
        # Set up mock results for two texts
        batch_results = [
            [MockRecognizerResult("PERSON", 0, 10, 0.85)],
            [MockRecognizerResult("EMAIL_ADDRESS", 0, 22, 0.95)]
        ]
        mock_analyze_dict.return_value = batch_results
        
        # Create analyzer and analyze batch
        analyzer = PresidioAnalyzer()
        results = analyzer.analyze_batch(
            texts=["John Smith", "john.smith@example.com"]
        )
        
        # Verify the analyze_dict method was called correctly
        mock_analyze_dict.assert_called_once()
        
        # Verify the results
        assert len(results) == 2
        assert results[0][0]["entity_type"] == "PERSON"
        assert results[1][0]["entity_type"] == "EMAIL_ADDRESS"
    
    @patch('src.analyzers.presidio_analyzer.AnalyzerEngine.get_supported_entities')
    @patch('src.analyzers.presidio_analyzer.RecognizerRegistry')
    @patch('src.analyzers.presidio_analyzer.NlpEngineProvider')
    @patch('src.analyzers.presidio_analyzer.spacy.load')
    def test_get_supported_entities(self, mock_spacy_load, mock_nlp_provider, 
                                  mock_registry, mock_get_supported):
        """Test getting supported entity types."""
        # Set up mock supported entities
        mock_get_supported.return_value = [
            "PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "CREDIT_CARD", "US_SSN"
        ]
        
        # Create analyzer and get supported entities
        analyzer = PresidioAnalyzer()
        entities = analyzer.get_supported_entities()
        
        # Verify the result
        assert entities == [
            "PERSON", "EMAIL_ADDRESS", "PHONE_NUMBER", "CREDIT_CARD", "US_SSN"
        ]
        mock_get_supported.assert_called_once()
    
    @patch('src.analyzers.presidio_analyzer.AnalyzerEngine.analyze')
    @patch('src.analyzers.presidio_analyzer.RecognizerRegistry')
    @patch('src.analyzers.presidio_analyzer.NlpEngineProvider')
    @patch('src.analyzers.presidio_analyzer.spacy.load')
    def test_analyze_with_empty_text(self, mock_spacy_load, mock_nlp_provider, 
                                   mock_registry, mock_analyze):
        """Test behavior with empty text."""
        # Create analyzer and analyze empty text
        analyzer = PresidioAnalyzer()
        results = analyzer.analyze_text("")
        
        # Verify that analyze was not called and empty results were returned
        mock_analyze.assert_not_called()
        assert results == []
    
    @patch('src.analyzers.presidio_analyzer.AnalyzerEngine.analyze')
    @patch('src.analyzers.presidio_analyzer.RecognizerRegistry')
    @patch('src.analyzers.presidio_analyzer.NlpEngineProvider')
    @patch('src.analyzers.presidio_analyzer.spacy.load')
    def test_analyze_with_error(self, mock_spacy_load, mock_nlp_provider, 
                              mock_registry, mock_analyze):
        """Test behavior when an error occurs during analysis."""
        # Set up the mock to raise an exception
        mock_analyze.side_effect = Exception("Analysis error")
        
        # Create analyzer and analyze text (should handle the exception)
        analyzer = PresidioAnalyzer()
        results = analyzer.analyze_text(SAMPLE_TEXT)
        
        # Verify that empty results were returned
        assert results == [] 