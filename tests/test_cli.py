import os
import json
import pytest
from unittest.mock import patch
from click.testing import CliRunner

from src.cli import cli, analyze, redact
from src.extractors.extractor_factory import ExtractorFactory
from src.analyzers.presidio_analyzer import PresidioAnalyzer
from src.anonymizers.presidio_anonymizer import PresidioAnonymizer

# Sample file paths for testing
SAMPLE_TEXT_FILE = os.path.join("tests", "sample_files", "text_samples", "sample_text.txt")

# Sample test data
SAMPLE_TEXT = "John Smith has email john.smith@example.com and phone 555-123-4567."
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
    }
]
SAMPLE_REDACTED_TEXT = "XXXXX XXXXX has email <EMAIL_ADDRESS> and phone 555-123-4567."


class TestCLI:
    """Tests for CLI interface."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()
    
    @patch('src.cli.ExtractorFactory')
    @patch('src.cli.PresidioAnalyzer')
    def test_analyze_file(self, mock_analyzer_class, mock_extractor_class):
        """Test analyzing a single file."""
        # Mock the extractor and analyzer
        mock_extractor = mock_extractor_class.return_value
        mock_extractor.extract_text.return_value = (SAMPLE_TEXT, {"extraction_method": "tika"})
        
        mock_analyzer = mock_analyzer_class.return_value
        mock_analyzer.analyze_text.return_value = SAMPLE_ENTITIES
        
        # Run CLI command with runner
        with self.runner.isolated_filesystem():
            # Create a sample file
            with open("test.txt", "w") as f:
                f.write(SAMPLE_TEXT)
            
            # Run the analyze command
            result = self.runner.invoke(cli, ["analyze", "-i", "test.txt", "-o", "output.json"])
            
            # Check that the command executed successfully
            assert result.exit_code == 0
            
            # Verify that extractor and analyzer were called correctly
            mock_extractor.extract_text.assert_called_once_with("test.txt", force_ocr=False)
            mock_analyzer.analyze_text.assert_called_once()
            
            # Check that output file was created
            assert os.path.exists("output.json")
            
            # Verify output content
            with open("output.json", "r") as f:
                output = json.load(f)
                assert output["file_path"] == "test.txt"
                assert len(output["entities"]) == 2
                assert output["metadata"]["extraction_method"] == "tika"
    
    @patch('src.cli.ExtractorFactory')
    @patch('src.cli.PresidioAnalyzer')
    @patch('src.cli.PresidioAnonymizer')
    def test_redact_file(self, mock_anonymizer_class, mock_analyzer_class, mock_extractor_class):
        """Test redacting a single file."""
        # Mock the extractor, analyzer, and anonymizer
        mock_extractor = mock_extractor_class.return_value
        mock_extractor.extract_text.return_value = (SAMPLE_TEXT, {"extraction_method": "tika"})
        
        mock_analyzer = mock_analyzer_class.return_value
        mock_analyzer.analyze_text.return_value = SAMPLE_ENTITIES
        
        mock_anonymizer = mock_anonymizer_class.return_value
        mock_anonymizer.anonymize_text.return_value = (
            SAMPLE_REDACTED_TEXT, 
            {"anonymized_count": 2}
        )
        
        # Run CLI command with runner
        with self.runner.isolated_filesystem():
            # Create a sample file
            with open("test.txt", "w") as f:
                f.write(SAMPLE_TEXT)
            
            # Run the redact command
            result = self.runner.invoke(cli, ["redact", "-i", "test.txt", "-o", "redacted.txt"])
            
            # Check that the command executed successfully
            assert result.exit_code == 0
            
            # Verify that extractor, analyzer, and anonymizer were called correctly
            mock_extractor.extract_text.assert_called_once_with("test.txt", force_ocr=False)
            mock_analyzer.analyze_text.assert_called_once()
            mock_anonymizer.anonymize_text.assert_called_once()
            
            # Check that output file was created
            assert os.path.exists("redacted.txt")
            
            # Verify output content
            with open("redacted.txt", "r") as f:
                output = f.read()
                assert output == SAMPLE_REDACTED_TEXT
    
    @patch('src.cli.ExtractorFactory')
    @patch('src.cli.PresidioAnalyzer')
    @patch('src.cli.find_files')
    def test_analyze_directory(self, mock_find_files, mock_analyzer_class, mock_extractor_class):
        """Test analyzing a directory of files."""
        # Mock find_files to return a list of files
        mock_find_files.return_value = ["file1.txt", "file2.txt"]
        
        # Mock the extractor and analyzer
        mock_extractor = mock_extractor_class.return_value
        mock_extractor.extract_text.return_value = (SAMPLE_TEXT, {"extraction_method": "tika"})
        
        mock_analyzer = mock_analyzer_class.return_value
        mock_analyzer.analyze_text.return_value = SAMPLE_ENTITIES
        
        # Run CLI command with runner
        with self.runner.isolated_filesystem():
            # Create a directory
            os.makedirs("input_dir")
            os.makedirs("output_dir")
            
            # Run the analyze command on the directory
            result = self.runner.invoke(cli, ["analyze", "-i", "input_dir", "-o", "output_dir"])
            
            # Check that the command executed successfully
            assert result.exit_code == 0
            
            # Verify that find_files was called with the correct arguments
            mock_find_files.assert_called_once()
            
            # Verify that extract_text and analyze_text were called for each file
            assert mock_extractor.extract_text.call_count == 2
            assert mock_analyzer.analyze_text.call_count == 2
    
    @patch('src.cli.ExtractorFactory')
    @patch('src.cli.PresidioAnalyzer')
    def test_analyze_with_specific_entities(self, mock_analyzer_class, mock_extractor_class):
        """Test analyzing with specific entity types."""
        # Mock the extractor and analyzer
        mock_extractor = mock_extractor_class.return_value
        mock_extractor.extract_text.return_value = (SAMPLE_TEXT, {"extraction_method": "tika"})
        
        mock_analyzer = mock_analyzer_class.return_value
        mock_analyzer.analyze_text.return_value = [SAMPLE_ENTITIES[0]]  # Only return PERSON entity
        
        # Run CLI command with runner and specify entities
        with self.runner.isolated_filesystem():
            # Create a sample file
            with open("test.txt", "w") as f:
                f.write(SAMPLE_TEXT)
            
            # Run the analyze command with specific entities
            result = self.runner.invoke(cli, [
                "analyze", 
                "-i", "test.txt", 
                "-o", "output.json",
                "-e", "PERSON"
            ])
            
            # Check that the command executed successfully
            assert result.exit_code == 0
            
            # Verify that analyzer was called with the correct entities
            mock_analyzer.analyze_text.assert_called_once()
            call_args = mock_analyzer.analyze_text.call_args[1]
            assert call_args["entities"] == ["PERSON"]
    
    @patch('src.cli.ExtractorFactory')
    @patch('src.cli.PresidioAnalyzer')
    def test_analyze_with_custom_threshold(self, mock_analyzer_class, mock_extractor_class):
        """Test analyzing with custom confidence threshold."""
        # Mock the extractor and analyzer
        mock_extractor = mock_extractor_class.return_value
        mock_extractor.extract_text.return_value = (SAMPLE_TEXT, {"extraction_method": "tika"})
        
        mock_analyzer = mock_analyzer_class.return_value
        
        # Run CLI command with runner and specify threshold
        with self.runner.isolated_filesystem():
            # Create a sample file
            with open("test.txt", "w") as f:
                f.write(SAMPLE_TEXT)
            
            # Run the analyze command with custom threshold
            result = self.runner.invoke(cli, [
                "analyze", 
                "-i", "test.txt", 
                "-o", "output.json",
                "-t", "0.9"
            ])
            
            # Check that the command executed successfully
            assert result.exit_code == 0
            
            # Verify that analyzer was created with the correct threshold
            mock_analyzer_class.assert_called_once_with(score_threshold=0.9)
    
    @patch('src.cli.ExtractorFactory')
    @patch('src.cli.PresidioAnalyzer')
    @patch('src.cli.PresidioAnonymizer')
    def test_redact_with_custom_method(self, mock_anonymizer_class, mock_analyzer_class, mock_extractor_class):
        """Test redacting with custom anonymization method."""
        # Mock the extractor, analyzer, and anonymizer
        mock_extractor = mock_extractor_class.return_value
        mock_extractor.extract_text.return_value = (SAMPLE_TEXT, {"extraction_method": "tika"})
        
        mock_analyzer = mock_analyzer_class.return_value
        mock_analyzer.analyze_text.return_value = SAMPLE_ENTITIES
        
        mock_anonymizer = mock_anonymizer_class.return_value
        
        # Run CLI command with runner and specify anonymization method
        with self.runner.isolated_filesystem():
            # Create a sample file
            with open("test.txt", "w") as f:
                f.write(SAMPLE_TEXT)
            
            # Run the redact command with custom method
            result = self.runner.invoke(cli, [
                "redact", 
                "-i", "test.txt", 
                "-o", "redacted.txt",
                "-a", "mask"
            ])
            
            # Check that the command executed successfully
            assert result.exit_code == 0
            
            # Verify that anonymizer was created with the correct method
            mock_anonymizer_class.assert_called_once_with(default_method="mask")
    
    def test_analyze_nonexistent_file(self):
        """Test analyzing a file that doesn't exist."""
        # Run CLI command with runner
        result = self.runner.invoke(cli, ["analyze", "-i", "nonexistent.txt"])
        
        # Check that the command failed with non-zero exit code
        assert result.exit_code != 0
        
    def test_serve_command(self):
        """Test serve command (should not be implemented yet)."""
        # Run the serve command
        result = self.runner.invoke(cli, ["serve"])
        
        # Check that the command executed successfully
        assert result.exit_code == 0
        
        # Verify that the output indicates the API server is not implemented
        assert "not implemented yet" in result.output 