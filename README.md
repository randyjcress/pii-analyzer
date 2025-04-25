# PII Analyzer

A robust, extensible pipeline for extracting text from various file formats, detecting Personally Identifiable Information (PII), and performing redaction.

## Features

- Text extraction from multiple file formats (DOCX, XLSX, CSV, RTF, PDF, JPG, PNG, TIFF)
- Automatic OCR fallback for image-based files
- PII detection using Microsoft Presidio
- Anonymization of detected PII
- Command-line interface for processing files individually or in batch

## Architecture

![PII Analyzer Architecture](docs/architecture.png)

```
File Input → Text Extraction → PII Analysis → Anonymization → Output
```

## Requirements

- Python 3.11+
- Docker (for Apache Tika)
- Tesseract OCR

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/randyjcress/pii-analyzer.git
cd pii-analyzer
```

### 2. Create a virtual environment

```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Install Spacy language model

```bash
python -m spacy download en_core_web_lg
```

### 5. Start Apache Tika Docker container

```bash
docker-compose up -d
```

## Usage

### Basic Usage

```bash
python -m src.cli analyze --input path/to/file.pdf --output results.json
```

### Batch Processing

```bash
python -m src.cli analyze --input path/to/directory --output output_directory
```

### Redaction

```bash
python -m src.cli redact --input path/to/file.pdf --output redacted.txt
```

### Options

```
--input, -i          Input file or directory
--output, -o         Output file or directory
--format, -f         Output format (json, text, csv)
--entities, -e       Comma-separated list of entities to detect (default: all)
--threshold, -t      Confidence threshold (0-1, default: 0.7)
--anonymize, -a      Anonymization method (mask, replace, hash, redact)
--ocr, -c            Force OCR for text extraction
--verbose, -v        Increase output verbosity
```

## Examples

### Analyzing a PDF file

```bash
python -m src.cli analyze -i documents/contract.pdf -o results.json -e PERSON,EMAIL_ADDRESS,PHONE_NUMBER
```

### Redacting information in a batch of files

```bash
python -m src.cli redact -i documents/ -o redacted/ -a mask
```

## Development

### Project Structure

```
pii-analysis/
├── src/
│   ├── extractors/      # Text extraction modules
│   ├── analyzers/       # PII analysis modules
│   ├── anonymizers/     # PII anonymization modules
│   ├── utils/           # Utility functions
│   └── cli.py           # Command-line interface
├── tests/               # Test modules
├── sample_files/        # Sample files for testing
├── requirements.txt     # Project dependencies
└── README.md            # Project documentation
```

### Running Tests

```bash
pytest
```

### Docker Deployment

```bash
docker-compose up -d
```

## License

[MIT License](LICENSE)

## Acknowledgments

- [Microsoft Presidio](https://github.com/microsoft/presidio)
- [Apache Tika](https://tika.apache.org/)
- [Tesseract OCR](https://github.com/tesseract-ocr/tesseract) 