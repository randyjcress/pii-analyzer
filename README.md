# PII Analyzer

A robust, extensible pipeline for extracting text from various file formats, detecting Personally Identifiable Information (PII), and performing redaction.

## Features

- Text extraction from multiple file formats (DOCX, XLSX, CSV, RTF, PDF, JPG, PNG, TIFF)
- Automatic OCR fallback for image-based files
- PII detection using Microsoft Presidio
- Anonymization of detected PII
- Command-line interface for processing files individually or in batch
- Enhanced CLI with better handling for DOCX files
- NC breach notification analysis for compliance

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

For detailed installation instructions on Ubuntu, see [ubuntu_installation.md](ubuntu_installation.md).

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

### 5. Configure environment variables

```bash
cp .env-template .env
```

Edit the `.env` file to adjust settings as needed.

### 6. Start Apache Tika Docker container

```bash
docker-compose up -d
```

## Usage

### Basic Usage

```bash
python -m src.cli analyze --input path/to/file.pdf --output results.json
```

### Enhanced CLI (Better DOCX Support)

```bash
python fix_enhanced_cli.py -i path/to/file.docx -o results.json
```

### Batch Processing

```bash
python fix_enhanced_cli.py -i path/to/directory -o output.json
```

### NC Breach Analysis

After generating analysis results:

```bash
python strict_nc_breach_pii.py analysis_results.json
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

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and contribution guidelines.

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
├── fix_enhanced_cli.py  # Enhanced CLI with better DOCX support
├── strict_nc_breach_pii.py # NC breach notification analysis
├── requirements.txt     # Project dependencies
└── README.md            # Project documentation
```

## License

[MIT License](LICENSE)

## Acknowledgments

- [Microsoft Presidio](https://github.com/microsoft/presidio)
- [Apache Tika](https://tika.apache.org/)
- [Tesseract OCR](https://github.com/tesseract-ocr/tesseract) 