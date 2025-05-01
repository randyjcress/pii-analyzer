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

### 3. Install the package

You can install the package in one of two ways:

#### Option A: Install for development

```bash
pip install -e .
```

This installs the package in development mode, allowing you to modify the code and see changes immediately.

#### Option B: Install from the repository

```bash
pip install .
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
# Run the main PII analyzer
pii-analyzer -i path/to/file.pdf -o results.json

# You can also run the script directly
python pii_analyzer.py -i path/to/file.pdf -o results.json
```

### Batch Processing

```bash
pii-analyzer -i path/to/directory -o output.json
```

### NC Breach Analysis

After generating analysis results:

```bash
python strict_nc_breach_pii.py analysis_results.json
```

### Redaction

For redaction functionality, use the src.cli directly:

```bash
python -m src.cli redact --input path/to/file.pdf --output redacted.txt
```

### Options

```
--input, -i          Input file or directory
--output, -o         Output file or directory
--entities, -e       Comma-separated list of entities to detect (default: all)
--threshold, -t      Confidence threshold (0-1, default: 0.7)
--debug              Show detailed debug information
--ocr                Force OCR for text extraction
--ocr-dpi            DPI for OCR
--ocr-threads        Number of OCR threads (0=auto)
--max-pages          Maximum pages per PDF
--sample             Analyze only a sample of files
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
├── pii_analyzer.py      # Main entry point
├── fix_enhanced_cli.py  # Enhanced CLI implementation
├── strict_nc_breach_pii.py # NC breach notification analysis
├── setup.py            # Package installation configuration
├── requirements.txt     # Project dependencies
└── README.md            # Project documentation
```

## License

[MIT License](LICENSE)

## Acknowledgments

- [Microsoft Presidio](https://github.com/microsoft/presidio)
- [Apache Tika](https://tika.apache.org/)
- [Tesseract OCR](https://github.com/tesseract-ocr/tesseract) 