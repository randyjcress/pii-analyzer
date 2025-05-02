# PII Analyzer

A robust, extensible pipeline for extracting text from various file formats, detecting Personally Identifiable Information (PII), and performing redaction.

## Features

- Text extraction from multiple file formats (DOCX, XLSX, CSV, RTF, PDF, JPG, PNG, TIFF)
- Automatic OCR fallback for image-based files
- PII detection using Microsoft Presidio
- Anonymization of detected PII
- Command-line interface for processing files individually or in batch
- Enhanced CLI with better handling for DOCX files
- Multi-threaded processing for handling multiple files simultaneously
- Intelligent thread allocation for OCR optimization
- NC breach notification analysis for compliance

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
# Now uses parallel processing by default for directories
pii-analyzer -i path/to/directory -o output.json

# Force sequential processing if needed
pii-analyzer -i path/to/directory -o output.json --sequential
```

### Explicitly Using Parallel Processing

If you need more control over the parallel processing:

```bash
# Explicitly specify parallel processing with custom worker count
pii-analyzer-parallel -i path/to/directory -o output.json --workers 8

# Or using the script directly
python pii_analyzer_parallel.py -i path/to/directory -o output.json --workers 8
```

### NC Breach Analysis

The PII Analyzer includes a specialized tool for North Carolina breach notification compliance (§75-61) that analyzes PII detection results to identify files that would trigger breach notification requirements.

#### Basic Usage

```bash
# Generate an executive summary report
python strict_nc_breach_pii.py analysis_results.json

# Generate a detailed verbose report
python strict_nc_breach_pii.py analysis_results.json --verbose

# Save the report to a file
python strict_nc_breach_pii.py analysis_results.json -o breach_report.txt

# Generate a JSON report
python strict_nc_breach_pii.py analysis_results.json -f json -o breach_report.json
```

#### Advanced Features

```bash
# Clone high-risk files to a separate directory while maintaining file structure
python strict_nc_breach_pii.py analysis_results.json -c /path/to/high_risk_files

# Adjust confidence threshold for entities
python strict_nc_breach_pii.py analysis_results.json -t 0.8

# Full example with multiple options
python strict_nc_breach_pii.py analysis_results.json -f json -o breach_report.json -t 0.75 -c /path/to/high_risk_files -v
```

#### NC Breach Script Options

```
positional arguments:
  report_file           Path to the PII analysis report JSON file

options:
  -h, --help            Show this help message and exit
  -o OUTPUT, --output OUTPUT
                        Output file path for the breach report (default: stdout)
  -f {text,json}, --format {text,json}
                        Output format (text or json) (default: text)
  -t THRESHOLD, --threshold THRESHOLD
                        Confidence threshold for entities (0.0-1.0) (default: 0.7)
  -c CLONE-DIR, --clone-dir CLONE-DIR
                        Directory to create cloned structure of high-risk files
  -v, --verbose         Generate detailed verbose report instead of executive summary
```

#### Report Features

The NC breach notification script provides two reporting formats:

1. **Executive Summary Report** (default):
   - Concise overview of breach notification findings
   - Document set statistics showing file types and counts
   - Tabular listing of high-risk files with risk classification
   - Summary statistics by classification category
   
2. **Detailed Verbose Report** (with `-v` flag):
   - Comprehensive analysis of each high-risk file
   - Detailed entity information with counts
   - Breach trigger explanation and reasoning
   - Sample of masked entities found in each file

The script uses an intelligent classification system to categorize breach types:

| Classification | Description |
|---------------|-------------|
| PII-SSN | Name with Social Security Number |
| PII-FIN | Name with Financial Information |
| PII-GOV | Name with Government ID |
| PII-MED | Name with Health Information |
| PII-GEN | Name with Other Sensitive Data |
| CREDS | Credential Pairs (Username/Email + Password) |
| HIGH-RISK | Multiple Sensitive Categories |

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
--workers            Number of parallel worker threads (0=auto, parallel processor only)
```

## Performance Optimization

The PII Analyzer offers two levels of thread optimization:

1. **OCR Thread Optimization**: Within the OCR extractor, threads are optimized based on file size, available memory, and CPU cores to efficiently process individual documents with multiple pages.

2. **Parallel File Processing**: Using `pii_analyzer_parallel.py`, multiple files can be processed simultaneously, each with its own OCR thread optimization, significantly reducing processing time for large collections of documents.

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
├── pii_analyzer_parallel.py  # Parallel processing entry point
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