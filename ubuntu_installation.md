# PII Analysis System - Ubuntu Installation Guide

## System Overview
This is a PII analysis system built on:
- Python 3.11
- Microsoft Presidio for PII detection
- Apache Tika for document text extraction
- Tesseract OCR for image-based text extraction

## Prerequisites

### System Requirements
- Ubuntu Linux
- Python 3.11+
- Docker (for Apache Tika)
- 8GB+ RAM recommended (Spacy models and OCR processing can be memory intensive)
- Sufficient disk space for document processing

### Installing Python 3.11 on Ubuntu

Python 3.11 is not available in the default repositories for many Ubuntu versions. Follow these steps to install it:

```bash
# Update package lists
sudo apt update

# Install required dependencies
sudo apt install -y software-properties-common

# Add deadsnakes PPA repository for Python 3.11
sudo add-apt-repository -y ppa:deadsnakes/ppa

# Update package lists again to include the new repository
sudo apt update

# Install Python 3.11 and development headers
sudo apt install -y python3.11 python3.11-dev python3.11-venv
```

### Installing Other System Packages

```bash
# Install Tesseract OCR, Docker, and other dependencies
sudo apt install -y tesseract-ocr libtesseract-dev poppler-utils 

# Install Docker if not already installed
sudo apt install -y docker.io docker-compose git

# Make sure Docker service is running
sudo systemctl start docker
sudo systemctl enable docker

# Optional: Add your user to the docker group to run Docker without sudo
sudo usermod -aG docker $USER
# Note: You'll need to log out and back in for this to take effect
```

## Installation Steps

1. **Clone the repository**
   ```bash
   git clone https://github.com/randyjcress/pii-analysis.git
   cd pii-analysis
   ```

2. **Set up Python virtual environment**
   ```bash
   python3.11 -m venv venv
   source venv/bin/activate
   ```

3. **Install Python dependencies**
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

4. **Install spaCy language model**
   ```bash
   python -m spacy download en_core_web_lg
   ```

5. **Start Apache Tika service**
   ```bash
   docker-compose up -d
   ```
   This will start the Tika server on port 9998.

## Running the Analysis

### Single File Analysis
```bash
python -m src.cli analyze -i path/to/file.pdf -o results.json -t 0.7
```

### Batch Processing
```bash
python -m src.cli analyze -i path/to/directory -o output_directory -t 0.7
```

### NC Breach Specific Analysis 
For applying NC breach notification rules, after running the full analysis:
```bash
python strict_nc_breach_pii.py full_analysis_results.json
```

### Using the fixed enhanced CLI
For processing large directories with better error handling (especially for DOCX files):
```bash
python fix_enhanced_cli.py -i path/to/directory -o full_results.json
```

## Processing the Full Dataset

To process the entire dataset, we recommend this approach:

1. **Start with a small sample to verify everything works**
   ```bash
   python fix_enhanced_cli.py -i docs -o sample_analysis.json --sample 50
   ```

2. **Process in batches if the dataset is large**
   - Split into subdirectories if needed
   - Run the analysis on each batch:
   ```bash
   python fix_enhanced_cli.py -i docs/batch1 -o batch1_results.json
   python fix_enhanced_cli.py -i docs/batch2 -o batch2_results.json
   ```

3. **Run the NC breach analysis on results**
   ```bash
   python strict_nc_breach_pii.py full_results.json
   ```

## Potential Issues and Solutions

1. **Memory limitations**: For large files, especially PDFs with many pages:
   - Use the `--max-pages` option to limit pages processed
   - Increase swap space on the Ubuntu system if needed

2. **DOCX file processing errors**: The fixed CLI handles these better with improved error logging and recovery

3. **Apache Tika performance**: Ensure Tika has enough memory:
   ```bash
   # Edit docker-compose.yml if needed to add:
   environment:
     - JAVA_OPTS=-Xmx4g  # Allocate 4GB to Tika
   ```

4. **Tesseract OCR tuning**: If OCR is slow, adjust threads and DPI:
   ```bash
   python fix_enhanced_cli.py -i docs -o results.json --ocr-threads 4 --ocr-dpi 200
   ```

## Code Structure
- `src/` - Core functionality
  - `extractors/` - Text extraction from various file formats
  - `analyzers/` - PII detection using Presidio
  - `anonymizers/` - PII redaction/anonymization
  - `utils/` - Helper utilities
  - `cli.py` - Command-line interface
- `fix_enhanced_cli.py` - Improved CLI with better error handling
- `strict_nc_breach_pii.py` - NC breach notification analysis

The system follows a pipeline architecture:
1. Document text extraction (Tika/OCR)
2. PII analysis (Presidio)
3. Results formatting 