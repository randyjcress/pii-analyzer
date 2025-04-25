FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies including Tesseract
RUN apt-get update && apt-get install -y \
    tesseract-ocr \
    libtesseract-dev \
    poppler-utils \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install spaCy model
RUN python -m spacy download en_core_web_lg

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONPATH=/app
ENV TIKA_SERVER_ENDPOINT=http://tika:9998
ENV PYTHONUNBUFFERED=1

# Default command
CMD ["python", "-m", "src.cli", "analyze", "--help"] 