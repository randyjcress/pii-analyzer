# PII Analyzer Tests

This directory contains tests for the PII Analyzer resumable processing functionality.

## Running Tests

To run all tests:

```bash
python -m unittest discover tests
```

To run a specific test:

```bash
python -m unittest tests/test_file_discovery.py
```

## Manual Testing

Some tests provide a manual testing mode for interactive exploration:

```bash
python tests/test_file_discovery.py --manual
```

This will create sample files and a database, then print information about what was created. The output will show where these files are located so you can examine them manually if needed.

## Test Coverage

- **test_file_discovery.py**: Tests the file discovery and registration functionality
  - Scanning directories for files
  - Registering files in the database
  - Finding resumption points for interrupted jobs
  - Resetting stalled files
  - Generating file statistics

## Creating New Tests

When creating new tests, follow these guidelines:

1. Name the test file with the prefix `test_` (e.g., `test_file_discovery.py`)
2. Use the unittest framework
3. Create temporary files/directories that are cleaned up after the test
4. Include detailed docstrings explaining what is being tested
5. Include both unit tests and an optional manual testing mode if appropriate 