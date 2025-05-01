# Contributing to PII Analyzer

Thank you for considering contributing to PII Analyzer! This document provides guidelines for contributing to the project.

## Development Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/your-username/pii-analysis.git
   cd pii-analysis
   ```

2. **Set up your development environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   python -m spacy download en_core_web_lg
   ```

3. **Create a .env file from the template**
   ```bash
   cp .env-template .env
   ```

4. **Start the Tika server**
   ```bash
   docker-compose up -d
   ```

## Development Workflow

1. **Create a feature branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes and ensure tests pass**
   ```bash
   pytest
   ```

3. **Format your code**
   ```bash
   black .
   flake8
   ```

4. **Commit your changes with a descriptive message**
   ```bash
   git commit -m "Add feature: description of your changes"
   ```

5. **Push your branch and create a pull request**
   ```bash
   git push origin feature/your-feature-name
   ```

## Key Files

When contributing, be aware of these key components:

- `src/cli.py` - Command-line interface
- `src/extractors/` - Text extraction modules
- `src/analyzers/` - PII detection modules
- `src/anonymizers/` - PII redaction modules
- `fix_enhanced_cli.py` - Improved CLI with better error handling
- `strict_nc_breach_pii.py` - NC breach notification analysis

## Adding Support for New File Types

To add support for a new file type:

1. Create a new extractor in `src/extractors/`
2. Update the `extractor_factory.py` to recognize and handle the new type
3. Add tests for the new functionality
4. Update documentation

## Pull Request Guidelines

- Keep PRs focused on a single feature or bug fix
- Write tests to cover new functionality
- Update documentation to reflect changes
- Follow the existing code style (we use Black for formatting)
- Make sure all tests pass before submitting

## Code of Conduct

Please be respectful and inclusive in your interactions with the project. We follow a standard code of conduct that promotes a positive environment for everyone. 