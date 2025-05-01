#!/usr/bin/env python3

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fh:
    requirements = [line.strip() for line in fh.readlines()]

setup(
    name="pii-analyzer",
    version="1.0.0",
    author="Randy J. Cress",
    author_email="username@example.com",
    description="A tool for detecting and analyzing personally identifiable information in documents",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/randyjcress/pii-analyzer",
    packages=find_packages(),
    py_modules=["pii_analyzer", "pii_analyzer_parallel", "fix_enhanced_cli", "strict_nc_breach_pii", "extract_nc_breach_pii"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "pii-analyzer=pii_analyzer:main",
            "pii-analyzer-parallel=pii_analyzer_parallel:main",
        ],
    },
) 