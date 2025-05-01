#!/usr/bin/env python3
import json
import sys
from collections import defaultdict

# NC breach notification law requires notification for PII including:
# - Social Security numbers (SSN)
# - Driver's license, state ID, or passport numbers
# - Financial account numbers (checking, credit card, etc.)
# - Digital signatures
# - Biometric data

# Map of Presidio entity types to NC breach notification categories - STRICT VERSION
NC_BREACH_ENTITIES = {
    'US_SSN': 'Social Security Number',
    'US_DRIVER_LICENSE': 'Driver\'s License',
    'US_PASSPORT': 'Passport Number',
    'CREDIT_CARD': 'Credit Card Number',
    'BANK_ACCOUNT': 'Bank Account Number',
    'US_BANK_NUMBER': 'Bank Account Number',
    'IBAN_CODE': 'Bank Account Number'
}

# Threshold values for high confidence PII
HIGH_CONFIDENCE_THRESHOLD = 0.7

def analyze_pii_report(report_path):
    with open(report_path, 'r') as f:
        data = json.load(f)
    
    # Dictionary to track files and their sensitive entities
    high_risk_files = defaultdict(list)
    
    # Process each file result
    for file_result in data.get('results', []):
        file_path = file_result.get('file_path', '')
        entities = file_result.get('entities', [])
        
        for entity in entities:
            entity_type = entity.get('entity_type', '')
            confidence = entity.get('score', 0.0)
            text = entity.get('text', '')
            
            # Check if this is a sensitive entity type with high confidence
            if entity_type in NC_BREACH_ENTITIES and confidence >= HIGH_CONFIDENCE_THRESHOLD:
                high_risk_files[file_path].append({
                    'type': entity_type,
                    'category': NC_BREACH_ENTITIES[entity_type],
                    'confidence': confidence,
                    'text': text
                })
    
    return high_risk_files

def report_high_risk_files(high_risk_files):
    print(f"Files containing high-confidence PII requiring breach notification under NC law (STRICT):\n")
    
    # Sort files by number of sensitive entities (highest first)
    sorted_files = sorted(high_risk_files.items(), key=lambda x: len(x[1]), reverse=True)
    
    for file_path, entities in sorted_files:
        print(f"File: {file_path}")
        print(f"Number of sensitive entities: {len(entities)}")
        
        # Count entity types
        entity_counts = defaultdict(int)
        for entity in entities:
            entity_counts[entity['category']] += 1
        
        print("Entity types:")
        for category, count in sorted(entity_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {category}: {count}")
        
        # Show sample of entity text (max 3 per type)
        print("Sample entities (max 3 per type):")
        samples_by_type = defaultdict(list)
        
        for entity in entities:
            category = entity['category']
            if len(samples_by_type[category]) < 3:
                samples_by_type[category].append(entity)
                
        for category, samples in samples_by_type.items():
            print(f"  {category}:")
            for sample in samples:
                # Mask part of the sensitive data for the report
                masked_text = mask_sensitive_text(sample['text'], sample['type'])
                print(f"    - {masked_text} (confidence: {sample['confidence']:.2f})")
        
        print("-" * 80)

def mask_sensitive_text(text, entity_type):
    """Masks sensitive text for display in reports"""
    if entity_type in ['US_SSN', 'CREDIT_CARD', 'BANK_ACCOUNT', 'US_BANK_NUMBER', 'IBAN_CODE']:
        # Show only last 4 characters
        if len(text) > 4:
            return f"****{text[-4:]}"
        return "****"
    else:
        # For other types, show first and last character
        if len(text) > 4:
            return f"{text[0]}***{text[-1]}"
        return "****"

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <pii_analysis_report.json>")
        sys.exit(1)
        
    report_path = sys.argv[1]
    high_risk_files = analyze_pii_report(report_path)
    
    if high_risk_files:
        report_high_risk_files(high_risk_files)
        print(f"Found {len(high_risk_files)} files containing high-confidence PII that would require")
        print("breach notification under North Carolina law for local governments (STRICT criteria).")
    else:
        print("No files found with high-confidence PII requiring breach notification.") 