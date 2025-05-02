#!/usr/bin/env python3
"""
Enhanced NC Breach Notification Analysis
Analyzes PII analysis results to identify files that would trigger 
breach notification requirements under North Carolina law (§75-61)
"""
import json
import sys
from collections import defaultdict

# Enhanced set of sensitive entity types based on both Presidio built-ins
# and custom recognizers that would trigger NC breach notification
SENSITIVE_TYPES = {
    # built‑in
    "US_SOCIAL_SECURITY_NUMBER", "US_SSN",
    "US_DRIVER_LICENSE", "US_PASSPORT",
    "CREDIT_CARD", "BANK_ACCOUNT", "IBAN_CODE", "US_BANK_ROUTING", "US_BANK_NUMBER",
    "MEDICAL_RECORD_NUMBER", "HEALTH_INSURANCE_POLICY_NUMBER",
    # any custom recognisers
    "PIN_CODE", "PASSWORD", "SECURITY_ANSWER",
    "DIGITAL_SIGNATURE", "BIOMETRIC_IDENTIFIER",
}

# User-friendly display names for entity types
ENTITY_DISPLAY_NAMES = {
    "US_SOCIAL_SECURITY_NUMBER": "Social Security Number",
    "US_SSN": "Social Security Number",
    "US_DRIVER_LICENSE": "Driver's License",
    "US_PASSPORT": "Passport Number",
    "CREDIT_CARD": "Credit Card Number",
    "BANK_ACCOUNT": "Bank Account Number",
    "US_BANK_NUMBER": "Bank Account Number",
    "US_BANK_ROUTING": "Bank Routing Number",
    "IBAN_CODE": "International Bank Account Number",
    "MEDICAL_RECORD_NUMBER": "Medical Record Number",
    "HEALTH_INSURANCE_POLICY_NUMBER": "Health Insurance Policy Number",
    "PIN_CODE": "PIN Code",
    "PASSWORD": "Password",
    "SECURITY_ANSWER": "Security Answer",
    "DIGITAL_SIGNATURE": "Digital Signature",
    "BIOMETRIC_IDENTIFIER": "Biometric Identifier",
    "PERSON": "Person Name",
    "FIRST_NAME": "First Name",
    "LAST_NAME": "Last Name",
    "EMAIL_ADDRESS": "Email Address",
    "USERNAME": "Username",
    "ACCESS_CODE": "Access Code"
}

# Threshold values for high confidence PII
HIGH_CONFIDENCE_THRESHOLD = 0.7

def breach_trigger(entity_set: set[str]) -> bool:
    """
    Return True if document meets NC §75‑61 personal‑info definition.
    entity_set = {entity_type strings detected by Presidio above the chosen score}
    """
    # (A) "first name/initial + last name" OR "PERSON" composite
    has_name = (
        "PERSON" in entity_set
        or ("FIRST_NAME" in entity_set and "LAST_NAME" in entity_set)
    )

    # (B) any sensitive token
    has_sensitive = bool(entity_set & SENSITIVE_TYPES)

    # (C) credential‑only path: username / email + password / access code
    credential_pair = (
        ("EMAIL_ADDRESS" in entity_set or "USERNAME" in entity_set)
        and ("PASSWORD" in entity_set or "ACCESS_CODE" in entity_set)
    )

    return (has_name and has_sensitive) or credential_pair

def analyze_pii_report(report_path):
    with open(report_path, 'r') as f:
        data = json.load(f)
    
    # Dictionary to track files and their sensitive entities
    high_risk_files = defaultdict(list)
    file_entity_sets = defaultdict(set)
    
    # Process each file result
    for file_result in data.get('results', []):
        file_path = file_result.get('file_path', '')
        entities = file_result.get('entities', [])
        
        # Collect all high-confidence entities for the file
        for entity in entities:
            entity_type = entity.get('entity_type', '')
            confidence = entity.get('score', 0.0)
            text = entity.get('text', '')
            
            if confidence >= HIGH_CONFIDENCE_THRESHOLD:
                # Add to entity set for breach trigger evaluation
                file_entity_sets[file_path].add(entity_type)
                
                # Store all entities (not just sensitive ones) for reporting
                high_risk_files[file_path].append({
                    'type': entity_type,
                    'category': ENTITY_DISPLAY_NAMES.get(entity_type, entity_type),
                    'confidence': confidence,
                    'text': text
                })
    
    # Filter to only files that trigger breach notification
    breach_files = {}
    for file_path, entity_set in file_entity_sets.items():
        if breach_trigger(entity_set):
            breach_files[file_path] = high_risk_files[file_path]
    
    return breach_files

def report_high_risk_files(high_risk_files):
    print(f"Files triggering NC §75-61 breach notification requirements:\n")
    
    # Sort files by number of sensitive entities (highest first)
    sorted_files = sorted(high_risk_files.items(), key=lambda x: len(x[1]), reverse=True)
    
    for file_path, entities in sorted_files:
        print(f"File: {file_path}")
        print(f"Number of entities: {len(entities)}")
        
        # Count entity types
        entity_counts = defaultdict(int)
        for entity in entities:
            entity_counts[entity['category']] += 1
        
        print("Entity types:")
        for category, count in sorted(entity_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {category}: {count}")
        
        # Extract the set of entity types for this file to explain trigger
        entity_types = {entity['type'] for entity in entities}
        print("Breach notification trigger reason:")
        has_name = "PERSON" in entity_types or ("FIRST_NAME" in entity_types and "LAST_NAME" in entity_types)
        has_sensitive = bool(entity_types & SENSITIVE_TYPES)
        credential_pair = (
            ("EMAIL_ADDRESS" in entity_types or "USERNAME" in entity_types)
            and ("PASSWORD" in entity_types or "ACCESS_CODE" in entity_types)
        )
        
        if has_name and has_sensitive:
            print("  - Contains personally identifiable information AND sensitive data")
        if credential_pair:
            print("  - Contains credential pair (username/email + password/access code)")
        
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
    if entity_type in SENSITIVE_TYPES:
        # Show only last 4 characters for sensitive data
        if len(text) > 4:
            return f"****{text[-4:]}"
        return "****"
    elif entity_type in ["EMAIL_ADDRESS", "USERNAME"]:
        # Partially mask email/username
        if '@' in text:  # Email address
            username, domain = text.split('@', 1)
            if len(username) > 2:
                return f"{username[0]}***@{domain}"
            return f"***@{domain}"
        elif len(text) > 4:  # Username
            return f"{text[0]}***{text[-1]}"
        return "****"
    elif entity_type in ["PERSON", "FIRST_NAME", "LAST_NAME"]:
        # Show initials for person names
        parts = text.split()
        if len(parts) > 1:
            return ' '.join([p[0] + '.' for p in parts])
        elif len(text) > 0:
            return text[0] + '.'
        return "****"
    else:
        # For other types, show first and last character
        if len(text) > 4:
            return f"{text[0]}***{text[-1]}"
        return "****"

def print_usage():
    """Print usage information for the script"""
    print(f"Usage: {sys.argv[0]} <pii_analysis_report.json>")
    print()
    print("Analyzes a PII analysis report to identify files that would trigger")
    print("breach notification requirements under North Carolina law (§75-61).")
    print()
    print("The script looks for:")
    print("1. Personal identifiers (name) combined with sensitive information, or")
    print("2. Credential pairs (username/email + password/access code)")
    print()
    print("Entities are only considered if they meet the confidence threshold (0.7).")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print_usage()
        sys.exit(1)
        
    report_path = sys.argv[1]
    high_risk_files = analyze_pii_report(report_path)
    
    if high_risk_files:
        report_high_risk_files(high_risk_files)
        print(f"Found {len(high_risk_files)} files that would trigger breach notification")
        print("requirements under North Carolina law (§75-61).")
    else:
        print("No files found that would trigger breach notification requirements.") 