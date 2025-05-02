#!/usr/bin/env python3
"""
Enhanced NC Breach Notification Analysis
Analyzes PII analysis results to identify files that would trigger 
breach notification requirements under North Carolina law (§75-61)
"""
import json
import sys
import os
import argparse
import shutil
from collections import defaultdict
from pathlib import Path
from datetime import datetime

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

# Classification labels for breach types
BREACH_CLASSIFICATIONS = {
    "NAME_WITH_SSN": "PII-SSN",           # Name with SSN
    "NAME_WITH_FINANCIALS": "PII-FIN",    # Name with financial info
    "NAME_WITH_GOV_ID": "PII-GOV",        # Name with government ID
    "NAME_WITH_HEALTH": "PII-MED",        # Name with health data
    "NAME_WITH_OTHER": "PII-GEN",         # Name with other sensitive data
    "CREDENTIALS": "CREDS",               # Credential pairs
    "MULTIPLE": "HIGH-RISK"               # Multiple breach types
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

def classify_breach(entity_types: set[str]) -> str:
    """
    Classify the breach type based on entity types present.
    Returns a concise classification label.
    """
    has_name = "PERSON" in entity_types or ("FIRST_NAME" in entity_types and "LAST_NAME" in entity_types)
    has_credential_pair = (
        ("EMAIL_ADDRESS" in entity_types or "USERNAME" in entity_types)
        and ("PASSWORD" in entity_types or "ACCESS_CODE" in entity_types)
    )
    
    classifications = []
    
    # Check for credentials
    if has_credential_pair:
        classifications.append(BREACH_CLASSIFICATIONS["CREDENTIALS"])
    
    # Only check for PII combinations if we have a name
    if has_name:
        # Check for SSN
        if "US_SSN" in entity_types or "US_SOCIAL_SECURITY_NUMBER" in entity_types:
            classifications.append(BREACH_CLASSIFICATIONS["NAME_WITH_SSN"])
        
        # Check for financial information
        if any(et in entity_types for et in ["CREDIT_CARD", "BANK_ACCOUNT", "US_BANK_NUMBER", "IBAN_CODE", "US_BANK_ROUTING"]):
            classifications.append(BREACH_CLASSIFICATIONS["NAME_WITH_FINANCIALS"])
        
        # Check for government IDs
        if any(et in entity_types for et in ["US_DRIVER_LICENSE", "US_PASSPORT"]):
            classifications.append(BREACH_CLASSIFICATIONS["NAME_WITH_GOV_ID"])
        
        # Check for health information
        if any(et in entity_types for et in ["MEDICAL_RECORD_NUMBER", "HEALTH_INSURANCE_POLICY_NUMBER"]):
            classifications.append(BREACH_CLASSIFICATIONS["NAME_WITH_HEALTH"])
        
        # If name with sensitive info but none of the above specific categories
        has_other_sensitive = bool(entity_types & SENSITIVE_TYPES) and len(classifications) == 0
        if has_other_sensitive:
            classifications.append(BREACH_CLASSIFICATIONS["NAME_WITH_OTHER"])
    
    # If multiple classifications, use HIGH-RISK
    if len(classifications) > 1:
        return BREACH_CLASSIFICATIONS["MULTIPLE"]
    elif len(classifications) == 1:
        return classifications[0]
    else:
        return "UNKNOWN"  # This should not happen given our breach_trigger logic

def analyze_pii_report(report_path, threshold=HIGH_CONFIDENCE_THRESHOLD):
    """
    Analyzes a PII report to identify files triggering breach notification.
    
    Args:
        report_path: Path to the PII analysis report JSON file
        threshold: Confidence threshold for entities (default: 0.7)
        
    Returns:
        Dictionary of high-risk files with their entities
    """
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
            
            if confidence >= threshold:
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

def generate_executive_summary(high_risk_files, original_report_path=None):
    """Generate a concise executive summary report of high-risk files."""
    output = []
    output.append(f"NC §75-61 BREACH NOTIFICATION EXECUTIVE SUMMARY")
    output.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Extract file type statistics if available
    file_type_stats = {}
    total_files = 0
    
    # Try to extract file type information from the original report
    if original_report_path and os.path.exists(original_report_path):
        try:
            with open(original_report_path, 'r') as f:
                data = json.load(f)
                
                # Count file types from all files in the report
                if 'results' in data:
                    for result in data.get('results', []):
                        file_path = result.get('file_path', '')
                        if file_path:
                            ext = os.path.splitext(file_path)[1].lower()
                            if ext:
                                # Normalize the extension (remove dot, handle jpeg/jpg)
                                ext = ext[1:]  # Remove the dot
                                if ext == 'jpeg':
                                    ext = 'jpg'
                                file_type_stats[ext] = file_type_stats.get(ext, 0) + 1
                                total_files += 1
                
                # If the report has file_type_stats, use those instead
                if 'file_type_stats' in data and isinstance(data['file_type_stats'], dict):
                    success_stats = data.get('file_type_stats', {}).get('success', {})
                    for ext, count in success_stats.items():
                        # Normalize extension (remove the dot)
                        ext = ext.lstrip('.').lower()
                        if ext == 'jpeg':
                            ext = 'jpg'
                        file_type_stats[ext] = file_type_stats.get(ext, 0) + count
                        
                    # If we have a total_files count in the report, use that
                    if 'total_files' in data and isinstance(data['total_files'], int):
                        total_files = data['total_files']
                    else:
                        total_files = sum(success_stats.values())
                        
        except Exception as e:
            # If there's an error, don't add file type stats
            print(f"Warning: Could not extract file type statistics: {e}")
            file_type_stats = {}

    # Display file type statistics if available
    if file_type_stats:
        output.append(f"Document Set Summary:")
        output.append(f"Total files processed: {total_files}")
        
        output.append(f"File types:")
        for ext, count in sorted(file_type_stats.items(), key=lambda x: x[1], reverse=True):
            output.append(f"  .{ext}: {count}")
    
    output.append(f"Files Found with PII: {len(high_risk_files)}")
    output.append("-" * 80)
    output.append(f"{'CLASSIFICATION':<12} {'ENTITIES':<8} {'FILE PATH':<60}")
    output.append("-" * 80)
    
    # For sorting based on classification severity - ordered from most to least severe
    severity_order = {
        "HIGH-RISK": 1,
        "PII-SSN": 2,
        "PII-FIN": 3,
        "PII-GOV": 4,
        "PII-MED": 5,
        "PII-GEN": 6,
        "CREDS": 7,
        "UNKNOWN": 8
    }
    
    # Process and collect file data for sorted output
    file_data = []
    for file_path, entities in high_risk_files.items():
        entity_types = {entity['type'] for entity in entities}
        classification = classify_breach(entity_types)
        file_data.append((classification, len(entities), file_path, severity_order.get(classification, 9)))
    
    # Sort by classification severity then by entity count (descending)
    sorted_files = sorted(file_data, key=lambda x: (x[3], -x[1]))
    
    # Generate the report lines
    for classification, entity_count, file_path, _ in sorted_files:
        # Truncate path if too long
        if len(file_path) > 59:
            display_path = "..." + file_path[-56:]
        else:
            display_path = file_path
            
        output.append(f"{classification:<12} {entity_count:<8} {display_path}")
    
    # Add summary statistics
    output.append("-" * 80)
    classification_counts = defaultdict(int)
    for classification, _, _, _ in sorted_files:
        classification_counts[classification] += 1
    
    output.append("Classification Summary:")
    for classification, count in sorted(classification_counts.items(), 
                                        key=lambda x: severity_order.get(x[0], 9)):
        output.append(f"  {classification:<12}: {count} files")
    
    output.append("-" * 80)
    output.append(f"LEGEND:")
    output.append(f"  PII-SSN: Name with Social Security Number")
    output.append(f"  PII-FIN: Name with Financial Information")
    output.append(f"  PII-GOV: Name with Government ID")
    output.append(f"  PII-MED: Name with Health Information")
    output.append(f"  PII-GEN: Name with Other Sensitive Data")
    output.append(f"  CREDS:   Credential Pairs (Username/Email + Password)")
    output.append(f"  HIGH-RISK: Multiple Sensitive Categories")
    
    return "\n".join(output)

def generate_report_text(high_risk_files):
    """Generate a human-readable text report of high-risk files."""
    output = []
    output.append(f"NC §75-61 Breach Notification Analysis Report")
    output.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append(f"\nFiles triggering NC §75-61 breach notification requirements:\n")
    
    # Sort files by number of sensitive entities (highest first)
    sorted_files = sorted(high_risk_files.items(), key=lambda x: len(x[1]), reverse=True)
    
    for file_path, entities in sorted_files:
        output.append(f"File: {file_path}")
        output.append(f"Number of entities: {len(entities)}")
        
        # Get classification
        entity_types = {entity['type'] for entity in entities}
        classification = classify_breach(entity_types)
        output.append(f"Classification: {classification}")
        
        # Count entity types
        entity_counts = defaultdict(int)
        for entity in entities:
            entity_counts[entity['category']] += 1
        
        output.append("Entity types:")
        for category, count in sorted(entity_counts.items(), key=lambda x: x[1], reverse=True):
            output.append(f"  - {category}: {count}")
        
        # Extract the set of entity types for this file to explain trigger
        output.append("Breach notification trigger reason:")
        has_name = "PERSON" in entity_types or ("FIRST_NAME" in entity_types and "LAST_NAME" in entity_types)
        has_sensitive = bool(entity_types & SENSITIVE_TYPES)
        credential_pair = (
            ("EMAIL_ADDRESS" in entity_types or "USERNAME" in entity_types)
            and ("PASSWORD" in entity_types or "ACCESS_CODE" in entity_types)
        )
        
        if has_name and has_sensitive:
            output.append("  - Contains personally identifiable information AND sensitive data")
        if credential_pair:
            output.append("  - Contains credential pair (username/email + password/access code)")
        
        # Show sample of entity text (max 3 per type)
        output.append("Sample entities (max 3 per type):")
        samples_by_type = defaultdict(list)
        
        for entity in entities:
            category = entity['category']
            if len(samples_by_type[category]) < 3:
                samples_by_type[category].append(entity)
                
        for category, samples in samples_by_type.items():
            output.append(f"  {category}:")
            for sample in samples:
                # Mask part of the sensitive data for the report
                masked_text = mask_sensitive_text(sample['text'], sample['type'])
                output.append(f"    - {masked_text} (confidence: {sample['confidence']:.2f})")
        
        output.append("-" * 80)
    
    output.append(f"\nFound {len(high_risk_files)} files that would trigger breach notification")
    output.append("requirements under North Carolina law (§75-61).")
    
    return "\n".join(output)

def generate_report_json(high_risk_files):
    """Generate a JSON representation of the breach report."""
    report = {
        "metadata": {
            "report_type": "NC §75-61 Breach Notification Analysis",
            "generated_at": datetime.now().isoformat(),
            "file_count": len(high_risk_files)
        },
        "breach_files": {}
    }
    
    for file_path, entities in high_risk_files.items():
        # Group entities by type
        entity_types = {entity['type'] for entity in entities}
        entity_by_type = defaultdict(list)
        
        for entity in entities:
            entity_by_type[entity['type']].append({
                "text": mask_sensitive_text(entity['text'], entity['type']),
                "confidence": entity['score'] if 'score' in entity else entity['confidence'],
                "category": entity['category']
            })
        
        # Determine breach trigger reason
        has_name = "PERSON" in entity_types or ("FIRST_NAME" in entity_types and "LAST_NAME" in entity_types)
        has_sensitive = bool(entity_types & SENSITIVE_TYPES)
        credential_pair = (
            ("EMAIL_ADDRESS" in entity_types or "USERNAME" in entity_types)
            and ("PASSWORD" in entity_types or "ACCESS_CODE" in entity_types)
        )
        
        breach_reasons = []
        if has_name and has_sensitive:
            breach_reasons.append("personal_info_with_sensitive_data")
        if credential_pair:
            breach_reasons.append("credential_pair")
        
        # Add classification
        classification = classify_breach(entity_types)
        
        # Add to the report
        report["breach_files"][file_path] = {
            "entity_count": len(entities),
            "entity_types": list(entity_types),
            "classification": classification,
            "breach_reasons": breach_reasons,
            "entities_by_type": dict(entity_by_type)
        }
    
    return json.dumps(report, indent=2)

def clone_high_risk_files(high_risk_files, clone_dir):
    """Clone the high-risk files to a specified directory maintaining structure."""
    if not os.path.exists(clone_dir):
        os.makedirs(clone_dir)
    
    copied_files = []
    
    for file_path in high_risk_files.keys():
        # Check if file exists
        if not os.path.exists(file_path):
            print(f"Warning: Could not find {file_path}")
            continue
            
        # Create the destination directory structure
        rel_path = os.path.relpath(file_path, '/')
        dest_path = os.path.join(clone_dir, rel_path)
        dest_dir = os.path.dirname(dest_path)
        
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)
        
        # Copy the file
        try:
            shutil.copy2(file_path, dest_path)
            copied_files.append(dest_path)
        except Exception as e:
            print(f"Error copying {file_path}: {e}")
    
    return copied_files

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

def parse_arguments():
    """Parse and validate command line arguments."""
    parser = argparse.ArgumentParser(
        description="NC §75-61 Breach Notification Analysis",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "report_file",
        help="Path to the PII analysis report JSON file"
    )
    
    parser.add_argument(
        "-o", "--output",
        help="Output file path for the breach report (default: stdout)"
    )
    
    parser.add_argument(
        "-f", "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (text or json)"
    )
    
    parser.add_argument(
        "-t", "--threshold",
        type=float,
        default=HIGH_CONFIDENCE_THRESHOLD,
        help="Confidence threshold for entities (0.0-1.0)"
    )
    
    parser.add_argument(
        "-c", "--clone-dir",
        help="Directory to create cloned structure of high-risk files"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Generate detailed verbose report instead of executive summary"
    )
    
    args = parser.parse_args()
    
    # Validate that the report file exists
    if not os.path.exists(args.report_file):
        parser.error(f"Report file not found: {args.report_file}")
    
    # Validate threshold is in range
    if args.threshold < 0.0 or args.threshold > 1.0:
        parser.error(f"Threshold must be between 0.0 and 1.0")
        
    return args

def main():
    """Main entry point for the script."""
    args = parse_arguments()
    
    # Analyze the report
    high_risk_files = analyze_pii_report(args.report_file, args.threshold)
    
    # Generate the report
    if args.format == "text":
        if args.verbose:
            report = generate_report_text(high_risk_files)
        else:
            report = generate_executive_summary(high_risk_files, args.report_file)
    else:  # json
        report = generate_report_json(high_risk_files)
    
    # Output the report
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"Report written to {args.output}")
    else:
        print(report)
    
    # Clone files if requested
    if args.clone_dir and high_risk_files:
        copied_files = clone_high_risk_files(high_risk_files, args.clone_dir)
        if copied_files:
            print(f"\nCloned {len(copied_files)} high-risk files to {args.clone_dir}")
        else:
            print(f"\nNo files were copied to {args.clone_dir}")
    
    # Return summary for non-interactive usage
    return {
        "files_analyzed": len(high_risk_files),
        "report_format": args.format,
        "output_file": args.output,
        "clone_directory": args.clone_dir if args.clone_dir else None
    }

if __name__ == '__main__':
    main() 