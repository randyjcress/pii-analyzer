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

# Add src directory to path to allow imports from PII analyzer modules
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.database.db_reporting import load_pii_data_from_db, get_file_type_statistics

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

def analyze_pii_database(db_path, job_id=None, threshold=HIGH_CONFIDENCE_THRESHOLD):
    """
    Analyzes PII data from a database to identify files triggering breach notification.
    
    Args:
        db_path: Path to the SQLite database file
        job_id: Specific job ID to analyze (most recent if None)
        threshold: Confidence threshold for entities
        
    Returns:
        Dictionary of high-risk files with their entities
    """
    # Load data from database
    data = load_pii_data_from_db(db_path, job_id, threshold)
    
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

def generate_executive_summary(high_risk_files, original_report_path=None, db_path=None, job_id=None):
    """Generate a concise executive summary report of high-risk files."""
    print("DEBUG: Entering generate_executive_summary")
    output = []
    output.append(f"NC §75-61 BREACH NOTIFICATION EXECUTIVE SUMMARY")
    output.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Extract file type statistics if available
    file_type_stats = {}
    total_files = 0
    file_processing_stats = None
    time_stats = None
    
    # Try to extract file type information and processing stats from the database if provided
    if db_path:
        print(f"DEBUG: db_path: {db_path}, job_id: {job_id}")
        try:
            file_type_stats = get_file_type_statistics(db_path, job_id)
            print(f"DEBUG: file_type_stats: {file_type_stats}")
            total_files = sum(file_type_stats.values())
            print(f"DEBUG: total_files: {total_files}")
            
            # Get file processing statistics
            from src.database.db_reporting import get_file_processing_stats, get_processing_time_stats
            file_processing_stats = get_file_processing_stats(db_path, job_id)
            print(f"DEBUG: file_processing_stats: {file_processing_stats}")
            time_stats = get_processing_time_stats(db_path, job_id)
            print(f"DEBUG: time_stats: {time_stats}")
        except Exception as e:
            import traceback
            print(f"DEBUG ERROR in generate_executive_summary: {e}")
            traceback.print_exc()
            print(f"Warning: Could not extract file statistics from database: {e}")
    
    # If database not provided or failed, try from the original report
    if not file_type_stats and original_report_path and os.path.exists(original_report_path):
        print(f"DEBUG: Trying to get stats from original_report_path: {original_report_path}")
        try:
            with open(original_report_path, 'r') as f:
                data = json.load(f)
                
                # Count file types from all files in the report
                if 'results' in data:
                    for result in data.get('results', []):
                        file_path = result.get('file_path', '')
                        if file_path:
                            ext = os.path.splitext(file_path)[1].lower()
                            file_type_stats[ext] = file_type_stats.get(ext, 0) + 1
                            total_files += 1
        except Exception as e:
            print(f"Warning: Could not extract file statistics from report: {e}")
    
    # If we have file type statistics, include them in the report
    if file_type_stats:
        output.append("")
        
        # Add detailed processing information if available
        if file_processing_stats:
            registered = int(file_processing_stats.get('total_registered', 0))
            completed = int(file_processing_stats.get('completed', 0))
            pending = int(file_processing_stats.get('pending', 0))
            processing = int(file_processing_stats.get('processing', 0))
            error = int(file_processing_stats.get('error', 0))
            
            output.append(f"Files Registered in Database: {registered}")
            output.append(f"Analysis Progress:")
            output.append(f"  Completed: {completed} files ({(completed/float(registered)*100):.1f}% of registered)")
            output.append(f"  Pending: {pending} files")
            output.append(f"  In Progress: {processing} files")
            output.append(f"  Error: {error} files")
            
            # Add processing time and completion estimates if available
            if time_stats:
                elapsed_time = time_stats.get('elapsed_time_formatted', '0:00:00')
                files_per_hour = float(time_stats.get('files_per_hour', 0))
                estimated_completion = time_stats.get('estimated_completion_time', 'Unknown')
                
                output.append(f"Processing Time Statistics:")
                output.append(f"  Total Processing Time: {elapsed_time}")
                output.append(f"  Processing Rate: {files_per_hour} files/hour")
                
                if pending > 0 and files_per_hour > 0:
                    output.append(f"  Estimated Time to Completion: {estimated_completion}")
        else:
            output.append(f"Total Files Analyzed: {total_files}")
            
        output.append("File Types:")
        for ext, count in sorted(file_type_stats.items(), key=lambda x: x[1], reverse=True):
            output.append(f"  {ext}: {count} files")
    
    # Count files by breach type
    breach_types = {}
    for file_path, entities in high_risk_files.items():
        entity_types = {e['type'] for e in entities}
        breach_type = classify_breach(entity_types)
        breach_types[breach_type] = breach_types.get(breach_type, 0) + 1
    
    # Generate breach summary
    output.append("")
    output.append(f"HIGH-RISK FILES: {len(high_risk_files)} files contain personal information subject to breach notification")
    
    # Show counts by breach type
    output.append("")
    output.append("Files by Risk Category:")
    for breach_type, label in sorted(BREACH_CLASSIFICATIONS.items(), key=lambda x: x[0]):
        count = breach_types.get(label, 0)
        if count > 0:
            output.append(f"  {label}: {count} files")
    
    # Summary of key risk areas
    output.append("")
    output.append("Top Risk Areas:")
    
    ssn_count = breach_types.get(BREACH_CLASSIFICATIONS["NAME_WITH_SSN"], 0)
    if ssn_count > 0:
        output.append(f"  • {ssn_count} files contain names with Social Security Numbers")
        
    financial_count = breach_types.get(BREACH_CLASSIFICATIONS["NAME_WITH_FINANCIALS"], 0)
    if financial_count > 0:
        output.append(f"  • {financial_count} files contain names with financial account information")
        
    gov_id_count = breach_types.get(BREACH_CLASSIFICATIONS["NAME_WITH_GOV_ID"], 0)
    if gov_id_count > 0:
        output.append(f"  • {gov_id_count} files contain names with government ID numbers")
    
    credentials_count = breach_types.get(BREACH_CLASSIFICATIONS["CREDENTIALS"], 0)
    if credentials_count > 0:
        output.append(f"  • {credentials_count} files contain credential pairs (username/email with password)")
    
    # Return formatted summary
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
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="NC Breach Notification Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze PII data from JSON file
  python strict_nc_breach_pii.py --input results.json

  # Analyze PII data from database
  python strict_nc_breach_pii.py --db-path results.db

  # Generate detailed report with examples
  python strict_nc_breach_pii.py --input results.json --detailed-report

  # Export high-risk files to separate location
  python strict_nc_breach_pii.py --input results.json --copy-high-risk-files high_risk/
"""
    )
    
    # Input options
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--input", "-i", type=str, help="Input JSON file with PII analysis results")
    input_group.add_argument("--db-path", "-d", type=str, help="Database file with PII analysis results")
    parser.add_argument("--job-id", type=int, help="Specific job ID to analyze (for database input)")
    
    # Output options
    parser.add_argument("--output", "-o", type=str, help="Output file for report (default: stdout)")
    parser.add_argument("--format", "-f", type=str, choices=["text", "json"], default="text", 
                        help="Output format (default: text)")
    parser.add_argument("--summary", "-s", action="store_true", help="Show only executive summary")
    parser.add_argument("--detailed-report", "-r", action="store_true", help="Include detailed file info with examples")
    
    # Processing options
    parser.add_argument("--threshold", "-t", type=float, default=HIGH_CONFIDENCE_THRESHOLD,
                        help=f"Confidence threshold (default: {HIGH_CONFIDENCE_THRESHOLD})")
    parser.add_argument("--copy-high-risk-files", "-c", type=str,
                       help="Copy high-risk files to specified directory")
    
    return parser.parse_args()

def main():
    """Main function."""
    args = parse_arguments()
    
    try:
        # Analyze PII data based on input type
        if args.input:
            print(f"Analyzing PII report from JSON file: {args.input}")
            high_risk_files = analyze_pii_report(args.input, args.threshold)
        else:
            print(f"Analyzing PII data from database: {args.db_path}")
            high_risk_files = analyze_pii_database(args.db_path, args.job_id, args.threshold)
        
        print(f"Found {len(high_risk_files)} high-risk files that trigger breach notification")
        
        # Generate appropriate report
        if args.format == "text":
            if args.summary or not args.detailed_report:
                if args.input:
                    report = generate_executive_summary(high_risk_files, args.input)
                else:
                    report = generate_executive_summary(high_risk_files, db_path=args.db_path, job_id=args.job_id)
            else:
                report = generate_report_text(high_risk_files)
        else:  # json format
            report = generate_report_json(high_risk_files)
        
        # Output report
        if args.output:
            with open(args.output, 'w') as f:
                f.write(report)
            print(f"Report saved to {args.output}")
        else:
            print("\n" + report)
        
        # Copy high-risk files if requested
        if args.copy_high_risk_files:
            copied_files = clone_high_risk_files(high_risk_files, args.copy_high_risk_files)
            print(f"Copied {len(copied_files)} high-risk files to {args.copy_high_risk_files}")
        
        return high_risk_files
        
    except Exception as e:
        import traceback
        import sys
        print(f"Error: {e}")
        print("\nDetailed traceback:")
        traceback.print_exc(file=sys.stdout)
        return {}

if __name__ == "__main__":
    main() 