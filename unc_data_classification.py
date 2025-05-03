#!/usr/bin/env python3
"""
UNC Data Classification Analysis
Analyzes PII analysis results to classify documents according to
UNC-System data classification tiers (Public, Internal, Confidential, Restricted)
"""

import json
import sys
import os
import argparse
import shutil
from enum import IntEnum
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Optional, Any, Tuple

# Add src directory to path to allow imports from PII analyzer modules
sys.path.append(os.path.abspath(os.path.dirname(__file__)))
from src.database.db_reporting import load_pii_data_from_db, get_file_type_statistics

# UNC-System classification tiers
class UNCTier(IntEnum):
    PUBLIC = 0        # Tier-0
    INTERNAL = 1      # Tier-1
    CONFIDENTIAL = 2  # Tier-2
    RESTRICTED = 3    # Tier-3

# Maps entity types to UNC classification tiers
RESTRICTED = {
    "US_SOCIAL_SECURITY_NUMBER", "US_SSN", "CREDIT_CARD", "BANK_ACCOUNT",
    "US_DRIVER_LICENSE", "US_PASSPORT",
    "MEDICAL_RECORD_NUMBER", "HEALTH_INSURANCE_POLICY_NUMBER",
    "PASSWORD", "ACCESS_CODE", "AWS_SECRET_KEY", "ITAR_CONTROLLED",
    "PIN_CODE", "SECURITY_ANSWER", "DIGITAL_SIGNATURE", "BIOMETRIC_IDENTIFIER",
}

CONFIDENTIAL = {
    "STUDENT_ID", "EMPLOYEE_ID", "GOV_ID",
    "IBAN_CODE", "US_BANK_NUMBER", "US_BANK_ROUTING", "SWIFT_CODE",
    "DONOR_NAME", "GRANT_ID"
}

INTERNAL = {
    "PERSON", "FIRST_NAME", "LAST_NAME", "EMAIL_ADDRESS", "PHONE_NUMBER",
    "ORG", "IP_ADDRESS", "US_POSTAL_CODE", "USERNAME", "ADDRESS"
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
    "SWIFT_CODE": "SWIFT Code",
    "MEDICAL_RECORD_NUMBER": "Medical Record Number",
    "HEALTH_INSURANCE_POLICY_NUMBER": "Health Insurance Policy Number",
    "PIN_CODE": "PIN Code",
    "PASSWORD": "Password",
    "SECURITY_ANSWER": "Security Answer",
    "DIGITAL_SIGNATURE": "Digital Signature",
    "BIOMETRIC_IDENTIFIER": "Biometric Identifier",
    "AWS_SECRET_KEY": "AWS Secret Key",
    "ITAR_CONTROLLED": "ITAR Controlled Data",
    "STUDENT_ID": "Student ID",
    "EMPLOYEE_ID": "Employee ID",
    "GOV_ID": "Government ID",
    "DONOR_NAME": "Donor Name",
    "GRANT_ID": "Grant ID",
    "PERSON": "Person Name",
    "FIRST_NAME": "First Name",
    "LAST_NAME": "Last Name",
    "EMAIL_ADDRESS": "Email Address",
    "PHONE_NUMBER": "Phone Number",
    "ORG": "Organization",
    "IP_ADDRESS": "IP Address",
    "US_POSTAL_CODE": "Postal Code",
    "USERNAME": "Username",
    "ADDRESS": "Address",
    "ACCESS_CODE": "Access Code"
}

# Tier display information
TIER_DISPLAY = {
    UNCTier.PUBLIC: {
        "name": "Tier-0 Public",
        "color": "green",
        "description": "Information that can be freely shared"
    },
    UNCTier.INTERNAL: {
        "name": "Tier-1 Internal",
        "color": "blue",
        "description": "Non-sensitive information, intended for internal use"
    },
    UNCTier.CONFIDENTIAL: {
        "name": "Tier-2 Confidential",
        "color": "yellow",
        "description": "Sensitive information requiring protection"
    },
    UNCTier.RESTRICTED: {
        "name": "Tier-3 Restricted",
        "color": "red",
        "description": "Highly sensitive information with regulatory requirements"
    }
}

# Threshold values for high confidence PII
HIGH_CONFIDENCE_THRESHOLD = 0.7

def tier_for_entities(entity_types: Set[str]) -> UNCTier:
    """
    Return the highest UNC tier indicated by the set of
    Presidio entity type strings found in a document.
    """
    if entity_types & RESTRICTED:
        return UNCTier.RESTRICTED
    if entity_types & CONFIDENTIAL:
        return UNCTier.CONFIDENTIAL
    if entity_types & INTERNAL:
        return UNCTier.INTERNAL
    return UNCTier.PUBLIC

def analyze_pii_report(report_path: str, threshold: float = HIGH_CONFIDENCE_THRESHOLD) -> Dict[str, Dict]:
    """
    Analyzes a PII report to classify files according to UNC data classification tiers.
    
    Args:
        report_path: Path to the PII analysis report JSON file
        threshold: Confidence threshold for entities (default: 0.7)
        
    Returns:
        Dictionary of files with their entities and classification tiers
    """
    with open(report_path, 'r') as f:
        data = json.load(f)
    
    # Dictionary to track files and their entities
    file_entities = defaultdict(list)
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
                # Add to entity set for tier evaluation
                file_entity_sets[file_path].add(entity_type)
                
                # Store all entities for reporting
                file_entities[file_path].append({
                    'type': entity_type,
                    'category': ENTITY_DISPLAY_NAMES.get(entity_type, entity_type),
                    'confidence': confidence,
                    'text': text
                })
    
    # Classify each file according to UNC tiers
    classified_files = {}
    for file_path, entity_set in file_entity_sets.items():
        tier = tier_for_entities(entity_set)
        
        # Only include files with entities (exclude purely public)
        if len(file_entities[file_path]) > 0:
            classified_files[file_path] = {
                'tier': tier,
                'tier_name': TIER_DISPLAY[tier]['name'],
                'entities': file_entities[file_path],
                'entity_types': list(entity_set)
            }
    
    return classified_files

def analyze_pii_database(db_path: str, job_id: Optional[int] = None, threshold: float = HIGH_CONFIDENCE_THRESHOLD) -> Dict[str, Dict]:
    """
    Analyzes PII data from a database to classify files according to UNC data classification tiers.
    
    Args:
        db_path: Path to the SQLite database file
        job_id: Specific job ID to analyze (most recent if None)
        threshold: Confidence threshold for entities
        
    Returns:
        Dictionary of files with their entities and classification tiers
    """
    # Load data from database
    data = load_pii_data_from_db(db_path, job_id, threshold)
    
    # Dictionary to track files and their entities
    file_entities = defaultdict(list)
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
            
            # Add to entity set for tier evaluation
            file_entity_sets[file_path].add(entity_type)
            
            # Store all entities for reporting
            file_entities[file_path].append({
                'type': entity_type,
                'category': ENTITY_DISPLAY_NAMES.get(entity_type, entity_type),
                'confidence': confidence,
                'text': text
            })
    
    # Classify each file according to UNC tiers
    classified_files = {}
    for file_path, entity_set in file_entity_sets.items():
        tier = tier_for_entities(entity_set)
        
        # Only include files with entities (exclude purely public)
        if len(file_entities[file_path]) > 0:
            classified_files[file_path] = {
                'tier': tier,
                'tier_name': TIER_DISPLAY[tier]['name'],
                'entities': file_entities[file_path],
                'entity_types': list(entity_set)
            }
    
    return classified_files

def generate_executive_summary(classified_files: Dict[str, Dict], original_report_path: str = None, db_path: str = None, job_id: Optional[int] = None) -> str:
    """Generate a concise executive summary report of classified files."""
    output = []
    output.append(f"UNC DATA CLASSIFICATION EXECUTIVE SUMMARY")
    output.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Extract file type statistics if available
    file_type_stats = {}
    total_files = 0
    file_processing_stats = None
    
    # Try to extract file type information and processing stats from the database if provided
    if db_path:
        try:
            file_type_stats = get_file_type_statistics(db_path, job_id)
            total_files = sum(file_type_stats.values())
            
            # Get file processing statistics
            from src.database.db_reporting import get_file_processing_stats, get_processing_time_stats
            file_processing_stats = get_file_processing_stats(db_path, job_id)
            time_stats = get_processing_time_stats(db_path, job_id)
        except Exception as e:
            print(f"Warning: Could not extract file statistics from database: {e}")
    
    # If database not provided or failed, try from the original report
    if not file_type_stats and original_report_path and os.path.exists(original_report_path):
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
            registered = file_processing_stats.get('total_registered', 0)
            completed = file_processing_stats.get('completed', 0)
            pending = file_processing_stats.get('pending', 0)
            processing = file_processing_stats.get('processing', 0)
            error = file_processing_stats.get('error', 0)
            
            output.append(f"Files Registered in Database: {registered}")
            output.append(f"Analysis Progress:")
            output.append(f"  Completed: {completed} files ({(completed/registered*100):.1f}% of registered)")
            output.append(f"  Pending: {pending} files")
            output.append(f"  In Progress: {processing} files")
            output.append(f"  Error: {error} files")
            
            # Add processing time and completion estimates if available
            if time_stats:
                elapsed_time = time_stats.get('elapsed_time_formatted', '0:00:00')
                files_per_hour = time_stats.get('files_per_hour', 0)
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
    
    # Count classification tiers
    tier_counts = {}
    for file_data in classified_files.values():
        tier = file_data['tier']
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
    
    # Generate summary
    output.append("")
    output.append(f"Classified Files: {len(classified_files)} files contain sensitive information")
    
    # Show counts by tier
    output.append("")
    output.append("Classification Summary:")
    for tier in sorted([UNCTier.RESTRICTED, UNCTier.CONFIDENTIAL, UNCTier.INTERNAL, UNCTier.PUBLIC]):
        count = tier_counts.get(tier, 0)
        if count > 0:
            tier_info = TIER_DISPLAY[tier]
            output.append(f"  {tier_info['name']}: {count} files")
    
    # Add entity type counts by tier
    entity_by_tier = defaultdict(set)
    for file_data in classified_files.values():
        tier = file_data['tier']
        entity_types = set(file_data['entity_types'])
        entity_by_tier[tier].update(entity_types)
    
    output.append("")
    output.append("Top Sensitive Entity Types by Tier:")
    
    # Restricted entities
    if UNCTier.RESTRICTED in entity_by_tier and entity_by_tier[UNCTier.RESTRICTED]:
        output.append(f"  {TIER_DISPLAY[UNCTier.RESTRICTED]['name']}:")
        for entity_type in sorted(entity_by_tier[UNCTier.RESTRICTED] & RESTRICTED):
            output.append(f"    • {ENTITY_DISPLAY_NAMES.get(entity_type, entity_type)}")
    
    # Confidential entities
    if UNCTier.CONFIDENTIAL in entity_by_tier and entity_by_tier[UNCTier.CONFIDENTIAL]:
        output.append(f"  {TIER_DISPLAY[UNCTier.CONFIDENTIAL]['name']}:")
        for entity_type in sorted(entity_by_tier[UNCTier.CONFIDENTIAL] & CONFIDENTIAL):
            output.append(f"    • {ENTITY_DISPLAY_NAMES.get(entity_type, entity_type)}")
    
    # Internal entities
    if UNCTier.INTERNAL in entity_by_tier and entity_by_tier[UNCTier.INTERNAL]:
        output.append(f"  {TIER_DISPLAY[UNCTier.INTERNAL]['name']}:")
        for entity_type in sorted(entity_by_tier[UNCTier.INTERNAL] & INTERNAL):
            output.append(f"    • {ENTITY_DISPLAY_NAMES.get(entity_type, entity_type)}")
    
    # Return formatted summary
    return "\n".join(output)

def generate_detailed_report(classified_files: Dict[str, Dict]) -> str:
    """Generate a detailed report of classified files."""
    output = []
    output.append(f"UNC DATA CLASSIFICATION DETAILED REPORT")
    output.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append(f"Total Files: {len(classified_files)}")
    output.append("-" * 80)
    
    # Sort files by tier (highest to lowest) and then by path
    sorted_files = sorted(
        classified_files.items(), 
        key=lambda x: (-x[1]['tier'], x[0])
    )
    
    # Generate detailed report for each file
    for file_path, file_data in sorted_files:
        tier = file_data['tier']
        tier_name = file_data['tier_name']
        entities = file_data['entities']
        
        # Get color code for tier
        tier_color = TIER_DISPLAY[tier]['color']
        
        # Add file information
        output.append(f"File: {file_path}")
        output.append(f"Classification: {tier_name}")
        output.append(f"Entities:")
        
        # Group entities by type
        entity_groups = defaultdict(list)
        for entity in entities:
            entity_groups[entity['type']].append(entity)
        
        # Print entities by type, sorted by highest sensitivity
        for entity_type, group in sorted(
            entity_groups.items(),
            key=lambda x: (
                -(3 if x[0] in RESTRICTED else 
                  2 if x[0] in CONFIDENTIAL else 
                  1 if x[0] in INTERNAL else 0),
                x[0]
            )
        ):
            # Get friendly name for entity type
            friendly_name = ENTITY_DISPLAY_NAMES.get(entity_type, entity_type)
            
            # Determine sensitivity tier of this entity type
            entity_tier = "Restricted" if entity_type in RESTRICTED else \
                         "Confidential" if entity_type in CONFIDENTIAL else \
                         "Internal" if entity_type in INTERNAL else "Public"
            
            output.append(f"  - {friendly_name} ({entity_tier}):")
            
            # List up to 5 examples with scores
            for i, entity in enumerate(sorted(group, key=lambda x: -x['confidence'])[:5]):
                # Mask sensitive text for reporting
                masked_text = mask_sensitive_text(entity['text'], entity_type)
                output.append(f"    {masked_text} (confidence: {entity['confidence']:.2f})")
            
            # If more than 5, show count
            if len(group) > 5:
                output.append(f"    ... and {len(group) - 5} more instances")
        
        output.append("-" * 80)
    
    return "\n".join(output)

def generate_report_json(classified_files: Dict[str, Dict]) -> str:
    """Generate a JSON report of classified files."""
    # Create a structure suitable for JSON serialization
    report = {
        "generated_at": datetime.now().isoformat(),
        "total_files": len(classified_files),
        "tier_counts": {
            "tier3_restricted": sum(1 for d in classified_files.values() if d['tier'] == UNCTier.RESTRICTED),
            "tier2_confidential": sum(1 for d in classified_files.values() if d['tier'] == UNCTier.CONFIDENTIAL),
            "tier1_internal": sum(1 for d in classified_files.values() if d['tier'] == UNCTier.INTERNAL),
            "tier0_public": sum(1 for d in classified_files.values() if d['tier'] == UNCTier.PUBLIC)
        },
        "files": []
    }
    
    # Convert IntEnum tiers to strings for JSON serialization
    tier_names = {
        UNCTier.RESTRICTED: "tier3_restricted",
        UNCTier.CONFIDENTIAL: "tier2_confidential",
        UNCTier.INTERNAL: "tier1_internal",
        UNCTier.PUBLIC: "tier0_public"
    }
    
    # Add file details
    for file_path, file_data in classified_files.items():
        # Create a serializable structure for each file
        file_entry = {
            "file_path": file_path,
            "classification": tier_names[file_data['tier']],
            "tier_name": file_data['tier_name'],
            "entity_count": len(file_data['entities']),
            "entity_types": file_data['entity_types'],
            "entities": []
        }
        
        # Add entity details
        for entity in file_data['entities']:
            # Mask sensitive text for reporting
            masked_text = mask_sensitive_text(entity['text'], entity['type'])
            
            entity_entry = {
                "type": entity['type'],
                "category": entity['category'],
                "text": masked_text,
                "confidence": entity['confidence']
            }
            file_entry["entities"].append(entity_entry)
        
        report["files"].append(file_entry)
    
    return json.dumps(report, indent=2)

def clone_classified_files(classified_files: Dict[str, Dict], clone_dir: str, min_tier: UNCTier = None) -> List[str]:
    """
    Clone classified files to a directory structure organized by tier.
    
    Args:
        classified_files: Dictionary of classified files
        clone_dir: Base directory to clone files to
        min_tier: Minimum tier to include (default: include all)
        
    Returns:
        List of copied file paths
    """
    os.makedirs(clone_dir, exist_ok=True)
    
    # Create tier subdirectories
    for tier in UNCTier:
        if min_tier is None or tier >= min_tier:
            tier_dir = os.path.join(clone_dir, TIER_DISPLAY[tier]['name'])
            os.makedirs(tier_dir, exist_ok=True)
    
    # Copy files to appropriate tier directories
    copied_files = []
    for file_path, file_data in classified_files.items():
        tier = file_data['tier']
        
        # Skip if below minimum tier
        if min_tier is not None and tier < min_tier:
            continue
        
        # Create target directory
        target_dir = os.path.join(clone_dir, TIER_DISPLAY[tier]['name'])
        
        # Copy file
        try:
            if os.path.exists(file_path):
                filename = os.path.basename(file_path)
                target_path = os.path.join(target_dir, filename)
                
                # Handle duplicate filenames by adding a suffix
                if os.path.exists(target_path):
                    base, ext = os.path.splitext(filename)
                    i = 1
                    while os.path.exists(target_path):
                        target_path = os.path.join(target_dir, f"{base}_{i}{ext}")
                        i += 1
                
                shutil.copy2(file_path, target_path)
                copied_files.append(file_path)
        except Exception as e:
            print(f"Error copying {file_path}: {e}")
    
    return copied_files

def mask_sensitive_text(text: str, entity_type: str) -> str:
    """
    Mask sensitive text for display in reports.
    Different entity types are masked differently.
    
    Args:
        text: Original text
        entity_type: Type of entity
        
    Returns:
        Masked text
    """
    if not text:
        return ""
    
    # Highly sensitive information (always mask completely)
    if entity_type in {"US_SOCIAL_SECURITY_NUMBER", "US_SSN", "CREDIT_CARD", 
                      "PASSWORD", "ACCESS_CODE", "PIN_CODE", 
                      "SECURITY_ANSWER", "AWS_SECRET_KEY"}:
        return "********"
    
    # Partially mask names
    if entity_type in {"PERSON", "FIRST_NAME", "LAST_NAME"}:
        parts = text.split()
        if len(parts) == 1:
            # Single name
            if len(text) <= 2:
                return text[0] + "*"
            return text[0] + "*" * (len(text) - 1)
        else:
            # Multiple parts (e.g., "John Smith")
            masked_parts = []
            for part in parts:
                if len(part) <= 1:
                    masked_parts.append(part)
                else:
                    masked_parts.append(part[0] + "*" * (len(part) - 1))
            return " ".join(masked_parts)
    
    # Partially mask IDs and numbers
    if entity_type in {"US_DRIVER_LICENSE", "US_PASSPORT", "BANK_ACCOUNT", 
                       "MEDICAL_RECORD_NUMBER", "HEALTH_INSURANCE_POLICY_NUMBER",
                       "US_BANK_NUMBER", "US_BANK_ROUTING", "IBAN_CODE"}:
        if len(text) <= 4:
            return "*" * len(text)
        return "*" * (len(text) - 4) + text[-4:]
    
    # Partially mask emails
    if entity_type == "EMAIL_ADDRESS" and "@" in text:
        username, domain = text.split("@", 1)
        if len(username) <= 2:
            masked_username = username
        else:
            masked_username = username[0] + "*" * (len(username) - 2) + username[-1]
        return f"{masked_username}@{domain}"
    
    # Default masking for other types - show first and last character
    if len(text) <= 2:
        return text
    return text[0] + "*" * (len(text) - 2) + text[-1]

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="UNC Data Classification Analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze PII data from JSON file
  python unc_data_classification.py --input results.json

  # Analyze PII data from database
  python unc_data_classification.py --db-path results.db

  # Generate detailed report with examples
  python unc_data_classification.py --input results.json --detailed-report

  # Export high-risk files to separate location
  python unc_data_classification.py --input results.json --copy-files classified/
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
    parser.add_argument("--min-tier", "-m", type=int, choices=[0, 1, 2, 3], default=1,
                        help="Minimum tier to include in report (0=Public, 1=Internal, 2=Confidential, 3=Restricted)")
    parser.add_argument("--copy-files", "-c", type=str,
                       help="Copy classified files to specified directory (organized by tier)")
    
    return parser.parse_args()

def main() -> Dict[str, Any]:
    """Main function."""
    args = parse_arguments()
    
    try:
        # Analyze PII data based on input type
        if args.input:
            print(f"Analyzing PII report from JSON file: {args.input}")
            classified_files = analyze_pii_report(args.input, args.threshold)
        else:
            print(f"Analyzing PII data from database: {args.db_path}")
            classified_files = analyze_pii_database(args.db_path, args.job_id, args.threshold)
        
        # Filter by minimum tier
        min_tier = UNCTier(args.min_tier)
        filtered_files = {
            path: data for path, data in classified_files.items()
            if data['tier'] >= min_tier
        }
        
        print(f"Found {len(filtered_files)} files classified as {TIER_DISPLAY[min_tier]['name']} or higher")
        
        # Generate appropriate report
        if args.format == "text":
            if args.summary or not args.detailed_report:
                if args.input:
                    report = generate_executive_summary(filtered_files, args.input)
                else:
                    report = generate_executive_summary(filtered_files, db_path=args.db_path, job_id=args.job_id)
            else:
                report = generate_detailed_report(filtered_files)
        else:  # json format
            report = generate_report_json(filtered_files)
        
        # Output report
        if args.output:
            with open(args.output, 'w') as f:
                f.write(report)
            print(f"Report saved to {args.output}")
        else:
            print("\n" + report)
        
        # Copy files if requested
        if args.copy_files:
            copied_files = clone_classified_files(filtered_files, args.copy_files)
            print(f"Copied {len(copied_files)} classified files to {args.copy_files}")
        
        return {
            "files_analyzed": len(classified_files),
            "files_reported": len(filtered_files),
            "min_tier": min_tier
        }
        
    except Exception as e:
        print(f"Error: {e}")
        return {
            "error": str(e)
        }

if __name__ == "__main__":
    main() 