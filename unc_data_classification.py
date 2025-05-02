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

def generate_executive_summary(classified_files: Dict[str, Dict], original_report_path: str = None) -> str:
    """Generate a concise executive summary report of classified files."""
    output = []
    output.append(f"UNC DATA CLASSIFICATION EXECUTIVE SUMMARY")
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
    
    # Count files by tier
    tier_counts = defaultdict(int)
    for file_info in classified_files.values():
        tier = file_info['tier']
        tier_counts[tier] += 1
    
    output.append(f"Files Classified: {len(classified_files)}")
    output.append(f"Classification Breakdown:")
    for tier in sorted(UNCTier, reverse=True):
        if tier_counts[tier] > 0:
            output.append(f"  {TIER_DISPLAY[tier]['name']}: {tier_counts[tier]} files")
    
    output.append("-" * 80)
    output.append(f"{'TIER':<20} {'ENTITIES':<8} {'FILE PATH':<52}")
    output.append("-" * 80)
    
    # Group files by tier for ordered output
    files_by_tier = defaultdict(list)
    for file_path, file_info in classified_files.items():
        tier = file_info['tier']
        entity_count = len(file_info['entities'])
        files_by_tier[tier].append((file_path, entity_count))
    
    # Generate the report lines, ordered by tier level (high to low)
    for tier in sorted(files_by_tier.keys(), reverse=True):
        tier_name = TIER_DISPLAY[tier]['name']
        
        # Sort files within tier by entity count
        sorted_files = sorted(files_by_tier[tier], key=lambda x: x[1], reverse=True)
        
        for file_path, entity_count in sorted_files:
            # Truncate path if too long
            if len(file_path) > 51:
                display_path = "..." + file_path[-48:]
            else:
                display_path = file_path
                
            output.append(f"{tier_name:<20} {entity_count:<8} {display_path}")
    
    # Add listing of entity types found by tier
    output.append("-" * 80)
    output.append("Entity Types by Classification Tier:")
    
    # Collect entity types found in each tier
    entities_by_tier = {
        UNCTier.RESTRICTED: set(),
        UNCTier.CONFIDENTIAL: set(),
        UNCTier.INTERNAL: set()
    }
    
    for file_info in classified_files.values():
        tier = file_info['tier']
        entity_types = set(file_info['entity_types'])
        
        # Add actual entities found to the respective tier
        if tier == UNCTier.RESTRICTED:
            entities_by_tier[UNCTier.RESTRICTED].update(entity_types & RESTRICTED)
        elif tier == UNCTier.CONFIDENTIAL:
            entities_by_tier[UNCTier.CONFIDENTIAL].update(entity_types & CONFIDENTIAL)
        elif tier == UNCTier.INTERNAL:
            entities_by_tier[UNCTier.INTERNAL].update(entity_types & INTERNAL)
    
    # Display entities by tier
    for tier in sorted(entities_by_tier.keys(), reverse=True):
        if not entities_by_tier[tier]:
            continue
            
        tier_name = TIER_DISPLAY[tier]['name']
        output.append(f"  {tier_name}:")
        
        for entity_type in sorted(entities_by_tier[tier]):
            display_name = ENTITY_DISPLAY_NAMES.get(entity_type, entity_type)
            output.append(f"    - {display_name} ({entity_type})")
    
    # Add legend
    output.append("-" * 80)
    output.append(f"UNC DATA CLASSIFICATION TIERS:")
    for tier in sorted(UNCTier, reverse=True):
        output.append(f"  {TIER_DISPLAY[tier]['name']}: {TIER_DISPLAY[tier]['description']}")
    
    return "\n".join(output)

def generate_detailed_report(classified_files: Dict[str, Dict]) -> str:
    """Generate a detailed report of classified files."""
    output = []
    output.append(f"UNC DATA CLASSIFICATION DETAILED REPORT")
    output.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    output.append(f"\nFiles requiring protection according to UNC data classification:\n")
    
    # Group files by tier for ordered output
    files_by_tier = defaultdict(list)
    for file_path, file_info in classified_files.items():
        tier = file_info['tier']
        files_by_tier[tier].append((file_path, file_info))
    
    # Generate the report lines, ordered by tier level (high to low)
    for tier in sorted(files_by_tier.keys(), reverse=True):
        tier_name = TIER_DISPLAY[tier]['name']
        output.append(f"\n{'='*40} {tier_name} {'='*40}")
        
        # Sort files within tier by entity count
        sorted_file_entries = sorted(files_by_tier[tier], key=lambda x: len(x[1]['entities']), reverse=True)
        
        for file_path, file_info in sorted_file_entries:
            output.append(f"\nFile: {file_path}")
            output.append(f"Classification: {tier_name}")
            output.append(f"Number of entities: {len(file_info['entities'])}")
            
            # Count entity types
            entity_counts = defaultdict(int)
            for entity in file_info['entities']:
                entity_counts[entity['category']] += 1
            
            output.append("Entity types:")
            for category, count in sorted(entity_counts.items(), key=lambda x: x[1], reverse=True):
                output.append(f"  - {category}: {count}")
            
            # Determine classification reason
            output.append("Classification reason:")
            entity_types = set(file_info['entity_types'])
            restricted_found = entity_types & RESTRICTED
            confidential_found = entity_types & CONFIDENTIAL
            internal_found = entity_types & INTERNAL
            
            if restricted_found:
                output.append(f"  - Contains Tier-3 Restricted data types: {', '.join(restricted_found)}")
            elif confidential_found:
                output.append(f"  - Contains Tier-2 Confidential data types: {', '.join(confidential_found)}")
            elif internal_found:
                output.append(f"  - Contains Tier-1 Internal data types: {', '.join(internal_found)}")
            
            # Show sample of entity text (max 3 per type)
            output.append("Sample entities (max 3 per type):")
            samples_by_type = defaultdict(list)
            
            for entity in file_info['entities']:
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
    
    return "\n".join(output)

def generate_report_json(classified_files: Dict[str, Dict]) -> str:
    """Generate a JSON representation of the classified files."""
    report = {
        "metadata": {
            "report_type": "UNC Data Classification Analysis",
            "generated_at": datetime.now().isoformat(),
            "file_count": len(classified_files)
        },
        "tier_counts": {
            str(tier.value): 0 for tier in UNCTier
        },
        "classified_files": {}
    }
    
    # Count files by tier
    for file_info in classified_files.values():
        tier = file_info['tier']
        report["tier_counts"][str(tier.value)] += 1
    
    # Add file details
    for file_path, file_info in classified_files.items():
        tier = file_info['tier']
        entity_types = set(file_info['entity_types'])
        entity_by_type = defaultdict(list)
        
        # Group entities by type
        for entity in file_info['entities']:
            entity_by_type[entity['type']].append({
                "text": mask_sensitive_text(entity['text'], entity['type']),
                "confidence": entity['confidence'],
                "category": entity['category']
            })
        
        # Determine classification reason
        restricted_found = entity_types & RESTRICTED
        confidential_found = entity_types & CONFIDENTIAL
        internal_found = entity_types & INTERNAL
        
        classification_reasons = []
        if restricted_found:
            classification_reasons.append({
                "tier": "restricted",
                "entities": list(restricted_found)
            })
        elif confidential_found:
            classification_reasons.append({
                "tier": "confidential",
                "entities": list(confidential_found)
            })
        elif internal_found:
            classification_reasons.append({
                "tier": "internal",
                "entities": list(internal_found)
            })
        
        # Add to the report
        report["classified_files"][file_path] = {
            "tier": int(tier),
            "tier_name": TIER_DISPLAY[tier]['name'],
            "entity_count": len(file_info['entities']),
            "entity_types": list(entity_types),
            "classification_reasons": classification_reasons,
            "entities_by_type": dict(entity_by_type)
        }
    
    return json.dumps(report, indent=2)

def clone_classified_files(classified_files: Dict[str, Dict], clone_dir: str, min_tier: UNCTier = None) -> List[str]:
    """
    Clone the classified files to a specified directory maintaining structure.
    Optionally filter by minimum classification tier.
    """
    if not os.path.exists(clone_dir):
        os.makedirs(clone_dir)
    
    copied_files = []
    
    for file_path, file_info in classified_files.items():
        # Skip files below the minimum tier if specified
        if min_tier is not None and file_info['tier'] < min_tier:
            continue
            
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

def mask_sensitive_text(text: str, entity_type: str) -> str:
    """Masks sensitive text for display in reports."""
    # Restricted tier - highly sensitive
    if entity_type in RESTRICTED:
        # Show only last 4 characters for sensitive data
        if len(text) > 4:
            return f"****{text[-4:]}"
        return "****"
    # Confidential tier
    elif entity_type in CONFIDENTIAL:
        # Partial masking
        if len(text) > 5:
            return f"{text[0:2]}****{text[-2:]}"
        return "****"
    # Internal tier - emails, names
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

def parse_arguments() -> argparse.Namespace:
    """Parse and validate command line arguments."""
    parser = argparse.ArgumentParser(
        description="UNC Data Classification Analysis",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        "report_file",
        help="Path to the PII analysis report JSON file"
    )
    
    parser.add_argument(
        "-o", "--output",
        help="Output file path for the classification report (default: stdout)"
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
        help="Directory to create cloned structure of classified files"
    )
    
    parser.add_argument(
        "-m", "--min-tier",
        type=int,
        choices=[0, 1, 2, 3],
        help="Minimum tier to include in report and cloning (0=Public, 3=Restricted)"
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

def main() -> Dict[str, Any]:
    """Main entry point for the script."""
    args = parse_arguments()
    
    # Analyze the report
    classified_files = analyze_pii_report(args.report_file, args.threshold)
    
    # Filter by minimum tier if specified
    if args.min_tier is not None:
        min_tier = UNCTier(args.min_tier)
        classified_files = {
            file_path: file_info 
            for file_path, file_info in classified_files.items() 
            if file_info['tier'] >= min_tier
        }
    
    # Generate the report
    if args.format == "text":
        if args.verbose:
            report = generate_detailed_report(classified_files)
        else:
            report = generate_executive_summary(classified_files, args.report_file)
    else:  # json
        report = generate_report_json(classified_files)
    
    # Output the report
    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        print(f"Report written to {args.output}")
    else:
        print(report)
    
    # Clone files if requested
    if args.clone_dir and classified_files:
        min_tier = UNCTier(args.min_tier) if args.min_tier is not None else None
        copied_files = clone_classified_files(classified_files, args.clone_dir, min_tier)
        if copied_files:
            print(f"\nCloned {len(copied_files)} classified files to {args.clone_dir}")
        else:
            print(f"\nNo files were copied to {args.clone_dir}")
    
    # Return summary for non-interactive usage
    return {
        "files_analyzed": len(classified_files),
        "report_format": args.format,
        "output_file": args.output,
        "clone_directory": args.clone_dir if args.clone_dir else None,
        "tier_counts": {
            TIER_DISPLAY[tier]['name']: sum(1 for info in classified_files.values() if info['tier'] == tier)
            for tier in UNCTier
        }
    }

if __name__ == '__main__':
    main() 