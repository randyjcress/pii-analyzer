#!/usr/bin/env python3
"""
Analyze PII Analysis Export JSON File
Generates comprehensive statistics about the dataset
"""

import argparse
import json
import sys
from collections import Counter, defaultdict
from typing import Dict, List, Any
from datetime import datetime

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Analyze PII Analyzer export JSON file')
    parser.add_argument('input', help='JSON export file to analyze')
    parser.add_argument('--output', '-o', help='Output file (default: stdout)')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed statistics')
    return parser.parse_args()

def load_data(file_path: str) -> Dict[str, Any]:
    """Load JSON data from file"""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError) as e:
        print(f"Error loading JSON file: {e}", file=sys.stderr)
        sys.exit(1)

def format_time_duration(start_time_str: str, end_time_str: str) -> str:
    """Calculate and format duration between two timestamps"""
    try:
        start = datetime.fromisoformat(start_time_str.replace(' ', 'T'))
        end = datetime.fromisoformat(end_time_str.replace(' ', 'T'))
        duration = end - start
        hours, remainder = divmod(duration.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    except Exception:
        return "Unknown"

def analyze_data(data: Dict[str, Any], verbose: bool = False) -> Dict[str, Any]:
    """Analyze the data and return statistics"""
    stats = {}
    
    # Job information
    stats['job_id'] = data.get('job_id', 'Unknown')
    stats['status'] = data.get('status', 'Unknown')
    stats['duration'] = format_time_duration(
        data.get('start_time', ''), 
        data.get('end_time', '')
    )
    
    # Basic file statistics
    results = data.get('results', [])
    stats['total_files'] = len(results)
    stats['total_entities'] = sum(len(f.get('entities', [])) for f in results)
    stats['avg_entities_per_file'] = stats['total_entities'] / stats['total_files'] if stats['total_files'] > 0 else 0
    
    # File status counts
    status_counts = Counter(f.get('status', 'unknown') for f in results)
    stats['file_status_counts'] = dict(status_counts)
    
    # File sizes
    file_sizes = [f.get('file_size', 0) for f in results]
    stats['total_size_bytes'] = sum(file_sizes)
    stats['total_size_mb'] = stats['total_size_bytes'] / (1024 * 1024)
    stats['avg_file_size_kb'] = sum(file_sizes) / (len(file_sizes) * 1024) if file_sizes else 0
    stats['min_file_size_kb'] = min(file_sizes) / 1024 if file_sizes else 0
    stats['max_file_size_kb'] = max(file_sizes) / 1024 if file_sizes else 0
    
    # File type counts
    file_types = Counter(f.get('file_type', 'unknown') for f in results)
    stats['file_type_counts'] = dict(file_types)
    
    # Entity statistics
    all_entities = []
    for result in results:
        all_entities.extend(result.get('entities', []))
    
    entity_types = Counter(e.get('entity_type', 'unknown') for e in all_entities)
    stats['entity_type_counts'] = dict(entity_types)
    
    # Calculate entity confidence scores
    entity_scores = defaultdict(list)
    for entity in all_entities:
        entity_type = entity.get('entity_type', 'unknown')
        score = entity.get('score', 0)
        entity_scores[entity_type].append(score)
    
    stats['entity_confidence'] = {
        entity_type: {
            'avg': sum(scores) / len(scores) if scores else 0,
            'min': min(scores) if scores else 0,
            'max': max(scores) if scores else 0,
            'count': len(scores)
        }
        for entity_type, scores in entity_scores.items()
    }
    
    # Extraction method statistics
    extraction_methods = Counter()
    ocr_counts = 0
    for result in results:
        metadata = result.get('metadata', {})
        if not metadata:
            continue
            
        method = metadata.get('extraction_method', 'unknown')
        extraction_methods[method] += 1
        
        # Check if OCR was used
        if metadata.get('OCR', False):
            ocr_counts += 1
    
    stats['extraction_method_counts'] = dict(extraction_methods)
    stats['ocr_used_count'] = ocr_counts
    
    # Processing time statistics
    processing_times = [f.get('processing_time', 0) for f in results if f.get('processing_time', 0) > 0]
    if processing_times:
        stats['total_processing_time'] = sum(processing_times)
        stats['avg_processing_time'] = sum(processing_times) / len(processing_times)
        stats['min_processing_time'] = min(processing_times)
        stats['max_processing_time'] = max(processing_times)
        stats['processing_rate'] = stats['total_files'] / stats['total_processing_time'] if stats['total_processing_time'] > 0 else 0
    
    # Additional detailed statistics for verbose mode
    if verbose:
        # File path analysis
        path_components = [f.get('file_path', '').split('/') for f in results]
        directories = Counter(('/'.join(p[:-1]) if len(p) > 1 else '/') for p in path_components)
        stats['directory_counts'] = dict(directories)
        
        # Entity frequency by file type
        entity_by_file_type = defaultdict(Counter)
        for result in results:
            file_type = result.get('file_type', 'unknown')
            for entity in result.get('entities', []):
                entity_type = entity.get('entity_type', 'unknown')
                entity_by_file_type[file_type][entity_type] += 1
        
        stats['entity_by_file_type'] = {
            file_type: dict(counter)
            for file_type, counter in entity_by_file_type.items()
        }
    
    return stats

def print_statistics(stats: Dict[str, Any], verbose: bool = False):
    """Print statistics in a human-readable format"""
    print("\n===== PII Analysis Export Statistics =====\n")
    
    print(f"Job ID: {stats['job_id']}")
    print(f"Status: {stats['status']}")
    print(f"Duration: {stats['duration']}")
    print()
    
    print("=== File Statistics ===")
    print(f"Total Files: {stats['total_files']}")
    print(f"Total Size: {stats['total_size_mb']:.2f} MB")
    print(f"Average File Size: {stats['avg_file_size_kb']:.2f} KB")
    print()
    
    print("File Types:")
    for file_type, count in sorted(stats['file_type_counts'].items(), key=lambda x: x[1], reverse=True):
        print(f"  {file_type}: {count}")
    print()
    
    print("File Status:")
    for status, count in stats['file_status_counts'].items():
        print(f"  {status}: {count}")
    print()
    
    print("=== PII Entity Statistics ===")
    print(f"Total Entities: {stats['total_entities']}")
    print(f"Average Entities per File: {stats['avg_entities_per_file']:.2f}")
    print()
    
    print("Entity Types:")
    for entity_type, count in sorted(stats['entity_type_counts'].items(), key=lambda x: x[1], reverse=True):
        confidence = stats['entity_confidence'].get(entity_type, {})
        avg_conf = confidence.get('avg', 0) * 100  # Convert to percentage
        print(f"  {entity_type}: {count} (avg confidence: {avg_conf:.1f}%)")
    print()
    
    print("=== Processing Statistics ===")
    print("Extraction Methods:")
    for method, count in sorted(stats['extraction_method_counts'].items(), key=lambda x: x[1], reverse=True):
        print(f"  {method}: {count}")
    print(f"OCR Used: {stats['ocr_used_count']} files")
    
    if 'total_processing_time' in stats:
        print(f"Total Processing Time: {stats['total_processing_time']:.2f} seconds")
        print(f"Average Processing Time: {stats['avg_processing_time']:.2f} seconds per file")
        print(f"Processing Rate: {stats['processing_rate']:.2f} files per second")
    print()
    
    if verbose:
        print("=== Detailed Statistics ===")
        
        print("Entity Confidence:")
        for entity_type, data in sorted(stats['entity_confidence'].items(), key=lambda x: x[1]['count'], reverse=True):
            print(f"  {entity_type}:")
            print(f"    Count: {data['count']}")
            print(f"    Avg Confidence: {data['avg']*100:.1f}%")
            print(f"    Min Confidence: {data['min']*100:.1f}%")
            print(f"    Max Confidence: {data['max']*100:.1f}%")
        print()
        
        print("Entities by File Type:")
        for file_type, entity_counts in stats['entity_by_file_type'].items():
            if entity_counts:
                print(f"  {file_type}:")
                for entity_type, count in sorted(entity_counts.items(), key=lambda x: x[1], reverse=True):
                    print(f"    {entity_type}: {count}")
        print()
        
        print("Top Directories:")
        for directory, count in sorted(stats['directory_counts'].items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {directory}: {count}")
        print()

def main():
    """Main entry point"""
    args = parse_args()
    data = load_data(args.input)
    stats = analyze_data(data, args.verbose)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(stats, f, indent=2)
        print(f"Statistics written to {args.output}")
    
    print_statistics(stats, args.verbose)

if __name__ == "__main__":
    main() 