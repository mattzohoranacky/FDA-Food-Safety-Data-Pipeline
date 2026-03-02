#!/usr/bin/env python3
"""
Part 2: FDA Adverse Event Data Analysis & Reporting
Analyzes FDA food safety adverse event JSON files with filtering and visualization.
"""

import json
import sys
import os
from pathlib import Path
from datetime import datetime
from collections import Counter, defaultdict
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from typing import Tuple, List, Dict, Optional

# Configuration
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"

# If data directory doesn't exist in Part 2, look in Part 1
if not DATA_DIR.exists():
    part1_data = SCRIPT_DIR.parent / "Part 1" / "data"
    if part1_data.exists():
        DATA_DIR = part1_data

CHARTS_DIR = SCRIPT_DIR / "charts"
CURRENT_YEAR = datetime.now().year

# Ensure charts directory exists
CHARTS_DIR.mkdir(exist_ok=True)


def parse_arguments() -> Tuple[Optional[int], Optional[int], Optional[str]]:
    """
    Parse command-line arguments.
    
    Returns:
        Tuple of (start_year, end_year, product_filter)
        - One year: analyze from that year to present
        - Two years: analyze inclusive range
        - No years: 2000 to present
        - Product filter is case-insensitive substring match
    """
    start_year = 2000
    end_year = CURRENT_YEAR
    product_filter = None
    
    numeric_args = []
    string_args = []
    
    # Separate numeric and string arguments
    for arg in sys.argv[1:]:
        if arg.isdigit():
            numeric_args.append(int(arg))
        else:
            string_args.append(arg.upper())
    
    # Process numeric arguments
    if len(numeric_args) == 1:
        start_year = numeric_args[0]
        end_year = CURRENT_YEAR
    elif len(numeric_args) >= 2:
        start_year = min(numeric_args[0], numeric_args[1])
        end_year = max(numeric_args[0], numeric_args[1])
    
    # Product filter is the first string argument
    if string_args:
        product_filter = string_args[0]
    
    return start_year, end_year, product_filter


def extract_year_from_filename(filename: str) -> Optional[int]:
    """Extract year from filename like event_2021-CFS-001144.json"""
    try:
        if 'CFS' in filename:
            year_part = filename.split('CFS')[0].split('_')[-1]
            return int(year_part)
    except (ValueError, IndexError):
        pass
    return None


def extract_year_from_date(date_str: str) -> Optional[int]:
    """Extract year from YYYYMMDD format"""
    try:
        if len(date_str) >= 4:
            return int(date_str[:4])
    except ValueError:
        pass
    return None


def normalize_text(text: str) -> str:
    """Normalize text for deduplication"""
    if not isinstance(text, str):
        return ""
    return text.strip().upper()


def similar_enough(text1: str, text2: str) -> bool:
    """
    Check if two texts are similar enough to count as the same.
    Uses simple logic: if one is a substring of the other with >80% overlap, consider them same.
    """
    t1 = normalize_text(text1)
    t2 = normalize_text(text2)
    
    if t1 == t2:
        return True
    
    # Check substring overlap
    if len(t1) == 0 or len(t2) == 0:
        return False
    
    min_len = min(len(t1), len(t2))
    max_len = max(len(t1), len(t2))
    
    # If one is substring of other and >70% overlap
    if t1 in t2 or t2 in t1:
        if min_len / max_len >= 0.7:
            return True
    
    return False


def consolidate_items(items: List[str]) -> Dict[str, str]:
    """
    Create a mapping of similar items to their canonical form.
    Returns dict mapping each unique normalized item to its representative.
    """
    if not items:
        return {}
    
    normalized = {normalize_text(item): item for item in items if item}
    
    # Group similar items
    groups = []
    used = set()
    
    sorted_items = sorted(normalized.keys())
    for item in sorted_items:
        if item in used:
            continue
        group = [item]
        used.add(item)
        
        for other in sorted_items:
            if other not in used and similar_enough(item, other):
                group.append(other)
                used.add(other)
        
        groups.append(group)
    
    # Map each to the longest (most complete) representative
    mapping = {}
    for group in groups:
        longest = max(group, key=lambda x: len(normalized[x]))
        for normalized_item in group:
            mapping[normalized_item] = normalized[longest]
    
    return mapping


def get_suspect_products(products: List[Dict]) -> List[str]:
    """Extract suspect product names from products list"""
    suspect_products = []
    for product in products:
        if product.get('role') == 'SUSPECT':
            name = product.get('name_brand')
            if name and name != 'EXEMPTION 4':  # Skip exemption entries
                suspect_products.append(name)
    return suspect_products


def load_and_filter_data(
    start_year: int,
    end_year: int,
    product_filter: Optional[str]
) -> Tuple[List[Dict], Dict]:
    """
    Load all JSON files from data directory and filter by criteria.
    
    Returns:
        Tuple of (filtered_records, stats_dict)
    """
    records = []
    
    if not DATA_DIR.exists():
        print(f"Error: Data directory not found: {DATA_DIR}")
        sys.exit(1)
    
    # Load all JSON files
    json_files = DATA_DIR.glob('*.json')
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Extract year from filename and date_created
                filename_year = extract_year_from_filename(json_file.name)
                date_year = extract_year_from_date(data.get('date_created', ''))
                
                # Use filename year if available, fall back to date_created
                record_year = filename_year or date_year
                
                # Filter by year range
                if record_year is None or not (start_year <= record_year <= end_year):
                    continue
                
                # Filter by product if specified
                if product_filter:
                    suspect_products = get_suspect_products(data.get('products', []))
                    if not any(product_filter.lower() in prod.lower() for prod in suspect_products):
                        continue
                
                # Add metadata for easier processing
                data['_year'] = record_year
                data['_suspect_products'] = get_suspect_products(data.get('products', []))
                records.append(data)
        
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not read {json_file.name}: {e}", file=sys.stderr)
            continue
    
    return records


def calculate_statistics(records: List[Dict]) -> Dict:
    """Calculate all required statistics"""
    stats = {
        'total_records': len(records),
        'outcomes': Counter(),
        'reactions': Counter(),
        'suspect_products': Counter(),
        'ages': [],
        'ages_female': [],
        'ages_male': []
    }
    
    # Consolidate items to deduplicate
    all_outcomes = []
    all_reactions = []
    all_products = []
    
    # First pass: collect all items
    for record in records:
        outcomes = record.get('outcomes', [])
        reactions = record.get('reactions', [])
        products = record.get('_suspect_products', [])
        
        all_outcomes.extend(outcomes)
        all_reactions.extend(reactions)
        all_products.extend(products)
        
        # Process ages
        consumer = record.get('consumer', {})
        age_str = consumer.get('age', '')
        gender = consumer.get('gender', '').upper()
        
        try:
            age = float(age_str)
            if 0 < age < 150:  # Sanity check
                stats['ages'].append(age)
                if gender == 'FEMALE':
                    stats['ages_female'].append(age)
                elif gender == 'MALE':
                    stats['ages_male'].append(age)
        except (ValueError, TypeError):
            pass
    
    # Consolidate and count
    outcome_mapping = consolidate_items(all_outcomes)
    reaction_mapping = consolidate_items(all_reactions)
    product_mapping = consolidate_items(all_products)
    
    for outcome in all_outcomes:
        canonical = outcome_mapping.get(normalize_text(outcome), outcome)
        stats['outcomes'][canonical] += 1
    
    for reaction in all_reactions:
        canonical = reaction_mapping.get(normalize_text(reaction), reaction)
        stats['reactions'][canonical] += 1
    
    for product in all_products:
        canonical = product_mapping.get(normalize_text(product), product)
        stats['suspect_products'][canonical] += 1
    
    return stats


def print_statistics(stats: Dict, start_year: int, end_year: int, product_filter: Optional[str]):
    """Print statistics to stdout"""
    print("=" * 80)
    print("FDA ADVERSE EVENT DATA ANALYSIS REPORT")
    print("=" * 80)
    
    if product_filter:
        print(f"Filter: Years {start_year}-{end_year}, Product contains: '{product_filter}'")
    else:
        print(f"Filter: Years {start_year}-{end_year}, Product: None")
    
    print()
    print(f"Total Records Matching Criteria: {stats['total_records']}")
    print()
    
    # Top 25 Outcomes
    print("TOP 25 OUTCOMES:")
    print("-" * 80)
    for i, (outcome, count) in enumerate(stats['outcomes'].most_common(25), 1):
        print(f"{i:2d}. {outcome:50s} {count:6d}")
    print()
    
    # Top 25 Reactions
    print("TOP 25 REACTIONS:")
    print("-" * 80)
    for i, (reaction, count) in enumerate(stats['reactions'].most_common(25), 1):
        print(f"{i:2d}. {reaction:50s} {count:6d}")
    print()
    
    # Top 25 Suspect Products
    print("TOP 25 SUSPECT PRODUCTS:")
    print("-" * 80)
    for i, (product, count) in enumerate(stats['suspect_products'].most_common(25), 1):
        print(f"{i:2d}. {product:50s} {count:6d}")
    print()
    
    # Average Ages
    print("AVERAGE CONSUMER AGE:")
    print("-" * 80)
    total_avg = np.mean(stats['ages']) if stats['ages'] else 0
    female_avg = np.mean(stats['ages_female']) if stats['ages_female'] else 0
    male_avg = np.mean(stats['ages_male']) if stats['ages_male'] else 0
    
    print(f"Total Average Age:  {total_avg:7.2f} (n={len(stats['ages'])})")
    print(f"Female Average Age: {female_avg:7.2f} (n={len(stats['ages_female'])})")
    print(f"Male Average Age:   {male_avg:7.2f} (n={len(stats['ages_male'])})")
    print()
    print("=" * 80)


def create_visualizations(records: List[Dict]):
    """Create and save visualizations"""
    # Aggregate data by year
    cases_by_year = defaultdict(int)
    all_ages = []
    
    for record in records:
        year = record.get('_year')
        if year:
            cases_by_year[year] += 1
        
        consumer = record.get('consumer', {})
        age_str = consumer.get('age', '')
        try:
            age = float(age_str)
            if 0 < age < 150:
                all_ages.append(age)
        except (ValueError, TypeError):
            pass
    
    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))
    
    # Bar chart: Cases by Year
    if cases_by_year:
        years = sorted(cases_by_year.keys())
        counts = [cases_by_year[y] for y in years]
        ax1.bar(years, counts, color='steelblue', edgecolor='black', alpha=0.7)
        ax1.set_xlabel('Year', fontsize=12, fontweight='bold')
        ax1.set_ylabel('Number of Cases', fontsize=12, fontweight='bold')
        ax1.set_title('Total Cases by Year', fontsize=14, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)
        ax1.set_xticks(years[::max(1, len(years)//10)])  # Limit x-axis labels
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    
    # Histogram: Age Distribution
    if all_ages:
        ax2.hist(all_ages, bins=50, color='coral', edgecolor='black', alpha=0.7)
        ax2.set_xlabel('Age (years)', fontsize=12, fontweight='bold')
        ax2.set_ylabel('Frequency', fontsize=12, fontweight='bold')
        ax2.set_title('Distribution of Consumer Ages', fontsize=14, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    
    # Save with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = CHARTS_DIR / f"{timestamp}.png"
    plt.savefig(output_file, dpi=100, bbox_inches='tight')
    plt.close()
    
    print(f"Visualization saved to: {output_file}")


def main():
    """Main entry point"""
    # Parse arguments
    start_year, end_year, product_filter = parse_arguments()
    
    print(f"Loading data from {DATA_DIR}...", file=sys.stderr)
    
    # Load and filter data
    records = load_and_filter_data(start_year, end_year, product_filter)
    
    if not records:
        print("No records found matching the criteria.", file=sys.stderr)
        sys.exit(0)
    
    # Calculate statistics
    stats = calculate_statistics(records)
    
    # Print statistics
    print_statistics(stats, start_year, end_year, product_filter)
    
    # Create visualizations
    print(f"Creating visualizations...", file=sys.stderr)
    create_visualizations(records)


if __name__ == '__main__':
    main()
