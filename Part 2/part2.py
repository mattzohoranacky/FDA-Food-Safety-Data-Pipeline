#!/usr/bin/env python3
"""
Part 2: FDA Adverse Event Data Analysis & Reporting
Analyzes FDA food safety adverse event JSON files with filtering and visualization.
"""

import json
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from typing import Tuple, Optional

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


def parse_arguments() -> Tuple[int, int, Optional[str]]:
    """Parse command-line arguments"""
    start_year = 2000
    end_year = CURRENT_YEAR
    product_filter = None
    
    numeric_args = []
    string_args = []
    
    for arg in sys.argv[1:]:
        if arg.isdigit():
            numeric_args.append(int(arg))
        else:
            string_args.append(arg.upper())
    
    if len(numeric_args) == 1:
        start_year = numeric_args[0]
        end_year = CURRENT_YEAR
    elif len(numeric_args) >= 2:
        start_year = min(numeric_args[0], numeric_args[1])
        end_year = max(numeric_args[0], numeric_args[1])
    
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
    except (ValueError, TypeError):
        pass
    return None




def load_data(start_year: int, end_year: int, product_filter: Optional[str]) -> pd.DataFrame:
    """
    Load JSON files efficiently and return DataFrame.
    Optimized for speed - minimal processing during load.
    """
    record_list = []
    
    if not DATA_DIR.exists():
        print(f"Error: Data directory not found: {DATA_DIR}")
        sys.exit(1)
    
    json_files = list(DATA_DIR.glob('*.json'))
    total_files = len(json_files)
    
    for idx, json_file in enumerate(json_files):
        if (idx + 1) % 100 == 0:
            print(f"  Processing file {idx + 1}/{total_files}...", file=sys.stderr)
        
        # Extract year from filename first (fast)
        filename_year = extract_year_from_filename(json_file.name)
        if filename_year is not None and not (start_year <= filename_year <= end_year):
            continue
        
        try:
            with open(json_file, 'r', encoding='utf-8', errors='ignore') as f:
                data = json.load(f)
        except (json.JSONDecodeError, IOError):
            continue
        
        # Determine record year
        if filename_year is None:
            date_year = extract_year_from_date(data.get('date_created', ''))
            if date_year is None or not (start_year <= date_year <= end_year):
                continue
            record_year = date_year
        else:
            record_year = filename_year
        
        # Extract consumer info
        consumer = data.get('consumer', {})
        age = None
        try:
            age_str = consumer.get('age', '')
            if age_str:
                age = float(age_str)
                if not (0 < age < 150):
                    age = None
        except (ValueError, TypeError):
            pass
        
        gender = consumer.get('gender', '').upper() if consumer.get('gender') else ''
        
        # Filter by product early
        products = []
        for product in data.get('products', []):
            if product.get('role') == 'SUSPECT':
                name = product.get('name_brand', '').strip()
                if name and name != 'EXEMPTION 4':
                    products.append(name.upper())
        
        if product_filter and not any(product_filter in p for p in products):
            continue
        
        # Add records
        for outcome in data.get('outcomes', []):
            if outcome:
                record_list.append([outcome.strip().upper(), 'outcome', record_year, age, gender])
        
        for reaction in data.get('reactions', []):
            if reaction:
                record_list.append([reaction.strip().upper(), 'reaction', record_year, age, gender])
        
        for product in products:
            record_list.append([product, 'product', record_year, age, gender])
    
    if record_list:
        return pd.DataFrame(record_list, columns=['value', 'type', 'year', 'age', 'gender'])
    else:
        return pd.DataFrame(columns=['value', 'type', 'year', 'age', 'gender'])




def calculate_statistics(df: pd.DataFrame) -> dict:
    """Calculate all required statistics using pandas operations - fast and simple"""
    if len(df) == 0:
        return {
            'total_records': 0,
            'outcomes': {},
            'reactions': {},
            'products': {},
            'age_total': 0,
            'age_female': 0,
            'age_male': 0,
            'age_total_n': 0,
            'age_female_n': 0,
            'age_male_n': 0
        }
    
    stats = {}
    stats['total_records'] = len(df)
    
    # Top outcomes - use pandas directly (no consolidation for speed)
    outcome_df = df[df['type'] == 'outcome']
    stats['outcomes'] = outcome_df['value'].value_counts().head(25).to_dict() if len(outcome_df) > 0 else {}
    
    # Top reactions
    reaction_df = df[df['type'] == 'reaction']
    stats['reactions'] = reaction_df['value'].value_counts().head(25).to_dict() if len(reaction_df) > 0 else {}
    
    # Top products
    product_df = df[df['type'] == 'product']
    stats['products'] = product_df['value'].value_counts().head(25).to_dict() if len(product_df) > 0 else {}
    
    # Age statistics
    all_ages = df['age'].dropna()
    stats['age_total'] = all_ages.mean() if len(all_ages) > 0 else 0
    stats['age_total_n'] = len(all_ages)
    
    female_ages = df[df['gender'] == 'FEMALE']['age'].dropna()
    stats['age_female'] = female_ages.mean() if len(female_ages) > 0 else 0
    stats['age_female_n'] = len(female_ages)
    
    male_ages = df[df['gender'] == 'MALE']['age'].dropna()
    stats['age_male'] = male_ages.mean() if len(male_ages) > 0 else 0
    stats['age_male_n'] = len(male_ages)
    
    return stats


def print_statistics(stats: dict, start_year: int, end_year: int, product_filter: Optional[str]):
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
    for i, (outcome, count) in enumerate(list(stats['outcomes'].items())[:25], 1):
        print(f"{i:2d}. {outcome:50s} {int(count):6d}")
    print()
    
    # Top 25 Reactions
    print("TOP 25 REACTIONS:")
    print("-" * 80)
    for i, (reaction, count) in enumerate(list(stats['reactions'].items())[:25], 1):
        print(f"{i:2d}. {reaction:50s} {int(count):6d}")
    print()
    
    # Top 25 Products
    print("TOP 25 SUSPECT PRODUCTS:")
    print("-" * 80)
    for i, (product, count) in enumerate(list(stats['products'].items())[:25], 1):
        print(f"{i:2d}. {product:50s} {int(count):6d}")
    print()
    
    # Average Ages
    print("AVERAGE CONSUMER AGE:")
    print("-" * 80)
    print(f"Total Average Age:  {stats['age_total']:7.2f} (n={stats['age_total_n']})")
    print(f"Female Average Age: {stats['age_female']:7.2f} (n={stats['age_female_n']})")
    print(f"Male Average Age:   {stats['age_male']:7.2f} (n={stats['age_male_n']})")
    print()
    print("=" * 80)


def create_visualizations(df: pd.DataFrame):
    """Create and save visualizations using pandas/matplotlib"""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    # Bar chart: Cases by Year
    if 'year' in df.columns and len(df) > 0:
        year_counts = df['year'].value_counts().sort_index()
        year_counts.plot(kind='bar', ax=ax1, color='steelblue', edgecolor='black', alpha=0.7)
        ax1.set_xlabel('Year', fontsize=11, fontweight='bold')
        ax1.set_ylabel('Number of Cases', fontsize=11, fontweight='bold')
        ax1.set_title('Total Cases by Year', fontsize=12, fontweight='bold')
        ax1.grid(axis='y', alpha=0.3)
        plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)
    
    # Histogram: Age Distribution
    ages = df['age'].dropna()
    if len(ages) > 0:
        ax2.hist(ages, bins=40, color='coral', edgecolor='black', alpha=0.7)
        ax2.set_xlabel('Age (years)', fontsize=11, fontweight='bold')
        ax2.set_ylabel('Frequency', fontsize=11, fontweight='bold')
        ax2.set_title('Distribution of Consumer Ages', fontsize=12, fontweight='bold')
        ax2.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    
    # Save with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = CHARTS_DIR / f"{timestamp}.png"
    plt.savefig(output_file, dpi=90, bbox_inches='tight')
    plt.close()
    
    print(f"Visualization saved to: {output_file}")


def main():
    """Main entry point"""
    start_year, end_year, product_filter = parse_arguments()
    
    print(f"Loading data from {DATA_DIR}...", file=sys.stderr)
    
    # Load data as DataFrame
    df = load_data(start_year, end_year, product_filter)
    
    if len(df) == 0:
        print("No records found matching the criteria.", file=sys.stderr)
        sys.exit(0)
    
    # Calculate statistics
    stats = calculate_statistics(df)
    
    # Print statistics
    print_statistics(stats, start_year, end_year, product_filter)
    
    # Create visualizations
    print(f"Creating visualizations...", file=sys.stderr)
    create_visualizations(df)


if __name__ == '__main__':
    main()

