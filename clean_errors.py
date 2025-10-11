#!/usr/bin/env python3
"""
Script to clean 403 ERROR records from jobs.csv
Removes rows where title contains '403 ERROR'
"""

import csv
import os
from datetime import datetime

def clean_403_errors(input_file='jobs.csv', backup=True):
    """
    Remove 403 ERROR records from CSV file
    
    Args:
        input_file (str): Path to CSV file to clean
        backup (bool): Whether to create backup before cleaning
    """
    
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found")
        return
    
    # Count initial records
    total_records = 0
    error_records = 0
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_records += 1
            if row.get('title', '').strip() == '403 ERROR':
                error_records += 1
    
    print(f"CSV CLEANUP ANALYSIS")
    print(f"File: {input_file}")
    print(f"Total records: {total_records}")
    print(f"403 ERROR records: {error_records}")
    print(f"Valid records: {total_records - error_records}")
    
    if error_records == 0:
        print("No 403 ERROR records found - file is clean")
        return
    
    # Create backup if requested
    if backup:
        backup_file = f"{input_file}.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        import shutil
        shutil.copy2(input_file, backup_file)
        print(f"Backup created: {backup_file}")
    
    # Read all records and filter out 403 errors
    clean_records = []
    headers = None
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        for row in reader:
            if row.get('title', '').strip() != '403 ERROR':
                clean_records.append(row)
    
    # Write cleaned data back to file
    with open(input_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(clean_records)
    
    print(f"\nCLEANUP COMPLETED")
    print(f"Removed: {error_records} records with 403 ERROR")
    print(f"Remaining: {len(clean_records)} valid records")
    print(f"File updated: {input_file}")

def main():
    """Main function"""
    print("=== CSV ERROR CLEANER ===")
    print("Removing 403 ERROR records from jobs.csv")
    print()
    
    clean_403_errors()

if __name__ == "__main__":
    main()
