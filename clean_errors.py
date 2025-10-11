#!/usr/bin/env python3
"""
Script to clean 403 ERROR records from jobs.csv and check for duplicates
Removes rows where title contains '403 ERROR' and identifies duplicate records
"""

import csv
import os
from datetime import datetime
from collections import defaultdict

def check_duplicates(input_file='jobs.csv'):
    """
    Check for duplicate records in CSV file based on ID and URL fields
    
    Args:
        input_file (str): Path to CSV file to check
        
    Returns:
        dict: Dictionary with duplicate analysis results
    """
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found")
        return None
    
    # Track duplicates by different criteria
    id_counts = defaultdict(int)
    url_counts = defaultdict(int)
    title_company_counts = defaultdict(int)  # Combination of title + company
    
    total_records = 0
    duplicate_ids = []
    duplicate_urls = []
    duplicate_title_company = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for row_num, row in enumerate(reader, start=2):  # Start from 2 (after header)
            total_records += 1
            
            # Check ID duplicates
            record_id = row.get('id', '').strip()
            if record_id:
                id_counts[record_id] += 1
                if id_counts[record_id] == 2:  # First duplicate found
                    duplicate_ids.append(record_id)
            
            # Check URL duplicates
            url = row.get('url', '').strip()
            if url:
                url_counts[url] += 1
                if url_counts[url] == 2:  # First duplicate found
                    duplicate_urls.append(url)
            
            # Check title + company combination duplicates
            title = row.get('title', '').strip()
            company = row.get('company', '').strip()
            if title and company:
                title_company_key = f"{title}|{company}"
                title_company_counts[title_company_key] += 1
                if title_company_counts[title_company_key] == 2:  # First duplicate found
                    duplicate_title_company.append(title_company_key)
    
    # Count total duplicates
    total_id_duplicates = sum(count - 1 for count in id_counts.values() if count > 1)
    total_url_duplicates = sum(count - 1 for count in url_counts.values() if count > 1)
    total_title_company_duplicates = sum(count - 1 for count in title_company_counts.values() if count > 1)
    
    return {
        'total_records': total_records,
        'duplicate_ids': duplicate_ids,
        'duplicate_urls': duplicate_urls,
        'duplicate_title_company': duplicate_title_company,
        'total_id_duplicates': total_id_duplicates,
        'total_url_duplicates': total_url_duplicates,
        'total_title_company_duplicates': total_title_company_duplicates,
        'id_counts': dict(id_counts),
        'url_counts': dict(url_counts),
        'title_company_counts': dict(title_company_counts)
    }

def clean_403_errors(input_file='jobs.csv', backup=True, check_duplicates_flag=True):
    """
    Remove 403 ERROR records from CSV file and optionally check for duplicates
    
    Args:
        input_file (str): Path to CSV file to clean
        backup (bool): Whether to create backup before cleaning
        check_duplicates_flag (bool): Whether to check for duplicates
    """
    
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found")
        return
    
    # First, check for duplicates if requested
    if check_duplicates_flag:
        print("=== DUPLICATE ANALYSIS ===")
        duplicate_analysis = check_duplicates(input_file)
        
        if duplicate_analysis:
            print(f"File: {input_file}")
            print(f"Total records analyzed: {duplicate_analysis['total_records']}")
            print()
            
            # ID duplicates
            if duplicate_analysis['total_id_duplicates'] > 0:
                print(f"WARNING: DUPLICATE IDs FOUND: {duplicate_analysis['total_id_duplicates']} duplicate records")
                print(f"   Duplicate IDs: {', '.join(duplicate_analysis['duplicate_ids'][:5])}")
                if len(duplicate_analysis['duplicate_ids']) > 5:
                    print(f"   ... and {len(duplicate_analysis['duplicate_ids']) - 5} more")
            else:
                print("OK: No duplicate IDs found")
            
            # URL duplicates
            if duplicate_analysis['total_url_duplicates'] > 0:
                print(f"WARNING: DUPLICATE URLs FOUND: {duplicate_analysis['total_url_duplicates']} duplicate records")
                print(f"   Duplicate URLs: {', '.join(duplicate_analysis['duplicate_urls'][:3])}")
                if len(duplicate_analysis['duplicate_urls']) > 3:
                    print(f"   ... and {len(duplicate_analysis['duplicate_urls']) - 3} more")
            else:
                print("OK: No duplicate URLs found")
            
            # Title + Company duplicates
            if duplicate_analysis['total_title_company_duplicates'] > 0:
                print(f"WARNING: DUPLICATE TITLE+COMPANY FOUND: {duplicate_analysis['total_title_company_duplicates']} duplicate records")
                print(f"   Duplicate combinations: {', '.join(duplicate_analysis['duplicate_title_company'][:3])}")
                if len(duplicate_analysis['duplicate_title_company']) > 3:
                    print(f"   ... and {len(duplicate_analysis['duplicate_title_company']) - 3} more")
            else:
                print("OK: No duplicate title+company combinations found")
            
            print()
    
    # Count initial records for 403 errors
    total_records = 0
    error_records = 0
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_records += 1
            if row.get('title', '').strip() == '403 ERROR':
                error_records += 1
    
    print(f"=== 403 ERROR CLEANUP ANALYSIS ===")
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
    print("=== CSV ERROR CLEANER & DUPLICATE CHECKER ===")
    print("Checking for duplicates and removing 403 ERROR records from jobs.csv")
    print()
    
    clean_403_errors()

if __name__ == "__main__":
    main()
