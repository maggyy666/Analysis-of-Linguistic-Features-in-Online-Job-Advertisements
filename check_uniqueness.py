"""
Script to check uniqueness of records in jobs.csv
Analyzes duplicates by different criteria and generates detailed report
"""

import csv
import os
from collections import defaultdict
from datetime import datetime


def analyze_uniqueness(input_file='jobs.csv'):
    """
    Analyzes uniqueness of records in CSV file by different criteria
    
    Args:
        input_file (str): Path to CSV file to analyze
        
    Returns:
        dict: Comprehensive analysis results
    """
    
    if not os.path.exists(input_file):
        print(f"Error: File {input_file} not found")
        return None
    
    # Initialize tracking dictionaries
    id_counts = defaultdict(list)  # Store row numbers for each ID
    url_counts = defaultdict(list)
    title_company_counts = defaultdict(list)
    full_content_counts = defaultdict(list)  # Complete record comparison
    
    total_records = 0
    valid_records = 0  # Excluding 403 ERROR records
    
    print(f"Analyzing file: {input_file}")
    print("=" * 60)
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        for row_num, row in enumerate(reader, start=2):  # Start from 2 (after header)
            total_records += 1
            
            # Skip 403 ERROR records
            if row.get('title', '').strip() == '403 ERROR':
                continue
                
            valid_records += 1
            
            # Extract key fields
            record_id = row.get('id', '').strip()
            url = row.get('url', '').strip()
            title = row.get('title', '').strip()
            company = row.get('company', '').strip()
            
            # Create composite keys
            title_company_key = f"{title}|{company}"
            full_content_key = '|'.join([str(row.get(field, '')).strip() for field in headers])
            
            # Track by ID
            if record_id:
                id_counts[record_id].append(row_num)
            
            # Track by URL
            if url:
                url_counts[url].append(row_num)
            
            # Track by title + company
            if title and company:
                title_company_counts[title_company_key].append(row_num)
            
            # Track by full content
            full_content_counts[full_content_key].append(row_num)
    
    # Analyze duplicates
    duplicate_ids = {k: v for k, v in id_counts.items() if len(v) > 1}
    duplicate_urls = {k: v for k, v in url_counts.items() if len(v) > 1}
    duplicate_title_company = {k: v for k, v in title_company_counts.items() if len(v) > 1}
    duplicate_full_content = {k: v for k, v in full_content_counts.items() if len(v) > 1}
    
    # Calculate totals
    total_id_duplicates = sum(len(positions) - 1 for positions in duplicate_ids.values())
    total_url_duplicates = sum(len(positions) - 1 for positions in duplicate_urls.values())
    total_title_company_duplicates = sum(len(positions) - 1 for positions in duplicate_title_company.values())
    total_full_content_duplicates = sum(len(positions) - 1 for positions in duplicate_full_content.values())
    
    return {
        'total_records': total_records,
        'valid_records': valid_records,
        'error_records': total_records - valid_records,
        'duplicate_ids': duplicate_ids,
        'duplicate_urls': duplicate_urls,
        'duplicate_title_company': duplicate_title_company,
        'duplicate_full_content': duplicate_full_content,
        'total_id_duplicates': total_id_duplicates,
        'total_url_duplicates': total_url_duplicates,
        'total_title_company_duplicates': total_title_company_duplicates,
        'total_full_content_duplicates': total_full_content_duplicates,
        'headers': headers
    }


def print_summary_report(analysis):
    """Prints a comprehensive summary report"""
    
    print(f"\n=== UNIQUENESS ANALYSIS REPORT ===")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"File analyzed: jobs.csv")
    print("=" * 60)
    
    # Basic statistics
    print(f"\nBASIC STATISTICS:")
    print(f"Total records in file: {analysis['total_records']}")
    print(f"Valid records (excluding 403 ERROR): {analysis['valid_records']}")
    print(f"Error records (403 ERROR): {analysis['error_records']}")
    
    # Duplicate analysis
    print(f"\nDUPLICATE ANALYSIS:")
    
    # ID duplicates
    if analysis['total_id_duplicates'] > 0:
        print(f"WARNING: ID DUPLICATES: {analysis['total_id_duplicates']} duplicate records")
        print(f"   Found {len(analysis['duplicate_ids'])} unique IDs with duplicates")
    else:
        print(f"OK: ID UNIQUENESS: No duplicate IDs found")
    
    # URL duplicates
    if analysis['total_url_duplicates'] > 0:
        print(f"WARNING: URL DUPLICATES: {analysis['total_url_duplicates']} duplicate records")
        print(f"   Found {len(analysis['duplicate_urls'])} unique URLs with duplicates")
    else:
        print(f"OK: URL UNIQUENESS: No duplicate URLs found")
    
    # Title + Company duplicates
    if analysis['total_title_company_duplicates'] > 0:
        print(f"WARNING: TITLE+COMPANY DUPLICATES: {analysis['total_title_company_duplicates']} duplicate records")
        print(f"   Found {len(analysis['duplicate_title_company'])} unique title+company combinations with duplicates")
    else:
        print(f"OK: TITLE+COMPANY UNIQUENESS: No duplicate title+company combinations found")
    
    # Full content duplicates
    if analysis['total_full_content_duplicates'] > 0:
        print(f"WARNING: FULL CONTENT DUPLICATES: {analysis['total_full_content_duplicates']} duplicate records")
        print(f"   Found {len(analysis['duplicate_full_content'])} completely identical records")
    else:
        print(f"OK: FULL CONTENT UNIQUENESS: No completely identical records found")
    
    # Overall assessment
    total_duplicates = analysis['total_id_duplicates'] + analysis['total_url_duplicates'] + analysis['total_title_company_duplicates']
    
    if total_duplicates == 0:
        print(f"\nSUCCESS: DATASET IS UNIQUE: No duplicates found!")
    else:
        print(f"\nWARNING: DATASET HAS DUPLICATES: {total_duplicates} duplicate records found")
    
    print("=" * 60)


def print_detailed_duplicates(analysis, limit=5):
    """Prints detailed information about duplicates"""
    
    print(f"\n=== DETAILED DUPLICATE INFORMATION ===")
    
    # ID duplicates details
    if analysis['duplicate_ids']:
        print(f"\nID DUPLICATES (showing first {limit}):")
        for i, (duplicate_id, positions) in enumerate(list(analysis['duplicate_ids'].items())[:limit]):
            print(f"  {i+1}. ID: {duplicate_id}")
            print(f"     Found at rows: {', '.join(map(str, positions))}")
            print(f"     Count: {len(positions)} occurrences")
    
    # URL duplicates details
    if analysis['duplicate_urls']:
        print(f"\nURL DUPLICATES (showing first {limit}):")
        for i, (duplicate_url, positions) in enumerate(list(analysis['duplicate_urls'].items())[:limit]):
            print(f"  {i+1}. URL: {duplicate_url[:80]}{'...' if len(duplicate_url) > 80 else ''}")
            print(f"     Found at rows: {', '.join(map(str, positions))}")
            print(f"     Count: {len(positions)} occurrences")
    
    # Title + Company duplicates details
    if analysis['duplicate_title_company']:
        print(f"\nTITLE+COMPANY DUPLICATES (showing first {limit}):")
        for i, (duplicate_key, positions) in enumerate(list(analysis['duplicate_title_company'].items())[:limit]):
            title, company = duplicate_key.split('|', 1)
            print(f"  {i+1}. Title: {title[:50]}{'...' if len(title) > 50 else ''}")
            print(f"     Company: {company[:30]}{'...' if len(company) > 30 else ''}")
            print(f"     Found at rows: {', '.join(map(str, positions))}")
            print(f"     Count: {len(positions)} occurrences")


def export_duplicates_to_csv(analysis, output_file='duplicates_report.csv'):
    """Exports duplicate information to CSV file"""
    
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Type', 'Value', 'Row_Positions', 'Count'])
        
        # Export ID duplicates
        for duplicate_id, positions in analysis['duplicate_ids'].items():
            writer.writerow(['ID', duplicate_id, ','.join(map(str, positions)), len(positions)])
        
        # Export URL duplicates
        for duplicate_url, positions in analysis['duplicate_urls'].items():
            writer.writerow(['URL', duplicate_url, ','.join(map(str, positions)), len(positions)])
        
        # Export Title+Company duplicates
        for duplicate_key, positions in analysis['duplicate_title_company'].items():
            writer.writerow(['Title+Company', duplicate_key, ','.join(map(str, positions)), len(positions)])
        
        # Export Full Content duplicates
        for duplicate_key, positions in analysis['duplicate_full_content'].items():
            writer.writerow(['Full_Content', duplicate_key[:100] + '...', ','.join(map(str, positions)), len(positions)])
    
    print(f"\nDuplicate report exported to: {output_file}")


def main():
    """Main function to run uniqueness analysis"""
    
    print("=== CSV UNIQUENESS CHECKER ===")
    print("Checking for duplicate records in jobs.csv")
    print()
    
    # Run analysis
    analysis = analyze_uniqueness('jobs.csv')
    
    if not analysis:
        print("Analysis failed - file not found")
        return
    
    # Print reports
    print_summary_report(analysis)
    print_detailed_duplicates(analysis, limit=10)
    
    # Export duplicates to CSV if any found
    total_duplicates = analysis['total_id_duplicates'] + analysis['total_url_duplicates'] + analysis['total_title_company_duplicates']
    
    if total_duplicates > 0:
        export_duplicates_to_csv(analysis)
        
        print(f"\n=== RECOMMENDATIONS ===")
        print(f"1. Review the duplicates_report.csv file for detailed information")
        print(f"2. Consider removing duplicate records to clean your dataset")
        print(f"3. Use the clean_errors.py script to remove 403 ERROR records")
        print(f"4. Re-run this analysis after cleaning to verify uniqueness")
    else:
        print(f"\n=== DATASET STATUS ===")
        print(f"SUCCESS: Your dataset is clean and unique!")
        print(f"SUCCESS: No duplicate records found")
        print(f"SUCCESS: Ready for analysis")


if __name__ == "__main__":
    main()
