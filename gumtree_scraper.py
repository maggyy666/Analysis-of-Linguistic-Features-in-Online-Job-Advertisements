"""
Gumtree (UK) Jobs Scraper
SSR-based scraper with single browser instance and low concurrency
"""

import asyncio
import csv
import time
import re
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse, urlunparse, parse_qs, urlencode
from playwright.async_api import async_playwright
from tabulate import tabulate

# Gumtree constants
GUMTREE_BASE = "https://www.gumtree.com"


def get_target_count_from_jobs_csv():
    """Counts jobs in jobs.csv to set target count for Gumtree scraper"""
    try:
        with open('jobs.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                if row.get('title') and row.get('title') != '403 ERROR':
                    count += 1
        return count
    except FileNotFoundError:
        return 1321  # Default fallback


def job_id_from_url(url: str) -> str:
    """Extracts job ID from Gumtree URL (e.g., '5417380692' from '.../5417380692')"""
    return url.rstrip("/").split("/")[-1]


def load_existing_gumtree_jobs():
    """Loads existing Gumtree jobs from gumtree_jobs.csv for deduplication by ID (not title - titles repeat on Gumtree)"""
    existing_ids = set()
    try:
        with open('gumtree_jobs.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = (row.get('url') or "").strip()
                if url:
                    job_id = job_id_from_url(url)
                    existing_ids.add(job_id)
        print(f"Loaded {len(existing_ids)} existing Gumtree jobs for deduplication (by ID)")
    except FileNotFoundError:
        print("No existing gumtree_jobs.csv file - starting fresh")
    return existing_ids


def count_existing_gumtree_jobs():
    """Counts how many Gumtree jobs we already have saved"""
    try:
        with open('gumtree_jobs.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                # Count valid jobs (not 403 errors)
                if row.get('title') and row.get('title') != '403 ERROR':
                    count += 1
        return count
    except FileNotFoundError:
        return 0


def normalize_base_url(url: str) -> str:
    """Normalizes base URL by removing page numbers"""
    url = url.rstrip("/")
    url = re.sub(r"/page\d+$", "", url)
    return url


def build_listing_urls(base_url: str, page_num: int) -> list:
    """Builds listing URLs for pagination (/page2, /page3, or ?page=2 fallback)"""
    base = normalize_base_url(base_url)
    urls = []
    if page_num == 1:
        urls.append(base)
    else:
        urls.append(f"{base}/page{page_num}")

    # Fallback query: ?page=2
    parsed = urlparse(base)
    q = parse_qs(parsed.query)
    q["page"] = [str(page_num)]
    urls.append(urlunparse(parsed._replace(query=urlencode(q, doseq=True))))
    return urls


async def collect_links_from_listing(page):
    """Collects job links and titles from already loaded listing page (like simple_scraper.py)"""
    links_with_titles = []  # List of dicts: {'url': ..., 'title': ...}

    # Primary: data-q anchors
    for a in await page.query_selector_all('a[data-q="search-result-anchor"]'):
        href = await a.get_attribute("href")
        if href:
            full_url = urljoin(GUMTREE_BASE, href.split("?")[0])
            # Try to get title from listing (like simple_scraper.py does)
            title = ""
            try:
                # Try to find title element near the link
                title_elem = await a.query_selector('h3, h4, .title, [class*="title"]')
                if title_elem:
                    title = (await title_elem.inner_text()).strip()
                # Fallback: try parent or sibling
                if not title:
                    parent = await a.query_selector('xpath=..')
                    if parent:
                        title_elem = await parent.query_selector('h3, h4, .title, [class*="title"]')
                        if title_elem:
                            title = (await title_elem.inner_text()).strip()
            except:
                pass
            
            links_with_titles.append({'url': full_url, 'title': title})

    # Fallback: /p/ style links
    if not links_with_titles:
        for a in await page.query_selector_all('a[href^="/p/"]'):
            href = await a.get_attribute("href")
            if href:
                full_url = urljoin(GUMTREE_BASE, href.split("?")[0])
                title = ""
                try:
                    title_elem = await a.query_selector('h3, h4, .title, [class*="title"]')
                    if title_elem:
                        title = (await title_elem.inner_text()).strip()
                except:
                    pass
                links_with_titles.append({'url': full_url, 'title': title})

    return links_with_titles


async def read_dt_dd(page, label: str) -> str:
    """Reads value from dt/dd pair (Gumtree job details format)"""
    # Try multiple XPath patterns for dt/dd pairs
    patterns = [
        f"xpath=//dt[normalize-space()='{label}']/following-sibling::dd[1]",
        f"xpath=//dt[contains(normalize-space(), '{label}')]/following-sibling::dd[1]",
        f"xpath=//*[normalize-space()='{label}']/following-sibling::*[1]",
    ]
    for pattern in patterns:
        loc = page.locator(pattern)
        if await loc.count():
            text = " ".join((await loc.first.inner_text()).split()).strip()
            if text:
                return text
    return ""


async def extract_job_data_gumtree(page, url):
    """Parses data from already opened Gumtree job page (no goto here)"""
    job = {
        "id": url.rstrip("/").split("/")[-1],
        "url": url,
        "title": "",
        "company": "",
        "salary": "",
        "location": "",
        "work_time": "",
        "contract_type": "",
        "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "description": "",
    }

    try:
        # Title
        h1 = await page.query_selector("h1")
        if h1:
            job["title"] = (await h1.inner_text()).strip()

        # Location: najczęściej jest bezpośrednio po h1 jako h4 (np. "Northwich, Cheshire")
        # Try multiple location selectors
        location_selectors = [
            "h1 + h4",
            "h1 + h3",
            "h1 + p",
            '[data-q="vip-location"]',
            '.location',
            '[itemprop="addressLocality"]',
        ]
        for selector in location_selectors:
            loc = page.locator(selector)
            if await loc.count():
                location_text = " ".join((await loc.first.inner_text()).split()).strip()
                if location_text:
                    job["location"] = location_text
                    break
        
        # Fallback: try to extract from dt/dd
        if not job["location"]:
            job["location"] = await read_dt_dd(page, "Location") or await read_dt_dd(page, "Area")

        # Pola z sekcji details (Salary / Recruiter / Contract Type / Hours)
        # Try multiple labels for salary
        salary_raw = (
            await read_dt_dd(page, "Salary")
            or await read_dt_dd(page, "Pay")
            or await read_dt_dd(page, "Wage")
            or await read_dt_dd(page, "Rate")
        )
        
        # Clean salary - remove phone numbers and extra text, keep only salary-related info
        if salary_raw:
            # Remove phone numbers (UK format: 07xxx, 074xx, etc.)
            salary_raw = re.sub(r'\b0\d{9,10}\b', '', salary_raw)
            # Remove "Call or Text" patterns
            salary_raw = re.sub(r'Call or Text[^.]*', '', salary_raw, flags=re.IGNORECASE)
            # Remove email-like patterns
            salary_raw = re.sub(r'\S+@\S+', '', salary_raw)
            # Keep only if it contains currency symbol or numbers with salary keywords
            if re.search(r'[£$€]|\d+.*(?:per|/|hour|day|week|month|year|annum|shift)', salary_raw, re.IGNORECASE):
                job["salary"] = " ".join(salary_raw.split()).strip()
            else:
                job["salary"] = ""  # Not a valid salary, clear it
        
        job["contract_type"] = await read_dt_dd(page, "Contract Type") or await read_dt_dd(page, "Contract")
        job["work_time"] = await read_dt_dd(page, "Hours") or await read_dt_dd(page, "Working hours") or await read_dt_dd(page, "Work hours")
        job["company"] = (
            await read_dt_dd(page, "Recruiter")
            or await read_dt_dd(page, "Company")
            or await read_dt_dd(page, "Advertiser")
            or await read_dt_dd(page, "Employer")
        )

        # Description (stabilne: nagłówek "Description" i pierwszy blok po nim)
        desc = page.locator("xpath=//h3[normalize-space()='Description']/following-sibling::*[1]")
        if await desc.count():
            job["description"] = " ".join((await desc.first.inner_text()).split()).strip()
        else:
            # fallback jeśli układ inny
            desc_selectors = [
                '[data-q="vip-description"]',
                '[itemprop="description"]',
                '#vip__description',
                '.vip__description',
                '.description'
            ]
            for selector in desc_selectors:
                d = await page.query_selector(selector)
                if d:
                    job["description"] = " ".join((await d.inner_text()).split()).strip()
                    break

        # Jeśli Salary brak, spróbuj wyłuskać z opisu (opcjonalnie, ale praktyczne)
        if not job["salary"] and job["description"]:
            # Try multiple salary patterns - more comprehensive
            salary_patterns = [
                # £10 - £25 per hour (with currency symbols)
                (r"£\s?(\d+(?:,\d{3})*(?:\.\d+)?)\s*-\s*£\s?(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:per|/)\s*(hour|hr|day|week|month|annum|year|shift)", 
                 lambda m: f"£{m.group(1)} - £{m.group(2)} per {m.group(3)}"),
                # £10-25 per hour (without second currency)
                (r"£\s?(\d+(?:,\d{3})*(?:\.\d+)?)\s*-\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:per|/)\s*(hour|hr|day|week|month|annum|year|shift)",
                 lambda m: f"£{m.group(1)} - £{m.group(2)} per {m.group(3)}"),
                # £10 per hour
                (r"£\s?(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:per|/)\s*(hour|hr|day|week|month|annum|year|shift)",
                 lambda m: f"£{m.group(1)} per {m.group(2)}"),
                # EARN £50-£120 PER DAY
                (r"EARN\s+£\s?(\d+(?:,\d{3})*(?:\.\d+)?)\s*-\s*£\s?(\d+(?:,\d{3})*(?:\.\d+)?)\s*PER\s*(DAY|SHIFT)",
                 lambda m: f"£{m.group(1)} - £{m.group(2)} per {m.group(3).lower()}"),
                # £50-£120 per day (alternative format)
                (r"£\s?(\d+(?:,\d{3})*(?:\.\d+)?)\s*-\s*£\s?(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:per|/)\s*(day|shift)",
                 lambda m: f"£{m.group(1)} - £{m.group(2)} per {m.group(3)}"),
            ]
            
            for pattern, formatter in salary_patterns:
                m = re.search(pattern, job["description"], re.IGNORECASE)
                if m:
                    try:
                        job["salary"] = formatter(m)
                        job["salary"] = " ".join(job["salary"].split()).strip()
                        break
                    except:
                        continue

    except Exception as e:
        print(f"Error extracting data from {url}: {str(e)}")

    return job


async def scrape_single_job_gumtree(context, url, semaphore):
    """Scrapes single job with semaphore for concurrency control"""
    async with semaphore:
        try:
            p = await context.new_page()
            await p.goto(url, wait_until="domcontentloaded", timeout=30000)
            await p.wait_for_timeout(900)

            data = await extract_job_data_gumtree(p, url)
            await p.close()

            await asyncio.sleep(1.2)
            return data
        except Exception as e:
            print(f"Error scraping {url}: {e}")
            return None


def save_to_csv(jobs, mode='w', filename='gumtree_jobs.csv'):
    """Saves data to CSV file with exact column order matching jobs.csv"""
    # Exact order from jobs.csv: id,url,title,company,salary,location,work_time,contract_type,scraped_at,description
    headers = ['id', 'url', 'title', 'company', 'salary', 'location', 'work_time', 'contract_type', 'scraped_at', 'description']

    # Check if file exists and has header
    file_exists = False
    has_header = False
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            first_line = f.readline().strip()
            file_exists = True
            # Check if first line matches header
            if first_line == ','.join(headers):
                has_header = True
    except FileNotFoundError:
        pass

    with open(filename, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)

        # Write header if creating new file or if file exists but has no header
        if mode == 'w' or (file_exists and not has_header):
            writer.writeheader()

        for job in jobs:
            # Ensure all fields are present and in correct order
            ordered_job = {key: job.get(key, '') for key in headers}
            writer.writerow(ordered_job)


def append_to_csv(new_jobs, filename='gumtree_jobs.csv'):
    """Appends new jobs to existing CSV (ensures header exists)"""
    if not new_jobs:
        return
    
    headers = ['id', 'url', 'title', 'company', 'salary', 'location', 'work_time', 'contract_type', 'scraped_at', 'description']
    
    # Check if file exists
    file_exists = False
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            file_exists = True
            first_line = f.readline().strip()
            # Check if first line is header
            if first_line != ','.join(headers):
                # File exists but no header - need to add it
                f.seek(0)
                existing_content = f.read()
                with open(filename, 'w', newline='', encoding='utf-8') as fw:
                    writer = csv.DictWriter(fw, fieldnames=headers)
                    writer.writeheader()
                    fw.write(existing_content)
    except FileNotFoundError:
        pass
    
    # Append new jobs
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        # Write header if file is new
        if not file_exists:
            writer.writeheader()
        for job in new_jobs:
            ordered_job = {key: job.get(key, '') for key in headers}
            writer.writerow(ordered_job)
    print(f"Saved {len(new_jobs)} new jobs to {filename}")


async def scrape_gumtree_batch_mode(start_url, target_count=None, concurrency=3):
    """
    Main Gumtree scraping function with single browser instance for entire run
    
    Args:
        start_url: Starting URL (e.g. "https://www.gumtree.com/jobs/cash-in-hand")
        target_count: Target number of jobs to scrape (if None, uses count from jobs.csv)
        concurrency: Number of parallel requests (2-3 recommended)
    """
    # Get target count from jobs.csv if not specified
    if target_count is None:
        target_count = get_target_count_from_jobs_csv()
        print(f"Target count set from jobs.csv: {target_count} jobs")
    
    # Load existing Gumtree jobs for deduplication (by ID - titles repeat too much on Gumtree)
    existing_gumtree_ids = load_existing_gumtree_jobs()
    existing_count_in_file = count_existing_gumtree_jobs()

    print(f"=== GUMTREE SCRAPER BATCH MODE ===")
    print(f"Existing jobs in gumtree_jobs.csv: {existing_count_in_file}")
    print(f"Target: {target_count} jobs")
    print(f"Concurrency: {concurrency} parallel requests")
    print(f"Start URL: {start_url}")
    print()

    all_collected_jobs = []
    total_scraped = 0
    total_found_on_site = 0
    page_num = 1
    base_url = normalize_base_url(start_url)
    last_fingerprint = None  # For detecting pagination loops

    # Semaphore for concurrency control
    semaphore = asyncio.Semaphore(concurrency)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()

        try:
            while True:
                # Check if we reached target (existing + new scraped)
                if target_count and (existing_count_in_file + total_scraped) >= target_count:
                    print(f"\nTarget count {target_count} reached!")
                    break

                listing_page = await context.new_page()

                try:
                    # Try /pageN, if no results try ?page=N
                    listing_links = []
                    listing_url_used = None
                    for candidate in build_listing_urls(base_url, page_num):
                        print(f"\n--- PAGE {page_num} ---")
                        print(f"Scanning: {candidate}")
                        await listing_page.goto(candidate, wait_until="domcontentloaded", timeout=30000)
                        await listing_page.wait_for_timeout(1200)
                        listing_links = await collect_links_from_listing(listing_page)
                        if listing_links:
                            listing_url_used = candidate
                            break

                    if not listing_links:
                        print(f"Page {page_num}: No listings found - stopping")
                        await listing_page.close()
                        break

                    # Check for pagination loop (same results as previous page)
                    page_ids = [job_id_from_url(x["url"]) for x in listing_links if x.get("url")]
                    fingerprint = tuple(page_ids[:10])  # First 10 IDs as fingerprint
                    if fingerprint == last_fingerprint and page_num > 1:
                        print(f"Page {page_num}: Pagination seems stuck (same results as previous page). Stopping.")
                        await listing_page.close()
                        break
                    last_fingerprint = fingerprint

                    total_found_on_site += len(listing_links)

                    # Find new jobs (not in gumtree_jobs.csv yet) - use ID for deduplication (titles repeat on Gumtree)
                    new_links = []
                    skipped_count = 0
                    for link_data in listing_links:
                        url = link_data['url']
                        title = link_data.get('title', '')
                        job_id = job_id_from_url(url)
                        
                        # Check if we already have this job by ID
                        if job_id in existing_gumtree_ids:
                            skipped_count += 1
                            if skipped_count <= 3:  # Show first 3 skipped for debugging
                                print(f"  Duplicate skipped (ID {job_id}): {title[:50] if title else url[:50]}...")
                            continue
                        
                        # New job - add to list and tracking set
                        new_links.append(url)
                        existing_gumtree_ids.add(job_id)
                    
                    if skipped_count > 3:
                        print(f"  ... and {skipped_count - 3} more duplicates skipped")

                    current_total = existing_count_in_file + total_scraped
                    print(f"Progress: {current_total}/{target_count} jobs")
                    print(f"Page {page_num}: Found {len(listing_links)} total jobs, {len(new_links)} new jobs")

                    # Scrape new jobs (limit to target_count)
                    if new_links:
                        # Limit new_links to what we still need (total target - existing - already scraped)
                        remaining_needed = target_count - (existing_count_in_file + total_scraped)
                        if remaining_needed <= 0:
                            print(f"Target count {target_count} reached!")
                            await listing_page.close()
                            break
                        
                        jobs_to_scrape = new_links[:remaining_needed]
                        print(f"Downloading [{len(jobs_to_scrape)} new jobs from page {page_num}]...")

                        # Scrape with concurrency control (use context, not browser)
                        tasks = [scrape_single_job_gumtree(context, url, semaphore) for url in jobs_to_scrape]
                        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

                        # Collect valid results
                        batch_jobs = []
                        for result in batch_results:
                            if isinstance(result, dict) and result and result.get('title'):
                                batch_jobs.append(result)
                            elif isinstance(result, Exception):
                                print(f"Error in batch: {result}")

                        if batch_jobs:
                            # Save batch to CSV
                            append_to_csv(batch_jobs)
                            all_collected_jobs.extend(batch_jobs)
                            total_scraped += len(batch_jobs)
                            print(f"Page {page_num} completed: {len(batch_jobs)} jobs scraped")
                            print(f"Total progress: {existing_count_in_file + total_scraped}/{target_count}")

                    await listing_page.close()

                    # Check if we reached target before going to next page
                    if (existing_count_in_file + total_scraped) >= target_count:
                        print(f"\nTarget count {target_count} reached!")
                        break

                    # Pause between pages
                    print(f"Waiting 6 seconds before next page...")
                    await asyncio.sleep(6)

                    page_num += 1

                except Exception as e:
                    print(f"Error on page {page_num}: {str(e)}")
                    await listing_page.close()
                    break

        finally:
            await browser.close()

    final_total = existing_count_in_file + total_scraped
    print(f"\n=== SCRAPING COMPLETED ===")
    print(f"Jobs found on site: {total_found_on_site}")
    print(f"Jobs in gumtree_jobs.csv before: {existing_count_in_file}")
    print(f"New jobs scraped: {total_scraped}")
    print(f"Total jobs in gumtree_jobs.csv: {final_total}")
    print(f"Target was: {target_count}")
    print(f"Saved to: gumtree_jobs.csv")

    return all_collected_jobs


async def main():
    """Run Gumtree scraper"""
    # Example URL - can be changed to any category
    start_url = "https://www.gumtree.com/jobs/cash-in-hand"
    
    # target_count=None will use count from jobs.csv (1322 jobs)
    jobs = await scrape_gumtree_batch_mode(
        start_url=start_url,
        target_count=None,  # Will use count from jobs.csv
        concurrency=3  # 2-3 recommended for Gumtree
    )

    print(f"\n=== FINAL SUMMARY ===")
    print(f"Total scraped: {len(jobs)} jobs")
    print(f"Saved to: gumtree_jobs.csv")

    if jobs:
        print("\nExamples:")
        for i, job in enumerate(jobs[:3], 1):
            print(f"{i}. {job['title']}")
            print(f"   Salary: {job['salary']}")
            print(f"   Location: {job['location']}")
            print()


if __name__ == "__main__":
    asyncio.run(main())
