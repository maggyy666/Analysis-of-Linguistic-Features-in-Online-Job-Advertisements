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
        return 1322  # Default fallback


def load_existing_gumtree_jobs():
    """Loads existing Gumtree jobs from gumtree_jobs.csv for internal deduplication only"""
    existing_jobs = set()
    try:
        with open('gumtree_jobs.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('url'):
                    existing_jobs.add(row['url'].strip().lower())
        print(f"Loaded {len(existing_jobs)} existing Gumtree jobs for deduplication")
    except FileNotFoundError:
        print("No existing gumtree_jobs.csv file - starting fresh")
    return existing_jobs


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
    """Collects job links from already loaded listing page"""
    links = set()

    # Primary: data-q anchors
    for a in await page.query_selector_all('a[data-q="search-result-anchor"]'):
        href = await a.get_attribute("href")
        if href:
            links.add(urljoin(GUMTREE_BASE, href.split("?")[0]))

    # Fallback: /p/ style links
    if not links:
        for a in await page.query_selector_all('a[href^="/p/"]'):
            href = await a.get_attribute("href")
            if href:
                links.add(urljoin(GUMTREE_BASE, href.split("?")[0]))

    return list(links)


async def read_dt_dd(page, label: str) -> str:
    """Reads value from dt/dd pair (Gumtree job details format)"""
    loc = page.locator(f"xpath=//dt[normalize-space()='{label}']/following-sibling::dd[1]")
    if await loc.count():
        return " ".join((await loc.first.inner_text()).split()).strip()
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
        loc = page.locator("h1 + h4")
        if await loc.count():
            job["location"] = " ".join((await loc.first.inner_text()).split()).strip()

        # Pola z sekcji details (Salary / Recruiter / Contract Type / Hours)
        job["salary"] = await read_dt_dd(page, "Salary")
        job["contract_type"] = await read_dt_dd(page, "Contract Type")
        job["work_time"] = await read_dt_dd(page, "Hours") or await read_dt_dd(page, "Working hours")
        job["company"] = (
            await read_dt_dd(page, "Recruiter")
            or await read_dt_dd(page, "Company")
            or await read_dt_dd(page, "Advertiser")
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
            m = re.search(
                r"(£\s?\d+(?:\.\d+)?(?:\s*-\s*£\s?\d+(?:\.\d+)?)?(?:\s*(?:per|/)\s*(?:hour|day|week|month|annum|year))?)",
                job["description"],
                re.IGNORECASE
            )
            if m:
                job["salary"] = " ".join(m.group(1).split()).strip()

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
    """Saves data to CSV file"""
    headers = ['id', 'url', 'title', 'company', 'salary', 'location', 'work_time', 'contract_type', 'scraped_at', 'description']

    with open(filename, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)

        if mode == 'w':
            writer.writeheader()

        for job in jobs:
            writer.writerow(job)


def append_to_csv(new_jobs, filename='gumtree_jobs.csv'):
    """Appends new jobs to existing CSV"""
    if new_jobs:
        save_to_csv(new_jobs, mode='a', filename=filename)
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
    
    # Load only Gumtree jobs for internal deduplication (separate file)
    existing_gumtree_jobs = load_existing_gumtree_jobs()
    existing_count_in_file = len(existing_gumtree_jobs)

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

                    total_found_on_site += len(listing_links)

                    # Find new jobs (not in gumtree_jobs.csv yet)
                    new_links = []
                    for link in listing_links:
                        # Use URL for deduplication (Gumtree has unique URLs)
                        link_key = link.lower()
                        if link_key not in existing_gumtree_jobs:
                            new_links.append(link)
                            existing_gumtree_jobs.add(link_key)

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
