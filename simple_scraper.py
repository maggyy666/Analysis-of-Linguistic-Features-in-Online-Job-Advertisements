"""
Prosty scraper OLX - 5 ogłoszeń ze strony 1
Minimalistycznie, bez emoji, bez zbędnych folderów
"""

import asyncio
import csv
import time
from datetime import datetime, timedelta
from playwright.async_api import async_playwright
from tabulate import tabulate


def load_existing_jobs():
    """Ładuje istniejące ogłoszenia z CSV dla deduplikacji"""
    existing_jobs = set()
    try:
        with open('jobs.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row['title'] and row['title'] != '403 ERROR':  # Tylko jeśli tytuł nie jest pusty i nie jest błędem
                    existing_jobs.add(row['title'].strip().lower())
        print(f"Loaded {len(existing_jobs)} existing jobs for deduplication")
    except FileNotFoundError:
        print("Brak istniejącego pliku jobs.csv - zaczynamy od zera")
    return existing_jobs


def count_existing_jobs():
    """Liczy ile ogłoszeń już mamy zapisanych"""
    try:
        with open('jobs.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                if row['title'] and row['title'] != '403 ERROR':
                    count += 1
        return count
    except FileNotFoundError:
        return 0


async def analyze_total_pages():
    """Analizuje ile jest stron i ogłoszeń na OLX praca"""
    
    print("Analyzing OLX pagination...")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            # Sprawdź stronę 1 - ile ogłoszeń na stronę
            url = "https://www.olx.pl/praca/?page=1"
            print(f"Scanning page 1: {url}")
            
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)
            
            job_links = await page.query_selector_all('a[href*="/oferta/praca/"]')
            jobs_per_page = len(job_links)
            print(f"Page 1: {jobs_per_page} jobs found")
            
            if not job_links:
                return 0, 0
            
            # Sprawdź stronę 25 - ostatnią
            url = "https://www.olx.pl/praca/?page=25"
            print(f"Scanning page 25: {url}")
            
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)
            
            job_links_page25 = await page.query_selector_all('a[href*="/oferta/praca/"]')
            jobs_on_last_page = len(job_links_page25)
            print(f"Page 25: {jobs_on_last_page} jobs found")
            
            # Oblicz szacunkową liczbę
            total_pages = 25
            estimated_total = (jobs_per_page * 24) + jobs_on_last_page
            
            print(f"\nESTIMATION COMPLETE:")
            print(f"Jobs per page (pages 1-24): {jobs_per_page}")
            print(f"Jobs on page 25: {jobs_on_last_page}")
            print(f"Total pages: {total_pages}")
            print(f"Estimated total jobs: {estimated_total}")
            
            return total_pages, estimated_total
        
        except Exception as e:
            print(f"Error analyzing pagination: {str(e)}")
            return 0, 0
        finally:
            await browser.close()


async def collect_all_job_links():
    """Zbiera linki do ogłoszeń ze wszystkich 25 stron"""
    
    existing_jobs = load_existing_jobs()
    all_links = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            for page_num in range(1, 26):  # Strony 1-25
                url = f"https://www.olx.pl/praca/?page={page_num}"
                print(f"Page {page_num}/25: {url}")
                
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(3000)
                
                # Znajdź wszystkie linki do ogłoszeń
                job_links = await page.query_selector_all('a[href*="/oferta/praca/"]')
                
                if not job_links:
                    print(f"Page {page_num}/25: No more job listings found - stopping")
                    break
                
                page_links = []
                for link in job_links:
                    href = await link.get_attribute('href')
                    if href and '/oferta/praca/' in href:
                        full_url = f"https://www.olx.pl{href}" if href.startswith('/') else href
                        
                        # Pobierz tytuł dla deduplikacji
                        try:
                            title_elem = await link.query_selector('h4')
                            if title_elem:
                                title = await title_elem.inner_text()
                                title_clean = title.strip().lower()
                                
                                # Sprawdź czy już mamy to ogłoszenie
                                if title_clean not in existing_jobs:
                                    page_links.append(full_url)
                                    existing_jobs.add(title_clean)
                                else:
                                    print(f"  Duplicate skipped: {title[:50]}...")
                        except:
                            # Jeśli nie można pobrać tytułu, dodaj link
                            page_links.append(full_url)
                
                print(f"Page {page_num}/25: Found {len(job_links)} total jobs, {len(page_links)} new jobs")
                all_links.extend(page_links)
                
                time.sleep(2)  # Pauza między stronami
        
        except Exception as e:
            print(f"Error collecting links: {str(e)}")
        finally:
            await browser.close()
    
    return all_links


async def scrape_jobs_in_batches(links, batch_size=5):
    """Scrapuje ogłoszenia w batch'ach z zapisem po każdym batch'u i ETA"""
    
    all_jobs = load_existing_jobs_from_csv()
    total_links = len(links)
    total_batches = (total_links + batch_size - 1) // batch_size
    
    # Statystyki czasu
    batch_times = []
    start_time = time.time()
    
    print(f"\nSCRAPING INITIATED")
    print(f"Target: {total_links} jobs in {total_batches} batches of {batch_size}")
    print(f"Start: {datetime.now().strftime('%H:%M:%S')}")
    print("=" * 60)
    
    for i in range(0, total_links, batch_size):
        batch_start_time = time.time()
        batch = links[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        
        print(f"\nBATCH {batch_num}/{total_batches}")
        print(f"Start: {datetime.now().strftime('%H:%M:%S')}")
        
        batch_jobs = []
        
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            
            try:
                # Scrapuj wszystkie linki z batch'a równolegle
                tasks = []
                for j, url in enumerate(batch):
                    print(f"Scraping {i+j+1}/{total_links}: {url[:50]}...")
                    tasks.append(scrape_single_job(browser, url))
                
                # Wykonaj wszystkie zadania w batch'u
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Dodaj tylko poprawne wyniki do batch'a
                for result in batch_results:
                    if isinstance(result, dict) and result:
                        batch_jobs.append(result)
                    elif isinstance(result, Exception):
                        print(f"Error in batch: {result}")
                
                print(f"Batch {batch_num} completed: {len(batch_jobs)} jobs")
                
            except Exception as e:
                print(f"Batch {batch_num} error: {str(e)}")
            finally:
                await browser.close()
        
        # ZAPISZ BATCH DO CSV
        if batch_jobs:
            append_to_csv(batch_jobs)
            all_jobs.extend(batch_jobs)
            print(f"Batch {batch_num} saved to CSV")
        else:
            print(f"Batch {batch_num} - no data to save")
        
        # Oblicz czasy i ETA
        batch_end_time = time.time()
        batch_duration = batch_end_time - batch_start_time
        batch_times.append(batch_duration)
        
        # Statystyki po batch'u
        elapsed_time = batch_end_time - start_time
        completed_batches = batch_num
        remaining_batches = total_batches - batch_num
        
        # Essential statistics only
        avg_batch_time = sum(batch_times) / len(batch_times)
        
        if remaining_batches > 0 and len(batch_times) > 0:
            eta_seconds = remaining_batches * avg_batch_time + (remaining_batches * 5)
            eta_time = datetime.now() + timedelta(seconds=eta_seconds)
            
            stats_data = [
                ["Total Elapsed", f"{elapsed_time:.1f}s"],
                ["ETA Completion", eta_time.strftime('%H:%M:%S')],
                ["Time Remaining", f"{eta_seconds/60:.1f} min"],
                ["Avg Job Time", f"{avg_batch_time:.1f}s"]
            ]
            
            print(f"\nBATCH {batch_num} STATUS:")
            print(tabulate(stats_data, headers=["Metric", "Value"], tablefmt="grid"))
        
        # Pause between batches - zwiększona pauza
        if i + batch_size < total_links:
            print(f"Pause 10 seconds before next batch...")
            time.sleep(10)
    
    # Podsumowanie końcowe
    total_time = time.time() - start_time
    final_avg_batch = sum(batch_times) / len(batch_times) if batch_times else 0
    
    # Final summary - essential data only
    final_stats = [
        ["Total Time", f"{total_time:.1f}s ({total_time/60:.1f} min)"],
        ["Jobs Collected", f"{len(all_jobs)} jobs"],
        ["Avg Job Time", f"{final_avg_batch:.1f}s"]
    ]
    
    print(f"\nSCRAPING COMPLETED")
    print(tabulate(final_stats, headers=["Metric", "Value"], tablefmt="grid"))
    
    return all_jobs


async def scrape_single_job(browser, url):
    """Scrapuje pojedyncze ogłoszenie"""
    try:
        new_page = await browser.new_page()
        await new_page.goto(url, wait_until="networkidle", timeout=30000)
        await new_page.wait_for_timeout(3000)  # Zwiększona pauza - 3 sekundy
        
        job_data = await extract_job_data(new_page, url)
        await new_page.close()
        
        # Dodatkowa pauza po każdym ogłoszeniu
        await asyncio.sleep(1)
        return job_data
        
    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        return None


async def scrape_jobs_batch_mode(target_count=None, batch_size=5):
    """Główna funkcja scrapowania w trybie batch - scrapuje od razu bez analizy"""
    
    # Sprawdź ile już mamy
    existing_count = count_existing_jobs()
    
    print(f"=== SCRAPER BATCH MODE ===")
    print(f"Current jobs in database: {existing_count}")
    if target_count:
        print(f"Target: {target_count} jobs in batches of {batch_size}")
    else:
        print(f"Target: ALL jobs in batches of {batch_size}")
    
    existing_jobs = load_existing_jobs()
    all_collected_jobs = []
    page_num = 1
    total_scraped = 0
    total_found_on_site = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        try:
            while True:
                print(f"\n--- PAGE {page_num}/25 ---")
                
                # Krok 1: Znajdź nowe linki na tej stronie
                page = await browser.new_page()
                url = f"https://www.olx.pl/praca/?page={page_num}"
                print(f"Scanning: {url}")
                
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await page.wait_for_timeout(5000)  # Zwiększona pauza - 5 sekund
                
                job_links = await page.query_selector_all('a[href*="/oferta/praca/"]')
                
                if not job_links:
                    print(f"Page {page_num}/25: No more job listings found - stopping")
                    break
                
                total_found_on_site += len(job_links)
                
                # Znajdź nowe ogłoszenia (nie ma w datasetcie)
                new_links = []
                for link in job_links:
                    href = await link.get_attribute('href')
                    if href and '/oferta/praca/' in href:
                        full_url = f"https://www.olx.pl{href}" if href.startswith('/') else href
                        
                        # Pobierz tytuł dla deduplikacji
                        try:
                            title_elem = await link.query_selector('h4')
                            if title_elem:
                                title = await title_elem.inner_text()
                                title_clean = title.strip().lower()
                                
                                if title_clean not in existing_jobs:
                                    new_links.append(full_url)
                                    existing_jobs.add(title_clean)
                        except:
                            new_links.append(full_url)
                
                current_total = existing_count + total_scraped
                print(f"Progress: {current_total}/{total_found_on_site} jobs")
                print(f"Page {page_num}/25: Found {len(job_links)} total jobs, {len(new_links)} new jobs")
                
                # Krok 2: Jeśli są nowe ogłoszenia, scrapuj je od razu
                if new_links:
                    print(f"Downloading [{len(new_links)} new jobs from page {page_num}]...")
                    page_jobs = await scrape_jobs_in_batches(new_links, batch_size)
                    
                    if page_jobs:
                        all_collected_jobs.extend(page_jobs)
                        total_scraped += len(page_jobs)
                        print(f"Page {page_num} completed: {len(page_jobs)} jobs scraped")
                        print(f"Total progress: {existing_count + total_scraped}/{total_found_on_site}")
                
                await page.close()
                
                # Sprawdź czy osiągnęliśmy target_count
                if target_count and total_scraped >= target_count:
                    print(f"Target count {target_count} reached")
                    break
                
                # Sprawdź czy to ostatnia strona
                if page_num >= 25:
                    print("Reached page 25 - last page")
                    break
                    
                page_num += 1
                print("Waiting 8 seconds before next page...")  # Zwiększona pauza
                time.sleep(8)  # Zwiększona pauza między stronami - 8 sekund
        
        finally:
            await browser.close()
    
    final_total = existing_count + total_scraped
    print(f"\nSCRAPING COMPLETED")
    print(f"Jobs found on site: {total_found_on_site}")
    print(f"Jobs in database before: {existing_count}")
    print(f"New jobs scraped: {total_scraped}")
    print(f"Total jobs in database: {final_total}")
    print(f"Saved to: jobs.csv")
    
    return all_collected_jobs


async def extract_job_data(page, url):
    """Wyciąga dane z pojedynczego ogłoszenia"""
    
    job = {
        'id': '',
        'url': url,
        'title': '',
        'company': '',
        'salary': '',
        'location': '',
        'work_time': '',
        'contract_type': '',
        'scraped_at': '',
        'description': ''
    }
    
    try:
        # Generuj ID z URL
        job['id'] = url.split('/')[-1].replace('.html', '')
        
        # Tytuł
        title_elem = await page.query_selector('h1')
        if title_elem:
            title_text = await title_elem.inner_text()
            job['title'] = title_text.strip()
        
        # Company (pierwszy tekst po H1)
        company_elem = await page.query_selector('h1 + p')
        if company_elem:
            company_text = await company_elem.inner_text()
            job['company'] = company_text.strip()
        
        # Opis - wyczyść i sformatuj
        desc_elem = await page.query_selector('.css-1i3492')
        if desc_elem:
            desc_text = await desc_elem.inner_text()
            # Usuń nadmiarowe białe znaki i nowe linie
            job['description'] = ' '.join(desc_text.split()).strip()
        
        # Atrybuty z tabelki - szukaj po etykietach i pobierz wartości
        # Wynagrodzenie
        salary_label = await page.query_selector('text="Wynagrodzenie"')
        if salary_label:
            parent = await salary_label.query_selector('xpath=..')
            if parent:
                value_elem = await parent.query_selector('.css-bf1kxk')
                if value_elem:
                    salary_text = await value_elem.inner_text()
                    job['salary'] = salary_text.strip()
        
        # Lokalizacja
        location_label = await page.query_selector('text="Lokalizacja"')
        if location_label:
            parent = await location_label.query_selector('xpath=..')
            if parent:
                value_elem = await parent.query_selector('.css-k31xxj')
                if value_elem:
                    location_text = await value_elem.inner_text()
                    job['location'] = location_text.strip()
        
        # Wymiar pracy
        work_time_label = await page.query_selector('text="Wymiar pracy"')
        if work_time_label:
            parent = await work_time_label.query_selector('xpath=..')
            if parent:
                value_elem = await parent.query_selector('.css-k31xxj')
                if value_elem:
                    work_time_text = await value_elem.inner_text()
                    job['work_time'] = work_time_text.strip()
        
        # Typ umowy
        contract_label = await page.query_selector('text="Typ umowy"')
        if contract_label:
            parent = await contract_label.query_selector('xpath=..')
            if parent:
                value_elem = await parent.query_selector('.css-k31xxj')
                if value_elem:
                    contract_text = await value_elem.inner_text()
                    job['contract_type'] = contract_text.strip()
        
        # Ustaw timestamp
        job['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    except Exception as e:
        print(f"Error extracting data: {str(e)}")
    
    return job


def save_to_csv(jobs, mode='w'):
    """Zapisuje dane do CSV w formacie Kaggle"""
    
    # Uporządkowane kolumny - opis na końcu
    headers = ['id', 'url', 'title', 'company', 'salary', 'location', 'work_time', 'contract_type', 'scraped_at', 'description']
    
    with open('jobs.csv', mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        
        # Pisz header tylko przy tworzeniu nowego pliku
        if mode == 'w':
            writer.writeheader()
        
        for job in jobs:
            writer.writerow(job)


def load_existing_jobs_from_csv():
    """Ładuje istniejące ogłoszenia z CSV"""
    existing_jobs = []
    try:
        with open('jobs.csv', 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_jobs.append(row)
        print(f"Loaded {len(existing_jobs)} existing jobs from CSV")
    except FileNotFoundError:
        print("Brak istniejącego pliku jobs.csv - zaczynamy od zera")
    return existing_jobs


def append_to_csv(new_jobs):
    """Dodaje nowe ogłoszenia do istniejącego CSV"""
    if new_jobs:
        save_to_csv(new_jobs, mode='a')
        print(f"Saved {len(new_jobs)} new jobs to CSV")


async def main():
    """Uruchomienie scrapera w trybie batch"""
    
    print("=== SCRAPER BATCH MODE ===")
    print("Full site scraping: All 25 pages (immediate scraping)")
    
    jobs = await scrape_jobs_batch_mode(target_count=None, batch_size=5)
    
    print(f"\n=== FINAL SUMMARY ===")
    print(f"Total scraped: {len(jobs)} jobs")
    print(f"Saved to: jobs.csv")
    
    if jobs:
        print("\nExamples:")
        for i, job in enumerate(jobs[:3], 1):
            print(f"{i}. {job['title']}")
            print(f"   Salary: {job['salary']}")
            print(f"   Location: {job['location']}")
            print()


async def scrape_5_jobs_quick():
    """Szybki test - 5 ogłoszeń (stara funkcjonalność)"""
    url = "https://www.olx.pl/praca/"
    jobs = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        try:
            print("Ładowanie strony...")
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(3000)
            
            job_links = await page.query_selector_all('a[href*="/oferta/praca/"]')
            
            for i, link in enumerate(job_links[:5]):
                href = await link.get_attribute('href')
                if href and '/oferta/praca/' in href:
                    full_url = f"https://www.olx.pl{href}" if href.startswith('/') else href
                    
                    print(f"Scrapowanie {i+1}/5: {full_url}")
                    
                    new_page = await browser.new_page()
                    try:
                        await new_page.goto(full_url, wait_until="networkidle", timeout=30000)
                        await new_page.wait_for_timeout(2000)
                        
                        job_data = await extract_job_data(new_page, full_url)
                        if job_data:
                            jobs.append(job_data)
                            
                    except Exception as e:
                        print(f"Błąd przy scrapowaniu {full_url}: {str(e)}")
                    finally:
                        await new_page.close()
        
        except Exception as e:
            print(f"Błąd: {str(e)}")
        finally:
            await browser.close()
    
    if jobs:
        save_to_csv(jobs)
        print(f"Zapisano {len(jobs)} ogłoszeń do jobs.csv")
    
    return jobs


if __name__ == "__main__":
    asyncio.run(main())
