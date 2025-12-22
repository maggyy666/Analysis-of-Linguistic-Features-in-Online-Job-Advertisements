# Job Advertisement Scrapers

Zestaw scraperów do pobierania ofert pracy z różnych źródeł:
- **OLX.pl** - polskie oferty pracy (Playwright)
- **Adzuna API** - angielskie oferty pracy (API)

## Funkcjonalności

### OLX Scraper (`simple_scraper.py`)
- Scraping listingu ofert pracy z OLX.pl
- Ekstrakcja szczegółów każdej oferty
- Stabilne selektory oparte na tekście
- Rate limiting i progress tracking
- Zapis danych do CSV (`jobs.csv`)

### Adzuna Scraper (`adzuna_scraper.py`)
- Pobieranie angielskich ofert pracy przez Adzuna API
- Filtrowanie ofert nie-IT (retail, warehouse, production, delivery, hospitality, cleaning)
- Normalizacja danych do formatu OLX
- Zapis danych do CSV (`jobs_en.csv`)

## Struktura danych

Oba scrapers zapisują dane w tym samym formacie CSV:

- `id` - unikalny identyfikator oferty
- `url` - link do oferty
- `title` - tytuł stanowiska
- `company` - nazwa firmy
- `salary` - wynagrodzenie (format zależny od źródła)
- `location` - lokalizacja
- `work_time` - wymiar pracy (pełny/niepełny etat)
- `contract_type` - typ umowy
- `scraped_at` - data i czas pobrania
- `description` - pełny opis oferty

## Instalacja

```bash
# Instalacja zależności
poetry install

# Instalacja przeglądarek Playwright
poetry run playwright install
```

## Użycie

### OLX Scraper

```bash
# Uruchomienie scrapera OLX
poetry run python simple_scraper.py
```

### Adzuna Scraper

1. **Uzyskaj klucze API:**
   - Zarejestruj się na https://developer.adzuna.com/
   - Utwórz aplikację i uzyskaj `APP_ID` oraz `APP_KEY`

2. **Ustaw zmienne środowiskowe:**
   ```bash
   # Windows (PowerShell)
   $env:ADZUNA_APP_ID="your_app_id"
   $env:ADZUNA_APP_KEY="your_app_key"
   
   # Linux/Mac
   export ADZUNA_APP_ID="your_app_id"
   export ADZUNA_APP_KEY="your_app_key"
   ```

3. **Uruchom scraper:**
   ```bash
   poetry run python adzuna_scraper.py
   ```

Scraper pobiera oferty z kategorii nie-IT:
- Retail (shop assistant, cashier)
- Warehouse (warehouse operative, picker packer)
- Production (production worker, factory operative)
- Delivery (delivery driver, courier)
- Hospitality (kitchen assistant, bar staff)
- Cleaning (cleaner, cleaning operative)

## Development

```bash
# Formatowanie kodu
poetry run black .
poetry run isort .

# Typy
poetry run mypy .

# Testy
poetry run pytest
```
