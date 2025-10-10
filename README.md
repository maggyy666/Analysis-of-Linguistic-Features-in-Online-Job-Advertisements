# OLX Job Scraper

Scraper do pobierania ofert pracy z OLX.pl przy użyciu Playwright.

## Funkcjonalności

- Scraping listingu ofert pracy z OLX.pl
- Ekstrakcja szczegółów każdej oferty
- Stabilne selektory oparte na tekście, a nie na kruchych klasach CSS
- Zapis danych do JSON/CSV

## Struktura danych

Każda oferta zawiera:
- `url` - link do oferty
- `title` - tytuł stanowiska (H1)
- `company` - nazwa firmy
- `salary_raw` - wynagrodzenie (jeśli dostępne)
- `location_short` - lokalizacja skrótowa
- `work_time` - wymiar pracy
- `contract_type` - typ umowy
- `address_street` - ulica (z sekcji Lokalizacja)
- `address_city_zip` - miasto i kod (z sekcji Lokalizacja)
- `description_raw` - pełny opis oferty
- `extras` - dodatkowe informacje (opcjonalne)

## Instalacja

```bash
# Instalacja zależności
poetry install

# Instalacja przeglądarek Playwright
poetry run playwright install
```

## Użycie

```bash
# Uruchomienie scrapera
poetry run python scraper.py
```

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
