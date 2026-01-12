#!/usr/bin/env python3
"""
Wyodrębnia rekordy z experience_label='unknown' z en_jobs_clean.csv
i zapisuje je do unknown_dataset.csv
"""

import pandas as pd

INPUT_CSV = "en_dataset/en_jobs_clean.csv"
OUTPUT_CSV = "unknown_dataset.csv"

def main():
    print(f"[1] Wczytywanie danych z {INPUT_CSV}...")
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    print(f"   Załadowano {len(df):,} rekordów")
    
    # Sprawdź czy kolumna experience_label istnieje
    if "experience_label" not in df.columns:
        print("   BŁĄD: Kolumna 'experience_label' nie istnieje w pliku!")
        print(f"   Dostępne kolumny: {', '.join(df.columns[:10])}...")
        return
    
    print(f"\n[2] Filtrowanie rekordów z experience_label='unknown'...")
    df_unknown = df[df["experience_label"] == "unknown"].copy()
    print(f"   Znaleziono {len(df_unknown):,} rekordów ({len(df_unknown)/len(df)*100:.1f}% wszystkich)")
    
    if len(df_unknown) == 0:
        print("   Brak rekordów do zapisania!")
        return
    
    print(f"\n[3] Zapisuję do {OUTPUT_CSV}...")
    df_unknown.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"   Zapisano {len(df_unknown):,} rekordów")
    
    print(f"\n[Done] Gotowe! Plik zapisany: {OUTPUT_CSV}")
    
    # Podsumowanie statystyczne
    print(f"\n=== Podsumowanie ===")
    print(f"Całkowita liczba rekordów: {len(df):,}")
    print(f"Rekordy 'unknown': {len(df_unknown):,} ({len(df_unknown)/len(df)*100:.1f}%)")
    
    if "desc_len" in df_unknown.columns:
        print(f"\nStatystyki opisów (unknown):")
        print(f"  Średnia długość: {df_unknown['desc_len'].mean():.0f} znaków")
        print(f"  Mediana długości: {df_unknown['desc_len'].median():.0f} znaków")
        print(f"  Min: {df_unknown['desc_len'].min():.0f}, Max: {df_unknown['desc_len'].max():.0f}")


if __name__ == "__main__":
    main()
