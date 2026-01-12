#!/usr/bin/env python3
"""
Wyodrębnia rekordy bez salary (gdzie salary_min i salary_max są null/puste)
z pl_jobs_clean.csv i zapisuje je do no_salary_dataset.csv
"""

import pandas as pd
from pathlib import Path

INPUT_CSV = "pl_dataset/pl_jobs_clean.csv"
OUTPUT_CSV = "no_salary_dataset.csv"

def main():
    print("=" * 60)
    print("Extract No Salary Dataset (PL)")
    print("=" * 60)
    
    print(f"[1] Wczytywanie danych z {INPUT_CSV}...")
    
    if not Path(INPUT_CSV).exists():
        print(f"   ❌ BŁĄD: Plik nie istnieje: {INPUT_CSV}")
        return
    
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    print(f"   Załadowano {len(df):,} rekordów")
    
    # Sprawdź czy kolumny salary istnieją
    salary_cols = ["salary_min", "salary_max"]
    missing_cols = [col for col in salary_cols if col not in df.columns]
    if missing_cols:
        print(f"   ❌ BŁĄD: Brak kolumn: {', '.join(missing_cols)}")
        print(f"   Dostępne kolumny: {', '.join(df.columns[:10])}...")
        return
    
    print(f"\n[2] Filtrowanie rekordów bez salary...")
    df_no_salary = df[
        df["salary_min"].isna() & df["salary_max"].isna()
    ].copy()
    
    print(f"   Znaleziono {len(df_no_salary):,} rekordów bez salary ({len(df_no_salary)/len(df)*100:.1f}% wszystkich)")
    
    if len(df_no_salary) == 0:
        print("   ⚠️  Brak rekordów do zapisania!")
        return
    
    print(f"\n[3] Zapisuję do {OUTPUT_CSV}...")
    df_no_salary.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"   ✅ Zapisano {len(df_no_salary):,} rekordów")
    
    print(f"\n{'='*60}")
    print(f"Done! Plik zapisany: {OUTPUT_CSV}")
    print(f"{'='*60}")
    
    # Podsumowanie statystyczne
    print(f"\n=== Podsumowanie ===")
    print(f"Całkowita liczba rekordów: {len(df):,}")
    print(f"Rekordy bez salary: {len(df_no_salary):,} ({len(df_no_salary)/len(df)*100:.1f}%)")
    
    if "desc_len" in df_no_salary.columns:
        print(f"\nStatystyki opisów (no salary):")
        print(f"  Średnia długość: {df_no_salary['desc_len'].mean():.0f} znaków")
        print(f"  Mediana długości: {df_no_salary['desc_len'].median():.0f} znaków")
        print(f"  Min: {df_no_salary['desc_len'].min():.0f}, Max: {df_no_salary['desc_len'].max():.0f}")
    
    if "experience_label" in df_no_salary.columns:
        print(f"\nRozkład experience_label:")
        exp_dist = df_no_salary["experience_label"].value_counts(dropna=False)
        print(exp_dist.to_string())
    
    if "remote_allowed" in df_no_salary.columns:
        remote_count = df_no_salary["remote_allowed"].notna().sum()
        print(f"\nRekordy z remote_allowed: {remote_count:,} ({remote_count/len(df_no_salary)*100:.1f}%)")


if __name__ == "__main__":
    main()
