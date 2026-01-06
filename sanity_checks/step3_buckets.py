#!/usr/bin/env python3
"""
Step 4 — Bucket coverage (no manual keyword lists in CLI)
- Buckets = categories built from Step3 outputs
- Prints overall + by group tables
- No CSV outputs. Optional one markdown via --out.
"""

import argparse
import re
from pathlib import Path
import pandas as pd

SEED = 42

TOKEN_PATTERN = r"(?u)\b[^\W\d_]{2,}\b"


def md_table(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except Exception:
        return df.to_string(index=False)


def normalize_text(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.lower().str.replace(r"\s+", " ", regex=True).str.strip()


def phrase_regex(p: str) -> str:
    """match word boundaries, flexible whitespace for phrases"""
    p = p.strip().lower()
    if not p:
        return ""
    if " " in p:
        parts = [re.escape(x) for x in p.split()]
        return r"\b" + r"\s+".join(parts) + r"\b"
    return r"\b" + re.escape(p) + r"\b"


def bucket_regex(phrases: list[str]) -> re.Pattern:
    regs = [phrase_regex(p) for p in phrases if p.strip()]
    regs = [r for r in regs if r]
    # OR of phrases (non-capturing group to avoid warnings)
    big = "(?:" + "|".join(regs) + ")" if regs else r"^$"
    return re.compile(big, re.IGNORECASE)


# ---- Buckets from your Step3 outputs ----
BUCKETS_EN = {
    # --- Industry / role signals (more specific) ---
    "healthcare": [
        "registered nurse", "rn", "patient care", "clinical", "hospital",
        "healthcare", "physician", "medical assistant"
    ],
    "retail_store": [
        "retail", "store associate", "cashier", "merchandising",
        "stockroom", "loss prevention"
    ],
    "finance_accounting": [
        "financial statements", "general ledger", "gaap", "audit",
        "tax returns", "accounts payable", "accounts receivable", "reconciliation"
    ],
    "manufacturing_maintenance": [
        "preventive maintenance", "troubleshoot", "cnc", "forklift",
        "production line", "machine operator", "quality assurance", "osha"
    ],
    "construction_project": [
        "construction", "site supervisor", "subcontractors",
        "project manager", "schedule", "blueprints", "estimating"
    ],
    "sales_marketing": [
        "account executive", "lead generation", "pipeline management",
        "quota", "crm", "b2b sales", "salesforce"
    ],

    # --- HR / ad language (generic, expected to be high) ---
    "education_requirements": [
        "high school diploma", "bachelor's degree", "bachelor degree",
        "master's degree", "degree required"
    ],
    "benefits_comp": [
        "paid time off", "401k", "life insurance", "health insurance",
        "dental", "vision", "disability", "employee assistance"
    ],
    "eeo_dei_boiler": [
        "equal opportunity employer", "we do not discriminate",
        "sexual orientation", "national origin", "gender identity"
    ],
}

BUCKETS_PL = {
    "umowa_forma": ["umowa o pracę", "umowa zlecenie", "umowa o dzieło", "b2b", "etat", "pełen etat"],
    "wynagrodzenie": ["wynagrodzenie", "premia", "stawka", "zł brutto", "zł netto", "brutto", "netto"],
    "prawo_jazdy": ["prawo jazdy", "jazdy kat", "kat b", "kat. b"],
    "czas_grafik": ["elastyczny grafik", "systemie zmianowym", "system zmianowy", "poniedziałku piątku", "weekend"],
    "sprzedaz_klient": ["sprzedaży", "sprzedaż", "obsługa klienta", "klienta", "klientów"],
    "gastronomia": ["restauracji", "restauracja", "mcdonald", "kelner", "kuchni", "posiłki"],
    "produkcja_magazyn": ["produkcji", "produkcja", "maszyn", "magazyn", "pakowanie", "kompletacja"],
    "franczyza_sklep": ["franczyzobiorc", "franczyza", "sklep", "sklepu"],
    "telekom": ["orange", "telekomunikacyjnych", "usług telekomunikacyjnych"],
}


def overall(flags: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "bucket": flags.columns,
        "count": [int(flags[c].sum()) for c in flags.columns],
    })
    out["share"] = out["count"] / len(flags)
    out = out.sort_values(["count", "bucket"], ascending=[False, True])
    out["share"] = out["share"].map(lambda x: f"{100*x:.2f}%")
    return out


def by_group(df: pd.DataFrame, flags: pd.DataFrame, group_cols: list[str], top_groups: int = 10) -> pd.DataFrame:
    tmp = df.copy()
    for c in group_cols:
        tmp[c] = tmp[c].fillna("unknown").astype(str)

    joined = pd.concat([tmp[group_cols], flags], axis=1)
    g = joined.groupby(group_cols, dropna=False)

    rows = []
    for key, part in g:
        if not isinstance(key, tuple):
            key = (key,)
        base = {group_cols[i]: key[i] for i in range(len(group_cols))}
        base["n"] = len(part)
        for b in flags.columns:
            base[b] = part[b].mean()
        rows.append(base)

    out = pd.DataFrame(rows).sort_values("n", ascending=False).head(top_groups)
    for b in flags.columns:
        out[b] = out[b].map(lambda x: f"{100*x:.1f}%")
    return out


def main():
    ap = argparse.ArgumentParser(
        description="Step 4 — Bucket coverage analysis for PL/EN job postings"
    )
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--en", action="store_true", help="Process EN dataset")
    g.add_argument("--pl", action="store_true", help="Process PL dataset")
    ap.add_argument("--out", default=None, help="optional ONE markdown output file")
    ap.add_argument("--top-groups", type=int, default=10, help="Number of top groups to show")
    args = ap.parse_args()

    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    if args.en:
        lang = "en"
        path = project_root / "en_dataset" / "en_postings_clean_sample.csv"
        buckets = BUCKETS_EN
        group_cols_list = [["formatted_work_type"], ["formatted_work_type", "formatted_experience_level"]]
    else:
        lang = "pl"
        path = project_root / "pl_dataset" / "pl_postings_clean.csv"
        buckets = BUCKETS_PL
        group_cols_list = [["formatted_work_type"], ["formatted_work_type", "contract_type_pl"]]

    if not path.exists():
        print(f"Error: File not found: {path}")
        print("Make sure you have run clean_datasets.py first to create the cleaned datasets.")
        return

    print(f"Loading {path}...")
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} rows")
    
    df["description"] = normalize_text(df["description"])

    # flags per bucket
    flags = {}
    for b, phrases in buckets.items():
        rx = bucket_regex(phrases)
        flags[b] = df["description"].str.contains(rx, na=False)
    flags = pd.DataFrame(flags)

    lines = []
    lines.append(f"# Step 4 — Bucket coverage ({lang})")
    lines.append(f"Input: `{path}`")
    lines.append(f"Rows: **{len(df)}**")
    lines.append("")
    lines.append("## 1) Overall coverage")
    lines.append(md_table(overall(flags)))
    lines.append("")

    for group_cols in group_cols_list:
        ok = all(c in df.columns for c in group_cols)
        if ok:
            lines.append(f"## 2) By {', '.join(group_cols)} (top groups)")
            lines.append(md_table(by_group(df, flags, group_cols, top_groups=args.top_groups)))
            lines.append("")

    report = "\n".join(lines)
    print(report)

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(report, encoding="utf-8")
        print(f"\nSaved report: {out_path}")


if __name__ == "__main__":
    main()
