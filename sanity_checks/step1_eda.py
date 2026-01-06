#!/usr/bin/env python3
"""
Step 1 — EDA (Exploratory Data Analysis)
Sanity checks + basic text statistics for PL and EN job postings datasets.
"""

import argparse
import pandas as pd
import re
from pathlib import Path

WORD_RE = re.compile(r"[A-Za-zÀ-ž0-9_]+", re.UNICODE)


def pct(x: float) -> str:
    return f"{100*x:.2f}%"


def series_word_count(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str)
    return s.map(lambda x: len(WORD_RE.findall(x)))


def quantiles(x: pd.Series) -> dict:
    x = x.dropna()
    if len(x) == 0:
        return {"min": None, "p25": None, "median": None, "p75": None, "p95": None, "max": None, "mean": None}
    return {
        "min": int(x.min()),
        "p25": float(x.quantile(0.25)),
        "median": float(x.quantile(0.50)),
        "p75": float(x.quantile(0.75)),
        "p95": float(x.quantile(0.95)),
        "max": int(x.max()),
        "mean": float(x.mean()),
    }


def top_table(s: pd.Series, n=15, fill="unknown") -> pd.DataFrame:
    s = s.fillna(fill).astype(str)
    vc = s.value_counts().head(n)
    df = vc.reset_index()
    df.columns = ["value", "count"]
    df["share"] = df["count"] / df["count"].sum()
    return df


def md_table(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except Exception:
        # fallback bez tabulate
        return df.to_string(index=False)


def make_report(df: pd.DataFrame, lang: str, path: str) -> str:
    lines = []
    lines.append(f"# Step 1 — EDA ({lang})")
    lines.append(f"Input: `{path}`")
    lines.append("")

    # 1) overview
    n_rows = len(df)
    n_cols = df.shape[1]
    mem_mb = df.memory_usage(deep=True).sum() / (1024**2)
    n_uid = df["job_id"].nunique() if "job_id" in df.columns else None

    lines.append("## 1) Overview")
    lines.append(f"- rows: **{n_rows}**")
    lines.append(f"- cols: **{n_cols}**")
    if n_uid is not None:
        lines.append(f"- unique job_id: **{n_uid}**")
    else:
        lines.append("- unique job_id: n/a")
    lines.append(f"- memory: **{mem_mb:.2f} MB**")
    lines.append(f"- columns: `{', '.join(df.columns.tolist())}`")
    lines.append("")

    # 2) missingness
    lines.append("## 2) Missingness")
    miss = pd.DataFrame({
        "col": df.columns,
        "missing_count": [int(df[c].isna().sum()) for c in df.columns],
        "missing_share": [df[c].isna().mean() for c in df.columns],
    }).sort_values("missing_share", ascending=False)
    miss["missing_share"] = miss["missing_share"].map(pct)
    lines.append(md_table(miss))
    lines.append("")

    # 3) duplicates
    lines.append("## 3) Duplicates")
    dup_lines = []
    if "job_id" in df.columns:
        dup_job = df.duplicated(subset=["job_id"]).mean()
        dup_lines.append(f"- dup by job_id: **{pct(dup_job)}**")
    key_cols = [c for c in ["title", "company_name", "location", "description"] if c in df.columns]
    if key_cols:
        dup_content = df.duplicated(subset=key_cols).mean()
        dup_lines.append(f"- dup by content key ({'+'.join(key_cols)}): **{pct(dup_content)}**")
    if not dup_lines:
        dup_lines.append("- no duplicate checks available (missing columns)")
    lines += dup_lines
    lines.append("")

    # 4) text length
    lines.append("## 4) Description length")
    if "description" in df.columns:
        desc = df["description"].fillna("").astype(str)
        char_len = desc.str.len()
        word_len = series_word_count(desc)

        q_char = quantiles(char_len)
        q_word = quantiles(word_len)

        length_df = pd.DataFrame([
            {"metric": "char_len", **q_char},
            {"metric": "word_len", **q_word},
        ])
        lines.append(md_table(length_df))
    else:
        lines.append("- missing `description` column")
    lines.append("")

    # 5) distributions
    lines.append("## 5) Basic distributions")
    for col in ["formatted_work_type", "formatted_experience_level", "remote_allowed"]:
        if col in df.columns:
            lines.append(f"### {col} (top 15)")
            t = top_table(df[col], n=15, fill="unknown")
            t["share"] = t["share"].map(lambda x: f"{100*x:.2f}%")
            lines.append(md_table(t))
            lines.append("")

    # 6) locations
    lines.append("## 6) Locations")
    if "location" in df.columns:
        lines.append("### location raw (top 15)")
        t1 = top_table(df["location"], n=15, fill="unknown")
        t1["share"] = t1["share"].map(lambda x: f"{100*x:.2f}%")
        lines.append(md_table(t1))
        lines.append("")

        lines.append("### city (location split by ',') (top 15)")
        city = df["location"].fillna("unknown").astype(str).map(lambda x: x.split(",")[0].strip().lower())
        t2 = top_table(city, n=15, fill="unknown")
        t2["share"] = t2["share"].map(lambda x: f"{100*x:.2f}%")
        lines.append(md_table(t2))
        lines.append("")
    else:
        lines.append("- missing `location` column")
        lines.append("")

    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser(
        description="Step 1 — EDA for PL/EN job postings datasets"
    )
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--en", action="store_true", help="Process EN dataset")
    g.add_argument("--pl", action="store_true", help="Process PL dataset")
    ap.add_argument("--out", default=None, help="Optional path to save markdown report file")
    args = ap.parse_args()

    if args.en:
        lang = "en"
        path = Path("en_dataset/en_postings_clean_sample.csv")
    else:
        lang = "pl"
        path = Path("pl_dataset/pl_postings_clean.csv")

    if not path.exists():
        print(f"Error: File not found: {path}")
        print("Make sure you have run clean_datasets.py first to create the cleaned datasets.")
        return

    print(f"Loading {path}...")
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} rows")
    print()

    report = make_report(df, lang=lang, path=str(path))

    print(report)
    
    if args.out:
        out_path = Path(args.out)
        out_path.write_text(report, encoding="utf-8")
        print(f"\nReport saved to: {out_path}")


if __name__ == "__main__":
    main()
