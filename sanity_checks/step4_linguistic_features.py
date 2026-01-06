#!/usr/bin/env python3
"""
Step 5 — Linguistic features analysis (EN/PL)
- No CSV outputs.
- Prints report to console. Optional one markdown via --out.
"""

import argparse
import re
from pathlib import Path
import pandas as pd


def md_table(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except Exception:
        return df.to_string(index=False)


# --- Buckets (same as Step4) ---
def phrase_regex(p: str) -> str:
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
    big = "(?:" + "|".join(regs) + ")" if regs else r"^$"
    return re.compile(big, re.IGNORECASE)


BUCKETS_EN = {
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


def quantile_summary(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    qs = [0.25, 0.5, 0.75, 0.95]
    rows = []
    for c in cols:
        s = df[c].dropna()
        if len(s) == 0:
            continue
        q = s.quantile(qs).to_dict()
        rows.append({
            "metric": c,
            "min": float(s.min()),
            "p25": float(q[0.25]),
            "median": float(q[0.5]),
            "p75": float(q[0.75]),
            "p95": float(q[0.95]),
            "max": float(s.max()),
            "mean": float(s.mean()),
        })
    return pd.DataFrame(rows)


def compute_features(text_raw: pd.Series) -> pd.DataFrame:
    raw = text_raw.fillna("").astype(str)
    char_len = raw.str.len().astype("int64")

    # words: unicode-ish letters only (skip digits/underscore)
    word_count = raw.str.count(r"(?u)\b[^\W\d_]+\b").astype("int64")

    # sentence heuristic
    sentence_count = raw.str.count(r"[.!?]+").astype("int64")
    sentence_count = sentence_count.where(sentence_count > 0, 1)

    avg_words_per_sentence = word_count / sentence_count

    # punctuation rates
    exclam = raw.str.count("!").astype("int64")
    quest = raw.str.count(r"\?").astype("int64")

    denom = char_len.where(char_len > 0, 1)
    exclam_per_1k = exclam / denom * 1000.0
    question_per_1k = quest / denom * 1000.0

    # digits / caps
    digit_count = raw.str.count(r"\d").astype("int64")
    digit_share = digit_count / denom  # 0..1

    caps_word_count = raw.str.count(r"\b[A-Z]{2,}\b").astype("int64")
    denom_words = word_count.where(word_count > 0, 1)
    caps_word_share = caps_word_count / denom_words  # 0..1

    # bullet markers (works best if text kept newlines, but still catches symbols)
    bullet_lines = raw.str.count(r"(?m)^\s*(?:[-•*·]|\d+[.)])").astype("int64")
    bullet_symbols = raw.str.count(r"[•·]").astype("int64") + raw.str.count(r"\s-\s").astype("int64")
    bullet_markers = bullet_lines + bullet_symbols
    bullet_markers_per_1k = bullet_markers / denom * 1000.0

    return pd.DataFrame({
        "char_len": char_len,
        "word_count": word_count,
        "sentence_count": sentence_count,
        "avg_words_per_sentence": avg_words_per_sentence,
        "exclam_per_1k": exclam_per_1k,
        "question_per_1k": question_per_1k,
        "digit_share": digit_share,
        "caps_word_share": caps_word_share,
        "bullet_markers_per_1k": bullet_markers_per_1k,
    })


def format_feature_table(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    # nicer formatting
    for c in out.columns:
        if c in {"digit_share", "caps_word_share"}:
            out[c] = (out[c] * 100).map(lambda x: f"{x:.2f}%")
        elif c.endswith("_per_1k"):
            out[c] = out[c].map(lambda x: f"{x:.2f}")
        elif isinstance(out[c].iloc[0] if len(out) > 0 else None, float):
            out[c] = out[c].map(lambda x: f"{x:.2f}" if pd.notna(x) else "")
        # Keep integer columns as is
    return out


def bucket_flags(df: pd.DataFrame, buckets: dict[str, list[str]]) -> pd.DataFrame:
    text = df["description"].fillna("").astype(str).str.lower()
    flags = {}
    for b, phrases in buckets.items():
        rx = bucket_regex(phrases)
        flags[b] = text.str.contains(rx, na=False)
    return pd.DataFrame(flags)


def group_means(df_feat: pd.DataFrame, group: pd.Series, top_groups: int) -> pd.DataFrame:
    tmp = df_feat.copy()
    g = group.fillna("unknown").astype(str)
    tmp["_g"] = g

    agg = tmp.groupby("_g").agg(
        n=("char_len", "size"),
        char_len=("char_len", "mean"),
        word_count=("word_count", "mean"),
        sentence_count=("sentence_count", "mean"),
        avg_words_per_sentence=("avg_words_per_sentence", "mean"),
        exclam_per_1k=("exclam_per_1k", "mean"),
        question_per_1k=("question_per_1k", "mean"),
        digit_share=("digit_share", "mean"),
        caps_word_share=("caps_word_share", "mean"),
        bullet_markers_per_1k=("bullet_markers_per_1k", "mean"),
    ).reset_index().rename(columns={"_g": "group"}).sort_values("n", ascending=False)

    return agg.head(top_groups)


def bucket_feature_table(df_feat: pd.DataFrame, flags: pd.DataFrame) -> pd.DataFrame:
    rows = []
    overall = df_feat.mean(numeric_only=True)

    for b in flags.columns:
        mask = flags[b].fillna(False)
        n = int(mask.sum())
        if n == 0:
            continue
        m = df_feat.loc[mask].mean(numeric_only=True)
        row = {
            "bucket": b,
            "n": n,
            "share": n / len(df_feat),
            "char_len": m["char_len"],
            "word_count": m["word_count"],
            "avg_words_per_sentence": m["avg_words_per_sentence"],
            "exclam_per_1k": m["exclam_per_1k"],
            "digit_share": m["digit_share"],
            "bullet_markers_per_1k": m["bullet_markers_per_1k"],
            "delta_char_len": m["char_len"] - overall["char_len"],
            "delta_exclam_per_1k": m["exclam_per_1k"] - overall["exclam_per_1k"],
        }
        rows.append(row)

    out = pd.DataFrame(rows).sort_values("n", ascending=False)
    out["share"] = out["share"].map(lambda x: f"{100*x:.2f}%")
    return out


def main():
    ap = argparse.ArgumentParser(description="Step 5 — Linguistic features for PL/EN job postings")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--en", action="store_true", help="Process EN dataset")
    g.add_argument("--pl", action="store_true", help="Process PL dataset")
    ap.add_argument("--top-groups", type=int, default=10, help="Number of top groups to show")
    ap.add_argument("--out", default=None, help="optional ONE markdown output file")
    args = ap.parse_args()

    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    if args.en:
        lang = "en"
        path = project_root / "en_dataset" / "en_postings_clean_sample.csv"
        buckets = BUCKETS_EN
        group_cols = ["formatted_work_type", "formatted_experience_level"]
    else:
        lang = "pl"
        path = project_root / "pl_dataset" / "pl_postings_clean.csv"
        buckets = BUCKETS_PL
        group_cols = ["formatted_work_type", "contract_type_pl"]

    if not path.exists():
        print(f"Error: File not found: {path}")
        print("Make sure you have run clean_datasets.py first to create the cleaned datasets.")
        return

    print(f"Loading {path}...")
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} rows\n")

    # features
    feat = compute_features(df["description"])

    # bucket flags
    flags = bucket_flags(df, buckets)

    lines = []
    lines.append(f"# Step 5 — Linguistic features ({lang})")
    lines.append(f"Input: `{path}`")
    lines.append(f"Rows: **{len(df)}**")
    lines.append("")

    # overall stats
    lines.append("## 1) Feature summary (quantiles)")
    qs = quantile_summary(feat, [
        "char_len", "word_count", "sentence_count", "avg_words_per_sentence",
        "exclam_per_1k", "question_per_1k", "digit_share", "caps_word_share",
        "bullet_markers_per_1k"
    ])
    lines.append(md_table(format_feature_table(qs)))
    lines.append("")

    # by main group
    if group_cols[0] in df.columns:
        lines.append(f"## 2) By {group_cols[0]} (top groups)")
        t = group_means(feat, df[group_cols[0]], top_groups=args.top_groups)
        lines.append(md_table(format_feature_table(t)))
        lines.append("")

    # second grouping if exists
    if len(group_cols) == 2 and all(c in df.columns for c in group_cols):
        combo = df[group_cols[0]].fillna("unknown").astype(str) + " | " + df[group_cols[1]].fillna("unknown").astype(str)
        lines.append(f"## 3) By {group_cols[0]} + {group_cols[1]} (top groups)")
        t = group_means(feat, combo, top_groups=args.top_groups)
        lines.append(md_table(format_feature_table(t)))
        lines.append("")

    # by buckets
    lines.append("## 4) By buckets (coverage + mean features)")
    bt = bucket_feature_table(feat, flags)
    lines.append(md_table(format_feature_table(bt)))
    lines.append("")

    report = "\n".join(lines)
    print(report)

    if args.out:
        out_path = Path(args.out)
        out_path.write_text(report, encoding="utf-8")
        print(f"\nSaved report: {out_path}")


if __name__ == "__main__":
    main()
