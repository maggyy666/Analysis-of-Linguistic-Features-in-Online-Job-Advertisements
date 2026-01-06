#!/usr/bin/env python3
import argparse
import re
from pathlib import Path

import pandas as pd

SEED = 42
MIN_LEN = 200
EN_TARGET = 30000  # jak EN jest ogromny, to ucinamy do sensownej próbki

# ---------------- EN ----------------
EEO_CUT = re.compile(
    r"(equal opportunity employer|eoe|applicants must be authorized|right to work|"
    r"we do not discriminate|privacy policy|by applying|gdpr|reasonable accommodations)",
    re.IGNORECASE
)

def strip_boilerplate_en(text: str) -> str:
    if not isinstance(text, str):
        return ""
    m = EEO_CUT.search(text)
    if m:
        text = text[:m.start()]
    return re.sub(r"\s+", " ", text).strip()

def clean_en(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["description"] = df["description"].map(strip_boilerplate_en)
    df = df[df["description"].astype(str).str.len() >= MIN_LEN]

    # dedup po opisie
    df["desc_key"] = df["description"].astype(str).str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
    df = df.drop_duplicates(subset=["desc_key"], keep="first").drop(columns=["desc_key"])

    # dedup po title+company+description
    df["title_key"] = df["title"].fillna("").astype(str).str.lower().str.strip()
    df["company_key"] = df["company_name"].fillna("").astype(str).str.lower().str.strip()
    df = df.drop_duplicates(subset=["title_key", "company_key", "description"], keep="first") \
           .drop(columns=["title_key", "company_key"])

    df["source"] = "linkedin"
    df["lang"] = "en"
    return df

def stratified_sample_en(df: pd.DataFrame, n: int) -> pd.DataFrame:
    if len(df) <= n:
        return df

    cols = [c for c in ["formatted_work_type", "formatted_experience_level"] if c in df.columns]
    if not cols:
        return df.sample(n=n, random_state=SEED)

    tmp = df.copy()
    for c in cols:
        tmp[c] = tmp[c].fillna("unknown")

    grp = tmp.groupby(cols, dropna=False, group_keys=False)
    sizes = grp.size()
    weights = (sizes / sizes.sum() * n).round().astype(int)

    parts = []
    for key, g in grp:
        k = int(weights.loc[key])
        if k > 0:
            parts.append(g.sample(n=min(k, len(g)), random_state=SEED))

    out = pd.concat(parts, ignore_index=True) if parts else tmp.sample(n=n, random_state=SEED)
    if len(out) > n:
        out = out.sample(n=n, random_state=SEED)
    return out

# ---------------- PL ----------------
CUT_PATTERNS_PL = [
    r"prosimy o dopisanie",
    r"wyrażam zgodę na przetwarzanie",
    r"administratorem danych",
    r"rodo",
    r"urz[eę]du ochrony danych",
    r"kodeks pracy",
    r"dane osobowe",
    r"informujemy, że skontaktujemy się tylko",
]
CUT_RE = re.compile("|".join(CUT_PATTERNS_PL), re.IGNORECASE)

EMAIL_RE = re.compile(r"\b[\w\.-]+@[\w\.-]+\.\w+\b")
PHONE_RE = re.compile(r"(\+?\d[\d\s\-]{7,}\d)|(\b\d{2,3}\*{3,}\d{2,3}\b)")

def clean_pl_description(text: str) -> str:
    if not isinstance(text, str):
        return ""
    m = CUT_RE.search(text)
    if m:
        text = text[:m.start()]
    text = EMAIL_RE.sub(" ", text)
    text = PHONE_RE.sub(" ", text)
    return re.sub(r"\s+", " ", text).strip()

def map_work_type_pl(work_time: str) -> str:
    if not isinstance(work_time, str):
        return "unknown"
    wt = work_time.strip().lower()
    mapping = {
        "pełny etat": "Full-time",
        "niepełny etat": "Part-time",
        "praca dodatkowa": "Part-time",
        "praca sezonowa": "Temporary",
    }
    return mapping.get(wt, "Other")

def clean_pl(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["description"] = df["description"].map(clean_pl_description)
    df = df[df["description"].astype(str).str.len() >= MIN_LEN]

    # dedup
    if "job_id" in df.columns:
        df = df.drop_duplicates(subset=["job_id"], keep="first")

    df["title_key"] = df["title"].fillna("").astype(str).str.lower().str.strip()
    df["company_key"] = df["company_name"].fillna("").astype(str).str.lower().str.strip()
    df = df.drop_duplicates(subset=["title_key", "company_key", "location", "description"], keep="first") \
           .drop(columns=["title_key", "company_key"])

    df["source"] = "olx"
    df["lang"] = "pl"
    return df

# ---------------- IO helpers ----------------
def pick_input(preferred: list[str], folder: str) -> Path:
    # 1) preferowane nazwy
    for name in preferred:
        p = Path(folder) / name
        if p.exists():
            return p

    # 2) jak nie ma, bierz pierwszy CSV z folderu
    d = Path(folder)
    if d.exists():
        candidates = sorted(d.glob("*.csv"))
        if candidates:
            return candidates[0]

    raise FileNotFoundError(f"Nie znalazłem inputu w {folder}/ (ani preferowanych nazw).")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--en", action="store_true")
    ap.add_argument("--pl", action="store_true")
    args = ap.parse_args()

    if not args.en and not args.pl:
        raise SystemExit("Daj chociaż jedno: --en albo --pl")

    out_parts = []

    if args.en:
        en_path = pick_input(
            preferred=["en_postings.csv", "en_postings_clean_sample.csv", "en_postings_clean.csv"],
            folder="en_dataset",
        )
        en = pd.read_csv(en_path)

        keep = [c for c in [
            "job_id","title","company_name","description","location",
            "formatted_work_type","formatted_experience_level","remote_allowed"
        ] if c in en.columns]
        en = en[keep].copy()

        en = clean_en(en)
        en = stratified_sample_en(en, EN_TARGET)  # auto-downsample
        out_parts.append(en)
        print(f"[EN] input={en_path} -> rows={len(en)}")

    if args.pl:
        pl_path = pick_input(
            preferred=["pl_jobs.csv", "pl_postings.csv", "pl_dataset.csv"],
            folder="pl_dataset",
        )
        pl = pd.read_csv(pl_path)

        pl = pl.rename(columns={
            "id": "job_id",
            "company": "company_name",
            "work_time": "work_time_pl",
            "contract_type": "contract_type_pl",
        })

        keep = [c for c in [
            "job_id","title","company_name","description","location",
            "work_time_pl","contract_type_pl","salary"
        ] if c in pl.columns]
        pl = pl[keep].copy()

        pl["formatted_work_type"] = pl.get("work_time_pl", pd.Series(["unknown"] * len(pl))).map(map_work_type_pl)
        pl["formatted_experience_level"] = "unknown"
        pl["remote_allowed"] = None

        pl = clean_pl(pl)
        out_parts.append(pl)
        print(f"[PL] input={pl_path} -> rows={len(pl)}")

    out = pd.concat(out_parts, ignore_index=True) if len(out_parts) > 1 else out_parts[0]

    # sensowny porządek kolumn
    order = [
        "job_id","title","company_name","description","location",
        "work_time_pl","contract_type_pl","salary",
        "formatted_work_type","formatted_experience_level","remote_allowed",
        "source","lang"
    ]
    cols = [c for c in order if c in out.columns] + [c for c in out.columns if c not in order]
    out = out[cols]

    out_path = Path("jobs_clean.csv")
    out.to_csv(out_path, index=False)
    print(f"[OUT] saved={len(out)} -> {out_path}")

if __name__ == "__main__":
    main()
