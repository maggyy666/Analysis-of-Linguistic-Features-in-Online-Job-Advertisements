#!/usr/bin/env python3
"""
Step 3 — Unsupervised term/topic discovery
- Top terms (doc frequency + mean TF-IDF)
- Top phrases (bigrams/trigrams)
- Topics (NMF)
No extra CSVs. Prints to console. Optional --out to save ONE md report.
"""

import argparse
from pathlib import Path
import re
import pandas as pd

# ---- try sklearn; fallback possible ----
try:
    from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer, ENGLISH_STOP_WORDS
    from sklearn.decomposition import NMF
    from sklearn.feature_selection import chi2
    SKLEARN_OK = True
except Exception:
    SKLEARN_OK = False


# Minimalne "boilerplate" stopwords typowe dla ofert pracy (żeby tematy nie były = experience/requirements)
JOB_BOILER_EN = {
    "job","role","position","company","team","work","working","candidate","candidates",
    "responsibilities","responsibility","requirements","required","preferred","skills","skill",
    "experience","years","year","ability","including","must","will","within","using","etc",
    "provide","provides","benefits","support","looking","seeking","apply","application"
}
JOB_BOILER_PL = {
    "praca","pracę","pracy","pracownik","pracownika","pracownicza","stanowisko","stanowisku",
    "oferujemy","wymagania","wymagamy","oczekujemy","mile","widziane","doświadczenie","lat",
    "umiejętność","umiejętności","zakres","obowiązków","obowiązki","firma","zespół","możliwość",
    "zapewniamy","prosimy","aplikuj","zgłoszenie"
}

# krótka lista PL stopwords (żeby nie wypychało “i”, “w”, “na”)
STOP_PL = {
    "i","oraz","a","ale","bo","że","to","na","w","we","z","za","do","od","dla","po","pod","nad",
    "jest","są","być","będzie","będziesz","będziemy","może","możesz","także","jak","jako",
    "się","nie","tak","tego","tej","ten","ta","tę","tym","tych","tu","tam","u","o","czy",
    "który","która","które","których","którym","którego","której","oraz","ponadto"
}

TOKEN_PATTERN = r"(?u)\b[^\W\d_]{2,}\b"  # unicode letters, min len=2


def md_table(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except Exception:
        return df.to_string(index=False)


def normalize_text(s: pd.Series) -> pd.Series:
    s = s.fillna("").astype(str)
    s = s.str.replace(r"\s+", " ", regex=True).str.strip()
    return s


def load_df(lang: str, project_root: Path) -> tuple[pd.DataFrame, str]:
    if lang == "en":
        path = project_root / "en_dataset" / "en_postings_clean_sample.csv"
    else:
        path = project_root / "pl_dataset" / "pl_postings_clean.csv"

    if not path.exists():
        raise FileNotFoundError(f"Brak pliku: {path}")

    df = pd.read_csv(path)
    if "description" not in df.columns:
        raise ValueError("Brak kolumny 'description'")

    df["description"] = normalize_text(df["description"])
    return df, str(path)


def build_vectorizers(lang: str):
    if lang == "en":
        stop = list(set(ENGLISH_STOP_WORDS) | JOB_BOILER_EN)
        min_df_terms = 10
        min_df_phr = 10
    else:
        stop = list(STOP_PL | JOB_BOILER_PL)
        min_df_terms = 3
        min_df_phr = 3

    tfidf_terms = TfidfVectorizer(
        lowercase=True,
        stop_words=stop,
        token_pattern=TOKEN_PATTERN,
        min_df=min_df_terms,
        max_df=0.95,
        max_features=50000,
        ngram_range=(1, 1)
    )
    tfidf_phr = TfidfVectorizer(
        lowercase=True,
        stop_words=stop,
        token_pattern=TOKEN_PATTERN,
        min_df=min_df_phr,
        max_df=0.95,
        max_features=80000,
        ngram_range=(2, 3)
    )
    count_bin = CountVectorizer(
        lowercase=True,
        stop_words=stop,
        token_pattern=TOKEN_PATTERN,
        min_df=min_df_terms,
        max_df=0.95,
        max_features=50000,
        ngram_range=(1, 1),
        binary=True
    )
    count = CountVectorizer(
        lowercase=True,
        stop_words=stop,
        token_pattern=TOKEN_PATTERN,
        min_df=min_df_terms,
        max_df=0.95,
        max_features=50000,
        ngram_range=(1, 1)
    )
    return tfidf_terms, tfidf_phr, count_bin, count


def top_docfreq(df: pd.DataFrame, cv_bin: CountVectorizer, topn: int = 30) -> pd.DataFrame:
    X = cv_bin.fit_transform(df["description"])
    terms = cv_bin.get_feature_names_out()
    dfreq = (X.sum(axis=0)).A1  # number of docs containing term
    out = pd.DataFrame({"term": terms, "doc_count": dfreq})
    out["share"] = out["doc_count"] / len(df)
    out = out.sort_values(["doc_count", "term"], ascending=[False, True]).head(topn)
    out["share"] = out["share"].map(lambda x: f"{100*x:.2f}%")
    out["doc_count"] = out["doc_count"].astype(int)
    return out


def top_mean_tfidf(df: pd.DataFrame, tfidf: TfidfVectorizer, topn: int = 30) -> pd.DataFrame:
    X = tfidf.fit_transform(df["description"])
    terms = tfidf.get_feature_names_out()
    mean_scores = X.mean(axis=0).A1
    out = pd.DataFrame({"term": terms, "mean_tfidf": mean_scores})
    out = out.sort_values(["mean_tfidf", "term"], ascending=[False, True]).head(topn)
    out["mean_tfidf"] = out["mean_tfidf"].map(lambda x: f"{x:.6f}")
    return out


def topics_nmf(df: pd.DataFrame, tfidf: TfidfVectorizer, n_topics: int = 8, top_words: int = 12, sample_for_fit: int = 12000):
    # dla EN bierzemy próbkę do tematyzacji żeby nie zamuliło; PL leci w całości
    text = df["description"]
    if len(df) > sample_for_fit:
        text = text.sample(n=sample_for_fit, random_state=42)

    X = tfidf.fit_transform(text)
    terms = tfidf.get_feature_names_out()

    nmf = NMF(n_components=n_topics, random_state=42, init="nndsvda", max_iter=400)
    W = nmf.fit_transform(X)
    H = nmf.components_

    rows = []
    for k in range(n_topics):
        idx = H[k].argsort()[::-1][:top_words]
        rows.append({"topic": k, "top_terms": ", ".join([terms[i] for i in idx])})

    return pd.DataFrame(rows)


def discriminative_terms_by_group(df: pd.DataFrame, cv: CountVectorizer, group_col: str, top_groups: int = 3, top_terms: int = 12):
    if group_col not in df.columns:
        return None

    tmp = df.copy()
    tmp[group_col] = tmp[group_col].fillna("unknown").astype(str)
    # tylko najliczniejsze grupy, żeby nie było ściany tekstu
    top_vals = tmp[group_col].value_counts().head(top_groups).index.tolist()
    tmp = tmp[tmp[group_col].isin(top_vals)]

    X = cv.fit_transform(tmp["description"])
    y = tmp[group_col].values
    terms = cv.get_feature_names_out()

    out_rows = []
    for val in top_vals:
        mask = (y == val)
        chi, p = chi2(X, mask)
        idx = chi.argsort()[::-1][:top_terms]
        out_rows.append({"group": val, "top_terms": ", ".join([terms[i] for i in idx])})

    return pd.DataFrame(out_rows)


def make_report(lang: str, df: pd.DataFrame, path: str) -> str:
    tfidf_terms, tfidf_phr, cv_bin, cv = build_vectorizers(lang)

    lines = []
    lines.append(f"# Step 3 — Unsupervised discovery ({lang})")
    lines.append(f"Input: `{path}`")
    lines.append(f"Rows: **{len(df)}**")
    lines.append("")

    lines.append("## 1) Most common terms (document frequency)")
    lines.append(md_table(top_docfreq(df, cv_bin, topn=30)))
    lines.append("")

    lines.append("## 2) Most characteristic terms (mean TF-IDF)")
    lines.append(md_table(top_mean_tfidf(df, tfidf_terms, topn=30)))
    lines.append("")

    lines.append("## 3) Most characteristic phrases (2–3 grams, mean TF-IDF)")
    lines.append(md_table(top_mean_tfidf(df, tfidf_phr, topn=30)))
    lines.append("")

    lines.append("## 4) Topics (NMF on TF-IDF, top terms per topic)")
    n_topics = 8 if lang == "en" else 6
    lines.append(md_table(topics_nmf(df, tfidf_terms, n_topics=n_topics, top_words=12)))
    lines.append("")

    # discriminative by work type
    grp = "formatted_work_type"
    disc = discriminative_terms_by_group(df, cv, grp, top_groups=3, top_terms=12)
    if disc is not None:
        lines.append(f"## 5) Terms that differentiate top `{grp}` groups (chi²)")
        lines.append(md_table(disc))
        lines.append("")

    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--en", action="store_true")
    g.add_argument("--pl", action="store_true")
    ap.add_argument("--out", default=None, help="optional ONE markdown output file")
    args = ap.parse_args()

    if not SKLEARN_OK:
        print("ERROR: Brak scikit-learn. Zainstaluj: pip install scikit-learn")
        return

    script_dir = Path(__file__).parent
    project_root = script_dir.parent  # repo root (parent of EDA)

    lang = "en" if args.en else "pl"
    df, path = load_df(lang, project_root)

    report = make_report(lang, df, path)
    print(report)

    if args.out:
        Path(args.out).write_text(report, encoding="utf-8")
        print(f"\nSaved report: {args.out}")


if __name__ == "__main__":
    main()
