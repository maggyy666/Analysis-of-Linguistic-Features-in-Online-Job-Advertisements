#!/usr/bin/env python3
r"""
Step 6 — Statistical tests (EN vs PL) for linguistic features

- Loads BOTH datasets (EN + PL) automatically.
- Computes features (same as Step 5).
- For each metric: Mann–Whitney U (normal approx w/ tie correction + continuity),
  effect size = rank-biserial correlation (≈ Cliff's delta).
- Optional balanced mode: downsample EN to match PL (or a given n).
- Optional repeats: repeat downsampling many times and report median + 95% CI.
- Optional ONE markdown file via --out.

Run:
  python .\EDA\step6_tests.py
  python .\EDA\step6_tests.py --downsample-en 1212 --seed 42
  python .\EDA\step6_tests.py --downsample-en 1212 --repeats 50 --seed 42
"""

import argparse
import math
from pathlib import Path
import pandas as pd


FEATURES = [
    "char_len",
    "word_count",
    "sentence_count",
    "avg_words_per_sentence",
    "exclam_per_1k",
    "question_per_1k",
    "digit_share",
    "caps_word_share",
    "bullet_markers_per_1k",
]


def md_table(df: pd.DataFrame) -> str:
    try:
        return df.to_markdown(index=False)
    except Exception:
        return df.to_string(index=False)


def compute_features(text_raw: pd.Series) -> pd.DataFrame:
    raw = text_raw.fillna("").astype(str)
    char_len = raw.str.len().astype("int64")

    word_count = raw.str.count(r"(?u)\b[^\W\d_]+\b").astype("int64")

    sentence_count = raw.str.count(r"[.!?]+").astype("int64")
    sentence_count = sentence_count.where(sentence_count > 0, 1)

    avg_words_per_sentence = word_count / sentence_count

    exclam = raw.str.count("!").astype("int64")
    quest = raw.str.count(r"\?").astype("int64")

    denom = char_len.where(char_len > 0, 1)
    exclam_per_1k = exclam / denom * 1000.0
    question_per_1k = quest / denom * 1000.0

    digit_count = raw.str.count(r"\d").astype("int64")
    digit_share = digit_count / denom  # 0..1

    caps_word_count = raw.str.count(r"\b[A-Z]{2,}\b").astype("int64")
    denom_words = word_count.where(word_count > 0, 1)
    caps_word_share = caps_word_count / denom_words  # 0..1

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


def norm_cdf(z: float) -> float:
    return 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))


def mann_whitney_u_two_sided(x: pd.Series, y: pd.Series):
    x = pd.Series(x).dropna()
    y = pd.Series(y).dropna()

    n1 = int(len(x))
    n2 = int(len(y))
    if n1 == 0 or n2 == 0:
        return None

    combined = pd.concat([x, y], ignore_index=True)
    ranks = combined.rank(method="average")

    r1 = float(ranks.iloc[:n1].sum())
    U1 = r1 - (n1 * (n1 + 1)) / 2.0

    mu = (n1 * n2) / 2.0
    N = n1 + n2

    vc = combined.value_counts(dropna=False)
    ties = vc[vc > 1].astype(float)
    tie_sum = float(((ties ** 3) - ties).sum())

    denom = (N * (N - 1)) if N > 1 else 1.0
    varU = (n1 * n2) / 12.0 * ((N + 1.0) - (tie_sum / denom))
    if varU <= 0:
        return None

    sigma = math.sqrt(varU)

    diff = U1 - mu
    cc = 0.5 * (1.0 if diff > 0 else (-1.0 if diff < 0 else 0.0))
    z = (diff - cc) / sigma

    p = 2.0 * min(norm_cdf(z), 1.0 - norm_cdf(z))

    # rank-biserial correlation (≈ Cliff's delta)
    r_rb = (2.0 * U1) / (n1 * n2) - 1.0

    return {"n1": n1, "n2": n2, "U1": U1, "z": z, "p": p, "r_rb": r_rb}


def bh_fdr(pvals: list[float]) -> list[float]:
    m = len(pvals)
    order = sorted(range(m), key=lambda i: pvals[i])
    q = [0.0] * m
    prev = 1.0
    for rank, i in enumerate(reversed(order), start=1):
        p = pvals[i]
        k = m - rank + 1
        val = min(prev, p * m / k)
        q[i] = val
        prev = val
    return q


def fmt_p(p: float) -> str:
    if p is None:
        return ""
    if p == 0.0:
        return "<1e-300"
    if p < 1e-6:
        return f"{p:.2e}"
    return f"{p:.6f}"


def fmt_num(metric: str, v: float) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return ""
    if metric in {"digit_share", "caps_word_share"}:
        return f"{100.0 * v:.2f}%"
    if metric.endswith("_per_1k"):
        return f"{v:.2f}"
    if abs(v) >= 100:
        return f"{v:.0f}"
    return f"{v:.2f}"


def metric_summary(s: pd.Series):
    s = pd.Series(s).dropna()
    if len(s) == 0:
        return None
    return {
        "median": float(s.median()),
        "p25": float(s.quantile(0.25)),
        "p75": float(s.quantile(0.75)),
    }


def run_once(df_en: pd.DataFrame, df_pl: pd.DataFrame) -> pd.DataFrame:
    feat_en = compute_features(df_en["description"])
    feat_pl = compute_features(df_pl["description"])

    rows, pvals = [], []
    for m in FEATURES:
        x, y = feat_en[m], feat_pl[m]
        summ_en = metric_summary(x)
        summ_pl = metric_summary(y)
        res = mann_whitney_u_two_sided(x, y)
        if res is None:
            continue
        pvals.append(res["p"])
        rows.append({
            "metric": m,
            "EN_median": summ_en["median"],
            "PL_median": summ_pl["median"],
            "delta_median": summ_en["median"] - summ_pl["median"],
            "p": res["p"],
            "r_rb": res["r_rb"],
            "z": res["z"],
        })

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out["q_fdr"] = bh_fdr(out["p"].tolist())
    out["direction"] = out["r_rb"].map(lambda r: "EN > PL" if r > 0 else ("EN < PL" if r < 0 else "≈"))
    return out


def summarize_repeats(results: list[pd.DataFrame]) -> pd.DataFrame:
    # assume same metrics present each time
    merged = []
    for i, df in enumerate(results):
        tmp = df[["metric", "EN_median", "PL_median", "delta_median", "r_rb"]].copy()
        tmp["rep"] = i
        merged.append(tmp)
    allr = pd.concat(merged, ignore_index=True)

    def q(x, p):
        return float(pd.Series(x).quantile(p))

    rows = []
    for m in allr["metric"].unique():
        part = allr[allr["metric"] == m]
        rows.append({
            "metric": m,
            "delta_median_med": float(part["delta_median"].median()),
            "delta_median_ci95": f"[{q(part['delta_median'],0.025):.2f}, {q(part['delta_median'],0.975):.2f}]",
            "r_rb_med": float(part["r_rb"].median()),
            "r_rb_ci95": f"[{q(part['r_rb'],0.025):.3f}, {q(part['r_rb'],0.975):.3f}]",
        })
    out = pd.DataFrame(rows)
    out["direction"] = out["r_rb_med"].map(lambda r: "EN > PL" if r > 0 else ("EN < PL" if r < 0 else "≈"))
    out = out.sort_values(by="r_rb_med", key=lambda s: s.abs(), ascending=False)
    return out


def main():
    ap = argparse.ArgumentParser(description="Step 6 — EN vs PL statistical tests for linguistic features")
    ap.add_argument("--out", default=None, help="optional ONE markdown output file")
    ap.add_argument("--seed", type=int, default=42, help="random seed")
    ap.add_argument("--downsample-en", type=int, default=None,
                    help="If set: sample EN down to this n (balanced mode). Example: 1212")
    ap.add_argument("--repeats", type=int, default=1,
                    help="If >1 and --downsample-en is set: repeat downsampling and report median + 95% CI.")
    args = ap.parse_args()

    script_dir = Path(__file__).parent
    project_root = script_dir.parent

    path_en = project_root / "en_dataset" / "en_postings_clean_sample.csv"
    path_pl = project_root / "pl_dataset" / "pl_postings_clean.csv"

    if not path_en.exists():
        print(f"Error: EN file not found: {path_en}")
        return
    if not path_pl.exists():
        print(f"Error: PL file not found: {path_pl}")
        return

    print(f"Loading EN: {path_en}")
    df_en_full = pd.read_csv(path_en)
    print(f"Loaded EN rows: {len(df_en_full)}\n")

    print(f"Loading PL: {path_pl}")
    df_pl = pd.read_csv(path_pl)
    print(f"Loaded PL rows: {len(df_pl)}\n")

    # Decide sampling
    if args.downsample_en is not None:
        n_target = args.downsample_en
        if n_target <= 0:
            print("Error: --downsample-en must be > 0")
            return
        if n_target > len(df_en_full):
            print(f"Error: --downsample-en={n_target} > EN size={len(df_en_full)}")
            return

        if args.repeats <= 1:
            df_en = df_en_full.sample(n=n_target, replace=False, random_state=args.seed)
            results = run_once(df_en, df_pl)
            mode_note = f"BALANCED (EN downsampled to n={n_target}, seed={args.seed})"
        else:
            reps = []
            for i in range(args.repeats):
                df_en = df_en_full.sample(n=n_target, replace=False, random_state=args.seed + i)
                reps.append(run_once(df_en, df_pl))
            mode_note = f"BALANCED (EN downsampled to n={n_target}, repeats={args.repeats}, seed={args.seed})"

            # report repeats summary
            summary = summarize_repeats(reps)
            lines = []
            lines.append("# Step 6B — Balanced statistical tests (EN vs PL)")
            lines.append(f"Mode: **{mode_note}**")
            lines.append(f"EN file: `{path_en}` (full n={len(df_en_full)})")
            lines.append(f"PL file: `{path_pl}` (n={len(df_pl)})")
            lines.append("")
            lines.append("**Test:** Mann–Whitney U (two-sided, normal approximation with tie correction + continuity correction)")
            lines.append("**Effect size:** rank-biserial correlation `r_rb` (≈ Cliff's delta), range [-1, 1]; sign shows direction.")
            lines.append("")
            lines.append("## Results across repeats (median + 95% CI)")
            lines.append(md_table(summary[["metric", "direction", "delta_median_med", "delta_median_ci95", "r_rb_med", "r_rb_ci95"]]))
            lines.append("")

            report = "\n".join(lines)
            print(report)
            if args.out:
                Path(args.out).write_text(report, encoding="utf-8")
                print(f"\nSaved report: {args.out}")
            return
    else:
        df_en = df_en_full
        results = run_once(df_en, df_pl)
        mode_note = "FULL (no downsampling)"

    if results.empty:
        print("No results (empty).")
        return

    # Pretty formatting (single run)
    pretty = results.copy()
    for c in ["EN_median", "PL_median", "delta_median"]:
        pretty[c] = [fmt_num(pretty.loc[i, "metric"], float(pretty.loc[i, c])) for i in range(len(pretty))]
    pretty["p"] = pretty["p"].map(fmt_p)
    pretty["q_fdr"] = pretty["q_fdr"].map(fmt_p)
    pretty["r_rb"] = pretty["r_rb"].map(lambda v: f"{v:.3f}")
    pretty["z"] = pretty["z"].map(lambda v: f"{v:.3f}")

    pretty = pretty.sort_values(by="r_rb", key=lambda s: s.astype(float).abs(), ascending=False)

    lines = []
    lines.append("# Step 6 — Statistical tests (EN vs PL)")
    lines.append(f"Mode: **{mode_note}**")
    lines.append(f"EN file: `{path_en}` (n={len(df_en)})")
    lines.append(f"PL file: `{path_pl}` (n={len(df_pl)})")
    lines.append("")
    lines.append("**Test:** Mann–Whitney U (two-sided, normal approximation with tie correction + continuity correction)")
    lines.append("**Effect size:** rank-biserial correlation `r_rb` (≈ Cliff's delta), range [-1, 1]; sign shows direction.")
    lines.append("**Multiple comparisons:** Benjamini–Hochberg FDR (`q_fdr`).")
    lines.append("")
    lines.append("## Results (sorted by |effect|)")
    cols = ["metric", "direction", "EN_median", "PL_median", "delta_median", "p", "q_fdr", "r_rb", "z"]
    lines.append(md_table(pretty[cols]))
    lines.append("")

    # quick highlights
    tmp = results.copy()
    tmp["abs_r"] = tmp["r_rb"].abs()
    top = tmp.sort_values("abs_r", ascending=False).head(5)

    lines.append("## Top differences (by effect size)")
    for _, r in top.iterrows():
        m = r["metric"]
        dir_ = "EN > PL" if r["r_rb"] > 0 else "EN < PL"
        lines.append(
            f"- **{m}**: {dir_}, median(EN)={fmt_num(m, r['EN_median'])}, "
            f"median(PL)={fmt_num(m, r['PL_median'])}, q_fdr={fmt_p(r['q_fdr'])}, r_rb={r['r_rb']:.3f}"
        )
    lines.append("")

    report = "\n".join(lines)
    print(report)

    if args.out:
        Path(args.out).write_text(report, encoding="utf-8")
        print(f"\nSaved report: {args.out}")


if __name__ == "__main__":
    main()
