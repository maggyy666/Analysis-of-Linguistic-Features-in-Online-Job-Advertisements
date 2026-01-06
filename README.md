
# Analysis of Linguistic Features in Online Job Advertisements (PL vs EN)

This project compares **Polish (PL)** and **English (EN)** job advertisements with a focus on:
- frequent words/phrases and latent topics (unsupervised discovery),
- “bucket” coverage (rule-based category flags built from phrases/signals),
- linguistic/style features (length, punctuation, digits, ALL-CAPS, bulleting),
- statistical testing of EN vs PL differences (including a balanced/downsampled mode).

---

## Data sources

### EN (LinkedIn Job Postings)
Kaggle dataset:  
`https://www.kaggle.com/datasets/arshkon/linkedin-job-postings`

### PL (OLX scraped)
PL data comes from scraping OLX (stored locally in this repo).

---

## Repository structure (key files)

- `clean_datasets.py`  
  Cleans and standardizes raw PL/EN datasets so all later steps run on consistent columns.
  Output: cleaned CSVs used by the pipeline.

- `en_dataset/`
  - `en_postings_clean_sample.csv` — cleaned (optionally sampled) EN dataset.

- `pl_dataset/`
  - `pl_postings_clean.csv` — cleaned PL dataset.

- `sanity_checks/` (main analysis pipeline; console reports, optional markdown via `--out`)
  - `step1_sanity.py`
  - `step2_discover.py`
  - `step3_buckets.py`
  - `step4_linguistic_features.py`
  - `step5_tests.py`

> Note: the old `EDA/` directory was removed. Everything was moved into `sanity_checks/`.
> The previous “step2” was removed and numbering was updated:
> - `step2_discover` (previously step3)
> - `step3_buckets` (previously step4)
> - `step4_linguistic_features` (previously step5)
> - `step5_tests` (previously step6)

---

## Quick start

### 1) Clean datasets (PL/EN)
Run separately for EN and PL:

```bash
python clean_datasets.py --en
python clean_datasets.py --pl
````

**Goal:** produce standardized, analysis-ready CSVs (e.g., consistent `description` and grouping columns such as
`formatted_work_type`, `formatted_experience_level` (EN) / `contract_type_pl` (PL)).

---

## Pipeline: `sanity_checks/`

Move into the pipeline folder:

```bash
cd sanity_checks
```

### Step 1 — sanity checks

```bash
python step1_sanity.py --en
python step1_sanity.py --pl
```

**What it does:**

* quick validation after cleaning,
* checks missing values / basic distributions,
* confirms grouping columns exist and look reasonable (work type, experience level, contract type).

**Why it matters:** prevents “garbage in → garbage out” before running heavier analyses.

---

### Step 2 — unsupervised discovery (terms/phrases/topics)

```bash
python step2_discover.py --en
python step2_discover.py --pl
```

**What it does (high-level):**

* most common terms (document frequency),
* most characteristic terms (mean TF-IDF),
* most characteristic phrases (2–3 grams, mean TF-IDF),
* topics via NMF on TF-IDF (top terms per topic),
* terms differentiating selected groups (e.g., `formatted_work_type`, chi²).

**What you get:** an interpretable “content map” of postings (themes + strong lexical signals), useful for defining buckets.

---

### Step 3 — bucket coverage (rule-based categories)

```bash
python step3_buckets.py --en
python step3_buckets.py --pl
```

**What it does:**

* defines buckets for EN/PL as sets of phrases/regex signals,
* computes overall bucket coverage,
* breaks coverage down by key groups:

  * EN: `formatted_work_type`, optionally + `formatted_experience_level`
  * PL: `formatted_work_type`, optionally + `contract_type_pl`

**What you get:** category prevalence (benefits, education, driving license, pay, etc.) and how it varies across job types/contract forms.

---

### Step 4 — linguistic features (style/format metrics)

```bash
python step4_linguistic_features.py --en
python step4_linguistic_features.py --pl
```

**What it computes (examples):**

* `char_len`, `word_count`, `sentence_count`, `avg_words_per_sentence`
* punctuation rates per 1k chars: `exclam_per_1k`, `question_per_1k`
* digit usage: `digit_share`
* ALL-CAPS usage: `caps_word_share`
* bullet/list intensity: `bullet_markers_per_1k`

**Report includes:**

* distribution summary (quantiles),
* group means (work type / experience / contract),
* mean feature profiles for postings matching each bucket.

**What you get:** a measurable description of posting style (length, structure, formality, list formatting, etc.).

---

### Step 5 — statistical tests (EN vs PL)

```bash
python step5_tests.py
```

**What it does:**

* loads both datasets (EN + PL),
* computes the same feature set as Step 4,
* runs **Mann–Whitney U** per metric (non-parametric),
* reports effect size: rank-biserial correlation `r_rb` (≈ Cliff’s delta),
* applies multiple-comparisons correction: **Benjamini–Hochberg FDR** (`q_fdr`).

**Balanced mode (recommended for defensible comparison):**

```bash
python step5_tests.py --downsample-en 1212 --seed 42
```

**Balanced mode + repeats (effect stability):**

```bash
python step5_tests.py --downsample-en 1212 --repeats 50 --seed 42
```

**What you get:** a clear answer whether EN and PL differ significantly and by how much (plus robustness when sample sizes are matched).

---

## Reports / outputs

Most scripts print results to stdout.
If a script supports `--out`, you can save a single markdown report, e.g.:

```bash
python step4_linguistic_features.py --en --out report_step4_en.md
```

---

## Interpretation (one-liners)

* Step 2: *what is being said* (terms/phrases/topics)
* Step 3: *how often key motifs appear* (bucket coverage + group breakdown)
* Step 4: *how it is written* (style/format features)
* Step 5: *whether EN vs PL differences are statistically real and how strong* (incl. balanced comparison)

---
