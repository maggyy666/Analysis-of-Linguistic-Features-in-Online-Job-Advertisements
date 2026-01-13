
# Job Posting Analysis Pipeline (PyQt5)

A desktop (PyQt5) application that runs an end-to-end pipeline for job posting datasets:
**data cleaning → train/val/test split → baseline model training → evaluation dashboards → interactive “Try it out” predictions**.

The app supports **EN (English)** and **PL (Polish)** workflows and stores all artifacts (splits, models, metrics, predictions) under `model_output/`.

---

## Features

### 1) Data Processing & Model Training (GUI)
- Run dataset cleaning scripts for **EN** and **PL**
- Create **stratified** train/val/test split
- Train a baseline model (TF-IDF + LinearSVC)
- View logs directly inside the GUI (runs scripts in a background thread to keep UI responsive)

### 2) Try It Out (Interactive Predictor)
- Select **EN/PL model** from dropdown (loads the correct `.joblib`)
- Paste a job title + description and predict experience level
- Shows **Top-K decision scores** (LinearSVC decision function)
- Loads curated **test prompts** from JSON (`test_prompts/en_tests.json`, `test_prompts/pl_tests.json`)

> Note: “Top 3 Predictions (decision scores)” are **not probabilities**. These are SVM margins (can be negative).

### 3) Dataset Statistics Dashboard
- Loads cleaned datasets and visualizes:
  - missingness by column
  - class distribution
  - text length distribution
  - work type / remote allowed distribution
  - salary distribution (where available)

### 4) Model Performance Dashboard
- Loads saved `metrics_*.json` + prediction CSVs and visualizes:
  - Test/Val metrics (Accuracy, Macro F1, Weighted F1)
  - Confusion matrix (seaborn if installed; otherwise matplotlib)
  - Per-class F1/Recall with support counts
  - CV stability metrics (if present)
  - Error table with filters (All / Errors / Correct)

---

## Project Structure (expected)

```

.
├─ app.py
├─ data_processing/
│  ├─ clean_en_dataset.py
│  └─ clean_pl_dataset.py
├─ en_dataset/
│  └─ en_jobs_clean.csv              # produced by cleaning
├─ pl_dataset/
│  └─ pl_jobs_clean.csv              # produced by cleaning
├─ model/
│  ├─ make_train_split.py            # creates train/val/test CSVs
│  └─ train_baseline_tfidf.py         # trains baseline + saves metrics
├─ model_output/
│  ├─ en_train.csv / en_val.csv / en_test.csv
│  ├─ pl_train.csv / pl_val.csv / pl_test.csv
│  ├─ baseline_tfidf_linearsvc.joblib
│  ├─ baseline_tfidf_linearsvc_pl.joblib
│  ├─ baseline_val_predictions.csv
│  ├─ baseline_test_predictions.csv
│  ├─ baseline_val_predictions_pl.csv
│  ├─ baseline_test_predictions_pl.csv
│  ├─ metrics_en.json / metrics_pl.json
│  └─ confusion_en.csv / confusion_pl.csv
└─ test_prompts/
├─ en_tests.json
└─ pl_tests.json

````

---

## Requirements

- Python 3.10+ recommended
- PyQt5
- scikit-learn
- pandas, numpy
- matplotlib
- joblib
- seaborn (optional, improves confusion matrix plots)

Example install (adjust to your environment):
```bash
pip install pyqt5 scikit-learn pandas numpy matplotlib joblib seaborn
````

---

## Running the App

From the repository root:

```bash
python app.py
```

You’ll get a GUI with three main views:

* **Main** (Data Processing & Model Training)
* **Try it out**
* **Dataset Statistics**

---

## Typical Workflow (EN / PL)

### Step 1 — Clean dataset

In **Main → Data Processing**:

* Click **Clean EN Dataset** or **Clean PL Dataset**

This is expected to produce:

* `en_dataset/en_jobs_clean.csv`
* `pl_dataset/pl_jobs_clean.csv`

### Step 2 — Create train/val/test split

In **Main → Model Training → Train Split**:

* Choose language (EN/PL)
* Click **Create Train/Val/Test Split**

Outputs:

* `model_output/{lang}_train.csv`
* `model_output/{lang}_val.csv`
* `model_output/{lang}_test.csv`

### Step 3 — Train baseline model

In **Main → Model Training → Train Baseline Model**:

* Choose language (EN/PL)
* Click **Train Baseline Model**

Outputs:

* `.joblib` model file in `model_output/`
* prediction CSVs for val/test
* `metrics_{lang}.json` and `confusion_{lang}.csv`

### Step 4 — Inspect performance

In **Main → Model Performance**:

* Switch language (EN/PL)
* Review metrics, confusion matrix, per-class plots, error table

### Step 5 — Try predictions

In **Try it out**:

* Choose language (EN/PL)
* Pick a test case from dropdown (auto-fills inputs), or paste your own text
* Click **Predict Experience Level**

---

## Test Prompts Format

The GUI loads test prompts from:

* `test_prompts/en_tests.json`
* `test_prompts/pl_tests.json`

Expected JSON format: an **array of objects**:

```json
[
  {
    "id": "EN_03_years_3_associate",
    "expected": "mid",
    "title": "Backend Engineer",
    "description": "3+ years of experience. Build REST APIs, write tests, work with CI/CD."
  }
]
```

In the GUI dropdown, each case displays as:
`<id> (expected: <expected>)`

---

## How the Baseline Works (high level)

### Training data

The model training pipeline expects a single `text` feature. Split scripts typically build it as:

* `title + "\n" + description_clean`

### Models

* **EN baseline**: word TF-IDF (1–2 grams) + LinearSVC (`class_weight="balanced"`)
* **PL baseline**: word TF-IDF + char n-grams (useful for inflection) + LinearSVC (`class_weight="balanced"`)

### Saved artifacts

* Model: `model_output/baseline_tfidf_linearsvc*.joblib`
* Predictions: `baseline_*_predictions*.csv`
* Metrics for GUI: `metrics_en.json`, `metrics_pl.json`
* Confusion matrices for GUI: `confusion_en.csv`, `confusion_pl.csv`

---

## Important Notes / Known Caveats

1. **Decision scores are not probabilities**
   The “Top 3 Predictions” shown in the UI are `LinearSVC.decision_function` values (margins). They can be negative and should not be displayed as `%`.

2. **Keep inference text consistent with training**
   In `TryItOutView.predict()`, the app currently builds input as:
   `title + "\n" + description + "\n" + YEARS_BUCKET`

If your training data **does not include** that bucket token in `text`, predictions can degrade.
Best practice: ensure the training pipeline and inference pipeline build `text` in the **same format**.

3. **Small PL dataset = unstable metrics**
   For very small PL training sizes, macro-F1 can swing a lot and minority classes may be dropped by the splitter (rare-class filtering).

4. **Dataset upload in the UI**
   The “Upload Dataset” section currently selects a file path for convenience, but cleaning scripts are executed from fixed paths (e.g., `en_dataset/`, `pl_dataset/`). If you want true upload/import, wire the selected file into the cleaning stage.

---

## CLI (optional)

You can run the pipeline without the GUI:

```bash
# Clean datasets
python data_processing/clean_en_dataset.py
python data_processing/clean_pl_dataset.py

# Create splits
python model/make_train_split.py en
python model/make_train_split.py pl

# Train baseline
python model/train_baseline_tfidf.py en
python model/train_baseline_tfidf.py pl
```

---

## Troubleshooting

* **“Script Not Found”**
  Ensure `data_processing/` and `model/` scripts exist at the expected paths.

* **Model not loading in “Try it out”**
  Train the baseline first, or place a compatible `.joblib` at:

  * `model_output/baseline_tfidf_linearsvc.joblib` (EN)
  * `model_output/baseline_tfidf_linearsvc_pl.joblib` (PL)

* **Confusion matrix plot looks basic**
  Install seaborn:

  ```bash
  pip install seaborn
  ```

* **Weird predictions for obvious “2 years” cases**
  Verify that inference text formatting matches training text formatting (see “Known Caveats”).


dataset link: https://www.kaggle.com/datasets/arshkon/linkedin-job-postings?resource=download

create folder en_dataset -> change name to "en_postings.csv"
---
