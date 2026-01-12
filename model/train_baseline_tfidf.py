import pandas as pd
import sys
import json
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.metrics import classification_report, confusion_matrix, f1_score, accuracy_score
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.model_selection import StratifiedKFold, cross_val_score
import numpy as np
import joblib

MODEL_OUT_DIR = Path("model_output")


def get_config(lang: str, train_df: pd.DataFrame = None):
    """
    Get configuration for EN or PL dataset.
    
    For PL: auto-detects label column (prefers platform_experience_label if available).
    """
    lang = lang.lower()
    
    if lang == "pl":
        # Auto-detect label column from train split
        if train_df is not None:
            if "platform_experience_label" in train_df.columns and train_df["platform_experience_label"].notna().sum() > 0:
                label_col = "platform_experience_label"
            elif "experience_label" in train_df.columns:
                label_col = "experience_label"
            else:
                raise ValueError("PL dataset: neither platform_experience_label nor experience_label found in train split!")
        else:
            # Default fallback
            label_col = "experience_label"
        
        return {
            "train": "model_output/pl_train.csv",
            "val": "model_output/pl_val.csv",
            "test": "model_output/pl_test.csv",
            "model_out": MODEL_OUT_DIR / "baseline_tfidf_linearsvc_pl.joblib",
            "pred_val_csv": MODEL_OUT_DIR / "baseline_val_predictions_pl.csv",
            "pred_test_csv": MODEL_OUT_DIR / "baseline_test_predictions_pl.csv",
            "label_col": label_col,
        }
    else:  # EN
        return {
            "train": "model_output/en_train.csv",
            "val": "model_output/en_val.csv",
            "test": "model_output/en_test.csv",
            "model_out": MODEL_OUT_DIR / "baseline_tfidf_linearsvc.joblib",
            "pred_val_csv": MODEL_OUT_DIR / "baseline_val_predictions.csv",
            "pred_test_csv": MODEL_OUT_DIR / "baseline_test_predictions.csv",
            "label_col": "platform_experience_label",
        }


def load_df(path: Path) -> pd.DataFrame:
    """
    Load a split CSV and ensure a `text` column is available.

    The training pipeline expects a single input field that contains the job
    title and cleaned description. If `text` is not present (e.g., an older
    split file), it is reconstructed as: title + newline + description_clean.

    Parameters
    ----------
    path : str
        Path to the CSV file.

    Returns
    -------
    pd.DataFrame
        Loaded dataframe with a guaranteed `text` column.
    """
    df = pd.read_csv(path, low_memory=False)

    # `text` should be present from the split step; rebuild defensively.
    if "text" not in df.columns:
        df["text"] = (
            df["title"].fillna("").astype(str).str.strip()
            + "\n"
            + df["description_clean"].fillna("").astype(str).str.strip()
        )

    return df


def main(lang: str = "en"):
    """
    Train and evaluate a TF-IDF + Linear SVM baseline for experience-level classification.

    The model is trained on the train split and evaluated on validation and test splits.
    For EN: uses `platform_experience_label` as the target.
    For PL: uses `experience_label` as the target.
    
    Reports include per-class precision/recall/F1 and a test confusion matrix.

    The fitted pipeline (TF-IDF vectorizer + LinearSVC) is serialized with joblib
    to `model_output/` directory. Predictions are also saved as CSV files.

    Parameters
    ----------
    lang : str
        Language: "en" or "pl"
    """
    lang = lang.lower()
    
    # Create output directory if it doesn't exist
    MODEL_OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load splits first to determine label column for PL
    train_path = Path("model_output/pl_train.csv" if lang == "pl" else "model_output/en_train.csv")
    val_path = Path("model_output/pl_val.csv" if lang == "pl" else "model_output/en_val.csv")
    test_path = Path("model_output/pl_test.csv" if lang == "pl" else "model_output/en_test.csv")
    
    train_df = load_df(train_path)
    val_df = load_df(val_path)
    test_df = load_df(test_path)
    
    # Get config (for PL, this will auto-detect label column from train_df)
    config = get_config(lang, train_df=train_df)
    label_col = config["label_col"]
    
    # Validate label column in all splits
    for name, d in [("train", train_df), ("val", val_df), ("test", test_df)]:
        if label_col not in d.columns:
            raise ValueError(f"ERROR: Column '{label_col}' missing in {name} split!")
        if d[label_col].notna().sum() == 0:
            raise ValueError(f"ERROR: Column '{label_col}' has no non-null values in {name} split!")

    X_train = train_df["text"].fillna("").astype(str)
    y_train = train_df[label_col].astype(str)

    X_val = val_df["text"].fillna("").astype(str)
    y_val = val_df[label_col].astype(str)

    X_test = test_df["text"].fillna("").astype(str)
    y_test = test_df[label_col].astype(str)

    # For PL, don't strip accents (preserve Polish characters)
    strip_accents = None if lang == "pl" else "unicode"
    min_df = 2 if lang == "pl" else 3  # Lower threshold for PL due to smaller dataset

    # Build feature extractor
    if lang == "pl":
        # For PL: use word + char n-grams (helps with inflectional languages)
        word_vectorizer = TfidfVectorizer(
            lowercase=True,
            strip_accents=None,
            min_df=min_df,
            max_df=0.95,
            ngram_range=(1, 2),
            max_features=100_000,  # Reduced for smaller PL dataset
        )
        
        char_vectorizer = TfidfVectorizer(
            lowercase=True,
            strip_accents=None,
            analyzer="char_wb",  # character n-grams with word boundaries
            ngram_range=(3, 5),
            min_df=1,  # Lower threshold for char n-grams (can help with small datasets)
            max_df=0.95,
            max_features=30_000,  # Reduced for smaller dataset
        )
        
        clf = Pipeline([
            ("features", FeatureUnion([
                ("word", word_vectorizer),
                ("char", char_vectorizer),
            ])),
            ("svm", LinearSVC(class_weight="balanced")),
        ])
    else:
        # For EN: word n-grams only
        clf = Pipeline([
            ("tfidf", TfidfVectorizer(
                lowercase=True,
                strip_accents=strip_accents,
                min_df=min_df,
                max_df=0.95,
                ngram_range=(1, 2),
                max_features=300_000,
            )),
            ("svm", LinearSVC(class_weight="balanced")),
        ])

    # For PL with small dataset, run K-Fold CV on train+val for more stable metrics
    # Do this BEFORE training final model to avoid double training
    cv_metrics = {}
    if lang == "pl" and len(X_train) < 500:
        print(f"\n[{lang.upper()}] Running 5-fold CV on train+val for more stable metrics (small dataset)...")
        # Combine train and val for CV
        X_cv = pd.concat([X_train, X_val], ignore_index=True)
        y_cv = pd.concat([y_train, y_val], ignore_index=True)
        
        skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        cv_f1_macro = cross_val_score(clf, X_cv, y_cv, cv=skf, scoring='f1_macro')
        cv_f1_weighted = cross_val_score(clf, X_cv, y_cv, cv=skf, scoring='f1_weighted')
        cv_acc = cross_val_score(clf, X_cv, y_cv, cv=skf, scoring='accuracy')
        
        cv_metrics = {
            "cv_f1_macro_mean": float(cv_f1_macro.mean()),
            "cv_f1_macro_std": float(cv_f1_macro.std()),
            "cv_f1_weighted_mean": float(cv_f1_weighted.mean()),
            "cv_f1_weighted_std": float(cv_f1_weighted.std()),
            "cv_accuracy_mean": float(cv_acc.mean()),
            "cv_accuracy_std": float(cv_acc.std()),
        }
        
        print(f"5-fold CV Macro F1: {cv_metrics['cv_f1_macro_mean']:.4f} (+/- {cv_metrics['cv_f1_macro_std'] * 2:.4f})")
        print(f"5-fold CV Weighted F1: {cv_metrics['cv_f1_weighted_mean']:.4f} (+/- {cv_metrics['cv_f1_weighted_std'] * 2:.4f})")
        print(f"5-fold CV Accuracy: {cv_metrics['cv_accuracy_mean']:.4f} (+/- {cv_metrics['cv_accuracy_std'] * 2:.4f})")
    
    # Train final model on train split
    clf.fit(X_train, y_train)

    # Validation metrics.
    pred_val = clf.predict(X_val)
    val_report = classification_report(y_val, pred_val, digits=4, output_dict=True)
    print(f"\n[{lang.upper()}] === VALIDATION ===")
    print(classification_report(y_val, pred_val, digits=4))

    # Test metrics.
    pred_test = clf.predict(X_test)
    test_report = classification_report(y_test, pred_test, digits=4, output_dict=True)
    print(f"\n[{lang.upper()}] === TEST ===")
    print(classification_report(y_test, pred_test, digits=4))

    labels = sorted(y_train.unique().tolist())
    cm = confusion_matrix(y_test, pred_test, labels=labels)
    print(f"\n[{lang.upper()}] Labels order:", labels)
    print("Confusion matrix (rows=true, cols=pred):")
    print(cm)
    
    # Print class distribution for context
    print(f"\n[{lang.upper()}] Class distribution in train:")
    print(y_train.value_counts().to_string())
    print(f"\n[{lang.upper()}] Class distribution in test:")
    print(y_test.value_counts().to_string())
    
    # Calculate summary metrics
    test_accuracy = accuracy_score(y_test, pred_test)
    test_f1_macro = f1_score(y_test, pred_test, average='macro', zero_division=0)
    test_f1_weighted = f1_score(y_test, pred_test, average='weighted', zero_division=0)
    
    # Support per class (handle classes that might not appear in test_report)
    support_per_class = {}
    for label in labels:
        if label in test_report:
            support_per_class[label] = int(test_report[label]['support'])
        else:
            # Class not in test set
            support_per_class[label] = 0
    
    # Per-class metrics (precision, recall, f1)
    per_class_metrics = {}
    for label in labels:
        if label in test_report:
            per_class_metrics[label] = {
                "precision": float(test_report[label].get('precision', 0)),
                "recall": float(test_report[label].get('recall', 0)),
                "f1": float(test_report[label].get('f1-score', 0)),
                "support": int(test_report[label].get('support', 0)),
            }
        else:
            # Class not in test set
            per_class_metrics[label] = {
                "precision": 0.0,
                "recall": 0.0,
                "f1": 0.0,
                "support": 0,
            }
    
    # Confusion matrix as nested list (for JSON serialization)
    confusion_matrix_list = cm.tolist()

    # Save model
    joblib.dump(clf, config["model_out"])
    print(f"\n[{lang.upper()}] Saved model:", config["model_out"])

    # Save predictions to CSV (handle missing columns gracefully)
    def safe_cols(df, cols):
        result = pd.DataFrame()
        for col in cols:
            if col in df.columns:
                result[col] = df[col]
            else:
                result[col] = ""
        return result

    val_pred_df = safe_cols(val_df, ["job_id", "title", "company_name"]).copy()
    val_pred_df["true_label"] = y_val.values
    val_pred_df["predicted_label"] = pred_val
    val_pred_df["correct"] = (val_pred_df["true_label"] == val_pred_df["predicted_label"])
    val_pred_df.to_csv(config["pred_val_csv"], index=False, encoding="utf-8")
    print(f"[{lang.upper()}] Saved validation predictions:", config["pred_val_csv"])

    test_pred_df = safe_cols(test_df, ["job_id", "title", "company_name"]).copy()
    test_pred_df["true_label"] = y_test.values
    test_pred_df["predicted_label"] = pred_test
    test_pred_df["correct"] = (test_pred_df["true_label"] == test_pred_df["predicted_label"])
    test_pred_df.to_csv(config["pred_test_csv"], index=False, encoding="utf-8")
    print(f"[{lang.upper()}] Saved test predictions:", config["pred_test_csv"])
    
    # Save metrics to JSON for GUI
    metrics_path = MODEL_OUT_DIR / f"metrics_{lang}.json"
    metrics_data = {
        "lang": lang.upper(),
        "label_col": label_col,
        "n_train": len(X_train),
        "n_val": len(X_val),
        "n_test": len(X_test),
        "n_classes": len(labels),
        "labels": labels,
        "test_metrics": {
            "accuracy": float(test_accuracy),
            "f1_macro": float(test_f1_macro),
            "f1_weighted": float(test_f1_weighted),
        },
        "val_metrics": {
            "accuracy": float(val_report.get('accuracy', 0)),
            "f1_macro": float(val_report.get('macro avg', {}).get('f1-score', 0)),
            "f1_weighted": float(val_report.get('weighted avg', {}).get('f1-score', 0)),
        },
        "support_per_class": support_per_class,
        "per_class_metrics": per_class_metrics,
        "confusion_matrix": confusion_matrix_list,
        "confusion_labels": labels,  # Order of labels in confusion matrix
        "class_distribution": {
            "train": {k: int(v) for k, v in y_train.value_counts().to_dict().items()},
            "test": {k: int(v) for k, v in y_test.value_counts().to_dict().items()},
        },
    }
    
    # Add filter info (inferred from dataset characteristics)
    # Note: Exact dropped classes would need to be passed from make_train_split, but we can infer thresholds
    # For PL with experience_label, use_confident_only is typically True, but we can't detect this reliably here
    filters_info = {
        "min_samples_per_class": 5 if lang == "pl" and len(X_train) < 300 else 10,
        "use_confident_only": label_col == "experience_label" if lang == "pl" else False,
    }
    metrics_data["filters"] = filters_info
    
    # Add CV metrics if available
    if cv_metrics:
        metrics_data["cv_metrics"] = cv_metrics
        # Calculate stability score (lower std = more stable)
        metrics_data["stability"] = {
            "f1_macro_std": cv_metrics["cv_f1_macro_std"],
            "accuracy_std": cv_metrics["cv_accuracy_std"],
        }
    
    # Add model info
    metrics_data["model_info"] = {
        "features": "word+char" if lang == "pl" else "word",
        "vectorizer": "TfidfVectorizer",
        "classifier": "LinearSVC",
        "class_weight": "balanced",
    }
    
    with open(metrics_path, 'w', encoding='utf-8') as f:
        json.dump(metrics_data, f, indent=2, ensure_ascii=False)
    print(f"[{lang.upper()}] Saved metrics:", metrics_path)
    
    # Save confusion matrix to CSV for GUI
    cm_path = MODEL_OUT_DIR / f"confusion_{lang}.csv"
    cm_df = pd.DataFrame(cm, index=labels, columns=labels)
    cm_df.index.name = "true_label"
    cm_df.to_csv(cm_path, encoding='utf-8')
    print(f"[{lang.upper()}] Saved confusion matrix:", cm_path)


if __name__ == "__main__":
    lang = (sys.argv[1] if len(sys.argv) > 1 else "en").lower()
    if lang not in ["en", "pl"]:
        print("Usage: python train_baseline_tfidf.py [en|pl]")
        sys.exit(1)
    main(lang)
