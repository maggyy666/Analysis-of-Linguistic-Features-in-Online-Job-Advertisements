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
    lang = lang.lower()
    
    if lang == "pl":
        if train_df is not None:
            if "platform_experience_label" in train_df.columns and train_df["platform_experience_label"].notna().sum() > 0:
                label_col = "platform_experience_label"
            elif "experience_label" in train_df.columns:
                label_col = "experience_label"
            else:
                raise ValueError("PL dataset: neither platform_experience_label nor experience_label found in train split!")
        else:
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
    else:
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
    df = pd.read_csv(path, low_memory=False)

    if "text" not in df.columns:
        df["text"] = (
            df["title"].fillna("").astype(str).str.strip()
            + "\n"
            + df["description_clean"].fillna("").astype(str).str.strip()
        )

    return df


def main(lang: str = "en"):
    lang = lang.lower()
    
    MODEL_OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    train_path = Path("model_output/pl_train.csv" if lang == "pl" else "model_output/en_train.csv")
    val_path = Path("model_output/pl_val.csv" if lang == "pl" else "model_output/en_val.csv")
    test_path = Path("model_output/pl_test.csv" if lang == "pl" else "model_output/en_test.csv")
    
    train_df = load_df(train_path)
    val_df = load_df(val_path)
    test_df = load_df(test_path)
    
    config = get_config(lang, train_df=train_df)
    label_col = config["label_col"]
    
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

    strip_accents = None if lang == "pl" else "unicode"
    min_df = 2 if lang == "pl" else 3

    if lang == "pl":
        word_vectorizer = TfidfVectorizer(
            lowercase=True,
            strip_accents=None,
            min_df=min_df,
            max_df=0.95,
            ngram_range=(1, 2),
            max_features=100_000,
        )
        
        char_vectorizer = TfidfVectorizer(
            lowercase=True,
            strip_accents=None,
            analyzer="char_wb",
            ngram_range=(3, 5),
            min_df=1,
            max_df=0.95,
            max_features=30_000,
        )
        
        clf = Pipeline([
            ("features", FeatureUnion([
                ("word", word_vectorizer),
                ("char", char_vectorizer),
            ])),
            ("svm", LinearSVC(class_weight="balanced")),
        ])
    else:
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

    cv_metrics = {}
    if lang == "pl" and len(X_train) < 500:
        print(f"\n[{lang.upper()}] Running 5-fold CV on train+val for more stable metrics (small dataset)...")
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
    
    clf.fit(X_train, y_train)

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
    
    print(f"\n[{lang.upper()}] Class distribution in train:")
    print(y_train.value_counts().to_string())
    print(f"\n[{lang.upper()}] Class distribution in test:")
    print(y_test.value_counts().to_string())
    
    test_accuracy = accuracy_score(y_test, pred_test)
    test_f1_macro = f1_score(y_test, pred_test, average='macro', zero_division=0)
    test_f1_weighted = f1_score(y_test, pred_test, average='weighted', zero_division=0)
    
    support_per_class = {}
    for label in labels:
        if label in test_report:
            support_per_class[label] = int(test_report[label]['support'])
        else:
            support_per_class[label] = 0
    
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
            per_class_metrics[label] = {
                "precision": 0.0,
                "recall": 0.0,
                "f1": 0.0,
                "support": 0,
            }
    
    confusion_matrix_list = cm.tolist()

    joblib.dump(clf, config["model_out"])
    print(f"\n[{lang.upper()}] Saved model:", config["model_out"])

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
        "confusion_labels": labels,
        "class_distribution": {
            "train": {k: int(v) for k, v in y_train.value_counts().to_dict().items()},
            "test": {k: int(v) for k, v in y_test.value_counts().to_dict().items()},
        },
    }
    
    filters_info = {
        "min_samples_per_class": 5 if lang == "pl" and len(X_train) < 300 else 10,
        "use_confident_only": label_col == "experience_label" if lang == "pl" else False,
    }
    metrics_data["filters"] = filters_info
    
    if cv_metrics:
        metrics_data["cv_metrics"] = cv_metrics
        metrics_data["stability"] = {
            "f1_macro_std": cv_metrics["cv_f1_macro_std"],
            "accuracy_std": cv_metrics["cv_accuracy_std"],
        }
    
    metrics_data["model_info"] = {
        "features": "word+char" if lang == "pl" else "word",
        "vectorizer": "TfidfVectorizer",
        "classifier": "LinearSVC",
        "class_weight": "balanced",
    }
    
    with open(metrics_path, 'w', encoding='utf-8') as f:
        json.dump(metrics_data, f, indent=2, ensure_ascii=False)
    print(f"[{lang.upper()}] Saved metrics:", metrics_path)
    
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
