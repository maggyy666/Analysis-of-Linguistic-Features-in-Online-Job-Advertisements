import pandas as pd
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.svm import LinearSVC
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.pipeline import Pipeline
import joblib

TRAIN = "model_output/en_train.csv"
VAL = "model_output/en_val.csv"
TEST = "model_output/en_test.csv"

MODEL_OUT_DIR = Path("model_output")
MODEL_OUT = MODEL_OUT_DIR / "baseline_tfidf_linearsvc.joblib"
PRED_VAL_CSV = MODEL_OUT_DIR / "baseline_val_predictions.csv"
PRED_TEST_CSV = MODEL_OUT_DIR / "baseline_test_predictions.csv"


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


def main():
    """
    Train and evaluate a TF-IDF + Linear SVM baseline for experience-level classification.

    The model is trained on the train split and evaluated on validation and test splits
    using `platform_experience_label` as the target (dataset-provided silver labels).
    Reports include per-class precision/recall/F1 and a test confusion matrix.

    The fitted pipeline (TF-IDF vectorizer + LinearSVC) is serialized with joblib
    to `model_output/` directory. Predictions are also saved as CSV files.
    """
    # Create output directory if it doesn't exist
    MODEL_OUT_DIR.mkdir(parents=True, exist_ok=True)
    
    train_df = load_df(TRAIN)
    val_df = load_df(VAL)
    test_df = load_df(TEST)

    X_train = train_df["text"].fillna("").astype(str)
    y_train = train_df["platform_experience_label"].astype(str)

    X_val = val_df["text"].fillna("").astype(str)
    y_val = val_df["platform_experience_label"].astype(str)

    X_test = test_df["text"].fillna("").astype(str)
    y_test = test_df["platform_experience_label"].astype(str)

    clf = Pipeline([
        ("tfidf", TfidfVectorizer(
            lowercase=True,
            strip_accents="unicode",
            min_df=3,
            max_df=0.95,
            ngram_range=(1, 2),
            max_features=300_000,
        )),
        ("svm", LinearSVC(class_weight="balanced")),
    ])

    clf.fit(X_train, y_train)

    # Validation metrics.
    pred_val = clf.predict(X_val)
    print("=== VALIDATION ===")
    print(classification_report(y_val, pred_val, digits=4))

    # Test metrics.
    pred_test = clf.predict(X_test)
    print("\n=== TEST ===")
    print(classification_report(y_test, pred_test, digits=4))

    labels = sorted(y_train.unique().tolist())
    cm = confusion_matrix(y_test, pred_test, labels=labels)
    print("\nLabels order:", labels)
    print("Confusion matrix (rows=true, cols=pred):")
    print(cm)

    # Save model
    joblib.dump(clf, MODEL_OUT)
    print("\nSaved model:", MODEL_OUT)

    # Save predictions to CSV
    val_pred_df = val_df[["job_id", "title", "company_name"]].copy()
    val_pred_df["true_label"] = y_val
    val_pred_df["predicted_label"] = pred_val
    val_pred_df["correct"] = (y_val == pred_val)
    val_pred_df.to_csv(PRED_VAL_CSV, index=False, encoding="utf-8")
    print("Saved validation predictions:", PRED_VAL_CSV)

    test_pred_df = test_df[["job_id", "title", "company_name"]].copy()
    test_pred_df["true_label"] = y_test
    test_pred_df["predicted_label"] = pred_test
    test_pred_df["correct"] = (y_test == pred_test)
    test_pred_df.to_csv(PRED_TEST_CSV, index=False, encoding="utf-8")
    print("Saved test predictions:", PRED_TEST_CSV)


if __name__ == "__main__":
    main()
