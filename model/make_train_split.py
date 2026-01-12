import pandas as pd
import sys
from sklearn.model_selection import train_test_split
from pathlib import Path

RANDOM_STATE = 42


def get_config(lang: str, df: pd.DataFrame = None):
    """
    Get configuration for EN or PL dataset.
    
    For PL: chooses label column with more non-null values.
    Prefers platform_experience_label if it has at least 50% of experience_label coverage.
    """
    lang = lang.lower()
    
    if lang == "pl":
        # Choose label column based on coverage
        if df is not None:
            n_platform = df["platform_experience_label"].notna().sum() if "platform_experience_label" in df.columns else 0
            n_inferred = df["experience_label"].notna().sum() if "experience_label" in df.columns else 0
            
            # Prefer platform_experience_label if it has substantial coverage (>= 50% of inferred)
            # or if it's the only one available
            if n_platform > 0 and (n_platform >= 0.5 * n_inferred or n_inferred == 0):
                label_col = "platform_experience_label"
                use_confident_only = False
                print(f"[PL] Using platform_experience_label ({n_platform} non-null, {n_inferred} in experience_label)")
            elif n_inferred > 0:
                label_col = "experience_label"
                use_confident_only = True  # Use confident only for inferred labels
                print(f"[PL] Using experience_label ({n_inferred} non-null, {n_platform} in platform_experience_label)")
            else:
                raise ValueError("PL dataset: neither platform_experience_label nor experience_label has data!")
        else:
            # Default fallback
            label_col = "experience_label"
            use_confident_only = True
        
        return {
            "input": "pl_dataset/pl_jobs_clean.csv",
            "out_train": "model_output/pl_train.csv",
            "out_val": "model_output/pl_val.csv",
            "out_test": "model_output/pl_test.csv",
            "label_col": label_col,
            "use_confident_only": use_confident_only,
        }
    else:  # EN
        return {
            "input": "en_dataset/en_jobs_clean.csv",
            "out_train": "model_output/en_train.csv",
            "out_val": "model_output/en_val.csv",
            "out_test": "model_output/en_test.csv",
            "label_col": "platform_experience_label",
            "use_confident_only": False,
        }


def main(lang: str = "en"):
    """
    Create a stratified train/validation/test split from the cleaned dataset.

    The split is built only from rows that contain the label column
    (platform_experience_label for EN, experience_label for PL).
    A single `text` feature is created by concatenating `title` and `description_clean`
    with a newline separator.

    Output files:
        - {lang}_train.csv: 80%
        - {lang}_val.csv:   10%
        - {lang}_test.csv:  10%

    Stratification is performed on the label column to preserve the
    class distribution across all splits.

    Parameters
    ----------
    lang : str
        Language: "en" or "pl"
    """
    lang = lang.lower()
    
    # Load dataset first to determine best label column for PL
    input_path = Path("pl_dataset/pl_jobs_clean.csv" if lang == "pl" else "en_dataset/en_jobs_clean.csv")
    df = pd.read_csv(input_path, low_memory=False)
    
    # Get config (for PL, this will check df and choose best label)
    config = get_config(lang, df=df)
    
    Path(config["out_train"]).parent.mkdir(parents=True, exist_ok=True)
    Path(config["out_val"]).parent.mkdir(parents=True, exist_ok=True)
    Path(config["out_test"]).parent.mkdir(parents=True, exist_ok=True)

    # Keep only rows with labels
    label_col = config["label_col"]
    if label_col not in df.columns:
        print(f"ERROR: Column '{label_col}' not found in dataset!")
        return
    
    df = df[df[label_col].notna()].copy()
    
    # Normalize is_confident (handle string/boolean/int types)
    if "is_confident" in df.columns:
        df["is_confident"] = df["is_confident"].astype(str).str.lower().isin(["true", "1", "yes"])
    
    # Optional: filter by is_confident for PL (when using inferred labels)
    if config["use_confident_only"] and "is_confident" in df.columns:
        before = len(df)
        df = df[df["is_confident"]].copy()
        print(f"Filtered to confident labels only: {len(df)} rows (from {before})")
    
    # Drop rare classes to avoid stratified split crash
    # For small datasets (PL), use lower threshold
    min_samples_per_class = 5 if lang == "pl" and len(df) < 300 else 10
    counts = df[label_col].value_counts()
    rare = counts[counts < min_samples_per_class].index
    dropped_classes = []
    if len(rare) > 0:
        dropped_classes = list(rare)
        print(f"WARNING: dropping rare classes (<{min_samples_per_class} samples): {dropped_classes}")
        print(f"  Class counts before filtering: {counts.to_dict()}")
        df = df[~df[label_col].isin(rare)].copy()
        print(f"  Rows after filtering: {len(df)}")
    
    # Store filter info for later use (will be saved in split metadata if needed)
    filter_info = {
        "use_confident_only": config["use_confident_only"],
        "min_samples_per_class": min_samples_per_class,
        "dropped_classes": dropped_classes,
        "n_rows_before_filters": len(df) + (len(df[df[label_col].isin(rare)]) if len(rare) > 0 else 0),
    }

    # Years bucket helper function
    def years_bucket(x):
        """Convert years_hint to bucket token."""
        if pd.isna(x):
            return "YEARS_NONE"
        try:
            x = float(x)
            if x <= 1:
                return "YEARS_0_1"
            elif x <= 3:
                return "YEARS_2_3"
            elif x <= 5:
                return "YEARS_4_5"
            elif x <= 8:
                return "YEARS_6_8"
            else:
                return "YEARS_9_PLUS"
        except (ValueError, TypeError):
            return "YEARS_NONE"
    
    # Add years bucket if years_hint column exists
    if "years_hint" in df.columns:
        df["years_bucket"] = df["years_hint"].apply(years_bucket)
    else:
        df["years_bucket"] = "YEARS_NONE"
    
    # Model input text: title + description + years_bucket (as token).
    df["text"] = (
        df["title"].fillna("").astype(str).str.strip()
        + "\n"
        + df["description_clean"].fillna("").astype(str).str.strip()
        + "\n"
        + df["years_bucket"].astype(str)
    )

    y = df[label_col].astype(str)

    # 80% train, 20% temporary pool (later split into val/test).
    train_df, temp_df = train_test_split(
        df, test_size=0.20, random_state=RANDOM_STATE, stratify=y
    )

    # Split the temporary pool evenly into validation and test (10%/10%).
    y_temp = temp_df[label_col].astype(str)
    val_df, test_df = train_test_split(
        temp_df, test_size=0.50, random_state=RANDOM_STATE, stratify=y_temp
    )

    train_df.to_csv(config["out_train"], index=False, encoding="utf-8")
    val_df.to_csv(config["out_val"], index=False, encoding="utf-8")
    test_df.to_csv(config["out_test"], index=False, encoding="utf-8")

    print(f"\n[{lang.upper()}] Saved:")
    print(" -", config["out_train"], len(train_df))
    print(" -", config["out_val"], len(val_df))
    print(" -", config["out_test"], len(test_df))

    print(f"\n[{lang.upper()}] Class distribution (train):")
    print(train_df[label_col].value_counts(normalize=True).round(3))


if __name__ == "__main__":
    lang = (sys.argv[1] if len(sys.argv) > 1 else "en").lower()
    if lang not in ["en", "pl"]:
        print("Usage: python make_train_split.py [en|pl]")
        sys.exit(1)
    main(lang)
