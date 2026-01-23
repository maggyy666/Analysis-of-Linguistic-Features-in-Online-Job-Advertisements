import pandas as pd
import sys
from sklearn.model_selection import train_test_split
from pathlib import Path

RANDOM_STATE = 42


def get_config(lang: str, df: pd.DataFrame = None):
    lang = lang.lower()
    
    if lang == "pl":
        if df is not None:
            n_platform = df["platform_experience_label"].notna().sum() if "platform_experience_label" in df.columns else 0
            n_inferred = df["experience_label"].notna().sum() if "experience_label" in df.columns else 0
            
            if n_platform > 0 and (n_platform >= 0.5 * n_inferred or n_inferred == 0):
                label_col = "platform_experience_label"
                use_confident_only = False
                print(f"[PL] Using platform_experience_label ({n_platform} non-null, {n_inferred} in experience_label)")
            elif n_inferred > 0:
                label_col = "experience_label"
                use_confident_only = True
                print(f"[PL] Using experience_label ({n_inferred} non-null, {n_platform} in platform_experience_label)")
            else:
                raise ValueError("PL dataset: neither platform_experience_label nor experience_label has data!")
        else:
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
    else:
        return {
            "input": "en_dataset/en_jobs_clean.csv",
            "out_train": "model_output/en_train.csv",
            "out_val": "model_output/en_val.csv",
            "out_test": "model_output/en_test.csv",
            "label_col": "platform_experience_label",
            "use_confident_only": False,
        }


def main(lang: str = "en"):
    lang = lang.lower()
    
    input_path = Path("pl_dataset/pl_jobs_clean.csv" if lang == "pl" else "en_dataset/en_jobs_clean.csv")
    df = pd.read_csv(input_path, low_memory=False)
    
    config = get_config(lang, df=df)
    
    Path(config["out_train"]).parent.mkdir(parents=True, exist_ok=True)
    Path(config["out_val"]).parent.mkdir(parents=True, exist_ok=True)
    Path(config["out_test"]).parent.mkdir(parents=True, exist_ok=True)

    label_col = config["label_col"]
    if label_col not in df.columns:
        print(f"ERROR: Column '{label_col}' not found in dataset!")
        return
    
    df = df[df[label_col].notna()].copy()
    
    if "is_confident" in df.columns:
        df["is_confident"] = df["is_confident"].astype(str).str.lower().isin(["true", "1", "yes"])
    
    if config["use_confident_only"] and "is_confident" in df.columns:
        before = len(df)
        df = df[df["is_confident"]].copy()
        print(f"Filtered to confident labels only: {len(df)} rows (from {before})")
    
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
    
    filter_info = {
        "use_confident_only": config["use_confident_only"],
        "min_samples_per_class": min_samples_per_class,
        "dropped_classes": dropped_classes,
        "n_rows_before_filters": len(df) + (len(df[df[label_col].isin(rare)]) if len(rare) > 0 else 0),
    }

    def years_bucket(x):
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
    
    if "years_hint" in df.columns:
        df["years_bucket"] = df["years_hint"].apply(years_bucket)
    else:
        df["years_bucket"] = "YEARS_NONE"
    
    df["text"] = (
        df["title"].fillna("").astype(str).str.strip()
        + "\n"
        + df["description_clean"].fillna("").astype(str).str.strip()
        + "\n"
        + df["years_bucket"].astype(str)
    )

    y = df[label_col].astype(str)

    train_df, temp_df = train_test_split(
        df, test_size=0.20, random_state=RANDOM_STATE, stratify=y
    )

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
