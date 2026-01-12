import pandas as pd
from sklearn.model_selection import train_test_split
from pathlib import Path
INPUT = "en_dataset/en_jobs_clean.csv"
OUT_TRAIN = "model_output/en_train.csv"
OUT_VAL = "model_output/en_val.csv"
OUT_TEST = "model_output/en_test.csv"

RANDOM_STATE = 42


def main():
    """
    Create a stratified train/validation/test split from the cleaned EN dataset.

    The split is built only from rows that contain `platform_experience_label`
    (the dataset-provided experience level). A single `text` feature is created
    by concatenating `title` and `description_clean` with a newline separator.

    Output files:
        - en_train.csv: 80%
        - en_val.csv:   10%
        - en_test.csv:  10%

    Stratification is performed on `platform_experience_label` to preserve the
    class distribution across all splits.
    """
    df = pd.read_csv(Path(INPUT), low_memory=False)

    Path(OUT_TRAIN).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_VAL).parent.mkdir(parents=True, exist_ok=True)
    Path(OUT_TEST).parent.mkdir(parents=True, exist_ok=True)

    # Keep only rows with platform-provided labels (used as silver supervision).
    df = df[df["platform_experience_label"].notna()].copy()

    # Model input text: title + description.
    df["text"] = (
        df["title"].fillna("").astype(str).str.strip()
        + "\n"
        + df["description_clean"].fillna("").astype(str).str.strip()
    )

    y = df["platform_experience_label"].astype(str)

    # 80% train, 20% temporary pool (later split into val/test).
    train_df, temp_df = train_test_split(
        df, test_size=0.20, random_state=RANDOM_STATE, stratify=y
    )

    # Split the temporary pool evenly into validation and test (10%/10%).
    y_temp = temp_df["platform_experience_label"].astype(str)
    val_df, test_df = train_test_split(
        temp_df, test_size=0.50, random_state=RANDOM_STATE, stratify=y_temp
    )

    train_df.to_csv(OUT_TRAIN, index=False, encoding="utf-8")
    val_df.to_csv(OUT_VAL, index=False, encoding="utf-8")
    test_df.to_csv(OUT_TEST, index=False, encoding="utf-8")

    print("Saved:")
    print(" -", OUT_TRAIN, len(train_df))
    print(" -", OUT_VAL, len(val_df))
    print(" -", OUT_TEST, len(test_df))

    print("\nClass distribution (train):")
    print(train_df["platform_experience_label"].value_counts(normalize=True).round(3))


if __name__ == "__main__":
    main()
