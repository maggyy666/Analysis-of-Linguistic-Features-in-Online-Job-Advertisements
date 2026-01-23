import re
import numpy as np
import pandas as pd

INPUT_CSV = "en_dataset/en_postings.csv"
OUTPUT_CSV = "en_dataset/en_jobs_clean.csv"

PAY_PERIOD_TO_ANNUAL = {
    "HOURLY": 2080,
    "WEEKLY": 52,
    "BIWEEKLY": 26,
    "MONTHLY": 12,
    "YEARLY": 1,
    "DAILY": 260,
}

MIN_DESC_LEN = 50
MAX_DESC_LEN = 20000

ENABLE_ROLE_DEFAULTS = False

NUM_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "eleven": 11, "twelve": 12,
}
NUM_WORDS_PATTERN = re.compile(r"\b(" + "|".join(NUM_WORDS.keys()) + r")\b", flags=re.I)


def clean_text(s):
    if not isinstance(s, str):
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"http\S+|www\.\S+", " ", s)
    s = re.sub(r"\S+@\S+\.\S+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _replace_number_words(text: str) -> str:
    if not isinstance(text, str) or not text:
        return ""

    def repl(m):
        return str(NUM_WORDS[m.group(1).lower()])

    return NUM_WORDS_PATTERN.sub(repl, text)


def _months_to_years(months: float) -> float:
    return float(months) / 12.0


def years_hint(text):
    if not isinstance(text, str) or not text:
        return np.nan

    t = _replace_number_words(text)

    m = re.search(r"\b(\d+)\s*(?:[-–]|to)\s*(\d+)\s*(years?|yrs?)\b", t, flags=re.I)
    if m:
        return float(m.group(2))

    m = re.search(r"\bat least\s+(\d+)\s*(?:[-–]?\s*)?(years?|yrs?)\b", t, flags=re.I)
    if m:
        return float(m.group(1))

    m = re.search(r"\b(minimum|min\.)\s+(?:of\s+)?(\d+)\s*(?:[-–]?\s*)?(years?|yrs?)\b", t, flags=re.I)
    if m:
        return float(m.group(2))

    m = re.search(r"\b(\d+)\s*(?:\+|plus)?\s*(?:[-–]?\s*)?(years?|yrs?)\b", t, flags=re.I)
    if m:
        return float(m.group(1))

    m = re.search(r"\b(\d+)\s*(?:[-–]|to)\s*(\d+)\s*(months?|mos?|mo\.?)\b", t, flags=re.I)
    if m:
        return _months_to_years(float(m.group(2)))

    m = re.search(r"\bat least\s+(\d+)\s*(months?|mos?|mo\.?)\b", t, flags=re.I)
    if m:
        return _months_to_years(float(m.group(1)))

    m = re.search(r"\b(minimum|min\.)\s+(?:of\s+)?(\d+)\s*(months?|mos?|mo\.?)\b", t, flags=re.I)
    if m:
        return _months_to_years(float(m.group(2)))

    m = re.search(r"\b(\d+)\s*(?:\+|plus)?\s*(months?|mos?|mo\.?)\b", t, flags=re.I)
    if m:
        return _months_to_years(float(m.group(1)))

    return np.nan


def title_hint_from_description(text):
    if not isinstance(text, str) or not text.strip():
        return None

    t = text.strip()

    m = re.search(
        r"\b(full job description|job title|title|position title|role)\b\s*(?:[:–-]\s*)?(.+)",
        t,
        flags=re.I
    )
    if not m:
        return None

    rest = (m.group(2) or "").strip()

    cut_markers = [
        r"\bdepartment\b",
        r"\breports?\s+to\b",
        r"\bsupervises?\b",
        r"\blocation\b",
        r"\btype\s+of\s+position\b",
        r"\bposition\s+type\b",
        r"\bwork\s+schedule\b",
        r"\bposition\s+summary\b",
        r"\bsummary\b",
        r"\babout\b",
        r"\bresponsibilit(?:y|ies)\b",
        r"\bqualifications?\b",
        r"\bhow\s+to\s+apply\b",
        r"\bapplication\b",
    ]
    cut_re = re.compile("|".join(cut_markers), flags=re.I)
    m2 = cut_re.search(rest)
    if m2:
        rest = rest[:m2.start()].strip()

    rest = re.sub(r"\s+", " ", rest).strip()

    rest = re.sub(r"(?<=[a-z])(?=[A-Z])", " ", rest)

    if len(rest) < 3:
        return None
    return rest[:80]


def label_from_title(title):
    if not isinstance(title, str) or not title.strip():
        return None
    t = title.lower().strip()

    if re.search(r"\b(intern|internship|trainee|co-?op)\b", t):
        return "intern"

    if re.search(r"\b(vp|vice president|chief|cfo|cto|ceo|president|director)\b", t):
        return "director_plus"

    if re.search(r"\b(co-?founder|founder|owner|proprietor)\b", t):
        return "director_plus"

    if re.search(r"\b(head|manager|supervisor)\b", t):
        return "manager"

    if re.search(r"\b(team lead|tech lead|service leader|lead)\b", t):
        return "lead"

    if re.search(r"\b(senior|sr\.?|staff|principal|experienced)\b", t):
        return "senior"

    if re.search(r"\b(junior|jr\.?|entry|entry-level|associate)\b", t):
        return "junior"

    return None


def label_from_years(y):
    if pd.isna(y):
        return None
    y = float(y)
    if y <= 1:
        return "junior"
    if 2 <= y <= 4:
        return "mid"
    if 5 <= y <= 7:
        return "senior"
    return "lead"


def label_from_description(text):
    if not isinstance(text, str) or not text:
        return None
    t = text.lower()

    if re.search(r"\b(senior|sr\.)\b", t):
        return "senior"
    if re.search(r"\b(mid[- ]level|mid level|intermediate)\b", t):
        return "mid"
    if re.search(r"\b(team lead|tech lead|lead)\b", t):
        return "lead"
    if re.search(r"\b(manager|people management|manage a team|supervise)\b", t):
        return "manager"

    if re.search(r"\b(entry[- ]level|entry level|junior role|junior position)\b", t):
        return "junior"

    if re.search(r"\b(no (prior|previous) .*experience (is )?(required|needed)|experience (is )?not required)\b", t):
        return "junior"

    if re.search(r"\b(training (will be )?provided|we will train|no experience necessary)\b", t):
        return "junior"

    if re.search(r"\b(new grad|recent graduate|graduate program)\b", t):
        return "junior"

    return None


def label_from_generic_role(title):
    if not isinstance(title, str) or not title.strip():
        return None
    t = title.lower().strip()

    if re.search(r"\b(package handler|warehouse|mover|crew member|driver|delivery|clerk|cashier|receptionist|assistant)\b", t):
        return "junior"
    if re.search(r"\b(salesperson)\b", t):
        return "junior"

    if re.search(r"\b(data analyst|analyst|software engineer|software developer|frontend developer|front end developer|back end developer|backend developer|web developer|copywriter|content editor|account executive|paralegal)\b", t):
        return "mid"

    return None


def normalize_work_type(x):
    if not isinstance(x, str):
        return "UNKNOWN"
    x = x.upper().strip()
    mapping = {
        "FULL_TIME": "FULL_TIME",
        "FULL-TIME": "FULL_TIME",
        "PART_TIME": "PART_TIME",
        "PART-TIME": "PART_TIME",
        "CONTRACT": "CONTRACT",
        "TEMPORARY": "TEMP",
        "TEMP": "TEMP",
        "INTERNSHIP": "INTERN",
        "INTERN": "INTERN",
    }
    return mapping.get(x, "UNKNOWN")


def platform_label_from_formatted_experience_level(x):
    if not isinstance(x, str) or not x.strip():
        return None
    s = x.strip().lower()

    if "intern" in s:
        return "intern"
    if "entry" in s:
        return "junior"

    if "associate" in s:
        return "mid"

    if "mid" in s or "senior" in s:
        return "senior"

    if "director" in s or "executive" in s:
        return "director_plus"

    return None


def main():
    print(f"Loading: {INPUT_CSV}")
    df = pd.read_csv(INPUT_CSV, low_memory=False)
    print(f"Rows raw: {len(df):,}")

    if "job_posting_url" in df.columns:
        df = df.drop_duplicates("job_posting_url")
        print(f"Rows deduped (job_posting_url): {len(df):,}")
    elif "job_id" in df.columns:
        df = df.drop_duplicates("job_id")
        print(f"Rows deduped (job_id): {len(df):,}")

    df["description_clean"] = df["description"].map(clean_text)
    df["desc_len"] = df["description_clean"].str.len()

    before = len(df)
    df = df[(df["desc_len"] >= MIN_DESC_LEN) & (df["desc_len"] <= MAX_DESC_LEN)].copy()
    print(f"Rows after desc filter ({MIN_DESC_LEN}-{MAX_DESC_LEN}): {before:,} -> {len(df):,}")

    df["salary_min"] = pd.to_numeric(df.get("min_salary"), errors="coerce")
    df["salary_max"] = pd.to_numeric(df.get("max_salary"), errors="coerce")
    df["pay_period"] = df.get("pay_period")
    df["currency"] = df.get("currency")

    mult = (
        df["pay_period"]
        .astype(str)
        .str.upper()
        .str.strip()
        .map(PAY_PERIOD_TO_ANNUAL)
    )
    df["salary_annual_min"] = df["salary_min"] * mult
    df["salary_annual_max"] = df["salary_max"] * mult

    if "formatted_work_type" in df.columns:
        df["work_type"] = df["formatted_work_type"].map(normalize_work_type)
    else:
        df["work_type"] = df.get("work_type", "UNKNOWN")
        df["work_type"] = df["work_type"].map(normalize_work_type)

    df["remote_allowed"] = pd.to_numeric(df.get("remote_allowed", 0), errors="coerce").fillna(0).astype(int)

    df["years_hint"] = df["description_clean"].map(years_hint)
    df["title_hint"] = df["description_clean"].map(title_hint_from_description)
    df["desc_label"] = df["description_clean"].map(label_from_description)

    if "formatted_experience_level" in df.columns:
        df["platform_experience_label"] = df["formatted_experience_level"].map(platform_label_from_formatted_experience_level)
    else:
        df["platform_experience_label"] = None

    if ENABLE_ROLE_DEFAULTS:
        df["role_default_label"] = df["title"].map(label_from_generic_role)
    else:
        df["role_default_label"] = None

    df["experience_label"] = (
        df["platform_experience_label"]
        .fillna(df["title"].map(label_from_title))
        .fillna(df["title_hint"].map(label_from_title))
        .fillna(df["years_hint"].map(label_from_years))
        .fillna(df["desc_label"])
        .fillna(df["role_default_label"])
        .fillna("unknown")
    )

    years_cov = df["years_hint"].notna().mean() * 100
    title_hint_cov = df["title_hint"].notna().mean() * 100
    platform_cov = df["platform_experience_label"].notna().mean() * 100

    print(f"Years_hint coverage: {years_cov:.1f}%")
    print(f"Title_hint coverage: {title_hint_cov:.1f}%")
    print(f"Platform_experience coverage (formatted_experience_level): {platform_cov:.1f}%")

    keep = [
        "job_id", "company_name", "title", "location",
        "description_clean", "desc_len",
        "work_type", "remote_allowed",
        "salary_min", "salary_max", "pay_period", "currency",
        "salary_annual_min", "salary_annual_max",
        "years_hint", "title_hint",
        "formatted_experience_level", "platform_experience_label",
        "experience_label",
        "job_posting_url", "original_listed_time",
    ]
    keep = [c for c in keep if c in df.columns]
    out = df[keep].copy()

    print(f"Saving: {OUTPUT_CSV}")
    out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Done. Rows saved: {len(out):,}")

    print("Top experience_label:")
    print(out["experience_label"].value_counts().head(12).to_string())

    unk = df[df["experience_label"] == "unknown"].copy()
    print("\n--- Unknown diagnostics ---")
    print("Unknown count:", len(unk))
    print("Unknown with years_hint:", int(unk["years_hint"].notna().sum()))
    print("Unknown with title_hint:", int(unk["title_hint"].notna().sum()))
    print("Unknown with platform label:", int(unk["platform_experience_label"].notna().sum()))

    print("\nUnknown work_type:")
    print(unk["work_type"].value_counts().head(10).to_string())

    print("\nUnknown titles (top 30):")
    print(unk["title"].fillna("").str.lower().value_counts().head(30).to_string())


if __name__ == "__main__":
    main()
