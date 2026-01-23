import re
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime

INPUT_CSV = "pl_dataset/pl_jobs.csv"
OUTPUT_CSV = "pl_dataset/pl_jobs_clean.csv"

MIN_DESC_LEN = 50
MAX_DESC_LEN = 20000

HOURS_PER_YEAR = 2016

WORK_TIME_MAP = {
    "Pełny etat": "FULL_TIME",
    "Niepełny etat": "PART_TIME",
    "Praca dodatkowa": "PART_TIME",
    "Praca sezonowa": "TEMP",
    "Praca tymczasowa": "TEMP",
}

TITLE_HINTS = {
    "intern": ["staż", "praktyk", "praktyki", "intern", "stażysta", "stażystka"],
    "junior": ["młodszy", "junior", "asystent", "początkujący", "asystentka", "młodsza"],
    "mid": ["specjalista", "regular", "ekspert", "samodzielny", "samodzielna", "spec\."],
    "senior": ["starszy", "senior", "doświadczony", "starsza", "doświadczona", "główny", "główna"],
    "lead": ["lider", "team leader", "koordynator", "lead", "koord\."],
    "manager": ["kierownik", "manager", "menedżer", "kierowniczka", "menedżerka", "kier\."],
    "director_plus": ["dyrektor", "head", "vp", "chief", "prezes", "dyrektorka"]
}

LEVEL_RANK = {
    "intern": 0,
    "junior": 1,
    "mid": 2,
    "senior": 3,
    "lead": 4,
    "manager": 5,
    "director_plus": 6
}


def clean_text(s):
    if not isinstance(s, str):
        return ""
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"http\S+|www\.\S+", " ", s)
    s = re.sub(r"\S+@\S+\.\S+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


BOILERPLATE_PATTERNS = [
    r"\brodo\b",
    r"wyrażam zgodę na przetwarzanie",
    r"administratorem danych osobowych",
    r"prezesa urzędu ochrony danych osobowych",
    r"\buodo\b",
    r"prawo żądać (?:do nich|dostępu)",
    r"dpo\.",
    r"\[at\]",
    r"skarga do prezesa",
    r"ochrony danych osobowych",
    r"przetwarzanie.*danych osobowych",
    r"rozporządzeni[ae]",
    r"parlament(u|em) europejsk",
    r"rada \(ue\)",
    r"\b2016/679\b",
    r"kodeks pracy",
    r"dz\.?\s*u\.",
    r"\bpoz\.\b",
    r"\bkrs\b",
    r"\bnip\b",
    r"\bregon\b",
    r"sygnalist",
    r"inspektor ochrony danych",
    r"\biod\b",
]


def remove_boilerplate(text: str) -> str:
    if not isinstance(text, str) or not text:
        return ""
    
    parts = re.split(r"(?<=\.)\s+|\n+", text)
    kept = []
    
    for p in parts:
        p_low = p.lower().strip()
        if not p_low:
            continue
        if any(re.search(pat, p_low) for pat in BOILERPLATE_PATTERNS):
            continue
        kept.append(p.strip())
    
    return " ".join([k for k in kept if k])


def parse_salary_pl(salary_str):
    """
    Unit-aware salary parser for PL strings.
    - extracts number+unit pairs with positions
    - prefers values outside parentheses
    - prefers ranges and period-consistent magnitudes
    
    Examples:
    - "5 500 - 6 500 zł / mies. brutto" -> (5500, 6500, "MONTHLY", "PLN")
    - "30 - 80 zł / godz. brutto" -> (30, 80, "HOURLY", "PLN")
    - "5400 zł ... (32 zł/h)" -> (5400, np.nan, "MONTHLY", "PLN") [prefers main value]
    """
    if not isinstance(salary_str, str) or not salary_str.strip():
        return np.nan, np.nan, np.nan, np.nan
    
    s = salary_str.strip()
    s_low = s.lower()
    
    currency = np.nan
    if "zł" in s_low or "pln" in s_low:
        currency = "PLN"
    elif "eur" in s_low or "€" in s_low:
        currency = "EUR"
    elif "usd" in s_low or "$" in s_low:
        currency = "USD"
    
    def to_float(x: str) -> float:
        x = x.replace("\u00A0", " ").strip()
        
        if re.fullmatch(r"\d{1,3}(\.\d{3})+(,\d+)?", x):
            x = x.replace(".", "")
        
        x = x.replace(" ", "")
        
        if "," in x and "." not in x:
            x = x.replace(",", ".")
        
        return float(x)
    
    par_spans = []
    stack = []
    for i, ch in enumerate(s):
        if ch == "(":
            stack.append(i)
        elif ch == ")" and stack:
            j = stack.pop()
            par_spans.append((j, i))
    
    def in_parens(pos: int) -> bool:
        return any(a <= pos <= b for a, b in par_spans)
    
    CUR = r"(?:zł|pln|eur|usd|€|\$)"
    
    unit_patterns = [
        ("HOURLY",  rf"(\d+(?:[\s\u00A0]\d{{3}})*(?:[.,]\d+)?)\s*{CUR}\s*/?\s*(?:h|godz\.?|godzin(?:a|y)?|hour)"),
        ("MONTHLY", rf"(\d+(?:[\s\u00A0]\d{{3}})*(?:[.,]\d+)?)\s*{CUR}\s*/?\s*(?:mies\.?|miesiąc(?:a|u)?|msc|month)"),
        ("YEARLY",  rf"(\d+(?:[\s\u00A0]\d{{3}})*(?:[.,]\d+)?)\s*{CUR}\s*/?\s*(?:rok(?:u)?|rocznie|year|annual)"),
        ("DAILY",   rf"(\d+(?:[\s\u00A0]\d{{3}})*(?:[.,]\d+)?)\s*{CUR}\s*/?\s*(?:dzień|dniówka|day)"),
    ]
    
    candidates = []
    for period, pat in unit_patterns:
        for m in re.finditer(pat, s_low):
            try:
                val = to_float(m.group(1))
                pos = m.start(1)
                candidates.append((val, period, pos, in_parens(pos)))
            except:
                pass
    
    range_pat = rf"(\d+(?:[\s\u00A0]\d{{3}})*(?:[.,]\d+)?)\s*[-–]\s*(\d+(?:[\s\u00A0]\d{{3}})*(?:[.,]\d+)?)\s*{CUR}\s*/?\s*(h|godz\.?|mies\.?|miesiąc(?:a|u)?|msc|rok(?:u)?|rocznie|dzień|dniówka)"
    range_hits = []
    for m in re.finditer(range_pat, s_low):
        try:
            v1 = to_float(m.group(1))
            v2 = to_float(m.group(2))
            unit = m.group(3)
            pos = m.start(1)
            par = in_parens(pos)
            if unit.startswith(("h", "godz")):
                period = "HOURLY"
            elif unit.startswith(("mies", "msc")):
                period = "MONTHLY"
            elif unit.startswith(("rok", "rocz")):
                period = "YEARLY"
            else:
                period = "DAILY"
            range_hits.append((v1, v2, period, pos, par))
        except:
            pass
    
    if range_hits:
        range_hits.sort(key=lambda x: (x[4], x[3]))
        v1, v2, period, _pos, _par = range_hits[0]
        return v1, v2, period, currency
    
    if candidates:
        score = {}
        vals_by_period = {}
        for val, period, pos, par in candidates:
            score[period] = score.get(period, 0) + (2 if not par else 1)
            vals_by_period.setdefault(period, []).append(val)
        
        best_period = sorted(score.items(), key=lambda kv: kv[1], reverse=True)[0][0]
        vals = vals_by_period[best_period]
        
        vals = sorted(vals)
        if len(vals) == 1:
            return vals[0], np.nan, best_period, currency
        return vals[0], vals[-1], best_period, currency
    
    raw_nums = re.findall(r"\d+(?:[\s\u00A0]\d{3})*(?:[.,]\d+)?", s)
    nums = []
    for rn in raw_nums:
        try:
            nums.append(to_float(rn))
        except:
            pass
    if not nums:
        return np.nan, np.nan, np.nan, currency
    
    pay_period = np.nan
    if re.search(r"(mies|miesiąc|/msc|month)", s_low):
        pay_period = "MONTHLY"
    elif re.search(r"(godz|godzin|/h|hour)", s_low):
        pay_period = "HOURLY"
    elif re.search(r"(rok|rocznie|year|annual)", s_low):
        pay_period = "YEARLY"
    elif re.search(r"(dzień|dniówka|day)", s_low):
        pay_period = "DAILY"
    
    nums = sorted(nums)
    if len(nums) == 1:
        return nums[0], np.nan, pay_period, currency
    return nums[0], nums[1], pay_period, currency


def fix_salary_period(min_val, max_val, pay_period):
    if pd.isna(pay_period):
        return min_val, max_val, pay_period, False
    
    x = max_val if not pd.isna(max_val) else min_val
    if pd.isna(x):
        return min_val, max_val, pay_period, False
    
    suspect = False
    new_period = pay_period
    
    if pay_period == "MONTHLY" and x < 500:
        new_period = "HOURLY"
        suspect = True
    
    if pay_period == "HOURLY" and x >= 1000:
        new_period = "MONTHLY"
        suspect = True
    
    if new_period == "HOURLY" and x > 300:
        suspect = True
    
    return min_val, max_val, new_period, suspect


SALARY_CTX = re.compile(
    r"(wynagrod|stawka|zarob|płac|pensj|oferujemy.*wynagrod|brutto|netto)",
    re.IGNORECASE
)

CUR_RE = r"(?:zł|pln|eur|usd|€|\$)"
NUM_RE = r"\d{1,3}(?:[ \u00A0]\d{3})*(?:[.,]\d+)?(?:\s*(?:tys\.?|k))?"

DESC_MONEY_RE = re.compile(
    rf"(?:\bod\b\s*)?"
    rf"(?P<min>{NUM_RE})"
    rf"(?:\s*(?:[-–]|\bdo\b)\s*(?P<max>{NUM_RE}))?"
    rf"\s*(?P<cur>{CUR_RE})"
    rf"(?P<tail>.{{0,80}})",
    re.IGNORECASE
)

DESC_MONEY_RE_CUR_FIRST = re.compile(
    rf"(?P<cur>{CUR_RE})\s*(?P<min>{NUM_RE})"
    rf"(?:\s*(?:[-–]|\bdo\b)\s*(?P<max>{NUM_RE}))?"
    rf"(?P<tail>.{{0,80}})",
    re.IGNORECASE
)


def _to_float_pl(x: str) -> float:
    x = x.replace("\u00A0", " ").strip().lower()
    
    mult = 1.0
    if re.search(r"\btys\.?\b", x):
        mult = 1000.0
        x = re.sub(r"\btys\.?\b", "", x).strip()
    elif re.search(r"\bk\b", x):
        mult = 1000.0
        x = re.sub(r"\bk\b", "", x).strip()
    
    if re.fullmatch(r"\d{1,3}(\.\d{3})+(,\d+)?", x):
        x = x.replace(".", "")
    x = x.replace(" ", "")
    
    if "," in x and "." not in x:
        x = x.replace(",", ".")
    
    return float(x) * mult


def parse_salary_from_description_pl(text: str):
    if not isinstance(text, str) or not text.strip():
        return np.nan, np.nan, np.nan, np.nan

    best = None

    for m in DESC_MONEY_RE.finditer(text):
        try:
            vmin = _to_float_pl(m.group("min"))
            vmax = _to_float_pl(m.group("max")) if m.group("max") else np.nan
        except Exception:
            continue

        cur_raw = m.group("cur").lower()
        if cur_raw in ("zł", "pln"):
            cur = "PLN"
        elif cur_raw in ("€", "eur"):
            cur = "EUR"
        elif cur_raw in ("$", "usd"):
            cur = "USD"
        else:
            cur = np.nan

        tail = (m.group("tail") or "").lower()
        # unit detection (in tail after currency)
        if re.search(r"(/h\b|zł/h|godz\.?|godzin|za\s*godzin|na\s*godzin|hour)", tail):
            period = "HOURLY"
        elif re.search(r"(mies\.?|miesiąc|miesięczn|msc|/msc|month|na\s*mies)", tail):
            period = "MONTHLY"
        elif re.search(r"(rok|roczn|annual|year)", tail):
            period = "YEARLY"
        elif re.search(r"(dzień|dniówka|day)", tail):
            period = "DAILY"
        else:
            period = np.nan

        # context check (±80 characters around match)
        s = max(0, m.start() - 80)
        e = min(len(text), m.end() + 80)
        ctx = text[s:e]
        has_ctx = bool(SALARY_CTX.search(ctx))

        # scoring: prefer real salary, not "1,40 zł"
        score = 0
        if has_ctx: 
            score += 3
        if not pd.isna(period): 
            score += 2
        # filter out micro-amounts like 1,40 zł
        if vmin < 10: 
            score -= 3
        # prefer monthly >= 1000
        x = vmax if not pd.isna(vmax) else vmin
        if period == "MONTHLY" and x >= 1000: 
            score += 1
        if period == "HOURLY" and 10 <= x <= 300: 
            score += 1

        snippet = text[m.start():m.end()]
        cand = (score, vmin, vmax, period, cur, snippet)
        if best is None or cand[0] > best[0]:
            best = cand
    
    for m in DESC_MONEY_RE_CUR_FIRST.finditer(text):
        try:
            vmin = _to_float_pl(m.group("min"))
            vmax = _to_float_pl(m.group("max")) if m.group("max") else np.nan
        except Exception:
            continue

        cur_raw = m.group("cur").lower()
        if cur_raw in ("zł", "pln"):
            cur = "PLN"
        elif cur_raw in ("€", "eur"):
            cur = "EUR"
        elif cur_raw in ("$", "usd"):
            cur = "USD"
        else:
            cur = np.nan

        tail = (m.group("tail") or "").lower()
        # unit detection (in tail after currency)
        if re.search(r"(/h\b|zł/h|godz\.?|godzin|za\s*godzin|na\s*godzin|hour)", tail):
            period = "HOURLY"
        elif re.search(r"(mies\.?|miesiąc|miesięczn|msc|/msc|month|na\s*mies)", tail):
            period = "MONTHLY"
        elif re.search(r"(rok|roczn|annual|year)", tail):
            period = "YEARLY"
        elif re.search(r"(dzień|dniówka|day)", tail):
            period = "DAILY"
        else:
            period = np.nan

        # context check (±80 characters around match)
        s = max(0, m.start() - 80)
        e = min(len(text), m.end() + 80)
        ctx = text[s:e]
        has_ctx = bool(SALARY_CTX.search(ctx))

        # scoring: prefer real salary, not "1,40 zł"
        score = 0
        if has_ctx: 
            score += 3
        if not pd.isna(period): 
            score += 2
        # filter out micro-amounts like 1,40 zł
        if vmin < 10: 
            score -= 3
        # prefer monthly >= 1000
        x = vmax if not pd.isna(vmax) else vmin
        if period == "MONTHLY" and x >= 1000: 
            score += 1
        if period == "HOURLY" and 10 <= x <= 300: 
            score += 1

        snippet = text[m.start():m.end()]
        cand = (score, vmin, vmax, period, cur, snippet)
        if best is None or cand[0] > best[0]:
            best = cand

    if best is None or best[0] < 2:
        return np.nan, np.nan, np.nan, np.nan

    _, vmin, vmax, period, cur, _ = best
    return vmin, vmax, period, cur


def annualize_salary(min_val, max_val, pay_period):
    if pd.isna(pay_period) or pd.isna(min_val):
        return np.nan, np.nan
    
    multipliers = {
        "HOURLY": HOURS_PER_YEAR,
        "MONTHLY": 12,
        "YEARLY": 1,
        "DAILY": 260,
    }
    
    multiplier = multipliers.get(pay_period, np.nan)
    if pd.isna(multiplier):
        return np.nan, np.nan
    
    annual_min = min_val * multiplier if not pd.isna(min_val) else np.nan
    annual_max = max_val * multiplier if not pd.isna(max_val) else np.nan
    
    return annual_min, annual_max


def map_work_time(work_time):
    if not isinstance(work_time, str):
        return "UNKNOWN"
    
    work_time = work_time.strip()
    return WORK_TIME_MAP.get(work_time, "UNKNOWN")


def infer_remote_allowed(description, title=""):
    if not isinstance(description, str):
        description = ""
    if not isinstance(title, str):
        title = ""
    
    text = (title + " " + description).lower()
    
    NEG_REMOTE = [
        r"brak możliwości.*zdal",
        r"bez możliwości.*zdal",
        r"nie ma możliwości.*zdal",
        r"wyłącznie stacjon",
        r"brak.*pracy zdalnej",
    ]
    
    if any(re.search(p, text) for p in NEG_REMOTE):
        return 0
    
    hybrid_keywords = ["hybryd", "hybrid"]
    
    remote_patterns = [
        r"\bzdaln",
        r"praca zdalna",
        r"\bremote\b",
        r"home office",
        r"w domu",
        r"praca z domu",
        r"\btelepraca\b",
        r"prac[ay] w trybie zdalnym",
        r"homeoffice",
        r"home-office"
    ]
    
    not_remote_keywords = [
        "praca stacjonarna", "na miejscu", "stacjonarnie",
        "obecność w biurze", "w lokalu"
    ]
    
    if any(kw in text for kw in hybrid_keywords):
        return 1
    
    has_remote = any(re.search(pattern, text) for pattern in remote_patterns)
    has_not_remote = any(kw in text for kw in not_remote_keywords)
    
    if has_remote and not has_not_remote:
        return 1
    elif has_not_remote and not has_remote:
        return 0
    else:
        return np.nan


def extract_title_hint(title):
    if not isinstance(title, str):
        return np.nan
    
    t = title.lower()
    
    def has_kw(text, keyword):
        if " " in keyword or "." in keyword:
            return re.search(keyword, text) is not None
        else:
            return re.search(rf"\b{re.escape(keyword)}\b", text) is not None
    
    hits = []
    for level, keywords in TITLE_HINTS.items():
        if any(has_kw(t, kw) for kw in keywords):
            hits.append(level)
    
    if not hits:
        return np.nan
    
    return max(hits, key=lambda lvl: LEVEL_RANK.get(lvl, -1))


def extract_years_hint_pl(text):
    if not isinstance(text, str) or not text:
        return np.nan
    
    t = text.lower()
    
    candidates = []
    
    for m in re.finditer(
        r"(\d+(?:[.,]\d+)?)\s*(?:\+|(?:[-–]|do)\s*(\d+(?:[.,]\d+)?)\s*)?"
        r"(?:lat(?:a|ach|ami)?|rok(?:u|i|iem)?|lata|r\.)",
        t
    ):
        a_str = m.group(1).replace(",", ".")
        a = float(a_str)
        b_str = m.group(2).replace(",", ".") if m.group(2) else None
        b = float(b_str) if b_str else None
        years = b if b is not None else a
        candidates.append((years, m.start(), m.end(), "years"))
    
    for m in re.finditer(
        r"(\d+(?:[.,]\d+)?)\s*(?:[-–]|do)?\s*(\d+(?:[.,]\d+)?)?\s*"
        r"(?:miesiąc(?:e|y|ach|ami)?|mies\.|msc)",
        t
    ):
        a_str = m.group(1).replace(",", ".")
        a = float(a_str)
        b_str = m.group(2).replace(",", ".") if m.group(2) else None
        b = float(b_str) if b_str else None
        months = (a + b) / 2 if b is not None else a
        years = months / 12.0
        candidates.append((years, m.start(), m.end(), "months"))
    
    if not candidates:
        return np.nan
    
    POS_CTX_STRONG = [
        "doświadczen", "doswiadczen", "staż", "staz",
        "na podobnym stanow", "w pracy na podobnym", "praktyk",
        "min", "minimum", "wymagan", "oczekiw"
    ]
    
    NEG_CTX_HARD = [
        "wiek", "lat życia", "lat zycia", "do 65 lat", "do 60 lat",
        "rozporządzen", "rozporzadzen", "parlament", "rada (ue", "ue 2016/679",
        "kodeks pracy", "dz. u.", "dz.u", "ustawa", "poz.", "krs", "nip", "regon",
        "ochrony danych", "administrator danych", "sygnalist"
    ]
    
    best = np.nan
    for years, s, e, _kind in candidates:
        if years > 20:
            continue
        
        window = t[max(0, s-80):min(len(t), e+80)]
        
        if any(neg in window for neg in NEG_CTX_HARD):
            continue
        
        if not any(pos in window for pos in POS_CTX_STRONG):
            continue
        
        best = years if pd.isna(best) else max(best, years)
    
    return best


def infer_experience_label(title_hint, years_hint):
    if not pd.isna(title_hint):
        return title_hint
    
    if pd.isna(years_hint):
        return np.nan
    
    if years_hint < 0.75:
        return "intern"
    elif years_hint <= 1.5:
        return "junior"
    elif years_hint <= 4:
        return "mid"
    else:
        return "senior"


def parse_scraped_at(scraped_at_str):
    if not isinstance(scraped_at_str, str):
        return np.nan
    
    try:
        dt = datetime.strptime(scraped_at_str, "%Y-%m-%d %H:%M:%S")
        return int(dt.timestamp() * 1000)
    except:
        return np.nan


def print_salary_outliers(df_clean, n=20):
    tmp = df_clean.copy()
    
    m = tmp[(tmp["pay_period"] == "MONTHLY") & (tmp["salary_max"].notna()) & (tmp["salary_max"] > 50_000)]
    if len(m) > 0:
        print(f"\n[OUTLIERS] MONTHLY salary_max > 50k: {len(m)} cases")
        print(m[["job_posting_url", "title", "salary_min", "salary_max", "pay_period"]].head(n).to_string(index=False))
    
    h_hi = tmp[(tmp["pay_period"] == "HOURLY") & (tmp["salary_max"].notna()) & (tmp["salary_max"] > 300)]
    if len(h_hi) > 0:
        print(f"\n[OUTLIERS] HOURLY salary_max > 300: {len(h_hi)} cases")
        print(h_hi[["job_posting_url", "title", "salary_min", "salary_max", "pay_period"]].head(n).to_string(index=False))
    
    h_lo = tmp[(tmp["pay_period"] == "HOURLY") & (tmp["salary_min"].notna()) & (tmp["salary_min"] < 10)]
    if len(h_lo) > 0:
        print(f"\n[OUTLIERS] HOURLY salary_min < 10: {len(h_lo)} cases")
        print(h_lo[["job_posting_url", "title", "salary_min", "salary_max", "pay_period"]].head(n).to_string(index=False))


def main():
    print("=" * 60)
    print("Cleaning Polish (OLX) Dataset")
    print("=" * 60)
    
    input_path = Path(INPUT_CSV)
    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}")
        return
    
    print(f"\nLoading: {input_path}")
    df = pd.read_csv(input_path, low_memory=False)
    raw_rows = len(df)
    print(f"Raw rows: {raw_rows:,}")
    
    df = df.drop_duplicates(subset=["url"], keep="first")
    deduped_rows = len(df)
    print(f"After deduplication (by URL): {deduped_rows:,} ({raw_rows - deduped_rows:,} duplicates removed)")
    
    df["description_clean"] = df["description"].apply(clean_text).apply(remove_boilerplate)
    
    df["desc_len"] = df["description_clean"].str.len()
    df = df[(df["desc_len"] >= MIN_DESC_LEN) & (df["desc_len"] <= MAX_DESC_LEN)]
    after_desc_filter = len(df)
    print(f"After desc length filter ({MIN_DESC_LEN}-{MAX_DESC_LEN} chars): {after_desc_filter:,}")
    
    df_clean = pd.DataFrame()
    
    df_clean["job_id"] = df["id"]
    df_clean["job_posting_url"] = df["url"]
    df_clean["company_name"] = df["company"].fillna("UNKNOWN").replace("", "UNKNOWN")
    df_clean["title"] = df["title"]
    df_clean["location"] = df["location"].fillna("")
    df_clean["description_clean"] = df["description_clean"]
    df_clean["desc_len"] = df["desc_len"]
    
    df_clean["work_type"] = df["work_time"].apply(map_work_time)
    df_clean["contract_type"] = df["contract_type"].fillna("")
    df_clean["contract_type_raw"] = df["contract_type"]
    
    df_clean["remote_allowed"] = df.apply(
        lambda row: infer_remote_allowed(row.get("description_clean", ""), row.get("title", "")),
        axis=1
    )
    
    print("\n[Salary] Parsing structured salary field...")
    structured = df["salary"].apply(parse_salary_pl)
    
    print("[Salary] Parsing salary from descriptions (fallback)...")
    desc_parsed = df["description_clean"].apply(parse_salary_from_description_pl)
    
    s_min = pd.Series([r[0] for r in structured], index=df.index)
    s_max = pd.Series([r[1] for r in structured], index=df.index)
    s_pp = pd.Series([r[2] for r in structured], index=df.index)
    s_cur = pd.Series([r[3] for r in structured], index=df.index)
    
    d_min = pd.Series([r[0] for r in desc_parsed], index=df.index)
    d_max = pd.Series([r[1] for r in desc_parsed], index=df.index)
    d_pp = pd.Series([r[2] for r in desc_parsed], index=df.index)
    d_cur = pd.Series([r[3] for r in desc_parsed], index=df.index)
    
    use_struct = s_min.notna()
    use_desc = (~use_struct) & d_min.notna()
    
    df_clean["salary_min"] = s_min.where(use_struct, d_min)
    df_clean["salary_max"] = s_max.where(use_struct, d_max)
    df_clean["pay_period_raw"] = s_pp.where(use_struct, d_pp)
    df_clean["currency"] = s_cur.where(use_struct, d_cur)
    
    df_clean["salary_source"] = np.select(
        [use_struct, use_desc],
        ["STRUCTURED", "DESCRIPTION"],
        default="NONE"
    )
    
    structured_count = use_struct.sum()
    desc_count = use_desc.sum()
    print(f"  Salary from structured field: {structured_count:,}")
    print(f"  Salary from description (fallback): {desc_count:,}")
    print(f"  No salary found: {(~use_struct & ~use_desc).sum():,}")
    
    fixed_salary = df_clean.apply(
        lambda row: fix_salary_period(row["salary_min"], row["salary_max"], row["pay_period_raw"]),
        axis=1
    )
    df_clean["salary_min"] = [r[0] for r in fixed_salary]
    df_clean["salary_max"] = [r[1] for r in fixed_salary]
    df_clean["pay_period"] = [r[2] for r in fixed_salary]
    df_clean["salary_suspect"] = [r[3] for r in fixed_salary]
    
    df_clean["salary_unit_conflict"] = (
        df_clean["pay_period_raw"].notna() & 
        df_clean["pay_period"].notna() & 
        (df_clean["pay_period_raw"] != df_clean["pay_period"])
    )
    
    annual_results = df_clean.apply(
        lambda row: annualize_salary(row["salary_min"], row["salary_max"], row["pay_period"]),
        axis=1
    )
    df_clean["salary_annual_min"] = [r[0] for r in annual_results]
    df_clean["salary_annual_max"] = [r[1] for r in annual_results]
    
    mask_swap = (
        df_clean["salary_min"].notna() & 
        df_clean["salary_max"].notna() & 
        (df_clean["salary_min"] > df_clean["salary_max"])
    )
    if mask_swap.sum() > 0:
        df_clean.loc[mask_swap, ["salary_min", "salary_max"]] = (
            df_clean.loc[mask_swap, ["salary_max", "salary_min"]].values
        )
        print(f"Swapped salary min/max for {mask_swap.sum()} rows where min > max")
    
    df_clean["salary_outlier_monthly"] = (
        (df_clean["pay_period"] == "MONTHLY") & 
        (df_clean["salary_max"].notna()) & 
        (df_clean["salary_max"] > 50_000)
    )
    df_clean["salary_outlier_hourly"] = (
        (df_clean["pay_period"] == "HOURLY") & 
        (df_clean["salary_max"].notna()) & 
        (df_clean["salary_max"] > 300)
    )
    
    df_clean["title_hint"] = df["title"].apply(extract_title_hint)
    df_clean["years_hint"] = df["description_clean"].apply(extract_years_hint_pl)
    
    df_clean["experience_label"] = df_clean.apply(
        lambda row: infer_experience_label(row["title_hint"], row["years_hint"]),
        axis=1
    )
    
    df_clean["is_confident"] = (
        df_clean["title_hint"].notna() | 
        df_clean["years_hint"].notna()
    )
    
    df_clean["platform_experience_label"] = np.nan
    
    df_clean["scraped_time_ms"] = df["scraped_at"].apply(parse_scraped_at)
    df_clean["original_listed_time"] = df_clean["scraped_time_ms"]
    
    df_clean["lang"] = "pl"
    
    df_clean["remote_allowed"] = df_clean["remote_allowed"].astype("Int8")
    df_clean["salary_suspect"] = df_clean["salary_suspect"].fillna(False).astype("boolean")
    df_clean["salary_unit_conflict"] = df_clean["salary_unit_conflict"].fillna(False).astype("boolean")
    df_clean["is_confident"] = df_clean["is_confident"].astype("boolean")
    df_clean["salary_outlier_monthly"] = df_clean["salary_outlier_monthly"].astype("boolean")
    df_clean["salary_outlier_hourly"] = df_clean["salary_outlier_hourly"].astype("boolean")
    
    # Statistics
    print("\n" + "=" * 60)
    print("Statistics:")
    print("=" * 60)
    
    rows_with_label = df_clean["experience_label"].notna().sum()
    rows_confident = df_clean["is_confident"].sum()
    salary_parsed = df_clean["salary_min"].notna().sum()
    salary_suspect_count = df_clean["salary_suspect"].sum() if "salary_suspect" in df_clean.columns else 0
    remote_inferred = df_clean["remote_allowed"].notna().sum()
    title_hint_count = df_clean["title_hint"].notna().sum()
    years_hint_count = df_clean["years_hint"].notna().sum()
    
    print(f"Rows with experience_label: {rows_with_label:,} ({rows_with_label/len(df_clean)*100:.1f}%)")
    print(f"Rows with is_confident=True: {rows_confident:,} ({rows_confident/len(df_clean)*100:.1f}%)")
    print(f"  - From title_hint: {title_hint_count:,} ({title_hint_count/len(df_clean)*100:.1f}%)")
    print(f"  - From years_hint: {years_hint_count:,} ({years_hint_count/len(df_clean)*100:.1f}%)")
    print(f"Salary parsed: {salary_parsed:,} ({salary_parsed/len(df_clean)*100:.1f}%)")
    if "salary_source" in df_clean.columns:
        source_dist = df_clean["salary_source"].value_counts()
        print(f"  - From structured field: {source_dist.get('STRUCTURED', 0):,}")
        print(f"  - From description: {source_dist.get('DESCRIPTION', 0):,}")
        print(f"  - Not found: {source_dist.get('NONE', 0):,}")
    print(f"Salary suspect (fixed): {salary_suspect_count:,} ({salary_suspect_count/len(df_clean)*100:.1f}%)")
    
    unit_conflict_count = df_clean["salary_unit_conflict"].sum() if "salary_unit_conflict" in df_clean.columns else 0
    print(f"Salary unit conflict (raw != fixed): {unit_conflict_count:,} ({unit_conflict_count/len(df_clean)*100:.1f}%)")
    outlier_monthly = df_clean["salary_outlier_monthly"].sum() if "salary_outlier_monthly" in df_clean.columns else 0
    outlier_hourly = df_clean["salary_outlier_hourly"].sum() if "salary_outlier_hourly" in df_clean.columns else 0
    print(f"Salary outliers: monthly >50k: {outlier_monthly:,}, hourly >300: {outlier_hourly:,}")
    print(f"Remote inferred: {remote_inferred:,} ({remote_inferred/len(df_clean)*100:.1f}%)")
    
    print(f"\nExperience label distribution:")
    label_counts = df_clean["experience_label"].value_counts()
    for label, count in label_counts.items():
        print(f"  {label}: {count:,} ({count/len(df_clean)*100:.1f}%)")
    
    print(f"\nWork type distribution:")
    work_counts = df_clean["work_type"].value_counts()
    for work_type, count in work_counts.items():
        print(f"  {work_type}: {count:,}")
    
    # Print salary outliers
    print_salary_outliers(df_clean)
    
    # Save
    output_path = Path(OUTPUT_CSV)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df_clean.to_csv(output_path, index=False)
    print(f"\n" + "=" * 60)
    print(f"Saved: {output_path}")
    print(f"Final rows: {len(df_clean):,}")
    print("=" * 60)


if __name__ == "__main__":
    main()
