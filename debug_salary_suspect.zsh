#!/usr/bin/env zsh
set -euo pipefail

python - <<'PY'
import pandas as pd

raw = pd.read_csv("pl_dataset/pl_jobs.csv", low_memory=False)
cln = pd.read_csv("pl_dataset/pl_jobs_clean.csv", low_memory=False)

sus = cln[cln["salary_suspect"]==True][["job_posting_url","title","salary_min","salary_max","pay_period","salary_suspect"]].copy()

if len(sus) == 0:
    print("No rows with salary_suspect=True")
else:
    m = sus.merge(
        raw[["url","salary"]], 
        left_on="job_posting_url", 
        right_on="url", 
        how="left"
    ).drop(columns=["url"])
    
    print(f"=== SALARY SUSPECT DEBUG ({len(m)} rows) ===\n")
    print(m.to_string(index=False))
    print(f"\nTotal suspect rows: {len(m)}")
PY
