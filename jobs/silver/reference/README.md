# Silver Reference Data (`jobs/silver/reference/`)

## `dpd_classification.csv`
Debt classification and provisioning rules as **data, not hard-coded SQL**, loaded into
`silver.base.dim_dpd_classification` by `jobs/silver/base/silver__base__dim_dpd_classification.py`.

- **Source of values:** State Bank of Vietnam (SBV) Circular 11/2021/TT-NHNN (quantitative debt groups 1–5, specific
  provision 0 / 5 / 20 / 50 / 100%, general provision 0.75% on groups 1–4 and 0% on group 5) combined with IFRS 9
  staging (Stage 1 ≤ 30 DPD, Stage 2 31–90 DPD, Stage 3 > 90 DPD) and standard collection buckets (`CURRENT`, `1-30`,
  `31-60`, `61-90`, `90+`).
- **Validity & continuity rule:** Within each `(valid_from, valid_to)` validity period, `[dpd_from, dpd_to]` ranges
  must **not overlap** and must cover `0..∞` without gaps (first row `dpd_from = 0`, last row `dpd_to` empty). The
  loader job validates this before writing.
- **How to update rules:**
  1. Edit `jobs/silver/reference/dpd_classification.csv` (to keep history for a new regulation, close old `valid_to`
     and add new rows starting from the new effective date).
  2. Re-run `spark-submit /home/spark/jobs/silver/base/silver__base__dim_dpd_classification.py`.
  3. Re-run `silver__base__fact_loan_balance` for the affected `cob_dt` date(s).
