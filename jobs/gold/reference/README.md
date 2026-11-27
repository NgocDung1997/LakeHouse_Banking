# Gold Reference Data (`jobs/gold/reference/`)

## `ews_rules.csv`
Early Warning Signal (EWS) credit-risk rules stored as **data, not hard-coded SQL**, loaded into
`gold.credit.dim_ews_rule` by `jobs/gold/credit/gold__credit__dim_ews_rule.py` and applied generically by
`jobs/gold/credit/gold__credit__credit_ews_snapshot.yaml`.

### Columns
| Column | Meaning |
|---|---|
| `rule_code` | Unique signal code (e.g. `DPD_1_30`, `LATE_90D`). Must not have overlapping validity periods. |
| `metric` | Metric computed in `gold__credit__credit_ews_snapshot.yaml` step1 (`max_dpd`, `debt_group`, `late_cnt_30d`, `late_cnt_90d`, `penalty_cnt_90d`, `dpd_change`, `debt_group_change`). |
| `operator` | Comparison operator: `>`, `>=`, `<`, `<=`, `=`, `between`. |
| `threshold_1`, `threshold_2` | Numeric threshold(s). `between` requires `threshold_1 <= threshold_2`; other operators require `threshold_2` empty. |
| `ews_level` | Severity level: `WATCH` (rank 1), `AMBER` (rank 2), `RED` (rank 3). |
| `points` | Non-negative integer risk points contributed to `ews_score` when the rule fires. |
| `valid_from`, `valid_to` | Effective period of the rule (`valid_to` empty = open-ended). |
| `description` | Human-readable rule explanation. |

### How to change a rule
**Never edit an active row in place** (that would silently change the meaning of historical snapshots):
1. Set `valid_to` on the old row to the last day the old threshold applied.
2. Add a new row with the same `rule_code`, the new threshold, and `valid_from` = the next day (leave `valid_to` empty).
3. Re-run `spark-submit /home/spark/jobs/gold/credit/gold__credit__dim_ews_rule.py`.
4. Re-run `gold__credit__credit_ews_snapshot` for the affected `cob_dt` date(s).
