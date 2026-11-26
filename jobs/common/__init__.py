"""
Code shared by the jobs of every layer:

    jdbc.py           class Jdbc — read Postgres, connect, create Iceberg tables
    load_patterns.py  how rows are written: overwrite_partition, overwrite_where, append, merge_upsert, scd1/2/3, ...
    dates.py          business dates: default_cob_dt, date_range, cob_dates, fill_window, run_stamp
    parsers.py        command line: parse_job_args, date options, --where
    pii.py            personal data: encrypt / decrypt, keyed_hash, mask_*
"""
