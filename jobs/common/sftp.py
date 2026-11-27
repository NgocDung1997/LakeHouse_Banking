"""
sftp.py — SFTP file access for bronze ingestion (flat files: CSV and Excel .xlsx).

Connection settings come from .env (key-based auth preferred):
    SFTP_HOST       remote SFTP host (e.g. 192.168.196.185)
    SFTP_PORT       remote SSH/SFTP port (default 22)
    SFTP_USER       remote SSH username
    SFTP_KEY_PATH   path to private key inside container (e.g. /home/spark/secrets/sftp_id_ed25519)
    SFTP_PASSWORD   optional fallback only when SFTP_KEY_PATH is not set

Source table specs map "<source_domain>.<source_table>" from bronze_tables.csv (source_system = sftp)
to the remote directory, filename pattern / glob, file format (csv | xlsx), sheet name and header row.
paramiko and openpyxl are imported lazily so validation/tests without SFTP do not require them.
"""
import contextlib
import csv
import fnmatch
import io
import os
from datetime import date, datetime
from typing import Dict, Iterator, List, Optional, Tuple

from common.jdbc import Jdbc

# Mapping of "<source_domain>.<source_table>" → remote SFTP file specification
SFTP_SPECS: Dict[str, Dict[str, object]] = {
    "card.card": {
        "remote_dir": "/home/dungnn/card",
        "file_pattern": "card.csv",
        "file_format": "csv",
    },
    "card.card_txn": {
        "remote_dir": "/home/dungnn/card",
        "file_pattern": "card_txn_{cob_dt_compact}.csv",
        "file_glob": "card_txn_*.csv",
        "file_format": "csv",
    },
    "ref.branch": {
        "remote_dir": "/home/dungnn/ref",
        "file_pattern": "reference_master_{cob_dt_compact}.xlsx",
        "file_glob": "reference_master_*.xlsx",
        "file_format": "xlsx",
        "sheet_name": "branch",
        "header_row": 4,
    },
    "ref.product": {
        "remote_dir": "/home/dungnn/ref",
        "file_pattern": "reference_master_{cob_dt_compact}.xlsx",
        "file_glob": "reference_master_*.xlsx",
        "file_format": "xlsx",
        "sheet_name": "product",
        "header_row": 1,
    },
    "ref.gl_mapping": {
        "remote_dir": "/home/dungnn/ref",
        "file_pattern": "reference_master_{cob_dt_compact}.xlsx",
        "file_glob": "reference_master_*.xlsx",
        "file_format": "xlsx",
        "sheet_name": "gl_mapping",
        "header_row": 1,
    },
}


def _normalize_header(value: object) -> str:
    return str(value or "").strip().lower()


def _cell_to_str(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    s = str(value)
    return s if s != "" else None


def _parse_csv_bytes(raw: bytes) -> Tuple[List[str], List[Tuple[Optional[str], ...]]]:
    text = raw.decode("utf-8-sig")
    reader = csv.reader(io.StringIO(text))
    try:
        raw_headers = next(reader)
    except StopIteration:
        raise ValueError("CSV file is empty (no header row)")
    headers = [_normalize_header(h) for h in raw_headers]
    width = len(headers)
    rows: List[Tuple[Optional[str], ...]] = []
    for row in reader:
        if not row or all(cell == "" for cell in row):
            continue
        padded = row[:width] + [""] * max(0, width - len(row))
        rows.append(tuple(cell if cell != "" else None for cell in padded))
    return headers, rows


def _parse_xlsx_bytes(raw: bytes, sheet_name: str, header_row: int = 1) -> Tuple[List[str], List[Tuple[Optional[str], ...]]]:
    import openpyxl

    wb = openpyxl.load_workbook(io.BytesIO(raw), read_only=True, data_only=True)
    try:
        if sheet_name not in wb.sheetnames:
            raise ValueError(f"sheet {sheet_name!r} not found in workbook (available: {wb.sheetnames})")
        ws = wb[sheet_name]
        row_iter = ws.iter_rows(min_row=header_row, values_only=True)
        try:
            raw_headers = list(next(row_iter))
        except StopIteration:
            raise ValueError(f"sheet {sheet_name!r} has no header at row {header_row}")
        while raw_headers and (raw_headers[-1] is None or str(raw_headers[-1]).strip() == ""):
            raw_headers.pop()
        headers = [_normalize_header(h) for h in raw_headers]
        width = len(headers)
        rows: List[Tuple[Optional[str], ...]] = []
        for raw_row in row_iter:
            vals = list(raw_row[:width]) + [None] * max(0, width - len(raw_row))
            if all(v is None or str(v).strip() == "" for v in vals):
                continue
            rows.append(tuple(_cell_to_str(v) for v in vals))
        return headers, rows
    finally:
        wb.close()


def _load_pkey(key_path: str):
    import paramiko

    last_err: Optional[Exception] = None
    for key_cls in (paramiko.Ed25519Key, paramiko.RSAKey, paramiko.ECDSAKey):
        try:
            return key_cls.from_private_key_file(key_path)
        except Exception as exc:
            last_err = exc
    raise ValueError(f"could not load SSH private key from {key_path!r}: {last_err}")


class Sftp:
    """SFTP client and file reader for bronze ingestion (CSV and Excel .xlsx)."""

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 user: Optional[str] = None, key_path: Optional[str] = None,
                 password: Optional[str] = None, timeout: int = 15) -> None:
        self.host = host or os.environ["SFTP_HOST"]
        self.port = int(port or os.environ.get("SFTP_PORT", "22"))
        self.user = user or os.environ["SFTP_USER"]
        self.key_path = key_path or os.environ.get("SFTP_KEY_PATH") or None
        self.password = password or os.environ.get("SFTP_PASSWORD") or None
        self.timeout = timeout
        if not self.key_path and not self.password:
            raise ValueError("SFTP authentication requires SFTP_KEY_PATH (preferred) or SFTP_PASSWORD")

    @staticmethod
    def get_spec(source_table: str) -> Dict[str, object]:
        if source_table not in SFTP_SPECS:
            raise ValueError(f"unknown SFTP source_table {source_table!r}; configured: {sorted(SFTP_SPECS)}")
        return SFTP_SPECS[source_table]

    @contextlib.contextmanager
    def connect(self) -> Iterator[Tuple[object, object]]:
        """Open SSH + SFTP clients using key-based auth (preferred) and close them on exit."""
        import paramiko

        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        kwargs = {
            "hostname": self.host,
            "port": self.port,
            "username": self.user,
            "timeout": self.timeout,
            "allow_agent": False,
            "look_for_keys": False,
        }
        if self.key_path:
            kwargs["pkey"] = _load_pkey(self.key_path)
        else:
            kwargs["password"] = self.password
        ssh.connect(**kwargs)
        sftp = ssh.open_sftp()
        try:
            yield ssh, sftp
        finally:
            try:
                sftp.close()
            finally:
                ssh.close()

    def _resolve_remote_paths(self, sftp, spec: Dict[str, object], cob_dt: Optional[date]) -> List[str]:
        remote_dir = str(spec["remote_dir"]).rstrip("/")
        pattern = str(spec["file_pattern"])
        if cob_dt is not None:
            filename = pattern.format(cob_dt=cob_dt.isoformat(), cob_dt_compact=cob_dt.strftime("%Y%m%d"))
            remote_path = f"{remote_dir}/{filename}"
            try:
                sftp.stat(remote_path)
            except IOError:
                raise FileNotFoundError(f"SFTP file not found: {remote_path}")
            return [remote_path]
        # Full / init load: match all files using file_glob (or the single static file_pattern)
        glob_pattern = str(spec.get("file_glob") or pattern)
        matched = sorted(f for f in sftp.listdir(remote_dir) if fnmatch.fnmatch(f, glob_pattern))
        if not matched:
            raise FileNotFoundError(f"no SFTP files matching {glob_pattern!r} in {remote_dir}")
        return [f"{remote_dir}/{f}" for f in matched]

    def _read_file_bytes(self, sftp, remote_path: str, spec: Dict[str, object]) -> Tuple[List[str], List[Tuple[Optional[str], ...]]]:
        with sftp.open(remote_path, "rb") as f:
            raw = f.read()
        fmt = str(spec["file_format"]).lower()
        if fmt == "csv":
            return _parse_csv_bytes(raw)
        if fmt == "xlsx":
            return _parse_xlsx_bytes(raw, str(spec["sheet_name"]), int(spec.get("header_row", 1)))
        raise ValueError(f"unsupported SFTP file_format {fmt!r}")

    def inspect_headers(self, source_table: str) -> List[str]:
        """Return normalized column headers from the latest matching SFTP file (used by validate/source_checks.py)."""
        spec = self.get_spec(source_table)
        with self.connect() as (_, sftp):
            paths = self._resolve_remote_paths(sftp, spec, cob_dt=None)
            headers, _ = self._read_file_bytes(sftp, paths[-1], spec)
            return headers

    def read(self, spark, source_table: str, schema=None, cob_dt: Optional[date] = None,
             where: Optional[str] = None):
        """Read one SFTP file (for cob_dt) or all matching files (cob_dt=None, init/full) into a Spark DataFrame.

        All columns are staged as StringType on the driver, then cast via Jdbc.apply_schema(df, schema).
        """
        from pyspark.sql.types import StringType, StructField, StructType

        spec = self.get_spec(source_table)
        all_rows: List[Tuple[Optional[str], ...]] = []
        headers: Optional[List[str]] = None

        with self.connect() as (_, sftp):
            paths = self._resolve_remote_paths(sftp, spec, cob_dt=cob_dt)
            for remote_path in paths:
                file_headers, file_rows = self._read_file_bytes(sftp, remote_path, spec)
                if headers is None:
                    headers = file_headers
                elif file_headers != headers:
                    raise ValueError(f"header mismatch in {remote_path}: got {file_headers}, expected {headers}")
                all_rows.extend(file_rows)

        assert headers is not None
        raw_schema = StructType([StructField(col, StringType(), True) for col in headers])
        df = spark.createDataFrame(all_rows, schema=raw_schema)
        df = Jdbc.apply_schema(df, schema)
        if where:
            df = df.filter(where)
        return df
