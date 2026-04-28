"""
Parses raw NASA HTTP log lines into structured TSV records.

Raw log format:
  host - - [DD/Mon/YYYY:HH:MM:SS ±HHMM] "METHOD path PROTO" status bytes

TSV output format (8 tab-separated fields):
  host  log_date  log_hour  http_method  resource_path  protocol_version  status_code  bytes_transferred
"""

import re
import os
import subprocess
import sys

LOG_RE = re.compile(
    r'^(\S+)'                                    # host
    r' - - '
    r'\[(\d{2}/\w{3}/\d{4})'                    # date: DD/Mon/YYYY
    r':(\d{2}):\d{2}:\d{2} [+-]\d{4}\] '        # hour HH
    r'"(\w+)? ?'                                  # method (optional)
    r'([^\s"]*) ?'                               # resource_path
    r'(HTTP/[\d.]+)?" '                          # protocol (optional)
    r'(\d{3}) '                                  # status_code
    r'(\S+)'                                     # bytes ('-' or integer)
)

MONTHS = {
    'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
    'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
    'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12',
}


def parse_line(line: str) -> dict | None:
    """Return a parsed record dict or None if the line is malformed."""
    try:
        m = LOG_RE.match(line.rstrip('\n'))
        if not m:
            return None
        host, raw_date, hour, method, path, proto, status, raw_bytes = m.groups()

        day, mon, year = raw_date.split('/')
        log_date = f"{year}-{MONTHS[mon]}-{day}"

        bytes_val = 0 if raw_bytes == '-' else int(raw_bytes)

        return {
            'host':             host,
            'log_date':         log_date,
            'log_hour':         int(hour),
            'http_method':      method or '',
            'resource_path':    path or '/',
            'protocol_version': proto or '',
            'status_code':      int(status),
            'bytes_transferred': bytes_val,
        }
    except Exception:
        return None


def record_to_tsv(r: dict) -> str:
    return (
        f"{r['host']}\t{r['log_date']}\t{r['log_hour']}\t"
        f"{r['http_method']}\t{r['resource_path']}\t"
        f"{r['protocol_version']}\t{r['status_code']}\t{r['bytes_transferred']}"
    )


def stage_batch_to_hdfs(records: list[dict], run_id: int, batch_id: int,
                         hdfs_staged_dir: str) -> None:
    """Write a batch of parsed records to a local temp file, then put to HDFS."""
    tmp_path = f'/tmp/nasa_batch_{run_id}_{batch_id}.tsv'
    try:
        with open(tmp_path, 'w', encoding='utf-8') as f:
            for r in records:
                f.write(record_to_tsv(r) + '\n')

        hdfs_dest = f'{hdfs_staged_dir}/run_{run_id}/batch_{batch_id:05d}.tsv'
        subprocess.run(
            ['hdfs', 'dfs', '-put', tmp_path, hdfs_dest],
            check=True, capture_output=True
        )
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
