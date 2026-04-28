#!/usr/bin/env python3
"""Q3 Hourly Error Analysis — reducer.
Input:  sorted lines: log_date \t log_hour \t host \t is_error \t 1  (key = first 2 fields)
Output: log_date \t log_hour \t error_request_count \t total_request_count \t error_rate \t distinct_error_hosts
"""
import sys

current_key   = None
error_count   = 0
total_count   = 0
error_hosts: set[str] = set()

for line in sys.stdin:
    parts = line.rstrip('\n').split('\t')
    if len(parts) != 5:
        continue
    log_date, log_hour, host, is_error, count = parts
    key = (log_date, log_hour)

    if key != current_key:
        if current_key is not None:
            rate = error_count / total_count if total_count > 0 else 0.0
            print(f"{current_key[0]}\t{current_key[1]}\t{error_count}\t"
                  f"{total_count}\t{rate:.4f}\t{len(error_hosts)}")
        current_key = key
        error_count = 0
        total_count = 0
        error_hosts = set()

    total_count += int(count)
    if is_error == '1':
        error_count += int(count)
        error_hosts.add(host)

if current_key is not None:
    rate = error_count / total_count if total_count > 0 else 0.0
    print(f"{current_key[0]}\t{current_key[1]}\t{error_count}\t"
          f"{total_count}\t{rate:.4f}\t{len(error_hosts)}")
