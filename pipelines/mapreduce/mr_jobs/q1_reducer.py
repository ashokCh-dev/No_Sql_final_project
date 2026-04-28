#!/usr/bin/env python3
"""Q1 Daily Traffic Summary — reducer.
Input:  sorted lines: log_date \t status_code \t 1 \t bytes  (key = first 2 fields)
Output: log_date \t status_code \t request_count \t total_bytes
"""
import sys

current_key = None
req_count   = 0
total_bytes = 0

for line in sys.stdin:
    parts = line.rstrip('\n').split('\t')
    if len(parts) != 4:
        continue
    log_date, status, count, bytes_v = parts
    key = (log_date, status)

    if key != current_key:
        if current_key is not None:
            print(f"{current_key[0]}\t{current_key[1]}\t{req_count}\t{total_bytes}")
        current_key = key
        req_count   = 0
        total_bytes = 0

    req_count   += int(count)
    total_bytes += int(bytes_v)

if current_key is not None:
    print(f"{current_key[0]}\t{current_key[1]}\t{req_count}\t{total_bytes}")
