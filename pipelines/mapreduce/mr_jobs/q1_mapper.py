#!/usr/bin/env python3
"""Q1 Daily Traffic Summary — mapper.
Input:  TSV line (8 fields): host log_date log_hour method path proto status bytes
Output: log_date \t status_code \t 1 \t bytes
"""
import sys

for line in sys.stdin:
    parts = line.rstrip('\n').split('\t')
    if len(parts) != 8:
        continue
    log_date = parts[1]
    status   = parts[6]
    bytes_v  = parts[7]
    print(f"{log_date}\t{status}\t1\t{bytes_v}")
