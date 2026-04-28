#!/usr/bin/env python3
"""Q3 Hourly Error Analysis — mapper.
Input:  TSV line (8 fields): host log_date log_hour method path proto status bytes
Output: log_date \t log_hour \t host \t is_error \t 1
        is_error = 1 if status in 400-599, else 0
"""
import sys

for line in sys.stdin:
    parts = line.rstrip('\n').split('\t')
    if len(parts) != 8:
        continue
    host     = parts[0]
    log_date = parts[1]
    log_hour = parts[2]
    status   = int(parts[6])
    is_error = '1' if 400 <= status <= 599 else '0'
    print(f"{log_date}\t{log_hour}\t{host}\t{is_error}\t1")
