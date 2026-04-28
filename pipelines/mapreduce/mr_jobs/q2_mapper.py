#!/usr/bin/env python3
"""Q2 Top Requested Resources — mapper.
Input:  TSV line (8 fields): host log_date log_hour method path proto status bytes
Output: resource_path \t host \t 1 \t bytes
"""
import sys

for line in sys.stdin:
    parts = line.rstrip('\n').split('\t')
    if len(parts) != 8:
        continue
    host    = parts[0]
    path    = parts[4]
    bytes_v = parts[7]
    print(f"{path}\t{host}\t1\t{bytes_v}")
