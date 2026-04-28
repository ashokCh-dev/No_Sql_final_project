#!/usr/bin/env python3
"""Q2 Top Requested Resources — reducer.
Input:  sorted lines: resource_path \t host \t 1 \t bytes  (key = first field)
Output: resource_path \t request_count \t total_bytes \t distinct_host_count
Top-20 ranking is applied by loader.py after reading this full output.
"""
import sys

current_path = None
req_count    = 0
total_bytes  = 0
host_set: set[str] = set()

for line in sys.stdin:
    parts = line.rstrip('\n').split('\t')
    if len(parts) != 4:
        continue
    path, host, count, bytes_v = parts

    if path != current_path:
        if current_path is not None:
            print(f"{current_path}\t{req_count}\t{total_bytes}\t{len(host_set)}")
        current_path = path
        req_count    = 0
        total_bytes  = 0
        host_set     = set()

    req_count   += int(count)
    total_bytes += int(bytes_v)
    host_set.add(host)

if current_path is not None:
    print(f"{current_path}\t{req_count}\t{total_bytes}\t{len(host_set)}")
