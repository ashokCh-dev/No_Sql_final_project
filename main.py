#!/usr/bin/env python3
"""
Multi-Pipeline ETL and Reporting Framework for Web Server Log Analytics
DAS 839 – NoSQL Systems

Usage:
  python main.py --pipeline mapreduce --batch-size 50000 --input data/NASA_access_log_Jul95
  python main.py --report --run-id 3
"""

import argparse
import sys
import os

# Ensure project root is on the path regardless of working directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog='main.py',
        description='Multi-pipeline ETL framework for NASA web server log analytics',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py --pipeline mapreduce --batch-size 50000 --input data/NASA_access_log_Jul95
  python main.py --report --run-id 3
        """
    )

    p.add_argument(
        '--pipeline',
        choices=['mapreduce', 'mongodb'],
        metavar='PIPELINE',
        help='Execution pipeline to use: mapreduce | mongodb'
    )
    p.add_argument(
        '--input',
        metavar='PATH',
        help='Path to raw NASA log file'
    )
    p.add_argument(
        '--batch-size',
        type=int,
        default=50000,
        metavar='N',
        help='Number of log lines per batch (default: 50000)'
    )
    p.add_argument(
        '--report',
        action='store_true',
        help='Display the report for a completed run'
    )
    p.add_argument(
        '--run-id',
        type=int,
        metavar='ID',
        help='Run ID to display the report for'
    )
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.report:
        if not args.run_id:
            parser.error("--report requires --run-id")
        from reporting.reporter import generate_report
        generate_report(args.run_id)

    elif args.pipeline:
        if not args.input:
            parser.error("--pipeline requires --input")
        if args.pipeline == 'mapreduce':
            from pipelines.mapreduce.runner import run
            run(input_path=args.input, batch_size=args.batch_size)
        elif args.pipeline == 'mongodb':
            from pipelines.mongodb.runner import run
            run(input_path=args.input, batch_size=args.batch_size)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
