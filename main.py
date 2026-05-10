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
  python main.py --pipeline pig       --batch-size 50000 --input data/NASA_access_log_Jul95
  python main.py --report --run-id 3
        """
    )

    p.add_argument(
        '--pipeline',
        choices=['mapreduce', 'mongodb', 'pig', 'hive', 'all'],
        metavar='PIPELINE',
        help='Execution pipeline: mapreduce | mongodb | pig | hive | all (run all four sequentially)'
    )
    p.add_argument(
        '--inputs',
        nargs='+',
        metavar='PATH',
        help='One or more raw NASA log files; each file is treated as one batch '
             '(e.g. --inputs Jul95 Aug95 -> batch 1 = July, batch 2 = August)'
    )
    p.add_argument(
        '--input',
        metavar='PATH',
        help='Single-file alias for --inputs (kept for backwards-compat)'
    )
    p.add_argument(
        '--query',
        choices=['q1', 'q2', 'q3', 'all'],
        default='all',
        metavar='Q',
        help='Which query to run: q1 | q2 | q3 | all (default: all)'
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
        inputs = args.inputs or ([args.input] if args.input else None)
        if not inputs:
            parser.error("--pipeline requires --inputs (or --input for a single file)")

        def _dispatch(name: str) -> None:
            if name == 'mapreduce':
                from pipelines.mapreduce.runner import run
            elif name == 'mongodb':
                from pipelines.mongodb.runner import run
            elif name == 'pig':
                from pipelines.pig.runner import run
            elif name == 'hive':
                from pipelines.hive.runner import run
            run(inputs=inputs, query=args.query)

        if args.pipeline == 'all':
            failures = []
            for name in ('mapreduce', 'mongodb', 'pig', 'hive'):
                print(f"\n{'#' * 70}\n# Pipeline: {name}\n{'#' * 70}")
                try:
                    _dispatch(name)
                except Exception as exc:
                    print(f"!! Pipeline {name} failed: {exc}", file=sys.stderr)
                    failures.append(name)
            if failures:
                print(f"\nCompleted with failures: {failures}", file=sys.stderr)
                sys.exit(1)
            print(f"\nAll 4 pipelines completed successfully.")
        else:
            _dispatch(args.pipeline)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == '__main__':
    main()
