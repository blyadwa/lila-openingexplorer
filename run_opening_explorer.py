#!/usr/bin/env python3
"""Utility to build and run a local Opening Explorer database."""

import argparse
import subprocess
import time
from pathlib import Path


def run_server(db_path):
    """Start the explorer server and return the process."""
    cmd = ["cargo", "run", "--release", "--", "--db", str(db_path)]
    return subprocess.Popen(cmd)


def run_importer(endpoint, pgns):
    """Run the PGN importer against the server."""
    cmd = ["cargo", "run", "--release", "--", "--endpoint", endpoint] + list(pgns)
    subprocess.run(cmd, cwd="import-pgn", check=True)


def main():
    parser = argparse.ArgumentParser(description="Build a local Lichess Opening Explorer database")
    parser.add_argument("pgns", nargs="+", help="Paths to PGN files to import")
    parser.add_argument(
        "--db",
        default=r"D:\\opening-explorer",
        help="Directory where RocksDB data will be stored (default: D:\\opening-explorer)",
    )
    parser.add_argument(
        "--endpoint",
        default="http://localhost:9002",
        help="Endpoint of the explorer server",
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=5,
        help="Seconds to wait for the server to start before importing",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    db_path.mkdir(parents=True, exist_ok=True)

    server_proc = run_server(db_path)
    try:
        time.sleep(args.wait)
        run_importer(args.endpoint, args.pgns)
    finally:
        server_proc.terminate()
        server_proc.wait()


if __name__ == "__main__":
    main()
