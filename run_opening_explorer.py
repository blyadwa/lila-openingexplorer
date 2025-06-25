#!/usr/bin/env python3
"""Start the Opening Explorer server and import PGN files."""

import argparse
import subprocess
import time
from pathlib import Path


def run_server(db_path: Path) -> subprocess.Popen:
    """Start the explorer server and return the process."""
    cmd = ["cargo", "run", "--release", "--", "--db", str(db_path)]
    return subprocess.Popen(cmd)


def run_importer(endpoint: str, pgns: list[Path]) -> None:
    """Run the PGN importer against the server."""
    cmd = ["cargo", "run", "--release", "--", "--endpoint", endpoint] + [str(p) for p in pgns]
    subprocess.run(cmd, cwd="import-pgn", check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a local Lichess Opening Explorer")
    parser.add_argument(
        "--db",
        default=r"D:\\opening-explorer",
        help="Directory where RocksDB data is stored",
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
    parser.add_argument(
        "pgns",
        nargs="+",
        help="PGN files to import",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    server_proc = run_server(db_path)
    try:
        time.sleep(args.wait)
        run_importer(args.endpoint, [Path(p) for p in args.pgns])
    finally:
        server_proc.terminate()
        server_proc.wait()


if __name__ == "__main__":
    main()
