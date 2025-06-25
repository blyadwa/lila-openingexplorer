#!/usr/bin/env python3
"""Utility to build and run a local Opening Explorer database."""

import argparse
import subprocess
import time
import requests
import zstandard as zstd
import io
import chess.pgn
from pathlib import Path
from datetime import datetime


def run_server(db_path):
    """Start the explorer server and return the process."""
    cmd = ["cargo", "run", "--release", "--", "--db", str(db_path)]
    return subprocess.Popen(cmd)


def run_importer(endpoint, pgns):
    """Run the PGN importer against the server."""
    cmd = ["cargo", "run", "--release", "--", "--endpoint", endpoint] + [str(p) for p in pgns]
    subprocess.run(cmd, cwd="import-pgn", check=True)


def month_range(start: str, end: str):
    """Yield YYYY-MM strings from start to end inclusive."""
    cur = datetime.strptime(start, "%Y-%m")
    last = datetime.strptime(end, "%Y-%m")
    while cur <= last:
        yield cur.strftime("%Y-%m")
        if cur.month == 12:
            cur = cur.replace(year=cur.year + 1, month=1)
        else:
            cur = cur.replace(month=cur.month + 1)


def download_file(url: str, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def filter_pgn_zst(in_path: Path, out_path: Path, min_elo: int, max_elo: int):
    """Decompress, filter games and recompress."""
    dctx = zstd.ZstdDecompressor()
    cctx = zstd.ZstdCompressor()

    with open(in_path, "rb") as f_in, open(out_path, "wb") as f_out:
        reader = io.TextIOWrapper(dctx.stream_reader(f_in), encoding="utf-8")
        writer = io.TextIOWrapper(cctx.stream_writer(f_out), encoding="utf-8")

        while True:
            game = chess.pgn.read_game(reader)
            if game is None:
                break
            event = game.headers.get("Event", "").lower()
            if "bullet" in event:
                continue
            try:
                white_elo = int(game.headers.get("WhiteElo", 0))
                black_elo = int(game.headers.get("BlackElo", 0))
            except ValueError:
                continue
            if not (min_elo <= white_elo <= max_elo and min_elo <= black_elo <= max_elo):
                continue
            result = game.headers.get("Result", "*")
            moves = game.accept(chess.pgn.StringExporter(columns=None, variations=False, comments=False)).strip()
            writer.write(f"{moves} {result}\n\n")

        writer.flush()


def prepare_pgns(dest_dir: Path, start: str, end: str, min_elo: int, max_elo: int):
    """Download and filter PGN dumps. Return list of processed files."""
    processed = []
    for month in month_range(start, end):
        url = f"https://database.lichess.org/standard/lichess_db_standard_rated_{month}.pgn.zst"
        orig = dest_dir / f"lichess_db_standard_rated_{month}.pgn.zst"
        filtered = dest_dir / f"filtered_{month}.pgn.zst"
        if not orig.exists():
            print(f"Downloading {url}")
            download_file(url, orig)
        print(f"Filtering {orig.name}")
        filter_pgn_zst(orig, filtered, min_elo, max_elo)
        processed.append(filtered)
    return processed


def main():
    parser = argparse.ArgumentParser(description="Build a local Lichess Opening Explorer database")
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
    parser.add_argument(
        "--start-month",
        default="2013-01",
        help="First month of PGN dump to download (YYYY-MM)",
    )
    parser.add_argument(
        "--end-month",
        default="2025-05",
        help="Last month of PGN dump to download (YYYY-MM)",
    )
    parser.add_argument(
        "--min-elo",
        type=int,
        default=0,
        help="Minimum Elo rating to keep",
    )
    parser.add_argument(
        "--max-elo",
        type=int,
        default=3000,
        help="Maximum Elo rating to keep",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    db_path.mkdir(parents=True, exist_ok=True)

    processed = prepare_pgns(db_path, args.start_month, args.end_month, args.min_elo, args.max_elo)

    server_proc = run_server(db_path)
    try:
        time.sleep(args.wait)
        run_importer(args.endpoint, processed)
    finally:
        server_proc.terminate()
        server_proc.wait()


if __name__ == "__main__":
    main()
