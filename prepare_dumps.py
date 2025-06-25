#!/usr/bin/env python3
"""Download PGN dumps and filter them by speed and Elo."""

import argparse
import io
import sys
import time
import requests
import zstandard as zstd
import chess.pgn
from pathlib import Path
from datetime import datetime


class ProgressBar:
    """Simple progress bar with moving average ETA.

    Parameters
    ----------
    total : int
        The total number of bytes/items to process.
    width : int, optional
        Width of the progress bar in characters (default 40).
    history : float, optional
        Time window in seconds to compute the moving average speed (default 5.0).
    interval : float, optional
        Minimum number of seconds between screen updates (default 1.0).
    """

    def __init__(self, total: int, width: int = 40, history: float = 5.0, interval: float = 1.0) -> None:
        self.total = total
        self.width = width
        self.history = history
        self.interval = interval
        self.start = time.time()
        self.last_print = 0.0
        self.done = 0
        self.samples = [(self.start, 0)]

    def update(self, n: int) -> None:
        self.done += n
        now = time.time()
        self.samples.append((now, self.done))
        while self.samples and now - self.samples[0][0] > self.history:
            self.samples.pop(0)
        if now - self.last_print >= self.interval:
            self.last_print = now
            self._print()

    def _print(self) -> None:
        now = time.time()
        if len(self.samples) >= 2:
            dt = self.samples[-1][0] - self.samples[0][0]
            dv = self.samples[-1][1] - self.samples[0][1]
            speed = dv / dt if dt > 0 else 0.0
        else:
            dt = now - self.start
            speed = self.done / dt if dt > 0 else 0.0
        eta = (self.total - self.done) / speed if speed > 0 else float("inf")
        progress = self.done / self.total if self.total else 0
        filled = int(self.width * progress)
        bar = "#" * filled + "-" * (self.width - filled)
        if eta != float("inf"):
            eta_str = f"ETA {eta:6.1f}s"
        else:
            eta_str = "ETA ?"
        percent = progress * 100 if self.total else 0
        print(f"\r[{bar}] {percent:5.1f}% {eta_str}", end="", file=sys.stderr)
        sys.stderr.flush()

    def finish(self) -> None:
        self._print()
        print(file=sys.stderr)


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
        total = int(r.headers.get("Content-Length", 0))
        progress = ProgressBar(total)
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                progress.update(len(chunk))
        progress.finish()


def filter_pgn_zst(in_path: Path, out_path: Path, min_elo: int, max_elo: int):
    """Decompress, filter games and recompress."""
    dctx = zstd.ZstdDecompressor()
    cctx = zstd.ZstdCompressor()

    with open(in_path, "rb") as f_in, open(out_path, "wb") as f_out:
        reader = io.TextIOWrapper(dctx.stream_reader(f_in), encoding="utf-8")
        progress = ProgressBar(in_path.stat().st_size)

        class ProgressReader:
            def __init__(self, file, pb):
                self.file = file
                self.pb = pb

            def read(self, n=-1):
                chunk = self.file.read(n)
                self.pb.update(len(chunk))
                return chunk

            def __getattr__(self, name):
                return getattr(self.file, name)

        reader = io.TextIOWrapper(
            dctx.stream_reader(ProgressReader(f_in, progress)), encoding="utf-8"
        )
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
            moves = game.accept(
                chess.pgn.StringExporter(columns=None, variations=False, comments=False)
            ).strip()
            writer.write(f"{moves} {result}\n\n")

        writer.flush()
        progress.finish()


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
    parser = argparse.ArgumentParser(description="Download and sanitize PGN dumps")
    parser.add_argument(
        "--dest",
        default=r"D:\\opening-explorer",
        help="Directory where processed PGNs will be stored",
    )