import argparse
import pickle
import io
from collections import defaultdict
from pathlib import Path

import chess.pgn
import zstandard as zstd


class Stats:
    def __init__(self) -> None:
        self.white = 0
        self.draws = 0
        self.black = 0

    def add(self, result: str) -> None:
        if result == "1-0":
            self.white += 1
        elif result == "0-1":
            self.black += 1
        else:
            self.draws += 1

    @property
    def count(self) -> int:
        return self.white + self.draws + self.black

    def to_dict(self) -> dict:
        return {
            "count": self.count,
            "white": self.white,
            "draw": self.draws,
            "black": self.black,
        }


def parse_game(line: str):
    parts = line.strip().split()
    if not parts:
        return None, None
    result = parts[-1]
    moves = parts[:-1]
    return moves, result


def build_index(pgns: list[Path], out_file: Path) -> None:
    stats: defaultdict[str, Stats] = defaultdict(Stats)
    dctx = zstd.ZstdDecompressor()

    for pgn_path in pgns:
        with open(pgn_path, "rb") as f:
            reader = dctx.stream_reader(f)
            text = io.TextIOWrapper(reader, encoding="utf-8")
            for line in text:
                line = line.strip()
                if not line:
                    continue
                moves, result = parse_game(line)
                if moves is None:
                    continue
                board = chess.Board()
                stats[board.fen()].add(result)
                for san in moves:
                    board.push_san(san)
                    stats[board.fen()].add(result)
    with open(out_file, "wb") as f:
        pickle.dump(dict(stats), f)


def query_index(index_file: Path, fen: str) -> None:
    with open(index_file, "rb") as f:
        data: dict[str, Stats] = pickle.load(f)
    stats = data.get(fen)
    if stats:
        print(stats.to_dict())
    else:
        print({"count": 0, "white": 0, "draw": 0, "black": 0})


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simple offline opening explorer")
    sub = parser.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="Build index from PGN dumps")
    b.add_argument("pgns", nargs="+", type=Path)
    b.add_argument("--out", default="index.pkl", type=Path)

    q = sub.add_parser("query", help="Query statistics for a FEN")
    q.add_argument("fen")
    q.add_argument("--index", default="index.pkl", type=Path)

    args = parser.parse_args()

    if args.cmd == "build":
        build_index(args.pgns, args.out)
    else:
        query_index(args.index, args.fen)
