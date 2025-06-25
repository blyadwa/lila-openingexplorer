import io
import zstandard as zstd

with open("D:/opening-explorer/filtered_2013-01.pgn.zst", "rb") as f:
    dctx = zstd.ZstdDecompressor()
    with dctx.stream_reader(f) as reader:
        text = io.TextIOWrapper(reader, encoding="utf-8")
        for i, line in enumerate(text):
            print(repr(line.strip()))
            if i >= 30:  # Print only the first 20 lines
                break
