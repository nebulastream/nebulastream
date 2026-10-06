#!/usr/bin/env python3
"""Generates the input files the asynchronous SEM_MAP measurements read.

The texts are invented. The real benchmark dataset is the Rotten Tomatoes critic reviews the
Python reference uses (`data/rotten_tomatoes_movie_reviews.csv` in the llm_operator repo), whose
`scoreSentiment` column is the ground truth. That file is not redistributable here, so these
measurements cover throughput and engine responsiveness only -- **no quality number can be derived
from this data**. Shape and length mirror the real reviews (one short paragraph, 11-26 words) so
that prompt sizes, and therefore model latencies, are representative.

    scripts/generate_async_benchmark_data.py [--out-dir cmake-build-debug/bench-data]

Then point systest at the parent directory, since the test files say `ATTACH FILE async/<name>`:

    systest -t nes-systests/semantic/AsyncResponsiveness.test:2 --data cmake-build-debug/bench-data
"""

import argparse
import random
from pathlib import Path

OPENERS = ["A", "An", "Easily the most", "Hardly the", "Somehow both a", "Not quite the",
           "Very nearly a", "Yet another"]
ADJECTIVES = ["ambitious", "weightless", "overlong", "charming", "exhausting", "tender",
              "mechanical", "inventive", "derivative", "sincere", "bloated", "nimble", "airless",
              "generous", "muddled", "assured", "forgettable", "disarming", "strained",
              "effortless", "plodding", "luminous", "clumsy", "quietly funny"]
NOUNS = ["spectacle", "character study", "sequel", "comedy", "thriller", "fable", "procedural",
         "melodrama", "road movie", "chamber piece", "blockbuster", "farce", "mystery"]
MIDDLES = ["that never decides what it wants to be", "carried almost entirely by its leads",
           "with more ideas than it can hold", "where the effects do the acting",
           "built on one very good joke", "that earns its ending honestly",
           "undone by a third act nobody wanted", "with a script sharper than its direction",
           "that mistakes volume for feeling", "whose small moments land hardest",
           "stretched well past its natural length",
           "assembled with real craft and little curiosity"]
CLOSERS = ["The result is hard to shake.", "I wanted to like it more than I did.",
           "It works far better than it has any right to.", "By the end I had stopped caring.",
           "There is a better film buried in here somewhere.", "It is a pleasure throughout.",
           "Worth seeing for the performances alone.",
           "A rare case of a sequel improving on the original.",
           "The whole thing collapses under its own weight.", "Modest ambitions met with real skill.",
           "Nothing here lingers.", "Easily the best time I have had at the cinema this year."]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", default="cmake-build-debug/bench-data")
    out = Path(parser.parse_args().out_dir) / "async"
    out.mkdir(parents=True, exist_ok=True)

    # Fixed seed: the same files on every machine, so two runs are comparable.
    random.seed(20261006)
    rows = []
    for identifier in range(1, 761):
        text = (f"{random.choice(OPENERS)} {random.choice(ADJECTIVES)} {random.choice(NOUNS)} "
                f"{random.choice(MIDDLES)}. {random.choice(CLOSERS)}")
        # The CSV source has no quoting, so a comma would split the record.
        assert "," not in text and '"' not in text
        rows.append(f"{identifier},{text}")

    for count in (12, 30, 40, 760):
        (out / f"reviews_{count}.csv").write_text("\n".join(rows[:count]) + "\n")

    # A second stream with no model in it, for the responsiveness measurement.
    (out / "plain_200.csv").write_text(
        "\n".join(f"{i},{i * 7 % 101}" for i in range(1, 201)) + "\n")

    # For the window test: a timestamp column, and the review text is the literal label, so the
    # mock backend's echo makes every answer deterministic and the counts can be asserted exactly.
    # The small set is three windows in one buffer; the large one is 76 seconds over many buffers.
    (out / "windowed_small.csv").write_text("\n".join([
        "1,1000,POSITIVE", "2,2000,negative", "3,3000,POSITIVE", "4,4000,POSITIVE",
        "5,6000,negative", "6,7000,negative", "7,8000,POSITIVE", "8,9000,negative",
        "9,11000,POSITIVE", "10,12000,POSITIVE", "11,13000,negative", "12,14000,POSITIVE"]) + "\n")
    (out / "windowed_large.csv").write_text("\n".join(
        f"{i},{i * 100}," + ("POSITIVE" if (i * 7) % 3 else "negative") for i in range(1, 761)) + "\n")

    print(f"wrote reviews_12/30/40/760.csv, plain_200.csv and windowed_small/large.csv to {out}")


if __name__ == "__main__":
    main()
