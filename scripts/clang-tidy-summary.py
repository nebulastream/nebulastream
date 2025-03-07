from collections import defaultdict
import argparse
import json
import os


def get_non_err_checks(conf: str) -> set[str]:
    """
    Returns all clang tidy checks, that are NOT considered errors (i.e. only warnings).

    Note: This assumes that WarningsAsErrors starts with `*`, i.e. all warnings should be
    turned into errors and then lists exceptions (thus the leading `-`).
    """
    in_warnings_as_err_list = False
    non_err_checks = set()
    for line in conf.split("\n"):
        line = line.strip()
        if "WarningsAsErrors:" in line:
            in_warnings_as_err_list = True
        if in_warnings_as_err_list and line == "'":
            # end of WarningsAsErrors
            break
        if line.startswith("-"):
            line = line.lstrip("-")
            line = line.rstrip(",")
            non_err_checks.add(line)

    return non_err_checks


def get_violations(clang_tidy_output: str) -> dict[str, set[str]]:
    """
    Returns violations of clang-tidy-checks, with common path prefix removed.

    The result looks like this:
    ```
    {
      "bugprone-unused-return-value": {
           "nes-query-engine/tests/QueryPlanTest.cpp:390:5",
           "nes-single-node-worker/tests/Integration/SingleNodeIntegrationTestsMixedSources.cpp:96:9"
      },
      "misc-unconventional-assign-operator": {
        "nes-client/src/API/Functions/Functions.cpp:105:1",
        "nes-client/src/API/Functions/Functions.cpp:110:1"
      }
    }
    ```
    """
    violations = defaultdict(set)

    for line in clang_tidy_output.split("\n"):
        if line.startswith("/") and line.endswith("]"):
            line = line.split(" ")
            file_loc = line[0].rstrip(":")
            warnings = line[-1]

            assert warnings[0] == "[" and warnings[-1] == "]"
            warnings = warnings[1:-1].split(",")

            for warning in warnings:
                violations[warning].add(file_loc)

    file_paths = list(set.union(*violations.values()))
    path_prefix = os.path.commonprefix(file_paths)
    return {
        warning: set(map(lambda loc: loc[len(path_prefix) :], locs))
        for warning, locs in violations.items()
    }


def main():
    parser = argparse.ArgumentParser(description="summarizes clang-tidy output")
    parser.add_argument(
        "-o",
        "--summary-json",
        help="file to put summary json",
        type=argparse.FileType("w", encoding="utf-8"),
    )
    parser.add_argument(
        "config",
        help="path to .clang-tidy config file",
        type=argparse.FileType("r", encoding="utf-8"),
    )
    parser.add_argument(
        "log",
        help="path to run-clang-tidy output",
        type=argparse.FileType("r", encoding="utf-8"),
    )
    args = parser.parse_args()

    warn_occurrences = get_violations(args.log.read())

    warnings = set(warn_occurrences.keys())
    non_err_checks = get_non_err_checks(args.config.read())

    not_occuring_warnings = non_err_checks.difference(warnings)

    print("# clang tidy summary")
    print()

    if not_occuring_warnings:
        print("## Warnings that can become errors")
        print()
        print("The following warnings did not appear in this run.")
        print("They can be turned into errors to fully prevent them by")
        print("deleting them from the WarningsAsErrors exclusion list in .clang-tidy")
        print()
        for w in not_occuring_warnings:
            print(f"- `{w}`")
        print()

    if args.summary_json:
        json.dump(
            # turn set into list since set cannot be json.dump'ed
            {w: list(locs) for w, locs in warn_occurrences.items()},
            args.summary_json,
        )

    warn_counts = [(warning, len(locs)) for warning, locs in warn_occurrences.items()]
    warn_counts = sorted(warn_counts, key=lambda x: x[1])

    # indent by 4 more lines to have markdown code block
    indent = len(str(warn_counts[-1][1])) + 4

    print("## clang-tidy warning occurences")
    print()
    print("See `clang-tidy-summary.json` artifact for all occurence locations.")
    print()
    for warning, count in warn_counts:
        print(f"{count:>{indent}} {warning}")
        if count <= 5:
            for loc in sorted(warn_occurrences[warning]):
                print(" " * indent + "  ", loc)
    print()


if __name__ == "__main__":
    main()
