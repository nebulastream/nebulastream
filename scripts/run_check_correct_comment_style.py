#!/usr/bin/env python

# This script checks that all comments in the source code are correctly formatted.
# The path to all folders containing source code is passed as an argument to the script.
# Additionally, the not allowed comment style is passed as an argument to the script.

import argparse
import os
import re


# We check that all comments in the source code are correctly formatted. If not, we return the files with errors.
# We consider that a comment is not correctly formatted if it contains the not allowed comment style.
# We check for the not allowed comment style in all files with the following endings: .c, .h, .cpp, .hpp, .cu, .cuh.
def check_correct_comment_style(source_folder, not_allowed_comment_style_regex, file_endings=[".c", ".h", ".cpp", ".hpp", ".cu", ".cuh"]):
    regex = re.compile(fr"{not_allowed_comment_style_regex}", re.MULTILINE)
    file_has_error = set([])
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            if file.endswith(tuple(file_endings)):
                with open(os.path.join(root, file), "r") as f:
                    for i, line in enumerate(f):
                        if "clang-format off" in line:
                            continue
                        if regex.findall(line):
                            print("Error: Not allowed comment style in file: " + os.path.join(root, file) + ", line: " + str(i + 1))
                            file_has_error.add(os.path.join(root, file))
    return file_has_error


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check that all comments in the source code are correctly formatted. "
                                                 "Return 0 if no wrongly formatted comments are found, 1 otherwise.")
    parser.add_argument("--source_dirs", help="Comma-separated list of folders containing source code.")
    parser.add_argument("--not_allowed_comment_style", help="Not allowed comment style.")
    args = parser.parse_args()

    source_dirs = args.source_dirs.split(",")
    not_allowed_comment_style = args.not_allowed_comment_style
    had_error = False
    files_with_error = set([])
    for source_folder in source_dirs:
        if len(source_folder) > 0 and os.path.exists(source_folder):
            print(f"Checking source code in folder: {source_folder}...")
            files_with_error.update(check_correct_comment_style(source_folder, not_allowed_comment_style))

    if len(files_with_error) > 0:
        had_error = True
        print(f"Error: Not allowed comment style in the following files:")
        for file in files_with_error:
            print(f"- {file}")
        print()
    else:
        print("All comments are correctly formatted.")

    exit(0 if not had_error else 1)
