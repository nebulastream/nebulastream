#!/usr/bin/env python3

import subprocess
import sys

if __name__ == "__main__":
    git_ls_files = subprocess.run(["git", "ls-files"], capture_output=True, text=True)
    result = True
    files = git_ls_files.stdout.split("\n")

    for filename in files:
        filename = filename.strip()
        if filename.endswith((".cpp", ".hpp", ".h")):
            with open(filename, 'r', encoding='utf-8') as fp:
                content = fp.read()
                lines = content.split("\n")

                for line in lines:
                    line = line.strip()
                    if line.startswith("#include "):
                        path = line.split(" ", 1)[1]
                        if not (path.startswith("<") and path.endswith(">")):
                            result = False
                            print(f'{filename} uses double quotes instead of angle brackets in includes.')
                            break

    if not result:
        sys.exit(1)

    sys.exit(0)

        




    

