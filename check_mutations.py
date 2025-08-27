#!/usr/bin/env python3

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import multiprocessing
import json
import subprocess
import os

from pathlib import Path


def get_patched_filename(patch):
    with open(patch, encoding="utf-8") as pfile:
        first_line = pfile.read(1000).split("\n")[0]
        filename = first_line.split(" ")[1]
    return filename


def check_patch(compile_commands, filename, orig_file, main_dir, patch):
    dir = compile_commands[filename]["dir"]
    cmd = compile_commands[filename]["cmd"]

    subprocess.run(["patch", filename, patch], text=True, check=True, capture_output=True)
    os.chdir(dir)

    try:
        subprocess.run(cmd.split(" "), text=True, check=True, capture_output=True)
        compiles = True
    except subprocess.CalledProcessError:
        compiles = False

    os.chdir(main_dir)
    with open(filename, "w", encoding="utf-8") as reset_file:
        reset_file.write(orig_file)
    return compiles


def check_patches(args):
    patches, compile_commands = args

    filename = get_patched_filename(patches[0])
    
    with open(filename, encoding="utf-8") as ofile:
        orig_file = ofile.read()
    main_dir = os.getcwd()

    for patch in patches:
        compiles = check_patch(compile_commands, filename, orig_file, main_dir, patch)
        print("y" if compiles else "n", patch, flush=True)


def check_mutation_dir(mutation_dir, compile_cmds):
    dirs = []
    for d in mutation_dir.iterdir():
        dirs.append(
            ([patch for patch in d.glob("*.patch")], compile_cmds)
        )

    with multiprocessing.Pool() as mutation_dir:
        mutation_dir.map(check_patches, dirs)


def main():
    mutation_thingy = Path(sys.argv[1])

    with open(sys.argv[2]) as compile_commands_json:
        compile_commands = json.load(compile_commands_json)

    compile_cmds = {cmd["file"]: {"dir": cmd["directory"], "cmd": cmd["command"]} for cmd in compile_commands}

    if mutation_thingy.is_dir():
        check_mutation_dir(mutation_thingy, compile_cmds)
    elif mutation_thingy.is_file():
        filename = get_patched_filename(mutation_thingy)
        with open(filename, encoding="utf-8") as ofile:
            orig_file = ofile.read()
        compiles = check_patch(compile_cmds, filename, orig_file, os.getcwd(), mutation_thingy)
        sys.exit(0 if compiles else 1)


if __name__ == "__main__":
    main()
