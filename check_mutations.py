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

def check_patches(args):
    patches, compile_commands = args

    with open(patches[0], encoding="utf-8") as pfile:
        first_line = pfile.read(1000).split("\n")[0]
        filename = first_line.split(" ")[1]
    
    with open(filename, encoding="utf-8") as ofile:
        orig_file = ofile.read()
    main_dir = os.getcwd()

    for patch in patches:
            
        dir = compile_commands[filename]["dir"]
        cmd = compile_commands[filename]["cmd"]

        subprocess.run(["patch", filename, patch], text=True, check=True, capture_output=True)
        os.chdir(dir)

        try:
            subprocess.run(cmd.split(" "), text=True, check=True, capture_output=True)
            print("y", patch, flush=True)
        except subprocess.CalledProcessError:
            print("n", patch, flush=True)

        os.chdir(main_dir)
        with open(filename, "w", encoding="utf-8") as reset_file:
            reset_file.write(orig_file)


def main():
    mutation_dir = Path(sys.argv[1])

    with open(sys.argv[2]) as compile_commands_json:
        compile_commands = json.load(compile_commands_json)

    compile_cmds = {cmd["file"]: {"dir": cmd["directory"], "cmd": cmd["command"]} for cmd in compile_commands}

    dirs = []
    for d in mutation_dir.iterdir():
        dirs.append(
            ([patch for patch in d.glob("*.patch")], compile_cmds)
        )

    with multiprocessing.Pool() as mutation_dir:
        mutation_dir.map(check_patches, dirs)

if __name__ == "__main__":
    main()
