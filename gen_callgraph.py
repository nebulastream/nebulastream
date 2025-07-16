import json
import sys
import os
import subprocess
import multiprocessing
from pathlib import Path
import re

def foo(args):
    cmd_arr, cwd, out = args
    subprocess.run(cmd_arr, check=True, cwd=cwd)

    out = cwd + "/" + out.replace(".o", ".bc")

    if not Path(out).exists():
        print(out)

    callgraph = subprocess.run(["opt-19", "-passes=print-callgraph", "-disable-output", out], capture_output=True, text=True, check=True).stderr
    with open(out.replace(".bc", ".cg"), "w") as f:
        f.write(callgraph)
    
    return callgraph



def main():
    compile_cmds_file = sys.argv[1]
    with open(compile_cmds_file) as f:
        compile_cmds = json.load(f)

    jobs = []

    output_regex = re.compile(" -o (.*) -c ")

    for cmd in compile_cmds:
        if "_generated_src" in cmd["output"]:
            continue

        c = cmd["command"].replace(".o ", ".bc ")
        c = c.replace('\\"', '"')
        out_file = output_regex.findall(c)[0]
        cmd_arr = c.split(" ")
        cmd_arr += ["-emit-llvm", "-O0"]
        jobs.append((cmd_arr, cmd["directory"], out_file))

    with multiprocessing.Pool(32) as p:
        for res in p.map(foo, jobs):
            pass


if __name__ == "__main__":
    main()
