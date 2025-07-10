# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import json
import sys
import os
import subprocess
import multiprocessing
from pathlib import Path
import re


def parse_callgraph_text(cg):
    callers = {}
    cur_caller = ""
    cur_callees = set()

    caller_regex = re.compile("Call graph node for function: '(.*)'<<0x[0-9a-f]*>>  #uses=.*")
    callee_regex = re.compile("  CS<.*?> calls function '(.*)'")

    for line in cg.split("\n"):
        if line == "":
            if cur_callees and cur_caller.startswith("_ZN3NES"):
                callers[cur_caller] = cur_callees
        elif line.startswith("Call graph node <<null function>>"):
            pass
        elif line.startswith("Call graph node for function"):
            cur_caller = caller_regex.match(line)[1]
            cur_callees = set()
        elif line.endswith(" calls external node"):
            pass
        elif line.startswith("  CS"):
            callee = callee_regex.match(line)[1]
            if callee.startswith("_ZN3NES"):
                cur_callees.add(callee)
        else:
            print(line)
            raise Exception("wat?!")

    return callers


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
    gcovr_json_file = sys.argv[1]

    with open(gcovr_json_file) as f:
        gcovr_json = json.load(f)

    for f in gcovr_json["files"]:
        for fun in f["functions"]:
            if fun["blocks_percent"] > 0:
                print(fun)

    callers = {}
    for cg_file in sys.argv[2:]:
        with open(cg_file) as f:
            callers = callers | parse_callgraph_text(f.read())

    mangled_callers = "\n".join(callers.keys())
    demngld_callers = subprocess.run("c++filt", input=mangled_callers, capture_output=True, text=True, check=True).stdout

    mangled_callers = mangled_callers.split("\n")
    demngld_callers = demngld_callers.split("\n")

    print(mangled_callers[9])
    print(demngld_callers[9])
