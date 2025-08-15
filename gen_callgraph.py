# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from typing import Dict
from collections import defaultdict
import json
import sys
import subprocess
import multiprocessing
import itertools
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
            callers[cur_caller] = cur_callees
        elif line.startswith("Call graph node <<null function>>"):
            pass
        elif match := caller_regex.match(line):
            cur_caller = match[1]
            cur_callees = set()
        elif line.endswith(" calls external node"):
            pass
        elif match := callee_regex.match(line):
            callee = match[1].replace(".", "_")
            cur_callees.add(callee)
        else:
            print(line)
            raise RuntimeError("wat?!")

    return callers


def shitty_make_caching(source, result):
    src_stat = Path(source).stat()
    try:
        res_stat = Path(result).stat()
    except FileNotFoundError:
        return None

    assert src_stat.st_mtime >= src_stat.st_ctime
    assert res_stat.st_mtime >= res_stat.st_ctime

    # result is older than source
    if res_stat.st_mtime < src_stat.st_mtime:
        return None

    with open(result, encoding="utf-8") as f:
        return f.read()


def compute_callgraph(args):
    cmd_arr, cwd, out, in_cpp = args

    out_cg = cwd + "/" + out.replace(".bc", ".cg")
    if callgraph := shitty_make_caching(in_cpp, out_cg):
        return callgraph

    subprocess.run(cmd_arr, check=True, cwd=cwd)

    out = cwd + "/" + out
    assert Path(out).exists()

    callgraph = subprocess.run(["opt-19", "-passes=print-callgraph", "-disable-output", out], capture_output=True, text=True, check=True).stderr
    with open(out.replace(".bc", ".cg"), "w", encoding="utf-8") as f:
        f.write(callgraph)
    
    return callgraph


def dot_escapce(s: str):
    escapes = {
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
    }
    for cha, esc in escapes.items():
        s = s.replace(cha, esc)
    return s


def filter_fns(elgnamed, gcovr_json):

    def ignore_fn(name: str):
        ignored_fns = {
            "absl::",
            "folly",
            "Logger",
            "LogCaller",
            "_Test(",
            "Schema::~Schema",
            "grpc::internal",
            "google::protobuf::",
            "fmt::v11",
            "boost::asio::",
            "spdlog::details::",
            "std::__cxx11",
            "testing::internal::",
        }

        for ignored_fn in ignored_fns:
            if ignored_fn in name:
                return True
        return False

    fns = {}

    for f in gcovr_json["files"]:
        for fun in f["functions"]:
            if ignore_fn(fun["name"]):
                continue

            # check if we miss anything important here
            if fun["name"] not in elgnamed:
                if int(fun["blocks_percent"]):
                    print("name not in elgnamed ", int(fun["blocks_percent"]), fun["name"])
                continue

            demngld_name = fun["name"]
            fns[elgnamed[demngld_name]] = {"file": f["file"], "fun": fun}

    return fns


def cov_percent_to_color(value):
    value = max(0, min(100, value))  # Clamp value between 0 and 100

    if value <= 50:
        red = 255
        green = int(255 * (value / 50))
        blue = 0
    else:
        red = int(255 * (1 - (value - 50) / 50))
        green = 255
        blue = 0

    return f'#{red:02x}{green:02x}{blue:02x}'

def to_graph(callers, elgnamed, gcovr_json) -> str:

    drawn_fns = {}

    ret = []

    ret.append("digraph G {")
    ret.append("overlap = false;")
    ret.append("maxiter = 3;")

    drawn_fns = filter_fns(elgnamed, gcovr_json)

    act_drawn_fn = {}

    for drawn_fn in drawn_fns:
        act_drawn_fn[drawn_fn] = []
        if drawn_fn in callers:
            for callee in callers[drawn_fn]:
                if callee in drawn_fns:
                    act_drawn_fn[drawn_fn].append(callee)

    drawn_fn_set = set(fn for fn in act_drawn_fn if act_drawn_fn[fn])
    drawn_fn_set |= set(el for li in act_drawn_fn.values() for el in li)

    grouped_fns: Dict[str, list[str]] = defaultdict(list)
    for fn in drawn_fn_set:
        f = drawn_fns[fn]
        fun = f["fun"]
        gv_node = f'{fn} [label="{short_name(f["fun"]["name"])}", tooltip="{int(fun["blocks_percent"])}% {dot_escapce(f["fun"]["name"])}", color="{cov_percent_to_color(fun["blocks_percent"])}", shape=box, penwidth={5 if fun["blocks_percent"] else 2}];'
        grouped_fns[cluster_by_root_folder(f["file"])].append(gv_node)

    for group, nodes in grouped_fns.items():
        ret.append('subgraph cluster_' + group.replace("-", "_") + ' {')
        for node in nodes:
            ret.append(node)
        ret.append("}")

    for caller, callees in act_drawn_fn.items():
        for callee in callees:
            ret.append(f"{caller} -> {callee};")

    ret.append("}")

    return "\n".join(ret).replace("$", "_")


def to_treemap(callers, elgnamed, gcovr_json) -> str:
    ret = []
    ret.append("graph {")

    drawn_fns = filter_fns(elgnamed, gcovr_json)

    ret.append("}")
    return "\n".join(ret).replace("$", "_")



def short_name(name: str):
    name = name.replace("(anonymous namespace)", "anon")
    name = name.replace("[abi:cxx11]", "")

    if name.startswith("_Z"):
        return "mangled"

    if "(" not in name and "::" not in name:
        return name

    if "TestOneProtoInput" in name:
        return "TestOneProtoInput"

    if "assertFutureStatus" in name:
        return "assertFutureStatus"

    if match := re.findall(r"(\w*)(?:<.*>)?::(operator[\^%<>=!|+-/&*\[ ][+=&|\]]?(?:bool)?)", name):
        klass, meth = match[0]
        return klass + "::" + meth

    rx = re.compile(r"(\w*)(?:<.*>)?::(~?\w*)(?:<.*>)?\(")
    if match := rx.findall(name):
        klass, meth = match[0]
        return klass + "::" + meth

    print("could not shorten ", name)
    return name

def fn_len_from_line(line, lines, filename):
    assert lines[line-1]

    try:
        lines_loc = None
        if "{" in lines[line-1] and (
            lines[line-1].endswith("}\n")
            or lines[line-1].endswith("};\n")
            or lines[line-1].endswith("});\n")
            or lines[line].endswith("});\n")
            or lines[line].endswith("}));\n")
            ):
            return (line, line + 1)
        if lines[line-1].strip().startswith("class"):
            return (line, line + 1)
        if lines[line-1].endswith("default;\n"):
            return (line, line + 1)
        if lines[line-1].strip().startswith("ASSERT"):
            return (line, line + 1)
        if lines[line-1].strip().startswith("TEST"):
            return (line, line + 1)
        if lines[line-1].strip().startswith("MOCK"):
            return (line, line + 1)
        if lines[line-1].strip().startswith("INSTANTIATE"):
            return (line, line + 1)
        if lines[line-1].strip().startswith("FMT"):
            return (line, line + 1)
        if lines[line-1].strip().startswith("DEFINE_OPERATOR_VAR_VAL_BINARY"):
            return (line, line + 1)
        if lines[line-1].strip().startswith("DEFINE_OPERATOR_VAR_VAL_UNARY"):
            return (line, line + 1)
        if lines[line-1].strip().startswith("EXCEPTION"):
            return (line, line + 1)
        if "[](" in lines[line-1]:
            return (line, line + 1)
        if "[&](" in lines[line-1]:
            return (line, line + 1)

        # TODO
        if "[](" in lines[line]:
            return (line, line + 1)
        if "[&](" in lines[line]:
            return (line, line + 1)
        if "{" in lines[line] and (
            lines[line].endswith("}\n")
            or lines[line].endswith("};\n")
            or lines[line].endswith("});\n")
            ):
            return (line, line + 1)

        brace_open = None
        brace_clos = None
        for i in range(6):
            if lines[line + i].strip() == "{":
                brace_open = line + i
                break

        if not brace_open:
            print("could not find opening: ", filename + ":" + str(line))
            return None

        j = line + i
        while j < len(lines):
            if lines[j].strip() == "}":
                brace_clos = j
                break
            j += 1

        if brace_open and brace_clos:
            return (brace_open + 1, brace_clos + 1)
    except IndexError as e:
        print("tja", e, filename, line)
    print("could not find fn range: ", filename + ":" + str(line))


def is_overlap(a, b):
    al, ar = a
    bl, br = b

    assert al < ar
    assert bl < br

    if ar <= bl or br <= al:
        return False

    return True

def mk_fn_len_estimator(gcovr_json):
    ret = {} # {file: {line: len}}

    for file in gcovr_json["files"]:
        try:
            with open(file["file"]) as f:
                lines = f.readlines()
        except Exception as e:
            continue

        fn_locs = set()

        for fn in file["functions"]:
            if fn_loc := fn_len_from_line(fn["lineno"], lines, file["file"]):
                fn_locs.add(fn_loc)

        print(file["file"], fn_locs)

        for loc1, loc2 in itertools.combinations(fn_locs, 2):
            if is_overlap(loc1, loc2):
                print(f"WARN: fns 'overlap' in {file["file"]}, ranges {loc1} and {loc2}")
                break


    return ret


def cluster_nested(gcovr_json):
    ret = {}

    for file in gcovr_json["files"]:
        filepath = file["file"]
        filepath = filepath.replace("/include/", "/src/")
        if not file["file"].startswith("nes-sql"):
            continue
        cur = ret
        for path_step in file["file"].split("/"):
            if not path_step in ret:
                cur[path_step] = {}
            cur = cur[path_step]

        for fn in file["functions"]:
            line = fn["lineno"]
            if not line in cur:
                cur[line] = []
            cur[line].append(fn)
    return ret


def cluster_by_root_folder(file: str):
    prefix = file.split("/")[0]

    vcpkg_deps = [
        "absl",
        "antlr4-runtime",
        "argparse",
        "boost",
        "cpptrace",
        "fmt",
        "folly",
        "gmock",
        "grpcpp",
        "gtest",
        "magic_enum",
        "nautilus",
        "nlohmann",
        "protobuf",
        "replxx",
        "spdlog",
        "yaml-cpp",
    ]

    if prefix.startswith("nes-"):
        return prefix

    if "vcpkg_installed" in file:
        for dep in vcpkg_deps:
            if dep in file:
                return dep
        raise RuntimeError(f"Unknown dependency: {file}")
    elif "antlr4_generated_src" in file:
        return "gen-parser"
    elif "grpc_generated_src" in file:
        return "gen-grpc"
    elif "registry" in file:
        return "registry"
    else:
        raise RuntimeError(f"Unknown dependency: {file}")


def main():
    compile_cmds_file = sys.argv[1]
    with open(compile_cmds_file, encoding="utf-8") as f:
        compile_cmds = json.load(f)

    gcovr_json_file = sys.argv[2]
    with open(gcovr_json_file, encoding="utf-8") as f:
        gcovr_json = json.load(f)

    actual_version = gcovr_json["gcovr/format_version"]
    expect_version = "0.6"
    assert actual_version == expect_version, f"Version mismatch: got {actual_version}, expected {expect_version}"

    jobs = []

    output_regex = re.compile(" -o (.*) -c (.*)")

    for cmd in compile_cmds:
        c = cmd["command"].replace(".o ", ".bc ")
        c = c.replace('\\"', '"')
        out_file, in_file = output_regex.findall(c)[0]
        cmd_arr = c.split(" ")
        cmd_arr += ["-emit-llvm", "-O0"]
        jobs.append((cmd_arr, cmd["directory"], out_file, in_file))

    with multiprocessing.Pool(32) as p:
        callers = {}
        for callgraph in p.map(compute_callgraph, jobs):
            if callgraph:
                for caller, callees in parse_callgraph_text(callgraph).items():
                    if caller not in callers:
                        callers[caller] = callees
                    else:
                        callers[caller].update(callees)

    fns = set(callers.keys())
    fns.update(*callers.values())

    mangled_callers = "\n".join(fns)
    demngld_callers = subprocess.run("llvm-cxxfilt-19", input=mangled_callers, capture_output=True, text=True, check=True).stdout

    mangled_callers = mangled_callers.split("\n")
    demngld_callers = demngld_callers.split("\n")

    assert demngld_callers[-1] == ""
    assert len(mangled_callers) == len(demngld_callers) - 1

    demngld_callers = demngld_callers[:-1]

    elgnamed = dict(zip(demngld_callers, mangled_callers))

    graph = to_graph(callers, elgnamed, gcovr_json)

    with open("cov.dot", "w", encoding="utf-8") as f:
        f.write(graph)

    print("cov.dot written")


if __name__ == "__main__":
    main()
