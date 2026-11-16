# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Converts a vmprof profile into the collapsed stacks of flamegraph tools. Needs the vmprof package.

    python vmprof_folded.py <profile> > <file.folded>
"""

import sys

import vmprof


def name(node):
    # A vmprof name is "py:function:line:file" for Python code; keep the function, the line and the base name of the file
    parts = node.name.split(":")
    if len(parts) >= 4 and parts[0] == "py":
        return "%s (%s:%s)" % (parts[1], parts[3].rsplit("/", 1)[-1], parts[2])
    return node.name


def walk(node, prefix, out):
    stack = prefix + [name(node)]
    own = node.count - sum(child.count for child in node.children.values())
    if own > 0:
        out.write(";".join(stack) + " %d\n" % own)
    for child in node.children.values():
        walk(child, stack, out)


def main():
    stats = vmprof.read_profile(sys.argv[1])
    walk(stats.get_tree(), [], sys.stdout)


if __name__ == "__main__":
    main()
