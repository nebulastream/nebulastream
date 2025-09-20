#!/usr/bin/env python

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
from collections import defaultdict


def collect_file(inp):

    patches_per_file = {}

    ct_tests = set()

    mutant_killed_by_ctest = defaultdict(set)
    mutant_killed_by = defaultdict(set)

    finished_mutants = set()

    all_mutations = set()
    cannot_build = set()
    cannot_apply = set()

    skip_strings = [
        "hunk FAILED",
        "build donezo",
        "tests written out to",
        " iter ",
        "assuring",
        "survived",
        "fluke",
        "Patches written to: mutations",
        "all done",
        "patch_types.txt written",
    ]

    last_checked = None

    for l in inp.split("\n"):
        if not l.strip():
            continue

        skip = False
        for s in skip_strings:
            if s in l:
                skip = True
        if skip:
            continue

        if l[0] == "[":
            continue

        if l[0] == "^":
            continue
        if l[0] == "\x1b":
            continue
        if l[0] == " ":
            continue
        if l[0] == "/":
            continue
        if l[0] == "-":
            continue

        words = l.split()

        if len(words) == 1 and words[0].startswith("ctestcase::"):
            ct_tests.add(words[0].split("::")[1])

        elif words[1] == "mutant" and words[4] == "non-zero" and words[-1] == "ctest":
            mutant = words[2]
            if mutant_killed_by_ctest[mutant]: # access inserts!
                raise RuntimeError("huh?!")

        elif words[1] == "mutant" and words[3] == "killed" and words[5] == "ctest":
            mutant = words[2]
            cttest = words[7]
            mutant_killed_by_ctest[mutant].add(cttest)
        
        elif words[1] == "mutant" and words[3] == "killed" and words[6] == "corpus":
            mutant = words[2]
            harness = words[5]
            mutant_killed_by[("corpus", harness)].add(mutant)

        elif words[1] == "mutant" and words[3] == "killed" and words[5] in {"honggfuzz", "libfuzzer"}:
            mutant = words[2]
            engine = words[5]
            harness = words[6]
            mutant_killed_by[(engine, harness)].add(mutant)
            print(f"check if plausible that {harness} {words[-1]} killed {mutant}")

        elif words[1] == "finished":
            finished_mutants.add(words[2])

        elif words[0] == "Generated":
            if words[4] not in patches_per_file:
                patches_per_file[words[4]] = words[1]
            elif patches_per_file[words[4]] != words[1] and "JITCompiler" not in words[4]:
                print(f"WARNING: differing no. of patches for file {words[4]}: {patches_per_file[words[4]]} != {words[1]}")
        
        elif words[0] == "Successfully":
            if words[2] != "43838":
                raise RuntimeError("Unexpected number of tests generated. Expected 43838 but got ", words[2]) 

        elif words[1] == "checking":
            last_checked = words[2]
            all_mutations.add(words[2])
        elif words[1] == "cannot" and words[2] == "build":
            cannot_build.add(words[3])
        elif words[1] == "cannot" and words[2] == "apply":
            cannot_apply.add(words[3])
        elif (words[1] == "running" or words[0] == "patching" or words[0] == "Hunk"
                  or len(words) > 2 and words[2] == "generated." or words[0] == "Refresh"
                  or words[0] == "mutanalysis.sh:"):
            pass
        else:
            print(l)

    if last_checked and last_checked not in finished_mutants:
        mutant_killed_by_ctest.pop(last_checked, None)
        for killer in mutant_killed_by:
            mutant_killed_by[killer].discard(last_checked)

        all_mutations.discard(last_checked)
        cannot_build.discard(last_checked)
        cannot_apply.discard(last_checked)

    for mutant, killers in mutant_killed_by_ctest.items():
        if not killers:
            print(f"warning: killer unknown for mutant {mutant}")
            killers.add("gardener")

    return patches_per_file, ct_tests, mutant_killed_by_ctest, mutant_killed_by, all_mutations, cannot_build, cannot_apply


def merge_set(a: set, b: set):
    if not a:
        return b
    if not b:
        return a
    if a == b:
        return a
    raise RuntimeError("merge_set")

def merge_dict(a: dict, b: dict):
    if not a:
        return b
    if not b:
        return a

    shared_keys = set(a.keys()).intersection(set(b.keys()))
    for k in shared_keys:
        if a[k] != b[k]:
            raise RuntimeError(f"Shared value differs for key {k}: {a[k]} != {b[k]}")
    
    return a | b

def merge_dict_warning(a: dict, b: dict, key_domain: set):
    if not a:
        return b

    a_shared = key_domain.intersection(a.keys())
    b_shared = key_domain.intersection(b.keys())

    for k in a_shared.symmetric_difference(b_shared):
        print("flaky?!", k)
    
    res = defaultdict(set)
    
    for k in a_shared.intersection(b_shared):
        if a[k] != b[k]:
            print(f"disagreement at key {k}. lens {len(a[k])} {len(b[k])}")
        res[k] = a[k].intersection(b[k])
    
    for k in set(a.keys()).difference(key_domain):
        res[k] = a[k]
    for k in set(b.keys()).difference(key_domain):
        res[k] = b[k]
    
    return res

def merge_set_warning(a: set, b: set, domain: set):
    if not a:
        return b

    a_shared = domain.intersection(a)
    b_shared = domain.intersection(b)

    for k in a_shared.symmetric_difference(b_shared):
        print("flaky?!", k)
    
    return set.union(a.difference(domain), b.difference(domain), a_shared.intersection(b_shared))

def merge_all(a, b):
    if not a:
        return b

    a_all_mutations: set

    a_patches_per_file, a_ct_tests, a_mutant_killed_by_ct, a_mutant_killed_by, a_all_mutations, a_cannot_build, a_cannot_apply = a
    b_patches_per_file, b_ct_tests, b_mutant_killed_by_ct, b_mutant_killed_by, b_all_mutations, b_cannot_build, b_cannot_apply = b

    if a_ct_tests and b_ct_tests and a_ct_tests != b_ct_tests:
        for t in a_ct_tests.symmetric_difference(b_ct_tests):
            if t.startswith("QueryTests/SystestE2ETest.correctAndIncorrectSchemaTestFile/64-byte_object_"):
                continue
            print(t)
    
    if not a_all_mutations <= b_all_mutations and not b_all_mutations <= a_all_mutations:
        pass
        # print("mutations not subset..")

    patches_per_file = merge_dict(a_patches_per_file, b_patches_per_file)

    shared_mutations = a_all_mutations.intersection(b_all_mutations)

    if shared_mutations.intersection(a_cannot_build) != shared_mutations.intersection(b_cannot_build):
        raise RuntimeError("Disagreement about which patches can be build")
    if shared_mutations.intersection(a_cannot_apply) != shared_mutations.intersection(b_cannot_apply):
        raise RuntimeError("Disagreement about which patches can be applied")

    cannot_build = a_cannot_build.union(b_cannot_build)
    cannot_apply = a_cannot_apply.union(b_cannot_apply)

    mutant_killed_by = defaultdict(set)
    
    mutant_killed_by_ct = merge_dict_warning(a_mutant_killed_by_ct, b_mutant_killed_by_ct, shared_mutations)
    for k in set.union(set(a_mutant_killed_by.keys()), set(b_mutant_killed_by.keys())):
        mutant_killed_by[k] = merge_set_warning(a_mutant_killed_by[k], b_mutant_killed_by[k], shared_mutations)

    return patches_per_file, a_ct_tests, mutant_killed_by_ct, mutant_killed_by, a_all_mutations.union(b_all_mutations), cannot_build, cannot_apply


def flip_dict(d: dict[str, set]) -> dict[str, set]:
    r = defaultdict(set)
    for k, vs in d.items():
        for v in vs:
            r[v].add(k)
    return r


def main():

    patches_per_file = {}

    yg_tests = set()
    ct_tests = set()

    ct_kills = defaultdict(set)

    mutant_killed_by_ct = defaultdict(set)
    mutant_killed_by = defaultdict(set)

    all_mutations = set()
    cannot_build = set()
    cannot_apply = set()

    merger = None

    for file in sys.argv[1:]:
        with open(file, encoding="utf-8") as f:
            inp = f.read()

        merger = merge_all(merger, collect_file(inp))
    
    patches_per_file, ct_tests, mutant_killed_by_ct, mutant_killed_by, all_mutations, cannot_build, cannot_apply = merger

    print("--------------------------------------")
    ct_unknown_tests = set(ct_kills.keys()).difference(ct_tests)
    if ct_unknown_tests:
        print("WARNING: uknown tests", ct_unknown_tests)
    print("--------------------------------------")

    applyable = set(all_mutations).difference(cannot_apply)
    buildable = set(all_mutations).difference(cannot_apply).difference(cannot_build)

    killed_by_fuzz_all = set.union(set(), *(v for v in mutant_killed_by.values()))
    killed_by_fuzz_act = set.union(set(), *(v for (mode, harness), v in mutant_killed_by.items() if mode != "corpus"))

    mutant_killed_by_crpus = set.union(set(), *(v for (m, h), v in mutant_killed_by.items() if m == "corpus"))
    mutant_killed_by_libfz = set.union(set(), *(v for (m, h), v in mutant_killed_by.items() if m == "libfuzzer"))
    mutant_killed_by_hngfz = set.union(set(), *(v for (m, h), v in mutant_killed_by.items() if m == "honggfuzz"))

    print("generated mutations:", len(all_mutations))
    print("applyable mutations:", len(applyable))
    print("buildable mutations:", len(buildable))
    print("   killed mutations:", len(buildable.intersection(set(mutant_killed_by_ct.keys()).union(killed_by_fuzz_all))))
    print("surviving mutations:", len(buildable.difference  (set(mutant_killed_by_ct.keys()).union(killed_by_fuzz_all))))
    print()
    print("killed by ctest:", len(mutant_killed_by_ct))
    print("killed by crpus:", len(mutant_killed_by_crpus))
    print("killed by libfz:", len(mutant_killed_by_libfz))
    print("killed by hngfz:", len(mutant_killed_by_hngfz))

    print("killed by ct \\ (crpus + libfz + hngfz)  ", len(set(mutant_killed_by_ct.keys()).difference(killed_by_fuzz_all)))
    print("killed by (crpus + libfz + hngfz) \\ ct  ", len(killed_by_fuzz_all.difference(set(mutant_killed_by_ct.keys()))))
    print("killed by (libfz + hngfz) \\ (ct + crpus)", len(killed_by_fuzz_act.difference(set(mutant_killed_by_ct.keys()).union(mutant_killed_by_crpus))))

    print()

    for harness in ["snw-proto-fuzz", "snw-strict-fuzz", "sql-parser-simple-fuzz"]:
        killed_by_harness = set.union(set(), *(v for (m, h), v in mutant_killed_by.items() if h == harness))
        print(f"killed by {harness:<22} {len(killed_by_harness):>2} {len(killed_by_harness.difference(mutant_killed_by_ct.keys())):>2}")

    print()

    for mode in ["corpus", "libfuzzer", "honggfuzz"]:
        for harness in ["snw-proto-fuzz", "snw-strict-fuzz", "sql-parser-simple-fuzz"]:
            print(f"killed by {mode:<9} {harness:<22} {len(set.union(set(), *(v for (m, h), v in mutant_killed_by.items() if m == mode and h == harness))):>2}")

    print()

    mutant_len = max(len(m) for m in buildable)

    ct_kills = flip_dict(mutant_killed_by_ct)

    ct_useless_tests = ct_tests.difference(ct_kills.keys())
    print("no. of seemingly useless ct tests:", len(ct_useless_tests))

    print()

    ct_top_killers = list(reversed(sorted(ct_kills.items(), key=lambda x: len(x[1]))))

    print("top 5 ct killers")
    for test, kills in ct_top_killers[:5]:
        print(f"{test:<50} {len(kills)}")

    mutant_killed_by_h_proto = set.union(set(), *(v for (m, h), v in mutant_killed_by.items() if h == "snw-proto-fuzz"))
    mutant_killed_by_h_strct = set.union(set(), *(v for (m, h), v in mutant_killed_by.items() if h == "snw-strict-fuzz"))
    mutant_killed_by_h_parsr = set.union(set(), *(v for (m, h), v in mutant_killed_by.items() if h == "sql-parser-simple-fuzz"))

    mutants_report = "report_mutants.txt"
    with open(mutants_report, "w", encoding="utf-8") as f:
        f.write(f"{"mutant":<{mutant_len}} {"ct":>3} corpus libfz hngfz proto strict parser")
        f.write("\n")
        for mutant in sorted(buildable):
            f.write(f"{mutant:<{mutant_len}} {len(mutant_killed_by_ct[mutant]) if mutant in mutant_killed_by_ct else 0:>3} {mutant in mutant_killed_by_crpus:>5} {mutant in mutant_killed_by_libfz:>5} {mutant in mutant_killed_by_hngfz:>5} {mutant in mutant_killed_by_h_proto:>5} {mutant in mutant_killed_by_h_strct:>5} {mutant in mutant_killed_by_h_parsr:>5}")
            f.write("\n")

    useless_ct_tests = "report_useless_ctests.txt"
    with open(useless_ct_tests, "w", encoding="utf-8") as f:
        for ct in sorted(ct_useless_tests):
            f.write(ct + "\n")

    print()
    print(f"{mutants_report}, {useless_ct_tests} written")

if __name__ == "__main__":
    main()
