#!/usr/bin/env python
# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import os
import subprocess
import sys

if __name__ == "__main__":
    """ Runs all tests from the /tests/ folder, after
    cmake is run. Needs a path for the qemu binary, sysroot
    path, and the test binaries.
    
    We run the tests like so:
        "qemu-aarch64 -L /path/to/libs path/to/compiled-tests".
    """
    parser = argparse.ArgumentParser(
        description="Runs clang format on all of the source "
                    "files. If --fix is specified,  and compares the output "
                    "with the existing file, outputting a unifiied diff if "
                    "there are any necessary changes")
    parser.add_argument("qemu_binary",
                        help="Path to the clang-format binary")
    parser.add_argument("sysroot_dir",
                        help="Path to the clang-format binary")
    parser.add_argument("tests_dir",
                        help="Path to the clang-format binary")
    parser.add_argument("--quiet", default=False,
                        action="store_true",
                        help="If specified, only print errors")

    args = parser.parse_args()

    success = True
    failed_tests = []
    for root, dirnames, filenames in os.walk(args.tests_dir): # all files in /build/tests
        for file in filenames:
            if file.endswith(('-tests', '-test')): # only files that are actually tests
                if not args.quiet:
                    print("Running {} with {}".format(file, args.qemu_binary))
                try:
                    # qemu-aarch64 -L /path/to/libs path/to/compiled-tests
                    stdout = subprocess.run([args.qemu_binary,
                                           "-L",
                                           args.sysroot_dir,
                                           os.path.join(root, file)],
                                   capture_output=True, text=True).stdout
                    print(stdout)
                except Exception as e:
                    success = False
                    failed_tests.append(file) # report later
                    pass # move to next, don't fail

    for failure in failed_tests:
        print("Running {} with {} failed!".format(failure, args.qemu_binary)) # report failed tests

    sys.exit(success)