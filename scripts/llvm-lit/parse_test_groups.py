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

import os
import re
import json
import argparse

def parse_test_groups(test_dir, file_endings):
    test_groups = {}
    for root, _, files in os.walk(test_dir):
        for file in files:
            if file.endswith(file_endings):
                file_path = os.path.join(root, file)
                with open(file_path, 'r') as f:
                    content = f.read()
                    groups = re.findall(r'# groups: \[(.*?)\]', content)
                    if groups:
                        groups = groups[0].split(',')
                        groups = [group.strip() for group in groups]
                        for group in groups:
                            if group not in test_groups:
                                test_groups[group] = []
                            test_groups[group].append(file_path)
    return test_groups

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Checks all test files in the test directory for groups and saves them to a JSON file.")
    parser.add_argument("--file_endings", help="Command-separated list of file endings to check for.")
    parser.add_argument("--test_dirs", help="Comma-separated list of folders containing test files.")
    parser.add_argument("--output", help="file to save the test groups json to.")
    args = parser.parse_args()

    test_dirs = args.test_dirs.split(",")
    file_endings = tuple(args.file_endings.split(","))
    for test_dir in test_dirs:
        test_groups = parse_test_groups(test_dir, file_endings)
        with open(args.output, 'w') as f:
            json.dump(test_groups, f, indent=2)
