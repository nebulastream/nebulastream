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

"""Generate GitHub Actions matrix JSON from a YAML matrix config.

Usage:
    python generate_matrix.py --config matrix.yaml --profile pr            # pretty-print
    python generate_matrix.py --config matrix.yaml --profile nightly --output-github

The script reads a YAML config (same directory) and produces a JSON array.
Each element maps directly to one GitHub Actions job.

Matrix fields (arch, stdlib, build_type, sanitizer) can be arrays, in
which case the script expands them into the cross product. An optional
"exclude" list filters out specific combinations, matching the same
semantics as GitHub Actions matrix excludes.

The "name" field supports {field} placeholders that are replaced with
the actual values from each expanded combination.

A single config file can serve multiple profiles (e.g. "pr" and "nightly").
Each job runs in every profile by default. Two profile-keyed knobs narrow
that down:

  - job["profiles"]: restricts the whole job to the listed profile(s).
  - job["overrides"][profile]: shallow-merges fields onto the job's common
    fields for that profile only (e.g. a different build_type list).

profile_cmake_flags[profile] adds/overrides cmake flags for one profile on
top of default_cmake_flags, before per-job cmake_flags are applied.
"""

import argparse
import itertools
import json
import os
import sys

import yaml


def load_config(config_file):
    here = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(here, config_file)
    with open(config_path) as f:
        return yaml.safe_load(f)


def ensure_list(value):
    """Wrap scalars in a list, pass lists through."""
    if isinstance(value, list):
        return value
    return [value]


def matches_exclude(combo, exclude):
    """Check if a combo matches an exclude rule (all fields in the rule must match)."""
    return all(combo.get(k) == v for k, v in exclude.items())


def expand_job(job, runners, default_cmake_flags=""):
    """Expand a single job definition into one or more matrix entries."""
    expandable_fields = ["arch", "stdlib", "build_type", "sanitizer"]

    # Collect the values for each field (may be scalar or list)
    field_values = {}
    for field in expandable_fields:
        field_values[field] = ensure_list(job[field])

    # Cross product of all field values
    keys = expandable_fields
    combos = list(itertools.product(*(field_values[k] for k in keys)))

    excludes = job.get("exclude", [])
    tests = job.get("tests", [])
    name_template = job["name"]
    # Merge default cmake flags with per-job overrides (job wins on conflict)
    merged_flags = dict(default_cmake_flags)
    merged_flags.update(job.get("cmake_flags", {}))
    cmake_flags = " ".join(f"-D{k}={v}" for k, v in merged_flags.items())
    runner_override = job.get("runner", None)
    entries = []
    for combo_values in combos:
        combo = dict(zip(keys, combo_values))

        # Apply excludes
        if any(matches_exclude(combo, exc) for exc in excludes):
            continue

        # Resolve runner: required field, supports {field} placeholders
        # that are looked up in the top-level "runners" map.
        if runner_override is None:
            print(
                f"ERROR: missing 'runner' in job '{name_template}'",
                file=sys.stderr,
            )
            continue

        runner_key = runner_override.format(**combo)
        runner = runners.get(runner_key)
        if runner is None:
            print(
                f"WARNING: runner '{runner_key}' not found in runners map "
                f"(job '{name_template}')",
                file=sys.stderr,
            )
            continue

        # Format the name with field values
        name = name_template.format(**combo)

        entries.append(
            {
                "name": name,
                "arch": combo["arch"],
                "stdlib": combo["stdlib"],
                "build_type": combo["build_type"],
                "sanitizer": combo["sanitizer"],
                "runner": runner,
                "cmake_flags": cmake_flags,
                "run_unit": "unit" in tests,
                "run_systest": "systest" in tests,
                "run_docker": "docker" in tests,
            }
        )

    return entries


def select_profile(job, profile):
    """Return the job as it applies to `profile`, or None if it doesn't apply.

    Common fields come first; profile-specific differences are branched in
    via `overrides[profile]`, shallow-merged on top.
    """
    allowed_profiles = job.get("profiles")
    if allowed_profiles is not None and profile not in allowed_profiles:
        return None

    resolved = dict(job)
    resolved.update(job.get("overrides", {}).get(profile, {}))
    return resolved


def generate_matrix(config, profile):
    known_profiles = config.get("profiles", [])
    if profile not in known_profiles:
        print(
            f"ERROR: unknown profile '{profile}', expected one of {known_profiles}",
            file=sys.stderr,
        )
        sys.exit(1)

    runners = config["runners"]
    default_cmake_flags = config.get("default_cmake_flags", {})
    profile_cmake_flags = config.get("profile_cmake_flags", {}).get(profile, {})
    merged_default_flags = dict(default_cmake_flags)
    merged_default_flags.update(profile_cmake_flags)

    matrix = []

    for job in config["jobs"]:
        # Skip comment-only entries
        if "arch" not in job:
            continue

        resolved_job = select_profile(job, profile)
        if resolved_job is None:
            continue

        matrix.extend(expand_job(resolved_job, runners, merged_default_flags))

    return matrix


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-github",
        dest="output_github",
        action="store_true",
        help="Write matrix JSON to $GITHUB_OUTPUT as 'matrix' variable",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Config file name in .github/ci/ (e.g. matrix.yaml)",
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="Profile to generate (e.g. pr or nightly)",
    )
    args = parser.parse_args()

    config = load_config(args.config)
    matrix = generate_matrix(config, args.profile)

    matrix_json = json.dumps(matrix, separators=(",", ":"))

    if args.output_github:
        github_output = os.environ.get("GITHUB_OUTPUT", "")
        if not github_output:
            print("ERROR: GITHUB_OUTPUT not set", file=sys.stderr)
            sys.exit(1)
        with open(github_output, "a") as f:
            f.write(f"matrix={matrix_json}\n")
        print(f"Wrote {len(matrix)} jobs to GITHUB_OUTPUT", file=sys.stderr)
    else:
        print(json.dumps(matrix, indent=2))

    return 0


if __name__ == "__main__":
    sys.exit(main())
