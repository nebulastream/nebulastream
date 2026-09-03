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

"""Validate the Rust workspace dependency policy used by dependency images."""

from __future__ import annotations

import argparse
import glob
import os
import shutil
import sys
from pathlib import Path
from typing import Any, Iterable

try:
    import tomllib
except ModuleNotFoundError:
    interpreter = None
    if not os.environ.get("NES_RUST_POLICY_REEXEC"):
        for minor in range(11, 30):
            interpreter = shutil.which(f"python3.{minor}")
            if interpreter:
                print(
                    f"Default python3 {sys.version_info.major}.{sys.version_info.minor} has no required tomllib, "
                    f"using python3 3.{minor} instead...",
                    file=sys.stderr,
                )
                os.environ["NES_RUST_POLICY_REEXEC"] = "1"
                os.execv(interpreter, [interpreter, *sys.argv])
    if interpreter is None:
        print(
            "Python 3.11 or newer is required to validate Rust workspace dependencies.",
            file=sys.stderr,
        )
    sys.exit(2)


DEPENDENCY_SECTIONS = ("dependencies", "dev-dependencies", "build-dependencies")


def load_manifest(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as manifest:
            return tomllib.load(manifest)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValueError(f"cannot read {path}: {error}") from error


def dependency_sections(manifest: dict[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    for section_name in DEPENDENCY_SECTIONS:
        section = manifest.get(section_name, {})
        if isinstance(section, dict):
            yield section_name, section

    targets = manifest.get("target", {})
    if not isinstance(targets, dict):
        return
    for target_name, target in targets.items():
        if not isinstance(target, dict):
            continue
        for section_name in DEPENDENCY_SECTIONS:
            section = target.get(section_name, {})
            if isinstance(section, dict):
                yield f"target.{target_name}.{section_name}", section


def workspace_manifests(root: Path, workspace: dict[str, Any]) -> list[Path]:
    manifests: set[Path] = set()
    for member_pattern in workspace.get("members", []):
        matches = glob.glob(str(root / member_pattern))
        if not matches:
            raise ValueError(f"workspace member pattern matches nothing: {member_pattern}")
        for match in matches:
            member_path = Path(match)
            manifest_path = member_path if member_path.name == "Cargo.toml" else member_path / "Cargo.toml"
            if not manifest_path.is_file():
                raise ValueError(f"workspace member has no Cargo.toml: {member_path.relative_to(root)}")
            manifests.add(manifest_path)
    return sorted(manifests)


def validate(root: Path) -> list[str]:
    root_manifest_path = root / "Cargo.toml"
    root_manifest = load_manifest(root_manifest_path)
    workspace = root_manifest.get("workspace")
    if not isinstance(workspace, dict):
        return ["root Cargo.toml does not define [workspace]"]

    workspace_dependencies = workspace.get("dependencies", {})
    if not isinstance(workspace_dependencies, dict):
        return ["root Cargo.toml does not define [workspace.dependencies]"]

    used_dependencies: set[str] = set()
    errors: list[str] = []
    try:
        member_manifests = workspace_manifests(root, workspace)
    except ValueError as error:
        return [str(error)]

    for manifest_path in member_manifests:
        relative_path = manifest_path.relative_to(root)
        try:
            manifest = load_manifest(manifest_path)
        except ValueError as error:
            errors.append(str(error))
            continue

        for section_name, dependencies in dependency_sections(manifest):
            for dependency_name, specification in dependencies.items():
                location = f"{relative_path} [{section_name}].{dependency_name}"
                if not isinstance(specification, dict) or specification.get("workspace") is not True:
                    errors.append(f"{location} must be declared as {{ workspace = true }}")
                    continue
                if dependency_name not in workspace_dependencies:
                    errors.append(
                        f"{location} is inherited but missing from root [workspace.dependencies]"
                    )
                    continue
                used_dependencies.add(dependency_name)

    for dependency_name in sorted(workspace_dependencies.keys() - used_dependencies):
        errors.append(
            f"Cargo.toml [workspace.dependencies].{dependency_name} is not used by any workspace member"
        )

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Require member dependencies to be inherited and workspace dependencies to be used."
    )
    parser.add_argument(
        "root",
        nargs="?",
        type=Path,
        default=Path.cwd(),
        help="repository root containing Cargo.toml (default: current directory)",
    )
    args = parser.parse_args()

    try:
        errors = validate(args.root.resolve())
    except ValueError as error:
        errors = [str(error)]

    if not errors:
        return 0

    print("Rust workspace dependency policy violations:", file=sys.stderr)
    for error in errors:
        print(f"  - {error}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
