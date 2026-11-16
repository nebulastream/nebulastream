# /// script
# dependencies = ["pyyaml"]
# ///
"""Expand a retry template into a composite action that retries one step.

Usage: uv run workflow-gen.py <name>/action.yml [...]

The template is the comment block at the top of the file. Everything after the marker line is
overwritten. See codecov/action.yml for an example.
"""

import re
import subprocess
import sys
import textwrap
import urllib.error
import urllib.request
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
HEAD = "# original workflow definition"
MARK = "# The following code is generated"


class NoAliasDumper(yaml.SafeDumper):
    # The attempts share one `with` map; write it out in full for each attempt, without YAML anchors.
    def ignore_aliases(self, data):
        return True


def attempt_steps(step, attempts, delay, backoff, retry_with):
    step = dict(step)
    step_id = step.pop("id", "attempt")
    first_if = step.pop("if", None)
    name = step.pop("name")
    result = []
    for n in range(1, attempts + 1):
        attempt = {"name": f"{name} (attempt {n}/{attempts})", "id": f"{step_id}-{n}"}
        if n == 1:
            if first_if is not None:
                attempt["if"] = first_if
        else:
            # A later attempt runs only if the attempt before it ran and failed.
            previous_failed = f"steps.{step_id}-{n - 1}.outcome == 'failure'"
            wait = delay * backoff ** (n - 2)
            result.append({"name": f"Wait {wait}s before attempt {n}", "if": previous_failed, "shell": "bash", "run": f"sleep {wait}"})
            attempt["if"] = previous_failed
        # Only the last attempt decides if the step fails.
        if n < attempts:
            attempt["continue-on-error"] = True
            result.append(attempt | step | {"with": step.get("with", {}) | retry_with})
        else:
            result.append(attempt | step)
    return result


def pin(uses):
    """Replace the tag or branch in `owner/repo[/path]@ref` with its commit SHA. Return the new `uses` and the old ref."""
    if uses.startswith(("./", "docker://")):
        return uses, None
    repo_path, ref = uses.split("@", 1)
    if re.fullmatch(r"[0-9a-f]{40}", ref):
        return uses, None
    owner, repo = repo_path.split("/")[:2]
    listing = subprocess.run(
        ["git", "ls-remote", f"https://github.com/{owner}/{repo}", f"refs/tags/{ref}", f"refs/tags/{ref}^{{}}", f"refs/heads/{ref}"],
        capture_output=True, text=True, check=True,
    ).stdout
    shas = dict(line.split("\t")[::-1] for line in listing.splitlines())
    # An annotated tag points to a tag object; its peeled entry (^{}) is the commit.
    sha = shas.get(f"refs/tags/{ref}^{{}}") or shas.get(f"refs/tags/{ref}") or shas.get(f"refs/heads/{ref}")
    if sha is None:
        sys.exit(f"{uses}: no tag or branch '{ref}' in github.com/{owner}/{repo}")
    return f"{repo_path}@{sha}", ref


def wrapped_inputs(uses):
    """Return the inputs of the wrapped action, with their defaults, `required` flags and descriptions."""
    if uses.startswith("docker://"):
        sys.exit(f"{uses}: a docker:// step has no action.yml to read inputs from")
    names = ("action.yml", "action.yaml")
    text = None
    if uses.startswith("./"):
        found = [ROOT / uses / name for name in names if (ROOT / uses / name).exists()]
        text = found[0].read_text() if found else None
    else:
        repo_path, ref = uses.split("@", 1)
        owner, repo, *sub = repo_path.split("/")
        for name in names:
            url = f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}/{'/'.join([*sub, name])}"
            try:
                text = urllib.request.urlopen(url, timeout=30).read().decode()
                break
            except urllib.error.HTTPError as error:
                if error.code != 404:
                    raise
    if text is None:
        sys.exit(f"{uses}: no action.yml or action.yaml found")

    inputs = yaml.safe_load(text).get("inputs") or {}
    # The runner warns for each deprecated input that a step sets, so do not pass them through.
    return {
        name: {key: spec[key] for key in ("description", "required", "default") if key in spec}
        for name, spec in inputs.items()
        if "deprecationMessage" not in spec
    }


def generate(path):
    lines = path.read_text().splitlines()
    end = next((i for i, line in enumerate(lines) if line.startswith(MARK)), None)
    if not lines or lines[0] != HEAD or end is None:
        sys.exit(f"{path}: the file must start with '{HEAD}' and contain a line that starts with '{MARK}'")

    template = lines[:end]
    spec = yaml.safe_load(textwrap.dedent("\n".join(line[1:] for line in template[1:])))
    step = dict(spec["step"])
    retry = step.pop("x-retry")
    step.setdefault("name", step["uses"])
    original_uses = step["uses"]
    step["uses"], ref = pin(original_uses)

    # Read the inputs at the pinned commit, so that they match the code that runs.
    inputs = wrapped_inputs(step["uses"])
    # The template `with` overrides the pass-through of an input, e.g. to set a constant.
    step["with"] = {name: f"${{{{ inputs.{name} }}}}" for name in inputs} | step.get("with", {})

    # retry_with: `with` values for every attempt except the last, e.g. to make an attempt fail on an error it would otherwise ignore.
    steps = attempt_steps(step, retry["attempts"], retry.get("delay", 0), retry.get("backoff", 1), retry.get("retry_with", {}))
    action = {
        "name": step["name"],
        "description": f"Run {original_uses} and retry it on failure. Generated by workflow-gen.py.",
        "inputs": inputs,
        "runs": {"using": "composite", "steps": steps},
    }
    body = yaml.dump(action, Dumper=NoAliasDumper, sort_keys=False, width=float("inf"))
    if ref is not None:
        body = body.replace(f"uses: {step['uses']}\n", f"uses: {step['uses']} # {ref}\n")

    unknown = set(re.findall(r"inputs\.([\w-]+)", body)) - set(inputs)
    if unknown:
        sys.exit(f"{path}: the step uses inputs that {step['uses']} does not define: {sorted(unknown)}")

    marker = f"{MARK} from the template above. To regenerate it, run: uv run workflow-gen.py {path}"
    path.write_text("\n".join(template + [marker, ""]) + body)


for arg in sys.argv[1:]:
    generate(Path(arg))
