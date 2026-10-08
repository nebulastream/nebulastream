#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# ///
"""Dispatch the reduced-nightly "Flaky Hunt" workflow whenever CI is (nearly) idle.

Each round: wait until few self-hosted jobs are active -> publish <base>+this branch's CI changes as
`--target-branch` (force-push!) -> dispatch flaky-hunt.yml -> wait for it -> print failed jobs.
Needs `gh` (authenticated, push access) and git. No PR is ever opened.

    tools/flaky_monitor.py [--base main] [--max-busy 2] [--poll 120]
"""
import argparse, json, os, re, shutil, subprocess, tempfile, time

SRC = ".github/workflows/flaky-hunt.yml"
# workflow_dispatch only works for workflow files that exist on the default branch, so on the throw-away
# branch we dispatch an existing one (nightly.yml) whose content is replaced by SRC. Runs are told apart by branch.
WF = "nightly.yml"


def sh(*cmd, cwd=None):
    return subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=cwd).stdout.strip()


def gh(path):
    return json.loads(sh("gh", "api", "--paginate", "--slurp", path))


def runs(status):
    return [r for page in gh(f"repos/{{owner}}/{{repo}}/actions/runs?status={status}&per_page=100")
            for r in page["workflow_runs"]]


def busy_self_hosted(target):
    """Self-hosted jobs queued or running, across all active runs (ours excluded by caller)."""
    n = 0
    for r in runs("in_progress") + runs("queued"):
        if r["head_branch"] == target:
            continue
        for page in gh(f"repos/{{owner}}/{{repo}}/actions/runs/{r['id']}/jobs?per_page=100"):
            n += sum(j["status"] in ("queued", "in_progress") and "self-hosted" in j["labels"]
                     for j in page["jobs"])
    return n


def publish(base, target):
    """target = origin/<base> merged with the current branch (workflow + matrix profile)."""
    mine = sh("git", "rev-parse", "HEAD")
    sh("git", "fetch", "-q", "origin", base)
    with tempfile.TemporaryDirectory() as tmp:
        wt = os.path.join(tmp, "wt")
        sh("git", "worktree", "add", "-q", "--detach", wt, f"origin/{base}")
        try:
            sh("git", "merge", "-q", "--no-edit", mine, cwd=wt)  # conflict -> CalledProcessError
            shutil.copy(os.path.join(wt, SRC), os.path.join(wt, ".github/workflows", WF))
            sh("git", "commit", "-qam", "flaky-hunt: replace nightly.yml", cwd=wt)
            sha = sh("git", "rev-parse", "HEAD", cwd=wt)
            sh("git", "push", "-q", "-f", "origin", f"{sha}:refs/heads/{target}", cwd=wt)
        finally:
            sh("git", "worktree", "remove", "-f", wt)
    return sha


def active_ours(target):
    return [r for r in runs("in_progress") + runs("queued") if r["head_branch"] == target]


def report(run_id, out):
    r = gh(f"repos/{{owner}}/{{repo}}/actions/runs/{run_id}")[0]
    jobs = [j for page in gh(f"repos/{{owner}}/{{repo}}/actions/runs/{run_id}/jobs?per_page=100")
            for j in page["jobs"]]
    failed = [j for j in jobs if j["conclusion"] == "failure"]
    print(f"[{time.strftime('%F %T')}] run {run_id} {r['conclusion']}: {r['html_url']}", flush=True)
    if not failed:
        return
    d = os.path.join(out, str(run_id))
    os.makedirs(d, exist_ok=True)
    for j in failed:
        print(f"    FAILED {j['name']}", flush=True)
        with open(os.path.join(d, re.sub(r"[^\w.-]+", "_", j["name"]) + ".log"), "w") as f:
            f.write(sh("gh", "api", f"repos/{{owner}}/{{repo}}/actions/jobs/{j['id']}/logs"))
    # uploaded test logs (artifacts "logs-<job>", only present for failed jobs); may be absent/expired
    try:
        sh("gh", "run", "download", str(run_id), "-D", os.path.join(d, "artifacts"), "-p", "logs-*")
    except subprocess.CalledProcessError as e:
        print(f"    no artifacts downloaded: {e.stderr.strip()}", flush=True)
    print(f"    logs in {d}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="main")
    ap.add_argument("--target-branch", default="ci/flaky-hunt")
    ap.add_argument("--max-busy", type=int, default=2, help="max active self-hosted jobs to still count as idle")
    ap.add_argument("--out", default="flaky-logs", help="where failed-run logs are saved")
    ap.add_argument("--poll", type=int, default=120)
    a = ap.parse_args()

    while True:
        if active_ours(a.target_branch):
            time.sleep(a.poll)
            continue
        busy = busy_self_hosted(a.target_branch)
        if busy > a.max_busy:
            print(f"[{time.strftime('%T')}] {busy} self-hosted jobs active, waiting", flush=True)
            time.sleep(a.poll)
            continue
        sha = publish(a.base, a.target_branch)
        sh("gh", "workflow", "run", WF, "--ref", a.target_branch)
        print(f"[{time.strftime('%T')}] dispatched flaky hunt on {a.base}+ci @ {sha[:10]}", flush=True)
        time.sleep(30)  # let the run register
        while active_ours(a.target_branch):
            time.sleep(a.poll)
        last = sh("gh", "run", "list", "-w", WF, "-b", a.target_branch, "-L", "1", "--json", "databaseId", "-q", ".[0].databaseId")
        report(last, a.out)


if __name__ == "__main__":
    main()
