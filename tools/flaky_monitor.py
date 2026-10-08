#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# ///
"""Dispatch the reduced-nightly "Flaky Hunt" workflow whenever CI is (nearly) idle.

Each round (up to --concurrency runs in parallel): wait until few self-hosted jobs of other runs are active -> publish <base>+this branch's CI changes as
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


def run_ids(target):
    return {r["id"]: r for r in gh(f"repos/{{owner}}/{{repo}}/actions/workflows/{WF}/runs?branch={target}&per_page=20")[0]["workflow_runs"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="main")
    ap.add_argument("--target-branch", default="ci/flaky-hunt")
    ap.add_argument("--max-busy", type=int, default=2, help="max active self-hosted jobs of OTHER runs to still count as idle")
    ap.add_argument("--concurrency", type=int, default=2, help="max simultaneous flaky runs")
    ap.add_argument("--out", default="flaky-logs", help="where failed-run logs are saved")
    ap.add_argument("--poll", type=int, default=120)
    a = ap.parse_args()
    t = a.target_branch
    mine = {}  # run id -> url, runs we started and haven't reported yet

    def log(msg):
        print(f"[{time.strftime('%T')}] {msg}", flush=True)

    while True:
        cur = active_ours(t)
        active = {r["id"] for r in cur}
        for r in cur:  # also adopts runs of a previous monitor instance / runs missed after dispatch
            mine.setdefault(r["id"], r["html_url"])
        for rid in [r for r in mine if r not in active]:
            report(rid, a.out)
            del mine[rid]
        if len(active) < a.concurrency:
            busy = busy_self_hosted(t)
            if busy <= a.max_busy:
                known = set(run_ids(t))
                sha = publish(a.base, t)
                sh("gh", "workflow", "run", WF, "--ref", t)
                log(f"dispatched flaky hunt on {a.base}+ci @ {sha[:10]}")
                for _ in range(12):  # the dispatch API returns no run id: wait for the new run to appear
                    time.sleep(5)
                    new = set(run_ids(t)) - known
                    if new:
                        rid = new.pop()
                        mine[rid] = run_ids(t)[rid]["html_url"]
                        log(f"run {rid}: {mine[rid]}")
                        break
                else:
                    log("new run not visible yet, it will be picked up on the next round")
                continue  # re-evaluate immediately (maybe start a second one)
            log(f"{busy} self-hosted jobs of other runs active, waiting ({len(active)} of ours running)")
        time.sleep(a.poll)


if __name__ == "__main__":
    main()
