#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# ///
"""Run the reduced-nightly "Flaky Hunt" workflow, one run at a time, whenever the runner pool is mostly idle.

Loop: if no run of ours is active and at most --max-busy org runners are busy -> publish <base>+this branch's CI
changes as `--target-branch` (force-push!) -> dispatch -> print the run link -> wait for it -> print failed jobs and
download their logs. A run that somebody cancelled (usually to free runners) is reported and followed by a
--cooldown pause before the next attempt. Needs `gh` (push access, admin:org for the runner API) and git. No PR.

    tools/flaky_monitor.py [--base main] [--max-busy 10] [--cooldown 1800] [--poll 60]
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


def busy_runners():
    """(busy, online) among the org's self-hosted runners (needs admin:org)."""
    rs = [r for page in gh("orgs/{owner}/actions/runners?per_page=100") for r in page["runners"] if r["status"] == "online"]
    return sum(r["busy"] for r in rs), len(rs)


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


def report(run_id, out):
    r = gh(f"repos/{{owner}}/{{repo}}/actions/runs/{run_id}")[0]
    jobs = [j for page in gh(f"repos/{{owner}}/{{repo}}/actions/runs/{run_id}/jobs?per_page=100")
            for j in page["jobs"]]
    failed = [j for j in jobs if j["conclusion"] == "failure"]
    print(f"[{time.strftime('%F %T')}] run {run_id} {r['conclusion']}: {r['html_url']}", flush=True)
    if not failed:
        return r["conclusion"]
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
    return r["conclusion"]


def run_ids(target):
    return {r["id"]: r for r in gh(f"repos/{{owner}}/{{repo}}/actions/workflows/{WF}/runs?branch={target}&per_page=20")[0]["workflow_runs"]}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="main")
    ap.add_argument("--target-branch", default="ci/flaky-hunt")
    ap.add_argument("--max-busy", type=int, default=10, help="don't start a run when more org runners than this are busy")
    ap.add_argument("--cooldown", type=int, default=1800, help="pause after a cancelled run (s)")
    ap.add_argument("--out", default="flaky-logs", help="where failed-run logs are saved")
    ap.add_argument("--poll", type=int, default=60)
    a = ap.parse_args()
    t = a.target_branch
    current = None  # id of the run we are waiting for

    def log(msg):
        print(f"[{time.strftime('%T')}] {msg}", flush=True)

    while True:
        try:
            active = [r for r in run_ids(t).values() if r["status"] != "completed"]
            if active:
                if current != active[0]["id"]:
                    current = active[0]["id"]
                    log(f"watching run {current}: {active[0]['html_url']}")
            elif current:
                rid, current = current, None
                if report(rid, a.out) == "cancelled":
                    log(f"run was cancelled, pausing {a.cooldown}s to leave the runners alone")
                    time.sleep(a.cooldown)
                    continue
            else:
                busy, online = busy_runners()
                if busy > a.max_busy:
                    log(f"{busy}/{online} runners busy (> {a.max_busy}), waiting")
                else:
                    known = set(run_ids(t))
                    sha = publish(a.base, t)
                    sh("gh", "workflow", "run", WF, "--ref", t)
                    log(f"dispatched flaky hunt on {a.base}+ci @ {sha[:10]} ({busy}/{online} runners busy)")
                    for _ in range(12):  # the dispatch API returns no run id: wait for the new run to appear
                        time.sleep(5)
                        new = set(run_ids(t)) - known
                        if new:
                            current = new.pop()
                            log(f"run {current}: {run_ids(t)[current]['html_url']}")
                            break
                    continue
        except subprocess.CalledProcessError as e:  # transient gh/git failure: log and retry
            log(f"{' '.join(e.cmd[:3])} failed: {(e.stderr or '').strip()[:300]}")
            current = None if current and "404" in (e.stderr or "") else current
        time.sleep(a.poll)


if __name__ == "__main__":
    main()
