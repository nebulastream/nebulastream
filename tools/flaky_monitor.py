#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.10"
# ///
"""Run the "Flaky Hunt" workflow (reduced nightly), one run at a time, whenever the runner pool is mostly idle.

Dispatches .github/workflows/nightly.yml *as it exists on --ref* (a pushed branch where nightly.yml is replaced by the
flaky workflow; GitHub only dispatches workflow files that exist on main). The code under test is --base (default main).
After a run, failed jobs are printed and their logs saved. Needs `gh` (admin:org for the runner API). No PR.

    tools/flaky_monitor.py [--ref <this branch, pushed>] [--base main] [--max-busy 10] [--cooldown 1800] [--poll 60]
"""
import argparse, json, os, re, subprocess, time

API = "repos/{owner}/{repo}/actions"
WF = "nightly.yml"


def sh(*cmd, cwd=None):
    return subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=cwd).stdout.strip()


def gh(path):
    return json.loads(sh("gh", "api", "--paginate", "--slurp", path))


def log(msg):
    print(f"[{time.strftime('%T')}] {msg}", flush=True)


def our_runs(ref):
    return {r["id"]: r for r in gh(f"{API}/workflows/{WF}/runs?branch={ref}&per_page=20")[0]["workflow_runs"]}


def busy_runners():
    rs = [r for page in gh("orgs/{owner}/actions/runners?per_page=100") for r in page["runners"] if r["status"] == "online"]
    return sum(r["busy"] for r in rs), len(rs)


def report(run, out):
    log(f"run {run['id']} {run['conclusion']}: {run['html_url']}")
    jobs = [j for page in gh(f"{API}/runs/{run['id']}/jobs?per_page=100") for j in page["jobs"]]
    failed = [j for j in jobs if j["conclusion"] == "failure"]
    if not failed:
        return
    d = os.path.join(out, str(run["id"]))
    os.makedirs(d, exist_ok=True)
    for j in failed:
        print(f"    FAILED {j['name']}", flush=True)
        with open(os.path.join(d, re.sub(r"[^\w.-]+", "_", j["name"]) + ".log"), "w") as f:
            f.write(sh("gh", "api", f"{API}/jobs/{j['id']}/logs"))
    # test logs uploaded by the failed jobs (may be missing or expired)
    subprocess.run(["gh", "run", "download", str(run["id"]), "-D", os.path.join(d, "artifacts"), "-p", "logs-*"])
    print(f"    logs in {d}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default=sh("git", "branch", "--show-current"), help="pushed branch carrying the flaky nightly.yml")
    ap.add_argument("--base", default="main", help="branch/sha whose code is tested")
    ap.add_argument("--max-busy", type=int, default=10, help="don't start while more org runners than this are busy")
    ap.add_argument("--cooldown", type=int, default=1800, help="pause after a cancelled run (s)")
    ap.add_argument("--out", default="flaky-logs")
    ap.add_argument("--poll", type=int, default=60)
    a = ap.parse_args()

    while True:
        try:
            run = next((r for r in our_runs(a.ref).values() if r["status"] != "completed"), None)  # e.g. from an earlier start
            if run:
                log(f"watching {run['html_url']}")
            else:
                busy, online = busy_runners()
                if busy > a.max_busy:
                    log(f"{busy}/{online} runners busy (> {a.max_busy}), waiting")
                    time.sleep(a.poll)
                    continue
                known = set(our_runs(a.ref))
                sh("gh", "workflow", "run", WF, "--ref", a.ref, "-f", f"head_sha={a.base}")
                while not run:  # the dispatch call returns no run id, wait for the new run to show up
                    time.sleep(5)
                    run = next((r for i, r in our_runs(a.ref).items() if i not in known), None)
                log(f"dispatched ({busy}/{online} runners busy): {run['html_url']}")
            while run["status"] != "completed":
                time.sleep(a.poll)
                run = our_runs(a.ref)[run["id"]]
            report(run, a.out)
            if run["conclusion"] == "cancelled":  # probably to free runners: stay away for a while
                log(f"cancelled, pausing {a.cooldown}s")
                time.sleep(a.cooldown)
        except subprocess.CalledProcessError as e:  # transient gh/git failure: retry next round
            log(f"{' '.join(e.cmd[:3])} failed: {(e.stderr or '').strip()[:300]}")
            time.sleep(a.poll)


if __name__ == "__main__":
    main()
