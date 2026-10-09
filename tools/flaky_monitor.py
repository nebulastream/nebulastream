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
import argparse, calendar, json, os, re, subprocess, time

API = "repos/{owner}/{repo}/actions"
WF = "nightly.yml"
JOB_TIMEOUT_MIN = 60  # keep in sync with timeout_minutes in nightly.yml; GitHub reports timed-out jobs as "cancelled"


def sh(*cmd, cwd=None):
    return subprocess.run(cmd, check=True, capture_output=True, text=True, cwd=cwd, timeout=600).stdout.strip()


def gh(path):
    return json.loads(sh("gh", "api", "--paginate", "--slurp", path))


def log(msg):
    print(f"[{time.strftime('%T')}] {msg}", flush=True)


def our_runs(ref):
    return {r["id"]: r for r in gh(f"{API}/workflows/{WF}/runs?branch={ref}&per_page=20")[0]["workflow_runs"]}


def busy_runners():
    rs = [r for page in gh("orgs/{owner}/actions/runners?per_page=100") for r in page["runners"] if r["status"] == "online"]
    return sum(r["busy"] for r in rs), len(rs)


def timed_out(job):
    if job["conclusion"] != "cancelled" or not job["started_at"] or not job["completed_at"]:
        return False
    t = lambda k: time.mktime(time.strptime(job[k], "%Y-%m-%dT%H:%M:%SZ"))
    return t("completed_at") - t("started_at") >= (JOB_TIMEOUT_MIN - 1) * 60


def report(run, out):
    log(f"run {run['id']} {run['conclusion']}: {run['html_url']}")
    jobs = [j for page in gh(f"{API}/runs/{run['id']}/jobs?per_page=100") for j in page["jobs"]]
    # the aggregate "Done" job only echoes the others (and fails on a manual cancel)
    failed = [j for j in jobs if (j["conclusion"] == "failure" or timed_out(j)) and not j["name"].endswith("/ Done")]
    if not failed:
        return
    d = os.path.join(out, str(run["id"]))
    os.makedirs(d, exist_ok=True)
    for j in failed:
        print(f"    {'TIMED OUT' if timed_out(j) else 'FAILED'} {j['name']}", flush=True)
        try:
            text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", sh("gh", "api", "--allow-escape-sequences", f"{API}/jobs/{j['id']}/logs"))
        except subprocess.CalledProcessError as e:  # e.g. 404: GitHub keeps no log for a job cut off by its timeout
            if "HTTP 404" not in e.stderr:
                raise  # transient (offline...): the whole report is retried
            text = f"no log available: {e.stderr.strip()[:200]}\n"
        with open(os.path.join(d, re.sub(r"[^\w.-]+", "_", j["name"]) + ".log"), "w") as f:  # created only after a successful fetch
            f.write(text)
    # test logs uploaded by the failed jobs (may be missing or expired)
    subprocess.run(["gh", "run", "download", str(run["id"]), "-D", os.path.join(d, "artifacts"), "-p", "logs-*"], timeout=1800)
    print(f"    logs in {d}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default=sh("git", "branch", "--show-current"), help="pushed branch carrying the flaky nightly.yml")
    ap.add_argument("--base", default="main", help="branch/sha whose code is tested")
    ap.add_argument("--max-busy", type=int, default=10, help="don't start while more org runners than this are busy")
    ap.add_argument("--cooldown", type=int, default=1800, help="pause after a cancelled run (s)")
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "flaky-logs"), help="failed-run logs and the .reported record go here")
    ap.add_argument("--poll", type=int, default=60)
    a = ap.parse_args()

    # State lives on GitHub plus the record of reported runs, so the monitor can be stopped, restarted or lose its
    # connection at any point: every round reports finished runs not yet recorded, adopts an active run, or starts one.
    reported_file = os.path.join(a.out, ".reported")
    os.makedirs(a.out, exist_ok=True)
    reported = set(open(reported_file).read().split()) if os.path.exists(reported_file) else set()
    watching = None

    while True:
        try:
            runs = sorted(our_runs(a.ref).values(), key=lambda r: r["id"])
            for r in runs:
                if r["status"] == "completed" and str(r["id"]) not in reported:
                    report(r, a.out)
                    reported.add(str(r["id"]))  # only after a successful report, so a failed one is retried
                    with open(reported_file, "a") as f:
                        f.write(f"{r['id']}\n")
            active = [r for r in runs if r["status"] != "completed"]
            if active:
                if watching != active[0]["id"]:
                    watching = active[0]["id"]
                    log(f"watching {active[0]['html_url']}")
            else:
                last = runs[-1] if runs else None
                since = time.time() - calendar.timegm(time.strptime(last["updated_at"], "%Y-%m-%dT%H:%M:%SZ")) if last else a.cooldown
                if last and last["conclusion"] == "cancelled" and since < a.cooldown:  # probably cancelled to free runners
                    log(f"last run was cancelled, pausing {int(a.cooldown - since)}s")
                else:
                    busy, online = busy_runners()
                    if busy > a.max_busy:
                        log(f"{busy}/{online} runners busy (> {a.max_busy}), waiting")
                    else:
                        known = {r["id"] for r in runs}
                        sh("gh", "workflow", "run", WF, "--ref", a.ref, "-f", f"head_sha={a.base}")
                        new = None
                        while not new:  # the dispatch call returns no run id, wait for the new run to show up
                            time.sleep(5)
                            new = next((r for i, r in our_runs(a.ref).items() if i not in known), None)
                        watching = new["id"]
                        log(f"dispatched ({busy}/{online} runners busy): {new['html_url']}")
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:  # offline etc.: retry next round
            log(f"{' '.join(e.cmd[:3])} failed: {(getattr(e, 'stderr', None) or 'timeout').strip()[:300]}")
        time.sleep(a.poll)


if __name__ == "__main__":
    main()
