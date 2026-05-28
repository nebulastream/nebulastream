# HOME_SETUP.md — Run the 10k OOC evaluation on your home device

> **For the Claude Code session on the home machine.** This file bootstraps a fresh
> clone of `nebulastream-Out-of-Core-Stream-Processing` (branch
> `feat/ooc-phase1-spill-skeleton`), installs everything needed, runs a 10-cell
> smoke test, then launches the full **10,044-cell** sweep at **4-way parallelism**
> (~3.5 days). Resumable — Ctrl-C and re-launch is safe.
>
> Reference plan: `.planning/eval-phase/EXPERIMENT-PLAN-FULL.md`.
> Harness reference: `.planning/eval-phase/e3big/HANDOFF.md`.

---

## 0. What this evaluation answers

### Research questions (proposal §2.2)
| RQ  | Question                                                                                                       | Tested by              |
|-----|----------------------------------------------------------------------------------------------------------------|------------------------|
| RQ1 | Integrate an OOC backend into NES's push-based model — survive state > memory without OOM.                     | Sweep A + Reference    |
| RQ2 | Throughput + tail latency (p95/p99) at a research-informed RocksDB config; spill/restore CPU + latency cost.   | Sweeps B, C, D + Refs  |
| RQ3 | CPU + disk I/O as state grows past memory; RocksDB write-amp + storage footprint.                              | Sweeps B, C + all OOC  |

### Hypotheses (H1–H5)
| H  | Claim                                                          | Tested in                                | Verdict criterion                                  |
|----|----------------------------------------------------------------|------------------------------------------|----------------------------------------------------|
| H1 | Throughput peaks at lz4 / wbuf=64 / cache=32                   | Sweep C (wbuf) + Sweep D (cache)         | Throughput @ proposed config vs neighbours        |
| H2 | Write stalls < 5% of execution time at ≥64 MB wbuf + lz4       | Derived from A + C (uses stall metric)   | `write_stall_µs / elapsed_µs < 0.05`              |
| H3 | Compression mandatory (none ⇒ ≥4× read penalty)                | Sweep B (none / lz4 / zstd)              | Throughput + footprint gap none vs lz4            |
| H4 | OOM ceiling = 2·wbuf + cache + ~100 MB; 70/90% watermarks safe | Sweep F (watermark) + Sweep A (P-knee)   | No OOM at proposed thresholds; pre-partition wins |
| H5 | Spilling 100 MB ≈ 2 s                                          | All OOC cells (no extra runs)            | s-per-100 MB observed vs the 2 s prediction        |

### Configuration matrix (186 distinct configs / rep)
- **Sweep A — Survival & residency** (RQ1, H4 context): `P × state × cap × workload` = 5 × 6 × 2 × 2 − skipped = 120
- **Sweep B — Compression** (RQ2, RQ3, H3): `comp × state` = 3 × 5 = 15
- **Sweep C — Write buffer** (RQ2, RQ3, H1): `wbuf × state` = 4 × 5 = 20
- **Sweep D — Block cache** (RQ2): `cache × state` = 3 × 5 = 15
- **Sweep F — Watermark** (H4): `(soft,hard) × state` = 3 × 3 = 9
- **Reference — in-mem baselines** (RQ1 §3.3.1, RQ2 overhead denominator): 7

> `Sweep E` (H2 stall) is derived from A+C using the write-stall metric — **zero** extra cells.

**Repetitions:** `REPS=54` → 186 × 54 = **10,044 cells** (≈ ±2 % 95% CI at typical CV=0.08).

---

## 1. Host requirements

| Resource | Required        | Why                                                  |
|----------|-----------------|------------------------------------------------------|
| Cores    | ≥ 16            | 4 parallel cells × 4 threads each                    |
| RAM      | ≥ 8 GB free     | 4 parallel × 512 MB cap + Docker overhead            |
| Disk     | ≥ 30 GB free    | `cmake-build-debug/` ≈ 14 GB, `/tmp/nes-spill-*` ≤ 2 GB/cell, results+logs ≤ 2 GB |
| OS       | Linux or macOS  | Docker Desktop on macOS; native Docker on Linux      |

> If you have **8 cores or 4 GB free RAM**, lower parallelism to `2` in the launch
> step. Walltime doubles to ~7 days but still completes.

---

## 2. Install dependencies (one-time)

### Linux (Ubuntu / Debian)
```bash
sudo apt-get update
sudo apt-get install -y git docker.io
sudo usermod -aG docker "$USER"     # log out + back in for group to apply
```

### macOS
```bash
# Docker Desktop — set Resources >= 8 GB RAM, >= 30 GB disk in Preferences
brew install --cask docker
brew install git
```

Verify:
```bash
docker run --rm hello-world         # must succeed
git --version
```

---

## 3. Clone the repo

```bash
git clone -b feat/ooc-phase1-spill-skeleton \
  git@github.com:Oblision/nebulastream-Out-of-Core-Stream-Processing.git
cd nebulastream-Out-of-Core-Stream-Processing
```

> If SSH isn't set up on the home machine, swap for the HTTPS URL.

---

## 4. Build the dev image + the systest binary

The eval runs **inside** the dev container. Build it once, then build the binary:

```bash
# (a) Pull or build the dev image. Tag MUST be exactly this:
docker pull nebulastream/nes-development:local-rocksdb || \
  docker build -t nebulastream/nes-development:local-rocksdb -f docker/Dockerfile .

# (b) Configure + build the systest binary inside that image:
docker run --rm --workdir /workspace -v "$PWD:/workspace" \
  nebulastream/nes-development:local-rocksdb bash -c "
    cmake -S . -B cmake-build-debug -G Ninja \
      -DCMAKE_BUILD_TYPE=Debug \
      -DUSE_LOCAL_VCPKG=1 && \
    cmake --build cmake-build-debug --target systest -j"
```

Verify:
```bash
ls cmake-build-debug/nes-systests/systest/systest   # binary must exist
```

> **First build is slow** (~30–60 min, vcpkg fetches dependencies). Re-builds are
> incremental. The `cmake-build-debug/` directory is gitignored; nothing pushes.

---

## 5. Workloads (already in repo)

Both workload `.test` files are committed under `.planning/eval-phase/e3big/work/`:
- `uniform5m.test` — uniform-key Generator, 5M tuples (forces in-mem OOM)
- `skewed.test` — binomial-distributed keys (hot-key skew), 2M tuples
- Plus `.planning/eval-phase/phase0c/SpillSmokeVoid.test` (2M uniform — used for ≤2M states)

No regeneration needed. Confirm:
```bash
ls .planning/eval-phase/e3big/work/
ls .planning/eval-phase/phase0c/SpillSmokeVoid.test
```

---

## 6. Smoke test (10 cells, ~20–30 min)

Validates: Docker plumbing, the systest binary, `run-cell.sh`, the metric pipeline,
and result-file writing — before committing to 3+ days of compute.

```bash
cd .planning/eval-phase/e3big

# (a) Generate a 1-rep matrix (186 cells), take the first 10 as smoke:
REPS=1 bash gen-matrix.sh
head -10 jobs.txt > smoke-jobs.txt

# (b) Run them at parallelism=2, into a separate results dir so a clean re-run is trivial:
mkdir -p smoke-results
xargs -P 2 -L 1 bash run-cell.sh < smoke-jobs.txt

# (c) Verify: 10 TSV files, all non-empty, status=ok on most:
ls results/*.tsv | wc -l                # expect: 10
awk -F'\t' 'NR>1{print $2}' results/A_*.tsv | sort -u   # expect status values, ok dominant
```

**Pass criteria:**
- 10 TSV files in `results/`
- ≥ 8 cells with `status=ok` (a couple may be `oom` — expected for boundary cells)
- Each TSV has 27 columns
- `logs/A_*.log` contains `NES_INFO` metric lines (stall, write-amp, spill latency)

**If smoke fails:** see `## 9. Troubleshooting`. Do NOT continue to the full sweep
until smoke is clean.

**Reset for the full run:**
```bash
rm -rf results smoke-results smoke-jobs.txt jobs.txt logs
```

---

## 7. Full sweep (10,044 cells @ 4-wide, ~3.5 days)

```bash
cd .planning/eval-phase/e3big

# (a) Generate the full matrix:
REPS=54 bash gen-matrix.sh
wc -l jobs.txt                          # expect ≈ 10044

# (b) Launch under nohup so it survives terminal disconnect:
nohup bash launch.sh 4 > launch.out 2>&1 &
echo $! > launch.pid                    # save PID
disown
```

> **Why 4-wide:** each cell uses `--cpus=4` and `--memory=512m`. 4 parallel × (4 cores
> + 512 MB) = 16 cores + 2 GB committed. Leaves headroom for the host.
> Bump to `8` only if you have ≥ 32 cores AND ≥ 8 GB free.

**Total cells:** ~10,044. **Wall-clock:** ~84 h ≈ 3.5 days. **Resumable:** killing
the process and re-running `launch.sh 4` continues from the last completed cell —
each cell writes its own TSV and is skipped on re-entry.

---

## 8. Monitor / resume

```bash
# Live launch output:
tail -f .planning/eval-phase/e3big/launch.out

# Cell-completion count (out of ~10044):
ls .planning/eval-phase/e3big/results/*.tsv | wc -l

# Recent finished cells with throughput / status / stall:
grep -h done .planning/eval-phase/e3big/logs/*.log | tail -20

# Is launch.sh still running?
kill -0 "$(cat .planning/eval-phase/e3big/launch.pid)" && echo alive || echo dead
```

**To resume after a crash / reboot / Ctrl-C:**
```bash
cd .planning/eval-phase/e3big
nohup bash launch.sh 4 > launch.out 2>&1 &
echo $! > launch.pid; disown
```
(Same command as the initial launch — `run-cell.sh` skips cells whose TSV exists.)

**To stop deliberately:**
```bash
kill "$(cat .planning/eval-phase/e3big/launch.pid)"
```

---

## 9. Collect + analyze (after sweep finishes)

```bash
cd .planning/eval-phase/e3big

# (a) Concatenate per-cell TSVs into one runs.tsv with a header:
bash collect.sh
wc -l runs.tsv                          # expect ≈ 10045 (10044 + header)

# (b) Run the extended analyzer — emits per-RQ + per-H figures + the verdict table:
python3 analyze.py runs.tsv figs

# (c) Inspect the verdict table:
cat figs/H1-H5-verdict.md
```

Outputs:
- `runs.tsv` — 27-column master table (every cell, every metric).
- `figs/` — survival, P-knee, compression, wbuf, cache, watermark, stall%,
  spill-latency-per-100MB, per-cell CV.
- `figs/H1-H5-verdict.md` — `confirmed` / `refuted` / `reframed` per hypothesis,
  computed from the data.

Drop these into the thesis chapter + the HTML/PDF summary
(`OOC-Thesis-Summary.html`).

---

## 10. Troubleshooting

| Symptom                                            | Likely cause                            | Fix                                                                                  |
|----------------------------------------------------|-----------------------------------------|--------------------------------------------------------------------------------------|
| `docker: permission denied`                        | User not in `docker` group              | `sudo usermod -aG docker $USER`, log out + back in.                                  |
| `systest` binary missing                           | Step 4(b) didn't finish                 | Re-run the `cmake --build` line. Check vcpkg fetch errors in the build log.          |
| All smoke cells `status=oom`                       | Host RAM too low, Docker memory cap fighting host swap | Lower parallelism to `1`; close other apps; check `docker info` "Total Memory".      |
| TSVs missing the stall / p99 columns               | E1 / H2 commits not on the branch        | `git log --oneline | head -5` — top commit should be `abfc4d0fd9` H2 metric.        |
| `/tmp` fills up                                    | Spill scratch dirs not cleaned          | Each cell cleans its own `/tmp/nes-spill-<label>`; if a cell crashed, `rm -rf /tmp/nes-spill-*`. |
| Cells suspiciously fast (< 30 s)                   | `cap`/`emit_lag` not being applied      | Inspect `logs/<label>.log` for the actual systest cmdline; verify run-cell.sh args.  |
| Throughput numbers wildly different from val.out   | Background load on host                 | Close other heavy processes; cells are only valid under steady-state host load.      |

---

## 11. Hand-off checklist (when Claude is done)

- [ ] `runs.tsv` exists with ≈ 10,044 data rows.
- [ ] No cell has `status=error`. (`oom` is allowed and expected on the high-state low-cap cells.)
- [ ] `figs/H1-H5-verdict.md` produced.
- [ ] `figs/` survival, P-knee, compression, wbuf, stall%, spill-latency plots produced.
- [ ] CV per cell is < 0.15 for ≥ 90% of cells. Cells exceeding that flagged for follow-up reps.
- [ ] Compress + copy `runs.tsv`, `figs/`, and `logs/` back to the development machine:
      ```bash
      tar -czf eval-10k-results.tar.gz runs.tsv figs/ logs/
      ```
