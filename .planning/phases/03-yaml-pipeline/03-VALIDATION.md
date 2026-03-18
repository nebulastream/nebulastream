---
phase: 3
slug: yaml-pipeline
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-14
---

# Phase 3 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | vitest 3.x + jsdom |
| **Config file** | `nes-topology-editor/vitest.config.ts` |
| **Quick run command** | `cd nes-topology-editor && npx vitest run` |
| **Full suite command** | `cd nes-topology-editor && npx vitest run` |
| **Estimated runtime** | ~10 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cd nes-topology-editor && npx vitest run`
- **After every plan wave:** Run `cd nes-topology-editor && npx vitest run`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 03-01-01 | 01 | 0 | OUTP-01 | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts` | ❌ W0 | ⬜ pending |
| 03-01-02 | 01 | 0 | OUTP-02 | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts` | ❌ W0 | ⬜ pending |
| 03-01-03 | 01 | 0 | OUTP-04 | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts` | ❌ W0 | ⬜ pending |
| 03-02-01 | 02 | 1 | OUTP-01 | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts` | ❌ W0 | ⬜ pending |
| 03-02-02 | 02 | 1 | OUTP-02 | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts` | ❌ W0 | ⬜ pending |
| 03-03-01 | 03 | 1 | OUTP-03 | manual-only | N/A — browser clipboard | N/A | ⬜ pending |
| 03-04-01 | 04 | 2 | OUTP-04 | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts` | ❌ W0 | ⬜ pending |
| 03-05-01 | 05 | 2 | OUTP-05 | unit | `cd nes-topology-editor && npx vitest run src/lib/yaml.test.ts` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `src/lib/yaml.test.ts` — stubs for OUTP-01, OUTP-02, OUTP-04 (storeToYaml, yamlToStore, roundtrip)
- [ ] `npm install js-yaml && npm install -D @types/js-yaml` — dependency installation

*Existing vitest infrastructure covers framework needs.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| YAML editor content is copyable | OUTP-03 | Requires browser clipboard API | 1. Open editor 2. Select YAML text 3. Ctrl+C 4. Paste elsewhere |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
