---
phase: 2
slug: configuration-panels
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-14
---

# Phase 2 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Vitest 3.x with jsdom |
| **Config file** | `nes-topology-editor/vitest.config.ts` |
| **Quick run command** | `cd nes-topology-editor && npx vitest run --reporter=verbose` |
| **Full suite command** | `cd nes-topology-editor && npx vitest run` |
| **Estimated runtime** | ~10 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cd nes-topology-editor && npx vitest run --reporter=verbose`
- **After every plan wave:** Run `cd nes-topology-editor && npx vitest run`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 02-01-01 | 01 | 1 | PROP-01 | unit | `npx vitest run src/components/Sidebar/WorkerPanel.test.tsx` | ❌ W0 | ⬜ pending |
| 02-01-02 | 01 | 1 | PROP-02 | unit | `npx vitest run src/components/Sidebar/SourcePanel.test.tsx` | ❌ W0 | ⬜ pending |
| 02-01-03 | 01 | 1 | PROP-03 | unit | `npx vitest run src/components/Sidebar/SinkPanel.test.tsx` | ❌ W0 | ⬜ pending |
| 02-01-04 | 01 | 1 | PROP-04 | unit | `npx vitest run src/components/Sidebar/FormFields.test.tsx` | ❌ W0 | ⬜ pending |
| 02-01-05 | 01 | 1 | PROP-05 | unit | `npx vitest run src/components/Sidebar/SchemaBuilder.test.tsx` | ❌ W0 | ⬜ pending |
| 02-02-01 | 02 | 1 | SRCE-01 | unit | `npx vitest run src/store/topologySlice.test.ts` | Partial | ⬜ pending |
| 02-02-02 | 02 | 1 | SRCE-02 | unit | `npx vitest run src/components/Sidebar/SchemaBuilder.test.tsx` | ❌ W0 | ⬜ pending |
| 02-02-03 | 02 | 1 | SRCE-03 | unit | `npx vitest run src/components/Sidebar/SourcePanel.test.tsx` | ❌ W0 | ⬜ pending |
| 02-03-01 | 03 | 2 | QURY-01 | unit | `npx vitest run src/store/querySlice.test.ts` | ❌ W0 | ⬜ pending |
| 02-03-02 | 03 | 2 | QURY-02 | unit | `npx vitest run src/store/querySlice.test.ts` | ❌ W0 | ⬜ pending |
| 02-03-03 | 03 | 2 | QURY-03 | unit | `npx vitest run src/components/QueryEditor/QueryPanel.test.tsx` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `src/store/querySlice.test.ts` — stubs for QURY-01, QURY-02
- [ ] `src/components/Sidebar/WorkerPanel.test.tsx` — stubs for PROP-01
- [ ] `src/components/Sidebar/SourcePanel.test.tsx` — stubs for PROP-02, SRCE-03
- [ ] `src/components/Sidebar/SinkPanel.test.tsx` — stubs for PROP-03
- [ ] `src/components/Sidebar/SchemaBuilder.test.tsx` — stubs for PROP-05, SRCE-02
- [ ] `src/components/Sidebar/LogicalSourcesPanel.test.tsx` — stubs for SRCE-01
- [ ] `src/components/QueryEditor/QueryPanel.test.tsx` — stubs for QURY-03
- [ ] Monaco editor mock setup for jsdom environment

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Monaco syntax highlighting renders correctly | QURY-03 | Monaco CDN loading and visual rendering not testable in jsdom | Open query panel, type SQL, verify keywords are color-highlighted |
| Sidebar resize drag UX | UIPL-03 | Mouse drag interaction not reliably testable in unit tests | Drag sidebar left edge, verify resize works smoothly |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
