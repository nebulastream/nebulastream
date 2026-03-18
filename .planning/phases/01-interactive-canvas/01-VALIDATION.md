---
phase: 1
slug: interactive-canvas
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-13
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Vitest 3.x + @testing-library/react 16.x |
| **Config file** | None — Wave 0 installs |
| **Quick run command** | `npx vitest run --reporter=verbose` |
| **Full suite command** | `npx vitest run` |
| **Estimated runtime** | ~15 seconds |

---

## Sampling Rate

- **After every task commit:** Run `npx vitest run --reporter=verbose`
- **After every plan wave:** Run `npx vitest run`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 01-01-01 | 01 | 0 | — | infra | `npx vitest run` | ❌ W0 | ⬜ pending |
| 01-01-02 | 01 | 1 | CANV-01 | integration | `npx vitest run src/__tests__/palette-dnd.test.tsx -t "worker"` | ❌ W0 | ⬜ pending |
| 01-01-03 | 01 | 1 | CANV-02 | integration | `npx vitest run src/__tests__/palette-dnd.test.tsx -t "source"` | ❌ W0 | ⬜ pending |
| 01-01-04 | 01 | 1 | CANV-03 | unit | `npx vitest run src/__tests__/store.test.ts -t "position"` | ❌ W0 | ⬜ pending |
| 01-02-01 | 02 | 1 | CANV-04 | integration | `npx vitest run src/__tests__/connections.test.tsx -t "downstream"` | ❌ W0 | ⬜ pending |
| 01-02-02 | 02 | 1 | CANV-05 | integration | `npx vitest run src/__tests__/connections.test.tsx -t "attach"` | ❌ W0 | ⬜ pending |
| 01-02-03 | 02 | 1 | CANV-08 | unit | `npx vitest run src/__tests__/validation.test.ts` | ❌ W0 | ⬜ pending |
| 01-03-01 | 03 | 1 | CANV-07 | unit | `npx vitest run src/__tests__/nodes.test.tsx -t "distinct"` | ❌ W0 | ⬜ pending |
| 01-03-02 | 03 | 1 | CANV-09 | manual-only | Verify `snapToGrid` prop — visual check | N/A | ⬜ pending |
| 01-03-03 | 03 | 1 | CANV-10 | unit | `npx vitest run src/__tests__/canvas.test.tsx -t "minimap"` | ❌ W0 | ⬜ pending |
| 01-03-04 | 03 | 1 | CANV-11 | unit | `npx vitest run src/__tests__/layout.test.ts` | ❌ W0 | ⬜ pending |
| 01-04-01 | 04 | 2 | CANV-06 | integration | `npx vitest run src/__tests__/delete.test.tsx` | ❌ W0 | ⬜ pending |
| 01-04-02 | 04 | 2 | CANV-12 | unit | `npx vitest run src/__tests__/copy-paste.test.ts` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `nes-topology-editor/` project scaffold (Vite + React + TypeScript)
- [ ] `vitest.config.ts` — Vitest configuration with jsdom environment
- [ ] `npm install -D vitest @testing-library/react jsdom` — framework install
- [ ] `src/__tests__/store.test.ts` — Zustand store unit tests
- [ ] `src/__tests__/validation.test.ts` — Cycle detection + connection type validation
- [ ] `src/__tests__/layout.test.ts` — Dagre layout utility tests
- [ ] `src/__tests__/copy-paste.test.ts` — Copy/paste with ID regeneration
- [ ] `src/__tests__/palette-dnd.test.tsx` — Palette drag-and-drop integration tests
- [ ] `src/__tests__/connections.test.tsx` — Connection creation integration tests
- [ ] `src/__tests__/nodes.test.tsx` — Node component rendering tests
- [ ] `src/__tests__/canvas.test.tsx` — Canvas feature tests (minimap)
- [ ] `src/__tests__/delete.test.tsx` — Select and delete tests

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Grid snapping enabled | CANV-09 | Visual alignment check — `snapToGrid` prop correctness verified by visual inspection | 1. Drag a node on canvas 2. Verify it snaps to grid increments 3. Check `snapToGrid` prop in source |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
