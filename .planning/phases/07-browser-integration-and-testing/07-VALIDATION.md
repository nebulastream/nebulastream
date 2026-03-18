---
phase: 7
slug: browser-integration-and-testing
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-16
---

# Phase 7 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | Vitest 3.2.4 |
| **Config file** | `nes-topology-editor/vitest.config.ts` |
| **Quick run command** | `cd nes-topology-editor && npx vitest run --reporter=verbose` |
| **Full suite command** | `cd nes-topology-editor && npx vitest run` |
| **Estimated runtime** | ~15 seconds |

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
| TBD | 01 | 0 | BINT-01 | integration | `npx vitest run src/__tests__/wasm-loading.test.ts` | ❌ W0 | ⬜ pending |
| TBD | 01 | 0 | BINT-03 | unit | `npx vitest run src/__tests__/validation-worker.test.ts` | ❌ W0 | ⬜ pending |
| TBD | 01 | 0 | BINT-04 | unit | `npx vitest run src/__tests__/validation-debounce.test.ts` | ❌ W0 | ⬜ pending |
| TBD | 01 | 0 | BINT-02 | unit | `npx vitest run src/__tests__/validation-display.test.ts` | ❌ W0 | ⬜ pending |
| TBD | 02 | 1 | BINT-01 | integration | `npx vitest run src/__tests__/wasm-loading.test.ts` | ❌ W0 | ⬜ pending |
| TBD | 03 | 2 | BINT-02 | unit | `npx vitest run src/__tests__/validation-display.test.ts` | ❌ W0 | ⬜ pending |
| TBD | 04 | 2 | TEST-02 | integration | `node --test wasm/test/test-validator.mjs` | ✅ | ⬜ pending |
| TBD | 04 | 2 | TEST-03 | manual-only | Deferred per user decision | N/A | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `src/__tests__/wasm-loading.test.ts` — stubs for BINT-01 (mock Worker, verify ready message handling, retry logic)
- [ ] `src/__tests__/validation-worker.test.ts` — stubs for BINT-03 (mock Worker postMessage protocol)
- [ ] `src/__tests__/validation-debounce.test.ts` — stubs for BINT-04 (verify debounce timing, stale result rejection)
- [ ] `src/__tests__/validation-display.test.ts` — stubs for BINT-02 (StatusBar rendering, error display states)
- [ ] Worker mock utility for Vitest (since happy-dom lacks Web Worker support)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| CI job builds WASM + runs tests | TEST-03 | Deferred per user decision — CI pipeline not included in this phase | Document test commands for future CI wiring |
| UI remains responsive during WASM load | BINT-01 | Requires visual confirmation of non-blocking behavior | Load app, verify canvas is interactive while "Loading validator..." spinner shows |
| Dragging/typing not blocked during validation | BINT-03 | Requires manual interaction timing | Trigger validation, immediately drag nodes and type in panels |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
