---
phase: 04
slug: polish-and-validation
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-15
---

# Phase 04 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | vitest 3.x |
| **Config file** | nes-topology-editor/vitest.config.ts |
| **Quick run command** | `cd nes-topology-editor && npx vitest run --bail 1` |
| **Full suite command** | `cd nes-topology-editor && npx vitest run` |
| **Estimated runtime** | ~4 seconds |

---

## Sampling Rate

- **After every task commit:** Run `cd nes-topology-editor && npx vitest run --bail 1`
- **After every plan wave:** Run `cd nes-topology-editor && npx vitest run`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 5 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 04-01-01 | 01 | 1 | UIPL-01 | unit | `npx vitest run src/store/` | ❌ W0 | ⬜ pending |
| 04-02-01 | 02 | 1 | PROP-06 | unit | `npx vitest run src/lib/validation` | ✅ | ⬜ pending |
| 04-03-01 | 03 | 2 | QURY-04 | unit | `npx vitest run src/lib/antlr` | ❌ W0 | ⬜ pending |
| 04-04-01 | 04 | 2 | UIPL-04 | manual | N/A (visual) | N/A | ⬜ pending |
| 04-05-01 | 05 | 3 | UIPL-02,UIPL-03 | manual | N/A (interaction) | N/A | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] Undo/redo middleware test stubs (zundo temporal integration)
- [ ] ANTLR parser test stubs (grammar compilation, parse valid/invalid SQL)

*Existing infrastructure covers topology validation (validation.ts already has tests).*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Dark mode toggle visual | UIPL-04 | CSS class switching needs browser | Toggle toolbar icon, verify all panels switch theme |
| Keyboard shortcut routing | UIPL-02 | Focus-dependent behavior | Ctrl+Z on canvas vs in Monaco editor |
| Query panel resize | UIPL-03 | Drag interaction | Drag top edge of query panel |
| Node error icon overlay | PROP-06 | Visual rendering | Create orphan source, verify warning icon |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
