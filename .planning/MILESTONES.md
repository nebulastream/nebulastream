# Milestones

## v1.0 — Topology Editor UI (completed 2026-03-15)

**Goal:** Visual topology editor for designing NES deployments and exporting YAML for nes-cli.

**Phases:** 4 (01-interactive-canvas, 02-configuration-panels, 03-yaml-pipeline, 04-polish-and-validation)
**Plans:** 21
**Last phase number:** 4

**What shipped:**
- Interactive canvas with drag-to-connect worker topology
- Source/sink nodes with host placement via drag-and-drop
- Context-sensitive property panels (worker, source, sink, logical sources)
- SQL query editor with ANTLR syntax validation and context-aware autocompletion
- YAML import/export with live preview overlay
- Topology validation with visual error overlays on canvas nodes
- Dark mode, keyboard shortcuts, history navigation
- 195 unit tests passing
