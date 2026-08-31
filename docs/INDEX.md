# INDEX — lhn/docs/

> One line per document: purpose | **audience** | **what invalidates it**. Top-level map
> of all HDL knowledge: the `hdl-index` skill (`~/.claude/skills/hdl-index/SKILL.md`).

| Doc | Purpose | Audience | Invalidated by |
|---|---|---|---|
| `PATTERNS.md` | Canonical lhn pipeline patterns/templates (target of hdl-harness round-trip reconciliation) | machine | code change (lhn) |
| `CONFIG_TEMPLATES.md` | Annotated 3-tier config hierarchy (config-global / config-RWD / project) | machine | code change |
| `ANALYSIS_SCHEMA.md` | Structured format describing an analysis so an LLM can generate the pipeline | machine | code change |
| `discern-ontology-reconstruction.md` | WORKING reconstruction map of the Discern ontology, tagged [CONFIRMED]/[PARTIAL]/[OPEN] — the epistemically-honest version (the settled catalog is `hdl-harness/docs/discern-ontology-tabulation.md`) | both | data + code change |
| `discern-single-pass-fix-plan.md` | Restore single-pass Discern extraction; implemented on `fix/discern-single-pass-scan` (same incident as `hmi/DISCERN-SCAN-REGRESSION.md`) | machine | code change (done — history once merged everywhere) |

Review packets (history; invalidated by nothing): `../reviews/asksage-loop/` (5 rounds ×
4 models on PATTERNS.md — read `SUMMARY.md` only), `../reviews/discern-single-pass-2026-08-10/`
(read `synthesis.md`).
