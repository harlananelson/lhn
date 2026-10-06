# INDEX — lhn/docs/

> One line per document: purpose | **audience** | **what invalidates it**.

| Doc | Purpose | Audience | Invalidated by |
|---|---|---|---|
| `PATTERNS.md` | Canonical lhn pipeline patterns/templates | machine | code change (lhn) |
| `CONFIG_TEMPLATES.md` | Annotated 3-tier config hierarchy (config-global / config-RWD / project) | machine | code change |
| `ANALYSIS_SCHEMA.md` | Structured format describing an analysis so an LLM can generate the pipeline | machine | code change |
| `discern-ontology-reconstruction.md` | WORKING reconstruction map of the Discern ontology, tagged [CONFIRMED]/[PARTIAL]/[OPEN] — the epistemically-honest version | both | data + code change |
| `discern-single-pass-fix-plan.md` | Restore single-pass Discern extraction; implemented on `fix/discern-single-pass-scan` | machine | code change (done — history once merged everywhere) |
