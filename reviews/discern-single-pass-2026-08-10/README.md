# Multi-model review: single-pass Discern fix (2026-08-10)

Plan review for `extract_concept_events` + `build_ontology_counts` N-scan regression.

| Doc | Role |
|---|---|
| [Issue](../../issues/2026-08-10-discern-n-scan-union.md) | Symptom, consumers, acceptance |
| [Plan](../../docs/discern-single-pass-fix-plan.md) | Implementation plan |
| [synthesis.md](synthesis.md) | Cross-model adjudication + agent seating |
| [fable-review.md](fable-review.md) | call-claude Fable thorough |
| [asksage/](asksage/) | Claude Opus 4.6, GPT-5.2, Gemini 2.5 Pro, Grok-4-20 |

**Consensus:** APPROVE with changes; explode+`isNotNull` default; consider PR1 (scan) vs PR2 (push subset); include `build_ontology_counts` for the datadictrwd verified-concept catalog path.
