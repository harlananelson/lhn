# Issues

Bugs and design traps found in `lhn` while running it against real data on HealtheDataLab.
One file per issue, `YYYY-MM-DD-short-slug.md`.

These originate from the `hdl-harness` issue log and are copied here because the fix belongs in
this package. Each records the symptom as well as the cause — several were misdiagnosed first,
and the wrong diagnosis is part of the report.

## Open

| Filed | Issue | Severity | Where |
|---|---|---|---|
| 2026-08-10 | [Discern N-scan from per-concept filter + union](2026-08-10-discern-n-scan-union.md) | high — **fix on branch** `fix/discern-single-pass-scan` | `extract_concept_events` / `build_ontology_counts` |
| 2026-08-04 | [`distill_labs`: `unit_field` does not prevent unit pooling; no first/last VALUE](2026-08-04-distill-labs-unit-field-and-missing-values.md) | high | `lhn/analytics.py` |
| 2026-08-04 | [One node, two artifacts, different schemas (Hive vs CSV)](2026-08-04-lhn-hive-csv-schema-divergence.md) | medium | `write_index_table` / `_auto_write` / `to_csv` |

## Why these two are here rather than in the calling project

Both are **contract** problems, not usage errors. In each case a caller followed the API as
documented (or as established elsewhere in the same package) and got a silently wrong result:

- `unit_field` **partitions** by unit in `lhn/features.py` and merely **records** one in
  `distill_labs` — same parameter name, opposite behaviour, same package.
- `write_index_table` writes Hive at method return, so augmenting `.df` afterwards and calling
  `to_csv()` leaves two artifacts of one node carrying different columns.

A project-side workaround is possible for both, but every project has to discover them
independently, and the failure is silent until someone reads the wrong artifact or pools the
wrong units.
