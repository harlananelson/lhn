# Review-Fix Loop — lhn/docs/PATTERNS.md

Started: 2026-06-26
Starting commit: `bc1149a`
Final commit: `7c0fdc6`
Rounds run: 4 of 4 (max cycles reached)
Models: google-claude-47-opus, gpt-5, google-gemini-2.5-pro, grok-4-20-reasoning
Custom prompt: `hdl-harness/reviews/asksage-loop/PATTERNS_PROMPT.md`

## Outcome

**Partial convergence.** Grok reported `CONVERGED: no changes recommended` in rounds 2–4.
Claude/GPT/Gemini continued to surface lower-confidence P1/P2 nits (often contradicting
the generated API — e.g. renaming `showIU`, `labsmallSource`, `find_method`). Applied fixes
were limited to issues verified against `lhn_api.json` / `hdl_catalog.json`.

## Round 1 (`1ae81c3`)

- P1 fixed: `--hdl-project-dir` quoting, Pattern 7 `F.col()` dates, remove standalone
  `write_index_table` alternative, `--all` includes `--validate`, txtarchive skeleton,
  Pattern 6 column naming, `ctx.projectSchema`, remove erroneous `o.*` refs
- P2 fixed: API casing cross-links, performance `.limit()` reminder, error-handling cross-link

## Round 2 (`9718468`)

- P1 fixed: deduplicate bootstrap blocks, mixed API casing note, Pattern 7 `index_<code>`,
  Pattern 2 YAML `indexFields` on codes table, PHI scan in workflow diagram, `--all` prose
- P2 fixed: `showIU` comment, `csv:` config key in §2 table

## Round 3 (`1690c2d`)

- P1 fixed: Pattern 8 attrition import, `maritalstatus` column name (catalog-verified)
- P2 fixed: `broadcast_flag` example, consistent `F` from `lhn.header`, Pattern 6 naming

## Round 4 (`7c0fdc6`)

- P1 fixed: `writeTable(df=...)` kwarg, drop unsupported `removeDuplicates`, labsmall vs labSource note
- Grok: CONVERGED

## P0 issues requiring your decision

1. **Pattern 9 ExtractItem `.df` assignment** (claude-47-opus, round 4): Is
   `e.final_cohort.df = final_cohort` a sanctioned contract or should export use only
   `writeTable` / Spark writes?

2. **create_extract source table choice** (gpt-5, round 4): Some models want
   `conditionDictionarySource` instead of `conditionSource` for regex code lookup — confirm
   per-project convention; current patterns follow SCD/allison `pipeline_refs.json`.

3. **Pattern 5 return-value vocabularies** (claude-47-opus): Exact strings from `group_*` /
   `assign_age_group` — confirm against implementations or link to docstrings only.

## Declined auto-fixes (API-verified as already correct)

- `showIU` — exists on ExtractItem (`lhn_api.json`)
- `find_method` / `broadcast_flag` — snake_case per generated API (not camelCase)
- `r.labsmallSource` — catalog name (`pipeline_refs.json`, `config-RWD.yaml`)
- `sourceField` not `searchColumn` — correct kwarg per `create_extract` signature

## Diff summary

```
docs/PATTERNS.md | 86 insertions(+), 63 deletions(-)
```

Reviews preserved under `reviews/asksage-loop/round-{1..4}/`.