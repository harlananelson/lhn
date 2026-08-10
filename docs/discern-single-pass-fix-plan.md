# Plan: Restore single-pass Discern extraction (`extract_concept_events` + `build_ontology_counts`)

**Status:** **implemented** on branch `fix/discern-single-pass-scan` (2026-08-10) — see issue for residual ops steps  
**Filed:** 2026-08-10  
**Canonical issue:** [`issues/2026-08-10-discern-n-scan-union.md`](../issues/2026-08-10-discern-n-scan-union.md)  
**Reviews:** [`reviews/discern-single-pass-2026-08-10/`](../reviews/discern-single-pass-2026-08-10/)  
**Primary target:** `lhn/core/extract.py` — **both** `extract_concept_events` **and** `build_ontology_counts` (same loop+union shape)  
**Drivers:**  
- `hmi/066-Echo-LVEF` (operator: `~/projects/hmi/DISCERN-SCAN-REGRESSION.md`)  
- **datadictrwd 015** — live verified-concept catalog (`build_ontology_counts` batches of ~100)  
- also hmi 035, 055 cabgEvents, any `extract_concept_events` caller  
**Research basis:** foresight SDK, `hnelson3` indicators + vector gate, lhn git history, multi-model plan review (Fable + AskSage)  

> **Catalog note:** datadictrwd exists to produce a catalog of *verified* concepts so
> extractions only use known concept/context pairs. That catalog path uses
> `build_ontology_counts` and must get the same single-pass fix — not a separate workaround.

---

## 1. Problem statement

### Symptom
`066-Echo-LVEF` was killed at 21600s two platform days in a row with no artifact. The notebook already does the right operational things (window, cohort, retained_fields, no attrition on the big event table). The bottleneck is in **lhn**, not the notebook.

### Root cause
`ExtractItem.extract_concept_events` **and** `ExtractItem.build_ontology_counts` build one filtered branch per `concept_flags` row and union them. Events:

```python
# extract_concept_events
for row in flags:
    matched = source_df.filter("has_concept_in_context(...)")
    parts.append(...)
result = union(parts)
```

Counts (same anti-pattern; datadictrwd 015 batches 100 concepts to survive it):

```python
# build_ontology_counts — comment in source: "Melt: one filter+tag per concept, unioned"
for row in flags:
    parts.append(df.filter(F.expr(has_concept_in_context...)).select(...).withColumn('conceptName', ...))
long = _union_aligned(parts)
```

Events detail (historical wording):

```python
for row in flags:
    matched = source_df.filter("has_concept_in_context(...)")
    parts.append(...)
result = parts[0]
for extra in parts[1:]:
    result = result.unionByName(extra)
```

On Spark **2.4.4**, union branches do **not** share a file scan. **N concepts ⇒ N full scans** of the windowed, cohort-joined source.

For `066` that is **4 scans** of `clinical_event` (88k-task stages × 4 ≈ multi-day work squeezed into an 08:00–17:00 platform day).

### Why this is a regression (relative to production practice)
The older Discern production path was never “one filter per concept + union.” It was:

1. **`push_discern(..., concepts=[...])`** — load only the needed concept subset into the broadcast.
2. **`add_concept_indicators`** — one boolean **column per concept** via `has_concept(code, 'CONCEPT')` (all on the same DF lineage ⇒ one scan).
3. **`has_any_concept(code, array(...))`** — optional vector gate: keep rows that match any concept.
4. Optionally **`stack`** those indicator columns into a long `conceptName` field (`identify_populated_concepts`).

`extract_concept_flags` (same era as events, same file) already uses the correct **single-pass multi-predicate** shape:

```python
agg_exprs = [F.max(F.expr("IF(has_concept_in_context(...), 1, 0)")).alias(flag) for row in flags]
result = source_df.groupBy(*index_fields).agg(*agg_exprs)  # ONE scan
```

`extract_concept_events` was added later (`278751e`, 2026-07-02) as the record-level counterpart and **abandoned** that shape for loop-and-union. Nothing about row-level output requires N scans.

---

## 2. Goals and non-goals

### Goals
1. **One physical scan** of the (windowed + cohort-joined) source for any number of `concept_flags` rows (modulo Spark plan quirks; no intentional multi-branch union).
2. **Preserve output contract** of `extract_concept_events`:
   - columns: `index_fields` + optional `datefield` + `flag` + retained fields
   - grain: **one output row per (source row × matching flag)**  
     (a source row matching two flags ⇒ two output rows — same as today’s union)
3. **Keep multi-context support** (different GUIDs per flag row) — required by 055/035/066.
4. **Restore push subsetting**: when pushing from `concept_flags`, pass the list of concept names for each context (not full context / `concepts=None`), matching old `push_discern(..., concepts=conceptName)`.
5. **Spark 2.4.4-safe** (no `F.call_udf`, no Spark 3-only APIs). Prefer APIs already used on HDL (`F.expr`, `F.array`, `F.explode`, optional higher-order `filter` if proven).
6. **No notebook/config change required** for 066/035/cabgEvents if the method is fixed correctly (same YAML `concept_flags`).
7. **Doc + API reference** updated; docstring coverage gate remains 100%.

### Non-goals
- Porting `query_flat_rwd` verbatim (known broken wrapper; wrong single-context abstraction for multi-context hmi).
- Changing `extract_concept_flags` behavior (already single-pass; only share helpers if clean).
- Re-running 066 on HDL as part of the lhn PR (separate platform step after lhn is deployed to the kernel).
- Redesigning concept→context validation / ontology tabulation (still a hard prerequisite: bad names still crash).

---

## 3. Design principles (from research)

| Principle | Source | Implication |
|---|---|---|
| Vector gate is boolean only | foresight `has_any_concept` / `has_any_concept_in_context` | Does **not** name which concept matched |
| Concept identity = per-concept indicators | `add_concept_indicators` + `stack` → `conceptName` | Events must evaluate `has_concept_in_context` once per flag **as columns/exprs in one plan**, then explode |
| Single active-context UDF can’t span multi-context | reconstruction + `_flags` docstring | Keep explicit GUID form for hmi |
| Full-context push is heavier than needed | old path always subset | `push_discern` concept_flags path should push per-context concept lists |
| Nested retained fields already work | `f93a5ea` | Keep `_keep_expr`; apply once on the single result DF |

### Clarification for “I thought has_any gave me which concept”
Correct memory of **capability**, wrong attribution of **which UDF**:

- `has_any_*` → boolean gate over a concept vector.
- Concept/flag **field** → `has_concept*` once per name as columns (or equivalent), optionally stacked to long form.

The fix restores that **indicator + long form** shape without N rescans.

---

## 4. Proposed implementation

### 4.1 Core algorithm for `extract_concept_events` (required)

Replace the loop+union block with **one-pass tag + explode**:

```python
def _lit(value):
    return str(value).replace("'", "''")

# Resolve keep expressions once against source columns (not per branch)
available = source_df.columns
exprs = [(c, _keep_expr(c, available)) for c in keep]
dropped = [c for c, x in exprs if x is None]
if dropped:
    logger.warning(...)
keep_cols = [x for _, x in exprs if x is not None]

# One expression per concept_flags row: null when no match, flag name when match
tag_exprs = [
    F.when(
        F.expr(
            "has_concept_in_context({code}, '{concept}', '{context}')".format(
                code=code,
                concept=_lit(row['concept']),
                context=_lit(row['context']),
            )
        ),
        F.lit(row['flag']),
    )
    for row in flags
]

tagged = source_df.select(*keep_cols, F.array(*tag_exprs).alias('_flags'))

# Prefer Spark 2.4 higher-order filter if we confirm on HDL; else explode-then-null-filter
result = (
    tagged
    .withColumn('_flags', F.expr("filter(_flags, x -> x is not null)"))
    .filter(F.size('_flags') > 0)
    .withColumn('flag', F.explode('_flags'))
    .drop('_flags')
)
```

**Fallback** if higher-order `filter` is undesirable or fails review:

```python
result = (
    source_df
    .select(*keep_cols, F.explode(F.array(*tag_exprs)).alias('flag'))
    .filter(F.col('flag').isNotNull())
)
```

Both evaluate all UDF predicates in **one** project/filter plan over one scan.

### 4.2 Output semantics (must match today)

| Case | Behavior |
|---|---|
| Row matches no concept | Dropped |
| Row matches one flag | One output row with that `flag` |
| Row matches two different flags | Two output rows (two flags) |
| Two concept_flags rows share the **same** flag name (066 echo: `ECHO_PROC` + `ECHO_OBSTYPE` both `flag: echo`) | If both match the same source row, explode yields **two rows both tagged `echo`** — same as two union branches both emitting `flag=echo`. Downstream that is usually fine (then distill/aggregate); do **not** dedupe unless we prove current callers need it |

**Optional later (not in v1 of this fix):** collapse same-flag multi-concept hits with `array_distinct` before explode so one source row ⇒ one row per distinct flag. Document as a behavior change if done; default is **preserve union semantics**.

### 4.3 `push_discern` subsetting (required companion)

Today, on the `concept_flags` path:

```python
to_push.setdefault(row['context'], None)  # FULL context
```

Change to:

```python
# context -> list of concept names (union of all flags under that GUID)
to_push.setdefault(row['context'], [])
to_push[row['context']].append(row['concept'])
# then unique-preserve order before push
```

Call:

```python
_push_discern(spark, discern_context=ctx, version=ver,
              discern_root=root, concepts=concept_list)  # not None
```

**Why:** old production always subset; full context broadcast is larger and slower. Document that explicit `discern_context=` + `concepts=` path is unchanged; only the `concept_flags` auto path gains subsetting.

**Risk:** if a concept name is wrong, crash still happens (same as today). If someone relied on full-context side effects (other concepts available without listing), that is unsupported — method contract is “only listed concepts.”

### 4.4 Optional micro-optimization (phase 2, not blocking)

Group `concept_flags` by `context` and add a single pre-filter:

```sql
has_any_concept_in_context(code, array('C1','C2'), 'GUID1')
OR has_any_concept_in_context(code, array('C3'), 'GUID2')
...
```

Then run indicator tags only on surviving rows. Same single scan if the OR is one filter expression; can reduce UDF work on non-matching rows. **Phase 1 does not require this** if tag+explode alone restores one scan. Measure on 066 before adding complexity.

### 4.5 Shared helper (recommended, small)

Extract a private helper used by both methods (or just events first):

```python
def _discern_lit(value): ...
def _concept_flag_when_exprs(flags, code): ...  # list of Column
def _resolve_concept_flags(...):  # validation already duplicated
```

Avoid a large refactor. Duplication of `_lit` and validation is fine if a shared helper is noisy.

### 4.6 What does **not** change

- Public method signature of `extract_concept_events` / `extract_concept_flags` / `push_discern` (additive only if we add kwargs; prefer no API change).
- Config shape for hmi: `concept_flags: [{flag, concept, context}, ...]`.
- Requirement: pass **raw** table with code **STRUCT** (`clinicaleventcode`, `labcode`, `conditioncode`), not flattened `_standard_id`.
- histStart/histStop + cohort join order (window before UDF stays).
- Nested `retained_fields` via `_keep_expr`.
- Auto-write / `set_self_df` behavior.

---

## 5. Files to touch

| File | Change |
|---|---|
| `~/projects/lhn/lhn/core/extract.py` | Single-pass `extract_concept_events`; subset push in `push_discern` concept_flags path; docstrings |
| `~/projects/lhn/tests/` | Add unit tests (pure logic / mocked where possible; see §6) |
| `~/projects/lhn/docs/` or regenerate via hdl-harness | After merge: `generate_package_docs.py` / review gate |
| `~/projects/hmi/DISCERN-SCAN-REGRESSION.md` | Append “fix plan / status” section (optional operator handoff) |
| hmi notebooks / `000-control.yaml` | **No change expected** for phase 1 |

Do **not** invent a new config dialect unless phase 2 adds an explicit single-context `concepts:` shortcut (optional, separate PR).

---

## 6. Testing plan

### 6.1 Offline (lhn package) — required before merge

No foresight JAR off-HDL, so:

1. **Static / unit tests of the transformation shape**  
   - With a tiny local Spark session (if CI has pyspark) **or** pure-Python tests of helper builders:  
     - Given N flags, emitted SQL expressions contain N `has_concept_in_context` calls and **no** loop producing multiple DataFrames.  
   - If Spark available: construct a DF with a dummy boolean-free path by **injecting** a test double — hard without UDF. Prefer structural tests:
     - `_concept_flag_when_exprs` length == len(flags)
     - explode plan columns == keep + `flag`
2. **Regression of validation**  
   - missing keys still raise ValueError  
   - empty concept_flags still raises  
   - code column missing still raises  
   - histStart without datefield still raises  
3. **push_discern subsetting unit test** with a mock `_push_discern` that records kwargs:  
   - two flags same context ⇒ one call, `concepts=['A','B']`  
   - two contexts ⇒ two calls, each with its own list  

### 6.2 On-HDL smoke (after deploy) — required before unblocking #47

Minimal notebook or cell (or extend `099`):

1. push one known context + subset concepts  
2. `extract_concept_events` with **2 concepts same context** on a **tiny sample** of raw table  
3. Assert:
   - action completes  
   - `flag` column present  
   - row count > 0 for a known concept  
   - explain / Spark UI shows **one** major scan stage for the extract (not 2× identical stage graphs) — operator judgment  
4. Compare counts on a **bounded** cohort sample vs old loop path if we keep a temporary `_legacy_union=True` flag for one release (optional safety switch — only if cheap).

### 6.3 Full 066 re-queue — after lhn is on the HDL kernel

1. Confirm kernel `pyspark-lhn-dev` (or project env) has the new lhn commit.  
2. Deploy is N/A for notebook if unchanged; may need env rebuild if lhn is installed from git/tag.  
3. `hdl_schedule.py add` 066 with note referencing this plan; priority can drop from 200 once fixed.  
4. Success criteria: finishes within platform day; produces `echoLvefEvents` + person summary; LVEF medians in plausible range (~10–75) per notebook checklist.

### 6.4 Callers to re-verify (same lhn bump)

| Notebook | Table | Concepts | Contexts |
|---|---|---|---|
| 066 | echoLvefEvents | 4 | 3 |
| 035 | labEvents | 4 | 2 |
| 055 | cabgEvents | 1 | 1 (should be fast either way) |
| 055 | ontologyComorbidities | uses `_flags` (unaffected) |

---

## 7. Implementation sequence

| Step | Work | Owner seat | Done when |
|---|---|---|---|
| **0** | This plan approved | user | explicit go |
| **1** | Branch `lhn`: fix `extract_concept_events` single-pass | implement | code + docstring |
| **2** | Branch `lhn`: `push_discern` concept subset from flags | implement | mock-tested |
| **3** | Unit tests in `lhn/tests/` | implement | pass locally |
| **4** | Docstrings 100%; run package doc regen / review gate if used | implement | gate green |
| **5** | PR to `harlananelson/lhn` (GitHub identity + trailer OK) | implement | PR open |
| **6** | Review (grounded second model optional: Fable/Grok on extract.py) | orchestrator | PR approved |
| **7** | Merge + ensure HDL env picks up new lhn | deploy / user | import path shows new commit |
| **8** | HDL smoke (2 concepts) | schedule | green |
| **9** | Re-queue 066; watch; free queue | schedule | artifact + HTML |
| **10** | Update regression note status; close #47 if fixed | operator | done |

Do **not** re-queue full 066 until steps 1–7 are done (wastes another platform day).

---

## 8. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Spark 2.4 higher-order `filter(array, λ)` unavailable or mis-planned | Use explode-then-`isNotNull` fallback (proven Spark 2.x) |
| Same-flag multi-concept duplicates surprise a caller | Preserve current union semantics in v1; document; optional distinct later |
| Push subset misses a concept needed by UDF | Only list concepts from concept_flags; crash-on-unknown still enforces validation against tabulation |
| One scan still too slow for full clinical_event | Window + cohort already applied; if still slow, phase-2 any-gate + further partition pruning; not a reason to keep N scans |
| Kernel still has old lhn after merge | Explicitly verify `lhn` version / git SHA on HDL before queueing 066 |
| Accidental API break | No signature change; only internal plan |

---

## 9. Acceptance criteria

1. **Code:** `extract_concept_events` has no per-concept `unionByName` loop over filters.  
2. **Code:** `push_discern(concept_flags=...)` passes non-`None` concept lists per context.  
3. **Contract:** Output schema and multi-match row multiplication match pre-fix behavior.  
4. **Tests:** Unit coverage for push kwargs + validation; offline checks pass.  
5. **Docs:** Method docstring states single-pass + relationship to indicators / `has_any_*`.  
6. **Ops:** 066 completes in one platform day and produces expected tables.  
7. **Regression note:** marked fixed with lhn commit SHA.

---

## 10. Explicit non-approaches (rejected)

| Approach | Why reject |
|---|---|
| Only switch to `has_any_concept_in_context` without per-concept tags | Loses flag/concept identity |
| Keep loop+union but cache source | Spark 2.4 still tends to re-scan; not reliable |
| Port `query_flat_rwd` | Broken wrapper; single-context only; allergy-era field lists |
| Fix only the notebook (split jobs / smaller windows) | Does not fix the library; every caller still pays N× |
| N parallel jobs per concept | Worse ops; queue pollution |

---

## 11. Open decisions for the user

1. **Duplicate same-flag rows** when two concepts under the same flag both match: keep union semantics (recommended for v1) or `array_distinct` flags?  
2. **Phase-2 any-gate prefilter** now or only if 066 still too slow after single-pass? (recommend: after first measurement)  
3. **Temporary `_legacy_union` flag** for A/B on HDL, or straight cutover? (recommend: straight cutover + sample smoke; legacy adds dead code)  
4. **Where to land the PR first** — only `lhn`, or also update `DISCERN-SCAN-REGRESSION.md` in `~/projects/hmi` in the same workstream?

Default recommendations if no preference: **(1) keep union semantics, (2) phase-2 later, (3) straight cutover, (4) lhn PR first, then operator note.**

---

## 12. Reference map (read when implementing)

| Artifact | Path |
|---|---|
| Regression diagnosis | `~/projects/hmi/DISCERN-SCAN-REGRESSION.md` |
| Current method | `~/projects/lhn/lhn/core/extract.py` (`extract_concept_events`, `push_discern`, `extract_concept_flags`) |
| Foresight SDK | `~/projects/hdl/foresight/discern.py` |
| Old vector + indicators | `~/projects/hdl/python/hnelson3.py` (`query_flat_rwd`, `add_concept_indicators`, `identify_populated_concepts`) |
| Old tabulation consumer | `~/projects/hdl/python/add_ontology_count_new.py` |
| Reconstruction / why not port query_flat | `~/projects/lhn/docs/discern-ontology-reconstruction.md` |
| 066 config | `~/projects/hmi/hdl/000-control.yaml` → `echoLvefEvents` |
| 066 notebook | `~/projects/hmi/hdl/extraction/066-Echo-LVEF.txt` |

---

## 13. One-paragraph summary

Restore the production Discern shape: **push concept subsets**, evaluate **all** `has_concept_in_context` predicates in **one** Spark plan (indicator columns / when-exprs), emit long `flag` via **explode**, and stop unioning per-concept filters. That is the same single-pass idea as `extract_concept_flags` and the old `add_concept_indicators` + optional `has_any_concept(array(...))` path; it is **not** a claim that `has_any_concept_in_context` returns the concept name. Fix lives in **lhn**; hmi config stays; re-queue 066 only after the kernel has the new lhn.
