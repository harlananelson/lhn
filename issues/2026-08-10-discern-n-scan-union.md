# Discern extract paths: one filter per concept + union = N full scans

**Filed:** 2026-08-10  
**Component:** `lhn/core/extract.py` — `extract_concept_events`, `build_ontology_counts`  
**Severity:** high — multi-hour / multi-day platform cost; silent structural waste  
**Found in:** `hmi` 066 Echo-LVEF (killed at 21600s ×2 days); same shape in datadictrwd 015 ontology rebuild  
**Status:** **fixed on branch `fix/discern-single-pass-scan`** (2026-08-10) — single-pass
`extract_concept_events` + `build_ontology_counts`; `push_discern(concept_flags=)` subsets
concepts per context; helpers + tests in `tests/test_discern_single_pass.py`. Merge + HDL
kernel bump still required before re-queueing 066 / 015.  

---

## What happens

Two methods build a long (concept-tagged) table by **looping `concept_flags`**, applying one
`has_concept_in_context` **filter per concept**, then **union**ing the branches:

| Method | Consumer | Typical N |
|---|---|---|
| `extract_concept_events` | hmi 066, 035, cabgEvents | 1–4 |
| `build_ontology_counts` | datadictrwd 015 (verified concept catalog) | batches of **100** |

On Spark **2.4.4**, union branches do not share a file scan when predicates differ.
**N concepts ⇒ N full scans** of the (windowed/cohort-joined) source — and the cohort join
is repeated per branch too.

Sibling methods already do the right shape (single plan, many predicates):

- `extract_concept_flags` — `groupBy` + many `MAX(IF(has_concept_in_context(...)))`
- `build_ontology_coverage` — `GREATEST(IF(...), …)` in one pass

---

## Why it is a package bug (not a project usage error)

Callers follow the documented Discern idiom: raw code **STRUCT**, validated
`concept_flags: [{flag, concept, context}]`, `histStart`/`histStop`, cohort. They still pay N× I/O.

datadictrwd already **works around** the cost (`BATCH_SIZE=100`, multi-day resume) because the
README states `build_ontology_counts` is “one UDF filter per concept.” That is a library shape
bug, not something every project should rediscover.

---

## Verified-concept catalog (datadictrwd) — not a separate story

One purpose of datadictrwd is to produce a **live catalog of verified concepts** (012 index →
015 counts/coverage), so project extracts (hmi, etc.) only use concept/context pairs that
exist in the ontology index — avoiding the foresight hard crash on unknown names.

That catalog path **uses** `build_ontology_counts` and therefore **needs the same single-pass
fix**. Crash-on-unknown remains a safety rail for typos; the catalog is the gate.

Do **not** feed the stale archive `ontology_tabulation.csv` as the producer path — 012+015
recreate the modern tabulation from live `standard_ontologies`.

---

## Correct shape (production history + sibling methods)

Old hnelson3 / foresight path:

1. `push_discern(..., concepts=[...])` — subset broadcast  
2. Per-concept **indicators** via `has_concept` / `has_concept_in_context` in **one** DF lineage  
3. Optional `has_any_concept(code, array(...))` boolean gate  
4. `stack` / explode → long `conceptName` / `flag`

`has_any_concept_in_context` itself is **boolean only**; concept identity comes from the
per-concept indicators (or explode of when-tags).

Proposed fix: one-pass `when` tags + explode (default on Spark 2.4.4: explode then
`isNotNull`), shared for events (`flag`) and counts (`conceptName`).

---

## Fix home / plan / reviews

| Artifact | Location in this repo |
|---|---|
| **Full plan** | [`docs/discern-single-pass-fix-plan.md`](../docs/discern-single-pass-fix-plan.md) |
| **Multi-model reviews + synthesis** | [`reviews/discern-single-pass-2026-08-10/`](../reviews/discern-single-pass-2026-08-10/) |
| **Reconstruction map (mechanism)** | [`docs/discern-ontology-reconstruction.md`](../docs/discern-ontology-reconstruction.md) |
| Operator handoff (hmi) | `~/projects/hmi/DISCERN-SCAN-REGRESSION.md` (symptom; fix is here) |

---

## Acceptance (short)

1. No per-concept `union` of filters in `extract_concept_events` or `build_ontology_counts`.  
2. Output contracts unchanged (events: one row per source×matching flag; counts: same groupBy grain).  
3. Offline stub-UDF tests + HDL smoke.  
4. hmi 066 and datadictrwd 015 both benefit after HDL kernel has the new lhn.  

See the plan for PR split recommendations (scan fix vs push subsetting) and review must-fixes.
