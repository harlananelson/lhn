# Discern Ontology System — Reconstruction Map (WORKING / PROVISIONAL)

> **Status: provisional model under construction.** Much of this system's knowledge
> "has been lost to keep track of" (per Harlan). This doc is the trace as it's verified,
> marked **[CONFIRMED]** (read the actual code), **[PARTIAL]** (glimpsed), or **[OPEN]**
> (not yet understood — do not act on these). The author (Claude) will overestimate its
> understanding; treat every claim as needing Harlan's red-team before reconstruction.
>
> Goal: revive the Cerner Discern ontology query + the schema-wide ontology *tabulation*
> on current HDL (HealtheDataLab), and fold the working pieces into current `lhn`.

## ⇒ HANDOFF — where we are, where we're going (updated 2026-07-02)

**Goal now:** implement Discern **inside the `lhn` package** so the *standard config-driven lhn
workflow* works with Discern available — notebooks call `e.<item>.push_discern(config)` +
`e.<item>.query_flat_rwd(source, schema)` (idiomatic `ExtractItem` methods), NOT raw `foresight`
SQL. Then ontology pulls look like every other lhn extraction, just with Discern on tap.

### DONE (all proven on live HDL)
1. **Mechanism mapped + proven.** `foresight.push_discern` + `has_concept` /
   `has_concept_in_context` run on current HDL (the `com.cerner.foresight` JAR is live). Proof:
   the `099` smoke test **and** the `055` real extraction both ran.
2. **Concept discovery is local + ranked.** `hdl/inst/ontology_tabulation.csv` (115k rows) → pick
   the context with the most **people** per concept. The six hmi comorbidity concepts + best
   contexts are mapped (see "hmi comorbidity concept → context mapping" below).
3. **A working RAW extraction exists & ran.** `hmi/hdl/extraction/055-Ontology-Comorbidities`:
   6 concepts → person-level flags for the NSTEMI cohort → `hmi_rwd.ontologyComorbidities_nstemi_RWD`.
   Cohort 594,823; counts sensible — **mi 100%** (cohort-defining), hf 52.9%, ckd 38.7%, afib 34.1%,
   cabg 27.1%, pci 2.3%. All 3 contexts loaded under **v1**; `conditioncode` is the struct; date
   cols `effectivedate/asserteddate/statusdate` confirmed.
4. **Rhino browser** over the tabulation: `hdl/shiny/ontology-rhino/`.

### REEVALUATION (2026-07-02) — what changed vs the original plan
A fresh pass over the referenced files revised steps 1–2. Three findings:
1. **The proven `055` never used `query_flat_rwd`.** It uses `has_concept_in_context(conditioncode,
   CONCEPT, CONTEXT)` aggregated to person-level `MAX(IF(...))` across **6 concepts in 3 contexts**.
   The standalone `query_flat_rwd` uses `has_any_concept(code, array(...))` — the **single active
   context** UDF — which structurally cannot span 3 contexts in one pass. It was the wrong tool.
2. **`ExtractItem.query_flat_rwd` (hnelson3) is latently broken.** Its body calls the standalone with
   `config_dict={}` but never supplies the standalone's **required positional `startDate`/`stopDate`**,
   and it drags a crufty dep web (`query_table`, `explode_columns`, hardcoded allergy field lists).
   Porting it verbatim would import the bug + the wrong abstraction.
3. **The "tabulation half" is already reconstructed** in `lhn/ontology/discern.py` (`summarizeCodes`,
   `search_ontologies`, `extractConcepts`, `select_top_contextId`, `add_concept_indicators`). The
   earlier "[OPEN]/deferred" framing under-credited the existing module.

⇒ Implemented the **proven pattern** as the idiomatic primitive instead of `query_flat_rwd`.

### DONE by the reevaluation (2026-07-02) — code written, HDL-run PENDING
1. **Two methods added to current lhn `ExtractItem`** (`lhn/core/extract.py`):
   - `push_discern(discern_context=None, discern_root=None, version=None, concepts=None)` — thin,
     **lazy** wrapper over `foresight.discern.push_discern` (import inside the method so lhn still
     imports off-HDL). Multi-context aware: pushes each distinct GUID from `self.concept_flags` (full
     context, the proven path), or an explicit `discern_context`, or `self.discern_context`.
   - `extract_concept_flags(source, cohort=None, ...)` — generalises `055`: per-person 0/1 flags via
     `MAX(IF(has_concept_in_context(<code_struct>, '<concept>', '<context>'), 1, 0))` grouped over
     `indexFields`, cohort-bounded, config-driven from a `concept_flags` list, auto-writes.
   Both carry full docstrings (lhn review-gate: 100% public docstring coverage).
2. **`055` rewritten to the idiomatic two-call form** (`hmi/hdl/extraction/055-Ontology-Comorbidities.txt`)
   + a new **`ontologyComorbidities` block** in `hmi/hdl/000-control.yaml` carrying `concept_flags`
   (the 6 {flag,concept,context}), `conditionCodefield: conditioncode`, `discern_root` (v1), `source`
   (→ `hmi_rwd.ontologyComorbidities_nstemi_RWD`), and `inputs.source=r.conditionSource /
   cohort=persontenant`. The notebook body is: `e.ontologyComorbidities.extract_concept_flags(
   source=r.conditionSource, cohort=e.persontenant)`.
   **PENDING:** deploy via txtarchivetransfer + `fetchupdate.sh` and run on HDL to confirm counts match
   the raw run (cohort 594,823; mi 100%, hf 52.9%, ckd 38.7%, afib 34.1%, cabg 27.1%, pci 2.3%).
   Cannot verify from off-HDL (no foresight JAR here).

### Hardened after a 5-model review (2026-07-02)
Reviewed by call-grok, call-claude(Fable-high), and AskSage (Claude-Opus, GPT-5.2,
Gemini-2.5-Pro). All 5 independently flagged one real bug + robustness gaps; applied:
- **BUG (unanimous):** `extract_concept_flags(concept_flags=<arg>, push=True)` never
  forwarded the arg flags to `push_discern` → `ValueError`/wrong contexts. Fixed:
  `push_discern` now takes a `concept_flags` param and `extract_concept_flags` passes
  the resolved flags (full context per GUID).
- SQL literals in the `has_concept_in_context` `F.expr` are now escaped (`'`→`''`).
  *Rejected* Gemini's `F.call_udf` fix — it's Spark 3.5+, HDL is **Spark 2.4.4**.
- Validate concept_flags rows (missing keys, duplicate/reserved flag names); fail-fast
  `source.df` guard (mirrors entityExtract); `discern_version` preferred over `version`.
- *Kept by design* (not bugs): inner-join to cohort (matches raw 055; `_targets.R`
  left-joins + fills 0), and `has_concept_in_context` after `push_discern` (055-proven).

### NEXT
1. **Deploy + run `055` on HDL**, confirm the count parity above. If a `push_discern` fails for a
   context, that GUID isn't in v1 — flip `discern_root` to `.../v2/` in `000-control.yaml`.
2. **Wire `ontologyComorbidities` into `hmi/hdl/_targets.R`** — left-join to the full cohort (fill
   non-matches to 0) + compute `prior_mi`/`prior_pci`/`prior_cabg` from `effectivedate` vs the STEMI
   index date, off the existing `build_comorbidity_flags` / `build_prior_flags`. (Table name unchanged,
   so this is independent of the lhn refactor.)
3. (Optional) If the Colon-Cancer *record-level single-context* path is still needed, add a clean
   `extract_concept_records` method (the correct replacement for `query_flat_rwd`) — NOT a verbatim port.
4. (Deferred) tabulation reconstruction — which of the many `call_add_ontology_count*` is canonical;
   only needed to RE-tabulate on a new schema (the existing CSV already covers hmi; and the tabulation
   half already lives in `lhn/ontology/discern.py`).

### Read first, in order
1. **This whole doc** — mechanism, gotchas, mapping.
2. `~/projects/hdl/python/hnelson3.py` — `ExtractItem.push_discern` / `query_flat_rwd` (port source).
3. `~/projects/hdl/foresight/discern.py` — the SDK (push_discern → JVM; UDF families).
4. `hmi/hdl/extraction/055-Ontology-Comorbidities.txt` + its rendered `.md` (in txtarchivetransfer).
5. `~/projects/lhn/lhn/core/extract.py` — the class to extend.

### Landmines (full detail in "Gotchas" below)
- A concept name **not in the loaded context CRASHES** (Py4JJavaError), not FALSE → only pass names
  validated against `ontology_tabulation.csv`.
- The code arg is the **struct** (`conditioncode`), not `conditioncode_standard_id`.
- `discern_root` **v1** is proven for every context used so far; a `push_discern` failure ⇒ try v2.
- **Commit identities:** lhn = **GitHub** (default id + Claude trailer); hmi/hdl = **Azure DevOps**
  (`hnelson3@rwd.org`, **no** trailer). Deploy hmi notebooks via the **txtarchivetransfer** repo
  (`hmi/` subdir) + `bash fetchupdate.sh` on HDL; render comes back as `.md` in that repo.

---

## The platform fact that makes this feasible [CONFIRMED]
- `push_discern` / `has_any_concept` are **Cerner `foresight`** (the HealtheIntent Discern SDK),
  not lhn. The lhn refactor (`lhn/ontology/discern.py`) is an **unreliable AI rewrite** that
  dropped/renamed these (`has_concept` vs the real `has_any_concept`); do **not** trust it.
- The real dependency is the **`com.cerner.foresight` JAR on the Spark classpath** — Harlan
  confirmed **it is present on current HDL**. So the mechanism runs on HDL today; this is
  reconstruction, not revival-from-nothing.
- **PROVEN end-to-end by the `099` smoke test (2026-07-01)** — raw foresight, no lhn:
  `from foresight.discern import push_discern` imported; `push_discern(discern_context='5E259FD…',
  discern_root='…/v1/', concepts=['COLORECTAL_CANCER_CLIN'])` returned OK; `has_concept(conditioncode,
  'COLORECTAL_CANCER_CLIN')` ran on the **`conditioncode` struct** in `real_world_data_ed_feb_2026.condition`
  — correctly `false` for 20 non-colorectal sampled rows, and `6,218,055` matching rows overall
  (records, not distinct people — sanity-check scope on a later run with `COUNT(DISTINCT personid)`).

## Gotchas / error modes [CONFIRMED] — `hdl/Projects/ontology/Testing Ontology Issues - Harlan Nelson.html`
- **A concept name that does NOT exist in the loaded context → hard `Py4JJavaError`, NOT a graceful
  `false`.** (Throws in `DiscernUdfs$HasAnyConceptInContextUdf` → `BroadcastableValueSets.hasCode`.)
  Their example: `BLOOD_QUANTITATIVE_URINE_OBSYTPE` (typo for `OBSTYPE`) crashed the job; the
  corrected spelling did not. **⇒ Every concept name MUST be validated against the ontology
  (`standard_ontologies`/`tabulated_ontologies` for that context) BEFORE use. The tabulation/lookup
  is therefore a prerequisite, not optional — you can never pass a guessed/typo'd name.**
- **The error is LAZY** — `spark.sql(...)` builds fine; the action (`df.head()`/`count`) is what
  throws. A query cell that "succeeds" proves nothing until forced.
- **The code field is the STRUCT** — `has_*_concept*(conditioncode, …)` takes the whole code struct
  (e.g. `labcode`, `conditioncode`), NOT `.standard.id`. (`.standard.primaryDisplay` is only for the
  SELECT display.)
- **A valid concept whose codes aren't in the data → 0 rows, no error** (distinct from the crash).
- **`discern_root` version pairs with the context:** Colon-Cancer `5E259FD…` ↔ `discernontology/v1/`;
  the testing notebook `D1EF6…` ↔ `discernontology/v2/`.
- **Two UDF families** (both available after `push_discern`, which also broadcasts):
  `has_concept`/`has_any_concept` (active context, no GUID) and
  `has_concept_in_context`/`has_any_concept_in_context(code, array(...), 'GUID')` (context-qualified).

## The mechanism (extraction half)
### foresight `push_discern` [CONFIRMED] — `~/projects/hdl/foresight/discern.py:207`
```python
def push_discern(spark_session, discern_context, version=None, discern_root=None, concepts=None):
    jconcepts = HashSet(concepts)   # subset to load (memory)
    jvm.com.cerner.foresight.poprec.fhir.mappings.DiscernUdfs.pushDiscernUdfs(
        jsparkSession, discern_context, version, discern_root, jconcepts)
```
- Loads a **Discern context** (the ontology) and **registers the `has_concept`/`has_any_concept`
  UDFs**. Source = the ontologies DB **or** the S3 `discern_root` (if `discern_root` is set, it
  "ignores the ontologies database").
- `pop_discern` unregisters; `broadcast_discern` is for the `*_in_context` UDF variants.
- The actual code→concept membership algorithm is **compiled Java in the JAR** — a black box;
  we know *what* and *where*, not the internals.

### `has_any_concept` usage [CONFIRMED]
- `has_any_concept(code_col, array('CONCEPT_A','CONCEPT_B'))` — per-row JVM UDF, true if the
  row's code resolves to **any** named concept in the loaded context.
- `add_concept_indicators(conceptName, code)` (hnelson3.py:1686) adds one boolean col per
  concept via the **single** form `has_concept(code, 'CONCEPT')`.
- The concept names in `array(...)` must be among what `push_discern(concepts=...)` loaded.

### `query_flat_rwd` (standalone) [CONFIRMED] — `healthcare-archive/healtheintent/python/hnelson3.py:3199`
Wrapper that applies the ontology to a flat RWD source DF:
```python
if add_concepts:    DF = add_concept_indicators(conceptName, code, tag)(DF)   # has_concept per col
if filter_concepts: DF = DF.filter(f"has_any_concept({code}, array(<quoted conceptName>))")
if startDate:       DF = DF.filter(f"{datefieldPrimary} >= '{startDate}'")    # date window
if stopDate:        DF = DF.filter(f"{datefieldPrimary} <= '{stopDate}'")
# ... reconcile date fields, optional dedup/last, write to outSchema.outTable
```

## The working end-to-end example [CONFIRMED] — `~/projects/hdl/Projects/Colon-Cancer/010-...RWD.ipynb`
A Colon-Cancer notebook that **ran on HDL**:
```python
# cell 3:  h.process_config2(...) builds e / r / db / proj from 000-config.yaml
# cell 40: e.conditionOnt.push_discern(config_dict)                          # set up ontology
# cell 41: e.conditionOnt.query_flat_rwd(r.conditionSource, projectSchema)  # extract -> writes table
# cell 42: proj['conditionOnt'].readTable()                                  # read it back
# cell 45: countDistinct(conditioncode_standard_id, ...primaryDisplay)       # inspect
```

## The wrapper layer [CONFIRMED] — `~/projects/hdl/python/hnelson3.py`, `class ExtractItem`:4654
The methods are **thin config-reading wrappers** that call the standalone/foresight functions via
`setFunctionParameters(func, funCall, config_dict)` (reconciles funCall + config_dict against the
target function's signature). NOT magic — the earlier confusion was just having the wrong file.
```python
# ExtractItem.push_discern(self, config_dict)  :4731
funCall = {'spark_session': spark, 'discern_context': self.discern_context, 'concepts': self.concepts}
push_discern(**setFunctionParameters(push_discern, funCall, config_dict))      # → foresight.push_discern
                                                                               # (discern_root/version may come from config_dict)
# ExtractItem.query_flat_rwd(self, source, schema)  :4767
funCall = {'DF': source, 'fields': source.columns, 'datefieldPrimary': self.datefieldPrimary,
           'datefields': self.datefields, 'write': True, 'outSchema': schema, 'outTable': self.location,
           'conceptName': self.concepts, 'add_concepts': False, 'filter_concepts': True,
           'code': self.conditionCodefield, 'write_index': False}
self.df = query_flat_rwd(**setFunctionParameters(query_flat_rwd, funCall, {}))  # → standalone query_flat_rwd
```
- The `ExtractItem` carries from its `000-config.yaml` block: **`discern_context`, `concepts`,
  `datefieldPrimary`, `datefields`, `conditionCodefield`, `location`**.
- `push_discern` registers the context (concepts subset). `query_flat_rwd` filters `source` to those
  concepts (`filter_concepts=True` → `has_any_concept`) and writes `schema.location`, sets `self.df`.
- Canonical live module: `~/projects/hdl/python/hnelson3.py` (methods) + `hnelson3parts.py`
  (`process_config2`). The `troponin-oldversion` `class Item` was an older/parallel copy — ignore it.

## The tabulation half [PARTIAL — flow understood, canonical version OPEN]
The tabulation is the **producer** of `tabulated_ontologies.*`; the `conditionOnt` extraction is
the **consumer**. Two levels (`~/projects/hdl/python/`):
- **`ontologyByTableCode(inTable, codefield, inSchema, ontList, ontSum, notMatched, …)`** — per data
  table: scan `inTable`'s `codefield` in `inSchema`, match against the ontology contexts/concepts
  (`ontList = tabulated_ontologies.indexContextGroups`), write **coverage** rows to
  `ontSum = tabulated_ontologies.context_concept_table_code_*` + **unmatched** codes to `notMatched`.
- **`call_add_ontology_count(table_sample, inTable, codefield, ontologyIndex, discern_root, …)`** —
  per concept-set: iterates `ontologyIndex`, calls `add_ontology_count`, then
  `INSERT INTO tabulated_ontologies.context_concept_table_code` — builds the coverage table up.
- Output coverage = `(contextId, contextName, conceptName, table, code, count, n, percent)` — the
  table you then search to get a concept's `discern_context` GUID for a `conditionOnt` block.

**[OPEN] which version is canonical:** there are 4× `call_add_ontology_count*`, 2× `summarizeCodes*`,
`add_ontology_count_new`, `communitycode*`, `ontologyByTableCode` — cannot tell from here which one
built the live HDL tables. Needs Harlan.

**Practical fork [decision needed]:** `tabulated_ontologies.master_ontology_ont_rwd` **already exists
on HDL**. So for hmi (find concept+GUID for hf/ckd/afib/MI/CABG): **read the existing coverage tables
first**; only **re-run the tabulation** if those concepts / the current schema aren't already covered.
(Schema has also drifted: `standard_ontologies.ontologies` is now 5 cols.)

## Current-lhn gap (reconstruction target) — **CLOSED for the flag use case (2026-07-02)**
- The refactor had dropped the extraction methods from `ExtractItem`. **Now added:**
  `push_discern` + `extract_concept_flags` (see the reevaluation block up top). `query_flat_rwd`
  was **deliberately not ported** (broken wrapper + wrong single-context abstraction for `055`).
- Still open only if a **record-level single-context** extraction is needed (Colon-Cancer):
  add `extract_concept_records` as the clean replacement — do not port `query_flat_rwd` verbatim.

## hmi comorbidity concept → context mapping [CONFIRMED] — `hdl/inst/ontology_tabulation.csv`
The local week-long tabulation (115,424 rows; 5,551 concepts; 33 contexts;
`contextId, codingSystemId, conceptName, table, code, count, n, percent`) contains all six contract
concepts, pre-validated (so none trips the crash-on-nonexistent-name gotcha).

**Why the tabulation matters: a concept lives in several contexts, and `count` (distinct people —
identical across a context's coding systems) is there to pick the context with the MOST people.**
`percent = count / n` where `n` is the cohort total. Best context per concept **by count**, from this
(older) run (cohort n≈5,352; condition table unless noted):

| contract | conceptName | best contextId | count | note |
|---|---|---|---|---|
| **hf** | `HEART_FAILURE_CLIN` | `53EF3068…` | 132 | 53EF3068 (132) > `DD774BB7…` (128) |
| **afib** | `ATRIAL_FIBRILLATION_CLIN` | `53EF3068…` | 154 | |
| **prior_mi** | `MYOCARDIAL_INFARCTION_CLIN` | `53EF3068…` | 86 | |
| **ckd** | `CHRONIC_KIDNEY_DISEASE_CLIN` | `81CD81CC…` | 159 | (+ `_STAGE_1..5_`) |
| **prior_cabg** | `CORONARY_ARTERY_BYPASS_GRAFT_CLIN` | `58FA49EF…` | 45 | CLIN (45) ≫ `_PROC` (1) |
| **prior_pci** | `PERCUTANEOUS_CORONARY_INTERVENTION_PROC` | `81CD81CC…`/`DD774BB7…` | 3 | ⚠ **LOW** — revisit (procedure table / different concept) |

Counts are from an **OLD run on a specific cohort** — the current data will differ, so **re-tabulate
on the target schema for exact picks**; the *ranking* (which context wins) is the durable signal.
`push_discern` loads the whole context, so hf/afib/mi share one push (`53EF3068`), ckd another
(`81CD81CC`). **Supersedes the manual troponin/UMLS code-curation.** Pick/validate contexts via the
Rhino app's Concept → Context panel (shows `count`, sorted). The per-`codingSystemId` rows also let
you inspect code-system overlap across contexts for a concept.

## Open questions (do not guess)
1. ~~Wrapper binding~~ **[RESOLVED]** — `ExtractItem` config-wrappers via `setFunctionParameters`.
2. ~~`conditionOnt` config block~~ **[RESOLVED]** — `hdl/Projects/Colon-Cancer/000-config.yaml`:
   `discern_context` (a GUID, e.g. `5E259FD575B54D4982D32D4E92DCA831`), `concepts`
   (`['COLORECTAL_CANCER_CLIN']`), `conditionCodefield` (`conditioncode`), `source` (→ output
   table), `datefieldPrimary`/`datefields`, `startDate`/`stopDate` (histStart/histStop). Top-level
   `discern_root: s3://…/discernontology/v1/`. → To add an hmi concept you need its **conceptName
   + discern_context GUID**, which the tabulation/ontology tables supply (open-q #3).
3. **Tabulation** — the `call_add_ontology_count` flow end-to-end; do we re-run it, or just read
   the existing `tabulated_ontologies` tables on HDL?
4. **Schema adaptation** — what changes given the drifted `standard_ontologies.ontologies` columns.
5. **`setFunctionParameters`** — confirm its binding rules (it's the glue for every method wrapper).

## Source index (where the pieces live)
| Piece | Location |
|---|---|
| foresight SDK (push/has_any_concept) | `~/projects/hdl/foresight/discern.py` |
| standalone `query_flat_rwd`, `add_concept_indicators`, `summarizeCodes` | `healthcare-archive/healtheintent/python/hnelson3.py` (+ `rwdcode.py`, `pymodule.py`) |
| `Item` class + `process_config2` | `troponin/oldversion/healtheintent-troponin-all.txt` (Item:1412, process_config2:63467) |
| working end-to-end | `~/projects/hdl/Projects/Colon-Cancer/010-Colon-Cancer-Extraction-RWD.ipynb` |
| schema-wide tabulation | `healthcare-archive/healtheintent/python/call_add_ontology_count.py` |
| ontology tables on HDL | `standard_ontologies.ontologies`, `tabulated_ontologies.master_ontology_ont_rwd` (full catalog) |
| current-lhn ExtractItem (target) | `lhn/lhn/core/extract.py` |
