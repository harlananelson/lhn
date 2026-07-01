# Discern Ontology System — Reconstruction Map (WORKING / PROVISIONAL)

> **Status: provisional model under construction.** Much of this system's knowledge
> "has been lost to keep track of" (per Harlan). This doc is the trace as it's verified,
> marked **[CONFIRMED]** (read the actual code), **[PARTIAL]** (glimpsed), or **[OPEN]**
> (not yet understood — do not act on these). The author (Claude) will overestimate its
> understanding; treat every claim as needing Harlan's red-team before reconstruction.
>
> Goal: revive the Cerner Discern ontology query + the schema-wide ontology *tabulation*
> on current HDL (HealtheDataLab), and fold the working pieces into current `lhn`.

## The platform fact that makes this feasible [CONFIRMED]
- `push_discern` / `has_any_concept` are **Cerner `foresight`** (the HealtheIntent Discern SDK),
  not lhn. The lhn refactor (`lhn/ontology/discern.py`) is an **unreliable AI rewrite** that
  dropped/renamed these (`has_concept` vs the real `has_any_concept`); do **not** trust it.
- The real dependency is the **`com.cerner.foresight` JAR on the Spark classpath** — Harlan
  confirmed **it is present on current HDL**. So the mechanism runs on HDL today; this is
  reconstruction, not revival-from-nothing.

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

## Current-lhn gap (reconstruction target) [CONFIRMED]
- Current `lhn` `ExtractItem` (`lhn/core/extract.py:45`) has `properties` but **no**
  `push_discern` / `query_flat_rwd`. The refactor dropped them.
- Reconstruction = add `push_discern` + `query_flat_rwd` (and the tabulation) to the current
  `ExtractItem`, wrapping `foresight.discern` + the standalone `query_flat_rwd` logic, against
  the **current** ontology schema.

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
| standalone `query_flat_rwd`, `add_concept_indicators`, `summarizeCodes` | `healthcare-archive/healtheintent/python/hnelson3.py` (+ `iuhealthcode.py`, `pymodule.py`) |
| `Item` class + `process_config2` | `troponin/oldversion/healtheintent-troponin-all.txt` (Item:1412, process_config2:63467) |
| working end-to-end | `~/projects/hdl/Projects/Colon-Cancer/010-Colon-Cancer-Extraction-RWD.ipynb` |
| schema-wide tabulation | `healthcare-archive/healtheintent/python/call_add_ontology_count.py` |
| ontology tables on HDL | `standard_ontologies.ontologies`, `tabulated_ontologies.master_ontology_ont_rwd` (full catalog) |
| current-lhn ExtractItem (target) | `lhn/lhn/core/extract.py` |
