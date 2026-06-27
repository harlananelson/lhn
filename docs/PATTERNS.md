# LHN Pipeline Patterns

This document describes common data pipeline patterns using the lhn package.
Use these patterns as templates for building healthcare data extraction workflows.

## Overview: The Three-Step Extraction Pattern

Most lhn pipelines follow this core pattern:

```
1. create_extract()   -> Search a reference/dictionary table for codes
2. entityExtract()    -> Pull patient records for those codes
3. write_index_table() -> Create patient-level summary with first/last dates
```

---

## Notebook Authoring Conventions

How to *write* an lhn pipeline notebook. These conventions are **enforced mechanically** by
the `hdl-harness` gate (`check_notebook.py`), which validates each notebook against the live
HDL catalog before it ships. The rule of thumb: **use the package method, and confirm column
names from the data dictionary up front — don't write defensive code for unknowns.** These
notebooks are single-project pipelines, not generalized libraries.

Run the gate before deploying (it resolves `r.*`/`e.*`/`d.*` columns from the catalog
and lints for the anti-patterns below). Prefer the orchestrator wrapper — it calls
`check_notebook.py` under `--validate`:

```bash
# Gate-only (no push/render) — for full deploy use --all (includes --validate); see §7
HARNESS=~/projects/hdl-harness
python $HARNESS/hdl_run.py <notebook>.txt \
  --config <project>/000-control.yaml \
  --refs <project>/pipeline_refs.json \
  --catalog ~/projects/txtarchivetransfer/scripts/hdl_catalog.json \
  --validate
```

### 1. Confirm column names when authoring — the notebook just uses them

**This is a directive for *writing* the notebook, not code to put *in* it.** There is
definitive metadata for every table — the Spark catalog (`r.*`/`e.*`/`d.*`/`o.*`, which the
hdl-harness records) and the `targets` schema. Confirm the real column names against that
metadata **as you write**; then the notebook simply uses them.

These are **readable analysis notebooks**, not production jobs — keep them clean. Do **not**
put column-name *handling* in the notebook: no discovery / print-the-columns cells, no
defensive `pick_col`/`getattr` resolution, and no "confirmed from catalog / verify, don't
assume" commentary. The metadata is the record of column names; the notebook neither
re-verifies nor re-documents them.

```python
# BAD — code that accounts for column-name possibilities, or verifies in the notebook
def pick_col(df, candidates, label): ...
code = pick_col(proc, ['procedurecode_standard_id', 'procedure_code', 'code'], 'code')
item = getattr(e, name, None)
print(proc.columns)   # "verify, don't assume" is YOUR job while authoring, not the notebook's

# GOOD — you confirmed the name from the metadata; just use it
df = r.procedureSource.df.filter(F.col('procedurecode_standard_id').isin(codes))
```

**Keep the source tables' real column names** — even long ones like
`typedvalue_numericValue_value`. For tables created by **others** (the RWD/OMOP sources),
people familiar with the data recognize those names, and the long name itself shows the
struct hierarchy; a simpler *alias they don't recognize* frustrates a knowledgeable reader
more than a complex name they do. Use the actual name **inline**, not behind a `LAB_VAL`-style
alias. (Columns **your** pipeline creates can use whatever names you give them.)

If you genuinely need a source column renamed, do it in **`config-RWD.yaml`** (the flatten
config — `colsRename`/`insert`), **not** via a per-notebook alias. Then the new name is
recorded in the metadata and every notebook sees the same name; the flatten parity stays
intact. Renaming is a config/metadata concern, never a notebook one.

### 2. Use lhn methods — don't hand-roll what the package does

| Don't hand-roll | Use the method |
|---|---|
| `e.X.df.groupBy([c]).count()` | `e.X.tabulate(group_cols=[c], order_by='count', show=True)` |
| `.groupBy([c]).agg(F.countDistinct(...))` | `e.X.tabulate(group_cols=[c], count_distinct=[...])` |
| `.groupBy([c]).agg(F.min/F.max(date))` | `e.X.write_index_table(inTable=...)` (grain/date/`code` from config) |
| `spark.read.csv(path)` | `e.X.load_csv_as_df()` (path from `csv:` in `000-control.yaml projectTables`) |

These are **ExtractItem** methods — call them on the item `e.X`, **not** on its DataFrame:

```python
e.codes.tabulate(group_cols=['group'], show=True)     # GOOD
e.codes.df.tabulate(group_cols=['group'])             # BAD — tabulate is not a DataFrame method
```

Before writing a new helper, grep the generated package API
(`~/projects/hdl-harness/docs/api_reference.md`) for an existing function rather than
re-implementing it.

### 3. Bootstrap: bind spark first, then `pipeline_setup`

Use the [Standard notebook setup](#standard-notebook-setup-use-in-every-pattern) block in every
notebook. `pipeline_setup` derives the project from the notebook's cwd — run from
`Projects/<project>`; no per-project kwargs needed.

### 4. Use the real API — correct names and kwargs

`entityExtract` not `entity_extract`; `elementList=` not `element_list=`. The gate flags
near-miss method names (edit-distance) and unknown kwargs against the generated package API,
so a typo is caught before HDL. The generated API mixes camelCase (`elementList`,
`elementListSource`, `cacheResult`) with snake_case (`find_method`, `broadcast_flag`) —
use the exact spellings from the API ref. See
`~/projects/hdl-harness/docs/api_reference.md` for authoritative signatures.

### 5. No hardcoded absolute paths

Use the configured paths (`ctx.dataLoc`, the ExtractItem `.csv`/`.location` defaults), never
`/home/<user>/...` literals — the notebook's cwd on HDL differs from your workstation.

### 6. txtarchive `.txt` format

Author notebooks as LLM-friendly `.txt` (full format:
`~/projects/txtarchive/create-archive-llm-instructions.md`). The YAML front matter goes in
**Raw Cell 1** as a Python multi-line string literal (`"""` … `"""`); code cells are
`# Cell N`, markdown `# Markdown Cell N`. Without the `"""` wrapper the YAML is lost on
extraction (no title, no embedded resources). The `---` YAML fences inside the string are
required — `txtarchive` parses them for `title` / `jupyter.kernelspec`.
Minimal header skeleton:

```
# Raw Cell 1
"""
---
title: "054-derm-cohort-identification"
jupyter:
  kernelspec:
    display_name: pyspark-lhn-dev
    name: pyspark-lhn-dev
---
"""
```

Validate locally:
`python -m txtarchive extract-notebooks <file>.txt <out> --kernel pyspark-lhn-dev`.

### 7. HDL deploy, execute, render, and review

Notebooks ship as LLM-friendly `.txt` archives via `txtarchivetransfer`, extract on HDL
with `fetchupdate.sh` / `extract-changed.sh`, then run through the harness render path:

```
nbconvert --execute  →  quarto --no-execute (md or html)  →  Projects/archive/<subdir>/  →  git push  →  local git pull  →  PHI scan
```

- **md** when the executed notebook has no graphics or formatted tables (gtsummary/gt HTML).
- **html** when it does (`render_format.py` auto-detects).

Archive subdir mirrors the transfer repo (`hmi/`, `allison/`, `SickleCell/`, …). Full
workflow, kernels, and quarto constraints:

`~/projects/hdl-harness/docs/txtarchive-hdl-integration.md`

**Orchestrator (local):** `~/projects/hdl-harness/hdl_run.py` — use **`--all`** for the
full loop (includes **`--validate`**), or toggle stages individually. Add **`--fix-loop`**
to cycle check → 3090 fix → re-check until the gate is clean (or `--max-rounds`).

```bash
HARNESS=~/projects/hdl-harness
CATALOG=~/projects/txtarchivetransfer/scripts/hdl_catalog.json

# Full loop (validate → push → fetchupdate → render → pull + PHI scan)
python $HARNESS/hdl_run.py allison/054-derm-cohort-identification.txt \
  --config ~/projects/allison/000-control.yaml \
  --refs ~/projects/allison/pipeline_refs.json \
  --catalog $CATALOG \
  --transfer ~/projects/txtarchivetransfer \
  --hdl-project-dir "$HOME/work/Users/$USER/Projects/derm" \
  --all --fix-loop

# Unattended: add --yes (skips confirmations; PHI scan on --pull still runs)
```

`--render` includes nbconvert execution. `--all` orchestrates the full sequence (validate,
push, fetch, execute+render, pull, PHI scan) — a separate `--execute` flag is unnecessary.
On HDL after `fetchupdate`, `hdl_run.py --all` triggers
`~/work/Users/$USER/scripts/render-and-push.sh` via the automation layer (no manual SSH
step when using the orchestrator).

---

## Standard notebook setup (use in every pattern)

Run the notebook from its `Projects/<project>` directory on HDL. `pipeline_setup` derives
the project from cwd — no per-project kwargs.

```python
# Bind spark FIRST so spark.sql works even if pipeline_setup raises.
from lhn.header import spark, F

from lhn.bootstrap import pipeline_setup
ctx = pipeline_setup('000-control.yaml')
r, e, d = ctx.r, ctx.e, ctx.d
dataLoc = ctx.dataLoc
```

> **Legacy note:** older examples used `Resources(local_config=...)`. New HDL notebooks
> should use `pipeline_setup('000-control.yaml')` as above; migrate inherited notebooks
> before running the gate.

---

## Pattern 1: Simple Diagnosis-Based Cohort

**Goal**: Find all patients with a specific diagnosis (e.g., Type 2 Diabetes)

### Step 1: Setup

Use **Standard notebook setup** above (`pipeline_setup('000-control.yaml')`).

### Step 2: Find Diagnosis Codes
```python
# e.diabetes_codes is defined in 000-control.yaml projectTables
# It searches conditionSource for ICD codes matching the pattern

e.diabetes_codes.create_extract(
    elementList=e.diabetes_icd_list,        # Table with search patterns
    elementListSource=r.conditionSource.df,  # Source to search
    find_method='regex'                       # Use regex matching
)

# Check what was found
e.diabetes_codes.showIU(obs=10)   # sample matched codes (IU tenant filter)
e.diabetes_codes.attrition()
```

### Step 3: Extract Condition Records
```python
# Get all condition records for patients with diabetes codes
e.diabetes_conditions.entityExtract(
    elementList=e.diabetes_codes,
    entitySource=r.conditionSource.df,
    cacheResult=True
)

e.diabetes_conditions.attrition()
```

### Step 4: Create Patient Index
```python
# Create patient-level table with first/last diagnosis dates.
#
# The ExtractItem METHOD e.X.write_index_table() takes only inTable (+ optional
# histStart/histEnd/indexLabel/lastLabel/filterSimple). The index grain, date
# column and code label come from the ExtractItem's CONFIG properties, NOT call
# kwargs — set them in 000-control.yaml projectTables:
#   diabetes_index:
#     indexFields      : [personid]     # NOTE: config property is `indexFields`
#     datefieldPrimary : servicedate
#     code             : diabetes
e.diabetes_index.write_index_table(
    inTable=e.diabetes_conditions,
    histStart='2015-01-01',
    histEnd='2025-01-28',
)

# Output column names default to index_<code> and last_<code> from the ExtractItem's
# `code` property (here: index_diabetes, last_diabetes). Override with indexLabel=/
# lastLabel= on the call if needed.

# Result has columns: personid, index_diabetes, last_diabetes, etc.
e.diabetes_index.attrition()
```

---

## Pattern 2: Medication-Based Analysis

**Goal**: Find patients on specific medications and track usage

### Step 1: Define Drug Search Patterns
```yaml
# In 000-control.yaml projectTables:
projectTables:
  hydroxyurea_codes:
    label: "Hydroxyurea drug codes"

  hydroxyurea_list:
    label: "Search patterns for hydroxyurea"
    # This references a CSV or inline list with regex patterns
```

### Step 2: Find Drug Codes
```python
# Search medication drug dictionary for hydroxyurea
e.hydroxyurea_codes.create_extract(
    elementList=e.hydroxyurea_list,
    elementListSource=r.medicationDrugSource.df,
    find_method='regex'
)
```

### Step 3: Extract Medication Records
```python
# Get all medication administrations for these drug codes
e.hydroxyurea_meds.entityExtract(
    elementList=e.hydroxyurea_codes,
    entitySource=r.medicationSource.df,
    cohort=e.study_cohort,  # Optional: limit to cohort
    cacheResult=True
)
```

### Step 4: Create Medication Index with Gaps
```python
# Track first/last use and therapy gaps. As in Pattern 1, the grain/date/code —
# and the gap-segmentation settings (datefieldStop, max_gap, sort_fields) — are
# CONFIG properties on the ExtractItem, not call kwargs. In 000-control.yaml:
#   hydroxyurea_index:
#     indexFields      : [personid]
#     datefieldPrimary : administrationdate
#     datefieldStop    : administrationenddate
#     code             : hydroxyurea
#     max_gap          : 90              # days gap → new therapy course
#     sort_fields      : [administrationdate]
e.hydroxyurea_index.write_index_table(
    inTable=e.hydroxyurea_meds,
    histStart='2015-01-01',
    histEnd='2025-01-28',
)

# Result includes: course_of_therapy, therapy_gaps, etc.
```

---

## Pattern 3: Lab-Based Analysis

**Goal**: Analyze lab values (e.g., HbA1c, hemoglobin)

### Step 1: Find Lab Codes
```python
# Search lab dictionary for hemoglobin tests (labsmallSource = lookup;
# labSource in Step 2 = full result table)
e.hgb_codes.create_extract(
    elementList=e.hgb_search_patterns,
    elementListSource=r.labsmallSource.df,
    find_method='regex'
)
```

### Step 2: Extract Lab Results
```python
e.hgb_labs.entityExtract(
    elementList=e.hgb_codes,
    entitySource=r.labSource.df,
    cohort=e.study_cohort,
    cacheResult=True,
    broadcast_flag=True,  # small code list — broadcast for join performance
)
```

### Step 3: Aggregate Lab Values
```python
from lhn import aggregate_fields
from lhn.header import F

# Get min, max, mean per patient (groupby defined by `index`)
lab_summary = aggregate_fields(
    df=e.hgb_labs.df,
    index=['personid'],
    values=['resultvalue'],
    aggfuncs=[F.min, F.max, F.avg, F.count],
    aggfunc_names=['min', 'max', 'avg', 'count']
)
```

### Step 4: Distill to person-level (the PySpark → R/CSV bridge)

Labs are the canonical case where the per-cohort record set is **too large to export
as a raw CSV** — many results per person. Before the data crosses to R (`targets` reads
`inst/extdata/<project>/*.csv`), reduce the long lab table to **one row per person**.
Use `distill_labs` — it encodes the best-practice reduction (don't hand-roll a
`groupBy().agg(...)`):

```python
from lhn import distill_labs

# value_field must already be NUMERIC and in ONE harmonized unit (unit conversion is
# assay-specific — see the troponin troponinLabsStd harmonization). Flag bad rows so a
# corrupt value can't win the peak. Optional index_date_field gives a pre/post split.
hs = distill_labs(
    df=e.troponinLabsStd.df,
    loinc_field='labcode_standard_id',
    loincs=['89579-7', '89577-1', '89578-9'],   # hs-cTnI
    value_field='troponin_value_ngL',
    date_field='datetimeLab',
    index=['personid', 'tenant'],
    index_date_field='pci_date',                 # join the per-person index date on first
    invalid_field='troponin_value_ngL_invalid',
    code='hs_tni',
)
# -> hs_tni_n/min/max/median/peak/first_date/last_date, and (with index_date_field)
#    hs_tni_pre_peak/post_peak/post_delta/pre_n/post_n/post_first_date/post_last_date.

# Join the human-readable lab name (LOINC -> name) from labs_factable, then export:
e.hs_troponin.df = hs
e.hs_troponin.to_csv()
```

**Distillation best practices (what `distill_labs` enforces):**
1. **Filter to the cohort first** (`entityExtract(cohort=...)`) — don't distill the world.
2. **Harmonize units up front** — LOINC does not identify the instrument; a calibrated
   absolute value is only valid within one assay. Convert to one unit and **flag invalid**
   (negative/implausible) rows; they are dropped so they can't win the peak.
3. **Reduce to person grain** — count, min, max, median, peak, first/last date; and a
   **pre/post-index split** (baseline vs post-event peak + delta) when there's an index date.
4. **Carry the LOINC name** (join `labs_factable`) so the R side has readable columns.
5. **Only then `to_csv()`** — the distilled table is small enough for the bridge.

> `distill_labs` is new (lhn ≥ this version); **smoke-test it on HDL (Spark 2.4)** before
> relying on it — the median uses `percentile_approx` via `F.expr`.

---

## Pattern 4: Encounter-Based Usage Analysis

**Goal**: Calculate healthcare utilization metrics

### Step 1: Get Encounters for Cohort
```python
e.cohort_encounters.entityExtract(
    elementList=e.study_cohort,  # Use cohort as filter
    entitySource=r.encounterSource.df,
    cacheResult=True
)
```

### Step 2: Calculate Usage Metrics
```python
from lhn import calcUsage
from lhn.header import F

usage = calcUsage(
    df=e.cohort_encounters.df,
    fields=['personid', 'actualarrivaldate', 'servicedate'],
    dateCoalesce=F.coalesce(F.col('actualarrivaldate'), F.col('servicedate')),
    minDate='2015-01-01',
    maxDate='2025-01-28',
    index=['personid'],
    countFieldName='encounter_count',
    dateFirst='first_encounter',
    dateLast='last_encounter'
)
```

### Step 3: Identify High Utilizers
```python
high_utilizers = usage.filter(F.col('encounter_count') > 10)
```

---

## Pattern 5: Demographics Standardization

**Goal**: Standardize demographic categories for analysis

```python
from lhn import (
    group_ethnicities,
    group_races,
    group_gender,
    group_marital_status,
    assign_age_group
)

# Start with demographics table
demo = r.demoSource.df

# Standardize each field
demo = group_ethnicities(demo, 'ethnicity', 'ethnicity_std')
# Returns: 'Hispanic', 'Not Hispanic'

demo = group_races(demo, 'race', 'race_std')
# Returns: 'Black', 'White', 'Asian', 'Indigenous', 'Other', 'Unknown'

demo = group_gender(demo, 'gender', 'gender_std')
# Returns: 'Female', 'Male', 'Other', 'Unknown'

demo = group_marital_status(demo, 'maritalstatus', 'marital_std')
# Returns: 'Married', 'Not Married', 'Unknown'

demo = assign_age_group(demo, 'age')
# Adds 'age_group' with quartiles: 'Q1', 'Q2', 'Q3', 'Q4'
```

---

## Pattern 6: Multi-Source Cohort Building

**Goal**: Build cohort requiring multiple criteria

```python
# The ExtractItem methods below are config-driven (grain/date/code/gaps from
# 000-control.yaml projectTables), not call kwargs — see Pattern 1 Step 4.
# Set code: diagnosis on dx_index and code: medication on rx_index so output
# columns are index_diagnosis / index_medication.

# Step 1: Get patients with diagnosis (see Pattern 1 Steps 2–3 for full kwargs)
e.dx_codes.create_extract(...)
e.dx_conditions.entityExtract(...)
e.dx_index.write_index_table(inTable=e.dx_conditions)

# Step 2: Get patients with medication
e.rx_codes.create_extract(...)
e.rx_meds.entityExtract(...)
e.rx_index.write_index_table(inTable=e.rx_meds)

# Step 3: Combine criteria (produces index_diagnosis, last_diagnosis,
# index_medication, last_medication from the code: values above)
cohort = e.dx_index.df.join(
    e.rx_index.df,
    on='personid',
    how='inner'  # Require both
)

# Step 4: Apply date criteria (diagnosis before medication)
cohort = cohort.filter(
    F.col('index_diagnosis') < F.col('index_medication')
)

# Step 5: Add demographics
final_cohort = cohort.join(
    r.demoSource.df.select('personid', 'birthdate', 'gender', 'race'),
    on='personid',
    how='left'
)
```

---

## Pattern 7: Time-Window Analysis

**Goal**: Analyze events within time windows (e.g., 90 days before/after index)

```python
from lhn.header import F

# Get index dates (column is index_<code> from write_index_table — here code: cohort)
index_df = e.cohort_index.df.select('personid', 'index_cohort')

# Get all events
events = r.conditionSource.df

# Join and filter to window
windowed_events = events.join(
    index_df,
    on='personid',
    how='inner'
).filter(
    (F.col('servicedate') >= F.date_sub(F.col('index_cohort'), 90)) &
    (F.col('servicedate') <= F.date_add(F.col('index_cohort'), 90))
)
```

---

## Pattern 8: Attrition Tracking

**Goal**: Track patient counts through pipeline steps

```python
from lhn import count_people

# After each major step, call the ExtractItem attrition method
e.initial_cohort.attrition()
# Output: Records: 50,000 | Patients: 10,000

e.with_diagnosis.attrition()
# Output: Records: 45,000 | Patients: 9,500

e.with_medication.attrition()
# Output: Records: 30,000 | Patients: 7,200

# Or use count_people for inline counts
count_people(final_df, description='Final cohort', person_id='personid')
```

See also: [Error handling on HDL](#error-handling-on-hdl-fail-loud-dont-hedge).

---

## Pattern 9: Writing Output Tables

**Goal**: Save results to Spark tables and export

```python
from lhn import writeTable

# Write to Spark table (schema from pipeline_setup context)
writeTable(
    df=final_cohort,
    outTable=f'{ctx.projectSchema}.final_cohort_scd_rwd',
    partitionBy='tenant',
    description='Final SCD cohort with demographics',
)

# Assign custom-join result back to the ExtractItem before using .to_csv() /
# .parquet (paths come from 000-control.yaml projectTables, not literals)
e.final_cohort.df = final_cohort
e.final_cohort.to_csv()

# Export to Parquet
e.final_cohort.df.write.parquet(e.final_cohort.parquet)
```

---

## Common Field Names Reference

Authoritative column names live in `hdl_catalog.json`; confirm via the harness before use.
This table is a quick reference only.

### Encounter Fields
- `personid` - Patient identifier
- `encounterid` - Encounter identifier
- `tenant` - Data source/facility
- `servicedate` / `actualarrivaldate` - Encounter start
- `dischargedate` - Encounter end
- `encountertype` - Inpatient, Outpatient, ED, etc.

### Condition Fields
- `personid`, `encounterid`, `tenant`
- `conditioncode_standard_id` - ICD code
- `conditioncode_standard_display` - Code description
- `servicedate` - Diagnosis date
- `confirmationstatus` - Confirmed, Provisional, etc.

### Medication Fields
- `personid`, `encounterid`, `tenant`
- `drugcode` - Medication code
- `drugname` - Medication name
- `administrationdate` - When given
- `administrationenddate` - End of administration
- `dosage`, `route`, `frequency`

### Lab Fields
- `personid`, `encounterid`, `tenant`
- `labcode` - Lab test code
- `labname` - Test name
- `resultvalue` - Numeric result
- `resultunit` - Unit of measure
- `collectiondate` - When collected
- `referencerange_low`, `referencerange_high`

### Demographics Fields
- `personid`, `tenant`
- `birthdate` - Date of birth
- `gender` - Gender
- `race` - Race
- `ethnicity` - Ethnicity
- `maritalstatus` - Marital status (raw field; standardized as `marital_std` in Pattern 5)
- `deceased` - Deceased flag
- `zip_code` - ZIP code

---

## Error handling on HDL (fail loud, don't hedge)

HDL pipeline notebooks are **single-project**, not reusable libraries. The harness
discourages defensive patterns (candidate column lists, `getattr(r, ...)`, silent skips).

**Preferred:** confirm columns and sources from the catalog up front; let extraction errors
surface so you fix config or code — don't wrap `create_extract` / `entityExtract` in
try/except that skips downstream steps.

```python
# GOOD — attrition after each step shows empty extracts immediately
e.my_codes.create_extract(...)
e.my_codes.attrition()          # Records: 0 | Patients: 0 → investigate, don't skip silently

e.my_conditions.entityExtract(...)
e.my_conditions.attrition()
```

Use `attrition()` / `count_people()` (Pattern 8) for visibility. Reserve try/except for
genuine infrastructure faults (e.g. transient Spark session), not for "maybe this column name
exists."

---

## Performance Tips

1. **Cache intermediate results**: Use `cacheResult=True` in entityExtract
2. **Broadcast small tables**: Use `broadcast_flag=True` on `entityExtract` for small lookup tables
3. **Filter early**: Apply cohort filters as early as possible
4. **Partition output**: Use `partitionBy='tenant'` for large tables
5. **Limit during development**: Use `.limit(1000)` while testing; remove any `.limit(...)`
   before `hdl_run.py --all` so full cohort outputs render and archive
