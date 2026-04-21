# `lhn.core.extract` — Human Guide

A practical guide for humans (and AI pair programmers) building a
pipeline on top of `lhn.core.Resources` + `lhn.core.Extract`.

**Scope:** what the Extract module is, what parameters come from
`000-control.yaml`, how to set up a notebook, how to refresh when
you change YAML, where the dictionary tables live, and a skeleton
you can copy for a new project.

---

## 1. Mental model in one page

```
     000-control.yaml                                          Hive
  ┌──────────────────────┐                              ┌──────────────────┐
  │ projectTables:       │  Resources(local=...) reads  │ <projectSchema>  │
  │   mycodes: {...}     │  the YAML and builds a       │   .mycodes       │
  │   mycodesVerified:…  │  TableList of ExtractItem    │   .mycodesVer…   │
  │   myEncounter: {...} │  instances — one per entry   │   .myEncounter   │
  │   myEncounterId:{…}  │                              │                  │
  └──────────┬───────────┘                              └────────▲─────────┘
             │                                                   │
             ▼                                                   │
     resource = Resources(...)                                   │
             │                                                   │
             ▼            (auto_write on each extract call)      │
          e = resource.e  ─────────────────────────────────►  write
             │
             ├─ e.mycodes.dict2pyspark()        # stage 1: load/build a code list
             ├─ e.mycodesVerified.create_extract(...)  # stage 2: verify against dictionary
             ├─ e.myEncounter.entityExtract(...)       # stage 3: pull patient records
             └─ e.myEncounterId.write_index_table(...) # stage 4: one row per person
```

The four things to remember:

1. **One ExtractItem per YAML entry.** Adding `myNewTable:` under
   `projectTables:` creates `e.myNewTable` automatically after
   `Resources(local_config=..., global_config=..., schemaTag_config=...)`
   runs. `e = resource.e` works because `finish_init=True` is the
   default — it's not an optional step you add later.
2. **Five extraction verbs** do the real work: `dict2pyspark`,
   `load_csv_as_df`, `create_extract`, `entityExtract`,
   `write_index_table`. All five **auto-write** to
   `<projectSchema>.<name>` when they complete. Manual `self.df = ...`
   assignment does NOT auto-write — call `.write()` explicitly after.
3. **`d` is NOT `resource.d`.** `resource.d` is a dict of config
   objects; the dictionary TableList is at `resource.dictrwd`.
   `locals().update(resource.load_into_local())` in the setup cell
   remaps `d` to point at `dictrwd`. Skip that line and
   `d.<table>.df` raises AttributeError. This is the single most
   common failure mode in this library.
4. **Dictionary tables** (`d.*`) are distinct-code reference tables from a
   *dictionary* schema (`dictrwdSchema` in YAML). They are **not**
   patient-level. Use them as the source for regex scans — never scan
   a patient-level table. A regex scan over patient-level data will
   silently hang.

### Status-check pattern (run after Resources init)

Processed items can silently fail in lenient mode — `item.process()`
catches flatten/insert errors, sets `status = ITEM_FAILED`, and
leaves `item.df` pointing at the **un-processed** raw DataFrame. No
exception is raised. Always run the report and confirm status
before relying on downstream data:

```python
print(r.report_str())                            # summary of every Item
assert all(item.status == 'PROCESSED'            # fail loud on silent loss
           for item in r.values())
```

For new pipelines / CI, pass `strict=True` to `processDataTables`
and `validate_tables_config`; this upgrades warnings to exceptions
at the table-load point.

### Vocabulary: `Extract` vs `ExtractItem`

These are different classes and confusing them will derail a
conversation or a commit message.

| Class | What it is | How you access one |
|---|---|---|
| `Extract` | The container (TableList). Holds every ExtractItem the config defines. Has `.write_all()`, `__iter__`, `__getitem__`. | `e = resource.e` |
| `ExtractItem` | A single configured table. Has `.df`, `.location`, `.label`, `.write()`, and the four verbs (`dict2pyspark`, `load_csv_as_df`, `create_extract`, `entityExtract`, `write_index_table`). | `e.mycodes`, `e.myEncounter`, etc. |

So in
`e.myEncounter.entityExtract(e.sicklecodesVerified, r.conditionSource.df)`:

- `e.myEncounter` — the receiver. An **ExtractItem** (destination).
- `e.sicklecodesVerified` — the `elementList` argument. An
  **ExtractItem** (source of keys).
- `r.conditionSource.df` — the `entitySource` argument. A plain Spark
  **DataFrame** (`r.*` members are `Item`s; `.df` is the DataFrame).

`elementList`, `entitySource`, and `inTable` are all duck-typed: any
object with a `.df` attribute works (so ExtractItem and Item are
interchangeable), and a raw DataFrame works too. A `dict` or a `list`
is also accepted for `elementList` by `create_extract` (the dict
becomes {group: pattern} and the list becomes sequential groups).

### Inspecting an ExtractItem's parameters

Every ExtractItem / Item carries its YAML-sourced parameters as
plain attributes. Two helpers, inherited from `SharedMethodsMixin`,
surface them:

```python
>>> e.sicklecodesVerified.properties()
{'name': 'sicklecodesVerified',
 'label': 'Verified Sickle Cell Codes',
 'location': 'sicklecell_ai.sicklecodesVerified',
 'groupName': 'group',
 'sourceField': 'conditioncode_standard_id',
 'indexFields': ['conditioncode_standard_primaryDisplay',
                 'conditioncode_standard_id',
                 'conditioncode_standard_codingSystemId'],
 'datefield': None,
 ...}

>>> e.sicklecodesVerified.values()    # same info, pretty-printed
```

- `.properties()` returns a dict (filters out `_private` attrs and `df`).
- `.values()` `pprint`s `self.__dict__` (includes everything).

These are the first thing to reach for when debugging "why is this
join picking the wrong key" or "why isn't the group column being
found" — both of which are driven by attributes on the receiver
*and* on the elementList.

### Parameters an ExtractItem brings into verb calls

Because `elementList` is usually an ExtractItem, **its config
attributes are consumed by the verb** — not just the caller's own
config. This is where a lot of subtle coupling hides. Concretely:

**`create_extract` (regex mode) reads from `elementList`:**

| Attribute (on elementList) | Consumed as |
|---|---|
| `listIndex` | The column name in `elementList.df` that contains the regex patterns. Defaults to `'codes'`. |
| `groupName` | The column in `elementList.df` that labels which regex matched. Falls back to the receiver's `groupName`, then to literal `'group'`, then to `listIndex`. |

**`create_extract` (merge mode) reads from `self` (the receiver):**

| Attribute (on the receiver, NOT elementList) | Consumed as |
|---|---|
| `self.listIndex` (with fallback `self.merge_column`, then `'code'`) | Join column between `elementList.df` and `elementListSource`. |

Note the asymmetry: regex mode reads the elementList's `listIndex`;
merge mode reads the receiver's `listIndex`. Keep the same value on
both when possible, or pass `elementIndex=` explicitly.

**`entityExtract` reads from `elementList`:**

| Attribute (on elementList) | Consumed as |
|---|---|
| `indexFields` | Default `elementIndex` (join key). Falls back to the receiver's `indexFields`, then to `['personid']`. |

This is why a verified-codes ExtractItem that has
`indexFields: [conditioncode_standard_primaryDisplay,
conditioncode_standard_id, conditioncode_standard_codingSystemId]` in
YAML causes `entityExtract` to join on those three columns by
default — the receiver didn't say anything about the join, the
**elementList** did, silently.

**Practical rule:** when you wire up a create_extract → entityExtract
chain, the YAML parameters on the intermediate `*_Verified` item
(`listIndex`, `groupName`, `indexFields`) determine both how
`create_extract` labels its output and how `entityExtract`'s
downstream join keys are picked. Check `e.thatItem.properties()` if
you're not sure what the verb is going to see.

---

## 2. Configuration reference

### 2.1 Project-wide keys (top of YAML, not under `projectTables`)

| Key | Purpose |
|---|---|
| `project` | Display name. Read by `Resources._extract_config_values` into `resource.project`. |
| `projectSchema` | Hive schema where every extract writes. Tables resolve to `<projectSchema>.<name>`. |
| `schemas:` block | Maps logical schema names (`RWDSchema`, `dictrwdSchema`, `omopSchema`, etc.) to physical Hive database names. |
| `schemaTag` | Short tag appended to table/CSV filenames in some conventions (e.g. `SCD_RWD`). |
| `disease` | Disease label; surfaces into filenames and logs. |
| `dataLoc` | Local filesystem root for CSV exports (`${systemuser}` substitution supported). |
| `parquetLoc` | HDFS root for parquet exports. |
| `historyStart` / `historyStop` | Default study window; inherited by `write_index_table` when `histStart`/`histEnd` not given per-extract. |
| `callFunProcessDataTables:` | Maps `data_type` → (`type_key`, `property_name`, `schema_type`, `updateDict`). Tells Resources how to process each schema type (RWD, dict, OMOP, …). |

### 2.2 Keys that `Resources` resolves from the `callFun` config

For each entry in `callFunProcessDataTables`, Resources creates two
bindings on the `resource` object:

- `resource.<type_key>` — the `TableList` of `Item`s (has `.df`).
  Example: `resource.r` (RWD), `resource.dictrwd` (dictionary).
- `resource.<property_name>` — the config objects (no `.df`).
  Short aliases like `resource.d` live here.

**Critical:** `resource.d` is **not** the dictionary table collection.
`resource.dictrwd` is. `load_into_local()` (called from the setup
cell) *remaps* the short name `d` to the TableList so that
`d.<table>.df` works. Do not use `resource.d` directly without
calling `load_into_local()`.

### 2.3 Per-extract keys (under `projectTables:`)

Everything under `projectTables:` becomes an ExtractItem. The keys
you can set on each item are grouped by which verb consumes them.

**Generic (read by any verb):**

| Key | Required? | What it does |
|---|---|---|
| `label` | **Yes** (for auto-write) | Passed as `description=` to `writeTable`. If missing, auto-write is skipped. |
| `indexFields` | Yes for writes | Default join / partition key list. |
| `fields` | No | If set, downstream `select()` may use it; informational. |
| `retained_fields` | No | After `create_extract` with `retained_fields` present, the result is projected to join keys + these + `'group'` if present. |
| `partitionBy` | No | Passed to `writeTable` for Hive partitioning. |
| `datefield` | No | Default date column used in logging / grain tagging. |

**`dict2pyspark` (builds codes from inline YAML dict):**

| Key | Purpose |
|---|---|
| `dictionary:` | `{group_name: regex_pattern}` dict embedded in YAML. This is the only YAML key `dict2pyspark` actually reads. |
| `listIndex` | Stored on the Item but NOT read by `dict2pyspark` itself. The method uses its `columnname=` parameter (default `'codes'`) for the pattern column. `listIndex` is read later by `_extract_by_regex` when this Item is passed as `create_extract`'s `elementList`. |
| `sourceField` | Stored on the Item but NOT read by `dict2pyspark`. Used by the downstream `create_extract` receiver. |
| `complete` | Stored on the Item but read nowhere in the codebase — informational only. |

**`load_csv_as_df` (builds codes from a CSV):**

| Key | Purpose |
|---|---|
| `csv` | Absolute path (or set via `e.foo.csv = ...` in the notebook). |
| `listIndex` | Column name for the code column in the CSV. |

**`create_extract` (verify code list against a dictionary):**

| Key | Purpose |
|---|---|
| `find_method` | `'regex'` (scan `sourceField` with regex patterns) or `'merge'` (exact-value join on `listIndex`). Default `'regex'`. |
| `sourceField` | Column in `elementListSource` to scan / join. |
| `groupName` | Column in the elementList that labels which regex matched. Defaults to `'group'`. |
| `retained_fields` | Output columns to keep (plus always-keep join keys + `'group'`). |
| `indexFields` | Treated as the natural join keys of the verified-codes table (used as `entityExtract`'s default `elementIndex`). |

**`entityExtract` (pull patient-level records):**

| Key | Purpose |
|---|---|
| `indexFields` | Usually `['personid']` or `['personid','tenant']`. Consumed as fallback join key if neither the elementList nor the caller pass one. |
| `datefield` | Primary date column for downstream filtering. |
| `cohortColumns` | Subset of columns to keep from a `cohort=` argument when used. |

**Arguments passed from the notebook, NOT the YAML:**

`elementList`, `entitySource`, `inTable`, and the receiver's
`resource`-scoped dependencies are **not** wired up from the YAML by
the library. You pass them explicitly:

```python
e.mycodesVerified.create_extract(
    elementList=e.mycodes,                     # ExtractItem arg
    elementListSource=d.condition_conditioncode.df,  # DataFrame arg
    find_method='regex',                       # could come from YAML
    sourceField='conditioncode_standard_id',   # could come from YAML
)
```

The `inputs: {elementList: mycodes, elementListSource: "dictrwd.condition_conditioncode"}`
blocks you'll see in existing project YAML files are **documentation
only** — they tell a human reviewer what the extract conceptually
depends on, but no library code auto-wires them into the call.
Treating them as runtime configuration has caused confusion before:
editing the `inputs:` block does not change what the notebook does.

**`write_index_table` (one row per person, first/last dates):**

| Key | Purpose |
|---|---|
| `datefieldPrimary` | Column used as "index date" (earliest); outputs a `{indexLabel}{code}` column. |
| `datefieldStop` | Column used as "last date"; outputs a `{lastLabel}{code}`. |
| `code` | Suffix appended to `index_` / `last_` in the output column names (e.g. `code: "SCD"` → `index_SCD`, `last_SCD`). |
| `sort_fields` | Tie-breaking sort order for picking the "first" record. |
| `histStart` / `histEnd` | Date filter (inclusive) applied before index computation. |
| `max_gap` | Days allowed between consecutive records; values above become a "new course." |
| `indexLabel` / `lastLabel` | Prefixes for the output columns. Default `'index_'` / `'last_'`. |

### 2.4 Where the values are actually read

- Most per-extract keys become **attributes** of the ExtractItem
  via `setattr(self, key, value)` in `ExtractItem.__init__`. So
  `e.foo.indexFields` is just `projectTables.foo.indexFields` from
  YAML.
- The verbs fall back to `getattr(self, KEY, DEFAULT)` if the
  caller doesn't pass KEY explicitly. This is why you can call
  `e.foo.write_index_table(inTable=...)` and the method
  automatically picks up `datefieldPrimary`, `code`, `sort_fields`
  from YAML.

---

## 3. Canonical notebook setup

Every notebook in the pipeline should start with these cells, in
this order. This is the pattern 053/054/056/057/058/064/067
currently use.

```python
# Cell 4 — environment + path discovery
import os
os.environ["PYSPARK_PYTHON"] = "python3"  # executors need this
import sys, getpass
from pathlib import Path

systemuser = getpass.getuser()
user_path = [p for p in (
    os.path.join(Path.home(), 'work', 'Users', systemuser),
    os.path.join(Path.home(), 'work', 'IUH', systemuser),
) if os.path.exists(p)][0]
project_path = os.path.join(user_path, 'Projects', '<YourProject>')
os.environ['DATA_PATH'] = user_path  # exposed to downstream CSV exports

# Cell 6 — imports
from lhn.header import *           # spark, F, Window
from lhn import Resources
from IPython.display import display, HTML
display(HTML("<style>.container { width:99% !important; }</style>"))

# Cell 7 — Resources
os.chdir(project_path)
resource = Resources(
    local_config='000-control.yaml',
    global_config='configuration/config-global.yaml',
    schemaTag_config='configuration/config-RWD.yaml',
    debug=True,
)

# Cell 8 — bind short names + surface schemas into locals
r = resource.r         # source tables (RWD)
e = resource.e         # project Extract (your outputs)
d = resource.d         # config-object form; rebound below to TableList
# Rebind d (and all schemas/dictLists) into this cell's locals so that
# d.<table>.df works. load_into_local() also surfaces RWDSchema,
# projectSchema, dataLoc, etc., as bare names.
locals().update(resource.load_into_local())
```

**Do not remove the `locals().update(...)` line.** The line after
it relies on the remap `d → resource.dictrwd`. Without it,
`d.<table>.df` raises `AttributeError` because `resource.d` is the
config-object dict, not the TableList.

---

## 4. Refreshing after editing `000-control.yaml`

Resources reads the YAML once at `__init__`. If you edit the YAML
while a notebook is open, you have three options, in order of
cost:

### 4.1 Quick: `reread_config_files()`

If you only changed values (no new `projectTables` entries):

```python
resource.reread_config_files()
# existing e.<name> attributes may be stale — re-read any affected
# one by touching .df (lazy-load from Hive).
```

### 4.2 Structural: re-run Cell 7

If you added or renamed a `projectTables` entry, or changed any
schema mapping:

```python
# Re-execute Cell 7 (the Resources() constructor).
os.chdir(project_path)
resource = Resources(local_config='000-control.yaml', ...)
# Then re-run Cell 8 to rebind r, e, d and surface schemas.
```

### 4.3 Nuclear: restart the kernel

If anything in Resources' caching, the Hive metastore, or the
Python module cache for `lhn`/`spark_config_mapper` is stale, just
restart. You'll lose the SparkSession but that's usually the
fastest fix.

---

## 5. Dictionary tables

### 5.1 Where they live

- Physical schema: whatever `schemas.dictrwdSchema` in YAML resolves to
  (typically `datadictrwd` for Cerner RWD data).
- Runtime binding: `resource.dictrwd` (the `type_key` from
  `dictrwdcallFunc` in `callFunProcessDataTables`). Also reachable as
  `d` after `load_into_local()` rebinds the short alias.

### 5.2 What they contain

One row per **distinct code** observed in the source schema, with
the standardized code value, its `primaryDisplay`, and its
`codingSystemId`. No personid, no tenant, no dates.

For Cerner RWD the most commonly used ones are:

| Table | Domain |
|---|---|
| `d.condition_conditioncode` | ICD-9 / ICD-10 / SNOMED condition codes |
| `d.lab_labcode` | LOINC lab codes |
| `d.procedure_procedurecode` | CPT / HCPCS / SNOMED procedure codes |
| `d.medication_drugcode` | Medication codes (RxNorm, NDC) |
| `d.measurement_measurementcode` | Vital-sign / measurement codes |
| `d.encounter_classification` | Encounter type levels |
| `d.demographics_source` | Data-source origin levels (EMR, BILLING, CLAIM) |

Run `sorted(d.keys())` in the notebook to see the full list.

### 5.3 Why you use them as `create_extract`'s `elementListSource`

A regex scan over `r.conditionSource.df` (tens of millions of
patient-level condition records) × N regex patterns is O(N·rows)
and in practice never completes on a full cohort. The same scan
over `d.condition_conditioncode.df` (a few tens of thousands of
distinct codes) runs in seconds.

**Rule:** `elementListSource` is *always* a `d.*` table. Never
`r.*`. This is a silent pipeline hang generator; catching it
requires domain knowledge of what the tables contain.

---

## 6. Skeleton pipeline for a new project

Copy this as a starting point. Substitute `<project>`, `<code-list>`,
etc.

### 6.1 YAML

```yaml
project: <project>
projectSchema: "<project>_ai"

schemas:
  RWDSchema: iuhealth_ed_data_cohort_202306
  dictrwdSchema: "datadictrwd"
  projectSchema: "<project>_ai"
schemaTag: RWD
disease: "<disease>"

dataLoc: "/home/${systemuser}/work/Users/${systemuser}/inst/extdata/<project>/"

callFunProcessDataTables:
  RWDcallFunc:
    data_type: RWDTables
    schema_type: RWDSchema
    type_key: "r"
    property_name: "rwd"
  dictrwdcallFunc:
    data_type: null
    schema_type: dictrwdSchema
    type_key: "dictrwd"
    property_name: "d"
    tableNameTemplate: "_rwd"
    updateDict: True

# Regex patterns for your cohort
search_string_mydisease: "<regex here>"

myDiseaseDict:
  group1: ${search_string_mydisease}

projectTables:
  # ---- Stage 1: raw codes from inline YAML ----
  mycodes:
    label: "Disease codes"
    dictionary: ${myDiseaseDict}
    listIndex: "codes"
    sourceField: "conditioncode_standard_id"

  # ---- Stage 2: verified against dictionary ----
  mycodesVerified:
    label: "Verified disease codes"
    groupName: "group1"
    sourceField: "conditioncode_standard_id"
    indexFields:
      - "conditioncode_standard_primaryDisplay"
      - "conditioncode_standard_id"
      - "conditioncode_standard_codingSystemId"

  # ---- Stage 3: patient-level encounter records ----
  myEncounter:
    label: "Patient-level disease condition records"
    datefield: "datetimeCondition"
    indexFields: ["personid", "tenant"]
    fields: ["personid", "tenant", "encounterid",
             "conditioncode_standard_id", "datetimeCondition", "group"]

  # ---- Stage 4: cohort index (one row per person) ----
  myCohort:
    label: "Disease cohort — one row per person"
    code: "MY"
    datefieldPrimary: ["datetimeCondition"]
    datefieldStop: ["datetimeCondition"]
    indexFields: ["personid", "tenant"]
    sort_fields: ["datetimeCondition"]
    histStart: "1990-01-01"
    histEnd: "2025-01-01"
    retained_fields: ["group"]

  # ---- Utility: person-tenant map ----
  persontenant:
    label: "Person-tenant infrastructure (full encounterSource)"
    indexFields: ["personid", "tenant"]
```

### 6.2 Notebook

```python
# Cells 4, 6, 7, 8 — copy verbatim from section 3 above.

# ---- Stage 1 ----
e.mycodes.dict2pyspark()
e.mycodes.print_pd(label='Disease codes')

# ---- Stage 2 ----
e.mycodesVerified.create_extract(
    elementList=e.mycodes,
    elementListSource=d.condition_conditioncode.df,   # DICTIONARY, not r.*
    find_method='regex',
    sourceField='conditioncode_standard_id',
)
e.mycodesVerified.print_pd(label='Verified disease codes')
e.mycodesVerified.attrition()

# ---- Stage 3 ----
e.myEncounter.entityExtract(
    e.mycodesVerified,
    r.conditionSource.df,
)
e.myEncounter.attrition()

# ---- Stage 4 ----
e.myCohort.write_index_table(
    inTable=e.myEncounter,
    indexLabel="index_",
    lastLabel="last_",
)
e.myCohort.attrition()

# ---- Utility ----
e.persontenant.df = (
    r.encounterSource.df
    .select('personid', 'tenant')
    .distinct()
)
e.persontenant.write()   # manual assignment doesn't auto-write
e.persontenant.attrition()

# ---- Persist ----
write_counts = e.write_all(names=[
    'mycodes', 'mycodesVerified', 'myEncounter', 'myCohort', 'persontenant',
])
```

---

## 7. Things that bite, in one list

These are the recurring failure modes that have caused real problems
in production pipelines. If something breaks, check these first.

1. **`elementListSource=r.*`** instead of `d.*` → query never terminates.
2. **`d = resource.d` without `locals().update(resource.load_into_local())`** → `d.<table>.df` raises AttributeError.
3. **Two notebooks writing to the same ExtractItem** → YAML last-write-wins on Hive; subtle content drift between runs. Enforce one writer per table.
4. **`dict2pyspark(columnname=['codes'])` with a list default** (lhn < 0.2.1) → creates pandas MultiIndex columns, Spark column names become `('codes',)`. Use `columnname='codes'`.
5. **`entityExtract(entitySource=..., elementIndex=...)` with no `elementList`** → first positional arg is required; signature changed in `c745401`.
6. **Manual `e.foo.df = ...` assignment** → does NOT auto-write. Call `e.foo.write()` or `e.write_all(...)`.
7. **`create_extract` result assigned directly to a patient-level target** → you get the dictionary rows, not patient records. Always follow `create_extract` with `entityExtract`.
8. **Changing `000-control.yaml` without refreshing Resources** → stale schema mappings, "table not found", or worse, silent old-value use. Re-run Cell 7 or restart kernel.
9. **Hardcoding `~/work/Users/hnelson3`** as a fallback path → breaks for any other user. Use `getpass.getuser()` + existence check.
10. **Writing a new table via `SimpleNamespace(df=None)`** as a workaround for no config entry → bypasses attrition, partitioning, and `write_all`. Add the entry to YAML instead.
11. **`item.df` after a process failure returns the raw, un-flattened DataFrame** — `Item.process()` catches errors, sets `status = ITEM_FAILED`, and the `df` setter refuses to clobber that status. No exception surfaces. Always check `item.status == 'PROCESSED'` or run `r.report_str()` after `processDataTables`.
12. **`inputRegex` patterns match the underscore-flattened column name**, not the dotted source path. `^encounter\.id$` silently matches nothing; use `^encounter_id$`. Matching is case-insensitive via `re.search`.
13. **Multi-array source tables silently drop arrays** under default lenient `process()` (`error_on_multiple_arrays=False`). The dropped array columns are named only in a debug log. Specify `explode_array=` at the call site, or pass `strict=True` to `processDataTables` so the drop becomes an error.
14. **`code: ""` or missing `code:` in a `write_index_table` target** produces columns literally named `index_` and `last_` (trailing underscore). Always set `code: "X"` so you get `index_X` / `last_X`.
15. **`max_gap` produces multi-row-per-person output.** Downstream joins on `personid` without also keying on the episode id will multiply rows. Read `identification.py:155-237` before adding `max_gap` to a new table.
16. **`insert:` directives run `eval(f"df.{code}")`** — arbitrary Python with caller privileges. Never load YAML with `insert:` entries from an untrusted source, and prefer explicit notebook transforms over YAML-embedded `eval`.
17. **`load_into_local(everything=False)` (default) silently filters out missing tables.** If a table you expect isn't there, you get no error. Call `everything=True` or run `print(r.report_str())` first.
18. **`ItemLoadError` raises only on first `.df` access.** After the first raise, `_df` is still `None` and subsequent `.df` access returns `None` because the `status == ITEM_FAILED` check fires before load retry. Don't rely on a second access to recover.
19. **`writeTable` partition silently skipped** when `partitionBy` is misspelled (`if partitionBy and partitionBy in df.columns`). No error, no partitioning. Inspect the resulting table's `DESCRIBE EXTENDED`.
20. **`dataLoc` trailing slash matters.** `Item.csv = f"{dataLoc}{TBL}_{disease}_{schemaTag}.csv"` has NO separator between `dataLoc` and `TBL`, so a missing `/` gives you paths like `.../SickleCell_AIscdpatient_SCD_RWD.csv`. Always end `dataLoc` with `/`.
