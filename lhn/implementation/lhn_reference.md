# LHN Package: LLM Reference Document

> **Purpose**: Machine-readable reference for LLM context injection. Provides precise definitions, class relationships, and workflow patterns for the lhn healthcare data package.

---

## 1. PACKAGE OVERVIEW

**lhn** is a Python package for healthcare data extraction and analysis built on PySpark. It provides:

1. **Cohort Building** - Patient identification, demographics, matching
2. **Data Extraction** - Three-step workflow: codes → encounters → index tables
3. **Analysis Methods** - Attrition tracking, tabulation, statistical summaries

**This package depends on spark_config_mapper** for configuration management.

---

## 2. GLOSSARY OF TERMS

| Term | Type | Definition | Example |
|------|------|------------|---------|
| `Resources` | Class | Central orchestrator managing configuration, schemas, and data access | `Resources(local_config=..., global_config=..., schemaTag_config=...)` |
| `Extract` | Class | Container for ExtractItem objects, auto-created during `Resources.finish_init` (default True) | `e = resource.e` |
| `ExtractItem` | Class | A single configured table; has `.df`, extraction verbs (`create_extract`, `entityExtract`, `write_index_table`, `load_csv_as_df`, `dict2pyspark`) | `e.cohort.create_extract(...)` |
| `Item` | Class | Single-source-table object from `spark_config_mapper`; `r.*` and `d.*` members are `Item`s | `r.conditionSource.df` |
| `TableList` | Class | Dict-like container of `Item`s indexed by name; what `resource.r`, `resource.rwd`, `resource.dictrwd` actually are | `for name in r: print(r[name].df.count())` |
| `SharedMethodsMixin` | Mixin | Methods common to Item / ExtractItem: `showIU`, `attrition`, `tabulate`, `print_pd`, `write`, `to_csv`, `properties`, `values` | Available on Item, ExtractItem |
| `showIU` | Method | Display sample records filtered by tenant (e.g., IUHealth) | `e.cohort.showIU(obs=10, tenant=127)` |
| `create_extract` | Method | Step 1: Identify codes from reference tables using regex/merge | `e.codes.create_extract(elementList=..., find_method='regex')` |
| `entityExtract` | Method | Step 2: Extract patient records matching identified codes. 1st positional arg is `elementList`, 2nd is `entitySource` | `e.encounters.entityExtract(e.codes, r.source.df)` |
| `write_index_table` | Method | Step 3: Create patient-level summary with first/last dates | `e.index.write_index_table(inTable=e.encounters)` |
| `attrition` | Method | Display record and patient counts with date ranges | `item.attrition()` |
| `tabulate` | Method | Aggregate + display counts by column. Actual signature: `tabulate(group_cols=None, count_distinct=None, order_by='count', limit=50, dropna=False, show=False)` | `item.tabulate(group_cols=['IcdPheno'], show=True)` |
| `cohort` | Concept | Set of patients meeting study criteria | Identified by personid + tenant |
| `index_date` | Concept | First occurrence date for a patient | `index_COND` = first condition date |
| `last_date` | Concept | Last occurrence date for a patient | `last_COND` = last condition date |

---

## 3. PACKAGE STRUCTURE

```
lhn/
├── __init__.py              # Public API exports
├── header.py                # Imports, logging, Spark session
├── core/
│   ├── __init__.py
│   ├── resource.py          # Resources class - central orchestration
│   ├── extract.py           # Extract, ExtractItem classes
│   ├── db.py                # DB class for DataFrame access
│   └── shared_methods.py    # SharedMethodsMixin
└── cohort/
    ├── __init__.py
    ├── identification.py    # write_index_table, identify_target_records
    └── demographics.py      # group_races, group_ethnicities, etc.
```

---

## 4. CLASS RELATIONSHIPS

`r`, `rwd`, `dictrwd`, `proj` etc. are all `TableList[Item]` — the same
type. The naming on `Resources` comes from the `type_key` /
`property_name` pair declared under `callFunProcessDataTables` in the
YAML. `DB` is a separate single-table wrapper (`db.py`) — it is NOT
what `resource.r` is.

```
Resources (orchestrator)
├── Configuration:
│   ├── self.config            : Merged YAML from local/global/schemaTag
│   ├── self.projectSchema     : Hive schema where Extract writes
│   ├── self.RWDSchema, etc.   : Source/dictionary schema names
│   └── self._original_paths   : Config file paths
│
├── Per-schema TableLists (type_key bindings from callFunProcessDataTables):
│   ├── self.r                 : TableList[Item] — RWD source tables
│   ├── self.rwd               : dict of ConfigObj (property_name variant)
│   ├── self.dictrwd           : TableList[Item] — dictionary tables
│   ├── self.d                 : dict of ConfigObj (property_name for dictrwd)
│   └── self.proj              : dict of ConfigObj used to build Extract
│
├── Extract (project outputs):
│   └── self.e                 : Extract — built from self.proj during finish_init
│
└── Helpers:
    ├── load_into_local(everything=False, load_schemas=True)
    │                          : returns dict that remaps `d → dictrwd`,
    │                            plus RWDSchema/projectSchema/dataLoc etc.
    ├── reread_config_files()  : re-read YAML without rebuilding tables
    └── list_tables()          : names of configured source tables

TableList[Item]
├── Iterable by name: `for name in r: ...`
├── Dict-style access: `r['conditionSource']` (same as attr access)
├── Attr-style access: `r.conditionSource`
└── Each member is an Item with .df, .location, .status, etc.

Item (spark_config_mapper)
├── .df            : Spark DataFrame (lazy-loaded from .location)
├── .location      : "{schema}.{name}" Hive table path
├── .status        : UNLOADED | LOADED | PROCESSED | FAILED | NOT_FOUND
├── .load_error    : exception string if status == FAILED during load
├── .process_error : exception string if status == FAILED during process
├── .process(strict=False) : apply inputRegex + insert + colsRename
├── All YAML keys attached as attributes (label, indexFields,
│                   retained_fields, datefield, partitionBy, ...)
└── Inherits SharedMethodsMixin

Extract (lhn.core.extract)
├── One attribute per entry in projectTables → ExtractItem
├── .properties()   : list of ExtractItem names
├── .__iter__       : iterate names
├── .__getitem__    : e['name-with-hyphens'] for non-identifier names
├── .__len__        : number of ExtractItems
└── .write_all(names=None, skip_empty=True, verbose=True)
                    : persist every (or specified) ExtractItem via .write()

ExtractItem (extends Item via SharedMethodsMixin + extraction verbs)
├── Inherited: df, location, status, all YAML attrs, SharedMethodsMixin
└── Verbs (all auto-write to .location on success):
    ├── dict2pyspark(columnname='codes')   — build from YAML dict
    ├── load_csv_as_df()                   — build from a CSV
    ├── create_extract(elementList, elementListSource, find_method, sourceField)
    ├── entityExtract(elementList, entitySource, elementIndex=None, ...)
    └── write_index_table(inTable, histStart=None, histEnd=None, ...)

DB (spark_config_mapper/core/db.py)
└── Single-table wrapper. Rarely used directly in notebook code.
    NOT what self.r is. Accessed explicitly when needed.

SharedMethodsMixin (inherited by Item and ExtractItem)
├── showIU(obs, tenant, sortfield)       — tenant-filtered sample
├── attrition(person_id, date_field)     — patient/record counts
├── tabulate(group_cols, count_distinct, order_by, limit, dropna, show)
├── print_pd(label='', sortfield='Subjects', obs=5, sort_order='desc')
├── toPandas(limit)
├── count()
├── write(outTable=None, mode='overwrite')
├── to_csv(path) / to_parquet(path)
├── filter / select / join / cache / unpersist — MUTATE self.df IN PLACE
├── properties()  — dict of YAML keys + runtime attrs (status, load_error, …)
└── values()      — pprint self.__dict__
```

---

## 5. THREE-STEP EXTRACTION WORKFLOW

### 5.1 Workflow Overview

```
Step 1: create_extract     → Identify codes of interest
        ↓
Step 2: entityExtract      → Extract patient records matching codes
        ↓
Step 3: write_index_table  → Create patient-level summary
```

### 5.2 Step 1: create_extract

Identify codes from reference tables using regex or exact matching.

```python
# Typical pipeline pattern — raw codes loaded from config dict or CSV,
# then verified against a dictionary table. The elementList is the
# *raw* ExtractItem (e.raw_codes); the elementListSource is the
# *dictionary* DataFrame (d.condition_conditioncode.df).
e.target_codes.create_extract(
    elementList=e.raw_codes,                          # ExtractItem: patterns
    elementListSource=d.condition_conditioncode.df,   # DataFrame: dictionary
    find_method='regex',                              # 'regex' or 'merge'
    sourceField='conditioncode_standard_id',          # column to scan
)
```

**Call-time arguments**:
- `elementList` — ExtractItem (or dict / list / DataFrame) containing
  search patterns. Usually `e.<raw_codes>` produced by `dict2pyspark`
  or `load_csv_as_df`.
- `elementListSource` — DataFrame to search within. Always a dictionary
  table (`d.*.df`), never a patient-level source.
- `find_method` — `'regex'` (scan `sourceField` with patterns) or
  `'merge'` (exact-value join on `listIndex`). Falls back to
  `self.find_method`, then defaults to `'regex'`.
- `sourceField` — column in `elementListSource` to scan / join on.
  Falls back to `self.sourceField` from YAML.

**Required YAML parameters on the receiver** (`e.target_codes`):

| Key | Required? | Purpose |
|---|---|---|
| `label` | Yes (for auto-write) | Description written to Hive. |
| `sourceField` | Yes if not passed explicitly | Column to scan in `elementListSource`. |
| `indexFields` | Yes | Columns retained on the verified output; used later as default `elementIndex` by `entityExtract` when this item is an elementList. |
| `groupName` | No | Column that tags which regex matched. Defaults to `'group'`. |
| `find_method` | No | `'regex'` (default) or `'merge'`. |
| `retained_fields` | No | Output columns to project to. `indexFields` + `'group'` are always kept. |

**Required YAML parameters on the elementList** (`e.raw_codes`):

| Key | Required? | Purpose |
|---|---|---|
| `label` | Yes (if you want auto-write) | Description. |
| `listIndex` | Yes (for regex mode) | Column in `elementList.df` that holds the regex patterns. Read by the verb: `getattr(elementList, 'listIndex', 'codes')`. |
| `dictionary` | If building via `dict2pyspark` | Inline `{group: pattern}` map from YAML. |
| `csv` | If building via `load_csv_as_df` | Path to a CSV with the codes. |
| `groupName` | No | Fallback group-column name, used if the receiver doesn't set one. |
| `sourceField` | No | Conventionally set equal to the receiver's `sourceField` — informational only when passed explicitly in the call. |

**Output**: DataFrame with matched codes stored in `e.target_codes.df`,
auto-written to `<projectSchema>.target_codes`.

### 5.3 Step 2: entityExtract

Extract patient records matching identified codes.

```python
e.condition_encounters.entityExtract(
    e.target_codes,                  # ExtractItem: verified codes from Step 1
    r.conditionSource.df,            # DataFrame: patient-level source table
    cohort=e.persontenant,           # Optional: filter patients to a cohort
    cacheResult=True,
)
```

**Call-time arguments**:
- `elementList` (1st positional) — ExtractItem (or object with `.df`)
  holding the verified codes from Step 1.
- `entitySource` (2nd positional) — patient-level source DataFrame or
  Item (e.g., `r.conditionSource`). This is the large table being
  filtered.
- `elementIndex` — Join columns. If not passed, falls back to
  `elementList.indexFields`, then `self.indexFields`, then
  `['personid']`.
- `cohort` — Optional ExtractItem/DataFrame to inner-join against
  before extraction, restricting results to cohort persons.
- `set_self_df` — If `False`, returns the DataFrame without
  assigning to `self.df` (and no auto-write). Default `True`.

**Required YAML parameters on the receiver** (`e.condition_encounters`):

| Key | Required? | Purpose |
|---|---|---|
| `label` | Yes (for auto-write) | Description. |
| `indexFields` | Yes | Default join key fallback (used if `elementIndex` isn't passed AND the elementList has no `indexFields`). |
| `datefield` | Usually | Date column for downstream filtering / windowing. |
| `fields` | No | Informational; you'll typically project downstream with `.select(...)`. |
| `retained_fields` | No | If set and the receiver is chained into another verb. |
| `partitionBy` | No | Hive partition column. |

**Required YAML parameters on the elementList** (`e.target_codes`):

| Key | Consumed as | Effect |
|---|---|---|
| `indexFields` | Default `elementIndex` (join key) | **This is the big one.** If `e.target_codes.indexFields = ['conditioncode_standard_primaryDisplay', 'conditioncode_standard_id', 'conditioncode_standard_codingSystemId']`, the `entityExtract` join happens on those three columns — even though the receiver never mentioned them. Check `e.target_codes.properties()` if a join is producing unexpected rows. |
| `label` | (not consumed by entityExtract; already set earlier for auto-write) | — |

**Output**: DataFrame with patient records stored in
`e.condition_encounters.df`, auto-written to
`<projectSchema>.condition_encounters`.

### 5.4 Step 3: write_index_table

Create patient-level summary with first/last dates.

```python
e.condition_index.write_index_table(
    inTable=e.condition_encounters,          # From Step 2
    histStart='2020-01-01',
    histEnd='2024-12-31'
)
```

**Output columns** (example with `code='COND'`):
- `personid`, `tenant` - Patient identifiers
- `index_COND` - First occurrence date
- `last_COND` - Last occurrence date
- `encounter_days` - Days between first and last
- `course_of_therapy` - Number of distinct courses (if gap-based)
- Retained fields from config

---

## 6. CONFIGURATION STRUCTURE

### 6.1 000-control.yaml (Project Config)

```yaml
project: MyStudy
disease: DM                          # Abbreviation for table naming
schemaTag: RWD                       # Schema type

schemas:
  RWDSchema: source_database         # Source data schema
  projectSchema: study_output        # Output schema

projectTables:
  persontenant:                      # Cohort identifier table
    label: "Cohort patients"
    indexFields: ["personid", "tenant"]

  target_codes:                      # Code identification table
    label: "Target condition codes"
    dictionary:
      diabetes: "E11\\.[0-9]"
    listIndex: "codes"
    sourceField: "conditioncode_standard_id"

  condition_encounters:              # Entity extraction table
    label: "Condition records"
    datefield: "datetimeCondition"
    histStart: "${historyStart}"
    histEnd: "${historyStop}"
    indexFields: ["personid", "tenant"]
    retained_fields:
      - "conditioncode_standard_id"
      - "datetimeCondition"

  condition_index:                   # Patient-level summary table
    label: "Condition patient index"
    indexFields: ["personid", "tenant"]
    datefieldPrimary: "datetimeCondition"
    code: "COND"                     # Creates index_COND, last_COND columns
```

### 6.2 Table Types by Purpose

| Table Type | Config Pattern | Purpose |
|------------|---------------|---------|
| Cohort ID | `indexFields: [personid, tenant]` | Identify study patients |
| Code Search | `dictionary:` or `csv:` | Define codes to search for |
| Entity | `datefield:`, `retained_fields:` | Extract source records |
| Index | `datefieldPrimary:`, `code:` | Patient-level summary |
| Demographics | `datefield: birthdate` | Patient characteristics |

---

## 7. COMMON FIELD NAMES

### 7.1 Condition Fields
- `conditioncode_standard_id` - ICD-10 or diagnosis code
- `conditioncode_standard_primaryDisplay` - Code description
- `datetimeCondition` - Condition date/time
- `classification_standard_primaryDisplay` - Diagnosis type

### 7.2 Medication Fields
- `drugcode_standard_id` - Drug code (MULTUM, NDC)
- `drugcode_standard_primaryDisplay` - Drug name
- `datetimeMed` - Medication date
- `startdate`, `stopdate` - Medication duration

### 7.3 Lab Fields
- `labcode_standard_id` - LOINC code
- `labcode_standard_primaryDisplay` - Test name
- `typedvalue_numericValue_value` - Numeric result
- `datetimeLab` - Lab date

### 7.4 Common Fields
- `personid` - Patient identifier
- `tenant` - Data source/tenant
- `encounterid` - Encounter identifier

---

## 8. DEMOGRAPHICS FUNCTIONS

### 8.1 Race Grouping

```python
from lhn.cohort import group_races

df = group_races(
    df=demographics_df,
    column_name='race',
    result_column_name='race_group'
)
# Returns one of: 'Black', 'Indigenous', 'Asian', 'White',
#                 'Middle Eastern', 'Caribbean', 'Hispanic',
#                 'Mixed', 'Other/Unknown'
# (see lhn/cohort/demographics.py:46-72 for the exact input-value mapping)
```

### 8.2 Ethnicity Grouping

```python
from lhn.cohort import group_ethnicities

df = group_ethnicities(
    df=demographics_df,
    column_name='ethnicity',
    result_column_name='ethnicity_group'
)
# Returns: 'Hispanic', 'Not Hispanic'
```

### 8.3 Gender Grouping

```python
from lhn.cohort import group_gender

df = group_gender(
    df=demographics_df,
    column_name='gender',
    result_column_name='gender_group'
)
# Returns: 'Female', 'Male', 'Other', 'Unknown'
```

---

## 9. ANALYSIS METHODS

### 9.1 showIU()

Display sample records filtered by tenant (useful for data exploration without exposing PHI from other data sources):

```python
e.cohort.showIU(
    obs=10,              # Number of rows to display
    tenant=127,          # IUHealth Expert Determination (82 = Safe Harbor)
    sortfield=['personid'],
    sort_order='asc',
    label='Cohort Sample'
)
```

**Tenant IDs**:
- `127` - IUHealth Expert Determination (default)
- `82` - IUHealth Safe Harbor

### 9.2 attrition()

Display record and patient counts with date ranges:

```python
e.cohort.attrition()
# Output:
# Table cohort: 1,234,567 rows, 45,678 unique patients
# Date range: 2020-01-01 to 2024-12-31
```

### 9.3 tabulate()

Aggregate and display counts by one or more columns.

**Actual signature** (`shared_methods.py:261`):

```python
tabulate(
    group_cols=None,        # list[str] — columns to group by
    count_distinct=None,    # column to count distinct values of
    order_by='count',       # 'count' | 'group_cols' — sort order
    limit=50,               # max rows to return
    dropna=False,           # drop groups with null in any group_col
    show=False,             # print to notebook output
)
```

Example — top 20 condition codes by distinct patient count:

```python
e.condition_index.tabulate(
    group_cols=['conditioncode_standard_primaryDisplay'],
    count_distinct='personid',
    limit=20,
    show=True,
)
```

Note: the parameters `by=`, `index=`, `obs=` do NOT exist; an older
version of this document invented them. If you see a call like
`.tabulate(by='...', obs=N)` in a notebook, fix it to the above.

### 9.4 count_people()

Count unique patients:

```python
from lhn import count_people

count_people(
    df=cohort_df,
    description='Final cohort',
    person_id='personid'
)
```

---

## 10. INITIALIZATION SEQUENCE

### 10.1 How Resources Works

`Resources` reads `callFunProcessDataTables` from config-global.yaml and automatically creates TableList/DB objects:

```yaml
# config-global.yaml
callFunProcessDataTables:
  RWDcallFunc:
    data_type: 'RWDTables'       # Table definitions (in config-RWD.yaml)
    schema_type: 'RWDSchema'     # Schema key (in 000-control.yaml)
    type_key: 'rwd'              # → resource.rwd (TableList)
    property_name: 'r'           # → resource.r (DB)
```

### 10.2 Standard Initialization

```python
from lhn import Resources, Extract

# Resources reads callFunProcessDataTables and creates all objects
resource = Resources(
    local_config='000-control.yaml',
    global_config='configuration/config-global.yaml',
    schemaTag_config='configuration/config-RWD.yaml',
    # finish_init=True is the default and drives all schema processing.
    # Set finish_init=False only if you want to instantiate Resources
    # without loading any data (rare; mostly for unit tests).
)

# Attributes created based on callFunProcessDataTables rules (type_key):
r = resource.r          # TableList[Item] for RWD source tables
d = resource.dictrwd    # TableList[Item] for dictionary tables
e = resource.e          # Extract of project output ExtractItems

# Access tables
r.conditionSource.df.filter(...)    # Item: .df is the DataFrame
e.cohort.showIU(obs=10)             # ExtractItem: rich methods
d.condition_conditioncode.df.count()  # Dictionary Item
```

### 10.3 Preferred Pattern: load_into_local

```python
from lhn import Resources

resource = Resources(
    local_config='000-control.yaml',
    global_config='configuration/config-global.yaml',
    schemaTag_config='configuration/config-RWD.yaml',
    debug=True,                                    # logs config load + processing
    # finish_init=True                             # default; skips only if False
)

# load_into_local surfaces all type_key + property_name bindings into
# the local namespace (r, e, d, rwd, RWDSchema, projectSchema, etc.).
# Signature: load_into_local(everything=False, load_schemas=True).
# Pass everything=True to include all config values (disease,
# schemaTag, dataLoc, parquetLoc) in addition to the objects.
locals().update(resource.load_into_local())

# Now available directly: e, r, db, etc.
e.cohort.showIU(obs=10)
r.conditionSource.count()
```

### 10.4 Objects Created

| Object | Type | Created By | Access |
|--------|------|------------|--------|
| `resource.rwd` | TableList | RWDcallFunc rule | `resource.rwd.conditionSource.df` |
| `resource.r` | DB | RWDcallFunc rule | `r.conditionSource` (DataFrame) |
| `resource.proj` | TableList | projectcallFunc rule | `resource.proj.cohort.df` |
| `resource.db` | DB | projectcallFunc rule | `db.cohort` (DataFrame) |
| `e` | Extract | `Extract(resource.proj)` | `e.cohort.showIU()` |

---

## 11. COMPLETE WORKFLOW PATTERN

```python
from lhn import Resources, Extract
from lhn.cohort import write_index_table
from spark_config_mapper.utils import writeTable
from pathlib import Path

# Initialize
resource = Resources(
    local_config='000-control.yaml',
    global_config='configuration/config-global.yaml',
    schemaTag_config='configuration/config-RWD.yaml',
)

e = resource.e          # auto-created by finish_init (default)
r = resource.r
d = resource.dictrwd    # dictionary tables — use as elementListSource

# Step 1a: Build raw code list from YAML dictionary
e.dm2_raw.dict2pyspark()

# Step 1b: Verify against the condition *dictionary* (d.*, not r.*).
# elementList and receiver must be different items; receiver holds the
# verified output.
e.dm2_codes.create_extract(
    elementList=e.dm2_raw,
    elementListSource=d.condition_conditioncode.df,   # DICTIONARY, not r.*
    find_method='regex',
    sourceField='conditioncode_standard_id',
)
e.dm2_codes.attrition()

# Step 2: Extract condition encounters — 1st positional is elementList,
# 2nd is entitySource
e.dm2_encounters.entityExtract(
    e.dm2_codes,                # verified codes
    r.conditionSource.df,       # patient-level source
)
e.dm2_encounters.attrition()

# Step 3: Create patient index
e.dm2_index.write_index_table(
    inTable=e.dm2_encounters
)
e.dm2_index.attrition()

# Step 4: Create cohort table
cohort = e.dm2_index.df.select('personid', 'tenant').distinct()
writeTable(cohort, e.persontenant.location, description='Study cohort')

# Step 5: Verify
e.persontenant.tabulate(group_cols=['tenant'], show=True)
```

---

## 12. ERROR HANDLING AND SILENT FAILURE MODES

### 12.1 Item.status lifecycle

Every `Item` has a lifecycle `status` attribute that callers should
check before trusting `item.df`. Five constants (defined in
`spark_config_mapper/schema/mapper.py`):

| Status | Meaning |
|---|---|
| `ITEM_UNLOADED` | Declared in config but no DataFrame loaded yet. Lazy-load triggers on first `.df` access. |
| `ITEM_LOADED` | DataFrame read from Hive or assigned by a caller; not yet processed. |
| `ITEM_PROCESSED` | `Item.process()` completed successfully (inputRegex filter, insert directives, colsRename applied). This is the only state where `item.df` is guaranteed to be the fully-processed frame. |
| `ITEM_FAILED` | `Item.process()` raised in lenient mode (the default) or strict mode. `item.df` points at the raw pre-processing DataFrame, NOT the processed one. No exception surfaces unless `strict=True`. |
| `ITEM_NOT_FOUND` | The physical table doesn't exist in the configured schema. |

**Critical silent-failure mode:** when flatten fails under the default
lenient `processDataTables(..., strict=False)`, `item.status` becomes
`ITEM_FAILED` and `item.df` quietly returns the unflattened raw frame.
Code that does `r.foo.df.select('personid')` can fail three frames
later with a nonsensical `AttributeError` instead of surfacing the
real cause.

**Always run the report after `Resources(...)` init:**

```python
print(r.report_str())                   # summary of every Item's status
assert all(item.status == 'PROCESSED' for item in r.values())
```

For new pipelines and CI, prefer `strict=True` at both the config
load step and the table processing step:

```python
validate_tables_config(cfg['RWDTables'], strict=True)   # typo catcher
r = processDataTables(cfg['RWDTables'], strict=True, ...)
```

### 12.2 What `Item.process()` actually interprets

Of all the keys you can put under a table entry in YAML, `Item.process()`
reads exactly three:

| Key | Type | Effect |
|---|---|---|
| `inputRegex` | list[str] | After `flattenTable` produces underscore-joined column names, only columns whose flat name matches one of these regexes are retained. Matches via `re.search`, case-insensitive. |
| `insert` | list[str] | Each string is `eval(f"df.{insert_code}")` — arbitrary Python in your caller's scope. Used for computed columns. **Security warning:** never load YAML from untrusted sources. |
| `colsRename` | dict[str, str] | Applied after inputRegex/insert to rename columns. |

**Every other key in the YAML entry** (`label`, `indexFields`,
`retained_fields`, `datefield`, `histStart`, `histEnd`, `partitionBy`,
`cohortColumns`, `code`, `sort_fields`, `max_gap`, `dictionary`,
`sourceField`, `listIndex`, `find_method`, `groupName`, `complete`,
`inputs`, `description`, …) is stored on the Item via `setattr` and
read by *other methods* (`create_extract`, `entityExtract`,
`write_index_table`, `load_csv_as_df`). If you edit one of these and
the call doesn't change, the key is either consumed by a method you
aren't calling or read nowhere in the codebase.

### 12.3 `inputRegex` gotchas

- **Patterns match the underscore-flattened name**, not the dotted
  source path. `^encounter\.id$` matches nothing; use `^encounter_id$`.
- **Matching is case-insensitive** via `re.search`.
- **No pattern → no columns.** Missing `inputRegex` in a table config
  produces an empty `select_cols` list and the processed frame has
  only the join keys.
- **Misspelled patterns produce silent empty selects** — no error
  surfaces; `status` stays `ITEM_PROCESSED`. Check
  `len(item.df.columns)` against your expectation.

**Worked example.** A Cerner source schema has this nested structure:

```
encounter (struct)
  .id (string)
  .type (struct)
    .standard (struct)
      .primaryDisplay (string)
```

After `flattenTable` runs, the flat leaf names are
`encounter_id`, `encounter_type_standard_primaryDisplay`, etc.
(dots → underscores). So the correct `inputRegex`:

```yaml
encounterSource:
  source: encounter
  inputRegex:
    - ^personid$
    - ^encounter_id$
    - ^encounter_type_standard_primaryDisplay$
```

NOT:

```yaml
encounterSource:
  source: encounter
  inputRegex:
    - ^encounter\.id$                           # matches nothing
    - ^encounter\.type\.standard\.primaryDisplay$   # matches nothing
```

The dotted-path form will silently produce an Item with only
`personid`, no error, and `status == ITEM_PROCESSED`. You find out
three frames later when `item.df.select('encounter_id')` raises
`AnalysisException`.

### 12.4 Multi-array tables silently drop arrays

`Item.process()` calls `flattenTable(..., error_on_multiple_arrays=False)`
by default. A source table with multiple array columns has its arrays
dropped and only a `debug`-level log record emits. The resulting frame
is marked `ITEM_PROCESSED` — there's no status-level signal.

If your source has multiple arrays:
- Specify `explode_array: <name>` in the table's YAML to pick which
  one to explode, **or**
- Pass `strict=True` to `processDataTables` so the drop becomes an
  error.

### 12.5 Common errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `AttributeError: 'Resources' has no attribute 'rwd'` | Schema not found or `finish_init=False` was passed | Ensure the schema exists and `finish_init=True` (the default) is in effect |
| `AttributeError` on `d.<table>.df` | `d` is bound to `resource.d` (config dict), not the TableList | Run `locals().update(resource.load_into_local())` after Resources init |
| `KeyError: 'projectTables'` | Config not loaded (typo in local_config filename, or wrong project_path) | Verify config file path; check `resource.config.get('projectTables', {})` |
| `Empty DataFrame` | Regex matches nothing, wrong source (patient-level `r.*` vs dictionary `d.*`), or inputRegex flattened-name mismatch | Check `r.report_str()`, verify actual column names with `item.df.columns`, confirm source table |
| `TypeError: entityExtract() missing 1 required positional argument: 'elementList'` | Called entityExtract with kwargs only, missing the first positional arg | Pass elementList as first positional (signature was changed in lhn commit c745401) |
| Hung query, never completes | `elementListSource=r.*.df` instead of `d.*.df` | Use dictionary table as elementListSource; scanning patient-level data with N regex patterns is O(N × rows) |
| `ItemLoadError` on second `.df` access only | First access raised, `_df` still None, `status=ITEM_FAILED` | Inspect `item.load_error`; re-derive the table from source |

---

## 13. IMPORT REFERENCE

```python
# Core classes
from lhn import Resources, Extract, ExtractItem, DB

# Cohort functions
from lhn.cohort import (
    write_index_table,
    identify_target_records,
    group_races,
    group_ethnicities,
    group_gender
)

# Utilities (from spark_config_mapper)
from spark_config_mapper.utils import writeTable, flattenTable

# Spark
from pyspark.sql import functions as F
```

---

## 14. DEPENDENCIES

```python
# Required
spark-config-mapper >= 0.1.0
pyspark >= 3.0.0
pyyaml >= 5.0
pandas >= 1.0.0

# Optional
matplotlib >= 3.0.0  # For plotting
scipy >= 1.5.0       # For statistics
```
