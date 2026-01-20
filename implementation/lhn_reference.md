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
| `Resources` | Class | Central orchestrator managing configuration, schemas, and data access | `resource = Resources(project='Study', spark=spark, process_all=True)` |
| `Extract` | Class | Container for ExtractItem objects representing project output tables | `e = Extract(resource.proj)` |
| `ExtractItem` | Class | Extends Item with extraction workflow methods | `e.cohort.create_extract(...)` |
| `DB` | Class | Wrapper providing direct DataFrame access from TableList | `resource.r.conditionSource` returns DataFrame |
| `SharedMethodsMixin` | Mixin | Common methods: showIU(), attrition(), write(), toPandas(), etc. | Available on Item, ExtractItem, DB |
| `showIU` | Method | Display sample records filtered by tenant (e.g., IUHealth) | `e.cohort.showIU(obs=10, tenant=127)` |
| `create_extract` | Method | Step 1: Identify codes from reference tables using regex/merge | `e.codes.create_extract(elementList=..., find_method='regex')` |
| `entityExtract` | Method | Step 2: Extract patient records matching identified codes | `e.encounters.entityExtract(elementList=e.codes, entitySource=r.source)` |
| `write_index_table` | Method | Step 3: Create patient-level summary with first/last dates | `e.index.write_index_table(inTable=e.encounters)` |
| `attrition` | Method | Display record and patient counts with date ranges | `item.attrition()` |
| `tabulate` | Method | Aggregate and display counts by field | `item.tabulate(by='field', obs=20)` |
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

```
Resources (orchestrator)
├── Configuration (from spark_config_mapper):
│   ├── self.config_dict        : Merged configuration
│   └── self.config_table_locations : Config file paths
│
├── Per-Schema Attributes:
│   ├── self.rwd                : TableList[Item] - source tables
│   ├── self.r                  : DB - DataFrame access to rwd
│   ├── self.proj               : TableList[Item] - project tables
│   └── self.db                 : DB - DataFrame access to proj
│
└── Extraction:
    └── Extract(resource.proj)  : Container of ExtractItem objects

Extract
├── Created from TableList of project tables
├── Contains ExtractItem for each table in config
└── Access: e.table_name returns ExtractItem

ExtractItem (extends Item)
├── Inherited: df, location, existsDF, csv, parquet
├── Additional: create_extract(), entityExtract(), load_csv_as_df()
└── Workflow methods for three-step extraction

SharedMethodsMixin (inherited by Item, ExtractItem, DB)
├── showIU(obs, tenant, sortfield) - display sample (tenant-filtered)
├── attrition(person_id, date_field) - patient/record counts
├── show(n, truncate) - basic DataFrame display
├── toPandas(limit) - convert to pandas
├── count() - row count
├── write(outTable, mode) - write to Spark table
├── to_csv(path) - export to CSV
├── to_parquet(path) - export to Parquet
├── load(table_path) - load DataFrame from table
├── filter(condition) - filter rows
├── select(*cols) - select columns
├── join(other, on, how) - join DataFrames
├── cache() / unpersist() - memory management
└── properties() / values() - introspection
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
e.target_codes.create_extract(
    elementList=d.condition_codes,           # Reference table Item
    elementListSource=d.condition_codes.df,  # Reference DataFrame
    find_method='regex'                      # 'regex' or 'merge'
)
```

**Parameters**:
- `elementList`: Item containing search patterns
- `elementListSource`: DataFrame to search within
- `find_method`: 'regex' for pattern matching, 'merge' for exact join

**Output**: DataFrame with matched codes stored in `e.target_codes.df`

### 5.3 Step 2: entityExtract

Extract patient records matching identified codes.

```python
e.condition_encounters.entityExtract(
    elementList=e.target_codes,              # From Step 1
    entitySource=r.conditionSource,          # Source data table
    cohort=e.persontenant.df,                # Optional: filter to cohort
    cacheResult=True
)
```

**Parameters**:
- `elementList`: ExtractItem with identified codes (from Step 1)
- `entitySource`: Source DataFrame or Item
- `cohort`: Optional DataFrame to filter patients
- `cacheResult`: Cache result for repeated access

**Output**: DataFrame with patient records stored in `e.condition_encounters.df`

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

### 6.1 000-config.yaml (Project Config)

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
# Returns: 'Black', 'White', 'Asian', 'Indigenous', 'Hispanic', 'Mixed', 'Unknown', 'Other'
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

Aggregate and display counts by field:

```python
e.condition_index.tabulate(
    by='conditioncode_standard_primaryDisplay',
    index=['personid'],
    obs=20                           # Top 20 values
)
# Output: Top 20 condition codes by patient count
```

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
    schema_type: 'RWDSchema'     # Schema key (in 000-config.yaml)
    type_key: 'rwd'              # → resource.rwd (TableList)
    property_name: 'r'           # → resource.r (DB)
```

### 10.2 Standard Initialization

```python
from lhn import Resources, Extract

# Resources reads callFunProcessDataTables and creates all objects
resource = Resources(
    project='MyStudy',
    spark=spark,
    basePath=Path.home()/'work/Users/hnelson3',
    config_file='000-config.yaml',
    process_all=True                 # Auto-process all configured schemas
)

# Attributes created based on callFunProcessDataTables rules:
r = resource.r      # DB for RWD source tables (DataFrame access)
db = resource.db    # DB for project tables (DataFrame access)

# Create Extract for rich methods
e = Extract(resource.proj)

# Access tables
r.conditionSource.filter(...)       # DB: direct DataFrame access
e.cohort.showIU(obs=10)             # Extract: rich methods
```

### 10.3 Preferred Pattern: load_into_local

```python
from lhn import Resources

resource = Resources(project='MyStudy', spark=spark, process_all=True)

# load_into_local creates Extract and loads everything to local namespace
locals().update(resource.load_into_local(
    everything=False,
    schemakey='projectSchema',
    extractName='e'
))

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
    project='DiabetesStudy',
    spark=spark,
    basePath=Path.home()/'work',
    process_all=True
)

e = Extract(resource.proj)
r = resource.r

# Step 1: Identify diabetes codes (E11.x)
e.dm2_codes.create_extract(
    elementList=e.dm2_codes,
    elementListSource=r.condition_codes.df,
    find_method='regex'
)
e.dm2_codes.attrition()

# Step 2: Extract condition encounters
e.dm2_encounters.entityExtract(
    elementList=e.dm2_codes,
    entitySource=r.conditionSource,
    cacheResult=True
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
e.persontenant.tabulate(by='tenant')
```

---

## 12. ERROR HANDLING

| Error | Cause | Resolution |
|-------|-------|------------|
| `AttributeError: 'Resources' has no attribute 'rwd'` | Schema not found or not processed | Check schema exists, use `process_all=True` |
| `KeyError: 'projectTables'` | Config not loaded | Verify config file path and structure |
| `Empty DataFrame` | No matching records | Check date ranges, code patterns, schema |
| `Table not found` | Output table doesn't exist yet | Run extraction workflow first |
| `No matching codes` | Regex pattern issue | Test pattern against actual data |

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
