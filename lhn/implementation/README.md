# LHN Implementation Guide

This directory contains documentation and examples for understanding and using the `lhn` healthcare data package.

## Directory Contents

| File | Purpose | Audience |
|------|---------|----------|
| `lhn_reference.md` | Complete API reference with class relationships | LLM context injection, developers |
| `lhn_project_initialization_guide.md` | Guide for generating project configs and notebooks | LLMs creating projects |
| `the_config-files/` | Example configuration files | All users |

## Three-Tier Configuration System

Only **000-control.yaml** changes per project. The other two tiers are shared infrastructure.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  TIER 1: 000-control.yaml (CHANGES PER PROJECT)                              │
│  Location: {basePath}/Projects/{project}/000-control.yaml                    │
│  Contains: project name, dates, schema mappings, projectTables              │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TIER 2: config-global.yaml (SHARED - DO NOT CHANGE)                        │
│  Location: {basePath}/configuration/config-global.yaml                      │
│  Contains: callFunProcessDataTables mappings, config_table_locations        │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  TIER 3: config-RWD.yaml (SHARED - DO NOT CHANGE)                           │
│  Location: {basePath}/configuration/config-RWD.yaml                         │
│  Contains: source table definitions (source, inputRegex, insert)            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### What Goes Where

| Configuration | File | Changes Per Project? |
|--------------|------|---------------------|
| Project name, disease, investigators | `000-control.yaml` | **YES** |
| Study dates (historyStart, historyStop) | `000-control.yaml` | **YES** |
| Schema mappings (RWDSchema → physical) | `000-control.yaml` | **YES** |
| `projectTables` (output tables) | `000-control.yaml` | **YES** |
| `callFunProcessDataTables` rules | `config-global.yaml` | NO |
| Source table definitions | `config-RWD.yaml` | NO |
| `inputRegex`, `insert` transforms | `config-RWD.yaml` | NO |

## Example Configuration Files

```
the_config-files/
├── 000-config-template.yaml   # Project-specific template (copy and modify)
├── config-global.yaml         # Shared - do not modify per project
├── config-RWD.yaml            # Shared - source table definitions
└── example-codes.csv          # Example CSV code file format
```

## Quick Start

### For Humans

1. Copy `000-config-template.yaml` to your project directory as `000-control.yaml`
2. Replace all `{placeholder}` values with your project-specific values
3. Create CSV code files following `example-codes.csv` format
4. Follow the notebook patterns in `lhn_project_initialization_guide.md`

### For LLM Context

Include these files in your prompt:
1. `lhn_reference.md` - For understanding classes and methods
2. `lhn_project_initialization_guide.md` - For generating project files

**Critical for LLMs**:
- **Only generate `000-control.yaml`** - never modify config-global.yaml or config-RWD.yaml
- `projectTables` define OUTPUT tables the project creates
- Source tables (conditionSource, medicationSource, etc.) are already defined in config-RWD.yaml

## Key Concepts

### Project Tables vs Source Tables

| Type | Defined In | Purpose | Examples |
|------|-----------|---------|----------|
| Source Tables | `config-RWD.yaml` | READ from source schemas | `conditionSource`, `medicationSource` |
| Project Tables | `000-control.yaml` | CREATED by this project | `cohort`, `demographics`, `ads` |

### Three-Step Extraction Workflow

```
Step 1: create_extract     → Identify codes of interest
        ↓
Step 2: entityExtract      → Extract patient records matching codes
        ↓
Step 3: write_index_table  → Create patient-level summary
```

### Code Input Methods

1. **CSV Files** (PREFERRED) - Clinician-verified codes
2. **Dictionary** - Simple regex patterns in 000-control.yaml
3. **Reference Join** - Join against existing dictionary tables

## Usage Pattern

### Standard Initialization

```python
from lhn import Resources, Extract

# Initialize - Resources reads callFunProcessDataTables from config-global.yaml
# and automatically creates all TableList/DB objects based on your schemas
resource = Resources(
    local_config='000-control.yaml',
    global_config='configuration/config-global.yaml',
    schemaTag_config='configuration/config-RWD.yaml',
    debug=True,                                    # logs config load + processing
    # finish_init=True                             # default; skips only if False
)

# Resources auto-creates based on callFunProcessDataTables rules.
# For each rule, `type_key` becomes a TableList[Item], and
# `property_name` becomes a dict of config-objects (not tables).
#
#   resource.r         → TableList[Item] — RWD source tables (.df is DF)
#   resource.dictrwd   → TableList[Item] — dictionary tables
#   resource.d         → dict of ConfigObj (NOT tables — use dictrwd)
#   resource.e         → Extract container — ExtractItems from projectTables

e = resource.e
r = resource.r
d = resource.dictrwd        # dictionary tables — USE AS elementListSource

# Three-step extraction (match 053/054/056/057/058/064/067 pattern)
e.raw_codes.dict2pyspark()                         # build codes from YAML
e.codes.create_extract(
    elementList=e.raw_codes,
    elementListSource=d.condition_conditioncode.df,  # DICTIONARY, not r.*
    find_method='regex',
    sourceField='conditioncode_standard_id',
)
e.encounters.entityExtract(e.codes, r.conditionSource.df)
e.index.write_index_table(inTable=e.encounters)

# Verify at each step
e.codes.attrition()
e.encounters.attrition()
e.index.attrition()
```

### How callFunProcessDataTables Works

In `config-global.yaml`, each rule maps a schema type to attribute names:

```yaml
callFunProcessDataTables:
  RWDcallFunc:
    data_type: 'RWDTables'       # Table definitions key (in config-RWD.yaml)
    schema_type: 'RWDSchema'     # Schema key (in 000-control.yaml)
    type_key: 'r'                # → resource.r (TableList[Item])
    property_name: 'rwd'         # → resource.rwd (dict of ConfigObj)

  dictrwdcallFunc:
    data_type: null              # Auto-discovered from schema
    schema_type: 'dictrwdSchema'
    type_key: 'dictrwd'          # → resource.dictrwd (TableList[Item])
    property_name: 'd'           # → resource.d (dict of ConfigObj)
    updateDict: true             # Auto-discover tables via schema scan
```

**Important asymmetry:** `type_key` binds a `TableList[Item]` (with
`.df`-bearing members); `property_name` binds a dict of `ConfigObj`
(no `.df`). `load_into_local()` rebinds the `property_name` short
alias (`d`) onto the `TableList` so `d.<table>.df` works in notebook
cells. Without that remap, `resource.d.<table>.df` raises
`AttributeError`.

When `Resources(...)` runs with `finish_init=True` (the default), it:
1. Reads `callFunProcessDataTables` from config-global.yaml
2. For each rule where the schema exists, calls `processDataTables` internally
3. Creates attributes on the Resource object (e.g., `resource.rwd`, `resource.r`, `resource.dictrwd`)
4. Builds `resource.e` as the `Extract` container over `projectTables`

**You don't call `processDataTables` directly** — it's automated by the config.

### The Extract Pattern (Method Enrichment)

`resource.e` is already an `Extract` instance after Resources init. Each
entry in `projectTables:` becomes an `ExtractItem` on it:

```python
resource = Resources(
    local_config='000-control.yaml',
    global_config='configuration/config-global.yaml',
    schemaTag_config='configuration/config-RWD.yaml',
)
e = resource.e   # Extract, auto-created by finish_init (default)

# ExtractItem methods:
e.death.showIU(obs=5)           # Display sample (tenant-filtered)
e.death.attrition()              # Record/person counts
e.death.location                 # Table path
e.death.df                       # Spark DataFrame
e.death.write()                  # Write to Spark table
e.death.create_extract(...)      # Find codes via regex/merge
e.death.entityExtract(...)       # Extract patient records
e.death.write_index_table(...)   # Create patient-level index
e.death.properties()             # Introspect YAML-sourced attributes
```

**Key Methods on ExtractItem:**

| Method | Purpose |
|--------|---------|
| `showIU(obs=6, tenant=127)` | Display sample records (filtered by tenant) |
| `attrition(person_id='personid')` | Show record/unique person counts |
| `write(outTable=None)` | Write DataFrame to Spark table |
| `create_extract(elementList, find_method)` | Find codes via regex or merge |
| `entityExtract(elementList, entitySource)` | Extract matching records |
| `write_index_table(inTable)` | Create first/last record index |
| `load_csv_as_df()` | Load CSV codes file |
| `toPandas(limit=None)` | Convert to pandas |

### Notebook Setup Pattern

For clean notebook code, use this setup cell with `load_into_local`:

```python
#| include: false
from lhn import Resources

resource = Resources(
    local_config='000-control.yaml',
    global_config='configuration/config-global.yaml',
    schemaTag_config='configuration/config-RWD.yaml',
    debug=True,                                    # logs config load + processing
    # finish_init=True                             # default; skips only if False
)

# load_into_local surfaces r, e, dictrwd/d, and schema name strings
# into this cell's locals namespace. Signature:
# load_into_local(everything=False, load_schemas=True)
# Pass everything=True for also disease, schemaTag, dataLoc, etc.
locals().update(resource.load_into_local())
# Now available: r, e, d (remapped to dictrwd), RWDSchema, projectSchema, etc.
```

Now you can write clean analysis cells:

```python
# Source-table access via r (TableList[Item]); .df is the DataFrame
r.conditionSource.df.filter(F.col('code').like('E11%')).count()

# Dictionary-table access via d (remapped from property_name 'd' onto
# the TableList resource.dictrwd by load_into_local()):
d.condition_conditioncode.df.count()

# ExtractItem methods via e (Extract)
e.cohort.showIU(obs=10)
e.cohort.attrition()
e.cohort.location
```

### What Gets Created

After `Resources` initialization with typical config:

| Attribute | Type | Built from | Access pattern |
|-----------|------|------------|----------------|
| `resource.r` | `TableList[Item]` | RWDTables + RWDSchema (type_key='r') | `r.conditionSource.df.count()` |
| `resource.rwd` | dict of ConfigObj | property_name variant — config metadata, NOT tables | rarely used directly |
| `resource.dictrwd` | `TableList[Item]` | Auto-discovered from dictrwdSchema (type_key='dictrwd') | `d.condition_conditioncode.df` (after `load_into_local()` remaps `d`) |
| `resource.d` | dict of ConfigObj | property_name for dictrwd — NOT tables | rarely used directly; use `d` only after `load_into_local()` |
| `resource.proj` | dict of ConfigObj | projectTables entries | internal — consumed to build `self.e` |
| `resource.e` | `Extract` | `Extract(resource.proj)` during finish_init | `e.cohort.showIU()` — the main user-facing API |

## Workflow for LLM Project Generation

1. **User describes study**: "I want to study diabetes patients..."

2. **LLM generates ONLY**:
   - `000-control.yaml` with `projectTables` definitions
   - CSV code files (if needed)
   - Jupyter notebooks following extraction patterns

3. **LLM does NOT generate**:
   - `config-global.yaml` (shared infrastructure)
   - `config-RWD.yaml` (shared infrastructure)
   - Source table definitions (already in config-RWD.yaml)

4. **User runs notebooks** in order:
   - 010-Cohort-Identification
   - 020-Demographics
   - 030-Condition-Extraction
   - etc.

## Dependencies

- `spark-config-mapper >= 0.1.0`
- `pyspark >= 3.0.0`
- `pyyaml >= 5.0`
- `pandas >= 1.0.0`

## Related Packages

- **spark_config_mapper**: Configuration management (dependency)
- **txtarchive**: Archive format for sharing code with LLMs
