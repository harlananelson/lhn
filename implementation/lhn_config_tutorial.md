# LHN Package Configuration System: Tutorial

> **Purpose**: Step-by-step guide for human developers. Explains the configuration system through concrete examples and "what happens when" traces.

---

## Introduction: What Problem Does This Solve?

The lhn package needs to work with multiple Spark schemas (databases) that share similar structures but contain different data. For example:
- `iuhealth_ed_data_cohort_202306` - ED cohort data
- `real_world_data_ed_omop_may_2024` - OMOP-formatted data
- `sicklecell_rerun` - Project output tables

Rather than hardcoding these connections, the configuration system provides:
1. **Dynamic schema mapping** - Switch between schemas by changing config, not code
2. **Consistent access patterns** - `resource.r.encounter` works regardless of which schema backs it
3. **Table metadata** - Column filtering, renaming, and transformations defined declaratively

---

## Part 1: The Configuration Files

### 1.1 Project Config: `000-config.yaml`

This file lives in your project directory and defines project-specific settings:

```yaml
# /home/user/work/Users/hnelson3/Projects/SickleCell/000-config.yaml

project: SickleCell
disease: SCD
schemaTag: RWD

# THE KEY MAPPING: Logical names → Actual Spark schema names
schemas:
  RWDSchema: iuhealth_ed_data_cohort_202306    # Source data
  projectSchema: sicklecell_rerun              # Where we write results
  omopSchema: real_world_data_ed_omop_may_2024

# Tables this project will CREATE (in projectSchema)
projectTables:
  cohort:
    label: "SCD Patient Cohort"
    indexFields: ["personid", "tenant"]
  demographics:
    label: "Patient Demographics"
    indexFields: ["personid"]
```

**Key insight**: The `schemas` dictionary maps *logical names* (like `RWDSchema`) to *physical Spark schemas* (like `iuhealth_ed_data_cohort_202306`). Your code uses the logical names; the config determines where they point.

### 1.2 Global Config: `config-global.yaml`

This file defines how to process each schema type:

```yaml
# configuration/config-global.yaml

# This is THE BRIDGE between logical schema names and processing rules
callFunProcessDataTables:
  
  RWDcallFunc:
    data_type     : 'RWDTables'      # Which table definitions to use
    schema_type   : 'RWDSchema'      # Which logical schema name to look up
    type_key      : 'rwd'            # resource.rwd will hold the TableList
    property_name : 'r'              # resource.r will hold the DB wrapper
    updateDict    : False
    
  projectcallFunc:
    data_type     : 'projectTables'  # Table definitions in 000-config.yaml
    schema_type   : 'projectSchema'  # Points to sicklecell_rerun
    type_key      : 'proj'           # resource.proj
    property_name : 'db'             # resource.db
    updateDict    : False

# Where to find structure definition files
config_table_locations:
  'config_RWD':
    'location': 'configuration/config-RWD.yaml'
```

### 1.3 Structure Config: `config-RWD.yaml`

This file defines what tables exist in RWD-structured schemas and how to process them:

```yaml
# configuration/config-RWD.yaml

RWDTables:
  conditionSource:
    source: condition                 # Actual Spark table name
    datefield: datetimeCondition
    inputRegex:                       # Only keep columns matching these patterns
      - ^personid
      - ^encounterId
      - ^conditionCode_standard_
      - ^effectiveDate
    insert:                           # PySpark transformations to apply
      - "withColumn('datetimeCondition', F.coalesce(F.to_timestamp(F.col('effectiveDate'))))"
    
  encounterSource:
    source: encounter
    inputRegex:
      - ^personid
      - ^encounterId
      - ^admitDate
      - ^dischargeDate
```

---

## Part 2: What Happens When You Initialize

### 2.1 The Initialization Call

```python
from lhn.resource import Resources
from pathlib import Path

resource = Resources(
    project='SickleCell',
    spark=spark,
    basePath=Path.home()/'work/Users/hnelson3',
    config_file='000-config.yaml',
    process_all=True  # ← This triggers full initialization
)
```

### 2.2 Step-by-Step Trace

**Step 1: Basic Setup** (in `__init__`)
```
- Set project = 'SickleCell'
- Set basePath = /home/hnelson3/work/Users/hnelson3
- Initialize config_table_locations with:
    config_local  → basePath/Projects/SickleCell/000-config.yaml
    config_global → basePath/configuration/config-global.yaml
- Set replace dict = {'today': '2025-01-19', 'dataPath': basePath}
```

**Step 2: Load Configuration** (in `read_config_all`)
```
1. Load 000-config.yaml
   - Perform ${variable} substitution
   - Merge into config_dict
   
2. Load config-global.yaml
   - Discover config_table_locations entries
   - Merge into config_dict
   
3. Load each referenced config (config-RWD.yaml, etc.)
   - config_dict now contains:
     - schemas: {RWDSchema: 'iuhealth_ed_data_cohort_202306', ...}
     - RWDTables: {conditionSource: {...}, encounterSource: {...}}
     - callFunProcessDataTables: {RWDcallFunc: {...}, ...}
     - projectTables: {cohort: {...}, demographics: {...}}
```

**Step 3: Process Schemas** (in `processAllDataTables`)
```
For each entry in schemas:

  Processing RWDSchema → iuhealth_ed_data_cohort_202306:
    1. Find matching callFunc where schema_type == 'RWDSchema'
       → Found: RWDcallFunc
    2. Check: database_exists('iuhealth_ed_data_cohort_202306')?
       → Yes, proceed
    3. Get table definitions: config_dict['RWDTables']
    4. Create TableList of Item objects
    5. Assign: resource.rwd = TableList
    6. Assign: resource.r = DB(resource.__dict__, 'rwd')
    
  Processing projectSchema → sicklecell_rerun:
    1. Find matching callFunc where schema_type == 'projectSchema'
       → Found: projectcallFunc
    2. Check: database_exists('sicklecell_rerun')?
       → Yes, proceed
    3. Get table definitions: config_dict['projectTables']
    4. Create TableList of Item objects
    5. Assign: resource.proj = TableList
    6. Assign: resource.db = DB(resource.__dict__, 'proj')
```

### 2.3 The Result

After initialization, `resource` has these attributes:

```python
resource.rwd   # TableList: access tables as resource.rwd.conditionSource
resource.r     # DB wrapper: resource.r.conditionSource returns DataFrame
resource.proj  # TableList for project output tables
resource.db    # DB wrapper for project tables

# You can also access config directly:
resource.config_dict['schemas']
resource.config_dict['RWDTables']
```

---

## Part 3: Accessing Data

### 3.1 Three Ways to Access a Table

```python
# Method 1: Via DB wrapper (returns DataFrame directly)
conditions_df = resource.r.conditionSource
# This is the most common pattern for analysis

# Method 2: Via TableList → Item (access metadata)
condition_item = resource.rwd.conditionSource
conditions_df = condition_item.df
print(condition_item.location)   # 'iuhealth_ed_data_cohort_202306.condition'
print(condition_item.existsDF)   # True

# Method 3: Via Extract (for project output tables)
e = Extract(resource.proj)
cohort_df = e.cohort.df
```

### 3.2 When to Use Each Method

| Use Case | Method | Example |
|----------|--------|---------|
| Quick analysis | DB wrapper | `resource.r.conditionSource.show()` |
| Check if table exists | Item | `resource.rwd.conditionSource.existsDF` |
| Get file paths | Item | `resource.rwd.conditionSource.csv` |
| Create/modify project tables | Extract | `e.cohort.create_extract(...)` |
| Attrition reports | Any | `resource.rwd.conditionSource.attrition()` |

### 3.3 Common Operations

```python
# Get patient counts
resource.rwd.conditionSource.attrition()
# Output: Table conditionSource: 1,234,567 rows, 45,678 unique patients

# Tabulate by a field
resource.rwd.conditionSource.tabulate(
    by='conditionCode_standard_id',
    index=['personid'],
    obs=20
)
# Output: Top 20 condition codes by patient count

# Export to CSV
resource.rwd.conditionSource.to_csv()
# Writes to the csv path defined in config

# Export to Parquet
resource.rwd.conditionSource.to_parquet()
# Writes to HDFS path defined in config
```

---

## Part 4: Creating Project Output Tables

### 4.1 The Extract Workflow

Project tables are defined in `000-config.yaml` under `projectTables`. They don't exist initially—you create them using the Extract workflow:

```python
# Get Extract object for project tables
e = Extract(resource.proj)

# Step 1: Identify codes of interest
e.target_conditions.create_extract(
    elementList=resource.d.condition_codes,    # Dictionary/reference table
    elementListSource=resource.d.condition_codes.df,
    find_method='regex'
)

# Step 2: Extract matching records from source
e.condition_events.entityExtract(
    elementList=e.target_conditions,
    entitySource=resource.r.conditionSource,
    cacheResult=True
)

# Step 3: Create index table with first occurrence
e.condition_index.write_index_table(
    inTable=e.condition_events
)

# Check results at each step
e.target_conditions.attrition()
e.condition_events.attrition()
e.condition_index.attrition()
```

### 4.2 Loading Results into Local Namespace

```python
# Get everything needed for analysis
local_vars = resource.load_into_local(
    schemakey='projectSchema',
    extractName='e',
    everything=False
)

# Now local_vars contains:
# - 'e': Extract object
# - 'rwd': TableList
# - 'r': DB wrapper
# - 'proj': TableList
# - 'db': DB wrapper

# Inject into notebook namespace
locals().update(local_vars)

# Now you can use directly:
e.cohort.tabulate(by='gender')
r.conditionSource.show()
```

---

## Part 5: Troubleshooting

### 5.1 "Schema not found" (silent skip)

**Symptom**: Expected attribute missing from resource
```python
>>> resource.rwd
AttributeError: 'Resources' object has no attribute 'rwd'
```

**Cause**: The Spark schema doesn't exist
**Fix**: Verify the schema exists in Spark catalog
```python
spark.sql("SHOW DATABASES").show()
# Ensure 'iuhealth_ed_data_cohort_202306' is listed
```

### 5.2 "KeyError: 'RWDTables'"

**Symptom**: Error during initialization
```python
KeyError: 'RWDTables'
```

**Cause**: `config-RWD.yaml` wasn't loaded
**Fix**: Check `config_table_locations` in `config-global.yaml`:
```yaml
config_table_locations:
  'config_RWD':
    'location': 'configuration/config-RWD.yaml'  # Must exist at this path
```

### 5.3 "No matching callFunProcessDataTables"

**Symptom**: Log shows "Not Found: schema_type... never matches"
```
Not Found: schema_type of callFunProcessDataTables never matches MyNewSchema
```

**Cause**: You added a schema to `000-config.yaml` but didn't add a corresponding entry to `callFunProcessDataTables`

**Fix**: Add entry to `config-global.yaml`:
```yaml
callFunProcessDataTables:
  myNewcallFunc:
    data_type     : 'MyNewTables'
    schema_type   : 'MyNewSchema'  # Must match the key in schemas dict
    type_key      : 'mynew'
    property_name : 'mn'
```

### 5.4 Empty DataFrame

**Symptom**: `resource.rwd.tablename.df` is empty but table has data

**Cause 1**: Table doesn't exist in schema
```python
>>> resource.rwd.tablename.existsDF
False
```

**Cause 2**: `inputRegex` matched no columns
```python
# Check what columns the regex is trying to match
print(resource.config_dict['RWDTables']['tablename']['inputRegex'])
# Compare against actual columns
spark.table('schema.tablename').columns
```

### 5.5 Template Variable Not Substituted

**Symptom**: Config value still contains `${variable}`

**Cause**: Variable referenced before it was defined

**Fix**: Define variables earlier in config, or add to `replace` dict:
```python
resource = Resources(
    ...,
    myVar='custom_value'  # Added via **kwargs → replace dict
)
```

---

## Part 6: Quick Reference

### Configuration File Locations

| File | Location | Contains |
|------|----------|----------|
| `000-config.yaml` | `{basePath}/Projects/{project}/` | schemas, projectTables, project variables |
| `config-global.yaml` | `{basePath}/configuration/` | callFunProcessDataTables, global defaults |
| `config-RWD.yaml` | `{basePath}/configuration/` | RWDTables structure definitions |

### Key Attributes After Initialization

| Attribute | Type | Purpose |
|-----------|------|---------|
| `resource.rwd` | TableList | RWD schema tables (Item objects) |
| `resource.r` | DB | RWD tables (DataFrame access) |
| `resource.proj` | TableList | Project output tables (Item objects) |
| `resource.db` | DB | Project tables (DataFrame access) |
| `resource.config_dict` | dict | Merged configuration |

### Common Method Calls

```python
# Initialization
resource = Resources(project='X', spark=spark, process_all=True)

# Data access
df = resource.r.tablename              # Get DataFrame
item = resource.rwd.tablename          # Get Item with metadata

# Analysis
item.attrition()                       # Patient counts
item.tabulate(by='field')              # Aggregate counts

# Export
item.to_csv()                          # Write to CSV
item.to_parquet()                      # Write to Parquet

# Project workflow
e = Extract(resource.proj)
e.table.create_extract(...)
e.table.entityExtract(...)
e.table.write_index_table(...)
```
