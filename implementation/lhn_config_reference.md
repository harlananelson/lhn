# LHN Package Configuration System: Reference Document

> **Purpose**: Machine-readable reference for LLM context injection. Provides precise definitions, resolution rules, and error states for the lhn package configuration system.

---

## 1. GLOSSARY OF TERMS

| Term | Type | Definition | Example |
|------|------|------------|---------|
| `Resources` | Class | Orchestrator class that loads configuration, resolves schemas, and creates data access objects | `resource = Resources(project='SickleCell', spark=spark, process_all=True)` |
| `schema_type` | Config Key | Key in `000-config.yaml` → `schemas` dict that maps to a Spark catalog schema name | `RWDSchema` → `iuhealth_ed_data_cohort_202306` |
| `data_type` | Config Key | Key that resolves to a table definition dictionary (from structure config like `config-RWD.yaml`) | `RWDTables` → dict of table definitions |
| `type_key` | String | Short identifier assigned as attribute on Resources; holds `TableList` of `Item` objects | `rwd`, `proj`, `scd` |
| `property_name` | String | Short identifier assigned as attribute on Resources; holds `DB` wrapper for DataFrame access | `r`, `db`, `d` |
| `Item` | Class | Represents a single table with metadata, DataFrame reference, and file paths | `resource.rwd.conditionSource` |
| `TableList` | Class | Dictionary-like container of `Item` objects with attribute access | `resource.rwd` (access via `resource.rwd.tablename`) |
| `DB` | Class | Wrapper providing direct DataFrame access from a `TableList` | `resource.r.conditionSource` returns DataFrame |
| `Extract` | Class | Container of `ExtractItem` objects for project output tables | `e = Extract(resource.proj)` |
| `ExtractItem` | Class | Extends `Item` with methods for data extraction workflows | `e.cohort.create_extract(...)` |
| `SharedMethodsMixin` | Mixin | Provides common methods: `attrition()`, `tabulate()`, `to_csv()`, `to_parquet()` | Inherited by `Item`, `ExtractItem`, `DB` |
| `recursive_template` | Function | Performs `${variable}` substitution in YAML values during load | `${today}` → `2025-01-19` |

---

## 2. CONFIGURATION FILE HIERARCHY

```
RESOLUTION ORDER (loaded sequentially, later overrides earlier):

1. config_local   : {basePath}/Projects/{project}/000-config.yaml
2. config_global  : {basePath}/configuration/config-global.yaml
3. Structure configs (referenced in config_global.config_table_locations):
   - config_RWD   : configuration/config-RWD.yaml
   - config_IUH   : configuration/config-IUH.yaml
   - config_OMOP  : configuration/config-OMOP.yaml
   - etc.
```

### 2.1 File Purposes

| File | Scope | Contains |
|------|-------|----------|
| `000-config.yaml` | Project-specific | `schemas` dict, `projectTables` dict, project variables (`disease`, `schemaTag`, dates) |
| `config-global.yaml` | Cross-project | `callFunProcessDataTables` mapping, `config_table_locations`, global defaults |
| `config-RWD.yaml` | Schema structure | `RWDTables` dict with table definitions (`source`, `inputRegex`, `insert`, `colsRename`) |

### 2.2 Template Substitution

All string values undergo `${variable}` substitution via `recursive_template()`:

```yaml
# In 000-config.yaml
stopDate: ${today}           # Resolved at load time
dataLoc: "${dataPath}/inst/extdata/${project}/"

# Built-in variables:
# - ${today}    : Current date (YYYY-MM-DD)
# - ${dataPath} : basePath from Resources.__init__
# - Any key defined earlier in the same or parent config
```

---

## 3. KEY DATA STRUCTURES

### 3.1 `schemas` Dictionary (in 000-config.yaml)

Maps logical schema names to physical Spark catalog schema names:

```yaml
schemas:
  RWDSchema: iuhealth_ed_data_cohort_202306    # Logical → Physical
  projectSchema: sicklecell_rerun
  omopSchema: real_world_data_ed_omop_may_2024
```

### 3.2 `callFunProcessDataTables` Dictionary (in config-global.yaml)

Maps schema types to processing parameters:

```yaml
callFunProcessDataTables:
  RWDcallFunc:
    data_type        : 'RWDTables'      # → Key for table definitions dict
    schema_type      : 'RWDSchema'      # → Key in schemas dict
    type_key         : 'rwd'            # → Attribute name for TableList
    property_name    : 'r'              # → Attribute name for DB wrapper
    updateDict       : False            # → Whether to auto-discover tables
    tableNameTemplate: null             # → Regex for table name normalization
```

### 3.3 Table Definition (in config-RWD.yaml)

```yaml
RWDTables:
  conditionSource:
    source: condition                    # Spark table name in schema
    datefield: datetimeCondition         # Primary date field
    inputRegex:                          # Column selection patterns
      - ^personid
      - ^conditionCode_standard_
    insert:                              # PySpark transformations (executed in order)
      - "withColumn('datetimeCondition', F.coalesce(...))"
    colsRename:                          # Column renaming map
      old_name: new_name
```

---

## 4. RESOLUTION CHAIN

### 4.1 Schema Resolution

```
User provides: schema_type = 'RWDSchema'
                    ↓
Lookup: config_dict['schemas']['RWDSchema']
                    ↓
Returns: 'iuhealth_ed_data_cohort_202306' (Spark catalog schema)
                    ↓
Validated: database_exists('iuhealth_ed_data_cohort_202306')
```

### 4.2 Table Definition Resolution

```
User provides: data_type = 'RWDTables'
                    ↓
Lookup: config_dict['RWDTables']
                    ↓
Returns: dict of table definitions from config-RWD.yaml
                    ↓
Each table becomes: Item(TBL, dataTables, TBLLoc, schema, ...)
```

### 4.3 Complete Processing Flow

```
schemas['RWDSchema'] = 'iuhealth_ed_data_cohort_202306'
         ↓
callFunProcessDataTables['RWDcallFunc'] matched by schema_type='RWDSchema'
         ↓
data_type='RWDTables' → config_dict['RWDTables'] → table definitions
         ↓
processDataTables() creates TableList of Item objects
         ↓
setattr(self, 'rwd', TableList)  # type_key
setattr(self, 'r', DB(self.__dict__, 'rwd'))  # property_name
```

---

## 5. CLASS RELATIONSHIPS

```
Resources (orchestrator)
│
├── Config Loading:
│   ├── self.config_dict        : Merged configuration dictionary
│   ├── self.config_table_locations : Paths to config files
│   └── self.replace            : Template substitution variables
│
├── Per-Schema Attributes (created by processDataTables):
│   ├── self.{type_key}         : TableList[Item]  (e.g., self.rwd)
│   └── self.{property_name}    : DB               (e.g., self.r)
│
└── Project Output:
    └── self.load_into_local()  : Returns dict with Extract object

TableList
├── Contains: dict of Item objects
├── Access: tableList.tablename or tableList['tablename']
└── Iteration: tableList.items()

Item (inherits SharedMethodsMixin)
├── self.df        : Spark DataFrame
├── self.name      : Table identifier
├── self.source    : Spark table name
├── self.location  : schema.source
├── self.csv       : CSV file path
├── self.parquet   : Parquet file path
├── self.existsDF  : Boolean - table exists in schema
└── Methods: attrition(), tabulate(), to_csv(), to_parquet()

DB (inherits SharedMethodsMixin)
├── Direct DataFrame access: db.tablename returns DataFrame
└── Created from: DB(resources.__dict__, type_key)

Extract
├── Contains: dict of ExtractItem objects
└── Created from: Extract(resource.proj)

ExtractItem (inherits SharedMethodsMixin)
├── Inherits all Item attributes
├── Additional methods: create_extract(), entityExtract(), load_csv_as_df()
└── Workflow: create_extract → entityExtract → write_index_table
```

---

## 6. INITIALIZATION SEQUENCE

```python
# Step 1: Instantiate (config NOT loaded by default)
resource = Resources(
    project='SickleCell',
    spark=spark,
    basePath=Path.home()/'work/Users/hnelson3',
    config_file='000-config.yaml',
    process_all=False  # DEFAULT: deferred loading
)

# Step 2a: Explicit full initialization
resource = Resources(..., process_all=True)
# OR
# Step 2b: Manual initialization
resource.finish_init()

# What finish_init() does:
#   1. read_config_all()       → Load and merge all config files
#   2. processAllDataTables()  → Create TableList/DB for each schema
#   3. Sets property_names_processed

# Step 3: Get local namespace variables
locals_dict = resource.load_into_local(schemakey='projectSchema', extractName='e')
# Returns: {'e': Extract, 'rwd': TableList, 'r': DB, ...}
```

---

## 7. METHOD REFERENCE

### 7.1 Resources Methods

| Method | Purpose | Returns |
|--------|---------|---------|
| `finish_init()` | Complete deferred initialization | None |
| `read_config_all()` | Load and merge all config files | None |
| `processAllDataTables()` | Process all schemas in `schemas` dict | None |
| `processDataTableBySchemakey(schemakey)` | Process single schema | None |
| `load_into_local(everything, schemakey, extractName)` | Export to local namespace | dict |
| `find_callFunProcessDataTables(schemakey, callFunDict)` | Find matching callFunc entry | list[dict] |

### 7.2 SharedMethodsMixin Methods

| Method | Purpose | Available On |
|--------|---------|--------------|
| `attrition()` | Print patient counts and date ranges | Item, ExtractItem, DB |
| `tabulate(by, index, ...)` | Aggregate and display counts | Item, ExtractItem, DB |
| `to_csv()` | Write DataFrame to CSV | Item, ExtractItem |
| `to_parquet(path, ...)` | Write DataFrame to Parquet | Item, ExtractItem |

---

## 8. ERROR STATES AND CAUSES

| Symptom | Cause | Resolution |
|---------|-------|------------|
| Schema silently skipped | `database_exists()` returns False | Verify schema name in Spark catalog |
| `KeyError: 'RWDTables'` | `data_type` not found in config_dict | Ensure config-RWD.yaml loaded via `config_table_locations` |
| `AttributeError: 'Resources' has no attribute 'rwd'` | `process_all=False` and `finish_init()` not called | Set `process_all=True` or call `finish_init()` |
| No matching callFunProcessDataTables | `schema_type` in callFunc doesn't match any key in `schemas` | Verify schema_type value matches a key in `000-config.yaml` → `schemas` |
| Empty DataFrame on Item | Table exists but `inputRegex` matched no columns | Check regex patterns against actual column names |
| Template variable not substituted | Variable not defined before use | Define variables earlier in config or add to `replace` dict |

---

## 9. ACCESS PATTERNS

### 9.1 DataFrame Access

```python
# Via DB wrapper (returns DataFrame directly)
df = resource.r.conditionSource

# Via TableList → Item (returns Item, access .df)
item = resource.rwd.conditionSource
df = item.df

# Via Extract → ExtractItem
e = Extract(resource.proj)
df = e.cohort.df
```

### 9.2 Metadata Access

```python
# Table location
location = resource.rwd.conditionSource.location  # 'schema.tablename'

# Check existence
exists = resource.rwd.conditionSource.existsDF  # Boolean

# File paths
csv_path = resource.rwd.conditionSource.csv
parquet_path = resource.rwd.conditionSource.parquet
```

### 9.3 Configuration Access

```python
# Direct config access
schemas = resource.config_dict['schemas']
tables = resource.config_dict['RWDTables']

# Processed property names
processed = resource.property_names_processed  # {'rwd', 'r', 'proj', 'db', ...}
```

---

## 10. CRITICAL INVARIANTS

1. **Schema existence is validated before processing**: `database_exists()` must return True or schema is skipped silently.

2. **Template substitution is order-dependent**: Variables must be defined before they are referenced.

3. **`data_type` can be null**: When `updateDict=True`, tables are auto-discovered from schema rather than read from config.

4. **`type_key` and `property_name` are both set**: Every processed schema creates both a TableList (type_key) and DB wrapper (property_name).

5. **Item.df may be empty**: If table doesn't exist or inputRegex matches nothing, `df` is an empty DataFrame, not None.

6. **Config files are path-resolved**: Relative paths in `config_table_locations` are joined with `basePath`.

7. **Convention over configuration for file paths**: The `csv` and `parquet` properties are auto-generated if not specified:
   - CSV pattern: `{dataLoc}{table_key}_{disease}_{schemaTag}.csv`
   - Parquet pattern: `{parquetLoc}{table_key}_{disease}_{schemaTag}.parquet`
   - Only specify these properties when overriding the default convention.
