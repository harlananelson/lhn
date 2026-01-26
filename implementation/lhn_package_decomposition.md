# LHN Package Decomposition Analysis

## Current State: Monolithic Module

The `lhn` package contains 37 modules that evolved from a single utility module. Analysis of import dependencies reveals **three natural clusters** that could become separate packages.

---

## Identified Package Boundaries

### Dependency Graph (Simplified)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           LAYER 3: ORCHESTRATION                            │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │  resource   │  │   extract   │  │     db      │  │    item     │        │
│  │  Resources  │  │   Extract   │  │     DB      │  │    Item     │        │
│  │             │  │ ExtractItem │  │             │  │  TableList  │        │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘        │
│         │                │                │                │               │
│         └────────────────┴────────────────┴────────────────┘               │
│                                   │                                         │
│                    ┌──────────────┴──────────────┐                         │
│                    │      shared_methods         │                         │
│                    │    SharedMethodsMixin       │                         │
│                    └──────────────┬──────────────┘                         │
└──────────────────────────────────┬──────────────────────────────────────────┘
                                   │
┌──────────────────────────────────┴──────────────────────────────────────────┐
│                        LAYER 2: CONFIGURATION                               │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌─────────────────────┐ │
│  │ data_transformation │  │  metadata_functions │  │  function_parameters│ │
│  │   read_config()     │  │  processDataTables  │  │  setFunctionParams  │ │
│  │ recursive_template  │  │  update_dictionary  │  │                     │ │
│  └─────────┬───────────┘  └─────────┬───────────┘  └─────────┬───────────┘ │
│            │                        │                        │              │
│            └────────────────────────┴────────────────────────┘              │
│                                     │                                       │
└─────────────────────────────────────┬───────────────────────────────────────┘
                                      │
┌─────────────────────────────────────┴───────────────────────────────────────┐
│                          LAYER 1: SPARK UTILITIES                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │ spark_utils │  │ spark_query │  │    query    │  │database_ops │        │
│  │ writeTable  │  │ pivot_wider │  │ query_table │  │ set_database│        │
│  │flattenTable │  │joinByIndex  │  │extract_flat │  │  drop_table │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
│                                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │ list_ops    │  │introspect_  │  │ pandas_utils│  │ file_ops    │        │
│  │noColColide  │  │  utils      │  │ dict2Pandas │  │ put_to_hdfs │        │
│  │find_single  │  │ coalesce    │  │             │  │             │        │
│  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
┌─────────────────────────────────────┴───────────────────────────────────────┐
│                           LAYER 0: HEADER                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │                            header.py                                 │   │
│  │  - All external imports (pyspark, pandas, yaml, etc.)               │   │
│  │  - Logger configuration                                              │   │
│  │  - Global spark session reference                                    │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Proposed Package Structure

### Option A: Three Packages (Recommended)

```
1. spark-config-mapper (Layer 1-2)
   - Configuration loading and template substitution
   - Dynamic schema mapping
   - No domain-specific logic
   
2. spark-data-utils (Layer 1)
   - Generic Spark DataFrame utilities
   - Write/read operations
   - Schema manipulation
   
3. lhn (Layer 3 + domain-specific)
   - Healthcare data extraction workflows
   - Resources, Extract, Item, DB classes
   - Domain-specific operations (cohort, discern, features)
```

### Option B: Two Packages (Simpler)

```
1. spark-config-mapper (Layer 1-2)
   - Everything needed to map configs to Spark schemas
   
2. lhn (Layer 3 + remaining)
   - Healthcare-specific workflows
   - Depends on spark-config-mapper
```

---

## Package 1: `spark-config-mapper`

### Purpose
Generic configuration management for Spark projects. No healthcare-specific code.

### Modules to Include

```
spark_config_mapper/
├── __init__.py
├── config/
│   ├── __init__.py
│   ├── loader.py          # read_config, recursive_template (from data_transformation)
│   ├── template.py        # Template substitution logic
│   └── validator.py       # Config validation
├── schema/
│   ├── __init__.py
│   ├── mapper.py          # processDataTables, update_dictionary (from metadata_functions)
│   ├── discovery.py       # database_exists, getTableList (from spark_utils)
│   └── introspection.py   # extractTableLocations, coalesce (from introspection_utils)
├── utils/
│   ├── __init__.py
│   ├── spark_ops.py       # writeTable, flattenTable, etc. (from spark_utils)
│   ├── list_ops.py        # find_single_level_items, noColColide (from list_operations)
│   └── parameters.py      # setFunctionParameters (from function_parameters)
└── header.py              # Imports and logger setup
```

### Key Classes/Functions to Extract

```python
# From data_transformation.py
- read_config()
- recursive_template()

# From metadata_functions.py  
- processDataTables()
- update_dictionary()

# From spark_utils.py
- database_exists()
- getTableList()
- writeTable()
- flattenTable()
- assignPropertyFromDictionary()

# From introspection_utils.py
- extractTableLocations()
- coalesce()
- translate_index()

# From list_operations.py
- find_single_level_items()
- noColColide()

# From function_parameters.py
- setFunctionParameters()
```

### Dependencies
- pyspark
- pyyaml
- pandas (minimal)

### Example Usage After Split

```python
from spark_config_mapper import ConfigLoader, SchemaMapper

# Load configuration with template substitution
config = ConfigLoader(
    local_config='000-config.yaml',
    global_config='config-global.yaml',
    replace={'today': '2025-01-19'}
)

# Map schemas to Spark tables
mapper = SchemaMapper(spark, config)
tables = mapper.process_schema('RWDSchema', 'RWDTables')
```

---

## Package 2: `lhn` (Refactored)

### Purpose
Healthcare data extraction and analysis workflows. Depends on `spark-config-mapper`.

### Modules to Keep

```
lhn/
├── __init__.py
├── core/
│   ├── __init__.py
│   ├── resource.py        # Resources class (simplified)
│   ├── extract.py         # Extract, ExtractItem
│   ├── item.py            # Item, TableList
│   ├── db.py              # DB class
│   └── shared_methods.py  # SharedMethodsMixin
├── cohort/
│   ├── __init__.py
│   ├── identification.py  # write_index_table, identify_target_records
│   ├── matching.py        # match_controls_to_cases
│   └── features.py        # select_only_baseline, analyze_clinical_measurements
├── ontology/
│   ├── __init__.py
│   └── discern.py         # All ontology/coding system operations
├── analysis/
│   ├── __init__.py
│   ├── summary.py         # attrition, groupCount, etc. (from data_summary)
│   ├── statistics.py      # chi_squared, percentiles (from statisticalSummary)
│   └── plot.py            # Visualization functions
├── io/
│   ├── __init__.py
│   ├── display.py         # print_pd, showIU (from data_display)
│   └── excel.py           # Excel operations
└── meta/
    ├── __init__.py
    ├── metaTable.py       # metaSchema, metaTable
    └── listTable.py       # ListTable
```

### Dependencies
- spark-config-mapper (new dependency)
- pyspark
- pandas
- matplotlib (optional, for plotting)

---

## Migration Strategy

### Phase 1: Extract `spark-config-mapper`

1. Create new package with modules identified above
2. Add `lhn` as dependent on `spark-config-mapper`
3. Update imports in `lhn` to use new package
4. Test existing notebooks work unchanged

```python
# Before (in lhn/resource.py)
from lhn.data_transformation import read_config
from lhn.metadata_functions import processDataTables

# After
from spark_config_mapper import read_config
from spark_config_mapper.schema import processDataTables
```

### Phase 2: Refactor `lhn` Internal Structure

1. Reorganize into subpackages (core, cohort, ontology, etc.)
2. Maintain backward-compatible imports in `__init__.py`
3. Deprecate direct module imports over time

### Phase 3: Documentation and Testing

1. Update all documentation to reflect new structure
2. Add package-level tests
3. Create migration guide for existing users

---

## Decision Criteria

### Split into 3 packages if:
- Other non-healthcare projects need config mapping
- You want to publish `spark-config-mapper` as a general utility
- Clear separation of concerns is a priority

### Split into 2 packages if:
- Config mapper is unlikely to be reused outside healthcare
- Simpler dependency management is preferred
- Faster migration timeline needed

### Keep as 1 package if:
- No immediate reuse needs
- Maintenance overhead of multiple packages is a concern
- Current structure is working well enough

---

## Immediate Recommendation

**Start with Option B (Two Packages)**:

1. **`spark-config-mapper`**: Extract the configuration and schema mapping logic
2. **`lhn`**: Keep healthcare workflows, depend on spark-config-mapper

This gives you:
- Reusable config management for future projects
- Cleaner `lhn` focused on healthcare domain
- Manageable migration effort
- Foundation for further decomposition if needed

The config mapper is the most obvious candidate for extraction because:
1. It has the fewest healthcare-specific dependencies
2. You mentioned wanting to reuse it across projects
3. It's a natural seam in the dependency graph

---

## Module Classification Table

| Module | Current Location | Proposed Package | Rationale |
|--------|-----------------|------------------|-----------|
| header.py | lhn | Both (duplicated or shared) | Core imports needed everywhere |
| data_transformation.py | lhn | spark-config-mapper | read_config, recursive_template are generic |
| metadata_functions.py | lhn | spark-config-mapper | processDataTables is generic schema mapping |
| spark_utils.py | lhn | spark-config-mapper | Generic Spark operations |
| introspection_utils.py | lhn | spark-config-mapper | Generic utilities |
| list_operations.py | lhn | spark-config-mapper | Generic utilities |
| function_parameters.py | lhn | spark-config-mapper | Generic utilities |
| resource.py | lhn | lhn | Healthcare orchestration |
| extract.py | lhn | lhn | Healthcare extraction workflows |
| item.py | lhn | lhn | Healthcare data containers |
| db.py | lhn | lhn | Healthcare data access |
| shared_methods.py | lhn | lhn | Healthcare analysis methods |
| cohort.py | lhn | lhn | Healthcare cohort operations |
| cohort_matching.py | lhn | lhn | Healthcare matching |
| discern.py | lhn | lhn | Healthcare ontology |
| features.py | lhn | lhn | Healthcare features |
| data_summary.py | lhn | lhn | Healthcare analysis |
| metaTable_module.py | lhn | lhn | Healthcare metadata |
| plot.py | lhn | lhn | Healthcare visualization |
| excel_operations.py | lhn | lhn | Healthcare reporting |
| statisticalSummary.py | lhn | lhn | Healthcare statistics |
