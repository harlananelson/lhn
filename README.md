# lhn - HealthEIntent Package

**STATUS: DEVELOPMENT (v0.2.0.dev0)**

> This is the development/refactored version. Production code is in `../lhn-original/`

Healthcare data extraction and analysis workflows for HealthEIntent systems.

## Overview

The `lhn` package provides specialized tools for clinical data processing, including:

- **Patient cohort identification** - Define and extract patient populations
- **Clinical feature engineering** - Transform raw data into analytical features
- **Temporal analysis** - Index events and track outcomes over time
- **Ontology integration** - Work with clinical coding systems via Discern

This refactored version separates generic configuration management into `spark-config-mapper`, keeping only healthcare-specific functionality in `lhn`.

## Installation

```bash
# Install spark-config-mapper dependency first
pip install -e ../spark_config_mapper

# Install lhn in development mode
pip install -e .
```

## Quick Start

### Initialize Resources

```python
from lhn import Resources

# Load project with configuration files
r = Resources(
    local_config='000-config.yaml',
    global_config='config-global.yaml',
    schemaTag_config='config-RWD.yaml',
    replace={'today': '2025-01-19'}
)

# Access source data tables
encounters = r.r.encounter.df
persons = r.r.person.df

# Access project extracts
cohort = r.e.cohort
```

### Create Cohort Index

```python
# Identify first encounter for each patient
r.e.cohort.write_index_table(
    inTable=r.r.encounter.df,
    histStart='2020-01-01',
    histEnd='2024-12-31'
)

# View results
r.e.cohort.attrition()
r.e.cohort.show(5)
```

### Extract Records for Cohort

```python
# Get all conditions for cohort patients
conditions = r.e.cohort.identify_target_records(
    entitySource=r.r.condition.df,
    elementIndex=['personid'],
    datefieldSource='effectiveDate',
    histStart=-365,  # 1 year before index
    histStop=365,    # 1 year after index
    datefieldElement='indexDate'
)
```

### Save Results

```python
# Write to Spark table
r.e.cohort.write()

# Export to CSV
r.e.cohort.to_csv()
```

## Package Structure

```
lhn/
├── __init__.py          # Package exports
├── header.py            # Imports, extends spark_config_mapper
├── core/
│   ├── __init__.py
│   ├── resource.py      # Resources class (orchestration)
│   ├── extract.py       # Extract, ExtractItem classes
│   ├── db.py            # DB class
│   └── shared_methods.py # SharedMethodsMixin
├── cohort/              # Cohort operations (to be added)
├── ontology/            # Discern integration (to be added)
└── analysis/            # Analysis utilities (to be added)
```

## Key Classes

### Resources

Central orchestration class that manages:
- Configuration loading (3-tier YAML hierarchy)
- Schema mapping (logical -> physical)
- Data table access

### Extract / ExtractItem

Container for project output tables with methods for:
- Index table creation (first/last records)
- Record extraction from source tables
- Data persistence

### SharedMethodsMixin

Common methods available on all data containers:
- `show()`, `count()`, `columns()`, `schema()`
- `attrition()` - Clinical data reporting
- `write()`, `to_csv()`, `to_parquet()`
- `filter()`, `select()`, `join()`

## Configuration Files

### Project Config (`000-config.yaml`)

```yaml
project: MyProject
disease: SCD
schemaTag: RWD

schemas:
  RWDSchema: iuhealth_ed_data_cohort_202306
  projectSchema: myproject_output

projectTables:
  cohort:
    label: "Patient Cohort"
    indexFields: [personid, tenant]
    datefieldPrimary: [serviceDate]
```

## Dependencies

- **spark-config-mapper** - Configuration and schema mapping
- **pyspark** >= 3.0.0
- **pandas**, **numpy**, **scipy**, **matplotlib**

## Migration from Monolithic lhn

If migrating from the original `lhn` package:

1. Install `spark-config-mapper` as a dependency
2. Update imports that used config functions:
   ```python
   # Old
   from lhn.data_transformation import read_config
   
   # New
   from spark_config_mapper import read_config
   # Or simply use lhn's re-export
   from lhn import read_config
   ```

3. Core classes remain the same:
   ```python
   # Still works
   from lhn import Resources, Extract, ExtractItem
   ```

## License

Proprietary - IU Health
