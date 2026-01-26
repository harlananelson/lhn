# LHN Package Reference for Claude

This was created by Claude, use a a guide but it not necessarily ground truth.

## Overview

**lhn** is a Python package for healthcare data processing built on PySpark. It provides utilities for extracting, transforming, and analyzing patient data from HealthEIntent systems, including support for medical ontologies (DISCERN), cohort building, and clinical data analysis.

- **Version**: 0.1.0
- **Author**: Harlan A Nelson
- **Dependencies**: PySpark 2.4.4, pandas, numpy, scipy, matplotlib, pyyaml

---

## Installation & Import

```python
# Import the entire package
import lhn

# Or import specific modules/functions
from lhn import Resources, Extract, ExtractItem
from lhn import group_ethnicities, group_races, calcUsage, write_index_table
from lhn import writeTable, flattenTable, search_ontologies
from lhn import count_people, attrition, topAttributes
```

---

## Core Architecture

### Configuration-Driven Design

The package uses YAML configuration files to define:
- Data table locations and schemas
- Project-specific parameters
- Extraction workflows

### Three-Step Extraction Workflow

1. **create_extract**: Identify codes/indices from reference tables using regex or exact matching
2. **entityExtract**: Retrieve entity records using the identified codes
3. **write_index_table**: Create cohort-style tables with first/last instance information

---

## Key Classes

### `Resources` - Configuration & Resource Management

Central class for managing project configuration, data sources, and schema definitions.

```python
from lhn import Resources

r = Resources(
    project="my_project",
    spark=spark,  # SparkSession
    basePath=Path.home() / "work/Users/username",
    config_file='000-config.yaml',
    systemuser="username",
    process_all=True  # Auto-process all configured data tables
)

# Access processed data tables
r.rwd           # Real-world data tables
r.proj          # Project-specific tables
r.config_dict   # Full configuration dictionary
```

**Key Parameters:**
- `project` (str): Project name
- `spark`: SparkSession instance
- `basePath` (Path): Base path for project files
- `config_file` (str): YAML configuration filename
- `process_all` (bool): Auto-process all data tables on init

---

### `Extract` & `ExtractItem` - Data Extraction

Container classes for managing healthcare data extraction workflows.

```python
from lhn import Extract, ExtractItem

# Create Extract from configuration dictionary
e = Extract(r.proj)

# Access individual extraction items
drug_codes = e.medication_codes
medication_encounters = e.medication_encounters
```

#### `ExtractItem` Methods

**`create_extract()`** - Identify codes from reference tables
```python
drug_codes.create_extract(
    elementList=d.medication_drugcode,          # Source of search patterns
    elementListSource=d.medication_drugcode.df, # Table to search
    find_method='regex'                         # 'regex' or 'merge'
)
```

**`entityExtract()`** - Extract entity records
```python
medication_encounters.entityExtract(
    elementList=drug_codes,      # Codes to search for
    entitySource=r.medSource.df, # Source data table
    cohort=my_cohort,            # Optional: filter to cohort
    cacheResult=True
)
```

**`write_index_table()`** - Create cohort-style index table
```python
medication_index.write_index_table(
    inTable=medication_encounters,
    histStart='2020-01-01',
    histEnd='2024-12-31'
)
```

**Other Methods:**
- `load_csv_as_df()` - Load CSV file as DataFrame
- `writeTBL()` - Write DataFrame to Spark table
- `to_csv()` - Export to CSV
- `showIU(obs=6)` - Display sample records
- `attrition()` - Show record/person counts

---

### `DB` - Database Wrapper

Simple wrapper for accessing data tables.

```python
from lhn import DB

db = DB(resource_dict, 'proj')
db.my_table.df  # Access DataFrame
```

---

## Cohort Building Functions

### Demographics Standardization

```python
from lhn import group_ethnicities, group_races, group_races2, group_gender, group_marital_status

# Standardize ethnicity to 'Hispanic' or 'Not Hispanic'
df = group_ethnicities(df, column_name='ethnicity', result_column_name='ethnicity_group')

# Standardize race categories
df = group_races(df, column_name='race', result_column_name='race_group')
# Returns: 'Black', 'Indigenous', 'Asian', 'White', 'Middle Eastern', 'Caribbean', 'Hispanic', 'Mixed', 'Unknown', 'Other'

# Alternative grouping with 'Other/Unknown' combined
df = group_races2(df, column_name='race', result_column_name='race_group')

# Standardize gender
df = group_gender(df, column_name='gender', result_column_name='gender_group')
# Returns: 'Female', 'Male', 'Other', 'Unknown'

# Standardize marital status
df = group_marital_status(df, column_target='marital_group', column_source='marital_status')
# Returns: 'Married', 'Not Married', 'Unknown'
```

### Age Groups

```python
from lhn import assign_age_group

# Assign age groups based on quartiles
df = assign_age_group(df, age_column='age')
# Adds 'age_group' column with values 'Q1', 'Q2', 'Q3', 'Q4'
```

### Usage Calculation

```python
from lhn import calcUsage

# Calculate first/last encounter dates and count
result = calcUsage(
    df=encounter_df,
    fields=['personid', 'actualarrivaldate', 'servicedate'],
    dateCoalesce=F.coalesce(F.col('actualarrivaldate'), F.col('servicedate')),
    minDate='1950-01-01',
    maxDate='2023-12-31',
    index=['personid'],
    countFieldName='encounters',
    dateFirst='encDateFirst',
    dateLast='encDateLast'
)
```

### Index Table Creation

```python
from lhn import write_index_table

# Create patient-level summary with first/last dates
cohort_index = write_index_table(
    inTable=encounters_df,
    index_field=['personid'],
    retained_fields=['diagnosis_code', 'provider_id'],
    datefieldPrimary='service_date',
    datefieldStop='discharge_date',
    code='diabetes',           # Tag for output field names
    sort_fields=['service_date'],
    max_gap=370,               # Max days between encounters for therapy continuity
    histStart='2020-01-01',
    histEnd='2024-12-31',
    indexLabel='index',        # Prefix for first date field
    lastLabel='last'           # Prefix for last date field
)
# Output columns include: index_diabetes, last_diabetes, encounter_days, course_of_therapy, etc.
```

### Target Record Identification

```python
from lhn import identify_target_records

# Extract records from source table using index
elements = identify_target_records(
    entitySource=medication_df,      # Source table
    elementIndex=['drug_code'],      # Join fields
    elementExtract=drug_codes_df,    # Index table
    datefieldSource='service_date',
    histStart='2020-01-01',
    histStop='2024-12-31',
    cacheResult=True,
    broadcast_flag=True
)
```

---

## Spark Utilities

### Writing Tables

```python
from lhn import writeTable

# Write DataFrame to Spark table with partitioning
result = writeTable(
    DF=my_df,
    outTable='schema.table_name',
    partitionBy='tenant',
    description='Patient medications table',
    removeDuplicates=True
)
```

### Flattening Nested Structures

```python
from lhn import flattenTable

# Flatten nested arrays and structs (common in FHIR data)
flat_df = flattenTable(
    df=nested_df,
    inclusionRegex=['code.*', 'display.*'],  # Patterns for columns to flatten
    maxTrys=5
)
```

### Date Conversion

```python
from lhn import convert_date_fields

# Convert multiple columns to date type
df = convert_date_fields(['date1', 'date2', 'date3'])(df)
```

### Database Operations

```python
from lhn import database_exists, getTableList, check_table_existence

# Check if database exists
if database_exists('my_database'):
    tables = getTableList('my_database')

# Check specific table
exists = check_table_existence('schema.table_name')
# or
exists = check_table_existence('schema', 'table_name')
```

### Column Utilities

```python
from lhn import distCol

# Get distinct columns (remove duplicates from list)
unique_cols = distCol(df.columns, masterList=allowed_columns)
```

---

## Data Summary Functions

### Attrition Tracking

```python
from lhn import attrition, count_people

# Display attrition statistics
attrition(
    df=cohort_df,
    table_name='diabetes_cohort',
    person_id=['personid'],
    description='Diabetes patients with HbA1c',
    date_field='service_date'
)

# Count unique people
count_people(df, description='Final cohort', person_id='personid')
```

### Aggregation Functions

```python
from lhn import aggregate_fields, aggregate_fields_count, countDistinct

# Aggregate with multiple functions
result = aggregate_fields(
    df=patient_df,
    index=['personid'],
    fields=['lab_value'],
    values=['lab_value'],
    aggfuncs=[F.min, F.max, F.avg],
    aggfunc_names=['min', 'max', 'avg']
)

# Count distinct combinations
result = aggregate_fields_count(
    df=patient_df,
    index=['personid'],
    values=['lab_value']
)

# Count distinct by field
result = countDistinct(tbl=df, field='diagnosis', index='personid')
```

### Top Values Analysis

```python
from lhn import topAttributes, getTopValues

# Get top conditions for a cohort
top_conditions = topAttributes(
    inTable='conditions',
    inSchema='rwd_schema',
    cohortTable='my_cohort',
    cohortSchema='project_schema',
    startDate='2020-01-01',
    stopDate='2024-12-31',
    dataLoc='/path/to/data/',
    outfile='top_conditions',
    index='personid',
    obs=1000
)
```

---

## Medical Ontology Functions (DISCERN)

### Search Ontologies

```python
from lhn import search_ontologies

# Search standard ontologies with regex
results = search_ontologies(
    name_regex='diabetes',           # Concept name pattern
    system_regex='ICD.*',            # Coding system pattern
    context_regex='.*',              # Context pattern
    code_regex='E11.*',              # Code pattern
    toPandas=True,
    limit=True,
    obs=20
)
```

### Ontology Queries

```python
from lhn import (
    contextId_ont,      # Get unique context IDs
    context_ont,        # Get context details
    conceptCode_ont,    # Get concept codes
    system_ont,         # Get coding systems
    concept_ont,        # Get unique concepts
    codingSystem_ont    # Get coding system summary
)

# Get coding systems for diabetes concepts
systems = system_ont(
    name_regex='diabetes',
    system_regex='.*',
    context_regex='.*',
    code_regex='.*'
)
```

### Add Concept Indicators

```python
from lhn import add_concept_indicators

# Add boolean columns indicating concept presence
df = add_concept_indicators(
    conceptName=['Diabetes', 'Hypertension'],
    code='conditioncode_standard_id'
)(df)
```

---

## Query Functions

### Extract and Flatten Fields

```python
from lhn import extract_fields_flat, extract_fields_flat_top, query_flat_rwd

# Extract selected fields with flattening
result = extract_fields_flat(
    DF=source_df,
    fields=['personid', 'code.standard.id', 'code.standard.display'],
    explode_fields=['codes'],
    toPandas=True
)

# Extract top N with counting
result = extract_fields_flat_top(
    table=source_df,
    i=['code.standard.id', 'code.standard.display'],
    index=['personid'],
    obs=100,
    countfield='Subjects',
    toPandas=True
)
```

### Query with Concepts

```python
from lhn import query_flat_rwd

# Query with concept filtering
result = query_flat_rwd(
    DF=source_df,
    fields=['personid', 'encounterid', 'code'],
    datefieldPrimary='service_date',
    conceptName=['Diabetes', 'HbA1c'],
    filter_concepts=True,
    write=True,
    outTable='schema.filtered_results'
)
```

---

## Data Transformation

### Configuration Loading

```python
from lhn import read_config

# Load YAML configuration with variable substitution
config = read_config(
    config_file='path/to/config.yaml',
    replace={'project': 'my_project', 'date': '2024-01-01'},
    debug=False
)
```

### Schema Operations

```python
from lhn import flatten_schema, flat_schema, flatten_df

# Get flat field list from nested schema
flat_fields = flatten_schema(df.schema, separator='_')

# Flatten DataFrame columns
flat_df = flatten_df(df, columns=['nested.field.name'])
```

### Pivoting

```python
from lhn import pivot_wider, stackedSpark

# Pivot data to wider format
wide_df = pivot_wider(df, index=['personid'], columns='category', values='value')

# Stack/melt DataFrame
long_df = stackedSpark(df, id_vars=['personid'], value_vars=['col1', 'col2'])
```

---

## Statistical Functions

```python
from lhn import (
    calculate_chi_squared,
    calculate_percentile,
    gamma_percentile,
    five_number_summary
)

# Chi-squared test
chi2_result = calculate_chi_squared(observed, expected)

# Percentile calculation
percentile = calculate_percentile(data, percentile=0.95)

# Five number summary
summary = five_number_summary(df, value_column='lab_value')
# Returns: min, Q1, median, Q3, max
```

---

## Visualization

```python
from lhn import plot_counts, plotByTime, plotTopEntities

# Bar chart of counts
plot_counts(df, x_col='category', y_col='count', title='Category Distribution')

# Time series plot
plotByTime(df, date_col='service_date', value_col='count', title='Encounters Over Time')

# Top entities visualization
plotTopEntities(df, entity_col='diagnosis', count_col='patients', n=10)
```

---

## File Operations

```python
from lhn import put_to_hdfs, list_files, create_excel_spreadsheet

# Upload to HDFS
put_to_hdfs(local_path='data.csv', hdfs_path='/user/me/data.csv')

# List files
files = list_files('/user/me/')

# Create Excel with multiple sheets
create_excel_spreadsheet(
    dataframes={'Sheet1': df1, 'Sheet2': df2},
    output_path='output.xlsx'
)
```

---

## Utility Functions

### Column Name Collision Handling

```python
from lhn import noColColide

# Get columns avoiding collisions
safe_cols = noColColide(
    columns=source_df.columns,
    colideColumns=other_df.columns,
    index=['personid'],
    masterList=allowed_columns
)
```

### String Operations

```python
from lhn import escape_and_bound_dot, extractTableName

# Escape regex special characters
pattern = escape_and_bound_dot('code.standard.id', complete=True)

# Extract table name from path
table_name = extractTableName('schema.database.table_name')
```

---

## Common Patterns

### Complete Extraction Workflow

```python
from lhn import Resources, Extract

# 1. Initialize resources
r = Resources(project='diabetes_study', spark=spark, process_all=True)

# 2. Create extraction objects
e = Extract(r.proj)

# 3. Find drug codes using regex
e.drug_codes.create_extract(
    elementList=e.drug_list,
    elementListSource=r.drug_dictionary.df,
    find_method='regex'
)

# 4. Extract medication records for cohort
e.medications.entityExtract(
    elementList=e.drug_codes,
    entitySource=r.medication_source.df,
    cohort=e.study_cohort
)

# 5. Create patient-level index
e.medication_index.write_index_table(
    inTable=e.medications,
    histStart='2020-01-01',
    histEnd='2024-12-31'
)

# 6. Check attrition
e.drug_codes.attrition()
e.medications.attrition()
e.medication_index.attrition()
```

### Cohort Building Pattern

```python
from lhn import (
    group_ethnicities, group_races, group_gender,
    calcUsage, write_index_table
)

# Start with base population
cohort = spark.table('patients')

# Standardize demographics
cohort = group_ethnicities(cohort, 'ethnicity', 'ethnicity_std')
cohort = group_races(cohort, 'race', 'race_std')
cohort = group_gender(cohort, 'gender', 'gender_std')

# Calculate usage metrics
usage = calcUsage(
    df=encounters,
    fields=['personid', 'service_date'],
    dateCoalesce=F.col('service_date'),
    minDate='2020-01-01',
    maxDate='2024-12-31',
    index=['personid'],
    countFieldName='encounter_count',
    dateFirst='first_encounter',
    dateLast='last_encounter'
)

# Join usage to cohort
cohort = cohort.join(usage, on='personid', how='left')
```

---

## Configuration File Structure (000-config.yaml)

```yaml
# Project settings
project: my_project
disease: diabetes
schemaTag: RWD

# Schema locations
schemas:
  projectSchema: project_db
  RWDSchema: rwd_database
  IUHSchema: iuhealth_data

# Data table definitions
projectTables:
  study_cohort:
    location: project_db.study_cohort
    label: Main study cohort
    indexFields: [personid]
    datefieldPrimary: index_date

  medications:
    location: project_db.medications
    label: Medication records
    indexFields: [personid, encounterid]
    retained_fields: [drug_code, drug_name, dose]

# RWD source tables
RWDTables:
  conditionSource:
    location: rwd_database.conditions
    label: Condition records
  medicationSource:
    location: rwd_database.medications
    label: Medication records
```

---

## Notes for Claude

1. **Always use PySpark functions** (`F.col()`, `F.lit()`, etc.) when working with DataFrames
2. **Configuration-driven**: Most operations depend on YAML config files
3. **Three-step workflow**: create_extract → entityExtract → write_index_table
4. **Healthcare-specific**: Functions assume medical data structures (personid, encounterid, codes)
5. **DISCERN integration**: Ontology functions work with Cerner DISCERN UDFs
6. **Tenant partitioning**: Many tables are partitioned by `tenant` for multi-site data
