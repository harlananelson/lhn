# LHN Project Initialization Guide

> **Purpose**: Complete guide for an AI to generate configuration files and Jupyter notebooks for a new healthcare data analytics project using the lhn package.

---

## Table of Contents

1. [Project Definition Template](#1-project-definition-template)
2. [000-control.yaml Generation](#2-000-configyaml-generation)
3. [Table Definition Patterns](#3-table-definition-patterns)
4. [Code Input Methods](#4-code-input-methods)
5. [Notebook Generation Patterns](#5-notebook-generation-patterns)
6. [Complete Worked Example](#6-complete-worked-example)
7. [Validation Checklist](#7-validation-checklist)

---

## 1. Project Definition Template

Before generating configuration, gather this information:

```yaml
PROJECT_DEFINITION:
  # Basic Identity
  project_name: ""              # e.g., "DiabetesStudy"
  disease_abbreviation: ""      # e.g., "DM2" (used in table naming)
  investigators: []             # List of investigator names

  # Schema Configuration
  source_schema: ""             # Spark schema with source data
  output_schema: ""             # Spark schema for project outputs
  schema_structure: "RWD"       # "RWD" | "OMOP" | "IUH"

  # Study Parameters
  study_dates:
    history_start: ""           # e.g., "2015-01-01"
    history_stop: ""            # e.g., "${today}"

  # Cohort Definition
  cohort_criteria:
    condition_codes: []         # ICD-10 patterns (regex OK)
    code_system: ""             # "ICD10CM" | "SNOMED"
    inclusion_logic: ""         # Natural language description
    exclusion_logic: ""         # Natural language description

  # Required Data Domains
  required_domains:
    - conditions                # Always needed
    - encounters
    - demographics
    - labs                      # Optional
    - medications               # Optional
    - procedures                # Optional

  # Code Sets (if known)
  code_sets:
    - name: ""
      codes: {}                 # Dictionary of category: pattern
      code_system: ""

  # CSV Code Files (preferred method)
  csv_code_files:
    - table_key: ""             # e.g., "comorbidity_codes"
      filename: ""              # e.g., "comorbidity_codes_DM2_RWD.csv"
      description: ""
      code_column: ""           # Column containing codes
      group_column: ""          # Column for grouping
```

---

## 2. 000-control.yaml Generation

### 2.1 Header Section

```yaml
# ============================================================================
# PROJECT CONFIGURATION: {project_name}
# ============================================================================
# Investigators: {investigators}
# Created: {date}
# Purpose: {description}
# ============================================================================

project: {project_name}
investigators: |
  {investigator_1}
  {investigator_2}
systemuser: hnelson3
```

### 2.2 Study Parameters

```yaml
# ============================================================================
# STUDY PARAMETERS
# ============================================================================

startDate: "{history_start}"
stopDate: ${today}
historyStart: "{history_start}"
historyStop: ${today}

disease: "{disease_abbreviation}"
schemaTag: "{schema_structure}"
gap: 90                              # Days for therapy courses
serviceToFirstDx: 60                 # Days for baseline period
```

### 2.3 Schema Mappings

```yaml
# ============================================================================
# SCHEMA MAPPINGS
# ============================================================================

schemas:
  RWDSchema: {source_schema}
  projectSchema: {output_schema}
```

### 2.4 projectTables Section

```yaml
# ============================================================================
# PROJECT TABLES
# ============================================================================

projectTables:
  # ... table definitions (see patterns below)
```

---

## 3. Table Definition Patterns

### 3.1 Cohort Identifier Table

First table to create - identifies all patients in cohort.

```yaml
persontenant:
  label: "Person and Tenant ID for cohort"
  indexFields: ["personid", "tenant"]
  partitionBy: "tenant"
```

### 3.2 Code Search Table (Dictionary Method)

For simple regex patterns:

```yaml
target_codes:
  label: "Target condition codes"
  dictionary:
    category1: "E11\\.[0-9]"           # Regex pattern
    category2: "E13\\.[0-9]"
  listIndex: "codes"
  sourceField: "conditioncode_standard_id"
  complete: True                        # True = exact, False = partial
```

### 3.3 Code Search Table (CSV Method - PREFERRED)

For clinician-verified codes:

```yaml
comorbidity_codes:
  label: "Comorbidity codes from clinical review"
  # CSV path auto-generated: {dataLoc}{table_key}_{disease}_{schemaTag}.csv
  indexFields:
    - "conditioncode_standard_id"
    - "Category"
    - "Description"
```

**CSV file format**:
```csv
Category,conditioncode_standard_id,Description
Diabetes,E11.9,"Type 2 diabetes without complications"
Diabetes,E11.65,"Type 2 diabetes with hyperglycemia"
```

### 3.4 Verified Codes Table

Output of create_extract:

```yaml
target_codes_verified:
  label: "Verified target codes"
  groupName: "regexgroup"
  indexFields:
    - "conditioncode_standard_primaryDisplay"
    - "conditioncode_standard_id"
  datefield: null
```

### 3.5 Entity Encounter Table

Output of entityExtract:

```yaml
condition_encounters:
  label: "Condition records for cohort"
  datefield: "datetimeCondition"
  histStart: "${historyStart}"
  histEnd: "${historyStop}"
  indexFields: ["personid", "tenant"]
  retained_fields:
    - "encounterid"
    - "conditionid"
    - "conditioncode_standard_id"
    - "conditioncode_standard_primaryDisplay"
    - "datetimeCondition"
    - "tenant"
```

### 3.6 Index Table

Output of write_index_table:

```yaml
condition_index:
  label: "Condition patient-level summary"
  indexFields: ["personid", "tenant"]
  datefieldPrimary: "datetimeCondition"
  datefieldStop: "datetimeCondition"
  code: "COND"                         # Creates index_COND, last_COND
  retained_fields:
    - "conditioncode_standard_primaryDisplay"
  sort_fields: ["datetimeCondition"]
  max_gap: 365
  histStart: "${historyStart}"
  histEnd: "${historyStop}"
```

### 3.7 Demographics Table

```yaml
demographics:
  label: "Patient demographics"
  datefield: "birthdate"
  indexFields: ["personid", "tenant"]

death:
  label: "Patient death records"
  indexFields: ["personid", "tenant"]
```

### 3.8 Analytic Dataset (ADS)

Final analysis-ready dataset:

```yaml
ads:
  label: "Analytic Dataset - Primary Analysis"
  indexFields: ["personid", "tenant"]
  datefield: "index_date"
```

---

## 4. Code Input Methods

### 4.1 Method 1: CSV File (PREFERRED)

**Why preferred**:
- Codes verified against actual data
- Patient counts known
- Clinician reviewed
- Grouped by clinical category

**CSV naming convention**:
```
{table_key}_{disease}_{schemaTag}.csv
```
Example: `comorbidity_codes_DM2_RWD.csv`

**CSV structure**:
```csv
Category,code_column,Description,N
Diabetes Type 2,E11.9,"Type 2 diabetes without complications",15234
Diabetes Type 2,E11.65,"Type 2 diabetes with hyperglycemia",8421
```

**Config entry**:
```yaml
comorbidity_codes:
  label: "Comorbidity codes"
  indexFields:
    - "code_column"
    - "Category"
    - "Description"
```

**Notebook usage**:
```python
# Load CSV
e.comorbidity_codes.load_csv_as_df()

# Verify
e.comorbidity_codes.attrition()

# Use in extraction
e.condition_encounters.entityExtract(
    elementList=e.comorbidity_codes,
    entitySource=r.conditionSource
)
```

### 4.2 Method 2: Dictionary in Config

For simple, well-known patterns:

```yaml
diabetes_codes:
  label: "Diabetes diagnosis codes"
  dictionary:
    type2: "E11\\.[0-9]"
    type1: "E10\\.[0-9]"
  listIndex: "codes"
  sourceField: "conditioncode_standard_id"
  complete: True
```

### 4.3 Method 3: Reference Table Join

For existing dictionary tables:

```yaml
lab_codes:
  label: "Lab codes from reference"
  indexFields:
    - "labcode_standard_id"
    - "labcode_standard_codingSystemId"
```

---

## 5. Notebook Generation Patterns

### 5.1 Standard Notebook Structure

```
010-Cohort-Identification.ipynb
020-Demographics.ipynb
030-Condition-Extraction.ipynb
040-Medication-Extraction.ipynb
050-Lab-Extraction.ipynb
060-ADS-Assembly.ipynb
```

### 5.2 Setup Cell Template

```python
# ============================================================================
# {PROJECT_NAME}: {NOTEBOOK_TITLE}
# ============================================================================
# Purpose: {description}
# Author: {author}
# ============================================================================

from lhn import Resources, Extract
from spark_config_mapper.utils import writeTable
from pyspark.sql import functions as F
from pathlib import Path

project = '{project_name}'
resource = Resources(
    local_config='000-control.yaml',
    global_config='configuration/config-global.yaml',
    schemaTag_config='configuration/config-RWD.yaml',
    debug=True,                                    # logs config load + processing
    # finish_init=True                             # default; skips only if False
)

locals().update(resource.load_into_local())
locals().update(local_vars)

r = resource.r
db = resource.db
```

### 5.3 Domain Extraction Pattern

```python
# ============================================================================
# {DOMAIN} EXTRACTION
# ============================================================================

# Step 1: Identify codes
print("Step 1: Identifying {domain} codes...")
e.{domain}_codes.create_extract(
    elementList=d.{reference_table},
    elementListSource=d.{reference_table}.df,
    find_method='regex'
)
e.{domain}_codes.attrition()

# Step 2: Extract encounters
print("Step 2: Extracting {domain} encounters...")
e.{domain}_encounters.entityExtract(
    elementList=e.{domain}_codes_verified,
    entitySource=r.{sourceTable},
    cacheResult=True
)
e.{domain}_encounters.attrition()

# Step 3: Create index
print("Step 3: Creating patient index...")
e.{domain}_index.write_index_table(
    inTable=e.{domain}_encounters
)
e.{domain}_index.attrition()

# Verify
e.{domain}_index.tabulate(by='{code_field}_standard_primaryDisplay', obs=20)
```

### 5.4 CSV Loading Pattern

```python
# ============================================================================
# LOAD CLINICIAN-VERIFIED CODES
# ============================================================================

# Load CSV codes
e.comorbidity_codes.load_csv_as_df()

# Verify
print(f"Loaded {e.comorbidity_codes.df.count()} codes")
e.comorbidity_codes.tabulate(by='Category')

# Use in extraction
e.comorbidity_encounters.entityExtract(
    elementList=e.comorbidity_codes,
    entitySource=r.conditionSource,
    cacheResult=True
)
```

### 5.5 Demographics Pattern

```python
# ============================================================================
# DEMOGRAPHICS
# ============================================================================
from lhn.cohort import group_races, group_ethnicities, group_gender

# Get demographics
demographics = r.demoPreferred.df.select(
    'personid', 'tenant', 'gender', 'race', 'ethnicity', 'birthdate'
)

# Join with cohort
cohort_demo = e.persontenant.df.join(demographics, on=['personid', 'tenant'], how='left')

# Standardize categories
cohort_demo = group_gender(cohort_demo, 'gender', 'gender_group')
cohort_demo = group_races(cohort_demo, 'race', 'race_group')
cohort_demo = group_ethnicities(cohort_demo, 'ethnicity', 'ethnicity_group')

# Calculate age
cohort_demo = cohort_demo.withColumn(
    'age_at_index',
    F.floor(F.datediff(F.col('index_date'), F.col('birthdate')) / 365.25)
)

# Save
writeTable(cohort_demo, e.demographics.location, description=e.demographics.label)
e.demographics.attrition()
```

### 5.6 ADS Assembly Pattern

```python
# ============================================================================
# ANALYTIC DATASET ASSEMBLY
# ============================================================================

# Start with cohort index
ads = e.condition_index.df.select(
    'personid', 'tenant',
    F.col('index_COND').alias('index_date'),
    F.col('last_COND').alias('last_condition_date')
)

# Join demographics
ads = ads.join(
    e.demographics.df.select('personid', 'tenant', 'age_at_index', 'gender_group', 'race_group'),
    on=['personid', 'tenant'],
    how='left'
)

# Join medication summary
ads = ads.join(
    e.medication_index.df.select('personid', 'tenant', F.col('index_MED').alias('first_med')),
    on=['personid', 'tenant'],
    how='left'
)

# Calculate follow-up
ads = ads.withColumn(
    'follow_up_days',
    F.datediff(F.col('last_condition_date'), F.col('index_date'))
)

# Save
writeTable(ads, e.ads.location, description=e.ads.label)
e.ads.attrition()
```

---

## 6. Complete Worked Example

### 6.1 Project Definition

```yaml
PROJECT_DEFINITION:
  project_name: "DiabetesT2Study"
  disease_abbreviation: "DM2"
  investigators: ["Jane Smith MD", "John Doe PhD"]
  source_schema: "iuhealth_ed_data_cohort_202306"
  output_schema: "diabetes_t2_study_rwd"
  schema_structure: "RWD"

  study_dates:
    history_start: "2015-01-01"
    history_stop: "${today}"

  cohort_criteria:
    condition_codes: ["E11\\.[0-9]"]
    code_system: "ICD10CM"
    inclusion_logic: "Patients with at least one E11.x diagnosis"
    exclusion_logic: "Exclude Type 1 diabetes (E10.x)"

  required_domains: [conditions, encounters, demographics, labs, medications]
```

### 6.2 Generated 000-control.yaml

```yaml
# ============================================================================
# PROJECT CONFIGURATION: DiabetesT2Study
# ============================================================================
# Investigators: Jane Smith MD, John Doe PhD
# Created: 2025-01-20
# ============================================================================

project: DiabetesT2Study
disease: "DM2"
schemaTag: "RWD"

startDate: "2015-01-01"
stopDate: ${today}
historyStart: "2015-01-01"
historyStop: ${today}

schemas:
  RWDSchema: iuhealth_ed_data_cohort_202306
  projectSchema: diabetes_t2_study_rwd

search_string_dm2: "E11\\.[0-9]"

projectTables:

  persontenant:
    label: "DM2 cohort patients"
    indexFields: ["personid", "tenant"]

  dm2_codes:
    label: "Type 2 Diabetes ICD-10 codes"
    dictionary:
      dm2: "${search_string_dm2}"
    listIndex: "codes"
    sourceField: "conditioncode_standard_id"
    complete: True

  dm2_codes_verified:
    label: "Verified DM2 codes"
    groupName: "regexgroup"
    indexFields:
      - "conditioncode_standard_primaryDisplay"
      - "conditioncode_standard_id"

  dm2_encounters:
    label: "DM2 condition records"
    datefield: "datetimeCondition"
    histStart: "${historyStart}"
    histEnd: "${historyStop}"
    indexFields: ["personid", "tenant"]
    retained_fields:
      - "encounterid"
      - "conditioncode_standard_id"
      - "conditioncode_standard_primaryDisplay"
      - "datetimeCondition"

  dm2_index:
    label: "DM2 patient index"
    indexFields: ["personid", "tenant"]
    datefieldPrimary: "datetimeCondition"
    code: "DM2"
    retained_fields:
      - "conditioncode_standard_primaryDisplay"
    sort_fields: ["datetimeCondition"]
    max_gap: 365
    histStart: "${historyStart}"
    histEnd: "${historyStop}"

  demographics:
    label: "Patient demographics"
    datefield: "birthdate"
    indexFields: ["personid", "tenant"]

  ads:
    label: "Analytic Dataset"
    indexFields: ["personid", "tenant"]
    datefield: "index_date"
```

### 6.3 Generated Notebook: 010-Cohort-Identification.ipynb

```python
# Cell 1: Setup
# ============================================================================
# DiabetesT2Study: Cohort Identification
# ============================================================================

from lhn import Resources, Extract
from spark_config_mapper.utils import writeTable
from pyspark.sql import functions as F
from pathlib import Path

project = 'DiabetesT2Study'
resource = Resources(
    local_config='000-control.yaml',
    global_config='configuration/config-global.yaml',
    schemaTag_config='configuration/config-RWD.yaml',
    debug=True,                                    # logs config load + processing
    # finish_init=True                             # default; skips only if False
)

locals().update(resource.load_into_local())
locals().update(local_vars)
r = resource.r

# Cell 2: Identify DM2 codes
# ============================================================================
# Step 1: Identify DM2 condition codes
# ============================================================================

e.dm2_codes.create_extract(
    elementList=e.dm2_codes,
    elementListSource=r.condition_codes.df,
    find_method='regex'
)

print(f"DM2 codes: {e.dm2_codes.df.count()}")
e.dm2_codes.attrition()

# Cell 3: Extract encounters
# ============================================================================
# Step 2: Extract DM2 condition encounters
# ============================================================================

e.dm2_encounters.entityExtract(
    elementList=e.dm2_codes_verified,
    entitySource=r.conditionSource,
    cacheResult=True
)

print(f"Encounters: {e.dm2_encounters.df.count()}")
e.dm2_encounters.attrition()

# Cell 4: Create index
# ============================================================================
# Step 3: Create patient index
# ============================================================================

e.dm2_index.write_index_table(inTable=e.dm2_encounters)

print(f"Patients: {e.dm2_index.df.select('personid').distinct().count()}")
e.dm2_index.attrition()

# Cell 5: Create cohort
# ============================================================================
# Create cohort table
# ============================================================================

cohort = e.dm2_index.df.select('personid', 'tenant').distinct()
writeTable(cohort, e.persontenant.location, description=e.persontenant.label)

print(f"Cohort: {cohort.count()} patients")

# Cell 6: Quality checks
# ============================================================================
# Quality Checks
# ============================================================================

# Date range
e.dm2_index.df.select(
    F.min('index_DM2').alias('earliest'),
    F.max('last_DM2').alias('latest')
).show()

# Condition distribution
e.dm2_index.tabulate(by='conditioncode_standard_primaryDisplay', obs=20)
```

---

## 7. Validation Checklist

### 7.1 Configuration Validation

- [ ] All `${variable}` references resolve
- [ ] `schemas` dict contains all referenced schemas
- [ ] Every `projectTables` entry has a `label`
- [ ] `indexFields` include `personid` and `tenant`
- [ ] Code tables have matching verified tables
- [ ] Entity tables have matching index tables

### 7.2 Notebook Validation

- [ ] Resources initialized with the 3-config signature (`local_config=`, `global_config=`, `schemaTag_config=`); `finish_init=True` is the default
- [ ] `locals().update(resource.load_into_local())` called before using `d`
- [ ] Each domain follows 3-step pattern
- [ ] `attrition()` called after each step
- [ ] Tables written with `writeTable()`

### 7.3 Data Quality Checks

- [ ] Patient counts decrease appropriately
- [ ] Date ranges within study period
- [ ] No unexpected NULL values
- [ ] Code distributions match expectations

---

## Appendix: Field Reference

### Condition Fields
- `conditioncode_standard_id` - ICD-10 code
- `conditioncode_standard_primaryDisplay` - Description
- `datetimeCondition` - Date

### Medication Fields
- `drugcode_standard_id` - Drug code
- `drugcode_standard_primaryDisplay` - Drug name
- `datetimeMed` - Date
- `startdate`, `stopdate` - Duration

### Lab Fields
- `labcode_standard_id` - LOINC code
- `labcode_standard_primaryDisplay` - Test name
- `typedvalue_numericValue_value` - Result
- `datetimeLab` - Date

### Common Fields
- `personid` - Patient ID
- `tenant` - Data source
- `encounterid` - Encounter ID
