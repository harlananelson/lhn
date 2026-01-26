# LHN Project Initialization Guide

> **Purpose**: This document provides complete specifications for an AI to generate a `000-config.yaml` configuration file and accompanying Jupyter notebooks for a new healthcare data analytics project using the lhn package.

---

## Table of Contents

1. [Project Definition Template](#1-project-definition-template)
2. [000-config.yaml Structure Specification](#2-000-configyaml-structure-specification)
3. [Table Type Patterns](#3-table-type-patterns)
4. [Clinical Code Discovery Workflow](#4-clinical-code-discovery-workflow)
5. [Extract Workflow Patterns](#5-extract-workflow-patterns)
6. [Notebook Generation Patterns](#6-notebook-generation-patterns)
7. [Complete Worked Example](#7-complete-worked-example)
8. [Validation Checklist](#8-validation-checklist)

---

## 1. Project Definition Template

Before generating configuration, gather the following information:

### Required Information

```yaml
PROJECT_DEFINITION:
  # Basic Identity
  project_name: ""           # e.g., "DiabetesStudy", "SickleCell"
  disease_abbreviation: ""   # e.g., "DM", "SCD" (used in table naming)
  investigators: []          # List of investigator names
  
  # Schema Configuration
  source_schema: ""          # Spark catalog schema with source data (e.g., "iuhealth_ed_data_cohort_202306")
  output_schema: ""          # Spark catalog schema for project outputs (e.g., "diabetes_study_rwd")
  schema_structure: ""       # "RWD" | "OMOP" | "IUH" - determines which config-*.yaml to use
  
  # Cohort Definition
  cohort_criteria:
    condition_codes: []      # ICD-10 codes defining the cohort (regex patterns OK)
    code_system: ""          # "ICD10CM" | "SNOMED" | "LOINC"
    inclusion_logic: ""      # Natural language description of inclusion criteria
    exclusion_logic: ""      # Natural language description of exclusion criteria
  
  # Study Parameters
  study_dates:
    history_start: ""        # e.g., "2000-01-01"
    history_stop: ""         # e.g., "${today}" or specific date
  
  # Data Elements Needed
  required_domains:
    - conditions             # Always needed for cohort identification
    - encounters             # Patient visits
    - demographics           # Patient characteristics
    - labs                   # Laboratory results (optional)
    - medications            # Medication records (optional)
    - procedures             # Procedure records (optional)
    - vitals                 # Vital signs (optional)
  
  # Analysis Goals
  outcomes:
    - name: ""               # e.g., "mortality", "hospitalization"
      definition: ""         # How to identify this outcome
    - name: ""
      definition: ""
  
  # Specific Code Sets (if known)
  code_sets:
    - name: ""               # e.g., "target_medications"
      codes: []              # List of codes or regex patterns
      code_system: ""        # "NDC" | "MULTUM" | "LOINC" etc.
  
  # CSV Code Files (from clinical exploration)
  # These are the PREFERRED method - codes verified against actual data
  csv_code_files:
    - table_key: ""          # e.g., "comorbidity_procedure_Codes"
      filename: ""           # e.g., "comorbidity_procedure_Codes_SCD_RWD.csv"
      description: ""        # What codes are in this file
      code_column: ""        # e.g., "procedurecode_standard_id"
      group_column: ""       # e.g., "Procedure" (category/grouping)
      has_counts: false      # Whether file includes patient counts
```

---

## 2. 000-config.yaml Structure Specification

### 2.1 Header Section

```yaml
# ============================================================================
# PROJECT CONFIGURATION: {project_name}
# ============================================================================
# Investigators: {investigators}
# Created: {date}
# Purpose: {brief description}
# ============================================================================

project: {project_name}
investigators: |
  {investigator_1}
  {investigator_2}
systemuser: hnelson3
discern_context: {context_id}  # Optional: for ontology lookups
discern_root: s3://iuh-datalab-persistence-s3-data/discernontology/v1/
```

### 2.2 Study Parameters Section

```yaml
# ============================================================================
# STUDY PARAMETERS
# ============================================================================

# Date Boundaries
startDate: "{history_start}"
stopDate: ${today}
historyStart: "{history_start}"
historyStop: ${today}

# Disease/Study Identifiers
disease: "{disease_abbreviation}"      # Used in table naming: tablename_{disease}_{schemaTag}
schemaTag: "{schema_structure}"        # RWD | OMOP | IUH

# Study-Specific Parameters
gap: 60                                 # Default gap for therapy courses (days)
serviceToFirstDx: 60                    # Days surrounding index for baseline
ageMin: null                            # Age filters (null = no filter)
ageMax: null
```

### 2.3 Schema Mapping Section

```yaml
# ============================================================================
# SCHEMA MAPPINGS
# ============================================================================
# Maps logical names to physical Spark catalog schemas

schemas:
  RWDSchema: {source_schema}            # Source data schema
  projectSchema: {output_schema}        # Project output schema
  # Add additional schemas as needed:
  # omopSchema: "real_world_data_ed_omop_may_2024"
  # metaSchema: "clinical_research_systems"
  # dictrwdSchema: "datadictrwd"
```

### 2.4 Cohort Definition Section

```yaml
# ============================================================================
# COHORT DEFINITION
# ============================================================================

# Search patterns for cohort identification (regex supported)
search_string_primary: "{regex_for_primary_condition}"
search_string_exclusion: "{regex_for_exclusions}"

# Structured code dictionary for create_extract
cohort_codes:
  primary: "${search_string_primary}"
  exclusion: "${search_string_exclusion}"
```

### 2.5 projectTables Section

This is the core section defining all output tables. See [Section 3](#3-table-type-patterns) for patterns.

```yaml
# ============================================================================
# PROJECT TABLES
# ============================================================================
# Each entry defines a table that will be created in {output_schema}

projectTables:
  # ... table definitions (see patterns below)
```

---

## 3. Table Type Patterns

### 3.1 Pattern: Cohort Identifier Table

**Purpose**: First table to create - identifies all patients in the cohort.

```yaml
  persontenant:
    label: "Person and Tenant ID for every member of the cohort"
    indexFields: ["personid", "tenant"]
    partitionBy: "tenant"
```

### 3.2 Pattern: Code Search Table (Three Methods)

**Purpose**: Define codes to search for in source data.

There are **three methods** for specifying codes, in order of preference:

---

#### **Method 1: CSV File Loading (PREFERRED)**

**Why this is best**: Codes come from actual data exploration, sorted by patient counts. A clinician uses an exploration app to find relevant codes, reviews the actual data distribution, and exports verified codes to CSV.

**CSV File Requirements**:
- **Naming convention**: `{tablename}_{disease}_{schemaTag}.csv`
  - Example: `comorbidity_procedure_Codes_SCD_RWD.csv`
- **Location**: `{dataLoc}` directory (e.g., `~/work/Users/hnelson3/inst/extdata/SickleCell/`)
- **Required columns**: Must include the code field (e.g., `procedurecode_standard_id`)
- **Recommended columns**: Group/category, description, patient counts

**Example CSV Structure**:
```csv
Procedure,procedurecode_standard_id,Description
Electrocardiogram (EKG/ECG),93000,"Electrocardiogram, routine ECG with at least 12 leads"
Electrocardiogram (EKG/ECG),93010,"Electrocardiogram, interpretation and report"
Echocardiogram,93306,"Echocardiography, transthoracic, complete study"
Pulmonary Function Test (PFT),94010,"Spirometry, including graphic record"
```

**Config Entry for CSV Loading**:
```yaml
  comorbidity_procedure_Codes:
    label: "Comorbidity Procedure Codes from Clinical Review"
    # NOTE: csv path is AUTO-GENERATED from table key + disease + schemaTag
    # Pattern: ${dataLoc}{table_key}_{disease}_{schemaTag}.csv
    # Only specify 'csv:' if overriding the default path
    indexFields:
      - "procedurecode_standard_id"
      - "Procedure"                    # Group/category column
      - "Description"
```

**Auto-Generated Path Example**:
```
Table key:  comorbidity_procedure_Codes
Disease:    SCD
SchemaTag:  RWD
DataLoc:    ~/work/Users/hnelson3/inst/extdata/SickleCell/

Auto-generated csv path:
  ~/work/Users/hnelson3/inst/extdata/SickleCell/comorbidity_procedure_Codes_SCD_RWD.csv
```

**Notebook Usage**:
```python
# Load codes from CSV into Spark table
e.comorbidity_procedure_Codes.load_csv_as_df()

# Verify loaded codes
e.comorbidity_procedure_Codes.df.show()
e.comorbidity_procedure_Codes.attrition()

# Now use in entityExtract
e.procedureEncounters.entityExtract(
    elementList=e.comorbidity_procedure_Codes,
    entitySource=r.procedureSource,
    cacheResult=True
)
```

**Key Advantages**:
1. **Data-driven**: Codes are verified to exist in the actual data
2. **Counted**: You know how many patients have each code before extraction
3. **Clinician-reviewed**: Domain expert selects relevant codes
4. **Grouped**: Natural groupings (e.g., "Echocardiogram" includes multiple CPT codes)
5. **Documented**: Descriptions provide clinical context

---

#### **Method 2: Dictionary in Config (for simple regex patterns)**

**When to use**: Small, well-defined code sets where patterns are known.

```yaml
  {domain}codes:
    label: "{Description} Codes for {Purpose}"
    dictionary:                           # Key-value pairs of categories and patterns
      category1: "{regex_pattern_1}"
      category2: "{regex_pattern_2}"
    listIndex: "codes"                    # Field name in output containing the pattern
    sourceField: "{field_to_search}"      # Field in source to match against
    complete: True                        # True = exact match, False = partial
```

---

#### **Method 3: Join-based (for code lookups)**

**When to use**: Joining against an existing reference/dictionary table.

```yaml
  {domain}codes:
    label: "{Description} Codes"
    indexFields:                          # Fields that uniquely identify codes
      - "{code_field}_standard_id"
      - "{code_field}_standard_codingSystemId"
      - "{code_field}_standard_primaryDisplay"
```

**Examples by Domain**:

```yaml
  # Condition codes
  targetConditionCodes:
    label: "ICD-10 codes for target conditions"
    dictionary:
      diabetes_type1: "E10\\.[0-9]"
      diabetes_type2: "E11\\.[0-9]"
    listIndex: "codes"
    sourceField: "conditioncode_standard_id"
    complete: True

  # Medication codes
  targetMedicationCodes:
    label: "Medication codes for study drugs"
    dictionary:
      metformin: "d00440"
      insulin: "d00021|d00022"
    listIndex: "codes"
    sourceField: "drugcode_standard_id"
    complete: True

  # Lab codes
  targetLabCodes:
    label: "LOINC codes for lab tests"
    dictionary:
      HbA1c: "4548-4|17856-6"
      glucose: "2339-0|2345-7"
    listIndex: "codes"
    sourceField: "labcode_standard_id"
    complete: True
```

### 3.3 Pattern: Verified Codes Table (output of create_extract)

**Purpose**: Stores the matched codes after running create_extract.

```yaml
  {domain}codesVerified:
    label: "Verified {Domain} Codes"
    groupName: "regexgroup"               # Column indicating which search term matched
    indexFields:                          # Fields from source dictionary
      - "{code_field}_standard_primaryDisplay"
      - "{code_field}_standard_id"
      - "{code_field}_standard_codingSystemId"
    datefield: null                       # Usually null for code lists
```

### 3.4 Pattern: Entity Encounter Table (output of entityExtract)

**Purpose**: Stores actual patient records matching the codes.

```yaml
  {domain}Encounters:
    label: "{Domain} Records from the {source} table"
    datefield: "datetime{Domain}"         # Primary date field
    histStart: "{history_start}"          # Date filter: start
    histEnd: "{history_stop}"             # Date filter: end
    indexFields:                          # Join keys
      - "personid"
      - "tenant"
    retained_fields:                      # Fields to keep in output
      - "encounterid"
      - "{domain}id"
      - "{code_field}_standard_id"
      - "{code_field}_standard_primaryDisplay"
      - "datetime{Domain}"
      - "tenant"
```

**Domain-Specific Examples**:

```yaml
  # Condition encounters
  conditionEncounters:
    label: "Condition Records for Cohort"
    datefield: "datetimeCondition"
    histStart: "${historyStart}"
    histEnd: "${historyStop}"
    indexFields: ["personid", "tenant"]
    retained_fields:
      - "encounterid"
      - "conditionid"
      - "conditioncode_standard_id"
      - "conditioncode_standard_primaryDisplay"
      - "classification_standard_primaryDisplay"
      - "datetimeCondition"
      - "tenant"

  # Medication encounters
  medicationEncounters:
    label: "Medication Records for Cohort"
    datefield: "datetimeMed"
    histStart: "${historyStart}"
    histEnd: "${historyStop}"
    indexFields: ["personid", "tenant"]
    retained_fields:
      - "medicationid"
      - "encounterid"
      - "drugcode_standard_id"
      - "drugcode_standard_primaryDisplay"
      - "startdate"
      - "stopdate"
      - "datetimeMed"
      - "tenant"

  # Lab encounters
  labEncounters:
    label: "Lab Results for Cohort"
    datefield: "datetimeLab"
    histStart: "${historyStart}"
    histEnd: "${historyStop}"
    indexFields: ["personid", "tenant"]
    retained_fields:
      - "labid"
      - "encounterid"
      - "labcode_standard_id"
      - "labcode_standard_primaryDisplay"
      - "typedvalue_numericValue_value"
      - "typedvalue_unitOfMeasure_standard_primaryDisplay"
      - "datetimeLab"
      - "tenant"
```

### 3.5 Pattern: Index Table (output of write_index_table)

**Purpose**: Patient-level summary with first/last dates, therapy courses.

```yaml
  {domain}Index:
    label: "{Domain} Patient-Level Summary"
    indexFields:                          # Patient identifier
      - "personid"
      - "tenant"
    datefieldPrimary: "datetime{Domain}"  # Field for first instance
    datefieldStop: "datetime{Domain}"     # Field for last instance (can differ)
    code: "{abbrev}"                      # Creates index_{abbrev}, last_{abbrev} columns
    retained_fields:                      # Additional fields to keep
      - "{code_field}_standard_primaryDisplay"
      - "tenant"
    sort_fields:                          # Sort order for determining first
      - "datetime{Domain}"
    max_gap: 365                          # Gap threshold for therapy courses (days)
    histStart: "${historyStart}"
    histEnd: "${historyStop}"
```

**Complete Example**:

```yaml
  conditionIndex:
    label: "Condition Patient-Level Summary"
    indexFields: ["personid", "tenant"]
    datefieldPrimary: "datetimeCondition"
    datefieldStop: "datetimeCondition"
    code: "COND"
    retained_fields:
      - "conditioncode_standard_primaryDisplay"
      - "classification_standard_primaryDisplay"
      - "tenant"
    sort_fields: ["datetimeCondition"]
    max_gap: 365
    histStart: "${historyStart}"
    histEnd: "${historyStop}"
```

### 3.6 Pattern: Follow-up Metrics Table

**Purpose**: Capture observation period and follow-up time.

```yaml
  follow{Domain}:
    label: "Follow-up metrics from {Domain}"
    indexFields: ["personid", "tenant"]
    retained_fields: []
    datefieldPrimary: "datetime{Domain}"
    datefieldStop: "datetime{Domain}"
    code: "{abbrev}"
    sort_fields: ["datetime{Domain}"]
    max_gap: 1096                         # ~3 years
    histStart: "${historyStart}"
    histEnd: "${historyStop}"
```

### 3.7 Pattern: Demographics Table

**Purpose**: Patient demographic characteristics.

```yaml
  demographics:
    label: "Patient Demographics"
    datefield: "birthdate"
    index: ["personid", "tenant"]
    indexFields: ["personid", "tenant"]

  death:
    label: "Patient Death Records"
    indexFields: ["personid", "tenant"]
```

### 3.8 Pattern: Analytic Dataset (ADS)

**Purpose**: Final analysis-ready dataset combining multiple sources.

```yaml
  ads:
    label: "Analytic Dataset - Primary Analysis"
    indexFields: ["personid", "tenant"]
    datefield: "index_date"
    retained_fields:
      - "age"
      - "gender"
      - "race"
      - "index_date"
      - "follow_up_days"
      - "outcome_flag"
      # ... all analysis variables
```

---

## 4. Clinical Code Discovery Workflow

### 4.1 The Problem with Theoretical Code Lists

A common mistake is to start with "theoretical" code lists from clinical guidelines or literature. These fail because:
- **Codes may not exist in your data** - Different health systems use different coding practices
- **Code variations** - Multiple codes can represent the same concept
- **Unknown frequency** - You don't know if a code is common (100,000 patients) or rare (3 patients)
- **Missing context** - No visibility into what the data actually contains

### 4.2 Data-First Code Discovery

The preferred workflow uses actual data exploration:

```
┌─────────────────────────────────────────────────────────────────────────┐
│  1. EXPLORE: Clinician uses exploration app to search source data       │
│     - Search by keyword, regex, or browse code hierarchies             │
│     - See patient counts for each code                                  │
│     - Review code descriptions and distributions                        │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  2. SELECT: Clinician marks relevant codes for the study                │
│     - Group codes by clinical category                                  │
│     - Add notes/rationale                                               │
│     - Verify codes match clinical intent                                │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  3. EXPORT: Save verified codes to CSV                                  │
│     - File: {tablename}_{disease}_{schemaTag}.csv                      │
│     - Location: {dataLoc} directory                                     │
│     - Includes: code, group, description, patient_count                │
└──────────────────────────────┬──────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  4. LOAD: Notebook loads CSV and proceeds with extraction               │
│     - e.{tablename}.load_csv_as_df()                                   │
│     - Codes are now in Spark, ready for entityExtract                  │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.3 CSV File Specifications

**File Naming Convention**:
```
{table_key}_{disease}_{schemaTag}.csv

Examples:
- comorbidity_procedure_Codes_SCD_RWD.csv      # Procedure codes for SCD study
- target_condition_Codes_DM2_RWD.csv           # Condition codes for diabetes study
- hba1c_lab_Codes_DM2_RWD.csv                  # Lab codes for diabetes study
```

**Required Structure**:
```csv
{GroupColumn},{CodeColumn},{DescriptionColumn}[,{PatientCountColumn}]
```

**Example - Procedure Codes**:
```csv
Procedure,procedurecode_standard_id,Description
Electrocardiogram (EKG/ECG),93000,"Electrocardiogram, routine ECG with at least 12 leads"
Electrocardiogram (EKG/ECG),93010,"Electrocardiogram, interpretation and report"
Echocardiogram,93306,"Echocardiography, transthoracic, complete study"
```

**Example - Condition Codes with Counts**:
```csv
Category,conditioncode_standard_id,Description,N
Sickle Cell Crisis,D57.00,"Hb-SS disease with crisis, unspecified",12453
Sickle Cell Crisis,D57.01,"Hb-SS disease with acute chest syndrome",3421
Vaso-occlusive Pain,D57.09,"Hb-SS disease with crisis with other complication",8932
```

**Example - Medication Codes**:
```csv
MedicationClass,drugcode_standard_id,GenericName,BrandName,N
Hydroxyurea,d00088,hydroxyurea,Droxia,4521
Hydroxyurea,d00088,hydroxyurea,Hydrea,4521
Blood Products,d99999,packed red blood cells,PRBC,2341
```

### 4.4 Config Entry for CSV-Loaded Codes

```yaml
projectTables:
  # The table key determines the CSV filename automatically
  # Pattern: {dataLoc}{table_key}_{disease}_{schemaTag}.csv
  
  comorbidity_procedure_Codes:
    label: "Comorbidity Procedure Codes - Clinician Verified"
    # csv: path is AUTO-GENERATED - only specify if overriding default
    # Index fields must match CSV column names
    indexFields:
      - "procedurecode_standard_id"     # The actual code
      - "Procedure"                     # Grouping category
      - "Description"                   # Human-readable description
```

**Convention**: If your CSV file follows the naming pattern `{table_key}_{disease}_{schemaTag}.csv` and is in the `dataLoc` directory, you don't need to specify the `csv` property at all.

### 4.5 Notebook Pattern for CSV Loading

```python
# ============================================================================
# LOAD CLINICIAN-VERIFIED CODES FROM CSV
# ============================================================================

# Load the CSV file into a Spark DataFrame and save to schema
e.comorbidity_procedure_Codes.load_csv_as_df()

# Verify the load
print(f"Loaded {e.comorbidity_procedure_Codes.df.count()} codes")
e.comorbidity_procedure_Codes.df.show(10, truncate=False)

# Check code distribution by category
e.comorbidity_procedure_Codes.tabulate(by='Procedure')

# Proceed with extraction using these codes
e.procedureEncounters.entityExtract(
    elementList=e.comorbidity_procedure_Codes,
    entitySource=r.procedureSource,
    cacheResult=True
)
```

### 4.6 When to Use Each Code Input Method

| Method | Use When | Example |
|--------|----------|---------|
| **CSV File** | Codes selected by clinician from actual data | Comorbidity procedures, target conditions |
| **Config Dictionary** | Simple regex patterns, well-known codes | ICD-10 ranges like `E11\.[0-9]` for diabetes |
| **Reference Table Join** | Using existing dictionary tables | Joining against LOINC reference |

### 4.7 Hybrid Approach

Often you'll combine methods:

```yaml
projectTables:
  # Method 1: CSV for complex, clinician-verified codes
  # csv path auto-generated: {dataLoc}comorbidity_procedure_Codes_{disease}_{schemaTag}.csv
  comorbidity_procedure_Codes:
    label: "Comorbidity Procedures - Clinician Verified"
    indexFields: ["procedurecode_standard_id", "Procedure"]

  # Method 2: Dictionary for simple, well-defined patterns
  sickle_cell_dx_codes:
    label: "Sickle Cell Diagnosis Codes"
    dictionary:
      scd_crisis: "D57\\.[0-4]"
      scd_trait: "D57\\.3"
    listIndex: "codes"
    sourceField: "conditioncode_standard_id"
    complete: True

  # Method 3: Join against reference for standardized codes
  loinc_lab_codes:
    label: "Lab Codes from LOINC Reference"
    indexFields:
      - "labcode_standard_id"
      - "labcode_standard_codingSystemId"
```

---

## 5. Extract Workflow Patterns

### 5.1 Three-Step Extraction Pattern

Every data domain follows this pattern:

```
Step 1: create_extract     → Identify codes of interest
Step 2: entityExtract      → Extract patient records matching codes
Step 3: write_index_table  → Create patient-level summary
```

### 5.2 Configuration Linkage

The tables link together via shared field names:

```yaml
# Step 1 output → Step 2 input
{domain}codes.indexFields → elementList for entityExtract

# Step 2 output → Step 3 input  
{domain}Encounters → inTable for write_index_table

# Step 3 output → Final analysis
{domain}Index → joins to create ADS
```

### 5.3 Notebook Cell Pattern for Each Domain

```python
# ============================================================================
# {DOMAIN} EXTRACTION
# ============================================================================

# Step 1: Identify codes
e.{domain}codes.create_extract(
    elementList=d.{source_dictionary},           # Source dictionary table
    elementListSource=d.{source_dictionary}.df,
    find_method='regex'                          # or 'exact'
)
e.{domain}codes.attrition()

# Step 2: Extract encounters
e.{domain}Encounters.entityExtract(
    elementList=e.{domain}codesVerified,         # From Step 1
    entitySource=r.{sourceTable}.df,             # Source data table
    cacheResult=True
)
e.{domain}Encounters.attrition()

# Step 3: Create index table
e.{domain}Index.write_index_table(
    inTable=e.{domain}Encounters                 # From Step 2
)
e.{domain}Index.attrition()

# Verify results
e.{domain}Index.tabulate(by='{code_field}_standard_primaryDisplay')
```

---

## 6. Notebook Generation Patterns

### 6.1 Standard Notebook Structure

```
Notebook: 0XX-{Domain}-Extraction.ipynb

1. Setup Cell (imports and configuration)
2. Resource Initialization
3. Domain-Specific Extraction (Steps 1-3)
4. Validation and Quality Checks
5. Export/Save Results
```

### 6.2 Setup Cell Template

```python
# ============================================================================
# {PROJECT_NAME}: {NOTEBOOK_TITLE}
# ============================================================================
# Purpose: {description}
# Author: {author}
# Date: {date}
# ============================================================================

# Imports
from lhn.resource import Resources
from lhn.extract import Extract
from lhn.header import spark, F
from pathlib import Path
import pandas as pd

# Configuration
project = '{project_name}'
basePath = Path.home() / 'work/Users/hnelson3'

# Initialize resources
resource = Resources(
    project=project,
    spark=spark,
    basePath=basePath,
    process_all=True
)

# Get local references
local_vars = resource.load_into_local(schemakey='projectSchema', extractName='e')
locals().update(local_vars)

# Shortcuts
r = resource.r    # RWD source tables (DataFrames)
d = resource.d    # Dictionary/reference tables
db = resource.db  # Project output tables
```

### 6.3 Domain Extraction Notebook Template

For each domain (conditions, medications, labs, procedures, vitals):

```python
# ============================================================================
# {DOMAIN} EXTRACTION
# ============================================================================

# -----------------------------------------------------------------------------
# Step 1: Identify {domain} codes of interest
# -----------------------------------------------------------------------------
print("Step 1: Identifying {domain} codes...")

e.{domain}codes.create_extract(
    elementList=d.{reference_table},
    elementListSource=d.{reference_table}.df,
    find_method='regex'
)

print(f"Codes identified: {e.{domain}codes.df.count()}")
e.{domain}codes.attrition()

# Verify codes found
e.{domain}codes.tabulate(by='{code_field}_standard_id', obs=20)

# -----------------------------------------------------------------------------
# Step 2: Extract {domain} encounters
# -----------------------------------------------------------------------------
print("Step 2: Extracting {domain} encounters...")

e.{domain}Encounters.entityExtract(
    elementList=e.{domain}codesVerified,
    entitySource=r.{sourceTable},
    cacheResult=True
)

print(f"Encounters extracted: {e.{domain}Encounters.df.count()}")
e.{domain}Encounters.attrition()

# -----------------------------------------------------------------------------
# Step 3: Create patient-level index
# -----------------------------------------------------------------------------
print("Step 3: Creating patient-level index...")

e.{domain}Index.write_index_table(
    inTable=e.{domain}Encounters
)

print(f"Patients indexed: {e.{domain}Index.df.select('personid').distinct().count()}")
e.{domain}Index.attrition()

# -----------------------------------------------------------------------------
# Quality Checks
# -----------------------------------------------------------------------------
print("Quality Checks:")

# Check date ranges
e.{domain}Index.df.select(
    F.min('index_{abbrev}').alias('earliest'),
    F.max('last_{abbrev}').alias('latest')
).show()

# Check distribution
e.{domain}Index.tabulate(by='{code_field}_standard_primaryDisplay', obs=20)
```

### 6.4 Demographics Notebook Template

```python
# ============================================================================
# DEMOGRAPHICS EXTRACTION
# ============================================================================

# Get demographics from preferred_demographics table
demographics = r.demoPreferred.df.select(
    'personid',
    'tenant',
    'gender',
    'race', 
    'ethnicity',
    'birthdate',
    'state',
    'zip'
)

# Join with cohort
cohort_demo = e.persontenant.df.join(
    demographics,
    on=['personid', 'tenant'],
    how='left'
)

# Calculate age at index
cohort_demo = cohort_demo.withColumn(
    'age_at_index',
    F.floor(F.datediff(F.col('index_date'), F.col('birthdate')) / 365.25)
)

# Save demographics table
from lhn.spark_utils import writeTable
writeTable(cohort_demo, e.demographics.location, description=e.demographics.label)

# Verify
e.demographics.attrition()
e.demographics.tabulate(by='gender')
e.demographics.tabulate(by='race')
```

### 6.5 Analytic Dataset (ADS) Assembly Template

```python
# ============================================================================
# ANALYTIC DATASET ASSEMBLY
# ============================================================================

# Start with cohort index
ads = e.cohortIndex.df.select(
    'personid',
    'tenant',
    F.col('index_COND').alias('index_date'),
    F.col('last_COND').alias('last_condition_date')
)

# Join demographics
ads = ads.join(
    e.demographics.df.select(
        'personid', 'tenant', 'age_at_index', 'gender', 'race', 'ethnicity'
    ),
    on=['personid', 'tenant'],
    how='left'
)

# Join medication summary
ads = ads.join(
    e.medicationIndex.df.select(
        'personid', 'tenant',
        F.col('index_MED').alias('first_med_date'),
        'drugcode_standard_primaryDisplay'
    ),
    on=['personid', 'tenant'],
    how='left'
)

# Join lab summary
ads = ads.join(
    e.labIndex.df.select(
        'personid', 'tenant',
        'typedvalue_numericValue_value'
    ),
    on=['personid', 'tenant'],
    how='left'
)

# Calculate follow-up time
ads = ads.withColumn(
    'follow_up_days',
    F.datediff(F.col('last_condition_date'), F.col('index_date'))
)

# Add outcome flags
ads = ads.withColumn(
    'had_outcome',
    F.when(F.col('outcome_date').isNotNull(), True).otherwise(False)
)

# Save ADS
writeTable(ads, e.ads.location, description=e.ads.label)

# Final verification
e.ads.attrition()
print(f"Final ADS: {ads.count()} rows, {ads.select('personid').distinct().count()} patients")
```

---

## 7. Complete Worked Example

### 7.1 Project Definition: Type 2 Diabetes Study

```yaml
PROJECT_DEFINITION:
  project_name: "DiabetesT2Study"
  disease_abbreviation: "DM2"
  investigators:
    - "Jane Smith MD"
    - "John Doe PhD"
  
  source_schema: "iuhealth_ed_data_cohort_202306"
  output_schema: "diabetes_t2_study_rwd"
  schema_structure: "RWD"
  
  cohort_criteria:
    condition_codes:
      - "E11\\.[0-9]"      # Type 2 diabetes
    code_system: "ICD10CM"
    inclusion_logic: "Patients with at least one E11.x diagnosis"
    exclusion_logic: "Exclude Type 1 diabetes (E10.x)"
  
  study_dates:
    history_start: "2015-01-01"
    history_stop: "${today}"
  
  required_domains:
    - conditions
    - encounters
    - demographics
    - labs
    - medications
  
  outcomes:
    - name: "hospitalization"
      definition: "Inpatient encounter after index"
    - name: "HbA1c_control"
      definition: "HbA1c < 7.0% at 12 months"
  
  code_sets:
    - name: "diabetes_medications"
      codes:
        metformin: "d00440"
        sulfonylureas: "d00234|d00326"
        insulin: "d00021|d00022|d00023"
      code_system: "MULTUM"
    - name: "hba1c_loinc"
      codes:
        HbA1c: "4548-4|17856-6"
      code_system: "LOINC"
```

### 7.2 Generated 000-config.yaml

```yaml
# ============================================================================
# PROJECT CONFIGURATION: DiabetesT2Study
# ============================================================================
# Investigators: Jane Smith MD, John Doe PhD
# Created: 2025-01-19
# Purpose: Type 2 Diabetes outcomes study
# ============================================================================

project: DiabetesT2Study
investigators: |
  Jane Smith MD
  John Doe PhD
systemuser: hnelson3

# ============================================================================
# STUDY PARAMETERS
# ============================================================================
startDate: "2015-01-01"
stopDate: ${today}
historyStart: "2015-01-01"
historyStop: ${today}

disease: "DM2"
schemaTag: "RWD"
gap: 90

# ============================================================================
# SCHEMA MAPPINGS
# ============================================================================
schemas:
  RWDSchema: iuhealth_ed_data_cohort_202306
  projectSchema: diabetes_t2_study_rwd
  dictrwdSchema: datadictrwd

# ============================================================================
# COHORT DEFINITION
# ============================================================================
search_string_dm2: "E11\\.[0-9]"
search_string_dm1_exclusion: "E10\\.[0-9]"

# ============================================================================
# PROJECT TABLES
# ============================================================================
projectTables:

  # --- COHORT IDENTIFICATION ---
  persontenant:
    label: "Person and Tenant ID for DM2 cohort"
    indexFields: ["personid", "tenant"]
    partitionBy: "tenant"

  dm2codes:
    label: "ICD-10 codes for Type 2 Diabetes"
    dictionary:
      dm2: "${search_string_dm2}"
    listIndex: "codes"
    sourceField: "conditioncode_standard_id"
    complete: True

  dm2codesVerified:
    label: "Verified DM2 Condition Codes"
    groupName: "regexgroup"
    indexFields:
      - "conditioncode_standard_primaryDisplay"
      - "conditioncode_standard_id"
      - "conditioncode_standard_codingSystemId"
    datefield: null

  dm2ConditionEncounter:
    label: "DM2 Condition Records"
    datefield: "datetimeCondition"
    histStart: "${historyStart}"
    histEnd: "${historyStop}"
    indexFields: ["personid", "tenant"]
    retained_fields:
      - "encounterid"
      - "conditionid"
      - "conditioncode_standard_id"
      - "conditioncode_standard_primaryDisplay"
      - "classification_standard_primaryDisplay"
      - "datetimeCondition"
      - "tenant"

  dm2Index:
    label: "DM2 Patient-Level Index"
    indexFields: ["personid", "tenant"]
    datefieldPrimary: "datetimeCondition"
    datefieldStop: "datetimeCondition"
    code: "DM2"
    retained_fields:
      - "conditioncode_standard_primaryDisplay"
      - "tenant"
    sort_fields: ["datetimeCondition"]
    max_gap: 365
    histStart: "${historyStart}"
    histEnd: "${historyStop}"

  # --- DEMOGRAPHICS ---
  demographics:
    label: "Patient Demographics"
    datefield: "birthdate"
    index: ["personid", "tenant"]
    indexFields: ["personid", "tenant"]

  death:
    label: "Patient Death Records"
    indexFields: ["personid", "tenant"]

  # --- MEDICATIONS ---
  diabetesMedCodes:
    label: "Diabetes Medication Codes"
    dictionary:
      metformin: "d00440"
      sulfonylureas: "d00234|d00326"
      insulin: "d00021|d00022|d00023"
    listIndex: "codes"
    sourceField: "drugcode_standard_id"
    complete: True
    indexFields:
      - "drugcode_standard_id"
      - "drugcode_standard_codingSystemId"
      - "drugcode_standard_primaryDisplay"

  diabetesMedCodesVerified:
    label: "Verified Diabetes Medication Codes"
    groupName: "medgroup"
    indexFields:
      - "drugcode_standard_primaryDisplay"
      - "drugcode_standard_id"
      - "drugcode_standard_codingSystemId"
    datefield: null

  diabetesMedEncounters:
    label: "Diabetes Medication Records"
    datefield: "datetimeMed"
    histStart: "${historyStart}"
    histEnd: "${historyStop}"
    indexFields: ["personid", "tenant"]
    retained_fields:
      - "medicationid"
      - "encounterid"
      - "drugcode_standard_id"
      - "drugcode_standard_primaryDisplay"
      - "startdate"
      - "stopdate"
      - "datetimeMed"
      - "tenant"

  diabetesMedIndex:
    label: "Diabetes Medication Patient-Level Summary"
    indexFields: ["personid", "tenant"]
    datefieldPrimary: "datetimeMed"
    datefieldStop: "stopdate"
    code: "MED"
    retained_fields:
      - "drugcode_standard_primaryDisplay"
      - "startdate"
      - "stopdate"
    sort_fields: ["datetimeMed"]
    max_gap: 90
    histStart: "${historyStart}"
    histEnd: "${historyStop}"

  # --- LABS ---
  hba1cCodes:
    label: "HbA1c LOINC Codes"
    dictionary:
      HbA1c: "4548-4|17856-6"
    listIndex: "codes"
    sourceField: "labcode_standard_id"
    complete: True
    indexFields:
      - "labcode_standard_id"
      - "labcode_standard_codingSystemId"
      - "labcode_standard_primaryDisplay"

  hba1cCodesVerified:
    label: "Verified HbA1c Codes"
    groupName: "labgroup"
    indexFields:
      - "labcode_standard_primaryDisplay"
      - "labcode_standard_id"
      - "labcode_standard_codingSystemId"
    datefield: null

  hba1cEncounters:
    label: "HbA1c Lab Results"
    datefield: "datetimeLab"
    histStart: "${historyStart}"
    histEnd: "${historyStop}"
    indexFields: ["personid", "tenant"]
    retained_fields:
      - "labid"
      - "encounterid"
      - "labcode_standard_id"
      - "labcode_standard_primaryDisplay"
      - "typedvalue_numericValue_value"
      - "typedvalue_unitOfMeasure_standard_primaryDisplay"
      - "datetimeLab"
      - "tenant"

  hba1cIndex:
    label: "HbA1c Patient-Level Summary"
    indexFields: ["personid", "tenant"]
    datefieldPrimary: "datetimeLab"
    datefieldStop: "datetimeLab"
    code: "HBA1C"
    retained_fields:
      - "labcode_standard_primaryDisplay"
      - "typedvalue_numericValue_value"
    sort_fields: ["datetimeLab"]
    max_gap: 365
    histStart: "${historyStart}"
    histEnd: "${historyStop}"

  # --- FOLLOW-UP ---
  followCondition:
    label: "Follow-up from Conditions"
    indexFields: ["personid", "tenant"]
    retained_fields: []
    datefieldPrimary: "datetimeCondition"
    datefieldStop: "datetimeCondition"
    code: "cond"
    sort_fields: ["datetimeCondition"]
    max_gap: 1096
    histStart: "${historyStart}"
    histEnd: "${historyStop}"

  followEnc:
    label: "Follow-up from Encounters"
    indexFields: ["personid", "tenant"]
    retained_fields: []
    datefieldPrimary: "datetimeEnc"
    datefieldStop: "dischargedate"
    code: "enc"
    sort_fields: ["datetimeEnc"]
    max_gap: 1096
    histStart: "${historyStart}"
    histEnd: "${historyStop}"

  # --- ANALYTIC DATASET ---
  ads:
    label: "Analytic Dataset - DM2 Study"
    indexFields: ["personid", "tenant"]
    datefield: "index_date"
```

### 7.3 Generated Notebook: 010-Cohort-Identification.ipynb

```python
# Cell 1: Setup
# ============================================================================
# DiabetesT2Study: Cohort Identification
# ============================================================================
# Purpose: Identify Type 2 Diabetes patients and create cohort index
# ============================================================================

from lhn.resource import Resources
from lhn.extract import Extract
from lhn.header import spark, F
from pathlib import Path

project = 'DiabetesT2Study'
basePath = Path.home() / 'work/Users/hnelson3'

resource = Resources(
    project=project,
    spark=spark,
    basePath=basePath,
    process_all=True
)

local_vars = resource.load_into_local(schemakey='projectSchema', extractName='e')
locals().update(local_vars)

r = resource.r
d = resource.d
db = resource.db

# Cell 2: Identify DM2 Codes
# ============================================================================
# Step 1: Identify DM2 condition codes
# ============================================================================

e.dm2codes.create_extract(
    elementList=d.condition_codes,
    elementListSource=d.condition_codes.df,
    find_method='regex'
)

print(f"DM2 codes identified: {e.dm2codes.df.count()}")
e.dm2codes.attrition()
e.dm2codes.tabulate(by='conditioncode_standard_id', obs=20)

# Cell 3: Extract DM2 Condition Encounters
# ============================================================================
# Step 2: Extract DM2 condition encounters
# ============================================================================

e.dm2ConditionEncounter.entityExtract(
    elementList=e.dm2codesVerified,
    entitySource=r.conditionSource,
    cacheResult=True
)

print(f"DM2 encounters: {e.dm2ConditionEncounter.df.count()}")
e.dm2ConditionEncounter.attrition()

# Cell 4: Create Patient Index
# ============================================================================
# Step 3: Create patient-level index
# ============================================================================

e.dm2Index.write_index_table(
    inTable=e.dm2ConditionEncounter
)

print(f"DM2 patients: {e.dm2Index.df.select('personid').distinct().count()}")
e.dm2Index.attrition()

# Cell 5: Create Cohort Table
# ============================================================================
# Create persontenant cohort table
# ============================================================================

from lhn.spark_utils import writeTable

cohort = e.dm2Index.df.select('personid', 'tenant').distinct()
writeTable(cohort, e.persontenant.location, description=e.persontenant.label)

print(f"Cohort created: {cohort.count()} patients")

# Cell 6: Quality Checks
# ============================================================================
# Quality Checks
# ============================================================================

# Date range
e.dm2Index.df.select(
    F.min('index_DM2').alias('earliest_dx'),
    F.max('last_DM2').alias('latest_dx')
).show()

# Condition distribution
e.dm2Index.tabulate(by='conditioncode_standard_primaryDisplay', obs=20)
```

---

## 8. Validation Checklist

### 8.1 Configuration Validation

- [ ] All `${variable}` references resolve to defined values
- [ ] `schemas` dictionary contains all referenced schema types
- [ ] Every `projectTables` entry has a `label`
- [ ] Code search tables have matching verified tables
- [ ] Entity tables have matching index tables
- [ ] `indexFields` include `personid` and `tenant`
- [ ] Date fields match actual column names in source data

### 8.2 Notebook Validation

- [ ] Resources initialized with `process_all=True`
- [ ] `load_into_local()` called before using `e` object
- [ ] Each domain follows 3-step pattern (create_extract → entityExtract → write_index_table)
- [ ] `attrition()` called after each step
- [ ] Final tables written with `writeTable()`

### 8.3 Data Quality Checks

- [ ] Patient counts decrease appropriately through pipeline
- [ ] Date ranges are within expected study period
- [ ] No unexpected NULL values in key fields
- [ ] Code distributions match clinical expectations

---

## Appendix A: Field Name Reference

### Condition Fields
- `conditioncode_standard_id` - ICD-10 or other code
- `conditioncode_standard_primaryDisplay` - Code description
- `conditioncode_standard_codingSystemId` - Code system identifier
- `datetimeCondition` - Condition date/time
- `classification_standard_primaryDisplay` - Diagnosis type (e.g., "Final diagnosis")

### Medication Fields
- `drugcode_standard_id` - Drug code (MULTUM, NDC, etc.)
- `drugcode_standard_primaryDisplay` - Drug name
- `datetimeMed` - Medication date
- `startdate` - Medication start
- `stopdate` - Medication stop

### Lab Fields
- `labcode_standard_id` - LOINC code
- `labcode_standard_primaryDisplay` - Test name
- `typedvalue_numericValue_value` - Numeric result
- `typedvalue_unitOfMeasure_standard_primaryDisplay` - Units
- `datetimeLab` - Lab date

### Encounter Fields
- `encounterid` - Encounter identifier
- `datetimeEnc` - Encounter date
- `dischargedate` - Discharge date
- `type_standard_primaryDisplay` - Encounter type

### Common Fields
- `personid` - Patient identifier
- `tenant` - Data tenant/source

---

## Appendix B: Error Messages and Solutions

| Error | Cause | Solution |
|-------|-------|----------|
| `KeyError: 'projectTables'` | Config not loaded | Ensure `process_all=True` |
| `Table not found in schema` | Table doesn't exist yet | Run extraction workflow first |
| `No matching codes found` | Regex pattern issue | Test pattern against actual data |
| `Empty DataFrame` | No data matches criteria | Check date ranges, code values |
| `Column not found` | Wrong field name | Verify against `r.{table}.df.columns` |
