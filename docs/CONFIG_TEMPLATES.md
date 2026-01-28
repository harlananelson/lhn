# LHN Configuration Templates

This document provides annotated configuration file templates for lhn projects.
Three configuration files work together in a hierarchy (later overrides earlier):

1. `config-global.yaml` - Organization-wide settings (shared across projects)
2. `config-RWD.yaml` (or schema-specific) - Data source settings
3. `000-config.yaml` - Project-specific settings

---

## Template: 000-config.yaml (Project Config)

This is the primary project configuration file. Place it in your project directory.

```yaml
# =============================================================================
# PROJECT CONFIGURATION
# File: 000-config.yaml
# Purpose: Project-specific settings that override global/schema configs
# =============================================================================

# -----------------------------------------------------------------------------
# PROJECT IDENTIFICATION
# -----------------------------------------------------------------------------

# Project name - used in file paths and table names
project: "DiabetesMedStudy"

# Disease/condition tag - appended to table names for uniqueness
# Example: cohort_diabetes_rwd (table name pattern)
disease: "diabetes"

# Schema tag - identifies data source version
# Example: "RWD" for Real World Data, "Claims" for claims data
schemaTag: "RWD"

# -----------------------------------------------------------------------------
# SCHEMA MAPPINGS
# Map logical schema names to actual database names
# -----------------------------------------------------------------------------

schemas:
  # Source data schema (read-only, contains raw healthcare data)
  RWDSchema: "iuhealth_ed_data_cohort_202306"

  # Project output schema (read-write, where extracts are saved)
  projectSchema: "diabetes_study"

  # Optional: Additional schemas for comparison or reference
  # sstudySchema: "another_study_schema"
  # SCDSchema: "sickle_cell_schema"

# -----------------------------------------------------------------------------
# FILE PATHS
# -----------------------------------------------------------------------------

# Local directory for CSV exports
dataLoc: "/home/hnelson3/data/exports/"

# HDFS directory for Parquet files
parquetLoc: "hdfs:///user/hnelson3/parquet/"

# -----------------------------------------------------------------------------
# DATE PARAMETERS
# Used for filtering and analysis windows
# -----------------------------------------------------------------------------

# Study observation period
historyStart: "2015-01-01"
historyStop: "{today}"  # Template variable replaced at runtime

# Specific analysis dates
startDate: "2020-01-01"
stopDate: "{today}"

# Gap threshold for therapy continuity (days)
gap: 90

# Days before index to look for prior events
serviceToFirstDx: 60

# -----------------------------------------------------------------------------
# PROJECT TABLES (Extract Definitions)
# These define the tables your pipeline will create
# -----------------------------------------------------------------------------

projectTables:

  # --- COHORT DEFINITION TABLES ---

  # Search patterns for finding diagnosis codes
  diabetes_icd_list:
    label: "ICD codes for Type 2 Diabetes"
    # Data source: CSV file with search patterns
    csv: "{dataLoc}diabetes_icd_patterns.csv"
    indexFields: [pattern]  # Primary key field(s)

  # Extracted diagnosis codes from search
  diabetes_codes:
    label: "Matched diabetes diagnosis codes"
    indexFields: [conditioncode_standard_id]

  # Patient condition records with diabetes
  diabetes_conditions:
    label: "Diabetes condition records"
    indexFields: [personid, encounterid]
    datefieldPrimary: "servicedate"
    retained_fields: [conditioncode_standard_id, conditioncode_standard_display]

  # Patient-level diabetes index
  diabetes_index:
    label: "Patient diabetes index (first/last dates)"
    indexFields: [personid, tenant]
    datefieldPrimary: "index_diabetes"
    code: "diabetes"
    histStart: "{historyStart}"
    histEnd: "{historyStop}"

  # --- MEDICATION TABLES ---

  metformin_codes:
    label: "Metformin drug codes"
    indexFields: [drugcode]

  metformin_meds:
    label: "Metformin medication records"
    indexFields: [personid, encounterid]
    datefieldPrimary: "administrationdate"
    datefieldStop: "administrationenddate"

  metformin_index:
    label: "Patient metformin usage index"
    indexFields: [personid, tenant]
    datefieldPrimary: "index_metformin"
    datefieldStop: "last_metformin"
    code: "metformin"
    max_gap: 90  # Days gap to define new therapy course
    sort_fields: [administrationdate]

  # --- OUTPUT/ANALYSIS TABLES ---

  study_cohort:
    label: "Final study cohort"
    indexFields: [personid, tenant]
    partitionBy: "tenant"

  demographics:
    label: "Cohort demographics"
    indexFields: [personid, tenant]
    partitionBy: "tenant"

  outcomes:
    label: "Study outcomes"
    indexFields: [personid, tenant]
    partitionBy: "tenant"

# -----------------------------------------------------------------------------
# METADATA
# -----------------------------------------------------------------------------

investigators: |
  Jane Smith, MD
  John Doe, PhD

description: |
  Analysis of metformin usage patterns in Type 2 Diabetes patients.
  Study period: 2020-2025
```

---

## Template: config-global.yaml (Global Config)

Shared across all projects in the organization. Defines how different schemas are processed.

```yaml
# =============================================================================
# GLOBAL CONFIGURATION
# File: config-global.yaml
# Purpose: Organization-wide settings shared across all projects
# Location: ~/work/Users/{username}/configuration/
# =============================================================================

# -----------------------------------------------------------------------------
# USER/SYSTEM SETTINGS
# -----------------------------------------------------------------------------

systemuser: "hnelson3"

# Default paths (can be overridden in project config)
dataLoc: "/home/{systemuser}/data/exports/"
parquetLoc: "hdfs:///user/{systemuser}/parquet/"

# -----------------------------------------------------------------------------
# SCHEMA PROCESSING CONFIGURATION
# Defines how each schema type is processed
# -----------------------------------------------------------------------------

callFunProcessDataTables:

  # --- RWD (Real World Data) Schema ---
  RWDcallFunc:
    # data_type: Key in config for table definitions (e.g., config['RWDTables'])
    data_type: 'RWDTables'

    # schema_type: Key in schemas dict (e.g., schemas['RWDSchema'])
    schema_type: 'RWDSchema'

    # type_key: Attribute name on Resources (e.g., resource.rwd)
    type_key: 'rwd'

    # property_name: Short alias (e.g., resource.r)
    property_name: 'r'

    # updateDict: If True, auto-discover tables from schema
    # If False, use table definitions from config
    updateDict: False

    # tableNameTemplate: Pattern for filtering tables (null = no filter)
    tableNameTemplate: null

  # --- Project Schema ---
  projectcallFunc:
    data_type: 'projectTables'
    schema_type: 'projectSchema'
    type_key: 'proj'
    property_name: 'db'
    updateDict: False
    tableNameTemplate: '_{disease}_{schemaTag}'

  # --- Study Schema (auto-discover tables) ---
  # Use this pattern when you want to read all tables from another schema
  sstudycallFunc:
    data_type: 'sstudyTables'
    schema_type: 'sstudySchema'
    type_key: 'ss'
    property_name: 's'
    updateDict: True  # Auto-discover all tables in schema
    tableNameTemplate: null

  # --- IUHealth Schema ---
  IUHdatacallFunc:
    data_type: 'IUHdataTables'
    schema_type: 'IUHSchema'
    type_key: 'iuhealth'
    property_name: 'iuh'
    updateDict: False
    tableNameTemplate: null

  # --- OMOP Schema ---
  omopcallFunc:
    data_type: 'omopTables'
    schema_type: 'omopSchema'
    type_key: 'omop'
    property_name: 'o'
    updateDict: True
    tableNameTemplate: null

# -----------------------------------------------------------------------------
# DEFAULT PARAMETERS
# -----------------------------------------------------------------------------

# Default person identifier field
personid: ['personid']

# Default partition field
partitionBy: 'tenant'

# Default observation limit for processing
obs: 100000000

# Whether to rerun existing tables
reRun: False
```

---

## Template: config-RWD.yaml (Schema Config)

Defines the source tables available in a specific data schema.

```yaml
# =============================================================================
# RWD SCHEMA CONFIGURATION
# File: config-RWD.yaml
# Purpose: Define source tables in the RWD (Real World Data) schema
# Location: ~/work/Users/{username}/configuration/
# =============================================================================

# -----------------------------------------------------------------------------
# RWD TABLE DEFINITIONS
# Each entry defines a source table and how to access it
# -----------------------------------------------------------------------------

RWDTables:

  # --- MORTALITY ---
  mortalitySource:
    # 'source' is the actual table name in the database
    # The key (mortalitySource) is the logical name used in code
    source: 'mortality'
    label: 'Patient mortality records'

    # inputRegex: Patterns to match column names to include
    # Use regex patterns; columns matching any pattern are included
    inputRegex:
      - "^personid"
      - "^tenant"
      - "MonthOfDeath"
      - "deceased"

    # datefield: Primary date field for this table
    datefield: "MonthOfDeath"

    # insert: PySpark transformations to apply when loading
    # These are executed as: df = df.{insert}
    insert:
      - "withColumn('dateofdeath', F.when(F.col('MonthOfDeath').isNotNull(), F.last_day('MonthOfDeath')).otherwise(F.col('MonthOfDeath')))"

  # --- ENCOUNTERS ---
  encounterSource:
    source: 'encounter'
    label: 'Patient encounters (filtered)'
    inputRegex:
      - "^personid"
      - "^encounterid"
      - "^tenant"
      - "servicedate"
      - "dischargedate"
      - "actualarrivaldate"
      - "encountertype"
      - "facilityid"
    datefield: "servicedate"

  encounterSourceAll:
    source: 'encounter'
    label: 'Patient encounters (all columns)'
    # No inputRegex = include all columns
    datefield: "servicedate"

  emergencyEncounter:
    source: 'encounter'
    label: 'Emergency department encounters'
    inputRegex:
      - "^personid"
      - "^encounterid"
      - "^tenant"
      - "servicedate"
      - "encountertype"
    datefield: "servicedate"
    # filter: Applied when loading
    filter: "encountertype = 'Emergency'"

  inpatientEncounterSource:
    source: 'encounter'
    label: 'Inpatient encounters'
    inputRegex:
      - "^personid"
      - "^encounterid"
      - "^tenant"
      - "servicedate"
      - "dischargedate"
    datefield: "servicedate"
    filter: "encountertype = 'Inpatient'"

  # --- CONDITIONS ---
  conditionSource:
    source: 'conditions'
    label: 'Patient conditions/diagnoses'
    inputRegex:
      - "^personid"
      - "^encounterid"
      - "^tenant"
      - "conditioncode"
      - "servicedate"
      - "confirmationstatus"
    datefield: "servicedate"

  conditionFinalSource:
    source: 'conditions'
    label: 'Final diagnosis conditions'
    inputRegex:
      - "^personid"
      - "^encounterid"
      - "^tenant"
      - "conditioncode"
      - "servicedate"
    datefield: "servicedate"
    filter: "diagnosistype = 'Final'"

  conditionFinalConfirmedSource:
    source: 'conditions'
    label: 'Confirmed final diagnoses'
    inputRegex:
      - "^personid"
      - "^encounterid"
      - "^tenant"
      - "conditioncode"
      - "servicedate"
    datefield: "servicedate"
    filter: "diagnosistype = 'Final' AND confirmationstatus = 'Confirmed'"

  # --- MEDICATIONS ---
  medicationSource:
    source: 'medication_administration'
    label: 'Medication administrations'
    inputRegex:
      - "^personid"
      - "^encounterid"
      - "^tenant"
      - "drugcode"
      - "drugname"
      - "administrationdate"
      - "dosage"
      - "route"
    datefield: "administrationdate"

  medicationDrugSource:
    source: 'medication_drug'
    label: 'Medication drug dictionary'
    inputRegex:
      - "drugcode"
      - "drugname"
      - "genericname"
      - "brandname"
      - "drugclass"

  medicationDatesSource:
    source: 'medication_administration'
    label: 'Medication with date range'
    inputRegex:
      - "^personid"
      - "^tenant"
      - "drugcode"
      - "administrationdate"
      - "administrationenddate"
    datefield: "administrationdate"

  # --- LABS ---
  labSource:
    source: 'lab_results'
    label: 'Laboratory results'
    inputRegex:
      - "^personid"
      - "^encounterid"
      - "^tenant"
      - "labcode"
      - "labname"
      - "resultvalue"
      - "resultunit"
      - "collectiondate"
      - "referencerange"
    datefield: "collectiondate"

  labsmallSource:
    source: 'lab_catalog'
    label: 'Lab test catalog'
    inputRegex:
      - "labcode"
      - "labname"
      - "loinc"

  # --- PROCEDURES ---
  procedureSource:
    source: 'procedures'
    label: 'Clinical procedures'
    inputRegex:
      - "^personid"
      - "^encounterid"
      - "^tenant"
      - "procedurecode"
      - "procedurename"
      - "servicedate"
    datefield: "servicedate"

  # --- DEMOGRAPHICS ---
  demoSource:
    source: 'demographics'
    label: 'Patient demographics'
    inputRegex:
      - "^personid"
      - "^tenant"
      - "birthdate"
      - "gender"
      - "race"
      - "ethnicity"
      - "maritalstatus"
      - "deceased"
      - "zip_code"

  demoPreferred:
    source: 'demographics'
    label: 'Demographics (core fields only)'
    inputRegex:
      - "^personid"
      - "^tenant"
      - "birthdate"
      - "gender"

  # --- REFERENCE TABLES ---
  ethnicities:
    source: 'ref_ethnicity'
    label: 'Ethnicity reference'

  races:
    source: 'ref_race'
    label: 'Race reference'

  gender:
    source: 'ref_gender'
    label: 'Gender reference'

  maritalstatus:
    source: 'ref_marital_status'
    label: 'Marital status reference'

  birthsex:
    source: 'ref_birth_sex'
    label: 'Birth sex reference'

  # --- PROVIDERS ---
  providerSource:
    source: 'providers'
    label: 'Healthcare providers'
    inputRegex:
      - "providerid"
      - "providername"
      - "specialty"
      - "npi"
```

---

## Configuration Variable Substitution

Variables in configs are replaced at runtime. Use `{variable_name}` syntax.

### Built-in Variables
- `{today}` - Current date (from `replace` parameter)
- `{dataPath}` - Base data path

### Custom Variables
Pass custom variables via the `replace` parameter:

```python
resource = Resources(
    local_config='000-config.yaml',
    global_config='config-global.yaml',
    schemaTag_config='config-RWD.yaml',
    replace={
        'today': '2025-01-28',
        'study_start': '2020-01-01',
        'my_custom_var': 'custom_value'
    }
)
```

Then use in config:
```yaml
historyStop: "{today}"
startDate: "{study_start}"
customField: "{my_custom_var}"
```

---

## Quick Reference: Key Configuration Fields

| Field | Location | Purpose |
|-------|----------|---------|
| `project` | 000-config | Project name |
| `disease` | 000-config | Condition tag for table names |
| `schemaTag` | 000-config | Data source version tag |
| `schemas` | 000-config | Map logical→physical schema names |
| `projectTables` | 000-config | Define extract tables |
| `RWDTables` | config-RWD | Define source tables |
| `callFunProcessDataTables` | config-global | Schema processing rules |
| `source` | RWDTables entry | Actual database table name |
| `inputRegex` | RWDTables entry | Column filter patterns |
| `datefield` | Any table | Primary date column |
| `indexFields` | projectTables entry | Primary key fields |
| `updateDict` | callFun entry | Auto-discover tables |
