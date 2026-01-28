# Analysis Description Schema

This document defines a structured format for describing healthcare data analyses.
Use this schema when requesting an LLM to generate data pipelines.

---

## Overview

An analysis description should contain:
1. **Metadata** - Project identification and context
2. **Cohort Definition** - Who is included/excluded
3. **Exposures** - Medications, procedures, or other treatments of interest
4. **Outcomes** - What you're measuring
5. **Covariates** - Variables for adjustment or stratification
6. **Time Windows** - Study periods and lookback/follow-up windows
7. **Output Specifications** - What tables/files to produce

---

## Analysis Description Template

```yaml
# =============================================================================
# ANALYSIS DESCRIPTION
# Use this template to describe your healthcare data analysis
# An LLM can use this to generate the data pipeline code
# =============================================================================

# -----------------------------------------------------------------------------
# METADATA
# -----------------------------------------------------------------------------
metadata:
  # Unique identifier for this analysis
  analysis_id: "DM_MET_001"

  # Human-readable title
  title: "Metformin Adherence in Type 2 Diabetes"

  # Brief description
  description: |
    Analyze metformin prescription patterns and adherence
    in newly diagnosed Type 2 Diabetes patients.

  # Principal investigator
  investigator: "Jane Smith, MD"

  # IRB or protocol number (if applicable)
  protocol: "IRB-2025-001"

  # Analysis version
  version: "1.0"

  # Date created
  date: "2025-01-28"

# -----------------------------------------------------------------------------
# DATA SOURCE
# Specify which schema/database to use
# -----------------------------------------------------------------------------
data_source:
  # Schema tag (matches config-{schemaTag}.yaml)
  schema_tag: "RWD"

  # Specific database (optional, uses config if not specified)
  database: "iuhealth_ed_data_cohort_202306"

  # Project output schema
  output_schema: "diabetes_study"

# -----------------------------------------------------------------------------
# STUDY PERIOD
# Define the observation window
# -----------------------------------------------------------------------------
study_period:
  # Start of data availability
  data_start: "2015-01-01"

  # End of data availability
  data_end: "2025-01-28"

  # Study enrollment period (when patients can enter cohort)
  enrollment_start: "2020-01-01"
  enrollment_end: "2024-12-31"

  # Minimum required baseline period (days before index)
  baseline_period: 365

  # Minimum required follow-up period (days after index)
  followup_period: 180

# -----------------------------------------------------------------------------
# COHORT DEFINITION
# Define inclusion and exclusion criteria
# -----------------------------------------------------------------------------
cohort:
  # --- INCLUSION CRITERIA ---
  # All conditions must be met (AND logic)
  inclusion:

    # Diagnosis-based criteria
    diagnoses:
      - name: "Type 2 Diabetes"
        codes:
          system: "ICD10"
          patterns:
            - "E11%"      # ICD-10 E11.* (Type 2 diabetes)
            - "E11.9"     # Exact code
        # How many occurrences required
        min_occurrences: 2
        # Within what time window
        occurrence_window_days: 365
        # Which diagnosis types to include
        diagnosis_types:
          - "Final"
          - "Discharge"
        # Confirmation status
        confirmation_status:
          - "Confirmed"

    # Age criteria
    age:
      min: 18
      max: 89
      # When to calculate age
      at_time: "index_date"

    # Enrollment/data availability
    enrollment:
      # Minimum continuous enrollment before index
      baseline_days: 365
      # Minimum continuous enrollment after index
      followup_days: 180

  # --- EXCLUSION CRITERIA ---
  # Any condition excludes patient (OR logic)
  exclusion:

    diagnoses:
      - name: "Type 1 Diabetes"
        codes:
          system: "ICD10"
          patterns:
            - "E10%"
        # Time window relative to index (null = any time)
        window:
          before_index_days: null  # Any time before
          after_index_days: 0      # Not after index

      - name: "Gestational Diabetes"
        codes:
          system: "ICD10"
          patterns:
            - "O24%"

      - name: "Secondary Diabetes"
        codes:
          system: "ICD10"
          patterns:
            - "E08%"
            - "E09%"
            - "E13%"

    medications:
      - name: "Insulin (pre-index)"
        patterns:
          - "insulin%"
        window:
          before_index_days: 365
          after_index_days: 0

    # Other exclusions
    other:
      - "Deceased before index date"
      - "Missing gender or birthdate"

  # --- INDEX DATE DEFINITION ---
  index_date:
    # What defines the index date
    definition: "First qualifying Type 2 Diabetes diagnosis"
    # Must be within enrollment period
    within_enrollment: true

# -----------------------------------------------------------------------------
# EXPOSURES
# Medications, procedures, or treatments of interest
# -----------------------------------------------------------------------------
exposures:

  - name: "Metformin"
    type: "medication"
    codes:
      patterns:
        - "metformin%"
        - "glucophage%"
      # Search in which table/field
      search_table: "medication_drug"
      search_field: "drugname"

    # Time window for exposure
    window:
      start: "index_date"
      end: "study_end"

    # What to capture
    measures:
      - "first_prescription_date"
      - "last_prescription_date"
      - "total_prescriptions"
      - "days_supply"
      - "therapy_gaps"  # Gaps > 90 days

    # Gap threshold for defining therapy courses
    gap_threshold_days: 90

  - name: "Sulfonylurea"
    type: "medication"
    codes:
      patterns:
        - "glipizide%"
        - "glyburide%"
        - "glimepiride%"
    window:
      start: "index_date"
      end: "study_end"
    measures:
      - "first_prescription_date"
      - "ever_used"

  - name: "GLP-1 Agonist"
    type: "medication"
    codes:
      patterns:
        - "semaglutide%"
        - "liraglutide%"
        - "dulaglutide%"
        - "ozempic%"
        - "wegovy%"
    window:
      start: "index_date"
      end: "study_end"
    measures:
      - "first_prescription_date"
      - "ever_used"

# -----------------------------------------------------------------------------
# OUTCOMES
# What you're measuring as endpoints
# -----------------------------------------------------------------------------
outcomes:

  - name: "Metformin Adherence"
    type: "continuous"
    definition: |
      Proportion of days covered (PDC) with metformin
      during the first year after index date.
    calculation: "days_with_metformin / 365"
    window:
      start: "index_date"
      duration_days: 365

  - name: "HbA1c Control"
    type: "binary"
    definition: "HbA1c < 7.0% at 12 months post-index"
    lab:
      codes:
        - "4548-4"  # LOINC for HbA1c
      patterns:
        - "%hemoglobin a1c%"
        - "%hba1c%"
    window:
      start_days_from_index: 335
      end_days_from_index: 395
    threshold:
      operator: "<"
      value: 7.0

  - name: "Hypoglycemia Event"
    type: "time_to_event"
    definition: "First hypoglycemia diagnosis or ED visit"
    diagnoses:
      patterns:
        - "E16.0%"  # Drug-induced hypoglycemia
        - "E16.1%"  # Other hypoglycemia
        - "E16.2%"  # Hypoglycemia unspecified
    window:
      start: "index_date"
      end: "study_end"
    censoring:
      - "death"
      - "disenrollment"
      - "study_end"

  - name: "All-Cause Mortality"
    type: "time_to_event"
    definition: "Death from any cause"
    window:
      start: "index_date"
      end: "study_end"

# -----------------------------------------------------------------------------
# COVARIATES
# Variables for adjustment, stratification, or description
# -----------------------------------------------------------------------------
covariates:

  # --- DEMOGRAPHICS ---
  demographics:
    - name: "age"
      type: "continuous"
      at_time: "index_date"
      also_create_groups:
        - "18-44"
        - "45-64"
        - "65-74"
        - "75+"

    - name: "gender"
      type: "categorical"
      categories: ["Female", "Male", "Other", "Unknown"]

    - name: "race"
      type: "categorical"
      categories: ["White", "Black", "Asian", "Other", "Unknown"]
      standardize: true  # Use group_races() function

    - name: "ethnicity"
      type: "categorical"
      categories: ["Hispanic", "Not Hispanic", "Unknown"]
      standardize: true

  # --- COMORBIDITIES (Baseline) ---
  comorbidities:
    window:
      end: "index_date"
      lookback_days: 365

    conditions:
      - name: "Hypertension"
        codes: ["I10%", "I11%", "I12%", "I13%"]

      - name: "Hyperlipidemia"
        codes: ["E78%"]

      - name: "Obesity"
        codes: ["E66%"]

      - name: "CKD"
        codes: ["N18%"]
        also_capture_stage: true  # N18.1, N18.2, etc.

      - name: "CHF"
        codes: ["I50%"]

      - name: "CAD"
        codes: ["I25%", "I20%", "I21%", "I22%"]

      - name: "Depression"
        codes: ["F32%", "F33%"]

  # --- BASELINE LABS ---
  baseline_labs:
    window:
      end: "index_date"
      lookback_days: 180
      use: "closest_to_index"  # or "first", "last", "mean"

    labs:
      - name: "HbA1c_baseline"
        codes: ["4548-4"]
        patterns: ["%hba1c%"]

      - name: "Creatinine_baseline"
        codes: ["2160-0"]

      - name: "eGFR_baseline"
        codes: ["33914-3", "48642-3", "48643-1"]

      - name: "LDL_baseline"
        codes: ["13457-7", "2089-1"]

  # --- HEALTHCARE UTILIZATION ---
  utilization:
    window:
      end: "index_date"
      lookback_days: 365

    measures:
      - name: "ED_visits_baseline"
        encounter_types: ["Emergency"]
        aggregation: "count"

      - name: "Hospitalizations_baseline"
        encounter_types: ["Inpatient"]
        aggregation: "count"

      - name: "Outpatient_visits_baseline"
        encounter_types: ["Outpatient", "Office Visit"]
        aggregation: "count"

# -----------------------------------------------------------------------------
# OUTPUT SPECIFICATIONS
# What tables and files to produce
# -----------------------------------------------------------------------------
outputs:

  # --- INTERMEDIATE TABLES ---
  intermediate_tables:
    - name: "diabetes_codes"
      description: "Matched diabetes ICD codes"
      persist: false  # Don't save, just use in memory

    - name: "diabetes_conditions"
      description: "Patient diabetes diagnosis records"
      persist: true
      partition_by: "tenant"

    - name: "metformin_codes"
      description: "Matched metformin drug codes"
      persist: false

    - name: "metformin_prescriptions"
      description: "Patient metformin prescription records"
      persist: true
      partition_by: "tenant"

  # --- FINAL TABLES ---
  final_tables:
    - name: "study_cohort"
      description: "Final analytic cohort with all variables"
      columns:
        - "personid"
        - "tenant"
        - "index_date"
        - "age"
        - "gender"
        - "race"
        - "ethnicity"
        - "all covariates"
        - "all exposures"
        - "all outcomes"
      persist: true
      partition_by: "tenant"
      also_export:
        - format: "csv"
          path: "{dataLoc}study_cohort.csv"
        - format: "parquet"
          path: "{parquetLoc}study_cohort"

    - name: "attrition_table"
      description: "Cohort attrition at each step"
      columns:
        - "step"
        - "description"
        - "n_patients"
        - "n_excluded"
        - "pct_remaining"

  # --- SUMMARY OUTPUTS ---
  summaries:
    - name: "table1"
      description: "Baseline characteristics table"
      stratify_by: "metformin_ever"
      include:
        - "demographics"
        - "comorbidities"
        - "baseline_labs"
        - "utilization"

    - name: "flowchart"
      description: "CONSORT-style flow diagram data"

# -----------------------------------------------------------------------------
# ANALYSIS NOTES
# Additional instructions or considerations
# -----------------------------------------------------------------------------
notes:
  - "Use confirmed diagnoses only for cohort entry"
  - "Censor at death or disenrollment"
  - "Handle missing race as 'Unknown'"
  - "Use last HbA1c before index if multiple available"
  - "Exclude patients with < 2 encounters in baseline period"

# -----------------------------------------------------------------------------
# VALIDATION CHECKS
# Quality checks to perform
# -----------------------------------------------------------------------------
validation:
  - "No future dates (after study_end)"
  - "Index date within enrollment period"
  - "Age between 18-89 at index"
  - "No duplicate person records"
  - "Exposure dates after index date"
```

---

## Minimal Analysis Description

For simpler analyses, you can use a reduced format:

```yaml
metadata:
  title: "Simple Diabetes Cohort"
  description: "Patients with Type 2 Diabetes diagnosis"

study_period:
  start: "2020-01-01"
  end: "2025-01-28"

cohort:
  inclusion:
    diagnoses:
      - name: "Type 2 Diabetes"
        codes: ["E11%"]
        min_occurrences: 1

    age:
      min: 18

  exclusion:
    diagnoses:
      - name: "Type 1 Diabetes"
        codes: ["E10%"]

  index_date:
    definition: "First Type 2 Diabetes diagnosis"

covariates:
  demographics: [age, gender, race]

outputs:
  final_tables:
    - name: "diabetes_cohort"
      columns: [personid, index_date, age, gender, race]
```

---

## Code Pattern Mapping

When generating code, map analysis description elements to lhn patterns:

| Analysis Element | LHN Pattern |
|------------------|-------------|
| `diagnoses.codes.patterns` | `create_extract()` with `find_method='regex'` |
| `diagnoses` extraction | `entityExtract()` on `conditionSource` |
| `medications.codes` | `create_extract()` on `medicationDrugSource` |
| `medications` extraction | `entityExtract()` on `medicationSource` |
| `labs` extraction | `entityExtract()` on `labSource` |
| `index_date` creation | `write_index_table()` |
| `demographics` | Join with `demoSource`, use `group_races()` etc. |
| `utilization.count` | `calcUsage()` or `aggregate_fields()` |
| `outcomes.time_to_event` | `write_index_table()` with date fields |
| `attrition_table` | `attrition()` after each step |

---

## Example: From Description to Code

Given this analysis description:
```yaml
cohort:
  inclusion:
    diagnoses:
      - name: "Type 2 Diabetes"
        codes: ["E11%"]
        min_occurrences: 2
```

Generate this code:
```python
# Step 1: Create search pattern list
e.diabetes_patterns.df = spark.createDataFrame(
    [("E11%",)], ["pattern"]
)

# Step 2: Find matching codes
e.diabetes_codes.create_extract(
    elementList=e.diabetes_patterns,
    elementListSource=r.conditionSource.df,
    find_method='regex'
)

# Step 3: Extract condition records
e.diabetes_conditions.entityExtract(
    elementList=e.diabetes_codes,
    entitySource=r.conditionSource.df,
    cacheResult=True
)

# Step 4: Filter to patients with 2+ occurrences
from pyspark.sql import functions as F
patient_counts = e.diabetes_conditions.df.groupBy('personid').count()
qualified_patients = patient_counts.filter(F.col('count') >= 2)

# Step 5: Create index with first diagnosis date
e.diabetes_index.write_index_table(
    inTable=e.diabetes_conditions.df.join(
        qualified_patients.select('personid'),
        on='personid'
    ),
    index_field=['personid'],
    datefieldPrimary='servicedate',
    code='diabetes'
)
```
