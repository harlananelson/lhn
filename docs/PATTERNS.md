# LHN Pipeline Patterns

This document describes common data pipeline patterns using the lhn package.
Use these patterns as templates for building healthcare data extraction workflows.

## Overview: The Three-Step Extraction Pattern

Most lhn pipelines follow this core pattern:

```
1. create_extract()   -> Find codes/identifiers from reference tables
2. entityExtract()    -> Retrieve entity records using those codes
3. write_index_table() -> Create patient-level summary with first/last dates
```

---

## Pattern 1: Simple Diagnosis-Based Cohort

**Goal**: Find all patients with a specific diagnosis (e.g., Type 2 Diabetes)

### Step 1: Setup
```python
from lhn import Resources
from lhn.core.extract import Extract

# Initialize resources
resource = Resources(
    local_config='000-config.yaml',
    global_config='config-global.yaml',
    schemaTag_config='config-RWD.yaml',
    replace={'today': '2025-01-28'},
    debug=False
)

r = resource.r  # RWD tables
e = resource.e  # Extract tables
```

### Step 2: Find Diagnosis Codes
```python
# e.diabetes_codes is defined in 000-config.yaml projectTables
# It searches conditionSource for ICD codes matching the pattern

e.diabetes_codes.create_extract(
    elementList=e.diabetes_icd_list,        # Table with search patterns
    elementListSource=r.conditionSource.df,  # Source to search
    find_method='regex'                       # Use regex matching
)

# Check what was found
e.diabetes_codes.showIU(obs=10)
e.diabetes_codes.attrition()
```

### Step 3: Extract Condition Records
```python
# Get all condition records for patients with diabetes codes
e.diabetes_conditions.entityExtract(
    elementList=e.diabetes_codes,
    entitySource=r.conditionSource.df,
    cacheResult=True
)

e.diabetes_conditions.attrition()
```

### Step 4: Create Patient Index
```python
# Create patient-level table with first/last diagnosis dates
e.diabetes_index.write_index_table(
    inTable=e.diabetes_conditions,
    histStart='2015-01-01',
    histEnd='2025-01-28',
    index_field=['personid'],
    datefieldPrimary='servicedate',
    code='diabetes'
)

# Result has columns: personid, index_diabetes, last_diabetes, etc.
e.diabetes_index.attrition()
```

---

## Pattern 2: Medication-Based Analysis

**Goal**: Find patients on specific medications and track usage

### Step 1: Define Drug Search Patterns
```yaml
# In 000-config.yaml projectTables:
projectTables:
  hydroxyurea_codes:
    label: "Hydroxyurea drug codes"
    indexFields: [drugcode]

  hydroxyurea_list:
    label: "Search patterns for hydroxyurea"
    # This references a CSV or inline list with regex patterns
```

### Step 2: Find Drug Codes
```python
# Search medication drug dictionary for hydroxyurea
e.hydroxyurea_codes.create_extract(
    elementList=e.hydroxyurea_list,
    elementListSource=r.medicationDrugSource.df,
    find_method='regex'
)
```

### Step 3: Extract Medication Records
```python
# Get all medication administrations for these drug codes
e.hydroxyurea_meds.entityExtract(
    elementList=e.hydroxyurea_codes,
    entitySource=r.medicationSource.df,
    cohort=e.study_cohort,  # Optional: limit to cohort
    cacheResult=True
)
```

### Step 4: Create Medication Index with Gaps
```python
# Track first/last use and therapy gaps
e.hydroxyurea_index.write_index_table(
    inTable=e.hydroxyurea_meds,
    histStart='2015-01-01',
    histEnd='2025-01-28',
    index_field=['personid'],
    datefieldPrimary='administrationdate',
    datefieldStop='administrationenddate',
    code='hydroxyurea',
    max_gap=90,  # Days gap to consider new therapy course
    sort_fields=['administrationdate']
)

# Result includes: course_of_therapy, therapy_gaps, etc.
```

---

## Pattern 3: Lab-Based Analysis

**Goal**: Analyze lab values (e.g., HbA1c, hemoglobin)

### Step 1: Find Lab Codes
```python
# Search lab dictionary for hemoglobin tests
e.hgb_codes.create_extract(
    elementList=e.hgb_search_patterns,
    elementListSource=r.labsmallSource.df,
    find_method='regex'
)
```

### Step 2: Extract Lab Results
```python
e.hgb_labs.entityExtract(
    elementList=e.hgb_codes,
    entitySource=r.labSource.df,
    cohort=e.study_cohort,
    cacheResult=True
)
```

### Step 3: Aggregate Lab Values
```python
from lhn import aggregate_fields
import pyspark.sql.functions as F

# Get min, max, mean per patient
lab_summary = aggregate_fields(
    df=e.hgb_labs.df,
    index=['personid'],
    fields=['resultvalue'],
    values=['resultvalue'],
    aggfuncs=[F.min, F.max, F.avg, F.count],
    aggfunc_names=['min', 'max', 'avg', 'count']
)
```

---

## Pattern 4: Encounter-Based Usage Analysis

**Goal**: Calculate healthcare utilization metrics

### Step 1: Get Encounters for Cohort
```python
e.cohort_encounters.entityExtract(
    elementList=e.study_cohort,  # Use cohort as filter
    entitySource=r.encounterSource.df,
    cacheResult=True
)
```

### Step 2: Calculate Usage Metrics
```python
from lhn import calcUsage
import pyspark.sql.functions as F

usage = calcUsage(
    df=e.cohort_encounters.df,
    fields=['personid', 'actualarrivaldate', 'servicedate'],
    dateCoalesce=F.coalesce(F.col('actualarrivaldate'), F.col('servicedate')),
    minDate='2015-01-01',
    maxDate='2025-01-28',
    index=['personid'],
    countFieldName='encounter_count',
    dateFirst='first_encounter',
    dateLast='last_encounter'
)
```

### Step 3: Identify High Utilizers
```python
high_utilizers = usage.filter(F.col('encounter_count') > 10)
```

---

## Pattern 5: Demographics Standardization

**Goal**: Standardize demographic categories for analysis

```python
from lhn import (
    group_ethnicities,
    group_races,
    group_gender,
    group_marital_status,
    assign_age_group
)

# Start with demographics table
demo = r.demoSource.df

# Standardize each field
demo = group_ethnicities(demo, 'ethnicity', 'ethnicity_std')
# Returns: 'Hispanic', 'Not Hispanic'

demo = group_races(demo, 'race', 'race_std')
# Returns: 'Black', 'White', 'Asian', 'Indigenous', 'Other', 'Unknown'

demo = group_gender(demo, 'gender', 'gender_std')
# Returns: 'Female', 'Male', 'Other', 'Unknown'

demo = group_marital_status(demo, 'marital_status', 'marital_std')
# Returns: 'Married', 'Not Married', 'Unknown'

demo = assign_age_group(demo, 'age')
# Adds 'age_group' with quartiles: 'Q1', 'Q2', 'Q3', 'Q4'
```

---

## Pattern 6: Multi-Source Cohort Building

**Goal**: Build cohort requiring multiple criteria

```python
# Step 1: Get patients with diagnosis
e.dx_cohort.create_extract(...)
e.dx_conditions.entityExtract(...)
e.dx_index.write_index_table(...)

# Step 2: Get patients with medication
e.rx_cohort.create_extract(...)
e.rx_meds.entityExtract(...)
e.rx_index.write_index_table(...)

# Step 3: Combine criteria
cohort = e.dx_index.df.join(
    e.rx_index.df,
    on='personid',
    how='inner'  # Require both
)

# Step 4: Apply date criteria (diagnosis before medication)
cohort = cohort.filter(
    F.col('index_diagnosis') < F.col('index_medication')
)

# Step 5: Add demographics
final_cohort = cohort.join(
    r.demoSource.df.select('personid', 'birthdate', 'gender', 'race'),
    on='personid',
    how='left'
)
```

---

## Pattern 7: Time-Window Analysis

**Goal**: Analyze events within time windows (e.g., 90 days before/after index)

```python
from pyspark.sql import functions as F

# Get index dates
index_df = e.cohort_index.df.select('personid', 'index_date')

# Get all events
events = r.conditionSource.df

# Join and filter to window
windowed_events = events.join(
    index_df,
    on='personid',
    how='inner'
).filter(
    (F.col('servicedate') >= F.date_sub('index_date', 90)) &
    (F.col('servicedate') <= F.date_add('index_date', 90))
)
```

---

## Pattern 8: Attrition Tracking

**Goal**: Track patient counts through pipeline steps

```python
from lhn import attrition, count_people

# After each major step, call attrition
e.initial_cohort.attrition()
# Output: Records: 50,000 | Patients: 10,000

e.with_diagnosis.attrition()
# Output: Records: 45,000 | Patients: 9,500

e.with_medication.attrition()
# Output: Records: 30,000 | Patients: 7,200

# Or use count_people for inline counts
count_people(final_df, description='Final cohort', person_id='personid')
```

---

## Pattern 9: Writing Output Tables

**Goal**: Save results to Spark tables and export

```python
from lhn import writeTable

# Write to Spark table
writeTable(
    DF=final_cohort,
    outTable='project_schema.final_cohort_scd_rwd',
    partitionBy='tenant',
    description='Final SCD cohort with demographics',
    removeDuplicates=True
)

# Export to CSV (via ExtractItem)
e.final_cohort.df = final_cohort
e.final_cohort.to_csv()

# Export to Parquet
e.final_cohort.df.write.parquet(e.final_cohort.parquet)
```

---

## Common Field Names Reference

### Encounter Fields
- `personid` - Patient identifier
- `encounterid` - Encounter identifier
- `tenant` - Data source/facility
- `servicedate` / `actualarrivaldate` - Encounter start
- `dischargedate` - Encounter end
- `encountertype` - Inpatient, Outpatient, ED, etc.

### Condition Fields
- `personid`, `encounterid`, `tenant`
- `conditioncode_standard_id` - ICD code
- `conditioncode_standard_display` - Code description
- `servicedate` - Diagnosis date
- `confirmationstatus` - Confirmed, Provisional, etc.

### Medication Fields
- `personid`, `encounterid`, `tenant`
- `drugcode` - Medication code
- `drugname` - Medication name
- `administrationdate` - When given
- `administrationenddate` - End of administration
- `dosage`, `route`, `frequency`

### Lab Fields
- `personid`, `encounterid`, `tenant`
- `labcode` - Lab test code
- `labname` - Test name
- `resultvalue` - Numeric result
- `resultunit` - Unit of measure
- `collectiondate` - When collected
- `referencerange_low`, `referencerange_high`

### Demographics Fields
- `personid`, `tenant`
- `birthdate` - Date of birth
- `gender` - Gender
- `race` - Race
- `ethnicity` - Ethnicity
- `maritalstatus` - Marital status
- `deceased` - Deceased flag
- `zip_code` - ZIP code

---

## Error Handling Pattern

```python
try:
    e.my_extract.create_extract(...)
except Exception as ex:
    logger.error(f"Failed to create extract: {ex}")
    # Fallback or skip

# Check if extract has data before proceeding
if e.my_extract.df is not None and e.my_extract.df.count() > 0:
    e.next_step.entityExtract(...)
else:
    logger.warning("No records found, skipping next step")
```

---

## Performance Tips

1. **Cache intermediate results**: Use `cacheResult=True` in entityExtract
2. **Broadcast small tables**: Use `broadcast_flag=True` for small lookup tables
3. **Filter early**: Apply cohort filters as early as possible
4. **Partition output**: Use `partitionBy='tenant'` for large tables
5. **Limit during development**: Use `.limit(1000)` while testing
