

## P1 (inaccuracies, contradictions, missing critical facts)
- Notebook Authoring Conventions → HDL deploy, execute, render, and review → Orchestrator (local) shell snippet: single-quoted --hdl-project-dir prevents $USER expansion and tilde expansion never occurs inside quotes. Suggested fix: replace the arg to expand correctly, e.g. use HOME and double quotes.
  Exact change:
  - Current: `--hdl-project-dir '~/work/Users/$USER/Projects/derm'`
  - Replace with: `--hdl-project-dir "$HOME/work/Users/$USER/Projects/derm"` or unquoted: `--hdl-project-dir ~/work/Users/$USER/Projects/derm`

- Pattern 7: Time-Window Analysis → Step 3: Join and filter to window: PySpark functions date_sub/date_add require Column, not a bare string. Using `'index_date'` will fail type checks at runtime.
  Exact change:
  - Current filter:
    ```
    .filter(
        (F.col('servicedate') >= F.date_sub('index_date', 90)) &
        (F.col('servicedate') <= F.date_add('index_date', 90))
    )
    ```
  - Replace with:
    ```
    .filter(
        (F.col('servicedate') >= F.date_sub(F.col('index_date'), 90)) &
        (F.col('servicedate') <= F.date_add(F.col('index_date'), 90))
    )
    ```

- Performance Tips → 2. Broadcast small tables: kwarg name uses snake_case (“broadcast_flag”) while the rest of the documented API uses camelCase kwargs (e.g., cacheResult, elementList). The gate flags unknown kwargs. Align to the actual API kwarg name.
  Exact change:
  - Current: `Use broadcast_flag=True for small lookup tables`
  - Replace with: `Use broadcastFlag=True for small lookup tables (e.g., in entityExtract(..., broadcastFlag=True))`

- Pattern 1: Simple Diagnosis-Based Cohort → Step 2: Find Diagnosis Codes and Pattern 2: Medication-Based Analysis → Step 2: Find Drug Codes and Pattern 3: Lab-Based Analysis → Step 1: Find Lab Codes: create_extract kwarg “find_method” is snake_case, inconsistent with the API style used elsewhere (elementList, elementListSource, cacheResult). Gate will flag unknown kwargs if the API expects camelCase.
  Exact change (apply in all three places):
  - Current: `find_method='regex'`
  - Replace with: `findMethod='regex'`

- Pattern 3: Lab-Based Analysis → Step 1: Find Lab Codes: resource name uses “labsmallSource” (lowercase s) while other resources follow camelCase (e.g., medicationDrugSource, conditionSource). Gate resolves r.* from catalog; a case mismatch will fail.
  Exact change:
  - Current: `elementListSource=r.labsmallSource.df`
  - Replace with: `elementListSource=r.labSmallSource.df` (or use the exact catalog name for the lab dictionary resource as resolved by the gate)

- Pattern 3: Lab-Based Analysis → Step 3: Aggregate Lab Values: aggregate_fields call duplicates parameters (“fields” and “values”) for the same column; most implementations expect only the value columns list. Avoid redundant args that the gate may flag as unknown.
  Exact change:
  - Current:
    ```
    lab_summary = aggregate_fields(
        df=e.hgb_labs.df,
        index=['personid'],
        fields=['resultvalue'],
        values=['resultvalue'],
        aggfuncs=[F.min, F.max, F.avg, F.count],
        aggfunc_names=['min', 'max', 'avg', 'count']
    )
    ```
  - Replace with:
    ```
    lab_summary = aggregate_fields(
        df=e.hgb_labs.df,
        index=['personid'],
        values=['resultvalue'],
        aggfuncs=[F.min, F.max, F.avg, F.count],
        aggfunc_names=['min', 'max', 'avg', 'count']
    )
    ```

- Pattern 1: Simple Diagnosis-Based Cohort → Step 4: Create Patient Index (Alternative — standalone FUNCTION): The example shows a standalone write_index_table with kwarg “index_field”, which conflicts with the earlier stated config property “indexFields” and the gate’s strict kwarg validation. This is likely to mislead users and break at the gate.
  Exact change (choose one of the following to eliminate the mismatch):
  - Prefer removal: Delete the entire “(Alternative — the standalone FUNCTION…)” paragraph and its code block.
  - Or correct the kwargs to the actual function signature used by lhn.cohort (if supported). If the function mirrors config names, replace with:
    ```
    from lhn.cohort import write_index_table
    write_index_table(
        inTable=e.diabetes_conditions.df,
        indexFields=['personid'],
        datefieldPrimary='servicedate',
        code='diabetes',
        retained_fields=[]
    )
    ```
    Use the exact kwarg names the generated API exposes; run the gate to confirm.

## P2 (clarity, style, cross-links, minor gaps)
- Pattern 7: Time-Window Analysis → imports: The rest of the guide standardizes imports via “from lhn.header import spark, F, Window” to ensure a pre-bound Spark context and consistent F symbol. This section re-imports pyspark.sql.functions as F, which is inconsistent and may confuse users.
  Exact change:
  - Current: `from pyspark.sql import functions as F`
  - Replace with: `from lhn.header import F` (and ensure the standard notebook setup cell has already bound spark and F)

- Common Field Names Reference → Demographics Fields: Inconsistent naming with earlier examples that use “marital_status” (snake_case) in standardization examples, while the reference lists “maritalstatus” (no underscore).
  Exact change:
  - Current: `maritalstatus - Marital status`
  - Replace with: `marital_status - Marital status` (and, if relevant, add a note that the standardized field becomes “marital_std” as shown earlier)

- Use the real API — correct names and kwargs: To reinforce consistency and reduce near-miss typos, explicitly state that all kwargs in examples use the generated API’s casing (camelCase for ExtractItem methods), and add a cross-link to the API reference.
  Exact change:
  - After “Use the real API — correct names and kwargs” add a sentence:
    `All kwargs must match the generated API’s casing (e.g., elementList, elementListSource, cacheResult, findMethod, broadcastFlag). See ~/projects/hdl-harness/docs/api_reference.md for the authoritative signatures.`

- Performance Tips → 5. Limit during development: Add a reminder to remove development limits before running hdl_run.py --all to avoid silently truncating outputs in the HDL render/archive.
  Exact change:
  - Append: `Remove any .limit(...) calls before running hdl_run.py --all to ensure full cohort outputs are rendered and archived.`