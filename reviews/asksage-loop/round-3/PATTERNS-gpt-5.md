

## P1 (inaccuracies, contradictions, missing critical facts)
- Section: Pattern 5: Demographics Standardization — code snippet
  What's wrong: Inconsistent raw marital status column name versus the “Common Field Names Reference”. The snippet uses marital_status, while the reference table and HDL catalog convention use maritalstatus. This inconsistency will cause column validation failures in the gate.
  Exact suggested fix: Change
  - demo = group_marital_status(demo, 'marital_status', 'marital_std')
  to
  - demo = group_marital_status(demo, 'maritalstatus', 'marital_std')
  and in “Demographics Fields” change the parenthetical to:
  - maritalstatus - Marital status (raw field; standardized as 'marital_std' in Pattern 5)

- Section: Pattern 1: Simple Diagnosis-Based Cohort — Step 2 (Find Diagnosis Codes)
  What's wrong: Uses a nonstandard/unreferenced API method showIU, which is not documented in the generated package API and will be flagged by the harness as an unknown method.
  Exact suggested fix: Replace
  - e.diabetes_codes.showIU(obs=10)
  with
  - e.diabetes_codes.df.show(10)

- Section: Pattern 3: Lab-Based Analysis — Step 1 (Find Lab Codes)
  What's wrong: Resource name casing likely incorrect for generated API conventions; r.labsmallSource is inconsistent with camelCase used elsewhere (e.g., r.procedureSource). Near-miss method/resource names are flagged by the gate.
  Exact suggested fix: Change
  - elementListSource=r.labsmallSource.df
  to
  - elementListSource=r.labSmallSource.df

## P2 (clarity, style, cross-links, minor gaps)
- Section: Pattern 2: Medication-Based Analysis — Step 1 (Define Drug Search Patterns) YAML
  What's wrong: The YAML snippet is incomplete and doesn’t demonstrate how hydroxyurea_list is sourced, despite earlier guidance that CSV paths come from csv: in 000-control.yaml and that e.X.load_csv_as_df uses that configuration.
  Exact suggested fix: Expand the YAML example to show the CSV binding:
  - hydroxyurea_list:
      label: "Search patterns for hydroxyurea"
      csv: "<path-to-patterns-csv-relative-to-dataLoc>"

- Section: Pattern 6: Multi-Source Cohort Building — code snippet
  What's wrong: Variable names dx_cohort and rx_cohort are used with create_extract, but they represent code lists, not cohorts. This naming causes confusion when read alongside entityExtract and write_index_table steps.
  Exact suggested fix: Rename for clarity:
  - e.dx_codes.create_extract(...)
  - e.dx_conditions.entityExtract(...)
  - e.dx_index.write_index_table(inTable=e.dx_conditions)
  - e.rx_codes.create_extract(...)
  - e.rx_meds.entityExtract(...)
  - e.rx_index.write_index_table(inTable=e.rx_meds)

- Section: Pattern 9: Writing Output Tables — code snippet
  What's wrong: partitionBy is passed as a scalar string. While some APIs accept this, many Spark write paths expect a list; using a list improves clarity and avoids ambiguity across versions.
  Exact suggested fix: Change
  - partitionBy='tenant'
  to
  - partitionBy=['tenant']