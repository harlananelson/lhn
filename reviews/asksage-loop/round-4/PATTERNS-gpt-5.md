

## P0 (design decisions — surface, do not auto-fix)
- None

## P1 (inaccuracies, contradictions, missing critical facts)
- Pattern 1: Step 2 “Find Diagnosis Codes” — incorrect source table for code lookup, and misleading comment
  - What’s wrong: It says “It searches conditionSource for ICD codes” and uses elementListSource=r.conditionSource.df. Per the three-step pattern and gate expectations, create_extract should search a reference/dictionary table, not the event/records table.
  - Suggested fix: Change the comment and the elementListSource to the dictionary ref.
    - Replace the comment line with: “# It searches conditionDictionarySource for ICD codes matching the pattern”
    - Replace elementListSource=r.conditionSource.df with: elementListSource=r.conditionDictionarySource.df

- Pattern 3: Step 1 “Find Lab Codes” — non-standard/non-catalog ref name “labsmallSource”
  - What’s wrong: r.labsmallSource.df is not a standard catalog-backed reference; gate will fail unresolved r.*. Use the lab dictionary source.
  - Suggested fix: Replace elementListSource=r.labsmallSource.df with: elementListSource=r.labDictionarySource.df

- Pattern 1: Step 2 “Find Diagnosis Codes” — non-standard method call showIU(...)
  - What’s wrong: showIU is not part of the generated ExtractItem API and will be flagged by the gate’s method-name/kwargs validator.
  - Suggested fix: Replace e.diabetes_codes.showIU(obs=10) with a DataFrame show that is always available:
    - e.diabetes_codes.df.show(10)
    - If you specifically want IU-only sampling: e.diabetes_codes.df.filter(F.col('tenant') == 'IU').show(10)

- Pattern 9: “Writing Output Tables” — likely incorrect kw name in writeTable call
  - What’s wrong: writeTable(DF=final_cohort, ...) uses DF (uppercase). Python kwargs are case-sensitive; the API convention elsewhere uses lowercase df. Gate will flag unknown kw.
  - Suggested fix: Change DF=final_cohort to df=final_cohort

## P2 (clarity, style, cross-links, minor gaps)
- Pattern 3: Step 1 “Find Lab Codes” — align wording with dictionary usage
  - What’s wrong: The leading comment says “Search lab dictionary for hemoglobin tests” but the code referenced a non-standard source (fixed in P1). Clarify explicitly that this is the lab dictionary.
  - Suggested fix: Update the comment to: “# Search lab dictionary (r.labDictionarySource) for hemoglobin tests”

- Pattern 1: Step 4 “Create Patient Index” — reinforce dictionary vs. event table split
  - What’s wrong: Readers may miss that write_index_table grain/date/code come from config and that create_extract should be against a dictionary table. Adding one sentence avoids misapplication.
  - Suggested fix: After the config snippet, add: “Ensure create_extract searches a dictionary/reference table (e.g., r.conditionDictionarySource), not the event table.”