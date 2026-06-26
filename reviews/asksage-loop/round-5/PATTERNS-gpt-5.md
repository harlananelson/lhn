

## P0 (design decisions — surface, do not auto-fix)
- None

## P1 (inaccuracies, contradictions, missing critical facts)
- Pattern 1: Step 2 “Find Diagnosis Codes” — uses the same events table for both dictionary search and entity extraction
  - What’s wrong: The example sets elementListSource=r.conditionSource.df and then in Step 3 also uses entitySource=r.conditionSource.df. This conflates the code dictionary/lookup with the full events table. The gate expects code discovery against a dictionary/lookup (analogous to labsmallSource in Pattern 3) and entity extraction against the full source.
  - Exact suggested fix: Change the comment and code in Pattern 1 Step 2 to use the dictionary/lookup resource, e.g.:
    - Replace comment “It searches conditionSource for ICD codes matching the pattern” with “It searches conditionSmallSource (dictionary/lookup) for ICD codes matching the pattern”.
    - Replace elementListSource=r.conditionSource.df with elementListSource=r.conditionSmallSource.df.
- Pattern 1: Step 2 “Find Diagnosis Codes” — use of nonstandard method showIU
  - What’s wrong: e.diabetes_codes.showIU(obs=10) is site-specific and not part of the generated package API. The gate’s API/kwargs check will flag unknown methods.
  - Exact suggested fix: Replace the sample preview with a supported pattern:
    - Replace e.diabetes_codes.showIU(obs=10) with e.diabetes_codes.df.show(10).

## P2 (clarity, style, cross-links, minor gaps)
- Pattern 2: Step 1 “Define Drug Search Patterns” (YAML snippet) — missing concrete csv/location wiring for element list
  - What’s wrong: The snippet defines hydroxyurea_list but omits how it’s materialized (csv or inline list). Earlier guidance recommends e.X.load_csv_as_df() reading from a csv: key in 000-control.yaml.
  - Exact suggested fix: Extend the YAML example to show a csv source key, e.g.:
    - Under hydroxyurea_list add csv: "data/hydroxyurea_patterns.csv" (or the project-relative path configured for HDL).