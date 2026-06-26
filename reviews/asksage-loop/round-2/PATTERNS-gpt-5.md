

## P0 (design decisions — surface, do not auto-fix)
- Section: Notebook Authoring Conventions → “Use the real API — correct names and kwargs”
  What's wrong: The guide mixes camelCase and snake_case kwargs (e.g., elementList/cacheResult vs find_method/broadcast_flag). This is a design inconsistency that can confuse authors and undermine the gate’s “unknown kwargs” lint.
  Exact suggested fix: Add an explicit design note clarifying the canonical, generated names per method (create_extract, entityExtract, write_index_table) with a complete kwarg matrix. Example addition: “Canonical kwargs: create_extract(elementList, elementListSource, find_method=...), entityExtract(elementList, entitySource, cohort=None, cacheResult=False, broadcast_flag=False), write_index_table(inTable, histStart=None, histEnd=None, indexLabel=None, lastLabel=None, filterSimple=None).”

- Sections: Pattern 1 Step 2 and Pattern 3 Step 1 (code-search sources)
  What's wrong: The patterns sometimes search event sources (e.g., conditionSource) and sometimes dedicated dictionaries (e.g., medicationDrugSource) for regex code discovery. This split is a design ambiguity that affects performance and reproducibility.
  Exact suggested fix: Add a design rule in “Notebook Authoring Conventions” Section 2: “Prefer dictionary/reference sources (e.g., r.medicationDrugSource, r.conditionDictionarySource, r.labTestDictionarySource) for create_extract regex searches; only fall back to event sources when a dictionary is unavailable. State the recommended table per domain in each pattern.”

## P1 (inaccuracies, contradictions, missing critical facts)
- Section: Pattern 1 Step 2 (“Check what was found”)
  What's wrong: Uses a likely non-existent method `e.diabetes_codes.showIU(obs=10)`. The gate flags near-miss/unknown API methods.
  Exact suggested fix: Replace with a valid preview call, e.g., `e.diabetes_codes.df.show(10)`.

- Section: Pattern 3 Step 1 (“Find Lab Codes”)
  What's wrong: References `r.labsmallSource.df` (lowercase “small”), which is inconsistent with the resource naming used elsewhere and likely fails resource resolution.
  Exact suggested fix: Change to `r.labSource.df` for consistency with Step 2, or if a small dictionary table is intended, use the correct casing `r.labSmallSource.df` (confirm actual name via the catalog and update accordingly).

- Section: Pattern 3 Step 3 (“Aggregate Lab Values”)
  What's wrong: The example passes `fields=[...]` while the comment says “fields is unused”; this is misleading and risks a runtime error if the function doesn’t accept it.
  Exact suggested fix: Remove the unused param and simplify the call:
  `lab_summary = aggregate_fields(df=e.hgb_labs.df, index=['personid'], values=['resultvalue'], aggfuncs=[F.min, F.max, F.avg, F.count], aggfunc_names=['min', 'max', 'avg', 'count'])`.

- Section: Pattern 7 Step 1 (“Get index dates”)
  What's wrong: Uses a hardcoded column name `index_date`, which contradicts earlier guidance that write_index_table outputs index_<code>/last_<code> based on the ExtractItem’s code or indexLabel. This will fail column validation.
  Exact suggested fix: Replace with the actual column produced by your index item, e.g., `index_df = e.cohort_index.df.select('personid', 'index_diagnosis')` (or the configured indexLabel). Add a note: “Use the concrete index column from your item (e.g., index_diabetes), not a generic index_date.”

## P2 (clarity, style, cross-links, minor gaps)
- Section: “Use the real API — correct names and kwargs”
  What's wrong: The kwarg list omits `elementListSource`, which is used in examples.
  Exact suggested fix: Amend the parenthetical list to include `elementListSource`, e.g., “(elementList, elementListSource, cacheResult, find_method, broadcast_flag, …)”.

- Section: Pattern 2 Step 1 (YAML snippet)
  What's wrong: The snippet hints that `hydroxyurea_list` references a CSV/inline list but doesn’t show how to declare it, which reduces reproducibility.
  Exact suggested fix: Add a concrete example, e.g., under `hydroxyurea_list`: `csv: ${dataLoc}/lookups/hydroxyurea_patterns.csv` or `elements: ['(?i)hydroxyurea', '(?i)hydrea']`.

- Section: Common Field Names → Demographics Fields vs Pattern 5
  What's wrong: Inconsistency between `maritalstatus` (Common Fields) and `marital_status` (Pattern 5) for the raw field name.
  Exact suggested fix: Standardize to one raw field name. For consistency with Pattern 5, change the Common Field name to `marital_status` and keep the note “standardized as marital_std”.

- Section: Pattern 9 (“Writing Output Tables”) writeTable example
  What's wrong: `partitionBy='tenant'` suggests a single string; Spark commonly accepts a list and this is clearer for multi-partition use.
  Exact suggested fix: Change to `partitionBy=['tenant']` in the example.

- Section: txtarchive “.txt format” (YAML front matter)
  What's wrong: It doesn’t explicitly state that the Raw Cell YAML must be the first cell; this is easy to miss and causes metadata loss.
  Exact suggested fix: Add: “Ensure this Raw Cell is the first cell of the notebook; otherwise the YAML is ignored during extraction.”