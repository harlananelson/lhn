

Based on my review of `docs/PATTERNS.md` against the provided context, here are the recommended changes.

## P0 (design decisions — surface, do not auto-fix)
- No issues identified. The overall design philosophy and patterns presented are coherent and well-justified within the document's context.

## P1 (inaccuracies, contradictions, missing critical facts)
- **Issue 1:** Inconsistent API pattern for source arguments in `create_extract` and `entityExtract`.
  - **Reference:** `Pattern 1: Step 2`, `Pattern 1: Step 3`, `Pattern 2: Step 2`, `Pattern 2: Step 3`, `Pattern 3: Step 1`, `Pattern 3: Step 2`, `Pattern 4: Step 1`.
  - **What's wrong:** The examples consistently show passing a DataFrame (e.g., `r.conditionSource.df`) to the `elementListSource` and `entitySource` arguments. This is a lower-level pattern than intended. The `lhn` API is designed to accept the higher-level `ExtractItem` objects directly (e.g., `r.conditionSource`), which is cleaner and more consistent with how other arguments like `elementList=e.some_item` are used. The current documentation is misleading and promotes a less robust pattern.
  - **Suggested fix:** In all `create_extract` and `entityExtract` examples, change the source arguments from `r.someSource.df` to `r.someSource`.
    - **Example from `Pattern 1, Step 2`:**
      - **Current:** `elementListSource=r.conditionSource.df,  # Source to search`
      - **Proposed:** `elementListSource=r.conditionSource,      # Source to search`
    - **Example from `Pattern 1, Step 3`:**
      - **Current:** `entitySource=r.conditionSource.df,`
      - **Proposed:** `entitySource=r.conditionSource,`
    - This change should be applied to all similar calls throughout the document.

- **Issue 2:** Contradictory description of the `hdl_run.py --all` workflow.
  - **Reference:** `Notebook Authoring Conventions` > `7. HDL deploy, execute, render, and review`.
  - **What's wrong:** The text describes the render path as `nbconvert --execute → quarto ...` but then states that for the orchestrator, "`--all` skips redundant `--execute`". This is contradictory. The `--all` flag is meant to run the full local loop, which must include execution to generate outputs before rendering. The word "skips" is misleading, suggesting execution does not happen.
  - **Suggested fix:** Replace the confusing sentence with a clearer explanation.
    - **Current:** "`--all` skips redundant `--execute`."
    - **Proposed:** "The `--all` flag orchestrates the entire sequence, including notebook execution, making a separate `--execute` flag unnecessary."

## P2 (clarity, style, cross-links, minor gaps)
- **Issue 1:** Confusing example for the `aggregate_fields` function.
  - **Reference:** `Pattern 3: Lab-Based Analysis` > `Step 3: Aggregate Lab Values`.
  - **What's wrong:** The example code for `aggregate_fields` includes the parameter `fields=['resultvalue']` along with a comment stating "`fields` is unused in the API". This is confusing as it demonstrates an API call with a non-functional argument. Examples should be clean and demonstrate correct, functional usage.
  - **Suggested fix:** Remove the unused `fields` parameter and its associated comment from the example call.
    - **Current:**
      ```python
      # Get min, max, mean per patient (`fields` is unused in the API; groupby is inferred)
      lab_summary = aggregate_fields(
          df=e.hgb_labs.df,
          index=['personid'],
          fields=['resultvalue'],
          values=['resultvalue'],
          aggfuncs=[F.min, F.max, F.avg, F.count],
          aggfunc_names=['min', 'max', 'avg', 'count']
      )
      ```
    - **Proposed:**
      ```python
      # Get min, max, mean per patient (groupby is inferred from `index`)
      lab_summary = aggregate_fields(
          df=e.hgb_labs.df,
          index=['personid'],
          values=['resultvalue'],
          aggfuncs=[F.min, F.max, F.avg, F.count],
          aggfunc_names=['min', 'max', 'avg', 'count']
      )
      ```