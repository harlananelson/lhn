

Excellent, Harlan. I have reviewed `docs/PATTERNS.md` against the provided context. The document is a strong guide, and my review has identified a few areas where accuracy and clarity can be improved. Here is a list of concrete, actionable issues.

## P1 (inaccuracies, contradictions, missing critical facts)
- **Section:** `Pattern 8: Attrition Tracking`
  - **What's wrong:** The example shows `from lhn import attrition, count_people`, but then calls `attrition` as an `ExtractItem` method (`e.initial_cohort.attrition()`). This is contradictory. The `attrition()` method is part of the `ExtractItem` API and does not need to be imported separately.
  - **Suggested fix:** Update the code block to reflect the correct usage.
    ```python
    # from lhn import attrition, count_people  <- DELETE THIS LINE
    from lhn import count_people              # <- CHANGE TO THIS

    # After each major step, call the attrition method
    e.initial_cohort.attrition()
    # Output: Records: 50,000 | Patients: 10,000
    ...
    ```

- **Section:** `Performance Tips`
  - **What's wrong:** Tip #2, "Broadcast small tables," mentions `broadcast_flag=True` for `entityExtract` but provides no example. Since this is a key performance tuning parameter, its usage should be demonstrated within a pattern.
  - **Suggested fix:** Update the `entityExtract` call in `Pattern 3: Lab-Based Analysis` to include the flag and a comment, as the code list is likely small.
    ```python
    # In Pattern 3, Step 2: Extract Lab Results
    e.hgb_labs.entityExtract(
        elementList=e.hgb_codes,
        entitySource=r.labSource.df,
        cohort=e.study_cohort,
        cacheResult=True,
        broadcast_flag=True  # Broadcast the small hgb_codes table for performance
    )
    ```

## P2 (clarity, style, cross-links, minor gaps)
- **Section:** `Pattern 3: Lab-Based Analysis`, Step 3
  - **What's wrong:** The comment `(groupby inferred from non-index columns)` is misleading. The `index` parameter explicitly defines the columns to group by.
  - **Suggested fix:** Change the comment to accurately describe the parameter.
    ```python
    # Get min, max, mean per patient (groupby defined by the `index` parameter)
    lab_summary = aggregate_fields(
        df=e.hgb_labs.df,
        index=['personid'],
        ...
    )
    ```

- **Section:** `Pattern 4: Encounter-Based Usage Analysis`, Step 2
  - **What's wrong:** The example for `calcUsage` lists `actualarrivaldate` and `servicedate` in the `fields` list and also uses them in the `dateCoalesce` expression. It may not be immediately clear to users that columns used in expressions like `dateCoalesce` must also be included in the `fields` list.
  - **Suggested fix:** Add a clarifying comment.
    ```python
    usage = calcUsage(
        df=e.cohort_encounters.df,
        # The `fields` list must include the index and any columns used by `dateCoalesce`.
        fields=['personid', 'actualarrivaldate', 'servicedate'],
        dateCoalesce=F.coalesce(F.col('actualarrivaldate'), F.col('servicedate')),
        ...
    )
    ```

- **Section:** `Pattern 9: Writing Output Tables`
  - **What's wrong:** The line `e.final_cohort.df = final_cohort` appears without explanation. This pattern of assigning a derived DataFrame to an `ExtractItem`'s `.df` attribute is powerful but not intuitive.
  - **Suggested fix:** Add a comment explaining the pattern.
    ```python
    # Assign the derived DataFrame to an ExtractItem defined in the control file.
    # This allows using ExtractItem methods like .to_csv() on the final cohort.
    e.final_cohort.df = final_cohort
    e.final_cohort.to_csv()
    ```

- **Section:** `Common Field Names Reference`, subsection `Demographics Fields`
  - **What's wrong:** The note `(raw field; standardized as marital_std in Pattern 5)` is correctly applied to `maritalstatus` but is missing for `gender`, `race`, and `ethnicity`, which are also standardized in Pattern 5.
  - **Suggested fix:** Add a similar note to the other fields for consistency.
    - `gender` - Gender (raw field; standardized as `gender_std` in Pattern 5)
    - `race` - Race (raw field; standardized as `race_std` in Pattern 5)
    - `ethnicity` - Ethnicity (raw field; standardized as `ethnicity_std` in Pattern 5)