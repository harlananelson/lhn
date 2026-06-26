

**Review of `docs/PATTERNS.md`**

## P0 (design decisions — surface, do not auto-fix)
- **Overview + Patterns 1, 2, 4, 6 (core three-step pattern and `write_index_table` sections)**: Heavy reliance on `000-control.yaml` `projectTables` config ( grain via `indexFields`, `datefieldPrimary`, `code`, `max_gap`, `sort_fields`, etc.) for `ExtractItem` method behavior vs. passing the same values as kwargs to the standalone `write_index_table` function. This is a foundational contract decision between config-driven vs. explicit Python. Surface to the team for explicit sign-off before further hardening in the harness.

## P1 (inaccuracies, contradictions, missing critical facts)
- **"Standard notebook setup" section (second code block) + repeated imports in Patterns 3, 4, 7**: The standard bootstrap already does `from lhn.header import spark, F, Window`. Patterns 3/4/7 then repeat `import pyspark.sql.functions as F` (or `from pyspark.sql import functions as F`).

  **What's wrong**: Direct contradiction of the "bind spark first, then pipeline_setup" rule and creates name shadowing risk. The gate standards lint expects clean, minimal imports.

  **Exact suggested fix**: Delete all `import ... functions as F` lines from the individual pattern examples. Add a note: "All patterns below assume the standard notebook setup (which already provides `spark`, `F`, and `Window`) has been run."

- **Pattern 9 ("Writing Output Tables") code block**:
  ```python
  e.final_cohort.df = final_cohort
  e.final_cohort.to_csv()
  ```

  **What's wrong**: Violates the documented ExtractItem contract emphasized earlier ("call them on the item `e.X`, not on its DataFrame" and "use configured `.csv`/`.location` defaults"). Manually mutating `.df` after the fact is not a pattern validated by `check_notebook.py` or the generated API reference. Also mixes `writeTable` (standalone) with ExtractItem methods without explaining the intended contract.

  **Exact suggested fix**: Replace the block with:
  ```python
  # If final_cohort is declared in projectTables, use the ExtractItem workflow:
  e.final_cohort.writeTable(...)  # or appropriate method
  e.final_cohort.to_csv()         # uses configured location
  ```
  Add comment: "See `api_reference.md` for `to_csv`, `writeTable`, and ExtractItem output contract. Never mutate `.df` directly after computation."

- **"Common Field Names Reference" section (entire table block)**: Lists concrete column names (`conditioncode_standard_id`, `servicedate`, `administrationdate`, etc.) as if authoritative.

  **What's wrong**: Directly contradicts the P1 rule "Confirm column names; don't guess" and the mechanical gate behavior that resolves `r.*`/`e.*` columns from the live catalog. Risks notebooks failing gate lint or using stale names.

  **Exact suggested fix**: Insert at the very top of the section (before "### Encounter Fields"):
  > **Critical**: These are illustrative/common names only. You must resolve the exact column names from the live HDL catalog for each `r.*` table using `check_notebook.py` or the data dictionary before writing code. Never hard-code from this reference.

## P2 (clarity, style, cross-links, minor gaps)
- **"4. Use the real API — correct names and kwargs" + "Use lhn methods" table**: Only shows a few corrected names (`entityExtract`, `elementList`).

  **What's wrong**: Missing mention of other validated kwargs shown in the patterns themselves (`cacheResult=True`, `find_method='regex'`, `cohort=`, `broadcast_flag=True`).

  **Exact suggested fix**: After the table add: "See the full current contract in `~/projects/hdl-harness/docs/api_reference.md`. The gate will reject any unknown kwargs or near-miss method names."

- **"HDL deploy, execute, render, and review" section (the long `hdl_run.py` example)**: Uses a specific notebook path (`allison/054-derm-cohort-identification.txt`) and full absolute paths.

  **What's wrong**: Reduces reusability of the example; the orchestrator doc (`txtarchive-hdl-integration.md`) is referenced but not linked.

  **Exact suggested fix**: Change the example command to use a placeholder (`<project>/<notebook>.txt`) and add a cross-link: "Full workflow and kernel constraints are in [`txtarchive-hdl-integration.md`](~/projects/hdl-harness/docs/txtarchive-hdl-integration.md)."

- **Performance Tips #2**: "`broadcast_flag=True` for small lookup tables".

  **What's wrong**: Does not show which method accepts the flag or the correct spelling (`broadcastFlag` vs `broadcast_flag`).

  **Exact suggested fix**: Change to: "`broadcastFlag=True` (or `broadcast_flag=True` per current API — confirm in `api_reference.md`) on `entityExtract()` or `create_extract()` for small lookup tables."

**Summary**: The document is strong, closely aligned with harness rules, `pipeline_setup`, txtarchive raw-cell YAML requirements, and the `hdl_run.py --all --fix-loop` workflow. The issues above are the only concrete, actionable discrepancies.