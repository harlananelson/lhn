

**Verdict**
APPROVE with changes

**Must-fix before implement**
1. Update the `push_discern` concept_flags path to collect *unique* concepts per context (use `dict` + list or `defaultdict(set)` then `list` in stable order) before calling `_push_discern(..., concepts=concept_list)`. Current plan text says "union of all flags" but code sketch uses simple append without deduplication.
2. Make the explode-then-`isNotNull` version the *default* implementation (not the higher-order `filter` version). Add an explicit comment referencing Spark 2.4.4 higher-order function behavior with UDFs inside array elements.
3. Add a unit test (or HDL smoke assertion) that a single source row matching two concepts that share the *same* `flag` value produces *two* output rows (both with that flag). This must match current union semantics; document it in the docstring.
4. In the new `_concept_flag_when_exprs` helper (or inline), ensure `_keep_expr` / column resolution runs *once* on the final filtered source (after cohort + hist window) and that the final `.select()` includes the exploded `flag` plus all kept columns. The plan sketch is close but must be exact to avoid regression on nested `retained_fields`.

**Should-fix / nits**
- Extract the private `_lit` and `_keep_expr` helpers once and share them with `extract_concept_flags` (tiny duplication is acceptable per plan, but the functions are already identical).
- Add one sentence to the `extract_concept_events` docstring linking the new shape back to the old `hnelson3` `add_concept_indicators` + `stack` pattern and stating "single physical scan; one output row per (source row × matching flag)".
- In phase-2 note, explicitly say the `has_any_*_in_context` pre-filter must be a single OR expression (not a Python loop of filters) to preserve the one-scan guarantee.
- Update the regression note with the chosen implementation SHA after merge.

**Agent recommendation**
Harlan (the lhn maintainer / PySpark domain expert who wrote the original methods and understands the foresight UDFs, Catalyst on 2.4.4, and hmi caller patterns) should implement this. A grounded second model (Grok or equivalent) should perform code-level PR review focused on the physical plan and edge cases. A general-purpose LLM without deep Spark 2.4 + Cerner foresight experience should NOT implement it (too easy to introduce subtle quoting, plan-splitting, or contract-breaking bugs).

**Brief answers to the 10 questions**
1. Yes, the diagnosis is correct and directly supported by the current code in `extract-discern-methods.py` (explicit Python `for` loop over `flags`, per-branch `.filter(...)`, then `unionByName`). Spark 2.4.4 does not share file scans across union branches when predicates contain UDF calls (`has_concept_in_context`). The `extract_concept_flags` method in the same file already demonstrates the single-pass alternative via multiple aggregate expressions. Caveat: on a perfectly optimized plan with predicate pushdown the difference might be smaller, but the 88k-task clinical_event stages and 21600s kills in `DISCERN-SCAN-REGRESSION.md` make it highly relevant.
2. Yes, the tag+explode (or array-of-when + filter + explode) is the correct canonical fix; it evaluates all predicates in one project, preserves the exact row-multiplication contract, and matches the old `add_concept_indicators` + stack shape from `hnelson3-discern-excerpts.py`. Edge cases are handled: same flag twice produces duplicate rows (matches current union behavior — plan correctly keeps this for v1); multi-match produces one row per flag; empty flags already raise `ValueError` in both methods; null code structs are treated as non-match by the UDFs (SDK behavior). Minor missing edge: a row with null flag name (validation should catch).
3. Yes, it is safe and aligns with the foresight SDK (`foresight-discern.py` explicitly supports `concepts=` as a `HashSet` to reduce broadcast memory; `push_discern` already accepts it on the explicit-context path). Failure modes are limited: missing concept still crashes at UDF evaluation time (same as today); downstream code that secretly relied on "extra" concepts from a full context would break, but that is outside the documented contract.
4. Worth it when the raw table has low concept prevalence (typical for `clinical_event`); the pre-filter can prune before the per-flag `when()` expressions run. It does not interact badly with indicator tags if expressed as one OR expression in the same plan (still one scan). Measure on 066 first, as the plan says; phase 1 (tag+explode) is sufficient to eliminate the N-scan regression.
5. Existing callers will not break if the output contract is preserved. Must stay identical: output grain (one row per source-row × matching flag, including duplicates when two concepts map to the same flag name — see 066 echo flags), column set (`index_fields` + optional `datefield` + `flag` + retained fields), error-raising behavior on missing keys / bad config / missing code column, and YAML `concept_flags` shape. No public signature change.
6. Both are supported on Spark 2.4.4. Recommend **explode-then-null-filter** as the default: it is simpler, avoids lambda-in-string quoting subtleties, produces a straightforward physical plan, and matches common pre-3.0 patterns. Higher-order `filter` is cleaner but should be the optional/verified path.
7. The offline (expression builder + mock push) + HDL smoke (2-concept sample, UI scan confirmation) + full 066 re-run steps are solid. Add: (a) deterministic row-count + content match test vs a frozen legacy union path on a small sample, (b) explicit test for same-flag multi-concept duplicate rows, (c) test with nested `retained_fields` struct path, (d) `spark.explain()` or stage-ID comparison to guarantee one major scan.
8. Medium severity: Catalyst still splits the plan or repeats scans when UDFs live inside `array(when(...))` (must verify on HDL). Medium: downstream notebooks assuming distinct flags per source row could be surprised (plan preserves semantics but should document). Low: quoting edge cases in `_lit` for concept names containing special characters. Low: interaction between pre-window cohort join and the new tagging.
9. See Agent recommendation section above.
10. See Verdict / must-fix list above.

**CONVERGENCE: NONE**