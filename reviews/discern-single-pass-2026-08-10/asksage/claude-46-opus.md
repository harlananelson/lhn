



## Verdict

**APPROVE with changes** — the plan is well-diagnosed, well-structured, and implementable after the must-fixes below.

---

## Must-fix before implement

1. **Resolve `_keep_expr` BEFORE the loop is removed.** The current code calls `_keep_expr(c, available)` inside the per-row loop against `matched.columns`. The plan's pseudocode moves it outside the loop and calls it against `source_df.columns`. This is correct, but the plan must **explicitly state** that the `available` list is `source_df.columns` (post-cohort-join, post-window), not the pre-join columns. The plan's §4.1 code block does `available = source_df.columns` — confirm this is post-cohort/window `source_df`, not the raw input.

2. **Handle the case where `concept_flags` has duplicate flag names in `extract_concept_events`.** The plan acknowledges (§4.2) that same-flag duplicates produce duplicate output rows matching union semantics, but `extract_concept_flags` **raises `ValueError` on duplicate flag names**. The events method currently does NOT validate for duplicates (it's fine because union naturally handles it). The plan should explicitly state that `extract_concept_events` must NOT add a duplicate-flag-name validation (unlike `_flags`), since duplicate flag names are the intentional design (e.g., `echo` from two different concepts). Add a comment in the implementation explaining this difference.

3. **`push_discern` subsetting: deduplicate concept lists before passing.** The plan says `to_push[row['context']].append(row['concept'])` then "unique-preserve order." The pseudocode doesn't show the actual dedup. The implementation must include `list(dict.fromkeys(concept_list))` or equivalent before the `_push_discern` call, because the foresight SDK builds a `java.util.HashSet` internally (so duplicates are harmless at the JVM level) but the `logger.info` output and the conceptual contract should be clean. Make this explicit in the plan.

4. **`_lit` escaping is duplicated across both methods.** The plan §4.5 mentions extracting it but says "fine if shared helper is noisy." Since both methods now use identical `_lit` logic, extract it to module scope or a shared private function. This is a must-fix because duplicated escaping logic is a security-adjacent concern (SQL injection into `F.expr`) — if one copy is updated and the other isn't, the mismatch creates a vulnerability window.

5. **The plan's §4.1 pseudocode uses `source_df.select(*keep_cols, F.array(*tag_exprs).alias('_flags'))` but `keep_cols` may include a `F.col(field).alias(...)` expression from `_keep_expr`.** If a retained field is dotted (e.g., `typedvalue.numericValue.value`), the alias is `typedvalue_numericValue_value`. The current loop evaluates `_keep_expr` inside each branch. In the single-pass version, this runs once — which is correct and better. But the plan must note that the column name `_flags` must not collide with any `keep` field. Add a check or use a more distinctive internal name like `__lhn_flags__`.

---

## Should-fix / nits

1. **Spark 2.4 `filter(array, x -> x is not null)` vs explode-then-null-filter:** The plan lists both but doesn't commit to a default. Recommendation below (Q6), but the plan should pick one as the implementation default, with the other as a documented fallback. Don't leave this as an implementor choice.

2. **§4.2 table says "Two concept_flags rows share the same flag name... two rows both tagged echo"** — this is correct for union semantics but should note that `array_distinct` in a future phase would change this. The plan does note this but could be more explicit that v1 must NOT apply `array_distinct`.

3. **Logging:** The plan should specify that the single-pass path logs how many tag expressions were built (i.e., `logger.info("extract_concept_events: %d concept predicates in single-pass plan", len(flags))`). This is critical for debugging on HDL where you can't inspect the Spark plan easily.

4. **§6.1 testing is weak on the Spark-side.** "Structural tests" of expression count are necessary but not sufficient. Consider a test with a mock UDF registered via `spark.udf.register("has_concept_in_context", lambda *_: True)` in a local SparkSession to verify the explode mechanics produce correct row multiplication. This is feasible even without the foresight JAR.

5. **§8 Risks table should include:** "Concept name containing a single quote after `_lit` escaping" — the `_lit` function replaces `'` with `''`, which is correct for SQL string literals inside `F.expr`, but the plan should confirm this is the right escaping for the Spark SQL parser (it is — Spark SQL uses standard SQL string escaping with `''`).

6. **Plan §7 step 7** says "ensure HDL env picks up new lhn" — should specify the mechanism (pip install from git tag? kernel rebuild? `importlib.reload`?). This is operationally critical.

---

## Agent recommendation

**Implementor:** A senior coding agent (Claude Opus or Sonnet 4) with access to the full `lhn` repo, running in an agentic coding environment. The implementor must:
- Have full file access to `lhn/core/extract.py`
- Be able to run `pytest` locally
- Follow the plan's §7 sequence strictly

**Reviewer:** A second model (or the same model in a separate session) should review the PR diff against:
- The output contract (schema, row multiplication semantics)
- Spark 2.4.4 API compatibility
- The `_keep_expr` resolution change

**What should NOT implement this:**
- A lightweight/fast model (e.g., Haiku-class) — the change touches escaping logic, UDF interaction, and Spark plan semantics that require careful reasoning about edge cases.
- A model without access to the full `extract.py` file — the context window must include the complete method and its callers.
- An agent that cannot run tests — the push_discern mock test is essential before merge.

---

## Answers to the 10 review questions

### 1. Correctness of the diagnosis

**Yes, the N-scan-from-union diagnosis is real and well-documented for Spark 2.4.4.** Spark's Catalyst optimizer in 2.x does not perform "scan sharing" across union branches — each branch independently plans its file scan [1]. This is a known limitation that was partially addressed in Spark 3.x with improved plan reuse, but on 2.4.4 it is fully expected behavior.

**Caveats:**
- If the source DataFrame were `.cache()`/`.persist()` before the loop, the rescans would hit the cache rather than re-reading files. However, caching the full windowed+cohort-joined clinical_event table is likely infeasible in memory on this cluster, and the plan correctly identifies this as unreliable (§10, "cache" rejected).
- The 88,456-task stage count and timing math (1 scan ≈ 8h, 4 scans ≈ 32h, killed at 6h = ~28% of one scan) is internally consistent with the Spark progress bar evidence.
- The diagnosis correctly identifies that `extract_concept_flags` does NOT have this problem because it uses a single `groupBy(...).agg(...)` with all concept predicates as parallel aggregate expressions.

### 2. Algorithm — tag+explode

**The algorithm is correct and is the right fix.** It mirrors the proven `add_concept_indicators` pattern from `hnelson3.py` (per-concept boolean columns in a single pass) adapted to long-form output via explode.

**Edge cases:**

| Edge case | Handled? | Notes |
|---|---|---|
| Same flag name from two concepts, both match same row | ✅ | Produces 2 rows (union semantics preserved). Plan §4.2 documents this. |
| Multi-match: row matches N of M flags | ✅ | `F.array(when(...), when(...), ...)` → array of N non-nulls → explode to N rows |
| Empty `concept_flags` | ✅ | Already raises `ValueError` before reaching the algorithm |
| Null code struct in source row | ✅ | `has_concept_in_context` on a null struct returns false (UDF contract from foresight); `F.when(false, lit(...))` → null → filtered out |
| Row matches zero concepts | ✅ | All array elements null → filtered by `size > 0` or `isNotNull` after explode |
| Single concept_flag entry | ✅ | Array of length 1, explode works fine |

**One concern:** If `concept_flags` is very large (dozens of concepts), the `F.array(*)` of `F.when` expressions could create a wide projection. This is unlikely in practice (hmi configs have 1–10 concepts) but worth a log warning if `len(flags) > 20`.

### 3. push_discern subsetting

**Safe with caveats.** The foresight SDK's `push_discern` explicitly supports `concepts=` as a subset parameter — the Java side builds a `HashSet` and filters. The old production code (`hnelson3-discern-excerpts.py` lines from `add_ontology_count`) **always** passed `concepts=conceptName` to `push_discern`, never `None`.

**Failure modes:**
- **Concept name typo in config:** Still crashes at action time with `Py4JJavaError` — same as today. No regression.
- **Concept not in tabulation for that context:** Same crash — same as today.
- **Caller relying on side-effect of full context broadcast:** If someone called `push_discern(concept_flags=...)` and then separately used `has_concept(code, 'SOME_OTHER_CONCEPT')` relying on the full context being loaded — that would break. However, this is not a documented or supported pattern, and the method docstring says "only listed concepts." Risk: **low**.
- **Two `ExtractItem`s sharing a Spark session and one pushing a subset that shadows the other's full context:** The `push_discern` SDK uses a stack (`push`/`pop`). If item A pushes context X with concepts [A,B] and item B later pushes context X with concepts [C,D], does B's push replace A's broadcast? Per the foresight SDK, `push_discern` re-broadcasts — so yes, B would shadow A. But this is the same risk that exists today (two items pushing the same context). The plan does not introduce new risk here.

### 4. has_any prefilter (phase 2)

**Worth it when:** The concept predicates are expensive (JVM UDF call per row per concept) and the selectivity is low (most source rows match NO concept). A single `has_any_concept_in_context(code, array(...), context)` call is one UDF invocation that short-circuits, versus N `has_concept_in_context` calls in the when-expressions.

**When it's NOT worth it:** When selectivity is high (most rows match) or N is small (N=1–3, the UDF overhead of one `has_any` + N `has_concept` ≈ N+1 calls vs just N calls).

**Interaction with indicator tags:** None — `has_any` is a pre-filter (drops rows before the tag expressions run). The tag expressions only run on surviving rows. No bad interaction. The plan correctly separates this as phase 2.

**Recommendation:** Implement phase 1 (tag+explode) first. Measure 066 runtime. If it fits in a platform day, phase 2 is unnecessary. If it's still tight, add the `has_any` pre-filter as a single OR expression across contexts.

### 5. API / output contract

**No breakage for existing callers.** The plan preserves:
- Method signature (no changes)
- Output schema: `index_fields + datefield + flag + retained_fields`
- Row multiplication semantics: one row per (source row × matching flag)
- Config shape: `concept_flags: [{flag, concept, context}, ...]`
- Validation: same ValueError on missing keys, missing code column, missing datefield

**One subtlety:** The column ordering might change (current loop produces `select(*keep_exprs, flag)` per branch; new approach produces `select(*keep_cols, explode('_flags').alias('flag'))`). Column ordering in Spark DataFrames is generally not relied upon by downstream consumers (they reference by name), but if any caller does positional access, this could break. Risk: **very low**.

**The `_flags` internal column is dropped** before returning, so no schema pollution.

### 6. Spark 2.4.4 — filter(array, λ) vs explode-then-null-filter

**Recommendation: Use explode-then-null-filter as the default.**

Reasons:
- `filter(array, x -> x is not null)` is a Spark 2.4 higher-order function and IS available, but its behavior with the Catalyst optimizer on 2.4.4 is less battle-tested than `explode` + `filter`.
- The explode-then-null-filter approach is simpler to reason about and debug.
- The explode path (`F.explode(F.array(*tag_exprs)).alias('flag')` then `.filter(F.col('flag').isNotNull())`) is a single Spark plan node (Generate + Filter) vs the higher-order function path which is two plan nodes (Project with `filter()` + Generate).
- The performance difference is negligible — the dominant cost is the UDF evaluation in the `F.when` expressions, not the array manipulation.

```python
# Recommended default (Spark 2.4.4-safe, proven)
result = (
    source_df
    .select(*keep_cols, F.explode(F.array(*tag_exprs)).alias('flag'))
    .filter(F.col('flag').isNotNull())
)
```

The higher-order `filter` approach has one advantage: it avoids generating rows for non-matching concepts (explode of a 4-element array produces 4 rows, then null-filter drops non-matches; `filter` + `explode` only produces matching rows). For N=4 this is irrelevant. For N=50, the `filter` approach would be measurably better. Since hmi configs have N ≤ 10, recommend the simpler path.

### 7. Testing

The offline + HDL smoke steps are **necessary but not sufficient**.

**What I would add:**

1. **Local SparkSession integration test with mock UDF.** Register a Python UDF `has_concept_in_context` that returns `True` for known (code, concept, context) tuples. Build a 10-row DataFrame with a struct column. Run `extract_concept_events` and verify:
   - Row count matches expected (multi-match produces correct multiplication)
   - `flag` values are correct
   - Retained fields (including dotted) are preserved
   - Zero-match rows are excluded

2. **Property-based test:** For any `concept_flags` of length N, the output row count should be ≤ N × input row count (no row can match more than N flags).

3. **Regression test for `push_discern` kwargs:** The mock test in §6.1.3 is good but should also verify that `concepts` is a list (not `None`) and that it's deduplicated.

4. **Schema assertion test:** Output DataFrame schema must be exactly `index_fields + [datefield] + [retained_fields] + ['flag']` — no extra columns (especially no `_flags`).

5. **A/B comparison on HDL:** Even without a `_legacy_union` flag, run both old and new code on a **tiny** sample (100 rows) and `subtract` the results. Zero difference = confidence.

### 8. Risks the plan underweights

| Risk | Severity | Notes |
|---|---|---|
| **UDF evaluation order in `F.array(F.when(...))`** — Spark may evaluate ALL when-expressions for every row even if using short-circuit `filter`. The plan assumes this but doesn't mention the CPU cost explicitly. | **Low** | Accepted cost; still 1 scan vs N scans. UDF calls are O(N) per row either way. |
| **Column name collision: `_flags` or `flag` already exists in source.** | **Medium** | If source has a column named `flag` or `_flags`, the new code silently shadows it. Add a check or use `__lhn_concept_flags__` as the internal array name. The `flag` output column name is part of the API contract so can't change, but should error if source already has `flag`. |
| **Spark 2.4.4 higher-order function codegen bugs** — There are known issues with lambda serialization in Spark 2.4.x under certain JVM/Scala configurations. | **Low** | Mitigated by recommending explode-then-null-filter as default. |
| **`push_discern` concept subsetting changes broadcast state for subsequent cells in the same notebook session.** If a notebook calls `push_discern` via `extract_concept_events`, then later does a raw `has_concept(code, 'SOME_CONCEPT')` expecting the full context, it will fail. | **Medium** | Document this. The old behavior (full context) was more permissive. The new behavior is correct per contract but could surprise. |
| **The plan doesn't address what happens if `extract_concept_events` is called twice in the same session with different concept_flags but overlapping contexts.** The second `push_discern` call pushes a new subset, potentially shadowing the first. | **Low** | Same risk as today; `push`/`pop` stack handles this, but only if callers `pop` between uses (they don't). Not a regression. |
| **Kernel deployment (step 7) has no rollback plan.** If the new lhn is buggy, 066 + 035 + 055 all break. | **Medium** | Add: "Keep the pre-fix lhn commit SHA noted; if smoke fails, `pip install` the old SHA." |

### 9. Implementation agent recommendation

**Implement:** A senior agentic coding model (Claude Opus-class) with:
- Full repo access to `~/projects/lhn/`
- Ability to run `pytest` and `python -c "import lhn"` locally
- Access to the plan and all reference files

**Why Opus-class:** The change involves:
- Careful Spark API usage (F.expr, F.array, F.when, F.explode — all must be Spark 2.4.4-compatible)
- SQL injection prevention in `_lit`
- Mock-based testing of JVM UDF interaction
- Maintaining exact output contract

**Review:** A second model (or the same model in a separate session) should review the PR diff. The human operator should verify the HDL smoke test.

**What should NOT implement this:**
- A fast/lightweight model (Haiku-class) — insufficient reasoning about Spark plan semantics and edge cases
- A model without file access — needs to see the full `extract.py` in context
- A general-purpose agent without PySpark experience — the Spark 2.4.4 constraints are non-obvious

### 10. Verdict

**APPROVE with changes.** Required changes as a numbered checklist:

1. ☐ Use `source_df.columns` (post-cohort, post-window) for `_keep_expr` resolution; state this explicitly
2. ☐ Do NOT add duplicate-flag-name validation to `extract_concept_events` (unlike `_flags`); add comment explaining why
3. ☐ Deduplicate concept lists in `push_discern` subsetting before the `_push_discern` call
4. ☐ Extract `_lit` to a shared private function (module-scope or class-level)
5. ☐ Use a collision-resistant internal column name (e.g., `__lhn_concept_flags__`) instead of `_flags`
6. ☐ Commit to explode-then-null-filter as the default implementation (not higher-order `filter`)
7. ☐ Add a `logger.info` line stating the number of concept predicates in the single-pass plan
8. ☐ Add a check/error if source already has a column named `flag`
9. ☐ Specify kernel deployment mechanism and rollback procedure in §7

---

`CONVERGENCE: NONE`