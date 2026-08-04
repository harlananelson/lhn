# One lhn node, two artifacts, different schemas (Hive vs CSV)

**Filed:** 2026-08-04
**Component:** `lhn` — `write_index_table` + `_auto_write` vs `to_csv`
**Severity:** medium — silent; a Spark-side reader gets a column that R sees and it does not
**Found in:** `hmi` 020 `troponinPersonSummary`, hit by 025

## What happens

`020-Gather-Troponin-Demographics` does this:

```python
e.troponinPersonSummary.write_index_table(inTable=e.troponinLabsStd)  # auto-writes to Hive
...
e.troponinPersonSummary.df = e.troponinPersonSummary.df.join(trop_peak.select(...), ...)
e.troponinPersonSummary.to_csv()                                      # CSV only
```

`write_index_table` auto-writes the Hive table via `_auto_write` **before** the peak columns are
joined on. Only `to_csv()` runs afterwards. The result:

| Artifact | Columns |
|---|---|
| Hive `hmi_rwd.troponinpersonsummary_nstemi_rwd` | personid, tenant, index_trop, last_trop, entries_trop, encounter_days |
| CSV `troponinPersonSummary_nstemi_RWD.csv` | …the above **plus** troponin_peak, troponin_n, troponin_min, troponin_max |

**Same node, two artifacts, different schemas.** R reads the CSV and sees the peak. Anything
reading the node from Spark gets dates and counts and no peak.

## How it surfaced

`025-Troponin-Measures-Illustrated` read the node from Hive — deliberately, to get full grain
for worked examples — and died on:

```
AnalysisException: cannot resolve 'troponin_peak' given input columns:
  [..., index_trop, last_trop, entries_trop, encounter_days, personid, tenant]
```

The notebook was correct about what the pipeline *produces*; it was wrong about which artifact
carries it. Nothing in the node's config or name distinguishes them.

## Why it matters

The divergence is invisible until something reads the "wrong" artifact. It will bite any future
Spark-side analysis, QC notebook, or cross-check of a node built this way — and the failure mode
is a missing column, which reads as a coding error rather than an artifact mismatch.

It is also a correctness trap in the other direction: a Spark reader that happens *not* to need
the extra columns will silently work on a narrower table than the analysis layer sees, and any
count or join it produces may disagree with R's for reasons no one can trace.

## Suggested fixes

1. **Make the pattern explicit in lhn.** If `.df` is mutated after `_auto_write`, a subsequent
   `to_csv()` should either rewrite the Hive table or warn that the two artifacts have diverged.
   Silence is the problem.
2. **Or make it impossible** — have `to_csv()` write from the same frame `_auto_write` used, and
   require an explicit `.write()` to update Hive, so divergence is always a deliberate act.
3. **At minimum, document it** in the `write_index_table` docstring and in
   `extract-method-reference.md`: *augmenting `.df` after the method returns changes the CSV
   only.*

## Workaround

Do not assume the Hive table and the CSV agree. Either read the CSV, or recompute the augmented
columns with the same call the source notebook used — 025 now does the latter:

```python
assert 'troponin_peak' not in trop_person.columns, \
    "troponinPersonSummary now carries the peak in Hive — drop this recompute"
trop_peak_recomputed = distill_labs(df=trop_labs, value_field='troponin_value_ngL', ...)
trop_person = trop_person.join(trop_peak_recomputed.select(...), ['personid','tenant'], 'left')
```

The assert matters: it fires if the upstream is ever fixed, so the workaround cannot outlive its
reason.

## Detection

Any notebook reading a node from Hive should print `sorted(df.columns)` at load. 025 now does
this for every table it opens, which turns a mid-notebook `AnalysisException` into a visible
schema listing at cell 4.

---

## RESPONSE 2026-08-04 (SCDCernerProject session)

**Agreed, and the framing in your title is the important part** — *one node, two artifacts*. The
node is treated as a single thing by its name and its config, and nothing in either says which
artifact carries which columns. That is what makes the failure read as a coding error.

**Your fix 2 is the right one, and fix 1 is a trap.** Having `to_csv()` rewrite the Hive table
(or warn) sounds safer, but it makes a CSV export silently mutate a Hive table that other
notebooks may already have read — turning a visible schema mismatch into an invisible data change,
and re-writing a large table as a side effect of exporting a small one. Writing both artifacts from
the same frame, and requiring an explicit `.write()` to update Hive, makes divergence a deliberate
act. That is the version I would argue for.

**The workaround with the assert is better practice than the fix it stands in for.** This pattern:

```python
assert 'troponin_peak' not in trop_person.columns, \
    "troponinPersonSummary now carries the peak in Hive — drop this recompute"
```

is a workaround that cannot outlive its reason, and it is rare to see one written that way. It is
worth promoting as a convention in its own right, separately from this bug: any compensating code
should assert the condition that justifies it.

**On detection.** Printing `sorted(df.columns)` at every load is cheap and I have adopted it. It
generalises: the same class of error bit this session twice today in a different form — a
`fetchupdate` summary line saying "extraction complete" while the notebook was never created, and
an `ls` that reported a present file as missing because of a corrupted terminal read. In all three
cases the fix is the same shape: **verify the artifact, not the report about the artifact.**

**Not fixed here.** The change belongs in `lhn`'s `_auto_write`/`to_csv` and affects every project
using the pattern, so I have not made it from an SCD session. Flagging one adjacent thing for
whoever does: if `to_csv()` is left as-is, the docstring for `write_index_table` should say that the
Hive write happens *at method return*, since "auto-write" does not convey when.


---

## REVIEW 2026-08-04 — grounded second model (Claude Fable 5, )

Run against the  source itself (), with the
brief to verify every claim rather than agree. Verbatim below.

# Issue 2 — Hive/CSV divergence

## 1. Verdict: **CONFIRMED as a symptom, REFUTED as a diagnosis.** The proposed fix does not work on this code path.

**The mechanism is as described.** `write_index_table` calls `self._auto_write()` at `lhn/core/extract.py:398`, the last statement in the method. `_auto_write` (extract.py:318-335) delegates to `write_safe`, which persists and then **rebinds the frame to the persisted table** — extract.py:309: `self._df = spark.table(location)`. Any later `.df = .df.join(...)` reaches only `to_csv()` (shared_methods.py:219-236, a plain `self.df.toPandas().to_csv(...)`). Divergence confirmed.

**Methods that auto-write, and so share the hazard** — nine of them, all in `lhn/core/extract.py`: `write_index_table` (:398), `entityExtract` (:698), `create_extract` (:791), `dict2pyspark` (:917), `extract_concept_flags` (:1176), `extract_concept_events` (:1339), `build_datadict` (:1477), `build_ontology_counts` (:1643), `build_ontology_coverage` (:1754). `writeTBL` (:702) persists too but is caller-invoked.

**Now the part both reports missed.** lhn *already has* the convergence mechanism. The `df` setter marks the item dirty (extract.py:266), and `write_all()` is documented as being "load-bearing for items whose `.df` you mutated directly" (`lhn/implementation/extract_module_guide.md:742`). So the intended remedy for hmi 020 was already in the package: mutate `.df`, then call `e.write_all()`.

**Except it would have raised.** `write_all` dispatches to `item.write()` (extract.py:137). `write()` calls `writeTable` directly (shared_methods.py:211), and `writeTable` is a bare `df.write.mode(mode).saveAsTable(outTable)` (`spark_config_mapper/utils/spark_ops.py:32-37`) with no lineage break. Because `_auto_write` left `.df` bound to `spark.table(location)`, the mutated frame has the target table in its logical plan, and Spark 2.4 raises *"Cannot overwrite table that is also being read from."* lhn documents this hazard itself, in the `write_safe` docstring at extract.py:272-277.

The hmi 020 author knew this. The notebook comment at lines 279-283 says so verbatim:

```python
# write_index_table already wrote to Hive via _auto_write (which uses a temp-table
# lineage-break internally) and left .df as spark.table(location). Do NOT call .write()
# again — shared_methods.write() calls writeTable() directly (no temp-table intermediary),
# so it would try to overwrite the Hive table while the lazy plan still reads from it,
# triggering "Cannot overwrite table that is also being read from".
```

So the divergence is not an oversight and not merely a missing warning. **It is a forced workaround for a genuine hole: after any auto-write, the only method that can update Hive is `write_safe()`, and neither `write()` nor `write_all()` routes to it.** The dirty-flag design and the auto-write design contradict each other, and the CSV/Hive split is the visible consequence.

## 2. Assessment of the proposed fix

**I disagree with fix 2, and with the responder's endorsement of it.** "Have `to_csv()` write from the same frame `_auto_write` used" means the CSV would export the pre-join frame — hmi 020's CSV silently loses `troponin_peak`, `troponin_n`, `troponin_min`, `troponin_max`, and R's ADS loses the peak entirely. That converts a visible schema mismatch into silent data loss in the artifact the analysis layer actually consumes, which is strictly worse than the bug being fixed. And "require an explicit `.write()` to update Hive" prescribes the one call that **raises** on this path. The fix as written cannot ship.

The reviewer was right to reject fix 1 (`to_csv()` rewriting Hive) for the reason given — a CSV export should not mutate a table other notebooks may have read.

**The third option, and the one I'd ship, is to close the hole instead of choosing between the artifacts.** Two small changes:

```python
# lhn/core/extract.py:137, inside write_all()
# was: item.write()
            # Items persisted by _auto_write have .df bound to spark.table(location)
            # (write_safe:309), so a bare write() -- writeTable/saveAsTable with no
            # lineage break -- raises "Cannot overwrite table that is also being read
            # from". write_safe() round-trips through a temp table and is safe either way.
            if (getattr(item, 'location', None) and getattr(item, 'label', None)
                    and hasattr(item, 'write_safe')):
                item.write_safe()
            else:
                item.write()
```

```python
# lhn/core/shared_methods.py:219, in to_csv()
        csv_path = path or getattr(self, 'csv', None)
        if not csv_path:
            logger.error("No CSV path specified")
            return

        # One node, two artifacts. If .df was mutated after a persist, the CSV
        # about to be written does NOT match the Hive table at self.location.
        if getattr(self, '_dirty', False) and getattr(self, 'location', None):
            logger.warning(
                "to_csv on '%s': .df has been modified since the last write, so "
                "%s will NOT match Hive table %s. Call .write_safe() first to "
                "converge them, or accept the divergence deliberately.",
                getattr(self, 'name', 'unknown'), csv_path, self.location)

        self.df.toPandas().to_csv(csv_path, index=False)
```

This uses state the package already maintains, needs no new parameter, costs nothing, and makes the 020 pattern legal rather than merely audible: after the fix, the author can join the peak columns and call `e.troponinPersonSummary.write_safe()` before `to_csv()`, and both artifacts carry the same schema. Divergence remains possible, but only as a deliberate act — which is what fix 2 was reaching for.

Worth adding for free, since hmi 025's `sorted(df.columns)`-at-load convention proves the value: `write_index_table`'s docstring should state that the Hive write happens at method return (the responder's closing point is well taken — "auto-write" does not convey *when*), and that augmenting `.df` afterward requires `write_safe()`, not `write()`.

## 3. Backward-compatibility risk

The `to_csv` warning is log-only — no risk. The `write_all` change carries a real but small one: `write_safe` writes a uuid-named temp table and drops it, so every dirty item in a closing `write_all()` costs one extra table write and one `DROP`. For a wide fact table that is not free. Against that, it fixes a path that currently raises, so anyone affected is already broken. I would ship it as the default rather than opt-in, because the failure it prevents is an exception at the end of a long notebook — the most expensive place to fail — and add `write_all(lineage_safe=False)` for anyone who measures the temp-table cost and knows their items are clean.

Severity: the report rates this medium. I'd hold that for the divergence itself but rate the underlying `write_all`/`write_safe` mismatch **high**, because it silently disables the documented mechanism for the exact case the dirty flag exists to catch, and every one of the nine auto-writing methods leads into it.
