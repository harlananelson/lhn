# `distill_labs`: `unit_field` does not prevent unit pooling, and there is no first/last VALUE

**Filed:** 2026-08-04
**Component:** `lhn.analytics.distill_labs`
**Severity:** high for the unit half — silently wrong summary statistics
**Found in:** `hmi` 035-Labs, 066-Echo-LVEF, 040-Post-PCI-Troponin

Two separate problems in one function.

---

## Problem 1 — `unit_field` reports a unit, it does not group by one

### The belief

Both `035-Labs` and `066-Echo-LVEF` carried this comment, written in good faith:

> Passing `unit_field` avoids silently pooling values across units.

### The reality

```python
if unit_field is not None:
    aggs.append(F.first(F.col(unit_field), ignorenulls=True).alias(f'{code}_unit'))
```

`unit_field` adds `F.first(unit)` to the aggregation. It **records** whichever unit happens to
come first and nothing else. `_min`, `_max`, `_median` and `_peak` are computed across **all**
rows regardless of unit.

So for a multi-unit analyte the summary statistics are pooled across incompatible scales, and
`<code>_unit` names one of them — which is worse than no unit column, because it looks
authoritative.

### Blast radius in `hmi`

- **040 §3** passed the **raw** `typedvalue_numericValue_value` (mixed ng/mL and ng/L) rather
  than the harmonized `troponin_value_ngL`. `hs_tni_post_peak` — an input to the hemorrhagic
  classification, already in the analytic dataset — spanned **0.003 to 458,188** for a single
  assay type. The section heading even hedged *"ng/L if harmonized"*. Fixed 2026-08-04.
- **035** summarizes creatinine, eGFR, HbA1c and BNP from raw values with **no harmonization
  step at all**. Creatinine (mg/dL vs µmol/L, 88×) and HbA1c (% vs mmol/mol) are the material
  risks; BNP is 1:1 pg/mL to ng/L and eGFR is usually single-unit. A unit-mix tabulation was
  added to 035 to measure it; not yet run.
- **066** takes LVEF the same way. Low risk (% is near-universal) but unverified.

The docstring is actually correct — *"`value_field` must be NUMERIC and in ONE harmonized
unit… it is the caller's job"* — so the contract was stated and then contradicted by the
parameter's apparent purpose. `unit_field` reads like a safety feature. It is a label.

### Suggested fixes

1. **Rename or re-document.** `unit_field` → something that cannot be read as "handle units",
   or a docstring line saying explicitly: *this records a unit; it does not harmonize or
   partition by one.*
2. **Warn on heterogeneity.** If `countDistinct(unit_field) > 1` within an index group, emit a
   warning naming the analyte and the units found. Cheap, and it converts a silent wrong number
   into a visible one.
3. **Consider `require_single_unit=True`** as an opt-in that raises instead of warning, for
   callers that know they should have harmonized upstream.

---

## Problem 2 — no first/last VALUE, only first/last DATE

`distill_labs` emits `<code>_first_date` and `<code>_last_date` but no corresponding values. So
a **serial delta** — last minus first, time-ordered — cannot be derived downstream from what it
exports.

### Why it matters

`hmi` needed a rise/fall measure. What the analytic dataset has instead is
`troponin_delta = troponin_max - troponin_min`: the peak-to-nadir **range**, which is not the
same quantity and cannot be negative. Measured on 385,465 patients with ≥2 troponins:

- **82.3%** have a peak−nadir that differs from the time-ordered last−first.
- **148,839 (38.6%)** have a **negative** serial change — troponin falling across the record —
  which a range structurally cannot express.

The Universal Definition of MI is built on a rise *and/or fall*, so the distinction is clinical,
not cosmetic.

### Suggested fix

Add `<code>_first_value` and `<code>_last_value` alongside the existing date columns. It is a
natural extension of what the function already computes and would let any caller derive a serial
delta without bespoke window code.

A clean implementation avoids window frames entirely — `min`/`max` over a struct sorts by the
first field:

```python
F.min(F.struct(F.col(date_field).alias('dt'), F.col(value_field).alias('val'))).alias('_first')
# then _first.val is the value at the earliest timestamp
```

One shuffle, no per-partition frames.

### Caveat worth documenting alongside it

A record-wide serial delta is often not the clinically meaningful one. In `hmi` the median span
between a person's first and last troponin was **536 days**. Whatever is added should make it
easy to scope to an episode or a fixed window — `distill_labs` already supports
`index_date_field` and `post_window_days`, so the pieces exist.

---

## RESPONSE 2026-08-04 (SCDCernerProject session) — both confirmed, and the cause is worse than naming

**Problem 1 verified in source.** `lhn/analytics.py` builds `aggs` as `count/min/max/median/peak`
over `_v` with no partitioning, then appends `F.first(unit_field)` only when the parameter is
given. Statistics are pooled across units exactly as you describe.

**The reason the belief was reasonable is not in your report, and it matters.** The same parameter
name means the *opposite thing* elsewhere in the same package:

```python
# lhn/features.py:141-142  -- PARTITIONS by unit
if unit_field and unit_field in baseline_data.columns:
    group_fields.append(unit_field)

# lhn/analytics.py         -- merely RECORDS a unit
aggs.append(F.first(F.col(unit_field), ignorenulls=True).alias(f'{code}_unit'))
```

So `unit_field` **does** prevent pooling in `features.py` and **does not** in `distill_labs`. A
caller who learned the parameter from one function and used it in the other is not misreading a
name — they are relying on a contract the package establishes and then silently breaks. That
reframes fix 1: renaming in `distill_labs` alone leaves the inconsistency, it just moves it. Either
`distill_labs` should partition too, or the two should be named differently, and whichever is
chosen should be stated in both docstrings.

Your fix 2 (warn when `countDistinct(unit_field) > 1` within an index group) is the right
minimum regardless, because it converts a silently wrong number into a visible one and costs one
aggregation.

**Problem 2 verified.** `F.min('_d')`/`F.max('_d')` give `_first_date`/`_last_date`; there is no
value counterpart. Your struct-ordering implementation is the right one — `F.min(F.struct(dt, val))`
sorts on the first field and gets the value at the earliest timestamp in a single shuffle, with no
window frame. That matters here specifically: the `hmi` 068 utilization notebook already crashed
once on per-partition frames over a full encounter table.

**The 82.3% / 38.6% figures are the persuasive part.** A range cannot be negative, so a measure
built on rise *and/or fall* is not derivable from `max − min` for the 148,839 patients whose
troponin falls. That is a correctness argument, not a convenience one.

**Not fixed here.** Both changes belong in `lhn` and touch a function used by several projects;
I have not made them unilaterally from an SCD session. The verification above is offered so
whoever does has the source references and the cross-function inconsistency in hand.

One caution on your closing caveat, which I would put more strongly: with a median 536 days between
first and last troponin, a record-wide serial delta is not merely "often not the clinically
meaningful one" — for MI it is usually meaningless. If `_first_value`/`_last_value` are added, they
should probably be documented as episode-scoped quantities that require `index_date_field` to be
interpretable, so the next caller does not repeat the pooling mistake in the time dimension instead
of the unit dimension.


---

## REVIEW 2026-08-04 — grounded second model (Claude Fable 5)

Run with `call-claude.sh -m claude-fable-5 --mode thorough --effort high --cwd ~/projects/lhn
--scope repo`, so the model read this repo's actual source rather than working from the reports.
Briefed to verify every claim and to say plainly where a report was wrong. Verbatim below.

# Issue 1 — `distill_labs` unit pooling and missing first/last value

## 1. Verdict: **CONFIRMED** (all four claims), with one correction to the responder's proposed remedy

**Claim 1 — `unit_field` only records, never partitions. CONFIRMED.** `lhn/analytics.py:495-505`:

```python
aggs = [
    F.count('_v').alias(f'{code}_n'),
    F.min('_v').alias(f'{code}_min'),
    F.max('_v').alias(f'{code}_max'),
    F.expr('percentile_approx(_v, 0.5)').alias(f'{code}_median'),
    F.max('_v').alias(f'{code}_peak'),
    F.min('_d').alias(f'{code}_first_date'),
    F.max('_d').alias(f'{code}_last_date'),
]
if unit_field is not None:
    aggs.append(F.first(F.col(unit_field), ignorenulls=True).alias(f'{code}_unit'))
```

The grouping is `base.groupBy(*index).agg(*aggs)` at line 522, where `index` defaults to `['personid', 'tenant']` (line 486). `unit_field` never enters `index`. Every statistic is pooled across units, and `F.first(..., ignorenulls=True)` with no ordering returns a non-deterministic unit — it depends on partition arrival order, so the same input can label the same person differently across runs.

**Claim 2 — the cross-function inconsistency is real. CONFIRMED.** `lhn/features.py:138-142`, inside `analyze_clinical_measurements`:

```python
group_fields = [*id_fields, *code_fields]
if display_field and display_field not in group_fields:
    group_fields.append(display_field)
if unit_field and unit_field in baseline_data.columns:
    group_fields.append(unit_field)
```

versus `lhn/analytics.py:504-505` quoted above. One package, one parameter name, opposite semantics. The report is right that this makes the caller's belief reasonable rather than careless.

**Claim 3 — no first/last value. CONFIRMED.** Lines 501-502 emit `F.min('_d')` and `F.max('_d')` only. There is no value counterpart anywhere in the function, and `{code}_post_delta` (lines 524-525) is `post_peak − pre_peak` — another extremum difference, not a serial one.

**Claim 4 — extent of the inconsistency.** I grepped the whole package. `unit_field` appears in exactly two functions, `analytics.distill_labs` and `features.analyze_clinical_measurements`; there is no other unit-like parameter (`uom`, `unitOfMeasure`) anywhere in `lhn/`. So the inconsistency is contained to these two, which makes it cheap to resolve.

## 2. What the reports got wrong or missed

**The responder's suggested resolution — "either `distill_labs` should partition too" (issue file line 129-130) — is not available.** `distill_labs` is contractually one row per person; its docstring calls it "the PySpark→R/CSV bridge" producing a frame "keyed by `index`, one row per person" (analytics.py:424, 472-473), and the hmi CSVs join it on `['personid','tenant']` assuming uniqueness. Adding `unit_field` to the grouping would emit two rows for any person with mixed units and silently fan out every downstream join. What `features.py` does is not "prevent pooling" in the sense the caller wanted — it *changes the grain*. That is fine for a per-patient-per-measurement feature table and fatal for a person-level bridge. So the only coherent resolutions are: rename the parameter in one of the two, or keep the name and document the difference loudly, plus detect heterogeneity. Partitioning is off the table.

**`features.py` fails open, which is arguably the worse of the two.** Line 141 guards with `unit_field in baseline_data.columns`. Pass a misspelled column and the partitioning silently does not happen — you get pooled statistics and no error. `distill_labs` at least raises an `AnalysisException` on a bad column name. Worth fixing in the same pass.

**Two defects neither report found, both in the same function:**

*(a) `_d` is date-truncated, so a within-day serial delta is not expressible at all.* Line 490: `.withColumn('_d', F.to_date(F.col(date_field)))`. This is the one that undercuts Problem 2's proposed fix. For hmi 040's post-PCI troponin the entire clinical question lives inside a 24-hour window — every draw collapses to one or two distinct `_d` values, so `F.min(F.struct(_d, _v))` would order by a key with no resolution and the "first value" would be decided by the tie-break on `_v`, not by time. **Adding `_first_value`/`_last_value` on the current `_d` would ship a column that looks time-ordered and is not.** The ordering key has to carry the timestamp. hmi's own source has one — memory records `labSource` date as `datetimeLab`, while 020 passes `date_field='dateLab'`.

*(b) A NULL invalid flag silently drops the row.* Lines 487 and 491:

```python
drop_invalid = F.col(invalid_field) if invalid_field else F.lit(False)
base = (df... .filter(F.col('_v').isNotNull() & F.col('_d').isNotNull() & ~drop_invalid))
```

Under Spark's three-valued logic `~NULL` is NULL, so `filter` drops the row. Any caller whose flag comes from a `F.when(...)` with no `.otherwise(False)` — the common way to write one — loses every unevaluated row from the denominator with no warning. I checked hmi: `troponin_value_ngL_invalid = F.col(VAL) < 0` (020 line 227) is NULL only when `VAL` is NULL, and those rows are already dropped by the `_v.isNotNull()` test, so hmi is not currently losing data. The defect is latent, not active. Severity low-to-moderate, but it is a one-word fix (`F.coalesce(..., F.lit(False))`).

**On severity.** "High" for the unit half is right, and I would go further than the report: the failure is not merely that statistics are pooled, it is that `{code}_unit` is non-deterministic, so the column that documents the corruption is itself unreliable. Problem 2 I would rate medium, not high — it is a missing feature with a correct workaround, whereas Problem 1 produces wrong numbers from correct-looking code.

## 3. Proposed patch

**Part A — heterogeneity detection (the cheap, high-value change).** Note the check runs on a one-column `distinct()`, not a per-group aggregate: if the whole input to this call has one unit, no group can have two, so the narrow distinct is both cheaper and sufficient. Per-group counting is kept as a free column for locating affected persons.

```python
def distill_labs(df, value_field, date_field, loinc_field=None, loincs=None,
                 index=None, index_date_field=None, invalid_field=None,
                 unit_field=None, post_window_days=None, code='lab',
                 on_mixed_units='warn'):
    """...
    unit_field (str): optional unit column. This RECORDS a unit as
        ``<code>_unit``; it does NOT harmonize or partition by one — all
        statistics are pooled across whatever units are present. (Contrast
        ``features.analyze_clinical_measurements``, where ``unit_field`` is
        added to the grouping and DOES partition. The names match; the
        semantics do not.) Harmonize upstream; use ``on_mixed_units`` to
        police it.
    on_mixed_units (str): 'warn' (default), 'raise', or 'ignore'. With a
        ``unit_field``, checks that the input carries exactly one unit.
        Costs one narrow Spark job over a single column; pass 'ignore' in
        hot paths.
    """
    index = list(index) if index else ['personid', 'tenant']
    # NULL flag must not drop the row: ~NULL is NULL and filter() discards it.
    drop_invalid = (F.coalesce(F.col(invalid_field), F.lit(False))
                    if invalid_field else F.lit(False))
    base = (df
            .withColumn('_v', F.col(value_field).cast('double'))
            .withColumn('_d', F.to_date(F.col(date_field)))
            .filter(F.col('_v').isNotNull() & F.col('_d').isNotNull() & ~drop_invalid))
    if loincs is not None:
        base = base.filter(F.col(loinc_field).isin(list(loincs)))

    if unit_field is not None and on_mixed_units != 'ignore':
        units = [r[0] for r in
                 base.select(unit_field).distinct().limit(21).collect()]
        if len(units) > 1:
            msg = ("distill_labs(code={!r}): value_field {!r} spans {} units "
                   "({}). Statistics are POOLED across them and <code>_unit "
                   "names only one. Harmonize before calling."
                   ).format(code, value_field, len(units), sorted(map(str, units))[:10])
            if on_mixed_units == 'raise':
                raise ValueError(msg)
            logger.warning(msg)
```

and in the aggregate list, alongside the existing `F.first(...)`:

```python
    if unit_field is not None:
        aggs.append(F.first(F.col(unit_field), ignorenulls=True).alias(f'{code}_unit'))
        # Free (same shuffle): lets a caller find WHICH persons are mixed.
        aggs.append(F.countDistinct(F.col(unit_field)).alias(f'{code}_unit_n'))
```

**Part B — first/last value, with a real ordering key.** `max_by`/`min_by` are Spark 3.0+ and unavailable here, so the struct trick is the right choice — but it needs `_o`, not `_d`:

```python
def distill_labs(..., order_field=None, ...):
    """order_field (str): optional column giving WITHIN-DAY order (e.g. a lab
        datetime). Defaults to ``date_field``, which ``F.to_date`` truncates —
        so with the default, ``<code>_first_value``/``_last_value`` are only
        meaningful when a person's draws fall on distinct days. For any
        episode-scoped question (post-PCI troponin), pass the timestamp column.
    """
    # after `base` is built:
    _o = (F.coalesce(F.col(order_field).cast('timestamp'), F.col('_d').cast('timestamp'))
          if order_field else F.col('_d').cast('timestamp'))
    base = base.withColumn('_o', _o)
```

then append to `aggs`:

```python
    # min/max over a struct orders by the leading field -> value at the
    # earliest/latest observation. One shuffle, no window frame. Spark 2.4-safe
    # (max_by/min_by are 3.0+). Ties on _o break on _v: first_value takes the
    # LOWEST of a tied earliest instant, last_value the HIGHEST of a tied
    # latest -- which biases toward showing a rise. Pass a finer order_field
    # to avoid ties rather than relying on the tie-break.
    _pair = F.struct(F.col('_o').alias('o'), F.col('_v').alias('v'))
    aggs += [
        F.min(_pair).alias(f'_{code}_first_s'),
        F.max(_pair).alias(f'_{code}_last_s'),
    ]
```

and after the `groupBy`:

```python
    out = base.groupBy(*index).agg(*aggs)
    out = (out
           .withColumn(f'{code}_first_value', F.col(f'_{code}_first_s').getField('v'))
           .withColumn(f'{code}_last_value',  F.col(f'_{code}_last_s').getField('v'))
           .withColumn(f'{code}_serial_delta',
                       F.col(f'{code}_last_value') - F.col(f'{code}_first_value'))
           .drop(f'_{code}_first_s', f'_{code}_last_s'))
```

I evaluated the two alternatives you asked about. A window frame (`F.first(...).over(Window.partitionBy(index).orderBy('_o').rowsBetween(unbounded, unbounded))`) needs a second full shuffle plus a per-partition frame, and hmi 068 has already crashed Spark on exactly that shape over an encounter table — reject. `F.first('_v')` after a global `.orderBy()` is non-deterministic under Spark's aggregation (`first` has no ordering guarantee inside `groupBy`) — reject, it would look right in a small test and be wrong at scale. The struct approach is correct and is the one I'd ship.

I'd also add the responder's framing to the docstring: with a median 536 days between a person's first and last troponin, the record-wide serial delta is not a rise-and-fall measure. Document `_first_value`/`_last_value` as requiring `index_date_field` + `post_window_days` to be clinically interpretable, and ideally emit the post-window pair too (`F.min(F.when(is_post, _pair))`) since that is the quantity the Universal Definition actually wants.

## 4. Backward-compatibility risk

Low, with one exception. New output columns (`{code}_unit_n`, `{code}_first_value`, `{code}_last_value`, `{code}_serial_delta`) widen the CSV; R readers that select explicitly are unaffected, and `readr` picks up new columns harmlessly. The `coalesce` on `invalid_field` changes row counts for any caller whose flag has NULLs — that is a bug fix, but it will move numbers, so it belongs in release notes rather than a silent patch. `order_field` defaults to current behavior.

The one real decision is `on_mixed_units='warn'` as the default: it adds a Spark job to every call that passes `unit_field`. I recommend defaulting to `'warn'` anyway — it is one narrow distinct, and this parameter's entire history is people believing it does something it doesn't. Callers in hot paths opt out with `'ignore'`. I would **not** make it opt-in; an opt-in safety check is one nobody who needs it will set.

---

---

## Correction accepted 2026-08-04 (SCDCernerProject session) — "partition too" is not available

**The review is right and I withdraw half my proposed resolution.** I wrote that either
`distill_labs` should partition by unit like `features.py` does, or the two should be named
differently. The first option is not on the table: `distill_labs` is contractually **one row per
person** — its own docstring calls it the PySpark→R/CSV bridge keyed by `index` — and the hmi CSVs
join it on `['personid','tenant']` assuming uniqueness. Adding `unit_field` to the grouping would
emit two rows for anyone with mixed units and **silently fan out every downstream join**.

The distinction I missed is that `features.py` does not "prevent pooling" in the sense the caller
wanted — it **changes the grain**. That is right for a per-measurement feature table and fatal for
a person-level bridge. So the resolutions are: rename in one of the two, or keep the name and
document the difference loudly — and detect heterogeneity either way. I had the cross-function
inconsistency right and the remedy wrong.

**Three findings I would highlight, because they change what should be built:**

1. **`_d` is date-truncated (`F.to_date`), so a within-day serial delta is not expressible.** This
   undercuts the proposed `_first_value`/`_last_value` fix directly: for post-PCI troponin the
   whole clinical question lives inside 24 hours, every draw collapses to one or two `_d` values,
   and `F.min(F.struct(_d, _v))` would then order by a key with no resolution — the "first value"
   decided by the tie-break on `_v` rather than by time. **A column that looks time-ordered and is
   not is worse than no column**, and it would have shipped had this not been caught. The ordering
   key has to carry the timestamp, which means the source datetime, not `dateLab`.
2. **`{code}_unit` is non-deterministic** — `F.first(..., ignorenulls=True)` with no ordering
   depends on partition arrival, so the same input can label the same person differently across
   runs. The column documenting the corruption is itself unreliable. That is a stronger argument
   for "high" than the pooling alone.
3. **`features.py` fails open**: `unit_field in baseline_data.columns` means a misspelled column
   silently skips partitioning, giving pooled statistics with no error — whereas `distill_labs`
   at least raises. The safer of the two functions is the one that fails loudly.

The latent NULL-invalid-flag defect (`~NULL` is NULL under three-valued logic, so the row is
dropped) is worth fixing in the same pass at one word — `F.coalesce(..., F.lit(False))` — even
though hmi is not currently affected.

**What survives from my response:** the cross-function inconsistency as the root cause rather than
a misleading name, and heterogeneity detection as the right minimum. **What does not:** the
partitioning half of the remedy, withdrawn.
