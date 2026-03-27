"""
lhn/cohort/case_control.py

Case-control matching for healthcare cohort studies.

Implements distance-based matching with exact matching on categorical variables
(gender, race, ethnicity, age group) and Euclidean distance minimization on
continuous variables (encounters, follow-up time).

Algorithm:
    1. Exclude case patients from control pool
    2. Standardize distance variables (z-scores)
    3. Exact-match cases to potential controls on categorical variables
    4. Compute Euclidean distance on standardized continuous variables
    5. For each control, pick closest case; for each case, pick N closest controls
    6. Cases with insufficient controls go to next iteration with relaxed matching
    7. Repeat until all cases matched or pool exhausted

Based on the matching algorithm from SCDCernerProject/Projects/SickleCell/054-Control-Cohort.ipynb
and lhn/cohort_matching.py from v0.1.0-monolithic.
"""

from functools import reduce
from operator import add

from pyspark.sql import DataFrame, Window
from lhn.header import F, get_logger

logger = get_logger(__name__)


DEFAULT_AGE_BINS = [0, 3, 6, 13, 20, 30, 40, 200]
DEFAULT_AGE_LABELS = [
    'Infants and Toddlers',  # 0-2
    'Preschoolers',          # 3-5
    'School age',            # 6-12
    'Teenagers',             # 13-19
    'Young Adults',          # 20-29
    'Adults',                # 30-39
    'Other',                 # 40+
]


def _validate_columns(df, required_cols, df_name="DataFrame"):
    """Validate that required columns exist in a DataFrame.

    Parameters:
        df (DataFrame): The DataFrame to validate.
        required_cols (list): List of required column names.
        df_name (str): Name for error messages.

    Raises:
        ValueError: If any required columns are missing.
    """
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(
            f"{df_name} is missing required columns: {sorted(missing)}. "
            f"Available columns: {sorted(df.columns)}"
        )


def _derive_case_suffix(id_col, case_id_col):
    """Derive the case suffix from ID column naming convention.

    Parameters:
        id_col (str): Base person ID column name.
        case_id_col (str): Case person ID column name.

    Returns:
        str: The suffix string (e.g., 'Case').

    Raises:
        ValueError: If case_id_col does not start with id_col.
    """
    if not case_id_col.startswith(id_col):
        raise ValueError(
            f"case_id_col '{case_id_col}' must start with id_col '{id_col}'. "
            f"Example: id_col='personid', case_id_col='personidCase'"
        )
    return case_id_col[len(id_col):]


def standardize_columns(df, columns, stats=None):
    """Add z-score standardized versions of columns.

    Parameters:
        df (DataFrame): Input DataFrame.
        columns (list): Column names to standardize.
        stats (DataFrame): Pre-computed mean/stddev (one row). If None,
            computed from ``df``.

    Returns:
        tuple: (DataFrame with ``{col}_standardized`` columns, stats DataFrame)
    """
    _validate_columns(df, columns, "standardize_columns input")

    if stats is None:
        agg_exprs = []
        for col in columns:
            agg_exprs.append(F.mean(F.col(col)).alias(f'{col}_mean'))
            agg_exprs.append(F.stddev(F.col(col)).alias(f'{col}_stddev'))
        stats = df.agg(*agg_exprs)

    # Check for zero/null stddev
    stats_row = stats.collect()[0]
    for col in columns:
        stddev_val = stats_row[f'{col}_stddev']
        if stddev_val is None or stddev_val == 0:
            logger.warning(
                f"Column '{col}' has zero or null standard deviation. "
                f"Setting stddev to 1 to avoid division by zero — "
                f"standardized values will equal (value - mean)."
            )
            stats = stats.withColumn(f'{col}_stddev', F.lit(1.0))

    df_with_stats = df.crossJoin(F.broadcast(stats))
    for col in columns:
        df_with_stats = df_with_stats.withColumn(
            f'{col}_standardized',
            (F.col(col) - F.col(f'{col}_mean')) / F.col(f'{col}_stddev')
        ).drop(f'{col}_mean', f'{col}_stddev')

    return df_with_stats, stats


def compute_distance(df, case_suffix='Case', columns=None):
    """Compute Euclidean distance between case and control standardized columns.

    Uses ``F.coalesce(..., F.lit(0))`` for null safety and adds a small random
    tiebreaker (``F.rand() * 0.0001``) to ensure unique distances, matching the
    original v0.1.0 implementation.

    Parameters:
        df (DataFrame): DataFrame with both case and control standardized columns.
        case_suffix (str): Suffix appended to case column names.
        columns (list): Base names of standardized columns. Expects
            ``{col}_standardized`` for controls and
            ``{col}_standardized{case_suffix}`` for cases.

    Returns:
        DataFrame: With ``distance`` column added.
    """
    if columns is None:
        columns = ['encounters', 'followtime']

    terms = [
        F.pow(
            F.coalesce(F.col(f'{col}_standardized{case_suffix}'), F.lit(0))
            - F.coalesce(F.col(f'{col}_standardized'), F.lit(0)),
            2
        )
        for col in columns
    ]
    sum_expr = reduce(add, terms)
    # Small random tiebreaker ensures unique distances for deterministic windowing
    return df.withColumn('distance', F.sqrt(sum_expr + (F.rand() * 0.0001)))


def match_controls_to_cases(cases, controls, match_cols, distance_cols=None,
                            id_col='personid', case_id_col='personidCase',
                            controls_per_case=4):
    """Match controls to cases using exact matching + distance minimization.

    For each control, selects the closest case (by distance). Then for each
    case, selects the N closest controls. Both steps are partitioned by the
    exact match columns.

    Parameters:
        cases (DataFrame): Case patients. Must have ``case_id_col`` and
            standardized distance columns (``{col}_standardized{Case}``).
        controls (DataFrame): Control pool. Must have ``id_col`` and
            standardized distance columns (``{col}_standardized``).
        match_cols (list): Columns for exact matching (e.g.,
            ``['gender', 'race', 'ethnicity', 'age_group']``).
        distance_cols (list): Base names of distance columns (default:
            ``['encounters', 'followtime']``).
        id_col (str): Control person ID column.
        case_id_col (str): Case person ID column.
        controls_per_case (int): Number of controls per case.

    Returns:
        DataFrame: Matched controls with ``id_col``, ``case_id_col``,
            match columns, and ``distance``.
    """
    if distance_cols is None:
        distance_cols = ['encounters', 'followtime']

    case_suffix = _derive_case_suffix(id_col, case_id_col)

    # Validate inputs
    case_std_cols = [f'{c}_standardized{case_suffix}' for c in distance_cols]
    _validate_columns(cases, [case_id_col, *match_cols, *case_std_cols], "cases")

    control_std_cols = [f'{c}_standardized' for c in distance_cols]
    _validate_columns(controls, [id_col, *match_cols, *control_std_cols], "controls")

    # Prepare field lists
    case_fields = [case_id_col, *match_cols, *case_std_cols]
    control_fields = [id_col, *match_cols, *control_std_cols]
    joined_fields = [id_col, case_id_col, *match_cols, 'distance']

    # Join cases to controls on exact match columns
    joined = (
        controls.select(control_fields)
        .join(cases.select(case_fields), on=match_cols, how='inner')
    )

    # Compute distance
    joined = compute_distance(joined, case_suffix=case_suffix, columns=distance_cols)
    joined = joined.select(joined_fields)

    # Step 1: For each control, pick the closest case
    window_control = (
        Window.partitionBy([*match_cols, id_col])
        .orderBy(F.col('distance').asc())
    )
    control_ranked = joined.withColumn('row_number', F.row_number().over(window_control))
    best_case_per_control = control_ranked.filter(F.col('row_number') == 1).drop('row_number')

    # Step 2: For each case, pick the N closest controls
    window_case = (
        Window.partitionBy([*match_cols, case_id_col])
        .orderBy(F.col('distance').asc())
    )
    case_ranked = best_case_per_control.withColumn('row_number', F.row_number().over(window_case))
    matched = case_ranked.filter(
        F.col('row_number') <= controls_per_case
    ).drop('row_number')

    return matched


def iterative_case_control_match(cases, control_pool, match_iterations,
                                 distance_cols=None,
                                 id_col='personid', case_id_col='personidCase',
                                 controls_per_case=4, break_lineage=True):
    """Run iterative case-control matching with progressively relaxed criteria.

    Accumulates ALL matches across iterations (including partial). Cases that
    still need more controls are carried to the next iteration with relaxed
    match criteria. Used controls are removed from the pool.

    Parameters:
        cases (DataFrame): Case patients with ``case_id_col``, match columns,
            and standardized distance columns.
        control_pool (DataFrame): Potential controls with ``id_col``, match
            columns, and standardized distance columns.
        match_iterations (list[list[str]]): List of match column sets, from
            strictest to most relaxed. Example::

                [
                    ['gender', 'race', 'ethnicity', 'age_group'],  # strict
                    ['gender', 'race', 'ethnicity'],                # relax age
                    ['gender', 'age_group'],                        # relax race
                ]

        distance_cols (list): Columns for distance calculation.
        id_col (str): Control person ID column.
        case_id_col (str): Case person ID column.
        controls_per_case (int): Target controls per case.
        break_lineage (bool): Cache intermediate results to break Spark lineage.

    Returns:
        DataFrame: All matched controls with ``id_col``, ``case_id_col``,
            and ``distance``.
    """
    if distance_cols is None:
        distance_cols = ['encounters', 'followtime']

    # Canonical output column order (from first iteration's match_cols + core cols)
    output_cols = None

    all_matched = None
    prev_cached = []  # track cached DFs for unpersist
    remaining_cases = cases
    remaining_controls = control_pool

    for i, match_cols in enumerate(match_iterations):
        iteration = i + 1
        n_cases = remaining_cases.select(case_id_col).distinct().count()
        n_controls = remaining_controls.select(id_col).distinct().count()

        logger.info(
            f"Iteration {iteration}: {n_cases} unmatched cases, "
            f"{n_controls} available controls, "
            f"matching on {match_cols}"
        )

        if n_cases == 0 or n_controls == 0:
            logger.info(f"Iteration {iteration}: nothing to match, stopping")
            break

        # Match
        # Wrap in try/except so a failed iteration doesn't lose previous matches
        try:
            new_matched = match_controls_to_cases(
                remaining_cases, remaining_controls,
                match_cols=match_cols,
                distance_cols=distance_cols,
                id_col=id_col,
                case_id_col=case_id_col,
                controls_per_case=controls_per_case
            )

            new_count = new_matched.count()
            logger.info(f"Iteration {iteration}: matched {new_count} control assignments")

            if new_count == 0:
                logger.warning(f"Iteration {iteration}: no matches found, continuing")
                continue

        except Exception as exc:
            logger.warning(
                f"Iteration {iteration}: failed with {type(exc).__name__}: {exc}. "
                f"Skipping this iteration — previous matches preserved."
            )
            continue

        # Set canonical column order from first non-empty result
        if output_cols is None:
            output_cols = list(new_matched.columns)

        # Accumulate ALL matches (not just sufficient — preserves partial matches
        # from stricter iterations, matching v0.1.0 behavior)
        if all_matched is None:
            all_matched = new_matched.select(output_cols)
        else:
            # Select only canonical columns for union stability
            all_matched = (
                all_matched.select(output_cols)
                .union(new_matched.select(output_cols))
                .distinct()
            )

        if break_lineage and all_matched is not None:
            # Unpersist previous cached DFs to free memory
            for cached_df in prev_cached:
                try:
                    cached_df.unpersist()
                except Exception:
                    pass
            prev_cached = []

            all_matched = all_matched.cache()
            all_matched.count()  # force materialization
            prev_cached.append(all_matched)

        # Update remaining controls (remove used)
        used_controls = all_matched.select(id_col).distinct()
        remaining_controls = remaining_controls.join(
            used_controls, on=id_col, how='left_anti'
        )

        # Carry forward cases that still need more controls
        case_counts = all_matched.groupBy(case_id_col).count()
        insufficient = case_counts.filter(F.col('count') < controls_per_case)
        n_insufficient = insufficient.count()

        if n_insufficient == 0:
            logger.info(f"Iteration {iteration}: all cases fully matched")
            break

        # Remaining = cases that have < controls_per_case matches so far
        remaining_cases = cases.join(
            insufficient.select(case_id_col), on=case_id_col, how='inner'
        )

        logger.info(
            f"Iteration {iteration}: {n_insufficient} cases still need "
            f"more controls, carrying to next iteration"
        )

        if break_lineage:
            remaining_controls = remaining_controls.cache()
            remaining_cases = remaining_cases.cache()
            prev_cached.extend([remaining_controls, remaining_cases])

    # Report
    if all_matched is not None:
        total_cases = all_matched.select(case_id_col).distinct().count()
        total_controls = all_matched.select(id_col).distinct().count()
        total_pairs = all_matched.count()

        # Distribution of controls per case
        case_counts = all_matched.groupBy(case_id_col).count()
        fully_matched = case_counts.filter(F.col('count') >= controls_per_case).count()
        partially_matched = case_counts.filter(F.col('count') < controls_per_case).count()

        unmatched_count = cases.join(
            all_matched.select(case_id_col).distinct(),
            on=case_id_col, how='left_anti'
        ).select(case_id_col).distinct().count()

        logger.info(
            f"Matching complete: {total_cases} cases matched to "
            f"{total_controls} controls ({total_pairs} total pairs)"
        )
        logger.info(
            f"  Fully matched ({controls_per_case}+ controls): {fully_matched}"
        )
        if partially_matched > 0:
            logger.warning(
                f"  Partially matched (<{controls_per_case} controls): {partially_matched}"
            )
        if unmatched_count > 0:
            logger.warning(
                f"  Unmatched (0 controls): {unmatched_count}"
            )

        # Controls per case distribution
        count_dist = (
            case_counts.groupBy('count')
            .agg(F.count('*').alias('n_cases'))
            .orderBy('count')
            .collect()
        )
        logger.info("Controls per case distribution:")
        for row in count_dist:
            logger.info(f"  {row['count']} controls: {row['n_cases']} cases")

    else:
        logger.warning("No matches produced across all iterations")

    return all_matched


def prepare_case_control(demo_df, case_ids, exclude_ids=None,
                         id_col='personid', case_id_col='personidCase',
                         match_cols=None, distance_cols=None,
                         age_col='age', age_bins=None, age_labels=None,
                         age_group_col='age_group',
                         standardize_over='controls'):
    """Prepare case and control DataFrames from demographics for matching.

    Handles the full setup: age grouping, exclusion, standardization,
    and column renaming for cases.

    Parameters:
        demo_df (DataFrame): Demographics table with ``id_col``, match columns,
            and distance columns.
        case_ids (DataFrame): DataFrame with ``id_col`` identifying cases.
            May include phenotype columns.
        exclude_ids (DataFrame): Optional DataFrame with ``id_col`` for people
            to exclude from controls (e.g., all SCD patients including UNK).
            If None, uses ``case_ids``.
        id_col (str): Person identifier column.
        case_id_col (str): Column name for case ID after renaming.
        match_cols (list): Exact match columns. Default:
            ``['gender', 'race', 'ethnicity', 'age_group']``.
        distance_cols (list): Continuous distance columns. Default:
            ``['encounters', 'followtime']``.
        age_col (str): Age column in demographics.
        age_bins (list): Age group boundaries.
        age_labels (list): Age group labels.
        age_group_col (str): Name for computed age group column.
        standardize_over (str): 'controls' to compute z-scores over controls
            only (recommended — avoids cases biasing the metric), or 'all'
            to compute over the full population (v0.1.0 behavior).

    Returns:
        tuple: (cases DataFrame, control_pool DataFrame, stats DataFrame)
    """
    # Lazy import to avoid circular dependency
    from lhn.cohort.demographics import assign_age_group

    if match_cols is None:
        # v0.1.0 default included 'tenant'; pass explicitly to override
        match_cols = ['gender', 'race', 'ethnicity', age_group_col]
    if distance_cols is None:
        distance_cols = ['encounters', 'followtime']
    if age_bins is None:
        age_bins = DEFAULT_AGE_BINS
    if age_labels is None:
        age_labels = DEFAULT_AGE_LABELS
    if exclude_ids is None:
        exclude_ids = case_ids

    case_suffix = _derive_case_suffix(id_col, case_id_col)

    # Required fields from demographics
    # Include ALL match_cols (except age_group which is computed), plus id, age, distance cols
    required_demo_cols = [id_col, *[c for c in match_cols if c != age_group_col],
                          age_col, *distance_cols]
    # Deduplicate while preserving order
    required_demo_cols = list(dict.fromkeys(required_demo_cols))
    logger.info(f"prepare_case_control: required_demo_cols={required_demo_cols}")
    _validate_columns(demo_df, required_demo_cols, "demo_df")
    _validate_columns(case_ids, [id_col], "case_ids")
    _validate_columns(exclude_ids, [id_col], "exclude_ids")

    demo = demo_df.select(required_demo_cols)

    # Assign age groups
    if age_group_col in match_cols:
        demo = assign_age_group(demo, age_col, age_group_col,
                                bins=age_bins, labels=age_labels)
        demo = demo.drop(age_col)

    # Build control pool first (needed for controls-only standardization)
    control_demo = demo.join(
        exclude_ids.select(id_col).distinct(), on=id_col, how='left_anti'
    )

    # Standardize distance columns
    if standardize_over == 'controls':
        # Compute stats from controls only (recommended — avoids cases
        # biasing the distance metric)
        _, stats = standardize_columns(control_demo, distance_cols)
        # Apply those stats to the full population
        demo_with_std, _ = standardize_columns(demo, distance_cols, stats=stats)
    else:
        # v0.1.0 behavior: compute over full population
        demo_with_std, stats = standardize_columns(demo, distance_cols)

    # Build case table (rename id and standardized cols)
    case_join_cols = [id_col]
    if hasattr(case_ids, 'columns'):
        extra_cols = [c for c in case_ids.columns if c != id_col]
        case_join_cols = [id_col, *extra_cols]

    cases = (
        case_ids.select(case_join_cols)
        .join(demo_with_std, on=id_col, how='inner')
        .withColumnRenamed(id_col, case_id_col)
    )
    for col in distance_cols:
        cases = cases.withColumnRenamed(
            f'{col}_standardized',
            f'{col}_standardized{case_suffix}'
        )

    # Build control pool with standardized columns
    # Filter nulls in standardized columns (matches v0.1.0 behavior)
    control_pool = demo_with_std.join(
        exclude_ids.select(id_col).distinct(), on=id_col, how='left_anti'
    )
    for col in distance_cols:
        control_pool = control_pool.filter(
            F.col(f'{col}_standardized').isNotNull()
        )

    n_cases = cases.select(case_id_col).distinct().count()
    n_controls = control_pool.select(id_col).distinct().count()
    logger.info(f"Prepared {n_cases} cases and {n_controls} potential controls")
    logger.info(f"Case columns: {cases.columns}")
    logger.info(f"Control columns: {control_pool.columns}")

    if n_cases == 0:
        logger.warning("No cases found after joining with demographics")
    if n_controls == 0:
        logger.warning("No controls available after exclusions")

    return cases, control_pool, stats
