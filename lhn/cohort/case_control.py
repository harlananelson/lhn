"""
lhn/cohort/case_control.py

Case-control matching for healthcare cohort studies.

Implements distance-based matching with exact matching on categorical variables
(gender, race, ethnicity, age group) and Euclidean distance minimization on
continuous variables (encounters, follow-up time).

Algorithm (pandas/NumPy — replaces Spark join/window approach):
    1. Collect cases and controls to driver (60K rows = ~10MB)
    2. Group by exact-match stratum
    3. Within each stratum: NumPy vectorized pairwise distance
    4. Greedy matching: hardest-to-match-first, without replacement
    5. Return as Spark DataFrame

Performance: seconds for 14K cases × 46K controls (vs hours with Spark).

Based on SCDCernerProject/Projects/SickleCell/054-Control-Cohort.ipynb
and lhn/cohort_matching.py from v0.1.0-monolithic.
"""

import time
from functools import reduce
from operator import add

import numpy as np
import pandas as pd

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
    """Validate that required columns exist in a DataFrame."""
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(
            f"{df_name} is missing required columns: {sorted(missing)}. "
            f"Available columns: {sorted(df.columns)}"
        )


def _derive_case_suffix(id_col, case_id_col):
    """Derive the case suffix from ID column naming convention."""
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
                f"Setting stddev to 1 to avoid division by zero."
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

    Uses ``F.coalesce(..., F.lit(0))`` for null safety.
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
    return df.withColumn('distance', F.sqrt(sum_expr + (F.rand() * 0.0001)))


def match_controls_to_cases(cases, controls, match_cols, distance_cols=None,
                            id_col='personid', case_id_col='personidCase',
                            controls_per_case=4):
    """Match controls to cases using pandas/NumPy within exact-match strata.

    Collects to driver and uses NumPy vectorized distance. Completes in
    seconds for 14K cases × 46K controls.

    Parameters:
        cases (DataFrame): Case patients with case_id_col, match_cols,
            and standardized distance columns.
        controls (DataFrame): Control pool with id_col, match_cols,
            and standardized distance columns.
        match_cols (list): Columns for exact matching.
        distance_cols (list): Base names of distance columns.
        id_col (str): Control person ID column.
        case_id_col (str): Case person ID column.
        controls_per_case (int): Number of controls per case.

    Returns:
        pandas.DataFrame: Matched pairs with case_id_col, id_col,
            match columns, distance, and match_rank.
    """
    if distance_cols is None:
        distance_cols = ['encounters', 'followtime']

    case_suffix = _derive_case_suffix(id_col, case_id_col)
    case_std_cols = [f'{c}_standardized{case_suffix}' for c in distance_cols]
    control_std_cols = [f'{c}_standardized' for c in distance_cols]

    t_start = time.time()

    # Collect to pandas — 60K rows fits easily in driver memory
    case_select = [case_id_col] + match_cols + case_std_cols
    control_select = [id_col] + match_cols + control_std_cols

    logger.info("Collecting cases to driver...")
    cases_pd = cases.select([F.col(c).alias(c) for c in case_select]).toPandas()
    logger.info(f"Collected {len(cases_pd)} cases")

    logger.info("Collecting controls to driver...")
    controls_pd = controls.select([F.col(c).alias(c) for c in control_select]).toPandas()
    logger.info(f"Collected {len(controls_pd)} controls")

    # Create stratum key
    cases_pd['_stratum'] = cases_pd[match_cols].apply(lambda r: tuple(r.values), axis=1)
    controls_pd['_stratum'] = controls_pd[match_cols].apply(lambda r: tuple(r.values), axis=1)

    # Rename distance columns to common names
    dist_rename_case = dict(zip(case_std_cols, [f'_d{i}' for i in range(len(distance_cols))]))
    dist_rename_ctrl = dict(zip(control_std_cols, [f'_d{i}' for i in range(len(distance_cols))]))
    cases_pd = cases_pd.rename(columns=dist_rename_case)
    controls_pd = controls_pd.rename(columns=dist_rename_ctrl)
    dist_cols_internal = [f'_d{i}' for i in range(len(distance_cols))]

    # Match within each stratum
    all_matches = []
    unmatched_case_ids = []

    strata_cases = cases_pd.groupby('_stratum')
    strata_controls = controls_pd.groupby('_stratum')
    control_strata_keys = set(strata_controls.groups.keys())

    for stratum_key, stratum_cases_df in strata_cases:
        if stratum_key not in control_strata_keys:
            logger.warning(f"Stratum {stratum_key}: {len(stratum_cases_df)} cases, 0 controls")
            unmatched_case_ids.extend(stratum_cases_df[case_id_col].tolist())
            continue

        stratum_controls_df = strata_controls.get_group(stratum_key)
        n_cases = len(stratum_cases_df)
        n_ctrls = len(stratum_controls_df)

        logger.info(f"Stratum {stratum_key}: {n_cases} cases, {n_ctrls} controls")

        # NumPy vectorized distance matrix
        case_coords = stratum_cases_df[dist_cols_internal].values.astype(np.float64)
        ctrl_coords = stratum_controls_df[dist_cols_internal].values.astype(np.float64)

        # Replace NaN with 0 (same as F.coalesce in Spark version)
        case_coords = np.nan_to_num(case_coords, nan=0.0)
        ctrl_coords = np.nan_to_num(ctrl_coords, nan=0.0)

        case_ids_arr = stratum_cases_df[case_id_col].values
        ctrl_ids_arr = stratum_controls_df[id_col].values

        # Pairwise distance: (n_cases, n_controls)
        diff = case_coords[:, np.newaxis, :] - ctrl_coords[np.newaxis, :, :]
        dist_matrix = np.sqrt(np.sum(diff ** 2, axis=2))

        # Greedy matching: hardest-to-match-first
        available = np.ones(n_ctrls, dtype=bool)
        min_dists = dist_matrix.min(axis=1)
        case_order = np.argsort(-min_dists)  # hardest first

        # Get match_cols values for this stratum (same for all rows)
        stratum_match_vals = {col: stratum_cases_df.iloc[0][col] for col in match_cols}

        for case_idx in case_order:
            if not available.any():
                unmatched_case_ids.append(case_ids_arr[case_idx])
                continue

            dists = dist_matrix[case_idx].copy()
            dists[~available] = np.inf

            n_to_pick = min(controls_per_case, int(available.sum()))
            if n_to_pick == 0:
                unmatched_case_ids.append(case_ids_arr[case_idx])
                continue

            # argpartition is O(n) — faster than full sort
            if n_to_pick < n_ctrls:
                nearest = np.argpartition(dists, n_to_pick)[:n_to_pick]
            else:
                nearest = np.where(available)[0]

            nearest = nearest[np.argsort(dists[nearest])]

            for rank, ctrl_idx in enumerate(nearest, start=1):
                match_row = {
                    case_id_col: case_ids_arr[case_idx],
                    id_col: ctrl_ids_arr[ctrl_idx],
                    'distance': float(dists[ctrl_idx]),
                    'match_rank': rank,
                }
                match_row.update(stratum_match_vals)
                all_matches.append(match_row)

            available[nearest] = False

    t_elapsed = time.time() - t_start

    if not all_matches:
        logger.warning("No matches produced!")
        return pd.DataFrame(columns=[case_id_col, id_col, 'distance', 'match_rank'] + match_cols)

    result_pd = pd.DataFrame(all_matches)

    # Report
    n_matched = result_pd[case_id_col].nunique()
    n_total = len(cases_pd)
    n_full = (result_pd.groupby(case_id_col).size() >= controls_per_case).sum()

    logger.info("=" * 60)
    logger.info(f"MATCHING COMPLETE in {t_elapsed:.1f} seconds")
    logger.info(f"Total cases:          {n_total}")
    logger.info(f"Matched cases:        {n_matched} ({100*n_matched/n_total:.1f}%)")
    logger.info(f"Fully matched (1:{controls_per_case}): {n_full}")
    logger.info(f"Unmatched:            {len(unmatched_case_ids)}")
    logger.info(f"Total pairs:          {len(result_pd)}")
    logger.info(f"Mean distance:        {result_pd['distance'].mean():.4f}")
    logger.info(f"Median distance:      {result_pd['distance'].median():.4f}")
    logger.info("=" * 60)

    # Controls per case distribution
    cpc = result_pd.groupby(case_id_col).size().value_counts().sort_index()
    logger.info("Controls per case distribution:")
    for n, count in cpc.items():
        logger.info(f"  {n} controls: {count} cases")

    return result_pd


def iterative_case_control_match(cases, control_pool, match_iterations,
                                 distance_cols=None,
                                 id_col='personid', case_id_col='personidCase',
                                 controls_per_case=4, **kwargs):
    """Run iterative case-control matching with progressively relaxed criteria.

    Collects to pandas, then matches within each iteration's match columns.
    Cases that receive fewer than controls_per_case controls carry to the
    next iteration. Used controls are removed from the pool.

    Parameters:
        cases (DataFrame): Case patients (Spark DataFrame).
        control_pool (DataFrame): Potential controls (Spark DataFrame).
        match_iterations (list[list[str]]): Match column sets, strictest first.
        distance_cols (list): Columns for distance calculation.
        id_col (str): Control person ID column.
        case_id_col (str): Case person ID column.
        controls_per_case (int): Target controls per case.

    Returns:
        pandas.DataFrame: All matched pairs, or None if no matches.
    """
    if distance_cols is None:
        distance_cols = ['encounters', 'followtime']

    all_matched_pd = None
    remaining_cases = cases
    remaining_controls = control_pool

    for i, match_cols in enumerate(match_iterations):
        iteration = i + 1

        logger.info(f"Iteration {iteration}: matching on {match_cols}")

        try:
            new_matched_pd = match_controls_to_cases(
                remaining_cases, remaining_controls,
                match_cols=match_cols,
                distance_cols=distance_cols,
                id_col=id_col,
                case_id_col=case_id_col,
                controls_per_case=controls_per_case
            )

            if len(new_matched_pd) == 0:
                logger.warning(f"Iteration {iteration}: no matches found")
                continue

        except Exception as exc:
            logger.warning(
                f"Iteration {iteration}: failed with {type(exc).__name__}: {exc}. "
                f"Skipping — previous matches preserved."
            )
            continue

        # Accumulate matches
        if all_matched_pd is None:
            all_matched_pd = new_matched_pd
        else:
            all_matched_pd = pd.concat([all_matched_pd, new_matched_pd], ignore_index=True)

        # Remove used controls from pool
        used_control_ids = set(all_matched_pd[id_col].unique())
        remaining_controls = remaining_controls.filter(
            ~F.col(id_col).isin(list(used_control_ids))
        )

        # Identify cases that still need more controls
        case_counts = all_matched_pd.groupby(case_id_col).size()
        insufficient = case_counts[case_counts < controls_per_case].index.tolist()

        if len(insufficient) == 0:
            logger.info(f"Iteration {iteration}: all cases fully matched")
            break

        # Filter remaining cases to only insufficient ones
        remaining_cases = remaining_cases.filter(
            F.col(case_id_col).isin(insufficient)
        )

        logger.info(
            f"Iteration {iteration}: {len(insufficient)} cases still need "
            f"more controls"
        )

    if all_matched_pd is not None:
        logger.info(f"Final: {all_matched_pd[case_id_col].nunique()} cases matched, "
                    f"{len(all_matched_pd)} total pairs")
    else:
        logger.warning("No matches produced across all iterations")

    return all_matched_pd


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
        demo_df (DataFrame): Demographics table with id_col, match columns,
            and distance columns.
        case_ids (DataFrame): DataFrame with id_col identifying cases.
        exclude_ids (DataFrame): Optional DataFrame with id_col for people
            to exclude from controls. If None, uses case_ids.
        id_col (str): Person identifier column.
        case_id_col (str): Column name for case ID after renaming.
        match_cols (list): Exact match columns.
        distance_cols (list): Continuous distance columns.
        age_col (str): Age column in demographics.
        age_bins (list): Age group boundaries.
        age_labels (list): Age group labels.
        age_group_col (str): Name for computed age group column.
        standardize_over (str): 'controls' or 'all'.

    Returns:
        tuple: (cases DataFrame, control_pool DataFrame, stats DataFrame)
    """
    from lhn.cohort.demographics import assign_age_group

    if match_cols is None:
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
    required_demo_cols = [id_col, *[c for c in match_cols if c != age_group_col],
                          age_col, *distance_cols]
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

    # Build control pool first
    control_demo = demo.join(
        exclude_ids.select(id_col).distinct(), on=id_col, how='left_anti'
    )

    # Standardize distance columns
    if standardize_over == 'controls':
        _, stats = standardize_columns(control_demo, distance_cols)
        demo_with_std, _ = standardize_columns(demo, distance_cols, stats=stats)
    else:
        demo_with_std, stats = standardize_columns(demo, distance_cols)

    # Build case table
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
