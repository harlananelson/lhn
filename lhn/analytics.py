"""
lhn/analytics.py

Analytical and statistical functions restored from v0.1.0.
These functions support the QC and analytical workflow that was lost
during the AI refactoring. They provide:
- Statistical summaries (five_number_summary)
- Signal detection (calculate_chi_squared / O/E analysis)
- Spark DataFrame reshaping (stackedSpark)
- Count/pivot operations (count_and_pivot, countDistinct)
- Aggregation utilities (aggregate_fields, aggregate_fields_count)

Restored: 2026-03-15 from v0.1.0-monolithic branch.
"""

from lhn.header import (
    spark, F, pd, DataFrame, display, Markdown, Window,
    gamma, get_logger
)
from pyspark.sql.types import FloatType

logger = get_logger(__name__)


# --- Gamma/Bayesian signal detection helpers ---

def calculate_percentile_pyspark(observed_A, expected_total,
                                  alpha_prior=1, beta_prior=1):
    """Compare observed count to gamma prior for signal detection.

    Args:
        observed_A: Observed count
        expected_total: Expected count
        alpha_prior: Prior alpha parameter
        beta_prior: Prior beta parameter

    Returns:
        float: Survival function value (p-value analog)
    """
    alpha = observed_A + alpha_prior
    beta = expected_total + beta_prior
    rv = gamma(alpha, scale=1 / beta)
    result = rv.sf(observed_A)
    return float(result)


calculate_percentile_pyspark_udf = F.udf(
    calculate_percentile_pyspark, FloatType()
)


def gamma_percentile(a, b, p=0.05):
    """Calculate the p-th percentile of a Gamma(a, 1/b) distribution.

    Args:
        a: Shape parameter (alpha posterior)
        b: Rate parameter (beta posterior)
        p: Percentile (default 0.05)

    Returns:
        float: Percentile value
    """
    rv = gamma(a, scale=1 / b)
    return float(rv.ppf(p))


gamma_percentile_udf = F.udf(gamma_percentile)


def calculate_percentile(row, observed, expected, alpha=1, beta=1):
    """Compare adjusted O/E to the gamma distribution given by alpha, beta.

    Args:
        row: pandas Series (row of DataFrame)
        observed (str): Column name for observed count
        expected (str): Column name for expected count
        alpha (str or int): Column name or value for alpha prior
        beta (str or int): Column name or value for beta prior

    Returns:
        float: Survival function value
    """
    alpha_Prior_num = row[alpha] if isinstance(alpha, str) else alpha
    beta_Prior_num = row[beta] if isinstance(beta, str) else beta

    alpha_Posterior_num = row[observed] + alpha_Prior_num
    beta_Posterior_num = row[expected] + beta_Prior_num
    oe = alpha_Posterior_num / beta_Posterior_num
    rv = gamma(alpha_Prior_num, scale=1 / beta_Prior_num)
    result = rv.sf(oe)
    return result


# --- Core analytical functions ---

def five_number_summary(df, index, groupby, value):
    """Calculate a five number summary for a column, grouped by another column.

    Converts Spark DataFrame to pandas internally for aggregation.

    Args:
        df: Spark DataFrame
        index (list): Index columns (used for unique counting)
        groupby (list): Columns to group by
        value (str): Column to summarize

    Returns:
        pandas DataFrame with min, Q1, median, Q3, max, mean, and unique counts
    """
    pandas_df = df.select([*index, *groupby, value]).distinct().toPandas()
    for col in groupby:
        pandas_df[col] = pandas_df[col].astype(str)
    result = pandas_df.groupby(groupby).agg(
        **{f'Unique_{col}': (col, 'nunique') for col in index},
        average=(value, 'mean'),
        min=(value, 'min'),
        max=(value, 'max'),
        Q1=(value, lambda x: x.quantile(0.25)),
        median=(value, 'median'),
        Q3=(value, lambda x: x.quantile(0.75))
    )
    return result


def calculate_chi_squared(data, indexFields, groupFields, outcomeFields,
                          alpha_prior=1, beta_prior=1):
    """Calculate Observed/Expected ratios with Bayesian signal detection.

    Uses FDA-style 2x2 contingency table methodology:
    - Observed (A) = count of index in each group x outcome
    - Expected = (A+B) * (A+C) / (A+B+C+D)
    - Bayesian adjustment with gamma prior
    - Signal detection via percentile intervals

    Args:
        data: Spark DataFrame
        indexFields (list): Observational unit columns (e.g., ['personid'])
        groupFields (list): Treatment/feature group columns
        outcomeFields (list): Target/outcome columns
        alpha_prior (int): Discount factor for observed counts
        beta_prior (int): Discount factor for expected counts

    Returns:
        pandas DataFrame with O/E ratios, adjusted O/E, percentiles,
        p-values, and signal indicators
    """
    # Count distinct index per group x outcome (A)
    observed_A = data.groupBy(groupFields + outcomeFields).agg(
        F.countDistinct(*indexFields).alias('observed_A')
    )
    # Count distinct index per group (A + B)
    group_A_B = data.groupBy(groupFields).agg(
        F.countDistinct(*indexFields).alias('group_A_B')
    )
    # Count distinct index per outcome (A + C)
    event_A_C = data.groupBy(outcomeFields).agg(
        F.countDistinct(*indexFields).alias('event_A_C')
    )
    # Total distinct index (A + B + C + D)
    expected_denominator = data.agg(
        F.countDistinct(*indexFields)
    ).collect()[0][0]

    oe_table = (
        observed_A
        .join(group_A_B, on=groupFields, how='left')
        .join(event_A_C, on=outcomeFields, how='left')
        .withColumn('observed_ratio',
                    F.col('observed_A') / F.col('group_A_B'))
        .withColumn('total_A_B_C_D', F.lit(expected_denominator))
        .withColumn('prevalence_ratio',
                    F.col('event_A_C') / F.col('total_A_B_C_D'))
        .withColumn('expected_total',
                    F.col('group_A_B') * F.col('event_A_C')
                    / F.col('total_A_B_C_D'))
        .withColumn('observed_over_expected',
                    F.col('observed_A') / F.col('expected_total'))
        .withColumn('alpha_prior', F.lit(alpha_prior))
        .withColumn('beta_prior', F.lit(beta_prior))
        .withColumn('alpha_post',
                    F.col('observed_A') + F.col('alpha_prior'))
        .withColumn('beta_post',
                    F.col('expected_total') + F.col('beta_prior'))
        .withColumn('observed_over_expected_Adj',
                    F.col('alpha_post') / F.col('beta_post'))
    )

    oe_table_pd = oe_table.toPandas()
    oe_table_pd['Percentile05'] = oe_table_pd.apply(
        lambda row: gamma_percentile(
            row['alpha_post'], row['beta_post'], 0.05
        ), axis=1
    )
    oe_table_pd['Percentile95'] = oe_table_pd.apply(
        lambda row: gamma_percentile(
            row['alpha_post'], row['beta_post'], 0.95
        ), axis=1
    )
    oe_table_pd['observed_pvalue'] = oe_table_pd.apply(
        lambda row: calculate_percentile(
            row, observed='observed_A', expected='expected_total',
            alpha='alpha_prior', beta='beta_prior'
        ), axis=1
    )
    oe_table_pd['observed_pvalue_pre'] = oe_table_pd.apply(
        lambda row: calculate_percentile(
            row, observed='observed_A', expected='expected_total',
            alpha='alpha_post', beta='beta_post'
        ), axis=1
    )

    oe_table_pd = oe_table_pd.sort_values(
        by='Percentile05', ascending=False
    )
    oe_table_pd['Signal'] = ~oe_table_pd.apply(
        lambda row: row['Percentile05'] <= 1 <= row['Percentile95'],
        axis=1
    )
    oe_table_pd['SignalPositive'] = oe_table_pd.apply(
        lambda row: row['Percentile05'] > 1, axis=1
    )

    return oe_table_pd


def stackedSpark(df, cols, names_to, values_to, values_drop_na=True):
    """Convert a wide Spark DataFrame into long format.

    Similar to tidyr::pivot_longer in R. Converts binary indicator columns
    into a single name/value pair, filtering to rows where the indicator is 1.

    Args:
        df: Spark DataFrame
        cols (list): Columns to stack (indicator columns)
        names_to (str): Name for the new column holding former column names
        values_to (str): Name for the new column holding values
        values_drop_na (bool): Drop null values in the result

    Returns:
        Spark DataFrame in long format
    """
    index_fields = [item for item in df.columns if item not in cols]
    stack_expr = (
        "stack(" + str(len(cols)) + ", "
        + ", ".join(["'{0}', {0}".format(x) for x in cols])
        + ")"
    )
    df_long = (
        df.selectExpr(*index_fields, stack_expr)
        .withColumnRenamed("col0", names_to)
        .withColumnRenamed("col1", values_to)
    )
    df_long = df_long.filter(F.col(values_to) == 1.0)
    if values_drop_na:
        df_long = df_long.filter(F.col(values_to).isNotNull())
    return df_long


def countDistinct(tbl, field, index):
    """Count distinct values of index grouped by field, displayed styled.

    Args:
        tbl: Spark DataFrame
        field (str): Column to group by
        index (str): Column to count distinct values of

    Returns:
        Styled pandas DataFrame
    """
    result = (
        tbl
        .groupBy(field)
        .agg(F.countDistinct(index).alias('distinct_count'))
        .sort(F.col('distinct_count').desc())
        .toPandas()
        .style
        .hide(axis='index')
    )
    return result


def count_and_pivot(df, id_cols, names_from, index, sort_field='', obs=10):
    """Count distinct levels of index by id_cols and names_from, then pivot.

    Pivot so values of names_from become column names, id_cols are rows,
    and cell values are distinct counts.

    Args:
        df: Spark DataFrame
        id_cols (list): Row identifier columns
        names_from (str): Column whose values become new column names
        index (list): Columns to count
        sort_field (str): Column to sort by (descending)
        obs (int): Max rows to display

    Returns:
        pandas DataFrame in wide format
    """
    counts = (
        df.select([*index, *id_cols, names_from])
        .distinct()
        .groupBy(*id_cols, names_from)
        .agg(F.count('*').alias('count'))
    )

    columns = (
        df.select(names_from).distinct()
        .rdd.flatMap(lambda x: x).collect()
    )

    pivot_counts = (
        counts.groupBy(id_cols)
        .pivot(names_from, columns)
        .sum('count')
    )

    if sort_field != '':
        if sort_field not in pivot_counts.columns:
            sort_field = pivot_counts.columns[0]
        pivot_counts = pivot_counts.sort(F.col(sort_field).desc())

    result = pivot_counts.limit(obs).toPandas()
    return result


def aggregate_fields(df, index, fields, values, aggfuncs,
                     aggfunc_names=None, debug=False):
    """Aggregate specified fields using specified aggregation functions.

    Args:
        df: Spark DataFrame
        index (list): Index columns for distinct counting
        fields: Not used (groupby inferred from non-index, non-value columns)
        values (list): Value columns to aggregate
        aggfuncs (list): PySpark aggregation functions (e.g., [F.mean, F.max])
        aggfunc_names (list): Names for each aggregation function
        debug (bool): Print aggregation info

    Returns:
        Spark DataFrame with aggregated results
    """
    if aggfunc_names is None:
        aggfunc_names = [f.__name__ for f in aggfuncs]

    excluded_elements = set(index + values)
    all_columns = df.columns
    groupby_fields = [x for x in all_columns if x not in excluded_elements]

    agg_exprs = [
        f(F.col(value)).alias(f"{value}_{name}")
        for f, name, value in zip(aggfuncs, aggfunc_names, values)
    ]
    agg_exprs.append(
        F.countDistinct(F.col(index[0])).alias(
            f"{index[0]}_distinct_count"
        )
    )

    if debug:
        print(
            f"Aggregating {values} by {groupby_fields} "
            f"with {aggfunc_names} and counting distinct by {index[0]}"
        )

    agg_aliases = [expr._jc.toString() if hasattr(expr, '_jc')
                   else str(expr) for expr in agg_exprs]

    return df.groupby(groupby_fields).agg(*agg_exprs)


def aggregate_fields_count(df, index, values, debug=False):
    """Count distinct combinations of non-index/non-value fields.

    Args:
        df: Spark DataFrame
        index (list): Index columns
        values (list): Value columns to exclude
        debug (bool): Print debug info

    Returns:
        Spark DataFrame with distinct counts
    """
    excluded_elements = set(index + values)
    all_columns = df.columns
    groupby_fields = [x for x in all_columns if x not in excluded_elements]

    if debug:
        print(
            f"Counting distinct combinations of {groupby_fields} "
            f"by {index[0]}"
        )

    result = df.groupby(groupby_fields).agg(
        F.countDistinct(F.col(index[0])).alias(
            f"{index[0]}_distinct_count"
        )
    )
    return result


def groupCount(df, field, id='personid'):
    """Count unique values of id grouped by field (pandas).

    Args:
        df: pandas DataFrame
        field (str): Column to group by
        id (str): Column to count unique values of

    Returns:
        pandas DataFrame sorted by count descending
    """
    result = (
        df.groupby(field)[id]
        .nunique()
        .sort_values(ascending=False)
        .reset_index()
    )
    return result
