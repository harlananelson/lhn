"""
lhn/features.py

Clinical feature engineering functions restored from v0.1.0.
These support the feature creation workflow for predictive modeling:
- Baseline period filtering (select_only_baseline)
- Clinical measurement statistical features (analyze_clinical_measurements)

Restored: 2026-03-15 from v0.1.0-monolithic branch.
"""

from lhn.header import (
    spark, F, pd, DataFrame, display, Markdown, Window, get_logger
)

logger = get_logger(__name__)


def select_only_baseline(source_data, patient_data,
                         date_field, value_fields,
                         baseline_days=365,
                         id_fields=None,
                         code_fields=None,
                         baseline_source='FirstTouchDate',
                         filter_to_baseline=False):
    """Filter source data to the baseline period relative to first touch.

    Creates baseline features by filtering source data (labs, vitals,
    medications) to include only records within the specified period
    from a baseline date. Adds indicator columns for:
    1. Observations in the first year of life
    2. Observations within the baseline period

    Args:
        source_data: Spark DataFrame with clinical measurements
        patient_data: Spark DataFrame with patient demographics/dates
        date_field (str): Date column in source_data
        value_fields (list): Value columns to retain
        baseline_days (int): Days after baseline to include (default: 365)
        id_fields (list): Patient identifier columns
        code_fields (list): Measurement type identifier columns
        baseline_source (str): Column in patient_data for baseline date
        filter_to_baseline (bool): If True, return only baseline records

    Returns:
        Spark DataFrame with baseline indicators added
    """
    if id_fields is None:
        id_fields = ['personid', 'tenant']
    if code_fields is None:
        code_fields = []

    source_fields = [*id_fields, date_field, *code_fields, *value_fields]

    patient_fields = [
        *id_fields, 'SCD', 'yearofbirth', baseline_source,
        'followdate', 'deceased'
    ]

    filtered_df = (
        source_data
        .select(source_fields)
        .join(patient_data.select(patient_fields), on=id_fields)
        .withColumn(
            'followed_from_birth',
            F.when(
                (F.year(F.col(date_field)) - F.col('yearofbirth')) <= 1,
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        .withColumn(
            'baseline_period',
            F.when(
                (F.datediff(F.col(date_field), F.col(baseline_source))
                 <= baseline_days)
                & (F.datediff(F.col(date_field), F.col(baseline_source))
                   >= 0),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    )

    if filter_to_baseline:
        filtered_df = filtered_df.filter(F.col('baseline_period'))

    return filtered_df


def analyze_clinical_measurements(baseline_data,
                                  code_fields,
                                  value_field,
                                  date_field,
                                  display_field=None,
                                  unit_field=None,
                                  id_fields=None,
                                  min_measurements=1):
    """Generate statistical features from filtered clinical measurements.

    Takes data already filtered to a baseline period and calculates
    statistical summaries and trends per patient per measurement type:
    - Basic stats: mean, median, min, max, stddev
    - Temporal: first/last date, date span, measurements per day
    - Trends: avg/min/max daily change, trend direction
    - Derived: coefficient of variation, normalized range

    Args:
        baseline_data: Spark DataFrame (output from select_only_baseline)
        code_fields (list or str): Measurement type identifier columns
        value_field (str): Numeric value column
        date_field (str): Date column
        display_field (str): Human-readable measurement name column
        unit_field (str): Unit of measurement column
        id_fields (list): Patient identifier columns
        min_measurements (int): Minimum measurements to compute features

    Returns:
        Spark DataFrame with one row per patient per measurement type
    """
    if id_fields is None:
        id_fields = ['personid', 'tenant']
    if not isinstance(code_fields, list):
        code_fields = [code_fields]
    if display_field is None and code_fields:
        display_field = code_fields[0]

    window_time_ordered = Window.partitionBy(
        *id_fields, *code_fields
    ).orderBy(date_field)

    analysis_df = baseline_data.withColumn(
        'value_numeric',
        F.when(
            F.col(value_field).isNotNull(),
            F.col(value_field).cast('double')
        ).otherwise(None)
    )

    group_fields = [*id_fields, *code_fields]
    if display_field and display_field not in group_fields:
        group_fields.append(display_field)
    if unit_field and unit_field in baseline_data.columns:
        group_fields.append(unit_field)

    feature_df = (
        analysis_df
        .groupBy(group_fields)
        .agg(
            F.count('*').alias('measurement_count'),
            F.countDistinct(date_field).alias('distinct_dates'),
            F.min(date_field).alias('first_date'),
            F.max(date_field).alias('last_date'),
            F.datediff(F.max(date_field), F.min(date_field))
            .alias('date_span'),
            F.avg('value_numeric').alias('mean_value'),
            F.stddev('value_numeric').alias('stddev_value'),
            F.min('value_numeric').alias('min_value'),
            F.max('value_numeric').alias('max_value'),
            F.expr('percentile(value_numeric, 0.5)')
            .alias('median_value')
        )
        .filter(F.col('measurement_count') >= min_measurements)
    )

    trend_df = (
        analysis_df
        .filter(F.col('value_numeric').isNotNull())
        .withColumn('prev_date', F.lag(date_field).over(window_time_ordered))
        .withColumn('prev_value',
                    F.lag('value_numeric').over(window_time_ordered))
        .withColumn('days_between',
                    F.datediff(F.col(date_field), F.col('prev_date')))
        .withColumn(
            'daily_change',
            F.when(
                F.col('days_between') > 0,
                (F.col('value_numeric') - F.col('prev_value'))
                / F.col('days_between')
            ).otherwise(None)
        )
        .groupBy(*id_fields, *code_fields)
        .agg(
            F.avg('daily_change').alias('avg_daily_change'),
            F.min('daily_change').alias('min_daily_change'),
            F.max('daily_change').alias('max_daily_change')
        )
    )

    result_df = (
        feature_df
        .join(trend_df, on=[*id_fields, *code_fields], how='left')
        .withColumn('value_range',
                    F.col('max_value') - F.col('min_value'))
        .withColumn(
            'coefficient_variation',
            F.when(
                F.col('mean_value') != 0,
                F.col('stddev_value') / F.abs(F.col('mean_value'))
            ).otherwise(None)
        )
        .withColumn(
            'normalized_range',
            F.when(
                F.col('mean_value') != 0,
                F.col('value_range') / F.abs(F.col('mean_value'))
            ).otherwise(None)
        )
        .withColumn(
            'measurements_per_day',
            F.when(
                F.col('date_span') > 0,
                F.col('measurement_count') / F.col('date_span')
            ).otherwise(F.lit(1))
        )
        .withColumn(
            'trend_direction',
            F.when(F.col('measurement_count') < 2, 'insufficient_data')
            .when(F.col('avg_daily_change').isNull(), 'no_trend')
            .when(F.col('avg_daily_change') > 0.01, 'increasing')
            .when(F.col('avg_daily_change') < -0.01, 'decreasing')
            .otherwise('stable')
        )
    )

    return result_df
