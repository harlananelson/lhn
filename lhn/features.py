from pandas import DataFrame
from lhn.header import F, Any, List, spark, pd, display, Markdown, Window, DataFrame

from lhn.data_display import noRowNum

from lhn.introspection_utils import deduplicate_fields, fields_reconcile, translate_index

from lhn.data_transformation import flatten_schema, get_selected_fields


from lhn.spark_utils import convert_date_fields, explode_columns, use_last_value

from lhn.discern import add_concept_indicators

from lhn.header import get_logger

logger = get_logger(__name__)


def select_only_baseline(source_data, patient_data, 
                             date_field, value_fields,
                             baseline_days=365, 
                             id_fields=['personid', 'tenant'],
                             code_fields=None,
                             baseline_source='FirstTouchDate',
                             filter_to_baseline=False):
    """
    Creates baseline features by filtering source data to include only measurements
    performed within the specified period from a baseline date.
    
    This function joins source data (such as labs, vital signs, or medications) with
    patient demographic data, then adds indicator columns showing whether the record:
    1. Occurred within the first year of the patient's life
    2. Occurred within the specified baseline period (default 365 days) of their first touch date
    
    The resulting DataFrame can be used to create feature vectors for predictive modeling,
    ensuring features are based only on information available at baseline.
    
    Parameters:
    -----------
    source_data : DataFrame
        PySpark DataFrame containing the source data (labs, vitals, medications, etc.)
    
    patient_data : DataFrame
        PySpark DataFrame containing patient reference data with baseline dates
    
    date_field : str
        Name of the date field in source_data for temporal filtering
    
    value_fields : list
        List of fields containing values to be used as features (e.g., lab values, 
        medication codes, vital sign measurements)
    
    baseline_days : int, default=365
        Number of days after the baseline date to include in the baseline period
    
    id_fields : list, default=['personid', 'tenant']
        Fields identifying unique patients for joining
    
    code_fields : list, optional
        Fields identifying the type of measurement or event (e.g., lab code)
    
    baseline_source : str, default='FirstTouchDate'
        Field in patient_data to use as the baseline reference date
        
    filter_to_baseline : bool, default=False
        If True, only return records where baseline_period=True
    
    Returns:
    --------
    DataFrame
        PySpark DataFrame with filtered data and added baseline indicator columns
    
    Examples:
    ---------
    # For lab values:
    lab_baseline = select_only_baseline(
        e.Lab_LOINCEncounter.df, 
        e.ADSpatient.df,
        date_field='dateLab',
        value_fields=['typedvalue_numericValue_value', 'typedvalue_unitOfMeasure_standard_id'],
        code_fields=['labcode_standard_id', 'labcode_standard_primaryDisplay'],
        filter_to_baseline=True
    )
    """
    # Default to empty list if code_fields is None
    if code_fields is None:
        code_fields = []
    
    # Determine fields to select from source data
    source_fields = [
        *id_fields, 
        date_field, 
        *code_fields, 
        *value_fields
    ]
    
    # Select needed fields from patient data
    patient_fields = [
        *id_fields, 
        'SCD', 
        'yearofbirth', 
        baseline_source, 
        'followdate', 
        'deceased'
    ]
    
    # Filter source data and join with patient data
    filtered_df = (
        source_data
        .select(source_fields)
        .join(
            patient_data.select(patient_fields),
            on=id_fields
        )
        # Add indicator for observations in first year of life
        .withColumn(
            'followed_from_birth',
            F.when(
                (F.year(F.col(date_field)) - F.col('yearofbirth')) <= 1,
                F.lit(True)
            ).otherwise(F.lit(False))
        )
        # Add indicator for observations within baseline period
        .withColumn(
            'baseline_period',
            F.when(
                (F.datediff(F.col(date_field), F.col(baseline_source)) <= baseline_days) &
                (F.datediff(F.col(date_field), F.col(baseline_source)) >= 0),
                F.lit(True)
            ).otherwise(F.lit(False))
        )
    )
    
    # Optionally filter to keep only baseline records
    if filter_to_baseline:
        filtered_df = filtered_df.filter(F.col('baseline_period') == True)
    
    return filtered_df


def analyze_clinical_measurements(baseline_data, 
                                 code_fields,
                                 value_field,
                                 date_field,
                                 display_field=None,
                                 unit_field=None,
                                 id_fields=['personid', 'tenant'],
                                 min_measurements=1):
    """
    Analyzes filtered clinical measurements to generate statistical features.
    
    This function takes data that has already been filtered to a baseline period and
    calculates various statistical measures and trends for each patient and measurement type,
    creating features suitable for predictive modeling.
    
    Parameters:
    -----------
    baseline_data : DataFrame
        PySpark DataFrame containing filtered clinical data (output from create_baseline_features)
    
    code_fields : list
        Fields identifying the measurement type (e.g., lab code, vital sign code)
    
    value_field : str
        Field containing the numeric values to analyze
    
    date_field : str
        Field containing measurement dates for temporal analysis
    
    display_field : str, optional
        Field containing human-readable description of the measurement
    
    unit_field : str, optional
        Field containing the unit of measurement
    
    id_fields : list, default=['personid', 'tenant']
        Fields identifying unique patients
    
    min_measurements : int, default=1
        Minimum number of measurements required to compute features
    
    Returns:
    --------
    DataFrame
        PySpark DataFrame with patient-level clinical measurement features
    
    Examples:
    ---------
    # Analyze vital sign measurements to create statistical features
e.vital_sign_encountermeasures.df = analyze_clinical_measurements(
    e.vital_sign_encounterDemoBaseline.df,  # Input: already filtered baseline data
    code_fields=['measurementcode_standard_id'],  # Fields identifying measurement type
    value_field='typedvalue_numericValue_value',  # Field containing numeric values
    date_field='dateMeasurement',  # Date field for temporal analysis
    display_field='measurementcode_standard_primaryDisplay',  # Human-readable measurement name
    unit_field='typedvalue_unitOfMeasure_standard_id'  # Unit of measurement field
)

# To view summary statistics of the resulting features
e.vital_sign_encountermeasures.attrition()

# To create a wide-format feature matrix
e.vital_sign_encountermeasuresMatrix.df = create_feature_matrix(
    e.vital_sign_encountermeasures.df,
    display_field='measurementcode_standard_primaryDisplay',
    feature_prefix='vital_'
)

Example Prompt:
I need to create a clinical feature engineering pipeline for healthcare data analytics. I have an Extract object 'e' containing DataFrames for various clinical measurements (e.g., vital signs, lab tests, medications) that have already been filtered to a baseline period.

I specifically want to analyze vital sign measurements that have been filtered to a one-year baseline period. The measurements are stored in 'e.vital_sign_encounterDemoBaseline.df' with these key columns:
- 'personid', 'tenant' (patient identifiers)
- 'measurementcode_standard_id', 'measurementcode_standard_primaryDisplay' (measurement type)
- 'dateMeasurement' (date field)
- 'typedvalue_numericValue_value' (the actual measurement value)
- 'typedvalue_unitOfMeasure_standard_id' (unit of measurement)

I need a function called 'analyze_clinical_measurements' that:
1. Groups measurements by patient and measurement type
2. Calculates statistical summaries (mean, median, min, max, standard deviation)
3. Analyzes trends over time (increasing, decreasing, stable)
4. Creates derived features like coefficient of variation and measurement frequency
5. Returns a DataFrame with one row per patient per measurement type

The output should be stored as 'e.vital_sign_encountermeasures.df'.

Please provide the implementation of this function and an example of how to call it with my specific data. The function should work for other clinical measurements with minimal modification.
    """
    # Ensure code_fields is a list
    if not isinstance(code_fields, list):
        code_fields = [code_fields]
    
    # Select display field if not specified
    if display_field is None and code_fields:
        display_field = code_fields[0]
    
    # Set up window specs for analysis
    window_time_ordered = Window.partitionBy(*id_fields, *code_fields).orderBy(date_field)
    
    # Convert value field to numeric
    analysis_df = (
        baseline_data
        .withColumn('value_numeric', 
                    F.when(F.col(value_field).isNotNull(), 
                           F.col(value_field).cast('double'))
                    .otherwise(None))
    )
    
    # Build the list of grouping fields
    group_fields = [*id_fields, *code_fields]
    if display_field and display_field not in group_fields:
        group_fields.append(display_field)
    
    # Add unit field to grouping if provided
    if unit_field:
        # Only include unit field if it exists in the dataset
        if unit_field in baseline_data.columns:
            group_fields.append(unit_field)
    
    # Calculate basic statistics
    feature_df = (
        analysis_df
        .groupBy(group_fields)
        .agg(
            # Count and date ranges
            F.count('*').alias('measurement_count'),
            F.countDistinct(date_field).alias('distinct_dates'),
            F.min(date_field).alias('first_date'),
            F.max(date_field).alias('last_date'),
            F.datediff(F.max(date_field), F.min(date_field)).alias('date_span'),
            
            # Basic statistics
            F.avg('value_numeric').alias('mean_value'),
            F.stddev('value_numeric').alias('stddev_value'),
            F.min('value_numeric').alias('min_value'),
            F.max('value_numeric').alias('max_value'),
            F.expr('percentile(value_numeric, 0.5)').alias('median_value')
        )
        .filter(F.col('measurement_count') >= min_measurements)
    )
    
    # Add trend analysis for measurements with at least 2 data points
    trend_df = (
        analysis_df
        .filter(F.col('value_numeric').isNotNull())
        .withColumn('prev_date', F.lag(date_field).over(window_time_ordered))
        .withColumn('prev_value', F.lag('value_numeric').over(window_time_ordered))
        .withColumn('days_between', F.datediff(F.col(date_field), F.col('prev_date')))
        .withColumn('daily_change', 
                   F.when(F.col('days_between') > 0,
                          (F.col('value_numeric') - F.col('prev_value')) / F.col('days_between'))
                   .otherwise(None))
        .groupBy(*id_fields, *code_fields)
        .agg(
            F.avg('daily_change').alias('avg_daily_change'),
            F.min('daily_change').alias('min_daily_change'),
            F.max('daily_change').alias('max_daily_change')
        )
    )
    
    # Join trend information and add derived features
    result_df = (
        feature_df
        .join(trend_df, on=[*id_fields, *code_fields], how='left')
        # Add derived features
        .withColumn('value_range', F.col('max_value') - F.col('min_value'))
        .withColumn('coefficient_variation', 
                   F.when(F.col('mean_value') != 0,
                          F.col('stddev_value') / F.abs(F.col('mean_value')))
                   .otherwise(None))
        .withColumn('normalized_range', 
                   F.when(F.col('mean_value') != 0,
                          F.col('value_range') / F.abs(F.col('mean_value')))
                   .otherwise(None))
        .withColumn('measurements_per_day',
                   F.when(F.col('date_span') > 0,
                          F.col('measurement_count') / F.col('date_span'))
                   .otherwise(F.lit(1)))
        .withColumn('trend_direction',
                   F.when(F.col('measurement_count') < 2, 'insufficient_data')
                   .when(F.col('avg_daily_change').isNull(), 'no_trend')
                   .when(F.col('avg_daily_change') > 0.01, 'increasing')
                   .when(F.col('avg_daily_change') < -0.01, 'decreasing')
                   .otherwise('stable'))
    )
    
    return result_df


def test(baseline_data, 
                                 code_fields,
                                 value_field,
                                 date_field,
                                 display_field=None,
                                 unit_field=None,
                                 id_fields=['personid', 'tenant'],
                                 min_measurements=1):
    
    # Ensure code_fields is a list
    if not isinstance(code_fields, list):
        code_fields = [code_fields]
    
    # Select display field if not specified
    if display_field is None and code_fields:
        display_field = code_fields[0]
    
    # Set up window specs for analysis
    window_time_ordered = Window.partitionBy(*id_fields, *code_fields).orderBy(date_field)
    
    # Convert value field to numeric
    analysis_df = (
        baseline_data
        .withColumn('value_numeric', 
                    F.when(F.col(value_field).isNotNull(), 
                           F.col(value_field).cast('double'))
                    .otherwise(None))
    )
    
    # Build the list of grouping fields
    group_fields = [*id_fields, *code_fields]
    if display_field and display_field not in group_fields:
        group_fields.append(display_field)
    
    # Add unit field to grouping if provided
    if unit_field:
        # Only include unit field if it exists in the dataset
        if unit_field in baseline_data.columns:
            group_fields.append(unit_field)
    
    # Calculate basic statistics
    feature_df = (
        analysis_df
        .groupBy(group_fields)
        .agg(
            # Count and date ranges
            F.count('*').alias('measurement_count'),
            F.countDistinct(date_field).alias('distinct_dates'),
            F.min(date_field).alias('first_date'),
            F.max(date_field).alias('last_date'),
            F.datediff(F.max(date_field), F.min(date_field)).alias('date_span'),
            
            # Basic statistics
            F.avg('value_numeric').alias('mean_value'),
            F.stddev('value_numeric').alias('stddev_value'),
            F.min('value_numeric').alias('min_value'),
            F.max('value_numeric').alias('max_value'),
            F.expr('percentile(value_numeric, 0.5)').alias('median_value')
        )
        .filter(F.col('measurement_count') >= min_measurements)
    )
    
    return(feature_df)
        