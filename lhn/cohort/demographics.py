"""
lhn/cohort/demographics.py

Demographic categorization functions for patient cohorts.
"""

from lhn.header import F, get_logger

logger = get_logger(__name__)


def group_ethnicities(df, column_name, result_column_name):
    """
    Group ethnicity values into standardized categories.
    
    Parameters:
        df (DataFrame): Input DataFrame
        column_name (str): Source ethnicity column
        result_column_name (str): Output column name
    
    Returns:
        DataFrame: DataFrame with new ethnicity column
    """
    return df.withColumn(result_column_name,
        F.when(F.col(column_name).isin([
            'Hispanic or Latino', 'Dominican', 'Mexican', 'South American',
            'Central American', 'Latin American', 'Puerto Rican', 'Nicaraguan',
            'Colombian', 'Peruvian', 'Mexicano', 'Guatemalan', 'Chilean',
            'Ecuadorian', 'Spaniard', 'Central American Indian', 'Salvadoran',
            'Cuban', 'Bolivian', 'Honduran'
        ]), 'Hispanic')
        .otherwise('Not Hispanic')
    )


def group_races(df, column_name, result_column_name):
    """
    Group race values into standardized categories.
    
    Parameters:
        df (DataFrame): Input DataFrame
        column_name (str): Source race column
        result_column_name (str): Output column name
    
    Returns:
        DataFrame: DataFrame with new race column
    """
    return df.withColumn(result_column_name,
        F.when(F.col(column_name).isin([
            'African', 'Black or African American', 'Black', 'African American',
            'Black, not of hispanic origin'
        ]), 'Black')
        .when(F.col(column_name).isin([
            'Alaska Indian', 'Alaska Native', 'American Indian',
            'American Indian or Alaska Native', 'Native Hawaiian',
            'Native Hawaiian or Other Pacific Islander', 'Other Pacific Islander'
        ]), 'Indigenous')
        .when(F.col(column_name).isin([
            'Asian', 'Asian Indian', 'Cambodian', 'Chinese', 'Filipino', 'Indian',
            'Indonesian', 'Japanese', 'Korean', 'Laotian', 'Malaysian', 'Sri Lankan',
            'Taiwanese', 'Thai', 'Vietnamese', 'Asian or Pacific islander'
        ]), 'Asian')
        .when(F.col(column_name).isin([
            'European', 'White or Caucasian', 'White', 'Caucasian',
            'Caucasian, not of hispanic origin'
        ]), 'White')
        .when(F.col(column_name).isin([
            'Middle Eastern or North African', 'Pakistani'
        ]), 'Middle Eastern')
        .when(F.col(column_name).isin([
            'Bahamian', 'Jamaican', 'Trinidadian'
        ]), 'Caribbean')
        .when(F.col(column_name).isin([
            'Hispanic', 'Hispanic, white', 'Hispanic, black'
        ]), 'Hispanic')
        .when(F.col(column_name).isin(['Mixed racial group']), 'Mixed')
        .otherwise('Other/Unknown')
    )


def group_races2(df, column_name, result_column_name):
    """
    Alternative race grouping with simplified categories.
    
    Parameters:
        df (DataFrame): Input DataFrame
        column_name (str): Source race column
        result_column_name (str): Output column name
    
    Returns:
        DataFrame: DataFrame with new race column
    """
    return df.withColumn(result_column_name,
        F.when(F.col(column_name).rlike('(?i)black|african'), 'Black')
        .when(F.col(column_name).rlike('(?i)white|caucasian|european'), 'White')
        .when(F.col(column_name).rlike('(?i)asian|chinese|japanese|korean|vietnamese'), 'Asian')
        .when(F.col(column_name).rlike('(?i)hispanic|latino'), 'Hispanic')
        .when(F.col(column_name).rlike('(?i)native|indian|alaska'), 'Indigenous')
        .otherwise('Other/Unknown')
    )


def group_gender(df, column_name, result_column_name):
    """
    Standardize gender values.
    
    Parameters:
        df (DataFrame): Input DataFrame
        column_name (str): Source gender column
        result_column_name (str): Output column name
    
    Returns:
        DataFrame: DataFrame with standardized gender
    """
    return df.withColumn(result_column_name,
        F.when(F.col(column_name).rlike('(?i)^male$|^m$'), 'Male')
        .when(F.col(column_name).rlike('(?i)^female$|^f$'), 'Female')
        .otherwise('Other/Unknown')
    )


def group_marital_status(df, column_name, result_column_name):
    """
    Group marital status into standardized categories.
    
    Parameters:
        df (DataFrame): Input DataFrame
        column_name (str): Source marital status column
        result_column_name (str): Output column name
    
    Returns:
        DataFrame: DataFrame with grouped marital status
    """
    return df.withColumn(result_column_name,
        F.when(F.col(column_name).rlike('(?i)married|spouse'), 'Married')
        .when(F.col(column_name).rlike('(?i)single|never'), 'Single')
        .when(F.col(column_name).rlike('(?i)divorced|separated'), 'Divorced/Separated')
        .when(F.col(column_name).rlike('(?i)widowed'), 'Widowed')
        .otherwise('Other/Unknown')
    )


def pick_preferred_death(demo_source,
                         priority=('EMR', 'BILLING', 'CLAIM'),
                         exclude_testpatients=True,
                         round_to_month_end=True,
                         id_fields=('personid', 'tenant'),
                         source_field='source',
                         deceased_field='deceased',
                         dateofdeath_field='dateofdeath',
                         testpatient_field='testpatientflag'):
    """
    Pick one death record per person from a demographics source, ranked by source priority.

    For each person, keeps the record whose ``source`` appears earliest in ``priority``,
    breaking ties by most recent ``dateofdeath``. Rows with null/empty deceased indicator
    are deprioritized. Test-patient rows are dropped by default.

    Parameters:
        demo_source (DataFrame): Flattened demographics source (e.g. ``r.demographicsSource.df``).
        priority (tuple): Source values in preferred order. Default ('EMR', 'BILLING', 'CLAIM').
        exclude_testpatients (bool): If True (default), drop rows where testpatientflag is True.
        round_to_month_end (bool): If True (default), coerce dateofdeath to month end via ``last_day()``.
        id_fields (tuple): Partition keys. Default ('personid', 'tenant').
        source_field (str): Column holding the source label.
        deceased_field (str): Column holding the deceased flag.
        dateofdeath_field (str): Column holding the death date (string or date).
        testpatient_field (str): Column holding the test-patient flag.

    Returns:
        DataFrame: One row per ``id_fields`` group, columns ``[*id_fields, deceased, dateofdeath]``.
    """
    from pyspark.sql import Window

    id_fields = list(id_fields)

    # Rank source in priority order; fall back to a catch-all high number.
    source_rank = F.when(F.col(source_field) == priority[0], 1)
    for i, src in enumerate(priority[1:], start=2):
        source_rank = source_rank.when(F.col(source_field) == src, i)
    source_rank = source_rank.otherwise(len(priority) + 1)

    # Deprioritize rows with missing/empty death indicator.
    missing_death_rank = F.when(
        F.col(deceased_field).isNull() | (F.col(dateofdeath_field) == ''), 1
    ).otherwise(0)

    win = Window.partitionBy(*id_fields).orderBy(
        missing_death_rank,
        source_rank,
        F.col(dateofdeath_field).desc(),
    )

    keep_cols = id_fields + [deceased_field, dateofdeath_field, source_field, testpatient_field]
    df = demo_source.select(*keep_cols)

    df = df.filter((F.col(deceased_field) == True) | (F.col(dateofdeath_field) != ''))
    if exclude_testpatients:
        df = df.filter(F.col(testpatient_field) != True)

    df = (df
          .withColumn('_row_number', F.row_number().over(win))
          .filter(F.col('_row_number') == 1)
          .drop('_row_number')
          .select(*id_fields, deceased_field, dateofdeath_field))

    if round_to_month_end:
        df = df.withColumn(dateofdeath_field,
                           F.last_day(F.to_date(F.col(dateofdeath_field))))
    return df


def assign_age_group(df, age_column, result_column_name,
                     bins=None, labels=None):
    """
    Assign patients to age groups.
    
    Parameters:
        df (DataFrame): Input DataFrame
        age_column (str): Column containing age values
        result_column_name (str): Output column name
        bins (list): Age boundaries (default: [0, 18, 35, 50, 65, 80, 200])
        labels (list): Group labels (default: standard clinical groups)
    
    Returns:
        DataFrame: DataFrame with age group column
    """
    if bins is None:
        bins = [0, 18, 35, 50, 65, 80, 200]
    if labels is None:
        labels = ['<18', '18-34', '35-49', '50-64', '65-79', '80+']

    if len(labels) != len(bins) - 1:
        raise ValueError(
            "labels must have exactly len(bins)-1 elements. "
            "Got {} labels for {} bins.".format(len(labels), len(bins)))

    # Build chained when expression from bins/labels
    age_col = F.col(age_column)
    expr = F.when(
        (age_col >= bins[0]) & (age_col < bins[1]), labels[0]
    )
    for i in range(1, len(labels)):
        expr = expr.when(
            (age_col >= bins[i]) & (age_col < bins[i + 1]), labels[i]
        )
    expr = expr.otherwise('Unknown')

    return df.withColumn(result_column_name, expr)
