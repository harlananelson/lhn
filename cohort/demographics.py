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
        F.when(F.col(column_name).rlike('(?i)^m|male'), 'Male')
        .when(F.col(column_name).rlike('(?i)^f|female'), 'Female')
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
    
    # Build case expression
    conditions = []
    for i in range(len(labels)):
        conditions.append(
            F.when(
                (F.col(age_column) >= bins[i]) & (F.col(age_column) < bins[i+1]),
                labels[i]
            )
        )
    
    # Chain conditions
    result = conditions[0]
    for cond in conditions[1:]:
        result = result.when(cond._jc.expr().child(), labels[conditions.index(cond)])
    
    # Simpler approach using nested when
    return df.withColumn(result_column_name,
        F.when(F.col(age_column) < 18, '<18')
        .when(F.col(age_column) < 35, '18-34')
        .when(F.col(age_column) < 50, '35-49')
        .when(F.col(age_column) < 65, '50-64')
        .when(F.col(age_column) < 80, '65-79')
        .otherwise('80+')
    )
