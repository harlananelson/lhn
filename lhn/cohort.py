
from lhn.spark_query import add_parsed_column
from lhn.header import F, StringType, DataFrame, display, Markdown, concat_ws, Window, StorageLevel, re, spark
from lhn.spark_utils import distCol, writeTable 
from lhn.list_operations import noColColide, unique_non_none
from lhn.header import JOIN_INNER, pprint
from lhn.header import get_logger

logger = get_logger(__name__)


# Example usage
""" 
histStop = '2023-12-31'  # Replace with your actual maxDate
result = calcUsage(
    df             = r.encounterSource,
    fields         = ['personid', 'actualarrivaldate', 'servicedate'],
    dateCoalesce   = F.coalesce(F.col('actualarrivaldate'), F.col('servicedate')),
    minDate        = '1950-01-01', maxDate=histStop,
    index          = ['personid'],
    countFieldName = 'encounters',
    dateFirst      = 'encDateFirst',
    dateLast       = 'encDateLast'
                   )
"""

def group_ethnicities(df, column_name, result_column_name):
    return df.withColumn(result_column_name,
        F.when(F.col(column_name).isin(['Hispanic or Latino', 'Dominican','Mexican', 'South American','Central American',
                       'Latin American', 'Puerto Rican', 'Nicaraguan', 'Colombian', 'Peruvian',
                       'Mexicano', 'Guatemalan', 'Chilean', 'Ecuadorian', 'Spaniard', 'Central American Indian',
                       'Salvadoran', 'Cuban', 'Bolivian', 'Honduran']), 'Hispanic')
        .otherwise('Not Hispanic')
    )
def group_races(df, column_name, result_column_name):
    return df.withColumn(result_column_name,
        F.when(F.col(column_name).isin(['African', 'Black or African American', 'Black', 'African American', 'Black, not of hispanic origin']), 'Black')
        .when(F.col(column_name).isin(['Alaska Indian', 'Alaska Native', 'American Indian', 'American Indian or Alaska Native',
                                       'Native Hawaiian', 'Native Hawaiian or Other Pacific Islander', 'Other Pacific Islander', 'American Indian']), 'Indigenous')
        .when(F.col(column_name).isin(['Asian', 'Asian Indian', 'Cambodian', 'Chinese', 'Filipino', 'Indian', 'Indonesian', 'Japanese',
                                       'Korean', 'Laotian', 'Malaysian', 'Sri Lankan', 'Taiwanese', 'Thai', 'Vietnamese', 'Asian or Pacific islander',
                                       'Asian Indian', 'Chinese', 'Filipino', 'Korean']), 'Asian')
        .when(F.col(column_name).isin(['European', 'White or Caucasian', 'White', 'Caucasian', 'Caucasian, not of hispanic origin']), 'White')
        .when(F.col(column_name).isin(['Middle Eastern or North African', 'Pakistani']), 'Middle Eastern')
        .when(F.col(column_name).isin(['Bahamian', 'Jamaican', 'Trinidadian']), 'Caribbean')
        .when(F.col(column_name).isin(['Hispanic', 'Hispanic, white', 'Hispanic, black']), 'Hispanic')
        .when(F.col(column_name).isin(['Mixed racial group']), 'Mixed')
        .when(F.col(column_name).isin(['Unknown racial group', 'None', 'Refusal by patient to provide information about racial group',
                                       'Patient data refused', 'Race not stated', 'Patient not asked', 'PATIENT_DECLINED']), 'Unknown')
        .when(F.col(column_name).isin(['Other Race']), 'Other')
        .otherwise('Unknown')
    )
    
def group_races2(df, column_name, result_column_name):
    return df.withColumn(result_column_name,
        F.when(F.col(column_name).isin(['African', 'Black or African American', 'Black', 'African American', 'Black, not of hispanic origin']), 'Black')
        .when(F.col(column_name).isin(['Alaska Indian', 'Alaska Native', 'American Indian', 'American Indian or Alaska Native',
                                       'Native Hawaiian', 'Native Hawaiian or Other Pacific Islander', 'Other Pacific Islander', 'American Indian']), 'Indigenous')
        .when(F.col(column_name).isin(['Asian', 'Asian Indian', 'Cambodian', 'Chinese', 'Filipino', 'Indian', 'Indonesian', 'Japanese',
                                       'Korean', 'Laotian', 'Malaysian', 'Sri Lankan', 'Taiwanese', 'Thai', 'Vietnamese', 'Asian or Pacific islander',
                                       'Asian Indian', 'Chinese', 'Filipino', 'Korean']), 'Asian')
        .when(F.col(column_name).isin(['European', 'White or Caucasian', 'White', 'Caucasian', 'Caucasian, not of hispanic origin']), 'White')
        .when(F.col(column_name).isin(['Middle Eastern or North African', 'Pakistani']), 'Middle Eastern')
        .when(F.col(column_name).isin(['Bahamian', 'Jamaican', 'Trinidadian']), 'Caribbean')
        .when(F.col(column_name).isin(['Hispanic', 'Hispanic, white', 'Hispanic, black']), 'Hispanic')
        .when(F.col(column_name).isin(['Mixed racial group']), 'Mixed')
        .when(F.col(column_name).isin(['Unknown racial group', 'None', 'Refusal by patient to provide information about racial group',
                                       'Patient data refused', 'Race not stated', 'Patient not asked', 'PATIENT_DECLINED', 'Other Race']), 'Other/Unknown')
        .otherwise('Other/Unknown')
    )
def group_gender(df, column_name, result_column_name):
    return df.withColumn(result_column_name,
        F.when(F.col(column_name).isin(['f', 'female', 'Female']), 'Female')
        .when(F.col(column_name).isin(['M', 'male', 'Male']), 'Male')
        .when(F.col(column_name).isin(['other', 'Other']), 'Other')
        .when(F.col(column_name).isin(['UNK', '394743007', '394744001', 'unknown', 'Gender unknown', 'Gender unspecified']), 'Unknown')
        .otherwise('Unknown')
    )
def group_marital_status(df, column_target, column_source):
    return df.withColumn(column_target,
        F.when(F.col(column_source).isin(['M', 'Married', 'MARRIED', '132793', '331328']), 'Married')
        .when(F.col(column_source).isin(['S', 'D', 'W', 'L', 'T', 'A', 'U', 'Single', 'Divorced', 'Widowed', 'Legally Separated',
                                      'Domestic partner', 'Separated', 'Unknown', 'Never Married', 'Annulled']), 'Not Married')
        .when(F.col(column_source).isNull(), 'Unknown')
        .when(F.col(column_source).isin(['None', 'UNK', 'O', 'Refusal by patient to provide information about marital status', 
                                       'Patient data refused', 'UNKNOWN', '- UTD']), 'Unknown')
        .otherwise('Unknown')
    )

def assign_age_group(df, age_column):
    """
    Assign age group based on quantile boundaries of age column in dataframe.

    :param df: PySpark dataframe
    :param age_column: str, name of column with age data
    :return: PySpark dataframe with new column 'age_group' containing age group labels
    """
    # Define quantile boundaries
    quantiles = [0.25, 0.5, 0.75]
    age_quantiles = df.approxQuantile(age_column, quantiles, 0.05)  # 0.05 is the relative error

    # Define function to assign age group
    def assign_age_group_(age):
        if age <= age_quantiles[0]:
            return 'Q1'
        elif age <= age_quantiles[1]:
            return 'Q2'
        elif age <= age_quantiles[2]:
            return 'Q3'
        else:
            return 'Q4'

    # Register function as a UDF
    assign_age_group_udf = F.udf(assign_age_group_, StringType())

    # Add new column 'age_group'
    df_with_age_group = df.withColumn('age_group', assign_age_group_udf(F.col(age_column)))

    return df_with_age_group

def calcUsage(df, fields, dateCoalesce, minDate, maxDate, index, countFieldName, dateFirst, dateLast):
    """
    Find the date of the first and last encounter record for each patient and count the number of encounters
    """
    result_df = (
        df.select(fields)
          .withColumn('dateEnc', dateCoalesce)
          .filter(F.col('dateEnc') <= F.to_date(F.lit(maxDate)))
          .filter(F.col('dateEnc') >= F.to_date(F.lit(minDate)))
          .groupBy(index)
          .agg(
              F.min('dateEnc').alias(dateFirst), 
              F.max('dateEnc').alias(dateLast),
              F.count('*').alias(countFieldName)  # count all rows in each group
           )
          .withColumn(dateFirst, F.to_date(F.col(dateFirst)))
          .withColumn(dateLast, F.to_date(F.col(dateLast)))
    )
    return result_df

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import logging
from pprint import pformat

# Initialize SparkSession
spark = SparkSession.builder.appName("write_index_table").getOrCreate()

# Initialize logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def unique_non_none_edit(*args, masterList=None):
    """Return a list of unique non-None values in the arguments, flattening lists and preserving order.
    If masterList is provided, the return list will be a subset of masterList.
    """
    unique_values = []
    for arg in args:
        if arg is not None:
            if isinstance(arg, list):
                for item in arg:
                    if item not in unique_values and (masterList is None or item in masterList):
                        unique_values.append(item)
            elif arg not in unique_values and (masterList is None or arg in masterList):
                unique_values.append(arg)
    return unique_values

def write_index_table_edit(inTable, index_field, retained_fields, datefieldPrimary, code, sort_fields, max_gap=370, indexLabel="index", lastLabel="last", histStart=None, histEnd=None, datefieldStop=None):
    """
    Create a cohort style table:
    - Identify the first (index) date and last date of care for a person and create a cohort style table.
    - Count the number of encounters
    - Calculate the number of days to the next encounter
    - Calculate the longest gap between encounters
    - Limit observation to the last encounter or encounters within a maximum gap.
    - Reduce to one encounter for each person and retain the retained_fields for the first encounter for each person.
    
    Parameters:
    - inTable: Spark DataFrame to be processed.
    - index_field: List of fields to identify unique records in the final result table.
    - retained_fields: List of columns from `inTable` to be kept in the output DataFrame.
    - datefieldPrimary: (str) Main field used for sorting. Can also accept custom sorting order via a dictionary.
    - code: Identifier for naming and tagging calculated fields.
    - sort_fields: List of fields to be used for sorting. If not provided, `datefieldPrimary` is used.
    - max_gap: Maximum gap in days to consider for encounters.
    - indexLabel: Prefix label for the index calculated field.
    - lastLabel: Prefix label for the last calculated field.
    - histStart: Date of history start.
    - histEnd: Date of history end.
    - datefieldStop: The date field used to evaluate histEnd dates, such as in medication Stop Day, otherwise can be a replicate of datefieldPrimary.
    
    Returns:
    - cohort_index: Spark DataFrame with the index date and other calculated fields.
    """
    
    callFun = {
        'inTable': inTable,
        'index_field': index_field,
        'retained_fields': retained_fields,
        'datefieldPrimary': datefieldPrimary,
        'code': code,
        'sort_fields': sort_fields,
        'max_gap': max_gap,
        'indexLabel': indexLabel,
        'lastLabel': lastLabel,
        'histStart': histStart,
        'histEnd': histEnd,
        'datefieldStop': datefieldStop
    }
    
    logger.info(f"The function write_index_table is called with the parameters: \n{pformat(callFun)}\n")
    
    # Check if datefieldPrimary is a list, and if so, take the first element
    datefieldPrimary = unique_non_none(datefieldPrimary, masterList=inTable.columns)[0]
    datefieldStop = unique_non_none(datefieldStop, datefieldPrimary, masterList=inTable.columns)[0]
    sort_fields = unique_non_none(sort_fields, datefieldPrimary, datefieldStop, masterList=inTable.columns)
    
    retained_fieldsEdit = noColColide(retained_fields, [*index_field, datefieldPrimary], index=[], masterList=inTable.columns)
    
    df = inTable.withColumn("unique_id", F.monotonically_increasing_id())
    df = df.withColumn("combined_index", F.concat_ws("_", *index_field))
    
    if code is None:
        code = ""
    
    distinct_dates = df.select(*index_field, datefieldPrimary).distinct()
    distinct_counts = distinct_dates.groupBy(*index_field).agg(F.countDistinct(datefieldPrimary).alias('encounter_days'))
    distinct_counts.createOrReplaceTempView("distinct_counts_temp")
    distinct_counts = spark.table("distinct_counts_temp")
    
    logger.info(f"datefieldPrimary is {datefieldPrimary} and index_field is {index_field} and sort_fields is {sort_fields} and retained_fields is {retained_fields}")
    
    lastName = lastLabel + "_" + code
    indexName = indexLabel + "_" + code
    lastTherapyName = lastLabel + "_therapy_" + code
    indexTherapyName = indexLabel + "_therapy_" + code
    
    logger.info(f"New Date fields {lastName} and {indexName} and {lastTherapyName} and {indexTherapyName}")
    
    if histStart is not None:
        logger.info(f"histStart is {histStart} and datefieldPrimary is : {datefieldPrimary} \n")
        df = df.filter(F.col(datefieldPrimary) >= histStart).filter(F.col(datefieldPrimary).isNotNull())
    else:
        logger.info("histStart is None \n")
    
    if histEnd is not None:
        logger.info(f"histEnd is {histEnd} and datefieldPrimary is : {datefieldPrimary} \n")
        df = df.filter(F.col(datefieldPrimary) <= histEnd).filter(F.col(datefieldPrimary).isNotNull())
    else:
        logger.info("histEnd is None \n")
    
    windowSpec = (
        Window.partitionBy(index_field)
        .orderBy(F.asc_nulls_last(datefieldPrimary), F.asc_nulls_last(datefieldStop))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    
    windowSpecLead = Window.partitionBy(*index_field).orderBy(F.desc_nulls_last(datefieldPrimary), F.asc_nulls_last(datefieldStop))
    windowSpecDistinctDate = Window.partitionBy(*index_field, datefieldPrimary).orderBy('unique_id')
    windowSpecDistinctPerson = Window.partitionBy(*index_field).orderBy(F.col(datefieldPrimary).desc_nulls_last(), 'unique_id')
    
    windowSpec_therapy = (
        Window.partitionBy([*index_field, 'course_of_therapy'])
        .orderBy(F.asc_nulls_last(datefieldPrimary), F.asc_nulls_last(datefieldStop))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    
    windowSpecDistinctPersonTherapy = Window.partitionBy(*index_field, 'course_of_therapy').orderBy(F.col(datefieldPrimary).asc_nulls_last(), F.asc_nulls_last(datefieldStop), 'unique_id')
    
    cohort_index_1 = (
        df
        .select([*index_field, *retained_fieldsEdit, datefieldPrimary, 'unique_id'])
        .withColumn(datefieldStop, F.greatest(F.coalesce(F.col(datefieldStop), F.col(datefieldPrimary)), F.col(datefieldPrimary)))
        .withColumn('rownum', F.row_number().over(windowSpecDistinctDate))
        .withColumn(indexName, F.first(datefieldPrimary, ignorenulls=True).over(windowSpec))
        .withColumn(lastName, F.last(datefieldStop, ignorenulls=True).over(windowSpec))
        .filter(F.col('rownum') == 1)
        .join(F.broadcast(distinct_counts), on=index_field, how='left')
        .withColumn('next_date', F.lag(datefieldPrimary).over(windowSpecLead))
        .withColumn('days_to_next_encounter', F.datediff(F.col('next_date'), F.col(datefieldStop)))
        .withColumn('last_course_day',
                    F.when(F.col('days_to_next_encounter') <= max_gap, False)
                    .when(F.col(datefieldStop) == F.col(lastName), False)
                    .otherwise(True))
        .withColumn('course_of_therapy', F.sum(F.when(F.col('last_course_day'), 1).otherwise(0)).over(windowSpecLead))
        .sort(['personid', datefieldPrimary])
    )
    
   

def write_index_table(inTable, index_field, retained_fields, datefieldPrimary, 
                      code, sort_fields, max_gap = 370, indexLabel="index", lastLabel="last",
                      histStart = None, histEnd = None, datefieldStop = None):
    
    """
    Create a cohort style table:
    - Identify the first (index) date and last date of care for a person and create a cohort style table.
    - Count the number of encounters
    - Caclulate the number of days to the next encounter
    - Calculate the longest gap between encounters
    - Limit observation to the last encounter or encounters within a maximum gap.
    - Reduce to one encounter for each person and retain the retained_fields for the first encounter for each person.

    Parameters:
    - inTable: Spark DataFrame to be processed.
    - index_field: List of fields to identify unique records in the final result table.
    - retained_fields: List of columns from `inTable` to be kept in the output DataFrame.
    - datefieldPrimary: (Character) Main field used for sorting. Can also accept custom sorting order
                        via a dictionary.
    - code: Identifier for naming and tagging calculated fields.
    - sort_fields: List of fields to be used for sorting. If not provided, `datefieldPrimary` is used.
    - indexLabel: Prefix label for the index calculated field.
    - lastLabel: Prefix label for the last calculated field.
    - histStart: Date of history start
    - histEnd: Date of history End
    - datefieldStop: The date field used to evaluate histEnd dates, such as in medication Stop Day, otherwise
      can by replicate of datefieldPrimary

    Returns:
    - cohort_index: Spark DataFrame with the index date and other calculated fields.
    """
    
    callFun = {
        'inTable': inTable,
        'index_field': index_field,  # Convert list to tuple
        'retained_fields': retained_fields,
        'datefieldPrimary': datefieldPrimary,
        'code': code,
        'sort_fields': sort_fields,
        'max_gap': max_gap,
        'indexLabel': indexLabel,
        'lastLabel': lastLabel,
        'histStart': histStart,
        'histEnd': histEnd,
        'datefieldStop': datefieldStop
    }
    logger.info(f"The function write_index_table is called with the parameters: \n")
    logger.info(pformat(callFun))
    logger.info("\n")
    
    
    
    
    # Check if datefieldPrimary is a list, and if so, take the first element
    # check if datefieldPrimary is a list and if so, take its first element
    datefieldPrimary = unique_non_none(datefieldPrimary, masterList=inTable.columns)[0]
    datefieldStop    = unique_non_none(datefieldStop, datefieldPrimary, masterList=inTable.columns )[0]
    sort_fields      = unique_non_none(sort_fields, datefieldPrimary, datefieldStop, masterList=inTable.columns)

    retained_fieldsEdit      = noColColide(retained_fields, [*index_field, datefieldPrimary], 
                                      index=[], masterList=inTable.columns)
    
    
    df = inTable.withColumn("unique_id", F.monotonically_increasing_id())
    
    # Create a concatenated column for all fields in index_field
    df = df.withColumn("combined_index", concat_ws("_", *index_field))
    
    if code is None:
        code = ""
    

    # Calculate distinct counts if sort_fields is provided
    
     
    distinct_dates  = df.select(*index_field, datefieldPrimary).distinct()
    distinct_counts = distinct_dates.groupBy(*index_field).agg(F.countDistinct(datefieldPrimary).alias('encounter_days'))
    distinct_counts.createOrReplaceTempView("distinct_counts_temp")
    distinct_counts = spark.table("distinct_counts_temp")

    if sort_fields is not None:
        # Add a distinct id field for  well defined sorting
        pass
    
    df = inTable.withColumn("unique_id", F.monotonically_increasing_id())
    
    logger.info(f" datefieldPrimary is {datefieldPrimary} and index_field is {index_field} and sort_fields is {sort_fields} and retained_fields is {retained_fields}")

    lastName         = lastLabel  + "_" + code
    indexName        = indexLabel + "_" + code
    lastTherapyName  = lastLabel  + "_therapy_" + code
    indexTherapyName = indexLabel + "_therapy_" + code
    
    logger.info(f"New Date fields {lastName} and {indexName} and {lastTherapyName} and {indexTherapyName}")
    
    # This histStart and histEnd are used to fiter out incorrect dates.  Use a different step to filter dates
    if histStart is not None:
        logger.info(f"histstart is {histStart} and datefieldPrimary is : {datefieldPrimary} \n")
        df = df.filter(F.col(datefieldPrimary) >= histStart).filter(F.col(datefieldPrimary).isNotNull())
    else:
        print(f"histStart is None \n")
        
    if histEnd is not None:
        print(f"histEnd is {histEnd} and datefieldPrimary is : {datefieldPrimary} \n")
        df = df.filter(F.col(datefieldPrimary) <= histEnd).filter(F.col(datefieldPrimary).isNotNull())
    else: print(f"histEnd is None \n")

    # look over all records for a perons
    windowSpec = (
        Window.partitionBy(index_field)
        .orderBy(F.asc_nulls_last(datefieldPrimary),F.asc_nulls_last(datefieldStop))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    
    windowSpecdatefieldStop = (
        Window.partitionBy(index_field)
        .orderBy(F.asc_nulls_last(datefieldPrimary))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    windowSpecLead           = Window.partitionBy(*index_field).orderBy(F.desc_nulls_last(datefieldPrimary),F.asc_nulls_last(datefieldStop))
    windowSpecDistinctDate   = Window.partitionBy(*index_field, datefieldPrimary).orderBy('unique_id')  
    # 
    windowSpecDistinctPerson = Window.partitionBy(*index_field).orderBy(F.col(datefieldPrimary).desc_nulls_last(), 'unique_id')
    
    windowSpec_therapy = (
        Window.partitionBy([*index_field,'course_of_therapy'])
        .orderBy(F.asc_nulls_last(datefieldPrimary) ,F.asc_nulls_last(datefieldStop))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    
    windowSpecDistinctPersonTherapy = Window.partitionBy(*index_field, 'course_of_therapy').orderBy(F.col(datefieldPrimary).asc_nulls_last(), F.asc_nulls_last(datefieldStop),'unique_id')
    
    cohort_index_1 = (
        df
        .select([*index_field, *retained_fieldsEdit, datefieldPrimary, 'unique_id'])
        # Make sure the stop data is the greatest of non missing stop and start dates.
        .withColumn(datefieldStop,F.greatest(F.coalesce(F.col(datefieldStop), F.col(datefieldPrimary)), F.col(datefieldPrimary)))
        .withColumn('rownum', F.row_number().over(windowSpecDistinctDate))
        #.sort([*index, *sort_fields])
        .withColumn(indexName, F.first(datefieldPrimary, ignorenulls=True).over(windowSpec))
        .withColumn(lastName,  F.last(datefieldStop ,    ignorenulls=True).over(windowSpec))
        .filter(F.col('rownum') == 1)
        .join(F.broadcast(distinct_counts), on=index_field, how='left') 
        .withColumn('next_date', F.lag(datefieldPrimary).over(windowSpecLead))
        .withColumn('days_to_next_encounter', F.datediff(F.col('next_date'), F.col(datefieldStop)))
        .withColumn('last_course_day', 
                F.when(F.col('days_to_next_encounter') <= max_gap, False)
                .when(F.col(datefieldStop) == F.col(lastName), False)
                .otherwise(True))
        .withColumn('course_of_therapy', F.sum(F.when(F.col('last_course_day'), 1).otherwise(0)).over(windowSpecLead))
        #.filter(F.col('keep') )
        #.withColumn('rownum', F.row_number().over(windowSpecDistinctPerson))
        #.filter(F.col('rownum') == 1)
        .sort(['personid', datefieldPrimary])
    )
    
    # one record per course of therapy
    cohort_index_2 = (
    cohort_index_1
    .withColumn(indexTherapyName, F.first(datefieldPrimary, ignorenulls=True).over(windowSpec_therapy))
    .withColumn(lastTherapyName, F.last(datefieldStop , ignorenulls=True).over(windowSpec_therapy))
    .withColumn('rownum_therapy', F.row_number().over(windowSpecDistinctPersonTherapy))
    .withColumn('max_gap', F.max(F.when(~F.col('last_course_day'), F.col('days_to_next_encounter'))).over(windowSpec_therapy))
    .filter(F.col('rownum_therapy') == 1)
    )
    
    

    # Calculate the distinct count for each group
    distinct_count_by_therapy = (
        cohort_index_2
        .groupBy(*index_field, 'course_of_therapy')
        .agg(F.countDistinct(datefieldPrimary).alias('encounter_days_for_course'))
    )
    
    
    # Join the distinct count back to the original DataFrame
    cohort_index_3 = (
        cohort_index_2
        .join(distinct_count_by_therapy, on=[*index_field, 'course_of_therapy'], how='left')
        .drop('unique_id')
        .drop('rownum')
        .drop('next_date')
        .drop('last_course_day')
        .drop('rownum_therapy')
        .sort([*index_field, datefieldPrimary])
    )

    return cohort_index_3


def identifyLevel(inTable, indexField, targetField, fieldLabel,
                  includedLevels, retainedFields = [],      sort_order = None):
    """
    Find a unique value of a field for each subject
    using write_index_table.  for the purpose of assiging a value to a field when there are conflicting duplicates
    """
    
    dfCount = (
        inTable
        .filter(F.col(targetField).isin(includedLevels ))
        .select([*indexField, targetField])
        .distinct()
        .groupby(targetField)
        .count()
        .withColumn('plurality',  -F.col('count'))
        .drop('count')
    )
    
    # Use only the levels suggested
    inTable0         = (
        inTable
        .filter(F.col(targetField).isin(includedLevels ))
        .join(dfCount, on = [targetField], how = 'inner')
    )
    
    result = write_index_table(
            inTable          =  inTable0,
            index_field      = indexField, 
            datefieldPrimary = 'plurality',
            code             = 'count',
            retained_fields  = [targetField],
            sort_order       = sort_order
            
    )
    return(result)

def select_elements_for_encounter_id(elementSource, encounterId, encounterIndex, encounterIdColumns):
    """
    Give the opportunity to limit the encounter source table to a selected cohort
    Does nothing if `encounterId` is not a `DataFrame`
    """
    if isinstance(encounterId, DataFrame):
        display(Markdown(f"Called with a study specific set of encounters used to narrow search"))
        elementSourceSelect = (
            elementSource
            .join(encounterId.select(encounterIdColumns), on = encounterIndex, how = 'inner')
        )
    else:
        elementSourceSelect = elementSource
        encounterIdColumns = []
    return elementSourceSelect, encounterIdColumns

def entity2Elements(cohortIndex, 
                          index, retained_fields, disease, histStart, histStop, elementSource, 
                          elementColumns, datefieldEntity, datefieldElement, outPersonEncounterTable):
    """
    Extract selected records from the Encounters Table for a list of people
    @param cohortIndex (data table): A table identifying the cohort. This table has `index` for SQL joining and possibly a date field
    @param index (list): A list of fields used to SQL join the cohortIndex to the elementSource
    @param disease (string): A postix string for the current disease.  does not affect processing or output
    @param startDate (string): Only elements records on or after this date will be considered
    @param elementSource (data table): A table with the source encounter records
    @param encounterColumns (list):  A list of the columns to be extracted from the encouter table
    @param datefieldEntity (string): the name of the date field in the `cohortIndex` table.
    @param datefieldElement (string): the name of the date field in the `elementSource` table.
    @param outPersonEncounterTable (string): Location where this table will be written
    """

    # A list of all the entities
    cohortIndex.select([*index, *retained_fields]).distinct().createOrReplaceTempView(f"personId_{disease}")
    person = spark.table(f"personId_{disease}")
    
    # create table of elements for  all entities
    personEncounter = (
        F.broadcast(person)
        .join(elementSource.select(elementColumns), on = index, how = 'inner')
        .filter(F.col(datefieldEntity).between(histStart, histStop))
    )
    
    (personEncounter
     .distinct()
     .write
     .saveAsTable(outPersonEncounterTable, mode = 'overwrite')
    )
    
    personEncounter = spark.table(outPersonEncounterTable)
    
    return(personEncounter)
        
#def createSchemaFromPd(tablePd, tableDF):
    #schema = conditionSource.select([item.strip() for item in conditionListPd.columns if item in conditionSource.columns]).schema 
    
# dateField
    
def select_by_entity(entity_table, 
                     element_table, 
                     entityIndex, 
                     elementIndex             = [],
                     listIndex                = [],
                     value_column             = 'entityGroup',
                     element_date_field_col   = 'servicedate',
                     entity_date_field_col    = None,
                     retained_entity_fields   = [],
                     retained_element_fields  = [],
                     hist_start               = None, 
                     hist_stop                =None,
                     elementList              = None,
                     elementListSource        = None,
                     outElementListTBL        = None,
                     outEntityBaseTBL         = None,
                     outEntityTBL             = "",
                     primaryDisplay           = ['conditioncode_standard_primaryDisplay'] ,
                     entityDescription        = '',
                     debug                    = False,
                     test                     = False,
                     useMemory                = True,
                     obs                      = 0):
    """
    Add medical information to a cohort.
    entity                    : Cohort identification, possibly the initial identification indexed by personid and unique by person
    element                   : medical history such as medication, labs or diagnosis.
    entity_index              : The unique identifier of each entity record ('personid')
    value_column              : The name of a column to be added that will track the value of elementList if it is a list
    element_date_field_col    : The date field used to subset records by hist_start and hist_stop
    retained_entity_fields    : Fields in the entity_table to retain
    retained_element_fields   : fields in the element table to retain
    entity_date_field_col     : A date field in the entity table typically used as a relative date anchor
    hist_start                : The start of the element history, could be a fixed date or relative to entity_date_field
    hist_stop                 : The end of the element history, could be a fixed date for relative to entity_date_field
    elementList               : A list, string, or table subsetting the element_table
    elementListSource         : A master table of all levels and the element index used to join to the element table
    primaryDisplay            : The field where the searc of items form elementList is searched in elementListSort.
    elementIndex              : The index for the element_table
    Logic:
      - The result will be the element table subset 
         - to the entity index of entity table
         - The elementList of primaryDisplay
         - The hist_start and hist_end
      1. Identify the levels of elementIndex  (which would include entityIndex )
      2. Identify levels of elements to be used, convert to element index
      3. join the entity with the element table by enity index keeping only the indexes, primaryDisplay, element_date_field_col, entity_date_field_col
      4. join the result by element index.  
      5. Subset to the history dates
      6. Save the result.
      7. Remerge back into the entity index to get other requested fields
      8. Merge result back into the element table to get requested fields
    """
    
    #
    
       
    print(f"Top of function   : entity_index is \n {entityIndex}, entity_date_field_col is \n {entity_date_field_col} from \n {entity_table.columns}\n")
    entity_index_copy         = entityIndex.copy()
    element_index_copy        = elementIndex.copy()
    list_index_copy           = listIndex.copy()
    element_cols_possible     = entity_index_copy + element_index_copy + [element_date_field_col] + [primaryDisplay] + list_index_copy
    entity_col_possible       = entity_index_copy + [entity_date_field_col] + list_index_copy
    
    # noColColide(columns, colideColumns, index, masterList=None)
    element_cols              = noColColide(columns = element_cols_possible, colideColumns=[], index = entity_index_copy, masterList = element_table.columns)
                                            
    entity_cols               = noColColide(columns = entity_col_possible, colideColumns = element_cols, index = entity_index_copy, masterList = entity_table.columns)
   
    
    # Identify the levels of the elements to use in the extract
   
    elementListExtract = elementList2TBL(elementList, elementListSource,
                                         outElementListTBL,
                                         primaryDisplay, 
                                         elementIndex    = list_index_copy,
                                         retained_fields = elementListSource.columns,
                                         value_column    = value_column,
                                         debug           = debug)
    
    elementListJ_cols         = noColColide(columns= element_cols + entity_cols, colideColumns = elementListExtract.columns,
                                            index = listIndex)
    # This should be the smallest possible table with needed index elements to identify records in the entity_table or element_table
    joined_table = (
        entity_table
        .select(entity_cols)
        .join(element_table.select(element_cols), on = entityIndex, how = 'inner')
        .select(elementListJ_cols)
        .join(elementListExtract, on = listIndex, how = 'inner')
    )
    print(f"The joined Base table has columns {joined_table.columns}\n")
    
        # Calculate the date range for selection
    if element_date_field_col is None:
        # No date filtering, select all elements
        selection_table = joined_table
    else:
        if entity_date_field_col is None:
            # Fixed date range
            date_range_cond = (F.col(element_date_field_col) >= hist_start) & (F.col(element_date_field_col) <= hist_stop)
        else:
            # Date range relative to entity date
            date_diff_cond = F.datediff(F.col(entity_date_field_col), F.col(element_date_field_col)) <= hist_start
            date_range_cond = (F.datediff(F.col(element_date_field_col), F.col(entity_date_field_col)) <= hist_stop) & date_diff_cond

        # Apply the date range condition and select the appropriate element record for each entity
        print(f"Filtering by date using {date_range_cond}\n")
        selection_table = joined_table.filter(date_range_cond)
        
    # Save selection_table to the schema {projectSchema} as a intermediate result
    print(f"The Base Table columns are {selection_table.columns}\n")
    writeTable(selection_table, outEntityBaseTBL, description = f"index for table")
    spark.sql(f"refresh table {outEntityBaseTBL}")
    entityBase = spark.table(outEntityBaseTBL)
    
    # Are any of the requested retained_entity_fields not already in the Base table?
    entityAdded_cols =                   noColColide(
                                           columns          = retained_entity_fields,
                                           colideColumns    = entityBase.columns,
                                           index            = entityIndex,
                                           masterList       = entity_table.columns)
    
    # Check to see if there are more colums needed from the entity table
    diff_list_entity = list(set(entityAdded_cols) - set(entityIndex))
    if set(entityAdded_cols) != set(entityIndex):
        print(f"add to Base the entityAdded_cols columns {entityAdded_cols}\n")
        entityListWRetained = (
            entity_table
            .select(entityAdded_cols)
            .join(entityBase, on = entityIndex)
        )
        if useMemory:
            entityListWRetained.persist()  # Persist in memory or
        else:
            entityListWRetained.persist(StorageLevel.DISK_ONLY)  # Persist on disk

        #entityListWRetained.write.mode('overwrite').save('/path/to/output/directory')
        
        print(f" Just added entity retained fields {diff_list_entity}\n")
        print(f" The table entityListWRetained has {entityListWRetained.count()} observations\n")

    else:
         entityListWRetained = entityBase
         print(f"no added retained colums for the entity table")
         
    # Get a list of added columns that will not duplicate those already in entityListWRetained
    elementAdded_cols =                  noColColide(
                                            columns         = element_cols_possible + retained_element_fields,
                                            colideColumns   = entityListWRetained.columns,
                                            index           = elementIndex, 
                                            masterList      = element_table.columns)
    diff_element_list = list(set(elementAdded_cols) - set(elementIndex))
    if elementAdded_cols != elementIndex:
        print(f"adding elementAdded_cols {elementAdded_cols}\n")

        result = (
            element_table
            .select(elementAdded_cols)
            .join(entityListWRetained, on = elementIndex, how = 'inner')  
            )
        
        print(f"Just added the element retained colums {diff_element_list} to entityListWRetained")
    else:
        print(f"no new retained colums from the element table are used. {elementAdded_cols}")
        result = entityListWRetained


    print(f"columns of result joined Table {result.columns}\n")
    # Create a written table of the resulting extract if a name is specified

    if outEntityTBL != '':
        if entityDescription == '':
            entityDescription = outEntityTBL
        if diff_list_entity and not diff_element_list:   # No new columns
            print(f"The final Entity table is the same as the base table, diff entity {diff_list_entity}, diff_element {diff_element_list} \n")
            writeTable(result, outEntityTBL, description = entityDescription)
        else:
            print(f"The final Entity table add columns to the base table, diff entity {diff_list_entity}, diff_element {diff_element_list} \n")
            
            writeTable(result, outEntityTBL, description = entityDescription)
            
        spark.sql(f"REFRESH {outEntityTBL}")
        result = spark.table(outEntityTBL)


    return result


def elementList2TBL(elementList, elementListSource, outElementListTBL, 
                    primaryDisplay  = 'conditioncode_standard_primaryDisplay',
                    elementIndex    = ['conditioncode_standard_id', 'conditioncode_standard_codingSystemId'],
                    listIndex       = None,
                    retained_fields = [],
                    value_column    = "group",
                    countfield      = 'Subjects',
                    debug           = False):
    """
    Given a list Of strings that serve a search terms and a master list of terms, return a pyspark Data Frame of index values
    @param elementList: A list of elements, such as medications.
    @param elementListSource: A master list of these elements
    @param outElementListTBL: An output table of the items found in the master list
                              Do not output if outElementListTBL is None
    @param primaryDisplay: The field to be searched.
    """
    
    pattern = None
    if type(elementList) == list:
        print("elementList is a list")
        if len(elementList) > 0:
            # Replace '.x' with wildcard character '.*'
            pattern = "|".join(['[a-z]*' + re.sub(r'\.x', '.*', name) + '[a-z]*' for name in elementList])
    elif type(elementList) == str:
        print("elementList is a string")
        # Replace '.x' with wildcard character '.*'
        pattern = re.sub(r'\.x', '.*', elementList)
        print(f" The rex pattern is {pattern}")
    elif isinstance(elementList, DataFrame):
        if listIndex is None:
            # Nothing to do in this case
            print("elementList is a DataFrame")
        return(elementList)
    elif elementList is None:
        print(f"Called with elment list of none, returning `None`")
        return(elementList)
    else:
        print(f"Called with an unknown type of element list")
        return()


    # Identify the fields to be retained in the output table
    retained_fields_edited = distCol(elementIndex + retained_fields + [countfield] + [value_column], masterList = elementListSource.columns)
    if debug == True:
        print(f"retaining {retained_fields_edited}")
        print(f"The field to search is {primaryDisplay}")
        
    
    if pattern is not None:
        result = (
            elementListSource
            .filter(F.col(primaryDisplay).rlike(f'(?i){pattern}'))
            .select(retained_fields_edited)
            .sort(F.col(countfield).desc())
            .distinct()
        )
    
    if type(elementList) == list:
        print("addind parsed Column")
        result = (
            add_parsed_column(result,primaryDisplay=primaryDisplay,  elementList=elementList, value_column = value_column)
            .sort(F. col(countfield).desc())
        )
    
    if isinstance(elementList, DataFrame):
        # First, search through the elementListSource field primaryDisplay using the elementList field listIndex
        #  - Select all records of elementListSource that match and keep both the elementListSource fields and elementList fields
        # Need editing
        result = (
            elementListSource
            .merge(elementList, on = elementIndex, how = 'inner' )
        )

    if outElementListTBL is not None and outElementListTBL != '':
        print(f"Saving to {outElementListTBL}")
        (
            
            result
            .distinct()
            .write
            .saveAsTable(outElementListTBL, mode = 'overwrite')
        )
        
        if spark.table(outElementListTBL).count() > 0:
            spark.sql(f"REFRESH TABLE {outElementListTBL}")
            elementListExtract = spark.table(outElementListTBL)
            print(f"save list of elements to {outElementListTBL}")
            #print(f"There are {elementListExtract.count()} elements in the list")
           

    
    
    return(elementListExtract)

def applyTimeBoundary(personEncounter, indexEncounter,  outTBL, index,
                      baselineAnchor, baselineEvent, baselineDays, timeToBaselineName, 
                      followAnchor, followEvent, followdays, followTimeName , retained_fields = [] ):
    
    """
    Calculate the number of days before the study start date of an event in the baseline
    and the number of days into followup of a followup event
    Limit encounters by number of days of these to events
    @param personEncounters:  Encounters where events occur
    @indexEncounters: The reference baseline and follow encounters
    @indexCol: The columns to be retained from indexEncounters.
    @outTBL: Name of output table
    @index: the list of fields used for the join: ["personid"]
    @param baselineAnchor: Generally the index event defining a cohort in (indexEncounters)
    @param baselineEvent: An event that is tracked as occuring in the baseline period before bawselineAnchor (personEncounters)
    @baselineDays: The maximum number of days from baselineEvent to baselineAnchor allowed for encounter inclusion
    @timeToBaselineName: Name of the field that will record the number of days from baselineEvent to baselineAnchor
    @param followAnchor: A date used to measure start of followup.
    @param followEvent: An event that is tracked as occuring in the followup period after followAnchor
    @followDays: The maximum number of days from followAnchor to followEvent allowed for encounter inclusion
    @followTimeName: Name of the field that will record the number of days from followAnchor to followEvent
    """
    
    
    (
        
        personEncounter
        .join(F.broadcast(indexEncounter.select(distCol([*index, baselineAnchor, followAnchor])))
              , on = index, how = 'inner')
        .withColumn(timeToBaselineName, F.datediff(baselineAnchor, baselineEvent ))
        .withColumn(followTimeName,     F.datediff(followEvent , followAnchor  ))
        .filter(F.col(timeToBaselineName) <= baselineDays) 
        .filter(F.col(followTimeName)     <= followdays )
        .distinct()
        .write.mode("overwrite").saveAsTable(outTBL) 
    )
    
def findConditionsByEncounter(conditionsSource
                              , encountersCohort
                              , comorbCodes
                              , conditionsColumns = [
                                  'personid', 'encounterid', 'effectiveDate', 
                                  F.col('conditionCode.standard.id'            ).alias('conditionCode_standard_id'),
                                  F.col('conditionCode.standard.primaryDisplay').alias('conditionCode_standard_primaryDisplay')
                              ]
                              , encountersColumns = [
                                  'personid', 'encounterId', 'serviceDate', 'dischargedate'
                              ]
                             ):
    
    """
    Identify records in the conditions table that match encounters of the disease chort encounters
    @par conditionsSource  Table with all observed conditions.  Matches with comorbCodes
    @par encountersCohort  The encounters associated with the cohort that are to be scanned (from the encounters table)
    @par comorbCodes       comorbitity classifications linked to conditionCoded_standard_id that classify conditions
    @par conditionsColumns The columns to be use in conditionssSource.  Must include unlisting
    @par encountersColumns The columns in the cohort encounters.  
    """

    conditionComorbEncounters = (
        conditionsSource
        .select(conditionsColumns)
        .filter(F.col('encounterid').isNotNull())
        .join(encountersCohort.select(encountersColumns), 
              on = ['personid', 'encounterId'], how = 'inner')
        .join(comorbCodes,
              on = ['conditionCode_standard_id'])
        .distinct()
    )
    
    return(conditionComorbEncounters)


    





# Constants for join types




def identify_target_records(entitySource, elementIndex, elementExtract,
                            datefieldSource, histStart=None, histStop=None,
                            datefieldElement=None, cacheResult=True, broadcast_flag=True, masterList=None,
                            howjoin=JOIN_INNER):
    """
    Identifies and extracts records(entities) from a source table (entitySource) based on an index (elementIndex) and corresponding levels in another table.
    Also allows for date filtering of the source table.

    Parameters:
    - entitySource (DataFrame): The source table where records are identified.
    - elementIndex (list): The index used to join entitySource and elementExtract.
    - elementExtract (DataFrame): The table whose indexes identify records in entitySource to be extracted.
    - datefieldSource (str): The date field of entitySource. If filtering by date is not used, set to None.
    - histStart (str or int, optional): The start date or an offset from datefieldElement. Defaults to None.
    - histStop (str or int, optional): The stop date or an offset from datefieldElement. Defaults to None.
    - datefieldElement (str, optional): Field name in elementSource for calculating histStart and histStop. Defaults to None.
    - cacheResult (bool, optional): Whether to cache the result. Defaults to True.
    - broadcast_flag (bool, optional): Whether to broadcast the index table. Defaults to True.
    - masterList (list, optional): A list of column names to limit the final dataset. Defaults to None.
    - howjoin (str, optional): Join type. Defaults to 'inner'.

    Returns:
    - DataFrame: A DataFrame containing the identified target records.
    """
    
    if not isinstance(entitySource, DataFrame):
        raise ValueError("entitySource must be a DataFrame")
    if not isinstance(elementExtract, DataFrame):
        logger.info("Skipping processing as elementExtract is not a DataFrame")
        return elementExtract
    
    
        
    
    callFunc = {
        'masterColumns': entitySource.columns,
        'colideColumns': elementExtract.columns,
        'index': elementIndex,
        'masterList': masterList
        
    }
    
    logger.info(f" identify_target_records {pformat(callFunc)}")
    
    
    
    logger.info(f"entitySourceSelect = noColColide({entitySource.columns}, {elementExtract.columns}, {elementIndex}, masterList={masterList})\n")
    entitySourceSelect   = noColColide(entitySource.columns, elementExtract.columns, elementIndex, masterList=masterList)
    logger.info(f"elementExtractSelect = noColColide({elementExtract.columns}, {entitySourceSelect}, {elementIndex}, masterList={masterList})\n")
    elementExtractSelect = noColColide(elementExtract.columns, entitySourceSelect, elementIndex, masterList=masterList)
    
    
    
    # Prepare the index table based on the flag
    logger.info(f"indextable = elementExtract.select({elementExtractSelect}).distinct() \n")
    indextable = elementExtract.select(elementExtractSelect).distinct()
    if broadcast_flag:
        logger.info(f"indextable = F.broadcast(indextable) \n")
        indextable = F.broadcast(indextable)
    
    # Perform the join operation
    logger.info(f"elements = entitySource.select({entitySourceSelect}).join(indextable, on={elementIndex}, how='{howjoin}')") 
    elements = entitySource.select(entitySourceSelect).join(indextable, on=elementIndex, how=howjoin)  
    
    # Make sure the joined DataFrame has records
    if elements.limit(1).count() == 0:
        logger.warning("Joined DataFrame has no records. Exiting.")
        return None 
    
    
    # if the entitySource table has a date field and it is desired to use that data as time anchor:
    # For example if elementIndex contains the first date of a diabetes diagnosis
    # and elementSource is all medications,
    #  and we want to extract all medication for the cohort within 2 years after and 1 year before the index date
    #  We can set set histStart to -1 and histEnd to 2.
    
    if datefieldElement is not None and histStart is not None and histStop is not None:
        if isinstance(histStart, int):
            logger.info(f"F.date_add(F.col({datefieldElement}), {histStart})")
            histStart = F.date_add(F.col(datefieldElement), histStart)
        else:
            logger.info(f"histStart is not and offset from {datefieldElement} but is {histStart}")
        if  isinstance(histStop, int):
            logger.info(f"F.date_add(F.col({datefieldElement}), {histStop})")
            histStop = F.date_add(F.col(datefieldElement), histStop)
        else:
            logger.info(f"histStop is not and offset form {datefieldElement} but is {histStop}")
    else:
        logger.info("In cohbort.py: history was Not an offset from datefield element")
        histStart = None
        histStop = None


    if datefieldElement is not None and histStart is not None and histStop is not None:
        if isinstance(histStart, int) and isinstance(histStop, int):
            logger.info(f"F.date_add(F.col({datefieldElement}), {histStart})")
            histStart = F.date_add(F.col(datefieldElement), histStart)
            logger.info(f"F.date_add(F.col({datefieldElement}), {histStop})")
            histStop = F.date_add(F.col(datefieldElement), histStop)
        else:
            logger.info("histStart and histStop are not integers representing number of days as offsets. No offset will be used.")
    
  
    # The Source data table might have a date and if it is desired to filter by date, for example
    # Only records between histStart = 1990 and histStop = 2024
    # or the time anchors described above
    # if histStop is None but histStart is not, then all records after histStart up to today are included
    if datefieldSource is not None and histStart is not None and histStop is not None and histStart < histStop:
        logger.info(f"elements = elements.filter(F.col({datefieldSource}).between({histStart}, {histStop})")
        elements = elements.filter(F.col(datefieldSource).between(histStart, histStop))
    
    if cacheResult:
        logger.info(f"elements.cache()")
        elements.cache()
    
    logger.info(f"identify_target returned columns: {elements.columns}")
    return elements
# Example: labelEntity(cohort = e.timeToReadmission.df, fieldName = 'stroke', entity = e.strokeConditioncodeVerified.df, elements = r.conditionSource,
#                      entityIndex = ['conditioncode_standard_id', 'conditioncode_standard_codingSystemId','conditioncode_standard_primaryDisplay'],
#                       elementIndex = ['personid', 'tenant'])



# Function to prepare DataFrame
def prepare_dataframe(df, indexFields, fieldValue, group='group'):
    """
    Prepare the DataFrame by creating or renaming the 'group' column based on fieldValue.
    
    Args:
    df (DataFrame): The Spark DataFrame to process.
    indexFields (list of str): The list of fields to keep in the DataFrame.
    fieldValue (str): The field name or a constant value for the 'group' column.
    
    Returns:
    DataFrame: The transformed DataFrame.
    """
    # Check if fieldValue is a column in the DataFrame
    if fieldValue in df.columns:
        # Rename the column to 'group'
        df = df.withColumn(group, F.col(fieldValue))
    else:
        # Create a new column 'group' and assign all rows the value of fieldValue
        df = df.withColumn(group, F.lit(fieldValue))
    
    # Select the required fields plus the now created or renamed 'group' column
    return df.select(indexFields + [group])

