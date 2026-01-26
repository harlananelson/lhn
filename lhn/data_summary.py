from lhn.header import display, Markdown, datetime, spark, F, Any, List, Window
from lhn.data_display import noRowNum
from lhn.introspection_utils import deduplicate_fields, fields_reconcile,  translate_index
from lhn.data_transformation import flatSchema, flatten_schema, get_selected_fields
from lhn.spark_utils import explode_columns
from lhn.query import extract_fields_flat_top, query_table
from lhn.header import get_logger

logger = get_logger(__name__)


def count_people(df, description, person_id='personid', label2=''):
    """
    Count the number of unique people in a dataframe, and display the result along with an optional label
    @param df: The dataframe to be counted
    @param description: A description for the table being counted
    @param person_id: The name of the column that contains person identifiers
    @param label2: An additional label to be displayed
    """
    count_p = df.select(person_id).distinct().count()
    display(Markdown(f"* {count_p:,}: Unique by {person_id} from Table {description}"))
    display(Markdown(label2))

def attrition(df, table_name, person_id=['personid'], description='', date_field=None):
    """
    Compute attrition statistics for a PySpark dataframe and display the results
    @param df: The PySpark dataframe to compute attrition statistics for
    @param table_name: The name of the table being processed
    @param person_id: The name of the column that contains person identifiers
    @param description: A description for the table being computed
    @param date_field: An optional field that contains dates for time-based attrition analysis
    """
    count = df.count()
    display(Markdown(f"**{description}**"))
    display(Markdown(f"* {count:,} records"))
    if person_id in df.columns:
        count_people(df = df, description = table_name, person_id = person_id)
    if date_field:
        if isinstance(date_field, str):
            date_stats = df.select(F.min(date_field), F.max(date_field)).toPandas()
            display(Markdown(f"* Date range: {date_stats.iloc[0][0]} - {date_stats.iloc[0][1]}"))
        elif isinstance(date_field, list):
            date_stats = df.select([F.min(field) for field in date_field] + [F.max(field) for field in date_field]).toPandas()
            display(Markdown(f"* Date range: {date_stats.iloc[0][0]} - {date_stats.iloc[0][1]}"))
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def groupCount(df, field, id = 'personid'):
    #pandas
    result = df.groupby(field)[id].nunique().sort_values(ascending=False).reset_index()
    return(result)



def countDistinct(tbl, field, index):
    result = (
    tbl
    .groupBy(field)
    .agg(F.countDistinct(index).alias('distinct_count'))
    .sort(F.col('distinct_count').desc())
    .toPandas()
    .style
    .hide_index()
    )
    return(result)

def topAttributes(inTable, inSchema, cohortTable, cohortSchema, startDate, stopDate
                     , dataLoc
                     , outfile      = False
                     , index        = 'personid'
                     , dateField = 'effectiveDate'
                     , sourceFields = ['personid', 'encounterid' ,'conditioncode']
                     , flatFields   = ['personid', 'encounterid', 'conditioncode_standard_id', 'conditioncode_standard_primaryDisplay']
                     , notNullField = 'encounterid'
                     , obs = 1000
                     ):
    """
    Given a cohort list of person IDs, return a tabulation of the top conditions over a time period
    - Get index from Cohort Table
    - Select from inTable according to the index
    - Count index levels by sourceFields
    - Write to spark data table
    - Write to csv
    """
    
    # Point to the Cohort with only the distinct index
    
    if not (cohortSchema is None) or len(cohortSchema) > 0:  # A Schema is being used
        cohortLoc = f'{cohortSchema}.{cohortTable}'
    else:
        cohortLoc = cohortTable
        
    cohort = (
        spark.table(f"""{cohortLoc}""")
        .select(index)
        .distinct()
    )
    
    # Point to the source Health Records Table
    
    if len(inSchema) >0:
        sourceDBLoc = f'{inSchema}.{inTable}'
    else:
        sourceDBLoc = inTable
        
    # Extract just the cohort records form the Health Data with the selected fields
    sourceDB = (
        spark.table(f"""{sourceDBLoc}""")
        .filter(F.col(dateField).between(startDate, stopDate))
        .select(sourceFields)
        .join(cohort, on = index, how = 'inner')
    )
    
    # flatten
    fields1 = [F.col(field).alias(field.replace(".","_")) for field in flatSchema(sourceDB)]
    
    flatSourceDB = (
        sourceDB
        .select(fields1)
        .select(flatFields)
        .filter(F.col(notNullField).isNotNull())
    )
  
    # Aggregate the results
    
    attribute_counts = extract_fields_flat_top(
          flatSourceDB
        , flatFields
        , concepts       = [] 
        , count          = True 
        , byIndex        = True 
        , index          = index
        , dedup_index    = index
        , dedup_fields   = index
        , last           = False
        , last_fields    = index
        , dedup          = False
        , limit          = True 
        , obs            = obs
        , toPandas       = False
        , countfield     = "Unique"
        , explode_fields = []
        , datefields     = []
        )
    
    if isinstance(outfile, str):
        print(f"Writing outfile {cohortSchema}.{outfile}")
        (
            attribute_counts
            .write.mode("overwrite")
            .saveAsTable(f'{cohortSchema}.{outfile}')
        )
        print(f"Writing csv outfile {dataLoc + outfile + '.csv'}")
        (
            attribute_counts
            .toPandas()
            .to_csv(dataLoc + outfile + '.csv', index = False)
        )
        
        attribute_counts = (
            spark.table(f"{cohortSchema}.{outfile}")
        )
         
    return(attribute_counts)



def aggregate_fields(df, index, fields, values, aggfuncs, aggfunc_names=None, debug=False):
    """
    Aggregates the specified fields in a pandas DataFrame using the specified aggregation functions.

    Args:
        df: The pandas DataFrame to aggregate.
        index: The name(s) of the column(s) to use as the index(es) when counting distinct units.
        fields: The name(s) of the column(s) to aggregate.
        values: The name(s) of the column(s) to use as the values for aggregation.
        aggfuncs: The aggregation functions to apply to the field(s).
        aggfunc_names: The name(s) to use for each aggregation function (defaults to the function's __name__ attribute).
        debug: If True, prints information about the aggregation being performed.

    Returns:
        A new pandas DataFrame containing the aggregated values, with the index columns, aggregated field columns, and
        specified aggregation function names as column headers.
    """
    if aggfunc_names is None:
        aggfunc_names = [f.__name__ for f in aggfuncs]

    # Create a set of the elements in index and values
    excluded_elements = set(index + values)

    # Get all columns from the dataframe, and select only the ones that are not in index or values
    all_columns = df.columns
    groupby_fields = [x for x in all_columns if x not in excluded_elements]

    agg_exprs = [f(F.col(value)).alias(f"{value}_{name}") for f, name, value in zip(aggfuncs, aggfunc_names, values)]

    # Add distinct count of index to aggregation expressions
    agg_exprs.append(F.countDistinct(F.col(index[0])).alias(f"{index[0]}_distinct_count"))

    if debug:
        print(f"Aggregating {values} by {groupby_fields} with {aggfunc_names} and counting {groupby_fields} distinct values by {index[0]}")

    # Get the aliases of the aggregation expressions
    agg_aliases = [expr.name for expr in agg_exprs]

    # Combine the groupby_fields and aliases into a single list for selection
    select_columns = groupby_fields + agg_aliases

    return df.groupby(groupby_fields).agg(*agg_exprs).select(*select_columns)






def aggregate_fields_count(df, index, values, debug=False):
    """
    Aggregates the specified fields in a PySpark DataFrame by counting distinct combinations.

    Args:
        df: The PySpark DataFrame to aggregate.
        index: The name(s) of the column(s) to use as the index(es) when counting distinct units.
        values: Values columns to be removed before counting
        debug: If True, prints information about the aggregation being performed.

    Returns:
        A new PySpark DataFrame containing the distinct counts of unique combinations of the fields that are not index or values.
    """

    # Create a set of the elements in index and values
    excluded_elements = set(index + values)

    # Get all columns from the dataframe, and select only the ones that are not in index or values
    all_columns = df.columns
    groupby_fields = [x for x in all_columns if x not in excluded_elements]

    if debug:
        print(f"Counting distinct combinations of {groupby_fields} by {index[0]}")

    # Count distinct combinations of groupby_fields by index[0]
    result = df.groupby(groupby_fields).agg(F.countDistinct(F.col(index[0])).alias(f"{index[0]}_distinct_count"))

    return result


def aggregate_columns(grouping_fields, value_columns, functions):
    def inner(df):
        for value_column in value_columns:
            for function_name in functions:
                # Get the actual function from the F module
                function = getattr(F, function_name)
                
                # Apply the function to the value_column and create a new column name
                new_column_name = f"{value_column}_{function_name}"
                df = df.withColumn(new_column_name, function(F.col(value_column)))

        # Group by the grouping_fields and aggregate the new columns
        agg_columns = [f"{value_column}_{function_name}" for value_column in value_columns for function_name in functions]
        df = df.groupBy(grouping_fields).agg(*[F.first(F.col(col)).alias(col) for col in agg_columns])

        return df

    return inner

# Instantiate the inner function with the desired configuration
# process_df = apply_functions_to_value_columns(grouping_fields, value_columns, functions)

# Apply the inner function to a DataFrame
# result_df = process_df(input_df)

def getTopValues(inTable, field, cohort, sourceSchema, outSchema, explode_fields = [], explodePre = False):
    codesDF = (
        spark.sql(f" SELECT * FROM {sourceSchema}.{inTable}")
        .select(['personid',field])
        .join(cohort.select('personid'), on = ['personid'], how = 'inner')
    )
    
    if explodePre:
        print("Exploding Before Extracting")
        codes = codesDF.withColumn(field, F.explode_outer(field))
    else:
        codes = codesDF
        
    
    print(codes.columns)
    
    codesCount = extract_fields_flat_top(codes, 1, index = ['personid'],
                                         obs = 10000, countfield     = "uniqueSubjects",
                                         explode_fields = explode_fields, 
                                         toPandas=False)
    
    print(f"writing to " + outSchema + "." + inTable + "_" +  field + "Count")
    (codesCount
     .write.mode("overwrite").saveAsTable(outSchema + "." + inTable + "_" +  field + "Count") 
    )
    
    result = spark.sql(f"SELECT * FROM {outSchema}.{inTable}_{field}Count")
    return(result.toPandas())

def topRecords(table, i
               ,  concepts     = []
               , index         = ['empiPersonMetadata_empiPersonId']
               , last          = False
               , last_fields   = []
               , dedup_index   = ["empiPersonMetadata_empiPersonId"]
               , dedup_fields  = ['races.raw.display','races.standard.primaryDisplay'] 
               , byIndex = True, dedup = False, obs = 10, toPandas = True, limit = True):
    
    index           = translate_index(index,table, byIndex = True)

    selected_fields = [*get_selected_fields(i, table, concepts = []), *index]
    
    selected_fields = fields_reconcile(selected_fields, table, delim = '_')
    concepts        = fields_reconcile(concepts,     table, delim = '_')
    dedup_index     = fields_reconcile(dedup_fields, table, delim = '_')
    dedup_fields    = fields_reconcile(dedup_fields, table, delim = '_')
    last_fields     = fields_reconcile(last_fields,  table, delim = '_')
    
    selected_fields_u = [field.replace(".","_") for field in selected_fields]
    
    index_u           = [field.replace(".","_") for field in index]
    
    windowSpec = (Window.partitionBy(index_u)
            #  .orderBy(['source_version'])
            #  .rowsBetween(Window.unboundedPreceding, .unboundedFollowing)
             )
    results_flat_all  = query_table(table, selected_fields
                                    , concepts      = concepts 
                                    , dedup         = dedup 
                                    , last          = last 
                                    , last_fields   = last_fields
                                    , dedup_fields  = dedup_fields  )
    
    
    result = (results_flat_all
     .distinct()
     .withColumn('valuesPerPerson', F.count(index_u[0]).over(windowSpec))
     .sort('valuesPerPerson', ascending=False)
     .select(['valuesPerPerson', *selected_fields_u])
     .distinct()
    )
    
    if limit:
        result_limited = result.limit(obs)
    else:
        result_limited = result
        
    if toPandas:
        result2 = result_limited.toPandas()
    else:
        result2 = result_limited
        
    return(result2)
    
def getSelectedLevels(
    df, source_field, index = "personid",
    removeRexEx = '(?i)unknown$|Undifferentiated|Other|unspecified$|Ambiguous|Indeterminate|Not applicable|None|Other|Unknown'
):
    
    selected_fields = (
        df
        .select(index, source_field)
        .distinct()
        .groupBy(source_field)
        .count()
        .filter(~F.col(source_field).rlike(removeRexEx) & F.col(source_field).isNotNull())
        .sort(F.col('count').desc())
        .toPandas()
    )
    
    return(selected_fields)

def count_and_pivot(df, id_cols, names_from, index, sort_field = '', obs=10):
    """
    Count the distinct levels of `index` by `id_cols` and `names_from`
    Then pivot so the values of `names_from` are now columns names 
    and the values of id_cols are the rows and the values of the table are the distinct counts 
    """

    # group by drugcode and age_category and count
    counts = (df.select([*index, *id_cols, names_from])
                 .distinct()
                 .groupBy(*id_cols, names_from)
                 .agg(F.count('*').alias('count')))

    # Get the unique values of `names_from` column
    columns = df.select(names_from).distinct().rdd.flatMap(lambda x: x).collect()

    # pivot the table
    pivot_counts = counts.groupBy(id_cols).pivot(names_from, columns).sum('count')

    # sort by count column in descending order
    if sort_field != '':
        if sort_field not in pivot_counts.columns:
            sort_field = pivot_counts.columns[0]
        pivot_counts = pivot_counts.sort(F.col(sort_field).desc())

    # show table
    result = pivot_counts.limit(obs).toPandas()

    return result


