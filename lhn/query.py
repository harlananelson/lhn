from pandas import DataFrame
from lhn.header import F, Any, List, spark, pd, display, Markdown, Window, DataFrame

from lhn.data_display import noRowNum

from lhn.introspection_utils import deduplicate_fields, fields_reconcile, translate_index

from lhn.data_transformation import flatten_schema, get_selected_fields


from lhn.spark_utils import convert_date_fields, explode_columns, use_last_value

from lhn.discern import add_concept_indicators

from lhn.header import get_logger

logger = get_logger(__name__)



def extract_fields_flat(table
                        ,fields_selected
                        ,toPandas = False
                        ,explod   = False
                        ,explode_field = ''
                        ,limit = 4):
    ''' Extract the selected columns from the `table` and flatten the results
    Replace the . with a _ 
    '''
    
    fields_selected           = fields_reconcile(fields_selected,  table)
    
    if explod:
        table2                = table.withColumn(explode_field, F.explode_outer(explode_field))
    else: 
        table2                = table
        
    table3                    = query_table(table2, fields_selected, concepts   = [],
                                            dedup       = False, last = False)
        
    if toPandas:
        return(table3.distinct().limit(limit).distinct().toPandas())
    else:
        return(table3)
    
def extract_fields_flat_top(
    table           : Any,
    i               : Any,
    concepts        : List[str] = [],
    count           : bool = True,
    byIndex         : bool = True,
    index           : Any = ["personid"],
    value           : str = "value",
    add_min_max     : bool = False,
    dedup_index     : Any = ["personid"],
    dedup_fields    : List[str] = [],
    last            : bool = False,
    last_fields     : List[str] = [],
    dedup           : bool = False,
    limit           : bool = True,
    obs             : int = 20,
    toPandas        : bool = True,
    countfield      : str = "Unique Subjects",
    explode_fields  : List[str] = [],
    datefields      : List[str] = [],
    debug           : bool = False,
    noRowNumFlag    : bool = False,
    sortfields      : Any = None
) -> None:
    
    ''' Create a listing of the top observations
    table:         The spark table to extract from
    i:             index of the field the identifies the field of interest.
    concept:       A list of concepts that have been added to the spark table
    count:         Should the records be counted , makes this run a little slower
    byIndex:       Should counting be by index, requires distinct, Otherwise will produce a record count
    index:         if counting by an index, what is the index
    dedup_index    Within the index, this index identifies levels, The levels with the highes count are selected
    last           Should only the last non-missing observation be selected
    last_fields    The fields that for whom only non missing last values will be used.
    dedup          Should the dedup algorithm be applied?
    limit:         Limit observations returned
    obs:           Number of observations limited to
    toPandas:      Should the result be converted to Pandas?
    sortfields:    A string or list of fields to sort the output by. Defaults to the count field if None.
    '''
    
    index           = translate_index(index,table, byIndex = True)
    if debug: print(f"The index will be {index}")

    
    concepts        = fields_reconcile(concepts,     table, delim = '_')
    dedup_index     = fields_reconcile(dedup_index,  table, delim = '_')
    dedup_fields    = fields_reconcile(dedup_fields, table, delim = '_')
    last_fields     = fields_reconcile(last_fields,  table, delim = '_')
    
    selected_fields = [*get_selected_fields(i, table, concepts = [], debug = False), value, *dedup_index, *dedup_fields, *last_fields]
    
    
    selected_fields = deduplicate_fields(table, selected_fields)
    # Create a link to the data table
    result1                      = query_table(table, 
                                               [*index, *selected_fields], 
                                               concepts          = concepts, 
                                               index             = index,
                                               dedup             = dedup, 
                                               last              = last, 
                                               dedup_fields      = dedup_fields,
                                               dedup_index       = dedup_index,
                                               last_fields       = last_fields,
                                               datefields        = datefields)
    
    result1_fields               = flatten_schema(result1.schema)
    
    if byIndex: 
        index_u                  = [col_name.replace('.','_') for col_name in index ]
        groupby_fields           = [item for item in result1_fields if ((item not in index_u) and (item not in dedup_fields) and (item != value))]
        result2                  = result1.distinct()  # count distinct
        
    else:
        groupby_fields           = result1_fields
        result2                  = result1
        
    if len(explode_fields) > 0:
        if debug: print(f"exploding results by {explode_fields}")
        result2b                 = explode_columns(explode_fields)(result2)
        #result2b= result2.withColumn(explode_field, explode_outer(explode_field))

    else:
        if debug: print("not Exploding")
        result2b                 =  result2

    if debug:
        print(f"index_u is {index_u}")
    if count:
        reordered_fields = [countfield, *groupby_fields]
        if add_min_max:
            result3               = (
                result2b
                .groupBy(groupby_fields)
                .agg(F.count(value).alias(countfield), F.max(value).alias("max_value"), F.min(value).alias("min_value"))
                .sort(F.col(countfield).desc())
                .select(reordered_fields)
                )
        else:
            result3               = (
                result2b
                .groupBy(groupby_fields)
                .agg(F.count(index_u[0]).alias(countfield))
                .sort(F.col(countfield).desc())
                .select(reordered_fields)
            )
    else:
        result3 = result2b

    if limit:
        result4 = result3.limit(obs)
    else:
        result4 = result3

    if toPandas:
        if debug:
            print(f"toPandas = {toPandas} converting to Pandas")
        resultPd = (
            result4
            .toPandas()
        )
        if noRowNumFlag:
            return(noRowNum(resultPd))
        else:
            return(resultPd)
    else:
        return(result4)





def query_flat_rwd(   
                           DF
                         , startDate
                         , stopDate
                         , fields              = ['personid']
                         , datefieldPrimary    = 'date'
                         , datefields          = ['date']
                         , explode_fields_post = []
                         , write               = False
                         , outTable            = 'extracted_table'
                         , conceptName         = []
                         , add_concepts        = False
                         , filter_concepts     = False
                         , cohort              = False
                         , code                = "code"
                         , tag                 = ""
                         , index               = ['personid']
                         , last                = False
                         , dedup               = False
                         , last_index          = ['personid']
                         , dedup_index         = ['personid']
                         , last_fields         = []
                         , dedup_fields        = []
                         , write_index         = False
                         , debug               = False
                         , dateConversion      = True):
    
    """ Use Oracle Discern to add or filter by concepts and then extract a flat table from the RWD
    @DF:                 The spark data frame
    @startDate:          The start date for the extraction, if None, no start date is used
    @stopDate:           The stop date for the extraction, if None, no stop date is used
    @fields:             The fields to extract
    @datefieldPrimary:   The primary date field, compared to  start and stop date
    @datefields:         The date fields to convert
    @explode_fields_post: The fields to explode
    @write:              Should the table be written out
    @outTable:           The table to write to
    @conceptName:        The name of the concept, a list
    @add_concepts:       Should the concepts be added as indicators?
    @filter_concepts:    Should the concepts be used to filter the data?
    @cohort:             The cohort to subset the extraction
    @code:               The code field used to identify the concept
    @tag:                A tag to add to the concept
    @index:              The index for the extraction, defines as distinct entity
    @last:               Should the last value be used when there are multiple values
    @dedup:              Should the deduplication algorithm be used
    @last_index:         The index for the last value
    @dedup_index:        The index for the deduplication algorithm
    @last_fields:        The fields for the last value
    @dedup_fields:       The fields for the deduplication algorithm
    @write_index:        Should an index table be written out
    @debug:              Should debug information be printed
    @dateConversion:     Should the date fields be converted to date
    """
    
    # Request sample subset according to a provided cohort
    if isinstance(cohort, str):
        cohort = spark.sql(f"""  
        select *  FROM {cohort}
        """)
        
    if isinstance(cohort, pd.DataFrame): 
        display(Markdown('* The extraction is relative to a cohort'))
        cohort_spark = spark.createDataFrame(cohort)
        use_cohort = (DF
                      .join(cohort_spark, on = index, how = 'inner' )
                     )
    elif isinstance(cohort, DataFrame):
        display(Markdown('* The extraction is relative to a cohort'))
        use_cohort = (DF
                      .join(cohort, on = index, how = 'inner' )
                     )
    else:
        
        display(Markdown(f"* The cohort parameter is {cohort}  Extraction is over the entire RWD"))
        # alias the input data set.
        use_cohort = DF
    
    

    if add_concepts:
        added_concepts = (
            add_concept_indicators(conceptName, code, tag)(use_cohort)
        )
    else:
        added_concepts = use_cohort

    if filter_concepts:
        filtered_concepts = (
            added_concepts
            .filter(f"""has_any_concept({code}, array({", ".join([f"'{x}'" for x in conceptName])}))""")
        )
    else:
        filtered_concepts = added_concepts
        
    # Take care of date fields
    #display(Markdown(f"* The proposed date fields are {datefields} "))
    date_fields_actual = fields_reconcile(datefields, filtered_concepts, delim = '_')
    
    if (len(date_fields_actual) > 0) & dateConversion:
        display(Markdown(f'* The actual date fields `datefields` are {date_fields_actual}'))
        dated_table = convert_date_fields(date_fields_actual)(filtered_concepts)
    else:
        display(Markdown('* No Date Fields'))
        dated_table = filtered_concepts
        
    # Handle startdate
    if startDate:
        display(Markdown(f"* Use `datefieldPrimary >= startDate: {datefieldPrimary}  >= '{startDate}"))
        startdate_table = dated_table.filter(f" {datefieldPrimary}  >= '{startDate}'")
    else:
        display(Markdown("* Not using a start date because `startDate` is None."))
        startdate_table = dated_table
        
    # Handle enddate
    if stopDate:
        display(Markdown(f"* Using the stop date `stopDate` of {stopDate}."))
        stopdate_table = startdate_table.filter(f" {datefieldPrimary}  <= '{stopDate}'")
    else:
        print('Not Using a stop date `stopDate` is None.')
        stopdate_table = startdate_table
    print(f"the fields are {fields}")
    queried_table = (query_table(  
                  stopdate_table
                , fields
                , datefields   = datefields
                , concepts     = conceptName
                , index        = index
                , last         = last
                , dedup        = dedup
                , last_index   = last_index
                , dedup_index  = dedup_index
                , last_fields  = last_fields
                , dedup_fields = dedup_fields
               )        
           )
    if debug:
        display(Markdown("* Write Table Error in call: `query_table`"))
        print(queried_table.limit(1).show())
  
    # Take care of exploted fields
    #display(Markdown(f"* The proposed post query explode fields are {explode_fields_post}"))
    explode_fields_actual = fields_reconcile(explode_fields_post, queried_table , delim = '_')

    if len(explode_fields_actual) > 0:
        display(Markdown("* Exploding over the fields: {explode_fields_actual}"))
        exploded_table = explode_columns(explode_fields_actual)(queried_table)
    else:
        exploded_table = queried_table

    if write_index:
        display(Markdown(f"* Creating a cohort style Index table with datefield {datefieldPrimary} and index {index}"))
        windowSpec = (
             Window.partitionBy(index)
             .orderBy(datefieldPrimary)
             .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        )
        cohort_index = (
            exploded_table
            .select([*index, datefieldPrimary])
            .distinct()
            .withColumn('index_' +  code,  F.first(datefieldPrimary, ignorenulls=True).over(windowSpec) )
            .withColumn('last_'  +  code,  F.last(datefieldPrimary , ignorenulls=True).over(windowSpec) )
            .withColumn('entries_' + code, F.count(*index).over(windowSpec) )
            .select([*index,'index_' + code,'last_'  + code, 'entries_' + code])
            .distinct()
            .sort(['personid','index_' + code])                
        )
    else:
        
        display(Markdown(f"* Not Creating a cohort style index table"))
        cohort_index = exploded_table
        
        
    if write:
        try:
            display(Markdown(f"* Writing the chort table out to {outTable}"))
            (cohort_index
            .write
            .mode("overwrite")
            .saveAsTable(outTable) 
            )   
        except:
            display(Markdown(f'* **Error saving the table to {outTable}**.  The first two rows'))
            print(cohort_index.columns)
            #print(cohort_index.limit(2).toPandas())
    return(cohort_index)
            
       


def query_table(  table
                , fields
                , datefields   = ['date']
                , concepts     = []
                , index        = ['']
                , last         = True
                , dedup        = True
                , last_index   = ['']
                , dedup_index  = ['']
                , last_fields  = []
                , dedup_fields = []
               ):
    
    ''' Create a one record per person table with the selected fields and concepts
    The fields are all flattened and dots are replaced with underscores
    @table A spark data table
    @fields A list of fields to select
    @concepts_selected Concepts to include: Must be in table.columns
    @index a list of the indexes
    @last Should the last element in an array be used. Use only if a specific last_index is available.
    '''
   
    # Change to using dots if it currently uses underscore
    
    # A flattened list of all fields.
    #table_schema = flatten_schema(table.schema)
    
    #Modify fields but allow for a list of not edited fields
    fields           = fields_reconcile(fields,       table)
    datefields       = fields_reconcile(datefields,   table)
    index            = fields_reconcile(index,        table)
    last_index       = fields_reconcile(last_index,   table)
    dedup_index      = fields_reconcile(dedup_index,  table)
    last_fields      = fields_reconcile(last_fields,  table)
    dedup_fields     = fields_reconcile(dedup_fields, table)
    concepts         = fields_reconcile(concepts,     table)
    
    
    # Reorder
    fields_ordered = []
    all_fields     = [*fields, *datefields, *last_index, *dedup_index,  *fields, *last_fields, *dedup_fields, *concepts]
    [fields_ordered.append(field) for field in all_fields  if field not in fields_ordered ]
    
    # Get underscore versions
    fields_u       = [item.replace('.','_') for item in fields_ordered]
    index_u        = [item.replace('.','_') for item in index]
    dedup_index_u  = [item.replace('.','_') for item in dedup_index]
    last_index_u   = [item.replace('.','_') for item in last_index]
    last_fields_u  = [item.replace('.','_') for item in last_fields]
    dedup_fields_u = [item.replace('.','_') for item in dedup_fields]
    concepts_u     = [item.replace('.','_') for item in concepts]
    
    # index for counting distinct units of observation
    windowSpecLast     = Window.partitionBy([*index]).orderBy(last_index_u)
    
    # partition to count replicate levels of dedup_fields by dedup_index
    # For example: gender, if male occurs 3 times and female occurs 2 times, male is chosen as the gender level.
    windowSpecReps      = Window.partitionBy([F.col(col1) for col1 in [*index, *dedup_index_u, *dedup_fields_u]])

    windowSpecOrder     = Window.partitionBy(index).orderBy(F.col("replicates").desc_nulls_last())   
    
    #.withColumn('deceased'           , F.last('deceased',ignorenulls=True       ).over(windowSpec))
    
    # Change from dots to underline
    fields_selected = [F.col(field).alias(field.replace('.','_')) for field in fields_ordered]
    
    #windowSpecOrder = Window.partitionBy(dedup_index_u).orderBy(F.col("replicates").desc())
    # windowSpec      = Window.partitionBy('empiPersonMetadata_empiPersonId')

    # Import the use_last_value function from the appropriate module

    result_with_dups = (
        table
        .select(fields_selected)
    )

    if last:
        results_last = use_last_value(last_fields_u, windowSpecLast)(result_with_dups)
    else:
        results_last = result_with_dups
                   
    if dedup:
         results = (
             results_last
             .withColumn('replicates', F.count(F.lit(1)).over(windowSpecReps))
             .withColumn("rn", F.row_number().over(windowSpecOrder))
             .withColumn('rn2', F.when(F.col(dedup_fields_u[0]).isNull(), None).otherwise(F.col('rn')))
             .withColumn("rn_min", min('rn2').over(windowSpecOrder))
             .where(F.col("rn2") == F.col('rn_min'))
             .drop(*['rn', 'rn2', 'rn_min'])
         )
    else:
        results = result_with_dups
    return(results)

