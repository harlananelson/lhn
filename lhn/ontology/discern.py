


from lhn.header import F, spark, datetime, Window, re
#from .header import spark, pprint, datetime


from spark_config_mapper import database_exists
from spark_config_mapper.utils.spark_ops import convert_date_fields

from lhn.header import get_logger

logger = get_logger(__name__)

import difflib


############################## Ontology Tabulation ##################################
def getCodingSystemId(flat_table, inTable, codefield,  index = ['personid'], explode_fields = [], dateGroupingFields = []):
    """
    Extracts the coding system ID from a given table and associates it with a specific codefield.

    This function extracts the coding system ID from the specified field in the given table and associates it with a specific codefield (e.g., "labcode" or "conditioncode"). The coding system ID is typically stored in a field named "standard_codingSystemId" in many tables in the dataset. To identify which type of code is being used, the codefield is prepended to the name, and the resulting dataset is returned. The table name and the codefield name are also retained with a set of selected fields.

    Parameters:
    flat_table (DataFrame): The input table from which to extract the coding system ID.
    inTable (str): The name of the input table.
    codefield (str): The name of the field from which to extract the coding system ID.
    index (list, optional): A list of fields to use as the index. Defaults to ['personid'].
    explode_fields (list, optional): A list of fields to explode. Defaults to an empty list.
    dateGroupingFields (list, optional): A list of fields to use for date grouping. Defaults to an empty list.

    Returns:
    DataFrame: A DataFrame containing the coding system ID, along with any specified grouping and exploding fields, the table name, and the codefield name.
    """
    
    # rest of your code
    print([f'{codefield}_{field}' for field in explode_fields])
    selectFields = ['tableName', 'codefield', 'standard_codingSystemId', 'codingSystemCount', *dateGroupingFields]
    
    codingSystemId = (
        flat_table
        .withColumnRenamed(f'{codefield}_standard_codingSystemId', 'standard_codingSystemId')  
        .withColumn('tableName', F.lit(inTable))
        .withColumn('codefield', F.lit(codefield)) 
        .select(selectFields)
    )
    
    return(codingSystemId)

def getCodesAndSystem(flat_table, inTable, codefield, index = ['personid'], explode_fields = [], dateGroupingFields = []):
    """
    Extract the code, coding system ID, and display name for a codefield.

    Parallel to :func:`getCodingSystemId` but returns the full code triplet
    (standard_id, standard_codingSystemId, standard_primaryDisplay) rather
    than only the coding system ID. Renames the codefield-prefixed columns
    to their generic ``standard_*`` names and tags each row with ``tableName``
    and ``codefield`` literals for downstream ontology tabulation.

    Parameters:
        flat_table (DataFrame): Flattened source table containing
            ``{codefield}_standard_id``, ``{codefield}_standard_codingSystemId``,
            and ``{codefield}_standard_primaryDisplay`` columns.
        inTable (str): Name of the source table (becomes the ``tableName``
            column value).
        codefield (str): The field name prefix (e.g. ``"conditioncode"``,
            ``"labcode"``).
        index (list, optional): Index fields. Defaults to ``['personid']``.
        explode_fields (list, optional): Array fields to explode before
            extraction. Defaults to ``[]``.
        dateGroupingFields (list, optional): Date fields to retain for
            downstream grouping. Defaults to ``[]``.

    Returns:
        DataFrame: Rows of ``tableName, codefield, standard_id,
        standard_codingSystemId, standard_primaryDisplay, codeCount``
        plus any ``dateGroupingFields``.
    """
    selectFields = ['tableName', 'codefield', 'standard_id', 'standard_codingSystemId'
                    , 'standard_primaryDisplay', 'codeCount', *dateGroupingFields]
    codes_and_system = (  
                        flat_table
        .withColumnRenamed(f'{codefield}_standard_id',             'standard_id')
        .withColumnRenamed(f'{codefield}_standard_codingSystemId', 'standard_codingSystemId')
        .withColumnRenamed(f'{codefield}_standard_primaryDisplay', 'standard_primaryDisplay')  
        .withColumn('tableName', F.lit(inTable))
        .withColumn('codefield', F.lit(codefield)) 
        .select(selectFields)
    )
    
    return(codes_and_system)


def summarizeCodes(inSchema, inTable, index, codefield, datefields, explode_fields,
                   codes_and_systems
                   , startDate
                   , stopDate
                   , dateGroupingFields
                   , ontCodingSystemIdPd
                   , codesNotMatchedMin                                    = 2
                   , transform_codingSystem                                = 'insertInto'
                   , transform_Crosswalk                                   = 'insertInto'
                   , transform_codingsystemsnotmatched                     = 'insertInto'
                   , transform_codesAndSystemNotMatched                    = 'insertInto'
                   , transform_codesAndSystemMatched                       = 'insertInto'
                   , transform_systemMatched                               = 'insertInto'
                   , tabulated_ontologies                                  = ''):
    """
    Superceded by ontologyByTableCode
    Summarize the code in the selected table for the selected date range.
    
    - table codingSystem
       - codingSystemCount standard_codingSystemId  month  year  tableName
    - codingSystemIdCrosswalk
       - tableName      codefield standard_codingSystemId
    - codingsystemsnotmatched   
       - codingSystemCount standard_codingSystemId  month  year  tableName
    
    
    Args: 
       inSchema:            The source table schema.
       inTable:             The source table name.
       index:               Identifier for an individual (personid)
       codefield:           The field name used by the Cerner Discern UDF
       datefields:          A list of all the datetime fields to be converted to date.
                            These are coalesced in order for startDate and stopDate.
       explode_fields:      Fields in the codefield that need exploding because they are in an array.
       startDate:           The starting date of the slice used.
       stopDate:            The ending date of the slice used.
       dateGroupingFields:  Fields used to group dates in the summary (year, month)
       ontCodingSystemIdPd: A table with the codingSystemId levels used as cataloged by Discern.
       transform_codingSystem:                  To to transform table codingSystem
       transform_Crosswalk:                     How to transform table 
       transform_data_codesAndSystemNotMatched: Boolean: should data_coding_system_not_matched_by_ontology be initialized?
       transform_codesAndSystemNotMatched:      Boolean, should the codesAndSystemNotMatched table be initialized?
       transform_codesAndSystemMatched:         Boolean, should the codesAndSystemMatched table be initialized?
       
       
    
    """
    # Get a pointer to the spark data frame
    # Create a SparkSession object
    

    table  = (
        spark.sql(f""" SELECT * FROM {inSchema}.{inTable} """ )
        .select([index, codefield, *datefields])
    )

    # Convert the datetime fields to date and filter by time slice
    table = (convert_date_fields(datefields)(table)
             .withColumn('date', F.coalesce(*datefields))
             .select([index, codefield, 'date'])
             .filter(F.col('date') <= stopDate)
             .filter(F.col('date') >= startDate)
             .withColumn('month', F.month('date'))
             .withColumn('year',  F.year('date'))
    )
    # The new coalesced date field
    datefield = 'date'
    # Need to make sure these fields are not emplty
    explode_fields_m = [field for field in [explode_fields] if len(explode_fields) > 0]
    
    ################################# codingSystem            ########################################
    ##################################################################################################
    
    codingSystemIdDt   = getCodingSystemId(
        table, inTable, codefield, explode_fields = explode_fields_m, dateGroupingFields = dateGroupingFields )
    # preserve the schema so it can be used to convert a pandas version back to a pyspark data table
    codingSystemId_schema = codingSystemIdDt.schema
        
    codingSystemIdPd      = codingSystemIdDt.toPandas()
    codingSystemId        = spark.createDataFrame(codingSystemIdPd, codingSystemId_schema)
    print("CodingSystemId")
    print(codingSystemIdPd[:1])
    
       
    updateTable(inTable
                , codefield
                , "codingSystem"
                , codingSystemId
                , dateGroupingFields
                , transform       = transform_codingSystem
                , convertToPandas = False
                , tabulated_ontologies = tabulated_ontologies)
    
    ############################### codingSystemIdCrosswalk Crosswalk #############################
    ################between codingSystemId (ontology) and standard_codingSystemId (tables) ########
    # This is needed because for some reason these are coded different in the ontology tables
    # as compared to the data tables.
    
    ## Crosswalk between coding systems in the data and in the ontology
    codingSystemIdCrosswalkPd = findCrosswalk(codingSystemIdPd, ontCodingSystemIdPd) 
    # Add the crosswalk field to the schema so this schema can be used to create a pandas dataframe
    crosswalkSchema = (
        codingSystemId
        .withColumn('codingSystemId', F.col('standard_codingSystemId'))
        .select([*[name for name in codingSystemIdCrosswalkPd.columns]])
        .schema
    )
    codingSystemIdCrosswalk = spark.createDataFrame(codingSystemIdCrosswalkPd, schema = crosswalkSchema)
    
    print("codingSystemIdCrosswalk")
    print(codingSystemIdCrosswalkPd[:1])
 
    if not codingSystemIdCrosswalkPd.empty:
        updateTable(inTable, codefield, 'codingSystemIdCrosswalk',codingSystemIdCrosswalk
                    , transform            = transform_Crosswalk
                    , convertToPandas      = False
                    , tabulated_ontologies = tabulated_ontologies)
    
    ############################### codingsystemsnotmatched ##############################
    #####################################################################################
    if not codingSystemIdCrosswalkPd.empty:   
        cwFields = ['tableName', 'codefield', 'standard_codingSystemId', 'codingSystemId']
        codingsystemsnotmatchedPd = (
            codingSystemIdPd
            .merge(
                codingSystemIdCrosswalkPd[cwFields]
                , on = ['tableName', 'codefield', 'standard_codingSystemId']
                , how = 'outer', indicator = True)
            .query('_merge == "left_only"')
            .reset_index()
            .drop(columns = ['_merge', 'codingSystemId', 'index'])
            .drop_duplicates()
            .sort_values(by = [f'codingSystemCount'], ascending = False)
        )  
        
    else:
            codingsystemsnotmatchedPd = codingSystemIdPd

    codingsystemsnotmatched = spark.createDataFrame(
        codingsystemsnotmatchedPd, schema = codingSystemId.schema)
    
    
    # The third positional arg is the Hive table name (literal string).
    # Interpolating the DataFrame via f'{codingsystemsnotmatched}' would
    # stringify a DataFrame repr — not a table name. Use the literal.
    updateTable(inTable, codefield, 'codingsystemsnotmatched', codingsystemsnotmatched
                    , transform       = transform_codingsystemsnotmatched
                    , convertToPandas = False
                    , tabulated_ontologies = tabulated_ontologies)

    
            
    print("codingsystemsnotmatched")
    print(codingsystemsnotmatchedPd[:1])

    ############# codingSystemEnriched ###########################################################
    ############## matched and not matched ###############################################    
    # codingSystemEnriched
    # - codingSystemId, codingSystem standard_id, tableName, codefield, standard_codingSystemId
    
    # Retrieve a list of all the coding systems and codes listed in the Ontologies.
    codingSystem = (
        spark.sql(f"""SELECT * FROM {tabulated_ontologies}.codingSystem""")
        .withColumnRenamed('conceptCode', 'standard_id')
    )

    
    # Add the standard_codingSystemId field, which links it to the concept field in the dataset
    # This will also subset to only the coding systems in the crosswalk
    codingSystemEnrichedDt     = (
        codingSystem
        .join(codingSystemIdCrosswalk, on = ['codingSystemId']
              , how = 'inner')
    )
    codingSystemEnrichedSchema = codingSystemEnrichedDt.schema
    codingSystemEnrichedPd     = codingSystemEnrichedDt.toPandas()
    print("codingSystemEnrichedPd")
    print(codingSystemEnrichedPd[:1])
    codingSystemEnriched       = spark.createDataFrame(codingSystemEnrichedPd, codingSystemEnrichedSchema)

    
    ############# codesAndSystem ###########################################################
    ############## matched and not matched ###############################################
    # codesAndSystem
    #    - tableName codefield standard_id standard_codingSystemId standard_primaryDisplay  
    #      codeCount  year  month

    codesAndSystemDt        = getCodesAndSystem(table, inTable, codefield = codefield, index = index
                                                , explode_fields = explode_fields
                                                , dateGroupingFields = dateGroupingFields)
    codesAndSystemPd        = codesAndSystemDt.toPandas()
    codesAndSystem          = spark.createDataFrame(codesAndSystemPd, schema = codesAndSystemDt.schema)
    
    print("codesAndSystem")
    print(codesAndSystemPd[:1])
    
    ############################# codesAndSystemNotMatched ###############################################
    ######################################################################################################
    
    # Given the coding system and codes found in this table, what combinations are not in the ontology
    # Add the codingSystemCount from codingSystemId
    
    # codesAndSystemNotMatched
    # tableName, codefield, year, month, standard_codingSystemId standard_id
    # standard_primaryDisplay, codeCount, codingSystemCount
    
    codesAndSystemNotMatchedDt = (
        codesAndSystem
        .join(
            codingSystemEnriched, 
            on = [f'standard_codingSystemId', 'standard_id'], how = 'left_anti')
        .select(['tableName', 'codefield', *dateGroupingFields, 'standard_codingSystemId', 'standard_id'
                 ,'standard_primaryDisplay',  'codeCount'])
        .sort(F.col('standard_codingSystemId'),F.col('codeCount').desc())
        .distinct()
        .join(codingSystemId, on = ['standard_codingSystemId', 'tableName', 'codefield', *dateGroupingFields ]
              , how = 'inner')
        .select(['tableName', 'codefield', *dateGroupingFields
                 ,'standard_codingSystemId',  'standard_id', 'standard_primaryDisplay'
                 , 'codeCount', 'codingSystemCount'])
    )
    
    codesAndSystemNotMatchedPd     = codesAndSystemNotMatchedDt.filter(F.col('codeCount') > 2).toPandas()
    codesAndSystemNotMatched       = spark.createDataFrame(codesAndSystemNotMatchedPd
                                                           , schema = codesAndSystemNotMatchedDt.schema)
    
    print("codesAndSystemNotMatched")
    print(codesAndSystemNotMatchedPd[:1])
 
    updateTable(inTable, codefield, 'codesAndSystemNotMatched', codesAndSystemNotMatched
                    , transform            = transform_codesAndSystemNotMatched
                    , convertToPandas      = False
                    , tabulated_ontologies = tabulated_ontologies)
           
    if codingSystemIdCrosswalkPd.empty:
        print(f"No matching systems in table {inTable} and codefield {codefield}")
        return()
    
   
    ############################# codesAndSystemMatched ###############################################
    ###################################################################################################
    
    # Given the coding system and codes found, in this table, what combinations match the ontology
    
    codesAndSystemMatchedDt = (
        codes_and_systems
        .join(
            codingSystemEnriched, 
            on = [f'standard_codingSystemId', 'standard_id', 'tableName', 'codefield'], how = 'inner')
        .withColumn('tableName', F.lit(inTable))
        .withColumn('codefield', F.lit(codefield))        
        .select(['tableName', 'codefield','standard_codingSystemId', 'codingSystemId', 'codingSystem', 'standard_id'
                 ,'standard_primaryDisplay', 'codeCount'])        .sort(F.col('standard_codingSystemId'),F.col('codeCount').desc())
        .distinct()       
        .join(codingSystemId, on = ['standard_codingSystemId', 'tableName', 'codefield'], how = 'inner')
        .select(['tableName', 'codefield', 'standard_codingSystemId', 'codingSystemId', 'codingSystem', 'standard_id', 'standard_primaryDisplay'
                 , *dateGroupingFields, 'codeCount', 'codingSystemCount'])


    )
    
    
    codesAndSystemMatchedSchema = codesAndSystemMatchedDt.schema
    codesAndSystemMatchedPd     = codesAndSystemMatchedDt.toPandas()
    codesAndSystemMatched       = spark.createDataFrame(codesAndSystemMatchedPd, schema = codesAndSystemMatchedSchema)
    
    print("codesAndSystemMatched")
    print(codesAndSystemMatchedPd[:1])
    
    updateTable(inTable, codefield, 'codesAndSystemMatched', codesAndSystemMatched
                    , transform            = transform_codesAndSystemMatched
                    , convertToPandas      = False
                    , tabulated_ontologies = tabulated_ontologies)
    
    ############################# codesAndSystemMatched ###############################################
    ###################################################################################################
    
    
    systemMatched = (
        codesAndSystemMatched
        .select(['tableName', 'codefield', *dateGroupingFields, 'standard_codingSystemId', 'codingSystemId', 'codingSystem', 'codingSystemCount'])
        .distinct()
    )
    
    print("systemMatched")
    print(systemMatched.limit(2).toPandas())
    init_systemMatched = True     # fix this
    if init_systemMatched:
        (
            systemMatched
            .write
            .mode("overwrite")
            .saveAsTable(f"{tabulated_ontologies}.data_coding_system_matched_by_ontology")
        )
    else:
        updateTable(inTable
                                     , codefield
                                     , "data_coding_system_matched_by_ontology"
                                     , systemMatched
                                     , dateGroupingFields
                                     , convertToPandas      = False
                                     , tabulated_ontologies = tabulated_ontologies)
    
 
    
    return(codesAndSystemMatched)



####################################################################################

def search_ontologies(name_regex, system_regex, context_regex = ".*", code_regex = ".*", systemDescription_regex = ".*"
                     ,toPandas = True, limit = False, obs = 20):
    """! Search the onthologies using a regular expression
    @param conceptName  A regular expression identifying the concept
    @param CodingSystem A regular expression identifying the coding system
    @param context      A regular expression identifying the concept
    @result             A Pandas data table with the identified ontologies
    """
    result = (spark.sql(f"""
    SELECT * FROM standard_ontologies.ontologies
    
    WHERE conceptName              RLIKE "(?i){name_regex}"
    AND   codingSystemId           RLIKE "(?i){system_regex}"
    AND   contextDescription       RLIKE "(?i){context_regex}"
    AND   conceptCode              RLIKE "(?i){code_regex}"
    AND   codingSystemDescription  RLIKE "(?i){systemDescription_regex}"
    """)
             )
    if limit:
        result2 = result.limit(obs)
    else:
        result2 = result
        
    if toPandas:
        return(result2.toPandas())
    else:
        return(result2)
    

def contextId_ont(name_regex, system_regex, context_regex, code_regex):
    """!List the unique contexts in an onthology
    @param ont A subset of the onthology table 
    """
    result = (spark.sql(f"""
    SELECT DISTINCT contextId FROM standard_ontologies.ontologies
    
    WHERE conceptName        RLIKE "(?i){name_regex}"
    AND   codingSystemId     RLIKE "(?i){system_regex}"
    AND   contextDescription RLIKE "(?i){context_regex}"
    AND   conceptCode        RLIKE "(?i){code_regex}"
    """)
              .toPandas()
    )
    return(result)

def context_ont(name_regex, system_regex, context_regex, code_regex, systemDescription_regex = ".*"):
    """!List the unique contexts in an onthology
    @param ont A subset of the onthology table 
    """
    result = (spark.sql(f"""
    SELECT DISTINCT contextId, contextDescription, contextVersion
    FROM standard_ontologies.ontologies
    WHERE conceptName              RLIKE "(?i){name_regex}"
    AND   codingSystemId           RLIKE "(?i){system_regex}"
    AND   contextDescription       RLIKE "(?i){context_regex}"
    AND   conceptCode              RLIKE "(?i){code_regex}"
    AND   codingSystemDescription  RLIKE "(?i){systemDescription_regex}"
    """)
              .toPandas()
    )
    return(result)

def conceptCode_ont(name_regex, system_regex, context_regex, code_regex
                   ,to_pandas = True):
    """!List the unique contexts in an onthology
    @param ont A subset of the onthology table 
    """
    result = (spark.sql(f"""
    SELECT DISTINCT codingSystemId, codingSystemDescription, conceptCode
    FROM standard_ontologies.ontologies
    WHERE conceptName                 RLIKE "(?i){name_regex}"
    AND   codingSystemDescription     RLIKE "(?i){system_regex}"
    AND   contextDescription          RLIKE "(?i){context_regex}"
    AND   conceptCode                 RLIKE "(?i){code_regex}"
    order by codingSystemId
    """)
    )
    if to_pandas:
        return(result.toPandas())
    else:
        return(result)
    
def system_ont(name_regex, system_regex, context_regex, code_regex, systemDescription_regex = "."):
    """!List the unique contexts in an onthology
    @param name_regex     A regular expression identifying the concept
    @param system_regex   A regular expression identifying the coding system
    @param context_regex  A regular expression identifying the concept
    @param code_regex     A regular expression identifying the actual code
    """
    result = (spark.sql(f"""
    SELECT DISTINCT codingSystemId, codingSystemDescription
    FROM standard_ontologies.ontologies
    WHERE conceptName              RLIKE "(?i){name_regex}"
    AND   codingSystemId           RLIKE "(?i){system_regex}"
    AND   codingSystemDescription  RLIKE "(?i){systemDescription_regex}"
    AND   contextDescription RLIKE "(?i){context_regex}"
    AND   conceptCode        RLIKE "(?i){code_regex}"
    """)
              .toPandas()
    )
    return(result)

def system_name_ont(name_regex, system_regex, context_regex, code_regex):
    """!List the unique contexts in an onthology
    @param name_regex     A regular expression identifying the concept
    @param system_regex   A regular expression identifying the coding system
    @param context_regex  A regular expression identifying the concept
    @param code_regex     A regular expression identifying the actual code
    @param ont A subset of the onthology table 
    """
    result = (spark.sql(f"""
    SELECT DISTINCT codingSystemId, codingSystemDescription, conceptName
    FROM standard_ontologies.ontologies
    WHERE conceptName                 RLIKE "(?i){name_regex}"
    AND   codingSystemDescription     RLIKE "(?i){system_regex}"
    AND   contextDescription          RLIKE "(?i){context_regex}"
    AND   conceptCode                 RLIKE "(?i){code_regex}"
    """)
              .toPandas()
    )
    return(result)

def concept_ont(name_regex, system_regex, context_regex, code_regex, systemDescription_regex = ".*", toPandas = True):
    """!List the unique concepts in an onthology extraction
    @param ont A data table that is a subset of the onthology table
    """  
    result = (spark.sql(f"""
    WITH G AS (
    SELECT DISTINCT 
       conceptName,contextId,conceptCode
        ,rank() OVER (PARTITION BY conceptName ORDER BY contextID)    as contextID
        ,rank() OVER (PARTITION BY conceptName ORDER BY conceptCode)  as conceptCodeRank
    FROM standard_ontologies.ontologies 
    WHERE conceptName              RLIKE "(?i){name_regex}"
    AND   codingSystemId           RLIKE "(?i){system_regex}"
    AND   contextDescription       RLIKE "(?i){context_regex}"
    AND   conceptCode              RLIKE "(?i){code_regex}"
    AND   codingSystemDescription  RLIKE "(?i){systemDescription_regex}"
    )
    
    SELECT DISTINCT
    conceptName
      
       ,max(contextIDRank) OVER (PARTITION BY conceptName)    as contextCount
       ,max(conceptCodeRank) OVER (PARTITION BY conceptName)  as CodeCount
    FROM G
    """)
              .orderBy(F.col('CodeCount').desc())
    )
    if toPandas:
        result2 = result.toPandas()
    else: 
        result2 = result
    return(result2)

def context_name_ont(name_regex, system_regex, context_regex, code_regex,systemDescription_regex = ".*"
                     , toPandas = True):
    """!List the unique contexts in an onthology
    @param ont A subset of the onthology table 
    """
    result = (spark.sql(f"""
    SELECT DISTINCT contextId, conceptName
    FROM standard_ontologies.ontologies
    WHERE conceptName              RLIKE "(?i){name_regex}"
    AND   codingSystemId           RLIKE "(?i){system_regex}"
    AND   contextDescription       RLIKE "(?i){context_regex}"
    AND   conceptCode              RLIKE "(?i){code_regex}"
    AND   codingSystemDescription  RLIKE "(?i){systemDescription_regex}"

    """)
    )
    if toPandas:
        result1 = result.toPandas()
    else:
        result1 = result
        
    return(result)

def context_name_system_ont(name_regex, system_regex, context_regex, code_regex,systemDescription_regex = ".*"
                     , toPandas = True, ontology = 'standard_ontologies.ontologies'):
    """!List the unique contexts in an onthology
    @param ont A subset of the onthology table 
    """
    result = (spark.sql(f"""
    SELECT DISTINCT contextId, conceptName, codingSystemId
    FROM {ontology}
    WHERE conceptName              RLIKE "(?i){name_regex}"
    AND   codingSystemId           RLIKE "(?i){system_regex}"
    AND   conceptCode              RLIKE "(?i){code_regex}"

    """)
    )
    if toPandas:
        result1 = result.toPandas()
    else:
        result1 = result
        
    return(result)

def codingSystem_ont(name_regex = '.*', system_regex = '.*', context_regex = '.*', code_regex = '.*', systemDesc_regex = '.*'):
    """
    Search the Discern standard ontologies catalog with regex filters.

    Queries ``standard_ontologies.ontologies`` and returns a summary of
    matching (contextId, codingSystemId) pairs with concept counts. All
    regex arguments are applied case-insensitively.

    Parameters:
        name_regex (str): Regex matched against ``conceptName``. Defaults
            to ``'.*'`` (all).
        system_regex (str): Regex matched against ``codingSystemId``.
        context_regex (str): Regex matched against ``contextDescription``.
        code_regex (str): Regex matched against ``conceptCode``.
        systemDesc_regex (str): Regex matched against
            ``codingSystemDescription``. Case-sensitive due to a typo in
            the inline pattern (``(?)`` instead of ``(?i)``).

    Returns:
        DataFrame: ``contextId, contextDescription, contextVersion,
        codingSystemId, codingSystemDescription, conceptCount`` sorted
        by ``conceptCount`` descending.
    """
    result_codingSystem = (spark.sql(f"""
    WITH G AS (
    SELECT DISTINCT 
       contextId, contextDescription, contextVersion, 
       codingSystemId, codingSystemDescription, conceptName  
    FROM standard_ontologies.ontologies 
    WHERE conceptName             RLIKE "(?i){name_regex}"
    AND   codingSystemId          RLIKE "(?i){system_regex}"
    AND   contextDescription      RLIKE "(?i){context_regex}"
    AND   conceptCode             RLIKE "(?i){code_regex}"
    AND   codingSystemDescription RLIKE "(?){systemDesc_regex}"
    )
    SELECT contextId, contextDescription, contextVersion, 
           codingSystemId, codingSystemDescription, 
           count(*) as conceptCount
    FROM G
    GROUP BY contextId, contextDescription, contextVersion, 
             codingSystemId, codingSystemDescription
    """)
       .orderBy(F.col('conceptCount').desc())
    )
    return(result_codingSystem)

def conceptName_list(ont):
    """!List the unique concepts in an onthology extraction
    @param ont A data table that is a subset of the onthology table
    """
    concept = (ont[['conceptName']]
               .drop_duplicates()
              )
    return(concept)

def contextId_list(ont):
    """
    Extract the context for use in push_discern()
    """
    context = (ont[['contextId']]
               .drop_duplicates()
              )
    return(context)

def context_list(ont):
    """!List the unique contexts in an onthology
    @param ont A subset of the onthology table 
    """
    context = (ont[['contextId','contextDescription']]
               .drop_duplicates()
              )
    return(context)

def coding_list(ont):
    """! List the unique coding system in an onthology
    @param ont A subset of the onthology table
    """
    coding = ont[['codingSystemId','codingSystemDescription']]\
    .drop_duplicates()
    return(coding)

def codes_list(ont):
    """ List the actual codes in the onthology
    @param ont A subset of the onthology table
    """
    coding = ont[['conceptName','codingSystemDescription','conceptCode']]\
    .drop_duplicates()
    return(coding)


def show_desc(inTable, code):
    """
    Show the descriptions.ontology table
    """
    desc = (spark.sql(
        f"""
        SELECT  *
        FROM descriptions.ontology
        WHERE table == '{inTable}' AND code RLIKE '{code}'
        """)
     .select(['conceptName','table','code','count','n','percent'])
     .distinct()
     .filter(F.col('percent') > 0)
     .limit(40)
     .toPandas()
    )
    return(desc)

#################################################################################################
########################  Ontology Dictionary Creation ##########################################
#################################################################################################

def createMetaOnt(flat_table, outTBL, config_dict = {}):
    """Creates a metadata table for a given source table.

    Args:
        sourceTable (dataTable): healthidata table
        inclusionRegEx (array): list of regex used to select columns
        outTBL (string): name of the output table
        controlDic (dict, optional): dictionary of control parameters. Defaults to {}.
    """

    
    
    result = flat_table
    print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'))
    print(f"Saving to {outTBL}")
    (result
    .distinct()
    .write
    .saveAsTable(outTBL, mode = 'overwrite')
    )
    print(datetime.now().strftime('%d-%m-%Y %H:%M:%S'))
    return(spark.table(outTBL))


def demoTable(field, inSchema, claim = False):
    """
    Load a FirstLastMost demographic lookup table from clinical_research_systems.

    Returns the two-column slice ``(personid, {field})`` from a table named
    ``clinical_research_systems.{inSchema}[_CLAIM]_{field}_FirstLastMost``.
    These "FirstLastMost" tables are precomputed by the Discern pipeline to
    pick one demographic value per person (first/last/most-frequent) for
    join-friendly use downstream.

    Parameters:
        field (str): Demographic field name, e.g. ``'gender'``, ``'race'``,
            ``'ethnicity'``. Used as both the table-name suffix and the
            selected column.
        inSchema (str): Schema prefix for the FirstLastMost table name
            (e.g. a dataset slug).
        claim (bool, optional): If True, inject ``'CLAIM_'`` into the table
            name (``{inSchema}_CLAIM_{field}_FirstLastMost``). Defaults to
            False.

    Returns:
        DataFrame: Two columns — ``personid`` and the requested ``field``.
    """
    if claim:
        claim = f"CLAIM_"
    else: claim = ""
        
    tableName = f"clinical_research_systems.{inSchema}_{claim}{field}_FirstLastMost"
    print(f"Using table {tableName}")
    df = (
    spark.table(f"clinical_research_systems.{inSchema}_{claim}{field}_FirstLastMost")
    .select(['personid', field])
    )
    
    return(df)

def extractConcepts(concept, description, tabulated_ontologies = ''):
    """
    Resolve a Discern concept to its canonical concept-code list per contextVersion.

    For a given ``conceptName``, reads
    ``{tabulated_ontologies}.conceptCodeConceptDesc``, groups codes by
    ``contextVersion``, deduplicates codes with identical code-lists
    (keeping the first via ``row_number == 1``), and joins against the
    ``description`` DataFrame to attach human-readable ``codingSystem``
    labels.

    Parameters:
        concept (str): Concept name to filter on (e.g. a single
            ``conceptName`` value like ``'SICKLE_CELL_ANEMIA_CLIN'``).
        description (DataFrame): A codingSystem description table with
            columns ``descriptionNumber`` and ``codingSystem``, joined
            onto the resolved concept codes.
        tabulated_ontologies (str): Fully qualified schema name containing
            ``conceptCodeConceptDesc`` (e.g. ``'tabulated_ontologies'``).

    Returns:
        DataFrame: Distinct ``contextVersion, conceptName, conceptCode,
        codingSystem`` rows sorted by all four columns.
    """
    w1 = Window.partitionBy('contextVersion', 'conceptName').orderBy('conceptCode')
    w2 = Window.partitionBy('contextVersion', 'conceptName').orderBy('conceptCode')

    uniqueConcepts = (
    spark.sql(f"""
    SELECT contextVersion, conceptName, descriptionNumber, conceptCode 
    FROM {tabulated_ontologies}.conceptCodeConceptDesc
    """)
    .filter(F.col('conceptName')==concept)
    .select(['contextVersion', 'conceptName','descriptionNumber', 'conceptCode'])
    .distinct()
    .withColumn('systemCode', F.array('conceptCode','descriptionNumber'))
    .select(['contextVersion', 'conceptName','systemCode'])
    .distinct()
    .withColumn('conceptCodeList',F.collect_list('systemCode').over(w1))
    .select(['contextVersion', 'conceptName', 'conceptCodeList'])
    .groupBy(['contextVersion', 'conceptName'])
    .agg(F.max('conceptCodeList').alias('conceptCodeList'))   # F.max, not Python max
    .select(['contextVersion', 'conceptName', 'conceptCodeList'])
    .withColumn('rank', F.row_number().over(w2))
    .filter(F.col('rank') == 1)
    .withColumn("conceptCodeSystem", F.explode(F.col('conceptCodeList')))
    .withColumn("conceptCode", F.col("conceptCodeSystem")[0])
    .withColumn("descriptionNumber", F.col("conceptCodeSystem")[1])
    .select(['contextVersion', 'conceptName', 'conceptCode', 'descriptionNumber'])
    .join(description, on = ['descriptionNumber'])
    .select(['contextVersion', 'conceptName', 'conceptCode', 'codingSystem'])
    .sort(['contextVersion', 'conceptName', 'conceptCode', 'codingSystem'])
    .distinct()
    )
    return(uniqueConcepts)

def calContextGroups(concept, tabulated_ontologies = ''):
    """
    Group equivalent contextVersions for a concept by shared code lists.

    Two contextVersions that resolve a concept to the same set of codes
    are functionally identical for extraction purposes. This function
    collapses them into ``contextGroup`` buckets so downstream code can
    pick one representative ``referenceContext`` per group instead of
    re-querying every version.

    Parameters:
        concept (str): ``conceptName`` to analyse.
        tabulated_ontologies (str): Fully qualified schema name holding
            ``conceptCodeConceptDesc``.

    Returns:
        pandas.DataFrame: Columns ``contextVersion, conceptName,
        contextGroup, referenceContext`` (where ``referenceContext == 1``
        marks the canonical version per group). Converted via
        ``.toPandas()`` because the result is small and used as a lookup
        in driver-side logic.
    """
    w1 = Window.partitionBy('contextVersion', 'conceptName').orderBy('conceptCode')
    w2 = Window.partitionBy('contextVersion', 'conceptName').orderBy('conceptCode')
    w3 = Window.partitionBy('conceptName').orderBy('contextVersion')
    
    # Correct
    # conceptName 	codingSystem 	conceptCode 	contexts
    groups = (spark.sql(f"""
    SELECT * FROM {tabulated_ontologies}.conceptCodeConceptDesc
    """)
     .filter(F.col('conceptName') == concept)
     .select(['contextVersion', 'conceptName','descriptionNumber', 'conceptCode'])
     .withColumn('systemCode', F.array('conceptCode','descriptionNumber'))
     .select(['contextVersion', 'conceptName','systemCode'])
     .distinct()
     .withColumn('conceptCodeList',F.collect_list('systemCode').over(w1))
     .select(['contextVersion', 'conceptName', 'conceptCodeList'])
     .groupBy(['contextVersion', 'conceptName'])
     .agg(F.max('conceptCodeList').alias('conceptCodeList'))     # F.max, not Python max
     .withColumn('sizeCodeList', F.size(F.col('conceptCodeList')))
     .select(['contextVersion', 'conceptName', 'conceptCodeList', 'sizeCodeList'])
     .withColumn('row_number', F.row_number().over(w2))
     .withColumn('referenceContext', (F.col('row_number') == 1).cast('integer'))
     .withColumn('contextGroup', F.sum(F.col('referenceContext')).over(w3))   # F.sum, not Python sum
     #.filter(F.col('contextVersion') == '0054')
     .sort(['conceptName', 'contextGroup', 'contextVersion', 'referenceContext'])
     .select(['contextVersion','conceptName',  'contextGroup', 'referenceContext'])
     #.filter(F.col('referenceContext') == 1)
     #.filter(F.col('rank') == 1)
     .distinct()
     .toPandas()
    )
    return(groups)

def updateTableDt(inTable, codefield, currentTableName, updateDt, tabulated_ontologies = ''):
    """ This doesn't work yet, use updateTable
    """
    
    currentTable1 = (
        spark.sql(f""" SELECT * FROM {tabulated_ontologies}.{currentTableName}""")
              .filter(F.col("tableName")  != inTable)
              .filter(F.col("codefield")  != codefield)
             )
    
   
    currentTable = currentTable1.select("*")  # Trick to get schema even if empty
    unionTable = currentTable.union(updateDt)
    
    unionTable.write.mode("overwrite").saveAsTable(f"{tabulated_ontologies}.temp")
    unionTable2 = spark.sql(f"SELECT * FROM {tabulated_ontologies}.temp")
    
    (unionTable2
     .write
     .mode("overwrite")
     .saveAsTable(f"{tabulated_ontologies}.{currentTableName}")
    )
    newTable = (
        spark.sql(f""" SELECT * FROM {tabulated_ontologies}.{currentTableName}""")
              .filter(F.col("tableName")  != inTable)
              .filter(F.col("codefield")  != codefield)
              .toPandas()
             )

    return(newTable)

def updateTable(inTable, codefield, currentTableName, update, dateGroupingFields = []
                , transform = ['initialize', 'update', 'insertInto']
                , convertToPandas = True, verify = False , tabulated_ontologies = ''):
    """
    Update or Replace current items in the pyspark datatable
    Args:
       replace:  Should the items be replace, requiring reading in the entire table or just added to
    """
    
    if transform == 'update':  # Update is not supported so this is a workaround
        currentTable = (
            spark.sql(f""" SELECT * FROM {tabulated_ontologies}.{currentTableName}""")
            .join(update.select(['tableName', 'codefield', *dateGroupingFields]).distinct()
                  ,on = ['tableName', 'codefield', *dateGroupingFields]
                  ,how = 'left_anti')
            .select("*")
        )
        updatedTable = (
            currentTable
            .union(update.select(currentTable.columns))
        )
        
        updatedTableSchema = updatedTable.schema
        updatedTablePd     = updatedTable.toPandas()
        
        (spark.createDataFrame(updatedTablePd, schema = updatedTableSchema)
         .write.mode("overwrite")
         .saveAsTable(f"{tabulated_ontologies}.{currentTableName}")
        )
        
    if transform == 'insertInto':  # Insert Into
        (update
         .write
         .insertInto(f"""{tabulated_ontologies}.{currentTableName}""",overwrite=False)
        )
           
    if transform == 'initialize':
        (
            update
            .write
            .mode("overwrite")
            .saveAsTable(f"""{tabulated_ontologies}.{currentTableName}""")
        )
        
    if verify:
        newTable = (
            spark.sql(f""" SELECT * FROM {tabulated_ontologies}.{currentTableName}""")   
            .join(update.select(['tableName', 'codefield', *dateGroupingFields]).distinct()
                  ,on = ['tableName', 'codefield', *dateGroupingFields]
                  ,how = 'inner')
            .select("*")
        )
    else:
        newTable = update
        
    if convertToPandas:
        return(newTable.toPandas())
    else:
        return(newTable)
    
def findCrosswalk(tableCodingSystem, ontCodingSystem):
    """Identify how the coding systems in a table match the coding systems in standard_ontologies.ontologies
    @param     codingSystemId The codingSystemId from a table
    @param     ontCodingSystemId The codingSystemId from standard_ontologies.ontologies
    @tableName The source table of the codingSystemId parameter.
    @codefield The fields of table used by the Discern UDF to extract concepts.
    """
    fieldOnt   = 'codingSystemId'
    fieldData  = f'standard_codingSystemId'
    system     = (
        tableCodingSystem.dropna().copy()
    )
    # Find the codingSystemId in the ontology that matches the codingSystemId in the table
    system[fieldOnt] = (
        system[fieldData]
        .apply(lambda x: difflib
               .get_close_matches(x, ontCodingSystem[fieldOnt],10)
              )
    )
    system = system.explode(fieldOnt).dropna()
    
    if not system.empty:
        i = system.apply(lambda x: bool(re.search(f'{x[fieldData]}$', x[fieldOnt])), axis = 1)
        result = (
            system
            .loc[i][['tableName', 'codefield', fieldData, fieldOnt, 'codingSystemCount']]
            .sort_values(fieldData, ascending = False)
        )
    else:
        result = system[['tableName', 'codefield', fieldData, fieldOnt, 'codingSystemCount']]
        
    return(result)

def check_sample_and_ontology(inTable, table_sample, datefield
                              , ontology = 'tabulated_ontologies.context_concept_table_code'
                             , tabulated_ontologies = ''):
    """
    Print a sample summary and return top ontology coverage for a table.

    Two-step diagnostic: (1) prints obs/person counts and date range for
    ``table_sample`` as a sanity check, (2) if the ontology coverage
    table exists, returns the top 40 (contextId, conceptName, code) rows
    by coverage percent for the given ``inTable``.

    Parameters:
        inTable (str): Name of the source table to filter ontology rows by.
        table_sample (str): Fully qualified table path to sample counts
            from (e.g. ``'dataset.encounterSource_sample'``).
        datefield (str): Date column in ``table_sample`` used for min/max
            reporting.
        ontology (str, optional): Name of the coverage ontology table.
            Defaults to ``'tabulated_ontologies.context_concept_table_code'``.
        tabulated_ontologies (str): Fully qualified schema name holding
            the ontology table.

    Returns:
        pandas.DataFrame | None: Top 40 coverage rows, or None if the
        ontology table does not exist.
    """
    r = (spark.sql(f"""
                SELECT count(*) as `Obs Count`
                ,count(DISTINCT PersonId) as `Person Count`
                ,MIN(F.to_date({datefield})) as `Min Date`
                ,MAX(F.to_date({datefield})) as `Max Date`
                FROM {table_sample}
                """)
             .toPandas()
    )
    print(r)
    
    # database_exists takes a single positional database_name argument
    # (no `db=` / `table=` kwargs). Check the database first; if the
    # specific ontology table is missing, the SHOW TABLES / sql below
    # will surface the gap.
    if database_exists(tabulated_ontologies):
        r = (spark.sql(f"""
        SELECT DISTINCT contextId, contextName, conceptName, table, code, n, count, percent
        FROM {tabulated_ontologies}.{ontology}
        WHERE table == '{inTable}'
        AND percent > 0
        """)
             .sort(F.col('percent').desc())
             .limit(40)
             .toPandas()
            )
        return(r)
    
def select_top_contextId(DF):
    """
    Restrict DF to the single most-populated contextId.

    The Discern ontology API only accepts one ``contextId`` at a time, so
    when multiple contexts cover a concept, we pick the one with the most
    distinct ``(conceptName, contextId)`` rows and filter the DataFrame
    to that context. Used upstream of ``get_ontology_codes``.

    Parameters:
        DF (DataFrame): Spark DataFrame with at least the columns
            ``conceptName`` and ``contextId``.

    Returns:
        pandas.DataFrame: The subset of ``DF`` restricted to the single
        most-populated ``contextId``, materialised via ``.toPandas()``
        (expected to be small).
    """
    result = (
        DF
        .select(['conceptName', 'contextId'])
        .distinct()
        .groupby('contextId')
        .count()
        .sort(F.col('count').desc())
        .limit(1)
        .join(DF, ['contextId'], 'inner')
        .drop('count')
        .toPandas()
    )
    return(result)

def get_ontology_codes(ontology, name_regex, inTable, code
                       , standard_ontologies = 'standard_ontologies'
                       , fields              = ['contextId','codingSystemId','conceptName','conceptCode']
                       ,system_regex = '.*'
                      ):
    """
    Given a DF with a target conceptName and codingSystemId, get the actual source codes.
    @param: ontology 
    """
    DF = (
        ontology
        .filter(F.col('conceptName').rlike(name_regex))
        .filter(F.col('table') == inTable)
        .filter(F.col('code').isin(code))   # F.column is not a thing — use F.col
    )
    # Identify the context with the most levels of context
    # The ontology API only takes on context, so make it the most populated one
    target_ontology = (
        DF
        .select(['conceptName', 'contextId'])
        .distinct()
        .groupby('contextId')
        .count()
        .sort(F.col('count').desc())
        .limit(1)
        .select(['contextId'])
        .join(DF, ['contextId'], 'inner')
        .select(['contextId', 'conceptName', 'table', 'code', 'count', 'n', 'percent'])
    )
    print(target_ontology.columns)
    
    result = (
        spark.table(f"{standard_ontologies}.ontologies")
        .filter(F.col('codingSystemId').rlike(system_regex))
        .join(target_ontology, on = ['contextId', 'conceptName'], how = 'inner')
        .select(fields)
        .distinct()
        .sort(['conceptCode'])
    )
    return(result)

def add_concept_indicators(conceptName, code, tag = ""):
    """
    Return a closure that adds boolean indicator columns for each concept.

    For each name in ``conceptName``, the returned transform appends a new
    boolean column to its input DataFrame, set to ``True`` when the row's
    ``code`` value matches the concept under Discern's ``has_concept()``
    UDF and ``False`` otherwise.

    Parameters:
        conceptName (list[str]): Concept names to evaluate (e.g.
            ``['SICKLE_CELL_ANEMIA_CLIN', 'THALASSEMIA_CLIN']``). Each
            name becomes the name of a new column on the output DataFrame.
        code (str): Name of the column on the input DataFrame that holds
            the coded value to test (unquoted — it is substituted into the
            SQL ``has_concept()`` call verbatim).
        tag (str, optional): Currently unused; reserved for prefixing new
            column names. Defaults to ``""``.

    Returns:
        Callable[[DataFrame], DataFrame]: A transform that applies all
        concept indicators in one pass when called on a Spark DataFrame.
    """
    def inner(df):
        for concept in conceptName:
            new_name = concept
            #new_name = "pre_" + concept
            df = df.withColumn(
                new_name,
                F.expr(f"if(has_concept({code},'{concept}'), True, False)")
            )
        return(df)
    return inner

