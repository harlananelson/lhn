"""
Extract Module for HealthEIntent Data Processing
===================================================

This module provides a framework for extracting, filtering, and analyzing healthcare data
through a three-step workflow designed for analyzing patient encounters, diagnoses,
medications, labs, and other healthcare events.

Workflow Overview
----------------
The typical workflow involves three sequential steps, each using a different method:

1. **create_extract**: Identifies codes or index values of interest from a reference table
   - Uses regex patterns or exact matching to find relevant codes
   - Creates a subset table containing only the codes/indices of interest
   - Example: Finding specific medication codes from a reference dictionary

2. **entityExtract**: Retrieves actual entity records using the identified codes
   - Joins the codes identified in step 1 with a source data table
   - Filters by date ranges if specified
   - Creates a table of actual patient encounter records
   - Example: Finding all medication administrations using the codes from step 1

3. **write_index_table**: Creates a cohort-style table with "first instance" information
   - Identifies the first (and optionally last) occurrence of events for each patient
   - Calculates continuous periods of care or therapy
   - Creates a patient-level summary table
   - Example: Determining when each patient first received a specific medication

Configuration via YAML
---------------------
Each step requires a configured object with specific properties, typically defined
in a YAML configuration file (e.g., 000-control.yaml). Required properties include:

For create_extract objects:
- label: Description of what this extract represents
- listIndex: Field containing search patterns
- sourceField: Field to search in the source table
- indexFields: Fields that uniquely identify each record
- dictionary (optional): Key-value pairs of categories and their code patterns

For entityExtract objects:
- label: Description of entity records
- datefield: Field for date filtering
- histStart/histEnd: Date boundaries (optional)
- indexFields: Fields to join with on
- retained_fields: Fields to keep in output

For write_index_table objects:
- label: Description of the index table
- indexFields: Entity identifier fields (e.g., patient ID)
- datefieldPrimary: Date field to identify first instance
- code: Tag for resulting index fields
- retained_fields: Additional fields to keep
- max_gap: Maximum gap in days to consider continuous therapy

Usage Example
------------
# Step 1: Identify medication codes of interest
drug_codes.create_extract(
    elementList=d.medication_administration_drugcode,
    elementListSource=d.medication_administration_drugcode.df,
    find_method='regex'
)

# Step 2: Extract medication encounters using the codes
medication_encounters.entityExtract(
    elementList=drug_codes,
    entitySource=r.medicationSource.df,
    cacheResult=True
)

# Step 3: Create index table with first medication instance
medication_encounters_index.write_index_table(
    inTable=medication_encounters
)

# Step 4: Analyze attrition at each step
drug_codes.attrition()
medication_encounters.attrition()
medication_encounters_index.attrition()

Key Classes
----------
- Extract: Container class that holds a collection of ExtractItem objects
- ExtractItem: The main class for data processing, with methods for extraction and analysis

Notes
-----
- Most methods support filtering by date ranges and cohort subsetting
- The system is designed to handle large healthcare datasets efficiently
- Intermediate results are typically saved to tables that can be referenced later
"""

from foresight.discern import push_discern
from lhn.shared_methods import SharedMethodsMixin
from lhn.introspection_utils import coalesce, set_default_params
from lhn.header import re, F, StringType, spark, time, pprint, pd, copy
from lhn.cohort import identify_target_records, write_index_table
from lhn.data_display import print_pd, showIU
from lhn.data_summary import attrition
from lhn.data_transformation import write_sorted_index_table
from lhn.file_operations import put_to_hdfs
from lhn.function_parameters import setFunctionParameters
from lhn.pandas_utils import dict2Pandas
from lhn.spark_query import add_parsed_column
from lhn.spark_utils import writeTable
from lhn.list_operations import noColColide, unique_non_none
from lhn.query import extract_fields_flat_top, query_flat_rwd
import unicodedata
from pprint import pformat
from lhn.features import select_only_baseline, analyze_clinical_measurements
from lhn.header import get_logger
from lhn.list_operations import escape_and_bound_dot, escape_and_bound_dot_udf

logger = get_logger(__name__)


class Extract:
    """Given the dictionary indicating data table properties, 
       create an object with a property for each table listed.
       These are the list of tables created by a project.
    """
    def __init__(self, proj, debug=False):
        for key, value in proj.items():
            setattr(self, key, ExtractItem(value))
            
    def properties(self):
        return([item for item in self.__dict__.keys()])
    
class ExtractItem(SharedMethodsMixin):
    """
    Purpose: Create data extracts
    """
    def __init__(self, item, debug = False):
        # copy the attributes of item, which are from the config YAML file.
        self.__dict__ = item.__dict__.copy()  # copy the attributes of item
        self.debug = debug
        if 'partitionBy' not in self.__dict__:
            self.partitionBy = None
        # Log inherited csv and parquet paths for debugging
        if debug:
            if hasattr(self, 'csv'):
                logger.info(f"Inherited csv from item: {self.csv}")
            else:
                logger.warning(f"No csv attribute inherited from item")
            if hasattr(self, 'parquet'):
                logger.info(f"Inherited parquet from item: {self.parquet}")
            else:
                logger.warning(f"No parquet attribute inherited from item")

    def properties(self):
        return [item for item in self.__dict__.keys()]

    def dict2pyspark(self, columnname = ['codes']):
        self.pd = dict2Pandas(self.dictionary, columnname = columnname)
        self.pd = self.pd.reset_index().rename(columns={'index': 'group'})
        self.df = spark.createDataFrame(self.pd)
        
    def load_csv_as_df_from_hdfs(self, filename = 'temp'):
        if hasattr(self, 'csv'):
            local_file_path = self.csv  
            filename = f'{filename}_{int(time.time())}.csv'
            hdfs_file_path = filename
            put_to_hdfs(local_file_path, hdfs_file_path)
            self.df = spark.read.csv(filename, inferSchema=True, header=True)
            writeTable(self.df, outTable=self.location, description=self.label, partitionBy = self.partitionBy)
            self.df = spark.table(self.location)
            


    def load_csv_as_df(self, **kwargs):
        """
        Read in a csv file as a DataFrame and save it as a table in the specified location.
        subset: if there are missing values, the subset of columns that have missing and non missing value
        thresh: the number of non missing values in the subset of columns
        dtype: the data type of the columns
        drop_missing: if True, drop the rows with missing values
        how: how to drop the missing values
        inplace: if True, do the operation in place
        How to call using the kwargs parameter
        load_csv_as_df(dtype=None, names=None, header=0, drop_missing=False, how='any', subset=None, thresh=None, inplace=False, normalize_unicode=True
        
        """
        params = ['dtype', 'names', 'header', 'drop_missing', 'how', 'subset', 'thresh', 'inplace', 'normalize_unicode']
        defaults = {'dtype': None, 'names': None, 'header': 0, 'drop_missing': False, 'how': 'any', 'subset': None, 'thresh': None, 'inplace': False, 'normalize_unicode': True}
        kwargs = set_default_params(self, params, defaults, **kwargs)
        dtype           = kwargs['dtype']
        names           = kwargs['names']
        header          = kwargs['header']
        drop_missing    = kwargs['drop_missing']
        how             = kwargs['how']  # default is 'any'
        subset          = kwargs['subset']
        thresh          = kwargs['thresh']
        inplace         = kwargs['inplace']
        normalize_unicode = kwargs['normalize_unicode']
        
        if header is False: header = None 
        # and so on for the rest of the parameters
        # Now you can use kwargs['dtype'], kwargs['drop_missing'], etc. in your function
        if hasattr(self, 'csv'):
            print(f"calling: pd.read_csv({self.csv}, dtype={dtype}, header={header}, names = {names})" )
            pandas_df = pd.read_csv(self.csv, dtype=dtype, header=header, names = names, skipinitialspace=True)

            if normalize_unicode:
                for column in pandas_df.columns:
                    pandas_df[column] = pandas_df[column].apply(lambda s: unicodedata.normalize('NFC', str(s)))

            # Create a temporary DataFrame with missing values dropped
            print(f"calling: pandas_df.dropna(how={how}, subset={subset}, thresh={thresh}, inplace={inplace})")
            temp_df = pandas_df.dropna(how=how, subset=subset, thresh=thresh, inplace=inplace)

            # Infer schema from the temporary DataFrame
            temp_spark_df = spark.createDataFrame(temp_df)
            schema = temp_spark_df.schema

            # Converting pandas DataFrame to Spark DataFrame
            self.df = spark.createDataFrame(pandas_df, schema=schema)

            # Collect the DataFrame as a list and parallelize it to break lineage
            data_list = self.df.collect()
            rdd = spark.sparkContext.parallelize(data_list)
            new_df = spark.createDataFrame(rdd, schema)
            
            if drop_missing:
                self.df = temp_spark_df
            else:
                self.df = new_df

            # Optional: Save the DataFrame as a table, if you have defined 'location' and 'label'
            if self.location and self.label:
                self.df.write.saveAsTable(name=self.location, mode='overwrite', description=self.label)

            # Optional: Load the DataFrame back from the table, if you have defined 'location'
            if self.location:
                self.df = spark.table(self.location)

    def push_discern(self, config_dict):
        funCall = {
            'spark_session'    : spark ,
            'discern_context'  : self.discern_contextID,
            'concepts'         : self.discern_concepts
        }
        push_discern_param = setFunctionParameters(push_discern, funCall, config_dict = config_dict)
        print(f"push_discern with {push_discern_param}")
        push_discern(**push_discern_param)

    def write_sorted_index_table(self, inTable, indexLabel = "index", lastLabel = "last", sort_order = None):
        
        self.df = write_sorted_index_table(inTable, self.indexFields, self.retained_fields,  self.datefieldPrimary,
                        self.code, indexLabel = indexLabel, lastLabel = lastLabel, sort_order = sort_order)
        
    
        
    def write_index_table(self, inTable, histStart = None, histEnd = None, 
                          indexLabel = "index", lastLabel = "last",
                          filterSimple = True):
        """
        Identify the first record by date.
        self: An object with properties
           - df: a data table to be populated
           - indexFields: The entity, observational unit or person identifier
           - retained_fields: Fields from the `inTable` to be retained in addition to the indexFields and datefieldPrimary
           - code: A tag used to name the resulting index fields
           - sort_fields: The fields used to sort the data table
           - datefieldPrimary: The date field (list) used to identify the first record
           - histStart: lower bound of datefield Primary
           - histEnd: upper bound of datefield Primary
           - max_gap: The maximum gap between dates
        inTable: The source table with encounters.
        indexLabel: prefix used for the index date field
        lastLable: prefix used to indicate the last date field
        """
        
        histStart = coalesce(histStart, self.histStart)
        histEnd   = coalesce(histEnd, self.histEnd)
        
        histStart = histStart if histStart else self.histStart if hasattr(self, 'histStart') else None
        histEnd = histEnd     if histEnd   else self.histEnd   if hasattr(self, 'histEnd')   else None
        self.datefieldStop = unique_non_none(self.datefieldStop, self.datefieldPrimary)[0]
        
        """
        if filterSimple:
            if histStart and histEnd:
                # Apply the filter
                df = inTable.df.filter(F.col(inTable.datefieldPrimary[0]).between(histStart, histEnd))
        else:
            if histStart and histEnd:
                # Create a list of conditions
                conditions = [F.col(col).between(histStart, histEnd) for col in inTable.datefieldPrimary]

                # Combine the conditions with OR
                condition = F.reduce(lambda a, b: a | b, conditions)

                # Apply the filter
                inTable = inTable.filter(condition)
        """
        
        # if inTable is and Extrac Object, use the df property, otherwise use the inTable directly
        if hasattr(inTable, 'df'):
            inTabledf = inTable.df
        else:    
            inTabledf = inTable
            
        self.df = write_index_table(inTable          = inTabledf, 
                                    index_field      = self.indexFields, 
                                    retained_fields  = self.retained_fields, 
                                    datefieldPrimary = self.datefieldPrimary,
                                    datefieldStop    = self.datefieldStop,
                                    code             = self.code, 
                                    sort_fields      = self.sort_fields, 
                                    histStart        = histStart, 
                                    histEnd          = histEnd,
                                    max_gap          = self.max_gap,
                                    indexLabel       = indexLabel, 
                                    lastLabel        = lastLabel)
        
    def query_flat_rwd(self, source, schema):
        funCall = {
            'DF'                   : source
            , 'fields'             : self.retained_fields
            , 'datefieldPrimary'   : self.datefieldPrimary
            , 'datefields'         : self.datefields
            , 'explode_fields_post': []
            , 'write'              : True
            , 'outTable'           : self.location
            , 'conceptName'        : self.discern_concepts
            , 'add_concepts'       : False        # Should indicators for the concepts be added?
            , 'filter_concepts'    : True         # Should observations be filtered to the concepts indicated?
            , 'cohort'             : self.cohort        # Are observations being extracted for a pre-defined cohort?
            , 'code'               : self.discern_codefield   # The field used by discern to identify concepts. 
            , 'tag'                : ""           # An extra tag to put at the end of the concepts to keep from clobbering.
            , 'write_index'        : False
            }
        query_flat_rwd_concepts_param = setFunctionParameters(query_flat_rwd, funCall, config_dict = {})
        # printParameters(query_flat_rwd_concepts_param)
        self.df = query_flat_rwd(**query_flat_rwd_concepts_param)
        return(self.df)
    
    
    def values(self):
        pprint.pprint(self.__dict__)
    
   


    
    def create_extract(self, elementList, elementListSource, usePandas = True,
                       find_method = 'regex', sourceField = None):
        """
        Use a list of elements to find items in the elementListSource.
        find_method = 'regex' or 'merge'
        - Merge the list of codes with the data set
        - Use the regular expressions given in the column `listIndex` of `elementList` to scan the `display` columns of elementListSource.
        Types of elementList:
        - list: 
         - if find_method = 'regex', seach the elementListSource field sourceField using regular expessions in elementList
         - if find_method = 'merge', create a table out of elementList then merge the elementListSource with elementList
        - dictionary
         - Same as list, but add the key as a grouping column
        - ExtractItem
         - if find_method = 'regex', seach the elementListSource field sourceField using regular expessions in elementList
         - if find_method = 'merge', merge the table df property of elementList with elementListSource
        Use the regular expressions given in the column `listIndex` of `elementList` to scan the `display` columns of elementListSource.
        A new column called `listIndex` (same name) will be added to resulting table (a subset of elementListSource corresponding to the
        records that match the listIndex expressions).
        
        Args:
            self 
                - groupName: Name of grouping column in the output (self) data set that indicates membership by search term in elementList field listIndex
                            Must not be the same name as sourceField or listIndex.
                            self.groupName <- regex_replace(elementList.listIndex)
            
            elementList (Union[ExtractItem, List[str]]): An object of class ExtractItem or a list. Used to create the list `pattern`.
                - If `elementList` is an object of class ExtractItem:
                    - listIndex: The field in elementList that contains the regex search pattern
                    - sourceField: The field in the data dictionary elementListSource that the regex search pattern should match
                    - complete = True:  If the search string is complete and can be ended with a $
                - If `elementList` is a list:
                    - listIndex: The field in elementList that contains the regex search pattern
                    - df
            
            elementListSource (DataFrame): A Spark DataFrame containing elements to search as one of the fields.
            
            sourceField (str, optional): A field of elementListSource that contains the text to search.
            findMethod: regex: use a regex search. merge, merge on the index field
            
        Returns:
            DataFrame: The resulting DataFrame with the applied filters and additions.
        """
        
        
        
        
        
        sourceField     = ( sourceField if sourceField is not None  
                           else self.sourceField if hasattr(self, 'sourceField')
                           else elementList.sourceField if hasattr(elementList, 'sourceField')
                           else None)

        funCall = {
            'elementList'        : elementList,
            'elementListSource'  : elementListSource,
            'sourceField'        : sourceField,
            'usePandas'          : usePandas,
            'find_method'        : find_method  ,
            'sourceField'        : sourceField
        }
        logger.info(f"create_extract with \n {pformat(funCall)} \n")
        
        if type(elementList) == list: # Use list to search elemtnListSource
            # Given a list of regular expressions instead of a data table of regular expressions
            if len(elementList) > 0:
                list_index_values = copy(elementList)
                # Replace '.x' with wildcard character '.*'
                pattern = "|".join(['[a-z]*' + re.sub(r'\.x', '\.*', name) + '[a-z]*' for name in elementList])
            
        elif find_method == 'merge':  # Merge elementList with elementListSource by self.listIndex
            selectIndexFields     = noColColide( elementListSource.columns, elementList.df.columns,index = self.listIndex, masterList=self.listIndex)
            keepElementListFields = noColColide( elementListSource.columns, elementList.df.columns,index = self.listIndex, masterList=None)
            elementIndex          =  elementList.df.select(keepElementListFields).join(elementListSource, on = self.listIndex, how = 'inner')
            
        elif find_method == 'regex':  
            logger.info(f"find_method == 'regex' \n elementList: {elementList} \n type(elementList): {type(elementList)} \n")
            if type(elementList) == ExtractItem:
                if hasattr(elementList, 'df'):
                    # prepare the list of codes to be regular expression strings
                    logger.info(f"count of elementList.df {elementList.df.count()} \n")
                    if usePandas:  # Faster than pyspark is table is small
                        logger.info(f"Using Pandas To create Regex List \n")
                        elementList_df = elementList.df.toPandas()
                        if type(elementList.listIndex) == list:
                            logger.info(f"elementList.listIndex is and array not a scalar, converting to scalar")
                            elementList.listIndex = elementList.listIndex[0]
                        elementList_df[self.groupName] = elementList_df[elementList.listIndex].apply(lambda x: escape_and_bound_dot(x, elementList.complete))
                        self.list_index_values = elementList_df[self.groupName].tolist()
                        self.pattern = "|".join(self.list_index_values)
                        logger.info(f"converting elementlist_df to pandas, \n elementList.df.columns:{elementList.df.columns} \n elementList_df.columns{elementList_df.columns}  \n")
                        df = spark.createDataFrame(elementList_df)
                        
                    else:
                        print(f"Using Pyspark To create Regex List")
                        # Apply the UDF to the DataFrame column
                        df = elementList.df.withColumn(self.groupName, escape_and_bound_dot_udf(F.col(elementList.listIndex), F.lit(elementList.complete))).cache()
                        print(f"count of elementList.df  at first step: {df.count()} \n")
                        # create a list of all the search strings
                        self.list_index_values = df.select(self.groupName).rdd.flatMap(lambda x: x).collect()   # Collect listIndex values
                        # Create one regular expression of the list of regular expressions.
                        self.pattern = "|".join(self.list_index_values)  # Create regex pattern
                else:
                    print(f"elementList does not have a data frame")
                    self.list_index_values = elementList.elementList  # Use elementList directly
                    self.pattern = "|".join(['[a-z]*' + re.sub(r'\.x', '.*', name) + '[a-z]*' for name in elementList.elementList])
                    print(self.pattern)
            logger.info(f"searching column {sourceField} in elementListSource using regex pattern: {self.pattern} \n")
            selectedRows =  elementListSource.filter(F.col(sourceField).rlike("(?i)" + self.pattern))
            
            print(f"count of selectedRows  after selecting from Source by regex pattern: {selectedRows.count()} \n")
            elementIndex_1 = (
                add_parsed_column(selectedRows, primaryDisplay=sourceField, elementList=self.list_index_values, value_column=self.groupName)
                    .filter(F.col(self.groupName) != '')
            )
            try:
            #print(f"count of elementIndex_1  after add_parsed_column: {elementIndex_1.count()} \n")
                selectIndexFields = noColColide( df.columns,elementIndex_1.columns, index = [self.groupName], masterList=None)
                print(f"joining with df {df.columns} on {self.groupName}")
                elementIndex = (
                    elementIndex_1
                .join(df.select(selectIndexFields), on = [self.groupName], how = 'inner')
                .distinct()
                .cache()
                )
            except:
                elementIndex = elementIndex_1
        
        # Verify that the elementIndex has records
        elementIndex_count = elementIndex.count()
        logger.info(f"count of elementIndex  after rejoining with elementList: {elementIndex_count} \n")
        if elementIndex_count == 0:
            logger.warning(f"create_extract: {self.label} has no records. Exiting. \n")
            return None
        
  
        try:
            self.df = elementIndex.distinct().sort(F.col('Subjects').desc())  # Use elementIndex directly
        except:
            self.df = elementIndex.distinct()
                
        print(f"writing to {self.location}: {self.label}")
        writeTable(self.df, outTable=self.location, description=self.label, partitionBy = self.partitionBy)
        self.df = spark.table(self.location)
        print(f"count of self.df reread: {self.df.count()} \n")
        print(f"writing csv to {self.csv}")
        self.df.toPandas().to_csv(self.csv, index = False)
        return(self.df)
 


            
    def entityExtract(self, elementList, entitySource, cohort = None, cacheResult = False, cohortColumns = None, 
                      howjoin = 'inner', howCohortJoin = 'inner', broadcast_flag = False, ):
        
        """
            Extracts entity records from an entity source using a list of elements (keys). 

            This function uses the provided elementList to identify records in the entitySource. 
            If a cohort is provided, the function performs additional processing to handle the cohort.
            If fieldList is provided, only the columns in fieldList will be included in the final DataFrame.

            Parameters:
            - self (Extract object): The Extract object calling this method. 
                - self.datefield (str, optional): A date field in the source table. Defaults to None.
                - self.histStart (str, optional): The first date from the source or offset from the elementList datefield. Defaults to None.
                - self.histEnd (str, optional): The last date from the source or offset from the elementList datefield. Defaults to None.
                - self.fieldList (list, optional): A list of field names selected from entitySource. If not provided, all columns are included. Defaults to None.
                - self.cohortColumns (list, optional): A list of column names to use when a cohort is provided. Defaults to None.
            - elementList (Extract object): Contains of table of the index values used to select rows in the entitySource. All columns of the table will be retained
                - elementList.indexFields (list): A list of index fields used to select records in the source.
                - elementList.df (DataFrame): The data frame.
                - elementList.datefield (str, optional): A date field in the element list indicating a time anchor. Defaults to None.
            - cohort
                - indexFields (list): A list of index fields used to join the cohort to the entitySource.
                - cohortColumns (list): A list of column names to use when a cohort is provided. If not provided, all columns of cohort.df are used.
            - entitySource (DataFrame): The source data frame from which to extract entity records.
            - cohort (Cohort object, optional): A cohort to use for additional processing. Defaults to None.
            - cacheResult (bool, optional): Whether to cache the result. Defaults to False.
            - cohortColumns (list, optional): A list of column names to use when a cohort is provided. If not provided, self.cohortColumns or all columns of cohort.df are used. Defaults to None.

            Returns:
            - DataFrame: A DataFrame containing the extracted entity records.
            """
        
        
        logger.info(f"entityExtract: {self.label} \n")
        for attr in ['datefield', 'histStart', 'histEnd']:
            if not hasattr(self, attr):
                setattr(self, attr, None)
            else:
                pass
            logger.info(f'self.{attr}: {getattr(self,attr)} \n')
                
        for attr in ['datefield']:
            if not hasattr(elementList, attr):
                setattr(elementList, attr, None)
            else:
                pass
            logger.info(f'elementList {attr}:  {getattr(elementList,attr)} \n')
                
        if hasattr(self, 'fieldList'):
            masterList = self.fieldList
        else:
            masterList = None
                
        logger.info(f"masterList is {masterList} \n")
        
        if cohort:
            logger.info(f"Using a Cohort \n")
            logger.info(f"entitySource.columns {entitySource.columns} \n")  
            logger.info(f"cohort.indexFields,  {cohort.indexFields} \n")
            
            if howCohortJoin == 'right':
                howjoin = 'left'  # this using in find_target_records where the cohort is entered first.
            logger.info(f"howCohortJoin: {howCohortJoin} \n")
            # If cohortColumns parameter is not provided, use the existing property or all columns of cohort.df
            if cohortColumns is None:
                if hasattr(self, 'cohortColumns'):
                    cohortColumns = noColColide(self.cohortColumns, [],cohort.indexFields, masterList=self.cohort.df.columns)
                else:
                    cohortColumns = cohort.df.columns
                    
            
            logger.info(f"cohortColumns:  {cohort.df.columns} \n")

            # Select the columns from entitySource that are in the cohort
            entitySourceSelect = noColColide(entitySource.columns, cohortColumns, cohort.indexFields, masterList=None)
            logger.info(f"entitySourceSelect: {entitySourceSelect} \n")
            logger.info(f""" SQL:
                         entitySource = (           
                            entitySource.select({entitySourceSelect})
                            .join(cohort.df.select({cohortColumns}), on = {cohort.indexFields}, how = "{howCohortJoin}")
                             )
                         \n""")
            entitySource = (
                entitySource.select(entitySourceSelect)
                .join(cohort.df.select(cohortColumns), on = cohort.indexFields, how = howCohortJoin)
            )
      
        
        callFun = {
            'entitySource':  entitySource,
            'elementIndex': elementList.indexFields,
            'elementExtract': elementList.df,
            'datefieldSource': self.datefield,
            'histStart': self.histStart,
            'histStop': self.histEnd,
            'datefieldElement': elementList.datefield,
            'cacheResult': cacheResult,
            'masterList': masterList,
            'broadcast_flag': broadcast_flag,
            'howjoin': howjoin
            
        }
        
        logger.info(f"Call identify_target_records(\n **{pformat(callFun)} )\n")
        logger.info(f"self.df = identify_target_records(**callFun) \n")
        self.df = identify_target_records(**callFun)
        
        logger.info(f"check to se if self.df has observations:  \n")
        if self.df.limit(5).count() == 0:
            logger.warning(f"entityExtract: {self.label} has no records. Exiting. \n")
            return None
        
        logger.info(f"writeTable({self.df}, outTable={self.location}, description={self.label}, partitionBy = {self.partitionBy})")
        writeTable(self.df, outTable=self.location, description=self.label, partitionBy = self.partitionBy)
        self.df = spark.table(self.location)
        return(self.df)
        
    def load_result_df(self):
        """
        Reads the DataFrame saved at self.location and sets it as the attribute self.result_df.
        """
        
        # The read_table function is a placeholder and needs to be replaced with actual function.
        self.df = spark.table(self.location)
        
    def countLevels(self, table, obs=30, index=['personId'], toPandas=False, custom_i=None, countfield: str = "Subjects"):
        if custom_i is None:
            i = self.root_columns
        else:
            i = custom_i

        setattr(self, 'df', extract_fields_flat_top(table=table.df, i=i, index=index, obs=obs, 
                                        toPandas=toPandas,countfield=countfield).cache())

    def writeTBL(self):
        if self.df.rdd.isEmpty():
            print("DataFrame is empty. Skipping write operation.")
        else:
            writeTable(self.df, self.location, description=self.label, partitionBy = self.partitionBy)
            self.df = spark.table(self.location)
        
    
    def to_csv(self):
        self.df.toPandas().to_csv(self.csv, index = False)
        
    def set_df_from_table(self, table):
        self.df = table
        
    def extractDemo(self, current, demo):
        """
        Extract demographic information from an index table representing the first touch of a cohort.
        
        Parameters:
        - self: An object with properties:
            - df (DataFrame): Target data table to which results will be assigned.
            - filterAge (bool): Indicator determining if age needs to be filtered.
            - ageMin (float): Minimum age for filtering.
            - ageMax (float): Maximum age for filtering.
            - label (str): Description label.
            - location (str): Schema and table name in the format 'schema.table'.
        - current: An object representing the extraction context with properties:
            - df (DataFrame): PySpark data table.
            - datefield (str): Name of the date field.
        - demo: An object containing demographic data properties:
            - df (DataFrame): PySpark data table with demographic information.
            - index (str): Identifier for an entity. This must also be present in 'current'.
            - datefield (str): Name of the date field.
        
        Returns:
        None. The demographic information is extracted, optionally filtered by age, and written to the specified location.
        """
        
        # Avoid column naming collisions between 'current' and 'demo' tables
        demoFields = noColColide(demo.df.columns, current.df.columns, demo.index, masterList =demo.df.columns)
        
        # Select distinct records based on the entity index and date field from 'current'
        current_index = current.df.select(demo.index + [current.datefield]).distinct().cache()
        current_index.collect()

        # Join the demographic information with the 'current' index, calculate age, and get distinct rows
        self.df = (
            demo.df
            .select(demoFields)
            .join(current_index, on=demo.index, how='inner')
            .withColumn('ageAtDate', F.datediff(F.col(current.datefield), F.col(demo.datefield))/365.3)
            .distinct()
        )

        # Filter the resulting data table by age range if the filterAge property is True
        if self.filterAge:
            self.df = self.df.filter(F.col('age').between(self.ageMin, self.ageMax))
        
        # Write the final dataframe to the specified location and refresh the 'df' property from the saved table
        writeTable(self.df, outTable=self.location, description=self.label, partitionBy = self.partitionBy)
        self.df = spark.table(self.location)

        
    def showIU(self,obs=6, index = ['personid'], sortfield = None):
        """Expert Determination IUH tenant ID = 127; Safe Harbor IUH tenant ID = 82
        """
        if not hasattr(self, 'index'):
            i = index
        else: 
            i = self.index
        showIU(self.df, obs=obs, index = i, sortfield = sortfield)
    
 
    




