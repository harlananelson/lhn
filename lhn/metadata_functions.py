

from lhn.data_transformation import flat_schema
from lhn.introspection_utils import extractTableLocations
from lhn.item import Item, TableList
from lhn.header import re, spark, pprint
from lhn.spark_utils import database_exists, getTableList
from lhn.listTable import ListTable

from lhn.item import Item, TableList

# possible circular import
from lhn.metaTable_module import metaSchema, metaTable

from lhn.header import get_logger

logger = get_logger(__name__)

def processDataTables(dataTables, schema, dataLoc, disease, schemaTag, project, parquetLoc, debug=False):
    """
    Return a dictionary of tables where every table listed in the dataTables dictionary is returned as class Item.
    The source of the dictionary is from the configuration YAML file and the tables are stored in the schema.

    Parameters:
        dataTables (dict): A dictionary of tables from the configuration YAML. Contains an entry for each table in the schema, each table entry is a dictionary with table metadata including a dictionary of field name search strings.
        schema (str): The schema where the tables are supposed to exist.
        dataLoc (str): A file location of the directory where csv versions of the files can be stored.
        disease (str): A tag that can be used in the storage name of the table to help differentiate it from other similar tables.
        schemaTag (str): A tag that can be used in the storage name of the table to help differentiate it from other similar tables.
        project (str): The project name (e.g., SickleCell) for reference.
        parquetLoc (str): HDFS base path for parquet files (e.g., hdfs:///user/hnelson3/SickleCell/).
        debug (bool): Use to generate additional code running annotations.

    Returns:
        TableList: A TableList object containing Item instances.
    """
    if not database_exists(schema):
        logger.error(f"Database {schema} does not exist")
        return None

    tableList = getTableList(schema)  # A list of all the tables in the schema: database.tableName
    TBLSource = {}  # The dictionary of tables to be populated here and returned
    logger.debug(f"The dataTables keys are: {dataTables.keys()}")
    TBLLoc = extractTableLocations(tableList, schema)  # Dictionary of name and location with schema.name

    for TBL in dataTables.keys():  # Iterate through the specified tables.
        # Create an Item instance
        TBLSourceItem = Item(
            TBL,
            dataTables,
            TBLLoc,
            schema,
            dataLoc,
            disease,
            schemaTag,
            project,
            parquetLoc,  # Pass parquetLoc
            debug=debug
        )
        if TBLSourceItem:
            TBLSource[TBL] = TBLSourceItem
    return TableList(TBLSource)

def update_dictionary(schema_dict, schema, projectSchema, schemaTag, obs, reRun, debug, personid, tableNameTemplate):
    """
    Updates a dictionary with metadata about a schema, filtering or modifying table names based on a template.

    Args:
        schema_dict (dict): The dictionary to update.
        schema (str): The name of the schema.
        projectSchema (str): The name of the project schema.
        schemaTag (str): The tag of the schema.
        obs (str): The observation to use.
        reRun (bool): Whether to rerun the function.
        debug (bool): Whether to run the function in debug mode.
        personid (str): The ID of the person.
        tableNameTemplate (str): A template or pattern to filter or modify table names.

    Returns:
        dict: The updated dictionary.
    """
    callFunc_metaSchema = {
        'schema': schema, 
        'outSchema': projectSchema, 
        'schemaTag': schemaTag,
        'pattern_strings': ['*'],
        'obs': obs,
        'reRun': reRun, 
        'debug': debug,
        'personid': personid,
    }
    logger.debug("inside: update_dictionary")
    logger.debug(callFunc_metaSchema)
    MSchema = metaSchema(**callFunc_metaSchema)  # Assuming metaSchema is a predefined class
    table_list = MSchema.TBLLoc  # Assuming TBLLoc is a property of MSchema instances

    # If there is a schema_dict, then copy it, otherwise create a new dictionary
    if schema_dict:
        new_dict = schema_dict.copy()
    else:
        new_dict = {}

    # Compile a regex pattern based on tableNameTemplate, if provided
    pattern = re.compile(rf"^(.*?){tableNameTemplate}") if tableNameTemplate else None
    logger.debug(f"Using tableNameTemplate pattern {pattern}")
    
    for key, value in table_list.items():
        logger.debug(f"{key}:{value}")
        new_key = key  # Use the original key if tableNameTemplate is None
        if pattern:
            match = pattern.match(key)
            if match:
                new_key = match.group(1).rstrip('_')  # Remove trailing underscore  
            else:
                new_key = key
        
        # Ensure unique keys by checking if new_key already exists and modifying it if necessary
        original_new_key = new_key
        counter = 1
        while new_key in new_dict:
            new_key = f"{original_new_key}_{counter}"
            counter += 1

        new_dict[new_key] = {
            'source': value,
            'inputRegex': [f'^{value}$' for value in flat_schema(value, spark=spark)]  # Assuming flat_schema is a predefined function
        }
    return new_dict
            
def current_tables_processed( schema, schemaTag,
                             pattern_strings = ['standard.id', 'standard.codingSystemId', 'standard.primaryDisplay', 'brandType', 'zip_code', 'deceased', 'tenant', 'birthdate']
                             ):
    Okeys = metaSchema(schema, schema, schemaTag, pattern_strings = pattern_strings).TBLLoc.keys()
    nested_list   = [key.split('_') for key in Okeys]
    modified_list = [sublist[:-1] for  sublist in nested_list]
    ctp = {"_".join(sublist) for sublist in modified_list}
    return([item for item in ctp if item is not ''])


def process_metadata_tables(
    input_dataSchema, 
    output_metaSchema, 
    schemaTag, 
    obs=1000000, 
    debug=False, 
    reRun=False, 
    keylist=None,
    pattern_strings = ['standard.id', 'standard.codingSystemId', 'standard.primaryDisplay', 'brandType',
                  'zip_code', 'deceased', 'tenant', 'birthdate'], 
    only_scan_current_tables = False,
    personid = ['personid']
    ):
    """
    @only_scan_current_tables: This is used when starting a session to scan in processed tables, not for processing tables
    """
    print("in module: resource.py")
    print(f"Process Metadata Tables for input_dataSchema: {input_dataSchema}, output_metaSchema: {output_metaSchema} \n")
    print(f" keylist is {keylist}, pattern_strings is: {pattern_strings}, only_scan_current_tables: {only_scan_current_tables} \n")
    ctp = current_tables_processed(output_metaSchema, schemaTag, pattern_strings = pattern_strings)  # tables already created
    #current_tables_processed = list(set([key for key in Okeys]))
    
    callFunc = {
        'schema'          : input_dataSchema, 
        'outSchema'       : output_metaSchema, 
        'schemaTag'       : schemaTag,
        'pattern_strings' : pattern_strings ,
        'obs'             : obs,
        'reRun'           : reRun, 
        'debug'           : debug,
        'personid'        : personid,
        
    }
    if debug:
        print("callFunc\n")
        print(callFunc)
    try:
        MSchema = metaSchema(**callFunc)
        print("process_metadata_tables: metaSchema class created")
    except Exception as e:
        print(f"Tried to create a metaSchema object: An error occurred: {e} \n")
        
    if debug: print(f'MSchema = metaSchema("{input_dataSchema}", "{output_metaSchema}") \n')
    
    mDataTables = {}
    
    if not keylist:
        keylist = MSchema.TBLLoc.keys()  #all tables in the input_dataSchema
        
    if only_scan_current_tables:
        keylist = [item for item in keylist if any(ctp_item.startswith(item) for ctp_item in ctp)]
    
        
    print(f"final key list is {keylist}")
    
    for item in MSchema.TBLLoc.keys():
            # Only process items that are also present in keylist
            if item in keylist:
                print(f"item = {item} \n")
                try:
                    if debug:
                        try:
                            print(f"tbl = metaTable({item}, MSchema, reRun = {reRun}) ")
                        except Exception as e:
                            print(f"An error occurred: {e} \n")
                    tbl = metaTable(item, MSchema, reRun = reRun)
                    if debug:
                        try:
                            print(f"id_cols: {tbl.id_cols} ")
                            print(f"root_column: {tbl.root_columns} ")
                        except Exception as e:
                            print(f"An error occurred: {e} \n")
                except Exception as e:
                    print(f"Can't use table {item} ")
                    print(f"An error occurred: {e} \n")

                try:
                    for column in tbl.root_columns:
                        if debug:
                            print(f"Working on Column {column} \n")
                        try:
                            tbl.column(column)
                            tbl.countLevels()
                            try:
                                mDataTables[tbl.listSource] = ListTable(tbl)  # Assign table to mDataTables
                            except Exception as e:
                                print(f"Unable to create table {tbl.listSource} by calling ListTable(tbl)\n")
                                print(f"An error occurred: {e} \n")
                            if debug:
                                print(f"using {tbl.col} at {tbl.listLocation} \n")
                            if debug:
                                print(f" listSource: {tbl.listSource} \n")
                        except Exception as e:
                            print(f"Unable to use column: {column} from table {item} \n")
                            print(f"An error occurred: {e} \n")
                            print(f"The root_colmns are {tbl.root_columns} \n")
                            print(f"the keylist is {keylist} \n")
                            
                    if debug:
                        print("next")
                except Exception as e:
                    print(f"problem iterating through {item}.root_columns for table {item} note: \n ")
                    print(f"An error occurred: {e} \n")

    return mDataTables




def get_standard_id_elements_1(flat_columns, relevant_elements=[], debug=False):
    """
    call: get_standard_id_elements(self.flat_columns, post_strings = self.post_strings, pre_strings = self.pre_strings, exact_strings = self.exact_strings, debug = debug)
    """
 
    original_pattern = r'((?:\w+\.)*\w+)\.standard\.id$'
    added_pattern = "|".join(["^" + re.escape(item) for item in relevant_elements])
    pattern = "|".join([original_pattern, added_pattern])

    elements = set()

    if debug:
        print(f"flat_columns are {flat_columns} \n")
        print(f"relevant_elements are {relevant_elements} \n")

    for col in flat_columns:
        match = re.findall(pattern, col)
        if match:
            element = match[0].split('.')[0]
            elements.add(element)

    if debug:
        print(f"elements are {elements} \n")

    return list(elements)

def get_standard_id_elements_2(flat_columns, relevant_elements = [], debug = False):
    """
    post_strings=
                 ['standard.id', 'standard.codingSystemId', 'standard.primaryDisplay', 'value', 'brandType'], 
                 pre_strings = ['zip_code'],
                 exact_strings = ['deceased', 'tenant', 'birthdate'],
    call: get_standard_id_elements(self.flat_columns, post_strings = self.post_strings, pre_strings = self.pre_strings, exact_strings = self.exact_strings, debug = debug)
    
    Given a list of flattened column names and a list of relevant elements, return a list of unique elements that have a flattened column name that ends in '.standard.id'

    Parameters:
    flat_columns (list): A list of flattened column names
    relevant_elements (list): A list of relevant elements

    Returns:
    list: A list of unique elements that represent root columns
    
    The flat_columns are a flattened version of a schema, for example
    [   'personid',
        'birthdate',
        'dateofdeath',
        'gender.standard.id',
        'gender.standard.codingSystemId',
        'gender.standard.primaryDisplay',
        'gender.standardCodings.id',
        'gender.standardCodings.codingSystemId',
        'gender.standardCodings.primaryDisplay',
        'other.races.standard.id',
        'other.races.standard.codingSystemId',
        'other.races.standard.primaryDisplay',
        'zipcodes.zipcode1',
        'zipcodes.begineffectiveyear']
        
    1. Extract the first word before standard.id, such as gender and races. Note that there can be multiple
       words such as in 'other.races.standard.id', but ontly races will be extracted.
    2. Allow a list called relevant_elements that can be coded, for example:
       ['birthdate', 'dateofdeath'] to pick up single words.  made the code for this separate and concatinate results
       Only include this if they can be found in flat_columns
    3. Allow single words in relevant_elements to also pick up 'zipcodes' by adding 'zipcodes' to relevant_columns
    ['birthdate', 'dateofdeath', 'zipcodes'.].  Only include this if they can be found in flat_columns
    This would return 'zipcodes'  
    
    """
    
    

    original_pattern = r'((?:\w+\.)*\w+)\.standard\.id$'
    added_pattern = "|".join([re.escape(item)   for item in relevant_elements])
    pattern = "|".join([original_pattern, added_pattern])
    if debug: print(f'the pattern is {pattern} \n')
    elements = set()
    
    if debug:
        print(f"flat_columns are {flat_columns} \n")
        print(f"relevant_elements are {relevant_elements} \n")

    for col in flat_columns:
        if debug: print(f"the col is {col} \n")
        for match in re.finditer(pattern, col):
            if match.group():
                elements.add(match.group())
    if debug:
        print(f"elements is {elements} \n")

    return list(elements.union([item for item in relevant_elements if item in flat_columns]))



def get_standard_id_elements_3(flat_columns, relevant_elements=[], debug=False):
    """
    Given a list of flattened column names and a list of relevant elements,
    return a list of unique elements that represent root columns.

    Parameters:
    flat_columns (list): A list of flattened column names
    relevant_elements (list): A list of relevant elements to be directly included

    Returns:
    list: A list of unique elements that represent root columns
    """
    
    # Initialize a set to store unique elements
    elements = set()

    # First Pass: Extract the last word before '.standard.id', '.standard.codingSystemId' or '.standard.primaryDisplay'
    # in the flat_columns list.
    pattern1 = r'((?:\w+\.)+)?(\w+)\.standard\.(id|codingSystemId|primaryDisplay)'
    for col in flat_columns:
        match = re.search(pattern1, col)
        if match:
            elements.add(match.group(2))

    # Second Pass: Directly include relevant_elements if they are exactly present in flat_columns.
    elements.update([el for el in relevant_elements if el in flat_columns])

    # Third Pass: Include elements from relevant_elements that also appear as the first word in some string in flat_columns,
    # but only if they are also in flat_columns.
    for rel_el in relevant_elements:
        for col in flat_columns:
            if col.startswith(rel_el + '.'):
                elements.add(rel_el)
                break
                
    if debug:
        print(f"Debug info: {elements}")

    return list(elements)

def get_standard_id_elements_4(flat_columns, post_strings, pre_strings, exact_strings, debug = False):
    
    filtered_post  = [col for col in flat_columns for end in post_strings if col.endswith(end)]
    filtered_pre   = [col for col in flat_columns for start in pre_strings if col.startswith(start)]
    filtered_exact = [col for col in flat_columns if col in exact_strings]

    # Combine all the filtered results into one list without duplicates
    combined_filtered = list(set(filtered_post + filtered_pre + filtered_exact))

    return combined_filtered

def find_schema_information():

    # Initialize Spark session with Hive support
    spark = SparkSession.builder \
        .appName("Get Tables with Metadata") \
        .enableHiveSupport() \
        .getOrCreate()

    # Specify the database name
    database_name = 'your_database_name'

    # Set the current database
    spark.catalog.setCurrentDatabase(database_name)

    # List all tables in the specified database
    tables = spark.catalog.listTables()

    # Collect table information
    table_info = []

    for table in tables:
        table_name = table.name
        comment = table.description  # Use table.description if comments are directly available
        
        # Attempt to retrieve additional metadata using DESCRIBE FORMATTED
        try:
            # DESCRIBE FORMATTED provides detailed metadata, including creation time if available
            df = spark.sql(f"DESCRIBE FORMATTED {database_name}.{table_name}")
            df.show(truncate=False)  # Display extended table properties for debugging

            # Extract relevant metadata
            metadata = df.collect()
            creation_date = 'N/A'
            for row in metadata:
                if 'CreateTime' in row.col_name:
                    creation_date = row.data_type.strip()
                    break
        except Exception as e:
            print(f"Failed to retrieve metadata for table {table_name}: {e}")
            creation_date = 'N/A'

        table_info.append((table_name, comment, creation_date))

    # Convert to DataFrame for better visualization
    df_metadata = spark.createDataFrame(table_info, ["table_name", "table_comment", "creation_date"])

    # Show the DataFrame content
    df_metadata.show(truncate=False)

"""
# Define your lists
flat_columns = [
    'personid',
    'birthdate',
    'dateofdeath',
    'gender.standard.id',
    'gender.standard.codingSystemId',
    'gender.standard.primaryDisplay',
    'other.races.standard.id',
    'other.races.standard.codingSystemId',
    'other.races.standard.primaryDisplay',
    'zipcodes.zipcode1',
    'zipcodes.begineffectiveyear'
]

post_strings = [
    'standard.id',
    'standard.codingSystemId',
    'standard.primaryDisplay',
    'value',
    'brandType'
]

pre_strings = ['zipcodes.']
exact_strings = ['birthdate']

# Call the function with your lists
filtered_columns = filter_columns(flat_columns, post_strings, pre_strings, exact_strings)
print(filtered_columns)

"""


