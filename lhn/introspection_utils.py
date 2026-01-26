import os
from lhn.header import re, DataFrame, List, spark

from lhn.data_transformation import fields_reconcile, flat_schema
from lhn.list_operations import extractTableName
from lhn.header import get_logger

logger = get_logger(__name__)


#from .spark_query import *
#from .database_operations import *
#from .data_transformation import *
def coalesce(*args):
    """Return the first non-None value in the arguments."""
    for arg in args:
        if arg is not None:
            return arg
    return None


def set_default_params(obj, params, default_values, **kwargs):
    for param in params:
        if hasattr(obj, param):
            kwargs.setdefault(param, getattr(obj, param))
        else:
            kwargs.setdefault(param, default_values.get(param))
    return kwargs

def return_codefields(tschema):
    fields = [item.split(".standard")[0] for item in tschema if re.search('standard',item)]
    fields_ordered = []
    [fields_ordered.append(field) for field in fields if field not in fields_ordered]
    return(fields_ordered)


def show_attributes(obj):
    # Get a list of all the methods and properties of the object
    attributes = dir(obj)

    # Filter out the built-in attributes
    attributes = [attr for attr in attributes if not attr.startswith('__')]

    # Display the methods and properties
    print(f"Methods and properties of {type(obj).__name__}:")
    for attr in attributes:
        print(attr)

# Example usage
# obj = MyClass()
# show_attributes(obj)

def getDFResources(resources):
    return([key for key, value in resources.__dict__.items() if isinstance(value, DataFrame)])

def prop2local(resource_obj, prop_list):
    """
    Returns a dictionary containing valid attributes present in the resource object.
    
    Parameters:
        resource_obj (object): The resource object from which to extract the attributes.
        prop_list (list): A list of attribute names to check for existence.
    
    Returns:
        dict: A dictionary containing the valid attributes present in the resource object.
    """
    valid_props = {}
    for prop in prop_list:
        if hasattr(resource_obj, prop):
            valid_props[prop] = getattr(resource_obj, prop)
        else:
            # If it doesn't exist, print a message
            print(f"Attribute '{prop}' not present in resources.")
    return valid_props


def get_standard_id_elements(columns, patterns, debug):
    """
    Extracts words from column names based on specified patterns.

    This function iterates over each column name and each pattern. It uses regular expressions to find matches
    of the pattern in the column name. If a match is found, the word preceding the pattern or the exact match
    is extracted and added to a set of extracted words. The function returns a list of the extracted words.

    Parameters:
    columns (list of str): The column names to search for matches.
    patterns (list of str): The patterns to match in the column names.
    debug (bool): If True, print debug information.

    Returns:
    list of str: The extracted words.
    """
    extracted_words = set()
    word = None
    # Iterate through each column
    for column in columns:
        # Iterate through each pattern
        for pattern in patterns:
            regex_pattern_preceding = r'(\w+)\.' + pattern
            regex_pattern_exact    = r'^(' + pattern + ')$'
            match_preceding = re.search(regex_pattern_preceding, column, re.IGNORECASE)
            match_exact     = re.search(regex_pattern_exact, column, re.IGNORECASE)
            if match_preceding:
                word = match_preceding.group(1)
            elif match_exact:
                if debug: print("match_exact")
                word = match_exact.group(1)
            if word:
                extracted_words.add(word)
                word = None
    
    return list(extracted_words)


def get_root_columns_elements(flat_columns, root_columns, pattern_strings, 
                              debug = False, debug_root_column = 'birthDate', debug_flat_column = 'birthDate',
                              debug_pattern_string = 'birthdate'):
    
    """
    Given a list of flat columns from a schema and a list of root columns to search for 
    and a list of pattern strings,
    iterate through the list of flat columns and root columns and pattern strings 
    to create a list of lists, where each sub list is associated with the root columns
    A pattern string is used in each instance to select fields from the list of 
    flat_columns
    There are two cases.
    - The last "words" of the flat columns match the root_column
    - The flat column is the same as the root columns
        - elif root_col == flat_col
    The 
    
    Alt parameter:
    debug_root_column = 'type', debug_flat_column = 'names.type.standard.id',
                              debug_pattern_string = 'standard.id'
    """
    
    results = []
    if debug: print(f"pattern_strings: {pattern_strings}")
    for root_column in root_columns:
        elements = set()  # Change this to a set
        for flat_column in flat_columns:
            if root_column.lower() in flat_column.lower():
                for pattern_string in pattern_strings:
                    
                    # pattern = r'((?:\w+\.)*(?:{}\.))(?:{})$'.format(root_column, pattern_string)
                    #pattern = r'((?:\w+\.)*{})\.?(?:{})$'.format(root_column, pattern_string)
                    #pattern = r'((?:\w+\.)*{})\.?(?:{})$'.format(root_column, pattern_string)
                    pattern = rf'((?:\w+\.)*{root_column})\.?(?:{pattern_string})$'
                    pattern_complicated = rf'((?:\w+\.)*{root_column})\.?(?:{pattern_string})$'
                    pattern_complicated = rf'(?:.*\.)?{root_column}\.{pattern_string}(?:\.|$)'

                    pattern_simple = rf'{root_column}'
                    pattern_simple = rf'^{root_column}$'
                    pattern = rf'{pattern_complicated}|{pattern_simple}'
                    match = re.search(pattern, flat_column, re.IGNORECASE)
                    if match:
                        if debug: print(f"{flat_column} matched {pattern}")
                        elements.add(flat_column)  # Use add instead of append
                        #if debug: print(f"added: {flat_columns}")
                
        if debug and flat_column == debug_flat_column: 
                if pattern_string == debug_pattern_string:
                    print(f"flat_column = '{flat_column}'")
                    print(f"root_column = '{root_column}'")
                    print(f"pattern_string = '{pattern_string}'")
                    print(f"pattern = '{pattern}'")
                    print(f"match: {match}")
        results.append(list(elements))  # Convert the set back to a list before appending
    return results

def get_root_columns(root_element: str, flat_columns: List[str], 
                     pattern_strings = ['standard.id', 'standard.codingSystemId', 'standard.primaryDisplay',
                                        'value', 'brandType','zip_code', 'death']) -> List[str]:
    pattern = r'((?:\w+\.)*)(?:{})$'.format('|'.join(pattern_strings))
    elements = set()

    for col in flat_columns:
        match = re.findall(pattern, col, re.IGNORECASE)
        if match:
            element = match[0].split('.')[0]
            if element == root_element:
                elements.add(col)
        
    return list(elements)

def extractSourceTables(dataTables):
    # This just swaps the key and value 
    try:
        TBLS = {dataTables[key]['source'].lower():key for key in dataTables.keys()}
    except Exception as e:
        print(f"Using  DataTables with keys {dataTables.keys()} got error {e}")
    return TBLS


def extractTableLocations(tableList, dataSchema):
    # get a table listing all the source table locations
    TBLLoc = {tuple_result[0].lower(): tuple_result[1] 
              for _, row in tableList.iterrows() 
              for tuple_result in [extractTableName(row['tableName'], schemaString=dataSchema)]
              }
    return TBLLoc



def translate_index(index, inTable, byIndex = True):
    '''The index is used to count distinct observational units
    There are a couple shortcut options 
    If underlines are used, they are converted to dots because that is what is in the data set.
    '''
    # Figure out the index and make sure it used dots.
    if not byIndex:
        index = []
    else:
        if index == 'person':
            index = ["empiPersonMetadata.empiPersonId"]
        if index  == 'full':
            index = ["empiPersonMetadata.empiPersonId","empiPersonMetadata.empiVersion","empiPersonMetadata.empiPersonVersion"]
    return(fields_reconcile(index,inTable))



def reinstantiate(df):
    if df.head() is not None:

        # Save the schema
        original_schema = df.schema

        # Convert PySpark DataFrame to Pandas DataFrame
        df_pd = df.toPandas()

        # Perform any Pandas operations you need (omitted in this example)

        # Convert Pandas DataFrame back to PySpark DataFrame
        result = spark.createDataFrame(df_pd, schema=original_schema)

    else:
        print("DataFrame is empty. Skipping conversion.")   
        

 




# Test the function
# Assuming e.cohortEncounters.df is your DataFrame
# show_first(e.cohortEncounters.df, "distinct_count", "personid")

#################################################################################################
########################### Spark Data Table Flattening #########################################
##################################################################################################




def deduplicate_fields(table, fields):
    ''' Return a list of fields in the table but without duplicates
    '''
    fields_ordered = []
    fields_flat = flat_schema(table)
    [fields_ordered.append(field) for field in fields  if (field not in fields_ordered) & (field in fields_flat )]
    return(fields_ordered)

##### analyze a package.



