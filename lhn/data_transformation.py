
from lhn.header import pprint, ArrayType, StructType, StringType, F, spark, re, yaml, Template, Dict, AnalysisException, Window
from lhn.list_operations import noColColide 
from lhn.header import get_logger, deepcopy

logger = get_logger(__name__)

def fields_reconcile(fields, inTable, delim = '_'):
    ''' Take a list of fields with understore delimiters and 
    replace with actual field names
    '''
    
    fschema             = flat_schema(inTable)
    #print(fschema)
    fields              = [field.lower() for field in fields]
    actual_fields       = [field for field in fschema if 
                           (field.lower() in fields) | 
                           (field.replace(".",'_').lower() in fields) |
                           (field.replace("_",'.').lower() in fields)]
    return(actual_fields)

def read_config(yaml_file, replace, debug = False):
    """
    Reads a YAML configuration file, applies replacements, and returns the resulting dictionary.

    This function opens a YAML file, reads its contents into a dictionary, and applies a set of
    replacements to the dictionary values. The replacements are applied recursively, allowing for
    deep modifications of nested structures within the YAML file. If debugging is enabled, it prints
    the path of the YAML file being processed.

    Parameters:
    - yaml_file (str or Path): The path to the YAML configuration file to be read. This can be a
    string or a Path object.
    - replace (dict): A dictionary containing key-value pairs for replacements to be applied to the
    YAML file's contents. The keys represent the placeholders to be replaced, and the values
    represent the replacement values.
    - debug (bool, optional): If set to True, prints the path of the YAML file being processed for
    debugging purposes. Defaults to False.

    Returns:
    - dict: A dictionary representation of the YAML file's contents after applying the replacements.

    Note:
    - The function relies on the `recursive_template` function to apply replacements to the
    dictionary. This function must be defined elsewhere and is responsible for recursively
    traversing the dictionary and replacing placeholders with the corresponding values from the
    `replace` dictionary.
    """
    replace = deepcopy(replace)
    with open(str(yaml_file)) as cf:
        dictionary = recursive_template(yaml.safe_load(cf), replace = replace)
          
    if debug: 
        print(f" projectTables at local step")
        pprint.pprint(yaml_file)
        
    return(dictionary)

def flatten_schema(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType

        if isinstance(dtype, StructType):
            fields += flatten_schema(dtype, prefix=name)
        else:
            fields.append(name)

    return fields

def flat_schema(in_table, spark = None, prefix=None):
    '''Return a flat schema'''
    if isinstance(in_table, str):
        in_table = spark.table(in_table)
    return flatten_schema(in_table.schema)

def flatSchema(in_table, prefix=None):
    return flatten_schema(in_table.schema)

def flatten_df(df):
    flat_cols = flatten_schema(df.schema)
    for col in flat_cols:
        df = df.withColumn(col.replace(".", "_"), F.col(col).cast(StringType()))
    return df.select([col.replace(".", "_") for col in flat_cols])

def regInList(field, regexList):
    """
    Identify if a regex pattern has a match in a list
    """
    cnt = 0
    for regexString in regexList:
        if re.search(regexString, field, re.I): 
            cnt = cnt + 1
    return(cnt > 0)

# re('conditioncode_standard_id', inclusionRegex)


def fieldRequested(df, field_name, inclusionRegex, debug = False):
    
    try:
        names_select = [F.col(field_name).alias(field_name.replace(".","_"))]
        x = [item for item in flatSchema(df.select(names_select)) if regInList(item.replace(".","_"), inclusionRegex)]
    except:
        if debug:
            print(f"fieldRequested: Can't select {names_select} in {df.columns} field_name")
            print(f" field_name {field_name} ")
        x = []
    #x = [item for item in flatSchema(df.select(field_name))]
    return(len(x) > 0)
    
def filter_and_group(df, category, date, id):
    grouped_df = df.groupby(category).agg({id: 'count', date: ['min', 'max']})
    grouped_df.columns = ['_'.join(col).rstrip('_') for col in grouped_df.columns.values]
    grouped_df = grouped_df.rename(columns={'person_count': 'count', 'date_min': 'min_date', 'date_max': 'max_date'})
    grouped_df.reset_index(inplace=True)
    return grouped_df.sort_values(by = ['personid_count'], ascending = False)


def dict_of_list_to_list(dict):
    """
    Given a dictionary of lists, return a concatination of those lists
    """
    items = [item for sublist in dict.values() for item in sublist]
    return(items)

def readParquet(tableName, inSchema, warehouse):
    #df = spark.read.parquet(f"""{warehouse}{inSchema.lower()}.db/{tableName.lower()}""")
    df = (
        spark.read.format("parquet")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"""{warehouse}{inSchema.lower()}.db/{tableName.lower()}""")
    )

    return(df)
  
def showfields(i, table):
        fschema = flat_schema(table)
        fields  = table.columns
        result =  [item for item in fschema if re.search("^" + fields[i] + "(\.|$)", item)]
        return(result)
    
def list_fields(index, fields):
    regex = re.compile(fields[index])
    r = [s for s in fields if regex.match(s)]
    return(r)


def recursive_template(d: Dict, replace: Dict, top_call: bool = True, debug: bool = False) -> Dict:
    """
    Recursively replace strings in a dictionary.

    :param d: Dict
        The dictionary to replace strings in.
    :param replace: Dict
        The replacements to use.
    :param top_call: bool
        Indicates if the function is at its top-level call.
    :return: Dict
        The dictionary with replacements made.
    """
    if debug: print("Start of recursive_template")
    new_dict = {}    
    for k, v in d.items():
        if k in replace.keys():
            v = replace[k]
        if isinstance(v, str):
            new_dict[k] = Template(v).safe_substitute(**replace)
            if top_call:
                replace.update(new_dict)
        elif isinstance(v, list):
            new_list = [recursive_template(i, replace, top_call=False) if isinstance(i, dict) else Template(i).safe_substitute(replace) for i in v]
            new_dict[k] = new_list        
        elif isinstance(v, dict):
            v_replaced = recursive_template(v, replace, top_call=False)
            new_dict[k] = v_replaced
        else:
            new_dict[k] = v

    return new_dict

def recursive_template(d: Dict, replace: Dict, top_call: bool = True, debug: bool = False) -> Dict:
    """
    Recursively replace strings in a dictionary.

    :param d: Dict
        The dictionary to replace strings in.
    :param replace: Dict
        The replacements to use.
    :param top_call: bool
        Indicates if the function is at its top-level call.
    :return: Dict
        The dictionary with replacements made.
    """
    if debug: print("Start of recursive_template")
    new_dict = {}    
    # to do need to first check if d is a dictionary or empty.
    for k, v in d.items():
        if k in replace.keys():
            v = replace[k]
        if isinstance(v, str):
            new_dict[k] = Template(v).safe_substitute(**replace)
            if top_call:
                replace.update(new_dict)
        elif isinstance(v, list):
            new_list = []
            for i in v:
                if isinstance(i, str):
                    new_list.append(Template(i).safe_substitute(replace))
                elif isinstance(i, list):
                    new_list.append([Template(item).safe_substitute(replace) for item in i if isinstance(item, str)])
                elif isinstance(i, dict):
                    new_list.append(recursive_template(i, replace, top_call=False))
                else:
                    new_list.append(i)
            new_dict[k] = new_list
        elif isinstance(v, dict):
            v_replaced = recursive_template(v, replace, top_call=False)
            new_dict[k] = v_replaced
        else:
            new_dict[k] = v

    return new_dict


def expand_arrays_in_df(df, cols):
    """
    Return a spark data table with levels indicated by '_' and resulting columns that are not arrays
    """
    operations = []
    non_array_cols = []

    for column in cols:
        if 'raw' not in column and 'value' not in column:  # Exclude columns that contain "raw" or "value"
            try:
                # split the column on '.' to get the top level column name
                top_level_col = column.split('.')[0]
                # check if the top level column is an ArrayType and needs to be exploded
                if isinstance(df.schema[top_level_col].dataType, ArrayType):
                    # add a transformation operation to explode the top level column and select the nested field
                    operations.append((top_level_col, column))
                else:
                    non_array_cols.append(F.col(column).alias(column.replace('.', '_')))
            except AnalysisException as e:
                print(f"Error occurred while processing column '{column}': {e}")
            except Exception as e:
                print(f"An error occurred for column '{column}': {e}")

    # apply all transformation operations
    exploded = set()  # Keep track of columns that have already been exploded
    for top_level_col, column in operations:
        try:
            if top_level_col not in exploded:
                df = df.withColumn(top_level_col, F.explode(F.col(top_level_col)))
                exploded.add(top_level_col)
            df = df.withColumn(column.replace('.', '_'), F.col(column))
        except Exception as e:
            print(f"Error occurred while expanding {column}: {str(e)}")

    try:
        return df.select(non_array_cols + [F.col(op[1].replace('.', '_')) for op in operations])
    except Exception as e:
        print(f"Error occurred while selecting columns: {str(e)}")
        return None

def write_sorted_index_table(inTable, index_field, retained_fields, sort_fields,
                      code, indexLabel="index_", lastLabel="last_", sort_order=None):
    """
    Identify the first (index) date for a person
    inTable: A pointer to a spark data table
    index_field: a list of fields used to identify unique records in the final result table
    retained_fields: Fields in `inTable` to be retained in the final result table. The levels of
                     these fields are chosen according to the sort by the field called `datefieldPrimary`
    sort_fields: A dictionary indicating the field and sort order
                      for example {'None': '10', 'Confirmed present': '0','Payment diagnosis [identifier]': '1', 'Complaint': '2'}
                      in this case, if this is the `confirmationstatus_standard_primaryDisplay` field, then
                      for the same level of `index_field`, the record with "Confirmed present" would be retained over
                      the record with 'None'. `datefieldPrimary` is currently just a field, not a list. But future
                      work would allow `datefieldPrimary` to be a list and just refer to a set of sorting fields.
    code: Used to name, tag calculated fields
    indexLabel: Used to name calculated fields
    lastLabel: Used to label calculated fields
    sort_order: A dictionary specifying the sort order for `datefieldPrimary`.

    """
    print(f"Create a cohort style table with datefield {sort_fields} and index {index_field}")

    df = inTable.withColumn("unique_id", F.monotonically_increasing_id())

    windowSpec = (
        Window.partitionBy(index_field)
        .orderBy(sort_fields, 'unique_id')
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    windowSpecr = (
        Window.partitionBy(index_field)
        .orderBy(sort_fields, 'unique_id')
    )

    retained_fieldsEdit = noColColide(retained_fields, [*index_field, sort_fields], index=[],
                                      masterList=inTable.columns)

    if sort_order is None:
        sort_columns = [sort_fields]
    else:
        sort_columns = [F.when(F.col(sort_fields) == key, value) for key, value in sort_order.items()]
        sort_columns.append(sort_fields)
        sort_columns = sort_columns[::-1]  # Reverse the order to prioritize values in the dictionary

    cohort_index = (
        df
        .select([*index_field, *retained_fieldsEdit, sort_fields, 'unique_id'])
        .distinct()
        .withColumn(indexLabel + code, F.first(sort_fields, ignorenulls=True).over(windowSpec))
        .withColumn(lastLabel + code, F.last(sort_fields , ignorenulls=True).over(windowSpec))
        .withColumn('entries_' + code, F.count(index_field[0]).over(windowSpec))
        .withColumn('rownum', F.row_number().over(windowSpecr))
        .select([*index_field, *retained_fieldsEdit, indexLabel + code, lastLabel + code, 'entries_' + code])
        .filter(F.col('rownum') == 1)
        .distinct()
        .sort([*index_field, indexLabel + code] + sort_columns)
    )
    return cohort_index



def get_selected_fields(i, table, concepts, replace_underlines = False, debug = False):
    ''' Use the root of a nested field branch to select which fields to extract
    Also alows for naming the flatted version of the fields
    If underlines are used, they are converted to dots because that is what is in the data set.
    '''
    fields_f      = table.columns
    fields_flat_f = flat_schema(table)
    if isinstance(i, int):
        # Flaten out the fields given by index i
        i              = [item for item in [*showfields(i,table)]]
    elif isinstance(i, list):
        # A list of fields was given so use that
        if debug: print(f"The list of fields is {i} and the columns in the source are {fields_flat_f}")
        if replace_underlines:
            i1                    = [item.replace('_','.') for item in i if item in fields_flat_f]
            i2                    = [item                  for item in i if item in fields_flat_f]
            i                     = [*i1, *i2]
    else:
        print("parameter i entered incorrectly")
        return
    selected_fields          = fields_reconcile(i, table, delim = '_')
    return(selected_fields)

