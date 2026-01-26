from lhn.header import List,  Optional, re, F, StringType
from lhn.header import get_logger

logger = get_logger(__name__)


def get_element_index(element: any, lst: List[any]) -> Optional[int]:
    """
    Get the index of the first occurrence of an element in a list.

    Args:
    - element: The element to search for in the list.
    - lst: The list in which to search for the element.

    Returns:
    - int: The index of the element if found, otherwise None.
    """

    # Function implementation remains the same
    if element in lst:
        index = lst.index(element)
        return index
    else:
        print(f"(Not Found) The element {element} is not in the list {lst}")
        return None


# Define a UDF to escape regex characters and conditionally add ^ and $


def escape_and_bound_dot(s, complete):
    escaped = s.replace('.', '\\.') # This escapes ONLY the dot
    if not escaped.startswith("^"):
        escaped = "^" + escaped
    if complete and not escaped.endswith("$"):
        escaped += "$"
    return escaped

def escape_and_bound_regex(s: str, complete: bool) -> str:
    """
    Escape all regex special characters in a string and optionally add anchors.

    Args:
    - s: The string to be escaped.
    - complete: A boolean that indicates whether to add '^' at the beginning and '$' at the end of the string.

    Returns:
    - str: The escaped string with optional anchors.
    """
    escaped = re.escape(s) # This escapes ALL regex special characters
    if not escaped.startswith("^"):
        escaped = "^" + escaped
    if complete and not escaped.endswith("$"):
        escaped += "$"
    return escaped
    
escape_and_bound_dot_udf = F.udf(escape_and_bound_dot, StringType())


# Apply the UDF to the DataFrame column
#df = elementList.df.withColumn(self.groupName, escape_and_bound_dot_udf(col(elementList.listIndex), elementList.complete))


def preprocess_string(s):
    # Replace any character that is not a letter, a number, or a space with nothing
    return re.sub(r'[^a-zA-Z0-9\s]', '', s)

def is_single_level(value, single_level_types=(str, list, bool, int, float)):
    return isinstance(value, single_level_types)

def find_single_level_items(config_dict, single_level_types=(str, list, bool, int, float)):
    single_level_items = {}
    for key, value in config_dict.items():
        if is_single_level(value, single_level_types=single_level_types):
            single_level_items[key] = value
    return single_level_items


# Example usage:
"""  config_dict = {
    'key1': 'value1',
    'key2': {'sub_key1': 'sub_value1'},
    'key3': 'value3',
    'key4': [1, 2, 3],
    'key5': 'value5'
} """

def unique_non_none(*args, masterList=None):
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



def noColColide(masterColumns, colideColumns=None, index = None, masterList=None):
    """
    Start wth index then add masterColumns that are not in colideColumns and are in masterList.
    Generates a list of column names from the masterColumns that do not collide with the colideColumns after a join operation.
    The function ensures that the column names specified in the index are always included in the resulting list.
    If a masterList is provided, the function will return a list that is a subset of the masterList.
    If no masterList is provided, the masterColumns are used as the masterList.

    Parameters:
    - masterColumns (list): A list of column names from the primary table.
    - colideColumns (list): A list of column names that could potentially cause a collision in the join operation.
    - index (list): A list of column names that should always be included in the resulting list.
    - masterList (list, optional): A list of column names that should be used as the master list. If not provided, masterColumns is used.

    Returns:
    - list: A list of column names from masterColumns that do not collide with colideColumns, ensuring that column names in index are included.
    """
    if masterList is None:
        masterList = masterColumns
    result = index.copy()
    [result.append(item) for item in masterColumns if item not in result and item in masterList and item not in colideColumns and item is not None]
    return result

def extractTableName(TBL, schemaString='iuhealth_prime'):
    """
    Extracts the table name from a given string, TBL. If TBL starts with the schemaString followed by an underscore, 
    it removes the schemaString and underscore from TBL. If not, it returns TBL and schemaString concatenated with TBL.

    Parameters:
    - TBL (str): The string from which to extract the table name.
    - schemaString (str, optional): The schema string to look for at the start of TBL. Defaults to 'iuhealth_prime'.

    Returns:
    - tuple: A tuple containing the extracted table name and the original string or the original string concatenated with the schemaString.
    """
    if TBL.startswith(schemaString + '_'):
        return TBL.replace(schemaString + '_', '', 1), TBL
    else:
        return TBL, f"{schemaString}.{TBL}"