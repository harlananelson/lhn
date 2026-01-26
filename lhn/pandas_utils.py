from lhn.header import pd


from lhn.header import get_logger

logger = get_logger(__name__)

################################################################################################
################################# Pandas Utilities ############################################3
################################################################################################

def uncapitalize(s:str) -> str:
    if s:
        return s[0].lower() + s[1:]
    else:
        return s

def pop_item(df, field:str) -> str:
    """Get the first observation of a pandas data table for a field return as a string

    Args:
        df (_type_): _description_
        field (str): _description_

    Returns:
        str: _description_
    """
    return df[field].unique().item()

def pop_unlist(df, field:str) -> list:
    return df[field].unique().tolist()

def dict2Pandas(dict, columnname = 'codes'):
    return(pd.DataFrame.from_dict(dict, orient='index',columns = ['codes']))