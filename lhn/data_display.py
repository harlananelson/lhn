from lhn.header import display, Markdown, pd, DataFrame, F
from lhn.header import get_logger

logger = get_logger(__name__)


def print_parameters(d):
    """print values in a dictionary

    Args:
        d (_type_): _description_
    """
    for item, value in d.items():
        display(Markdown(f"**{item}**:  <ul>  {value} </ul>"))
        




def print_pd(df, label='', sortfield='Subjects', obs=5, sort_order='desc', hide_index=True):
    # Ensure sortfield is a list if it's not already
    if isinstance(sortfield, str):
        sortfield = [sortfield]
    
    # Check if the DataFrame is a PySpark DataFrame
    if isinstance(df, DataFrame):
        if all(field in df.columns for field in sortfield):
            # Apply sorting based on sort_order
            sort_expr = [F.desc(f) if sort_order == 'desc' else F.asc(f) for f in df.columns for f in sortfield]
            df = df.sort(*sort_expr)
        # Convert the Spark DataFrame to a pandas DataFrame for displaying
        df = df.limit(obs).toPandas()
    elif isinstance(df, pd.DataFrame):
        if all(field in df.columns for field in sortfield ):
            df = df.sort_values(by=sortfield, ascending=[sort_order != 'desc']*len(sortfield))
        df = df.head(obs)

    display(Markdown(f"**{label}**"))
    df_styled = df.style
    if hide_index:
        df_styled.hide_index()
    display(df_styled)

def showIU(df, obs=6, index=['personid'], tenant = 127, sortfield = None, sort_order = 'asc', label = None):
    """Expert Determination IUH tenant ID = 127; Safe Harbor IUH tenant ID = 82
       List records related only to tenant 82  (IUHealth Patients)
    Args:
        df (_type_): _description_
        obs (int, optional): _description_. Defaults to 6.
        index (list, optional): _description_. Defaults to ['personid'].

    Returns:
        _type_: _description_
    """
    if not sortfield:
        sortfield = index
    result = df
    if hasattr(df, 'columns') and 'tenant' in df.columns:
        result = result.filter(F.col('tenant') == tenant)
        
    print_pd(result, label=label, sortfield=sortfield, obs=obs,  hide_index=True, 
             sort_order= sort_order)
    
    return None 

def noRowNum(df, obs = 6):
    result = (
        df
        .style
        .hide_index()
    )
    return(result)

def show_first(df: DataFrame, sort_field: str, merge_index: str):
    """
    This function sorts the DataFrame based on a field, takes the value of the merge_index from the first row,
    and then filters the original DataFrame to only include rows with that value in the merge_index.
    
    Parameters:
    - df: DataFrame to be sorted and filtered
    - sort_field: Column name by which to sort the DataFrame
    - merge_index: Column name to take the value from the first row after sorting, and then to filter
    
    """
    # Sort the DataFrame and limit to 10 rows
    sorted_df = df.sort(F.col(sort_field).desc()).limit(10)
    
    # Convert the sorted DataFrame to Pandas to easily extract the first row
    sorted_pd_df = sorted_df.toPandas()
    
    # Extract the value of the merge_index column from the first row
    first_value = sorted_pd_df.loc[0, merge_index]
    
    # Filter the original DataFrame to only include rows with the merge_index matching the first_value
    filtered_df = df.filter(F.col(merge_index) == first_value)
    
    # Show the filtered DataFrame
    filtered_df.show()


