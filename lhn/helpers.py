"""
lhn/helpers.py

Core helper functions restored from v0.1.0.
These were the most-used analytical workflow functions in the original
monolithic lhn package, providing data display, cohort reporting,
and inspection utilities.

Restored: 2026-03-15 from v0.1.0-monolithic branch.
"""

from lhn.header import (
    spark, F, pd, DataFrame, display, Markdown, get_logger
)

logger = get_logger(__name__)


def print_parameters(d):
    """Print values in a dictionary with markdown formatting.

    Args:
        d (dict): Dictionary of parameter name -> value pairs
    """
    for item, value in d.items():
        display(Markdown(f"**{item}**:  <ul>  {value} </ul>"))


def print_pd(df, label='', sortfield='Subjects', obs=5, sort_order='desc',
             hide_index=True):
    """Display a DataFrame (Spark or pandas) with optional sorting and limiting.

    Converts Spark DataFrames to pandas for rich display in Jupyter notebooks.
    This was the primary data inspection function in v0.1.0, used extensively
    for QC and exploratory analysis.

    Args:
        df: Spark DataFrame or pandas DataFrame
        label (str): Bold header displayed above the table
        sortfield (str or list): Column(s) to sort by
        obs (int): Number of rows to display. All `obs` rows are rendered
            in full (no pandas first-5/last-5 summary truncation); raise
            `obs` to verify an entire code list.
        sort_order (str): 'desc' or 'asc'
        hide_index (bool): Whether to hide the row index
    """
    if isinstance(sortfield, str):
        sortfield = [sortfield]

    if isinstance(df, DataFrame):
        if all(field in df.columns for field in sortfield):
            sort_expr = [
                F.desc(f) if sort_order == 'desc' else F.asc(f)
                for f in sortfield
            ]
            df = df.sort(*sort_expr)
        df = df.limit(obs).toPandas()
    elif isinstance(df, pd.DataFrame):
        if all(field in df.columns for field in sortfield):
            df = df.sort_values(
                by=sortfield,
                ascending=[sort_order != 'desc'] * len(sortfield)
            )
        df = df.head(obs)

    display(Markdown(f"**{label}**"))
    df_styled = df.style
    if hide_index:
        df_styled = df_styled.hide(axis='index')
    # Override pandas' default display.max_rows (60) and the Styler's
    # render.max_rows so that when the caller passes a large obs to
    # verify an entire code list, every row renders instead of the
    # first-5/last-5 summary view.
    with pd.option_context(
        'display.max_rows', obs,
        'styler.render.max_rows', obs,
    ):
        display(df_styled)


def showIU(df, obs=6, index=None, tenant=127, sortfield=None,
           sort_order='asc', label=None):
    """Display sample records filtered to a specific tenant.

    Expert Determination IUH tenant ID = 127; Safe Harbor IUH tenant ID = 82.
    Convenience wrapper around print_pd for quick IUHealth data inspection.

    Args:
        df: Spark DataFrame
        obs (int): Number of rows to display
        index (list): Index columns for sorting (default: ['personid'])
        tenant (int): Tenant ID to filter (127=IUH Expert, 82=IUH Safe Harbor)
        sortfield (list): Columns to sort by (defaults to index)
        sort_order (str): 'asc' or 'desc'
        label (str): Label for display header
    """
    if index is None:
        index = ['personid']
    if not sortfield:
        sortfield = index
    result = df
    if hasattr(df, 'columns') and 'tenant' in df.columns:
        result = result.filter(F.col('tenant') == tenant)

    print_pd(result, label=label, sortfield=sortfield, obs=obs,
             hide_index=True, sort_order=sort_order)


def count_people(df, description, person_id='personid', label2=''):
    """Count the number of unique people in a DataFrame and display the result.

    Args:
        df: Spark DataFrame
        description (str): Description for the table being counted
        person_id (str): Column containing person identifiers
        label2 (str): Additional label to display
    """
    count_p = df.select(person_id).distinct().count()
    display(Markdown(f"* {count_p:,}: Unique by {person_id} from Table {description}"))
    if label2:
        display(Markdown(label2))


def attrition(df, table_name, person_id='personid', description='',
              date_field=None):
    """Compute and display attrition statistics for a PySpark DataFrame.

    Shows record count, unique person count, and date range. This is the
    standalone function version (v0.1.0 style). The method version exists
    on SharedMethodsMixin.

    Args:
        df: PySpark DataFrame
        table_name (str): Name of the table being processed
        person_id (str or list): Column(s) containing person identifiers
        description (str): Description for display
        date_field (str or list): Optional date field(s) for range reporting
    """
    from datetime import datetime

    count = df.count()
    display(Markdown(f"**{description}**"))
    display(Markdown(f"* {count:,} records"))
    if isinstance(person_id, list):
        person_id = person_id[0]
    if person_id in df.columns:
        count_people(df=df, description=table_name, person_id=person_id)
    if date_field:
        if isinstance(date_field, str):
            date_stats = df.select(
                F.min(date_field), F.max(date_field)
            ).toPandas()
            display(Markdown(
                f"* Date range: {date_stats.iloc[0][0]} - {date_stats.iloc[0][1]}"
            ))
        elif isinstance(date_field, list):
            date_stats = df.select(
                [F.min(field) for field in date_field]
                + [F.max(field) for field in date_field]
            ).toPandas()
            display(Markdown(
                f"* Date range: {date_stats.iloc[0][0]} - {date_stats.iloc[0][1]}"
            ))
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def noRowNum(df, obs=6):
    """Display a pandas DataFrame with hidden index.

    Args:
        df: pandas DataFrame
        obs (int): Not used (kept for API compatibility)

    Returns:
        Styled pandas DataFrame with hidden index
    """
    return df.style.hide(axis='index')


def show_first(df, sort_field, merge_index):
    """Sort DataFrame, take top value of merge_index, filter to that value.

    Useful for inspecting all records belonging to the highest-count entity.

    Args:
        df: Spark DataFrame
        sort_field (str): Column to sort by (descending)
        merge_index (str): Column to extract value from first row, then filter
    """
    sorted_df = df.sort(F.col(sort_field).desc()).limit(10)
    sorted_pd_df = sorted_df.toPandas()
    first_value = sorted_pd_df.loc[0, merge_index]
    filtered_df = df.filter(F.col(merge_index) == first_value)
    filtered_df.show()
