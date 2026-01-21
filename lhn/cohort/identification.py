"""
lhn/cohort/identification.py

Cohort identification functions - identifying index events and target records.
"""

from lhn.header import (
    F, DataFrame, Window, get_logger, pformat, display, Markdown
)
from spark_config_mapper import noColColide, distCol

logger = get_logger(__name__)

# Join type constant
JOIN_INNER = 'inner'


def write_index_table(inTable, index_field, retained_fields, datefieldPrimary,
                      code, datefieldStop=None, sort_fields=None,
                      histStart=None, histEnd=None, max_gap=None,
                      indexLabel="index_", lastLabel="last_"):
    """
    Create a cohort-style index table identifying first/last events per entity.
    
    This function identifies the first (index) and last dates for each entity
    (typically a patient) based on the specified date field, with optional
    date filtering.
    
    Parameters:
        inTable (DataFrame): Source data with events
        index_field (list): Fields defining unique entities (e.g., ['personid'])
        retained_fields (list): Additional fields to retain
        datefieldPrimary (list or str): Date field(s) for ordering
        code (str): Tag for naming output fields
        datefieldStop (str, optional): End date field for range
        sort_fields (list, optional): Additional sort criteria
        histStart (str): Start date filter
        histEnd (str): End date filter
        max_gap (int, optional): Maximum gap between events
        indexLabel (str): Prefix for first date column
        lastLabel (str): Prefix for last date column
    
    Returns:
        DataFrame: Index table with one row per entity
    
    Example:
        >>> cohort = write_index_table(
        ...     encounters,
        ...     index_field=['personid'],
        ...     retained_fields=['tenant', 'facilityid'],
        ...     datefieldPrimary=['servicedate'],
        ...     code='enc'
        ... )
    """
    # Ensure list format for fields
    if isinstance(index_field, str):
        index_field = [index_field]
    if isinstance(datefieldPrimary, str):
        datefieldPrimary = [datefieldPrimary]
    
    datefieldPrimary_col = datefieldPrimary[0]
    
    # Apply date filters
    df = inTable
    if histStart and histEnd:
        df = df.filter(F.col(datefieldPrimary_col).between(histStart, histEnd))
    elif histStart:
        df = df.filter(F.col(datefieldPrimary_col) >= histStart)
    elif histEnd:
        df = df.filter(F.col(datefieldPrimary_col) <= histEnd)
    
    # Add unique ID for deterministic ordering
    df = df.withColumn("_unique_id", F.monotonically_increasing_id())
    
    # Define window for first/last aggregations
    windowSpec = (
        Window.partitionBy(index_field)
        .orderBy(datefieldPrimary_col, '_unique_id')
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    
    # Window for row number
    windowSpecRow = (
        Window.partitionBy(index_field)
        .orderBy(datefieldPrimary_col, '_unique_id')
    )
    
    # Prepare columns to select
    select_cols = [*index_field]
    if retained_fields:
        retained_clean = [f for f in retained_fields if f not in index_field]
        select_cols.extend(retained_clean)
    
    # Build the index table
    cohort_index = (
        df
        .select([*select_cols, datefieldPrimary_col, '_unique_id'])
        .distinct()
        .withColumn(indexLabel + code, 
                    F.first(datefieldPrimary_col, ignorenulls=True).over(windowSpec))
        .withColumn(lastLabel + code,
                    F.last(datefieldPrimary_col, ignorenulls=True).over(windowSpec))
        .withColumn('entries_' + code,
                    F.count(index_field[0]).over(windowSpec))
        .withColumn('_rownum', F.row_number().over(windowSpecRow))
        .filter(F.col('_rownum') == 1)
        .select([*select_cols, indexLabel + code, lastLabel + code, 'entries_' + code])
        .distinct()
    )
    
    return cohort_index


def identify_target_records(entitySource, elementIndex, elementExtract,
                            datefieldSource=None, histStart=None, histStop=None,
                            datefieldElement=None, cacheResult=True,
                            broadcast_flag=True, masterList=None,
                            howjoin=JOIN_INNER):
    """
    Extract records from a source table based on an index table.
    
    Uses elementExtract as the index to filter entitySource, with optional
    date-based filtering relative to dates in the index.
    
    Parameters:
        entitySource (DataFrame): Source table to extract from
        elementIndex (list): Columns for join
        elementExtract (DataFrame): Index table defining entities to extract
        datefieldSource (str): Date field in source for filtering
        histStart: Start date or offset from datefieldElement
        histStop: Stop date or offset from datefieldElement
        datefieldElement (str): Date field in index for relative offsets
        cacheResult (bool): Cache the result
        broadcast_flag (bool): Broadcast the index table
        masterList (list): Columns to include in output
        howjoin (str): Join type
    
    Returns:
        DataFrame: Extracted records
    
    Example:
        >>> # Get conditions within 1 year of index date
        >>> conditions = identify_target_records(
        ...     entitySource=all_conditions,
        ...     elementIndex=['personid'],
        ...     elementExtract=cohort,
        ...     datefieldSource='effectiveDate',
        ...     histStart=-365,
        ...     histStop=365,
        ...     datefieldElement='indexDate'
        ... )
    """
    if not isinstance(entitySource, DataFrame):
        raise ValueError("entitySource must be a DataFrame")
    
    if not isinstance(elementExtract, DataFrame):
        logger.warning("elementExtract is not a DataFrame, returning as-is")
        return elementExtract
    
    logger.info(f"identify_target_records: index={elementIndex}")
    
    # Resolve columns to avoid collisions
    entitySourceSelect = noColColide(
        entitySource.columns, elementExtract.columns, 
        elementIndex, masterList=masterList
    )
    elementExtractSelect = noColColide(
        elementExtract.columns, entitySourceSelect,
        elementIndex, masterList=masterList
    )
    
    # Prepare index table
    indextable = elementExtract.select(elementExtractSelect).distinct()
    if broadcast_flag:
        indextable = F.broadcast(indextable)
    
    # Perform join
    elements = entitySource.select(entitySourceSelect).join(
        indextable, on=elementIndex, how=howjoin
    )
    
    # Check for empty result
    if elements.limit(1).count() == 0:
        logger.warning("Join produced no records")
        return None
    
    # Handle date offsets
    if datefieldElement and histStart is not None and histStop is not None:
        if isinstance(histStart, int):
            histStart = F.date_add(F.col(datefieldElement), histStart)
        if isinstance(histStop, int):
            histStop = F.date_add(F.col(datefieldElement), histStop)
        
        # Apply date filter
        if datefieldSource:
            elements = elements.filter(
                F.col(datefieldSource).between(histStart, histStop)
            )
    
    if cacheResult:
        elements = elements.cache()
    
    logger.info(f"identify_target_records returned {len(elements.columns)} columns")
    return elements


def calcUsage(df, fields, dateCoalesce, minDate, maxDate, index,
              countFieldName='count', dateFirst='dateFirst', dateLast='dateLast'):
    """
    Calculate usage statistics (count, first date, last date) per entity.
    
    Parameters:
        df (DataFrame): Source data
        fields (list): Fields to select
        dateCoalesce: Column expression for coalesced date
        minDate (str): Minimum date filter
        maxDate (str): Maximum date filter
        index (list): Grouping columns
        countFieldName (str): Name for count column
        dateFirst (str): Name for first date column
        dateLast (str): Name for last date column
    
    Returns:
        DataFrame: Usage statistics per entity
    """
    result = (
        df
        .select(fields)
        .withColumn('_date', dateCoalesce)
        .filter(F.col('_date').between(minDate, maxDate))
        .groupBy(index)
        .agg(
            F.count('*').alias(countFieldName),
            F.min('_date').alias(dateFirst),
            F.max('_date').alias(dateLast)
        )
    )
    return result


def identifyLevel(df, levelField, levelValue, index):
    """
    Filter records to a specific level value.
    
    Parameters:
        df (DataFrame): Source data
        levelField (str): Field containing level values
        levelValue: Value to filter for
        index (list): Fields for distinct selection
    
    Returns:
        DataFrame: Filtered records
    """
    return (
        df
        .filter(F.col(levelField) == levelValue)
        .select(distCol(index))
        .distinct()
    )
