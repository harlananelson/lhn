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

    Identifies the first (index) and last dates for each entity (typically a
    patient). When ``max_gap`` is specified, also computes therapy segmentation:
    periods of continuous care separated by gaps exceeding ``max_gap`` days.

    Parameters:
        inTable (DataFrame): Source data with events
        index_field (list): Fields defining unique entities (e.g., ['personid'])
        retained_fields (list): Additional fields to retain
        datefieldPrimary (list or str): Date field(s) for ordering
        code (str): Tag for naming output fields (e.g., 'SCD')
        datefieldStop (str, optional): End date field (e.g., medication stop date).
            If None, uses datefieldPrimary.
        sort_fields (list, optional): Additional sort criteria
        histStart (str): Start date filter
        histEnd (str): End date filter
        max_gap (int, optional): Maximum gap in days between events to consider
            as same therapy course. If None, no therapy segmentation. Default 370
            in config.
        indexLabel (str): Prefix for first date column (e.g., 'index_')
        lastLabel (str): Prefix for last date column (e.g., 'last_')

    Returns:
        DataFrame: Index table with one row per entity (or per entity per
        therapy course if max_gap is specified).

    Output columns:
        - ``{indexLabel}{code}`` — first event date (overall)
        - ``{lastLabel}{code}`` — last event date (overall)
        - ``entries_{code}`` — total event count
        - ``course_of_therapy`` — therapy segment number (if max_gap set)
        - ``{indexLabel}therapy_{code}`` — first date per therapy (if max_gap set)
        - ``{lastLabel}therapy_{code}`` — last date per therapy (if max_gap set)
        - ``encounter_days_for_course`` — distinct event days per therapy (if max_gap set)
        - ``max_gap`` — longest gap within therapy (if max_gap set)
    """
    # Ensure list format for fields
    if isinstance(index_field, str):
        index_field = [index_field]
    if isinstance(datefieldPrimary, str):
        datefieldPrimary = [datefieldPrimary]

    datefieldPrimary_col = datefieldPrimary[0]

    # datefieldStop defaults to datefieldPrimary
    if datefieldStop is None:
        datefieldStop = datefieldPrimary_col

    if code is None:
        code = ""

    # Apply date filters
    df = inTable
    if histStart:
        df = df.filter(F.col(datefieldPrimary_col) >= histStart)
        df = df.filter(F.col(datefieldPrimary_col).isNotNull())
    if histEnd:
        df = df.filter(F.col(datefieldPrimary_col) <= histEnd)
        df = df.filter(F.col(datefieldPrimary_col).isNotNull())

    # Add unique ID for tie-breaking in ordering windows.
    # monotonically_increasing_id() is NOT stable across re-runs after a
    # shuffle (the ids depend on partition layout and can change), so
    # retained_fields that happen to be chosen by an id-tiebreak can
    # differ across runs. Prefer a stable hash of the natural keys + date
    # as tiebreak; this gives reproducible choices without dependence on
    # Spark's physical plan.
    _tiebreak_inputs = [F.coalesce(F.col(c).cast("string"), F.lit(""))
                        for c in index_field + [datefieldPrimary_col]]
    df = df.withColumn("_unique_id", F.sha2(F.concat_ws("|", *_tiebreak_inputs), 256))

    # Prepare column names
    indexName = indexLabel + code
    lastName = lastLabel + code

    # Prepare retained fields (exclude index + date to avoid duplicates)
    select_cols = list(index_field)
    if retained_fields:
        retained_clean = [f for f in retained_fields
                          if f not in index_field and f in df.columns]
        select_cols.extend(retained_clean)

    # Calculate distinct encounter days per person
    distinct_dates = df.select(*index_field, datefieldPrimary_col).distinct()
    distinct_counts = distinct_dates.groupBy(*index_field).agg(
        F.countDistinct(datefieldPrimary_col).alias('encounter_days')
    )

    # Windows
    windowSpec = (
        Window.partitionBy(index_field)
        .orderBy(F.asc_nulls_last(datefieldPrimary_col),
                 F.asc_nulls_last(datefieldStop))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    windowSpecLead = (
        Window.partitionBy(*index_field)
        .orderBy(F.desc_nulls_last(datefieldPrimary_col),
                 F.asc_nulls_last(datefieldStop))
    )
    windowSpecDistinctDate = (
        Window.partitionBy(*index_field, datefieldPrimary_col)
        .orderBy('_unique_id')
    )

    # Stage 1: Per-person overall dates + gap calculation
    cohort = (
        df
        .select([*select_cols, datefieldPrimary_col, '_unique_id'])
        .withColumn(datefieldStop,
                    F.greatest(
                        F.coalesce(F.col(datefieldStop), F.col(datefieldPrimary_col)),
                        F.col(datefieldPrimary_col)))
        .withColumn('_rownum_date', F.row_number().over(windowSpecDistinctDate))
        .withColumn(indexName,
                    F.first(datefieldPrimary_col, ignorenulls=True).over(windowSpec))
        .withColumn(lastName,
                    F.last(datefieldStop, ignorenulls=True).over(windowSpec))
        .withColumn('entries_' + code,
                    F.count(index_field[0]).over(windowSpec))
        .filter(F.col('_rownum_date') == 1)
        .join(F.broadcast(distinct_counts), on=index_field, how='left')
    )

    if max_gap is None:
        # No therapy segmentation — return one row per person
        final_cols = [*select_cols, indexName, lastName, 'entries_' + code,
                      'encounter_days']
        valid_cols = [c for c in final_cols if c in cohort.columns]
        windowSpecPerson = (
            Window.partitionBy(*index_field)
            .orderBy(F.col(datefieldPrimary_col).asc_nulls_last(), '_unique_id')
        )
        return (
            cohort
            .withColumn('_rownum', F.row_number().over(windowSpecPerson))
            .filter(F.col('_rownum') == 1)
            .select(valid_cols)
            .distinct()
        )

    # --- Therapy segmentation (max_gap specified) ---

    indexTherapyName = indexLabel + "therapy_" + code
    lastTherapyName = lastLabel + "therapy_" + code

    # Calculate gap to next encounter (using descending lead = ascending lag)
    cohort = (
        cohort
        .withColumn('_next_date', F.lag(datefieldPrimary_col).over(windowSpecLead))
        .withColumn('_days_to_next',
                    F.datediff(F.col('_next_date'), F.col(datefieldStop)))
        .withColumn('_last_course_day',
                    F.when(F.col('_days_to_next') <= max_gap, False)
                    .when(F.col(datefieldStop) == F.col(lastName), False)
                    .otherwise(True))
        .withColumn('course_of_therapy',
                    F.sum(F.when(F.col('_last_course_day'), 1).otherwise(0))
                    .over(windowSpecLead))
    )

    # Stage 2: Per-therapy first/last dates
    windowSpec_therapy = (
        Window.partitionBy([*index_field, 'course_of_therapy'])
        .orderBy(F.asc_nulls_last(datefieldPrimary_col),
                 F.asc_nulls_last(datefieldStop))
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    windowSpecTherapyRow = (
        Window.partitionBy(*index_field, 'course_of_therapy')
        .orderBy(F.col(datefieldPrimary_col).asc_nulls_last(),
                 F.asc_nulls_last(datefieldStop), '_unique_id')
    )

    cohort = (
        cohort
        .withColumn(indexTherapyName,
                    F.first(datefieldPrimary_col, ignorenulls=True)
                    .over(windowSpec_therapy))
        .withColumn(lastTherapyName,
                    F.last(datefieldStop, ignorenulls=True)
                    .over(windowSpec_therapy))
        .withColumn('_rownum_therapy',
                    F.row_number().over(windowSpecTherapyRow))
        .withColumn('max_gap',
                    F.max(F.when(~F.col('_last_course_day'),
                                 F.col('_days_to_next')))
                    .over(windowSpec_therapy))
        .filter(F.col('_rownum_therapy') == 1)
    )

    # Stage 3: Encounter days per therapy course
    therapy_counts = (
        cohort
        .groupBy(*index_field, 'course_of_therapy')
        .agg(F.countDistinct(datefieldPrimary_col)
             .alias('encounter_days_for_course'))
    )

    # Final assembly
    final_cols = [*select_cols, indexName, lastName, 'entries_' + code,
                  'encounter_days', 'course_of_therapy',
                  indexTherapyName, lastTherapyName,
                  'encounter_days_for_course', 'max_gap']

    cohort = (
        cohort
        .join(therapy_counts, on=[*index_field, 'course_of_therapy'], how='left')
    )

    valid_cols = [c for c in final_cols if c in cohort.columns]
    return (
        cohort
        .select(valid_cols)
        .sort([*index_field, datefieldPrimary_col])
    )


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
        raise ValueError(
            f"identify_target_records: entitySource must be a DataFrame, "
            f"got {type(entitySource).__name__}"
        )

    if not isinstance(elementExtract, DataFrame):
        # Previously this logged a warning and returned elementExtract
        # as-is. That silently propagated None up to the caller (typically
        # via entityExtract), which then assigned None to self.df — the
        # real failure surfaced three frames later as AttributeError. An
        # inner-join against a non-DataFrame index is never valid; refuse.
        raise ValueError(
            f"identify_target_records: elementExtract must be a DataFrame, "
            f"got {type(elementExtract).__name__}. The index table has no "
            f"DataFrame — check whether create_extract() was called and "
            f"produced a non-empty result, or whether the Item's status is "
            f"'ITEM_FAILED'."
        )
    
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
