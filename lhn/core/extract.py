"""
lhn/core/extract.py

Extract and ExtractItem classes for healthcare data extraction workflows.
Provides methods for creating, processing, and analyzing extracted data.
"""

from lhn.header import (
    spark, F, re, time, pd, pprint, display, Markdown,
    copy, get_logger, StringType, DataFrame
)
from lhn.core.shared_methods import SharedMethodsMixin
from spark_config_mapper import (
    writeTable, coalesce, setFunctionParameters, fields_reconcile,
    noColColide
)

logger = get_logger(__name__)


class Extract:
    """
    Container for ExtractItem instances representing project output tables.
    
    Given a dictionary of table configurations, creates an ExtractItem
    for each entry. Tables are accessible as attributes.
    
    Example:
        >>> e = Extract(project_tables_config)
        >>> e.cohort.df  # Access cohort DataFrame
        >>> e.demographics.write()  # Write to Spark table
    """
    
    def __init__(self, proj, debug=False):
        """
        Initialize Extract from configuration dictionary.
        
        Parameters:
            proj (dict): Dictionary of table name -> configuration object
            debug (bool): Enable debug logging
        """
        for key, value in proj.items():
            setattr(self, key, ExtractItem(value, debug=debug))
    
    def properties(self):
        """Return list of table names."""
        return [item for item in self.__dict__.keys() if not item.startswith('_')]
    
    def __iter__(self):
        """Iterate over table names."""
        return iter(self.properties())
    
    def __len__(self):
        return len(self.properties())


class ExtractItem(SharedMethodsMixin):
    """
    Represents a single extracted data table with processing methods.
    
    ExtractItem extends SharedMethodsMixin with healthcare-specific
    data extraction and analysis operations. It manages:
    - DataFrame loading and persistence
    - Index table creation (first/last record identification)
    - Cohort extraction and filtering
    - Clinical feature extraction
    
    Attributes inherited from configuration:
        - location: Spark table path
        - label: Human-readable description
        - csv/parquet: Export paths
        - indexFields: Key columns for deduplication
        - datefieldPrimary: Primary date field for temporal analysis
        - retained_fields: Fields to keep in extracts
    """
    
    def __init__(self, item, debug=False):
        """
        Initialize ExtractItem from configuration object.
        
        Parameters:
            item: Configuration object with table attributes
            debug (bool): Enable debug logging
        """
        # Copy all attributes from configuration
        self.__dict__ = item.__dict__.copy()
        self.debug = debug
        
        # Ensure required attributes have defaults
        if 'partitionBy' not in self.__dict__:
            self.partitionBy = None
        if 'df' not in self.__dict__:
            self._df = None
        
        if debug:
            if hasattr(self, 'csv'):
                logger.info(f"ExtractItem {getattr(self, 'name', 'unknown')}: csv={self.csv}")
            if hasattr(self, 'parquet'):
                logger.info(f"ExtractItem: parquet={self.parquet}")
    
    @property
    def df(self):
        """Lazy-load DataFrame from location if not set."""
        if not hasattr(self, '_df') or self._df is None:
            if hasattr(self, 'location'):
                try:
                    self._df = spark.table(self.location)
                except Exception as e:
                    logger.debug(f"Could not load {self.location}: {e}")
        return self._df if hasattr(self, '_df') else None
    
    @df.setter
    def df(self, value):
        self._df = value

    def _auto_write(self):
        """Write self.df to Hive if location and label are configured.

        Called by extract methods (create_extract, entityExtract, write_index_table,
        dict2pyspark) after they set self.df. NOT called by the df setter — manual
        df assignments don't auto-write (use .write() explicitly for those).

        Before writing, creates a temporary table to break read-write lineage.
        This avoids the Hive "Cannot overwrite table that is also being read from"
        error that occurs when the new df was derived from the same table.
        """
        if (self._df is not None
                and getattr(self, 'location', None)
                and getattr(self, 'label', None)):
            try:
                # Break lineage: write to a temp table first, then overwrite target.
                # This avoids "Cannot overwrite table being read from" when the new df
                # was derived from reading the same table (e.g., lazy-loaded then filtered).
                temp_name = self.location + "_tmp"
                self._df.write.saveAsTable(temp_name, mode='overwrite')
                temp_df = spark.table(temp_name)

                partition = getattr(self, 'partitionBy', None)
                writeTable(temp_df, self.location,
                           description=self.label, partitionBy=partition)

                # Clean up temp table
                try:
                    spark.sql(f"DROP TABLE IF EXISTS {temp_name}")
                except Exception:
                    pass

                self._df = spark.table(self.location)
                logger.info(f"Auto-wrote {self.location}")
            except Exception as ex:
                logger.warning(f"Auto-write failed for {self.location}: {ex}")
    
    def write_index_table(self, inTable, histStart=None, histEnd=None,
                          indexLabel="index", lastLabel="last",
                          filterSimple=True):
        """
        Create an index table identifying first/last records per entity.
        
        This method identifies the first (index) date for each entity
        (typically a person) and optionally filters by date range.
        
        Parameters:
            inTable: Source DataFrame or object with df attribute
            histStart: Start date filter (inclusive)
            histEnd: End date filter (inclusive)
            indexLabel (str): Prefix for first date column
            lastLabel (str): Prefix for last date column
            filterSimple (bool): Use simple date filtering
        
        Sets:
            self.df: DataFrame with index records
        """
        from lhn.cohort import write_index_table
        from spark_config_mapper.utils import unique_non_none
        
        histStart = coalesce(histStart, getattr(self, 'histStart', None))
        histEnd = coalesce(histEnd, getattr(self, 'histEnd', None))
        
        datefieldStop = unique_non_none(
            getattr(self, 'datefieldStop', None),
            getattr(self, 'datefieldPrimary', None)
        )
        if datefieldStop:
            datefieldStop = datefieldStop[0]
        
        # Get DataFrame from input
        if hasattr(inTable, 'df'):
            inTabledf = inTable.df
        else:
            inTabledf = inTable
        
        self.df = write_index_table(
            inTable=inTabledf,
            index_field=getattr(self, 'indexFields', ['personid']),
            retained_fields=getattr(self, 'retained_fields', []),
            datefieldPrimary=getattr(self, 'datefieldPrimary', []),
            datefieldStop=datefieldStop,
            code=getattr(self, 'code', ''),
            sort_fields=getattr(self, 'sort_fields', []),
            histStart=histStart,
            histEnd=histEnd,
            max_gap=getattr(self, 'max_gap', None),
            indexLabel=indexLabel,
            lastLabel=lastLabel
        )
        self._auto_write()

    def load_csv_as_df(self, dtype=None, names=None, header=0,
                       drop_missing=False, how='any', subset=None,
                       thresh=None, normalize_unicode=True):
        """
        Load a CSV file as the DataFrame.
        
        Parameters:
            dtype: Data types for columns
            names: Column names (if no header)
            header: Header row number
            drop_missing (bool): Drop rows with missing values
            how (str): How to determine missing ('any' or 'all')
            subset: Columns to check for missing
            thresh: Minimum non-null count
            normalize_unicode (bool): Normalize unicode strings
        """
        import unicodedata
        
        if not hasattr(self, 'csv'):
            logger.error("No CSV path configured")
            return
        
        logger.info(f"Loading CSV: {self.csv}")
        
        pandas_df = pd.read_csv(
            self.csv, dtype=dtype, header=header, names=names,
            skipinitialspace=True
        )
        
        if normalize_unicode:
            for column in pandas_df.columns:
                if pandas_df[column].dtype == object:
                    pandas_df[column] = pandas_df[column].apply(
                        lambda s: unicodedata.normalize('NFC', str(s)) 
                        if pd.notna(s) else s
                    )
        
        if drop_missing:
            pandas_df = pandas_df.dropna(how=how, subset=subset, thresh=thresh)
        
        self.df = spark.createDataFrame(pandas_df)
        
        # Optionally save to Spark table
        if hasattr(self, 'location') and hasattr(self, 'label'):
            self.df.write.saveAsTable(
                name=self.location, mode='overwrite'
            )
            self.df = spark.table(self.location)
    
    def identify_target_records(self, entitySource, elementIndex,
                                 datefieldSource=None, histStart=None,
                                 histStop=None, datefieldElement=None,
                                 cacheResult=True, broadcast_flag=True,
                                 masterList=None, howjoin='inner'):
        """
        Extract records from a source table based on this extract's index.
        
        Uses self.df as the index to filter entitySource.
        
        Parameters:
            entitySource: Source DataFrame to extract from
            elementIndex (list): Columns for join
            datefieldSource (str): Date column in source for filtering
            histStart: Start date/offset
            histStop: Stop date/offset
            datefieldElement (str): Date column in self.df for offsets
            cacheResult (bool): Cache result DataFrame
            broadcast_flag (bool): Broadcast index for join
            masterList (list): Columns to include in output
            howjoin (str): Join type
        
        Returns:
            DataFrame: Extracted records
        """
        from lhn.cohort import identify_target_records
        
        return identify_target_records(
            entitySource=entitySource,
            elementIndex=elementIndex,
            elementExtract=self.df,
            datefieldSource=datefieldSource,
            histStart=histStart,
            histStop=histStop,
            datefieldElement=datefieldElement,
            cacheResult=cacheResult,
            broadcast_flag=broadcast_flag,
            masterList=masterList,
            howjoin=howjoin
        )
    
    def entityExtract(self, elementList, entitySource, elementIndex=None,
                      datefieldSource=None, histStart=None, histStop=None,
                      datefieldElement=None, masterList=None,
                      howjoin='inner', cacheResult=True, broadcast_flag=True,
                      set_self_df=True,
                      cohort=None, cohortColumns=None,
                      howCohortJoin='inner'):
        """
        Extract records from a source table using an element list as the index.

        Matches v0.1.0 signature: first arg = element list (small, the keys),
        second arg = entity source (large, the table to extract from).
        Result is assigned to self.df.

        Parameters:
            elementList: Element/index table (ExtractItem or DataFrame) — the
                small table of keys used to filter the source.
            entitySource: Source DataFrame (or object with df attribute) — the
                large table to extract from.
            elementIndex (list): Columns for join (default: elementList.indexFields)
            histStart: Start date/offset
            histStop: Stop date/offset
            datefieldElement (str): Date column in self.df for offsets
            masterList (list): Columns to include in output
            howjoin (str): Join type
            cacheResult (bool): Cache result DataFrame
            broadcast_flag (bool): Broadcast index for join
            set_self_df (bool): If True, assign result back to self.df
            cohort: Optional cohort object (with .df, .indexFields) to
                filter entitySource to cohort members before extraction.
                Restored from v0.1.0 — this parameter was lost in refactoring.
            cohortColumns (list): Columns to select from cohort.df. If None,
                uses cohort.df.columns.
            howCohortJoin (str): Join type for cohort join (default: 'inner')

        Returns:
            DataFrame: Extracted records
        """
        # Get index fields from the elementList (the keys table)
        if elementIndex is None:
            elementIndex = getattr(elementList, 'indexFields',
                                   getattr(self, 'indexFields', ['personid']))

        # Get the actual DataFrames
        if hasattr(elementList, 'df'):
            element_df = elementList.df
        else:
            element_df = elementList

        if hasattr(entitySource, 'df'):
            entity_df = entitySource.df
        else:
            entity_df = entitySource

        # Cohort filtering (restored from v0.1.0)
        if cohort is not None:
            logger.info("entityExtract: Using cohort filter")

            if cohortColumns is None:
                if hasattr(self, 'cohortColumns'):
                    cohortColumns = self.cohortColumns
                else:
                    cohortColumns = cohort.df.columns

            # Reconcile columns to prevent duplicates
            entitySourceSelect = noColColide(
                entity_df.columns, cohortColumns,
                cohort.indexFields, masterList=None
            )

            entity_df = (
                entity_df.select(entitySourceSelect)
                .join(
                    cohort.df.select(cohortColumns),
                    on=cohort.indexFields,
                    how=howCohortJoin
                )
            )

        # Call identify_target_records with elementList as the extract (index table)
        from lhn.cohort import identify_target_records
        result = identify_target_records(
            entitySource=entity_df,
            elementIndex=elementIndex,
            elementExtract=element_df,
            datefieldSource=datefieldSource,
            histStart=histStart,
            histStop=histStop,
            datefieldElement=datefieldElement,
            cacheResult=cacheResult,
            broadcast_flag=broadcast_flag,
            masterList=masterList,
            howjoin=howjoin
        )

        if set_self_df and result is not None:
            self.df = result
            self._auto_write()

        return result

    def writeTBL(self):
        """Write DataFrame to Spark table. Alias for v0.1.0 compatibility.

        Skips if DataFrame is empty. Re-reads from table after writing
        to ensure Spark metadata is consistent.
        """
        if not hasattr(self, 'df') or self.df is None:
            logger.error("No DataFrame to write")
            return

        if self.df.rdd.isEmpty():
            print("DataFrame is empty. Skipping write operation.")
        else:
            partition = getattr(self, 'partitionBy', None)
            description = getattr(self, 'label', '')
            writeTable(
                self.df, self.location,
                description=description, partitionBy=partition
            )
            self.df = spark.table(self.location)

    def create_extract(self, elementList=None, elementListSource=None,
                       find_method=None, sourceField=None):
        """
        Create an extract by matching elements against a source.

        All parameters fall back to config-defined attributes (from YAML) when
        not passed explicitly. This enables config-driven extraction where the
        notebook just calls ``e.foo.create_extract(e.bar, d.baz.df)`` and
        sourceField, retained_fields, etc. come from the YAML.

        Parameters:
            elementList: List/dict/ExtractItem of search patterns.
                Falls back to self.elementList.
            elementListSource: DataFrame to search (typically a dictionary table).
                Falls back to self.elementListSource (must be a DataFrame, not a string).
            find_method (str): 'regex' or 'merge'. Falls back to self.find_method,
                then defaults to 'regex'.
            sourceField (str): Field to search in (for regex). Falls back to
                self.sourceField.

        Sets:
            self.df: Matched records, filtered to retained_fields if configured.
        """
        # Read config defaults for any unspecified parameters
        # Use 'is None' not 'or' to avoid swallowing falsy-but-intentional values
        if find_method is None:
            find_method = getattr(self, 'find_method', 'regex')
        if sourceField is None:
            sourceField = getattr(self, 'sourceField', None)

        if find_method == 'regex':
            if sourceField is None:
                raise ValueError(
                    f"sourceField is required for regex extraction on "
                    f"'{getattr(self, 'name', 'unknown')}'. "
                    f"Pass it explicitly or define 'sourceField' in config."
                )
            self._extract_by_regex(elementList, elementListSource, sourceField)
        elif find_method == 'merge':
            self._extract_by_merge(elementList, elementListSource)
        else:
            raise ValueError(f"Unknown find_method: '{find_method}'. Use 'regex' or 'merge'.")

        # Apply retained_fields filter if configured
        retained = getattr(self, 'retained_fields', None)
        if retained and self.df is not None:
            # Always keep join keys and group column alongside retained fields
            index_fields = getattr(self, 'indexFields', [])
            keep = list(set(retained + index_fields))
            # Add 'group' if it exists (from regex group preservation)
            if 'group' in self.df.columns and 'group' not in keep:
                keep.append('group')
            # Only select columns that actually exist in the DataFrame
            valid = [c for c in keep if c in self.df.columns]
            if valid:
                self.df = self.df.select(valid)

        # Auto-persist to Hive
        self._auto_write()

    def _extract_by_regex(self, patterns, source, field):
        """Extract records matching regex patterns, preserving group labels.

        Uses iterate-per-group + union approach: each pattern is applied
        individually with its group label, then results are unioned. This
        preserves which group each matched row belongs to.

        For small pattern tables (typical: 4-20 patterns), this is efficient
        and avoids cross-join or combined-regex approaches that lose grouping.

        Parameters:
            patterns: ExtractItem (with .df containing 'group' + pattern columns),
                dict (group_name -> pattern), or list of patterns.
            source: DataFrame to search (should be a dictionary table, not
                patient-level data, for performance).
            field (str): Column in source to match against.
        """
        if hasattr(patterns, 'df'):
            # ExtractItem with DataFrame — has 'group' and pattern columns
            list_index = getattr(patterns, 'listIndex', 'codes')
            rows = patterns.df.select('group', list_index).collect()
            group_patterns = [(row['group'], row[list_index]) for row in rows]
        elif isinstance(patterns, dict):
            # dict: group_name -> regex_pattern
            group_patterns = list(patterns.items())
        elif isinstance(patterns, list):
            # plain list — no group info, assign sequential group names
            group_patterns = [(f'group_{i}', p) for i, p in enumerate(patterns)]
        else:
            group_patterns = [('default', str(patterns))]

        # Iterate per group, filter, add group label, union
        results = []
        for group_name, pattern in group_patterns:
            matched = source.filter(F.col(field).rlike(pattern))
            matched = matched.withColumn('group', F.lit(group_name))
            results.append(matched)

        if results:
            self.df = results[0]
            for additional in results[1:]:
                try:
                    # PySpark >= 3.1
                    self.df = self.df.unionByName(additional, allowMissingColumns=True)
                except TypeError:
                    # PySpark 2.4 — align schemas manually before union
                    all_cols = set(self.df.columns) | set(additional.columns)
                    def _align(df, cols):
                        return df.select([
                            F.col(c) if c in df.columns else F.lit(None).alias(c)
                            for c in sorted(cols)
                        ])
                    self.df = _align(self.df, all_cols).union(_align(additional, all_cols))
        else:
            logger.warning("No patterns provided to _extract_by_regex")
            self.df = source.limit(0)

    def _extract_by_merge(self, element_list, source):
        """Extract records by merging with element list."""
        if hasattr(element_list, 'df'):
            merge_df = element_list.df
        elif isinstance(element_list, list):
            merge_df = spark.createDataFrame(
                [(x,) for x in element_list], ['value']
            )
        else:
            merge_df = element_list

        join_col = getattr(self, 'listIndex',
                           getattr(self, 'merge_column', 'code'))
        self.df = source.join(merge_df, on=join_col, how='inner')
    
    def dict2pyspark(self, columnname=['codes']):
        """
        Convert a dictionary attribute to DataFrame.
        
        Parameters:
            columnname (list): Column names for the values
        """
        if hasattr(self, 'dictionary'):
            from spark_config_mapper.utils.pandas import dict2Pandas
            self.pd = dict2Pandas(self.dictionary, columnname=columnname)
            self.pd = self.pd.reset_index().rename(columns={'index': 'group'})
            self.df = spark.createDataFrame(self.pd)
            self._auto_write()
