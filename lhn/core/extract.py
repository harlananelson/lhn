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
    writeTable, coalesce, setFunctionParameters, fields_reconcile
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
    
    def entityExtract(self, entitySource, elementIndex=None,
                      datefieldSource=None, histStart=None, histStop=None,
                      datefieldElement=None, masterList=None,
                      howjoin='inner', cacheResult=True, broadcast_flag=True,
                      set_self_df=True):
        """
        Extract records from a source table using this item as the index.

        Convenience wrapper around identify_target_records that uses
        self.df as the index (cohort/element) table.

        Parameters:
            entitySource: Source DataFrame (or object with df attribute)
            elementIndex (list): Columns for join (default: self.indexFields)
            datefieldSource (str): Date column in source for filtering
            histStart: Start date/offset
            histStop: Stop date/offset
            datefieldElement (str): Date column in self.df for offsets
            masterList (list): Columns to include in output
            howjoin (str): Join type
            cacheResult (bool): Cache result DataFrame
            broadcast_flag (bool): Broadcast index for join
            set_self_df (bool): If True, assign result back to self.df

        Returns:
            DataFrame: Extracted records
        """
        if elementIndex is None:
            elementIndex = getattr(self, 'indexFields', ['personid'])

        result = self.identify_target_records(
            entitySource=entitySource,
            elementIndex=elementIndex,
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

        return result

    def create_extract(self, elementList, elementListSource,
                       find_method='regex', sourceField=None):
        """
        Create an extract by matching elements against a source.
        
        Parameters:
            elementList: List/dict/ExtractItem of search patterns
            elementListSource: DataFrame to search
            find_method (str): 'regex' or 'merge'
            sourceField (str): Field to search in (for regex)
        
        Sets:
            self.df: Matched records
        """
        # Implementation depends on find_method
        if find_method == 'regex':
            self._extract_by_regex(elementList, elementListSource, sourceField)
        elif find_method == 'merge':
            self._extract_by_merge(elementList, elementListSource)
    
    def _extract_by_regex(self, patterns, source, field):
        """Extract records matching regex patterns."""
        if isinstance(patterns, list):
            pattern_list = patterns
        elif isinstance(patterns, dict):
            pattern_list = list(patterns.values())
        elif hasattr(patterns, 'df'):
            pattern_list = patterns.df.select(
                getattr(patterns, 'listIndex', 'pattern')
            ).rdd.flatMap(lambda x: x).collect()
        else:
            pattern_list = [str(patterns)]
        
        # Build combined regex
        combined = '|'.join(f'({p})' for p in pattern_list)
        
        self.df = source.filter(F.col(field).rlike(combined))
    
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
        
        join_col = getattr(self, 'merge_column', 'code')
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
