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

    def __getitem__(self, name):
        """Access an ExtractItem by name.

        Handles config-derived names that are not valid Python identifiers
        (hyphens, leading digits, etc.) which ``getattr`` can't see.
        Raises KeyError (not AttributeError) to match dict semantics so
        callers can ``.get()`` safely.
        """
        if name in self.__dict__:
            return self.__dict__[name]
        raise KeyError(
            "Extract has no item '{}' (available: {})".format(
                name, sorted(self.properties())[:10]))

    def __contains__(self, name):
        return name in self.__dict__ and not name.startswith('_')

    def write_all(self, names=None, skip_empty=True, skip_clean=True,
                  verbose=True):
        """
        Write extract tables to the project schema in one call.

        Replaces the recurring per-notebook pattern::

            for name in ['a', 'b', 'c']:
                item = getattr(e, name, None)
                if item is not None and item.df is not None:
                    item.write()

        By default this only writes items whose in-memory ``df`` differs
        from what was last persisted. Every extract method
        (``create_extract``, ``entityExtract``, ``write_index_table``,
        ``dict2pyspark``, ``extract_concept_flags``, ``extract_concept_events``)
        already call
        ``_auto_write`` internally, so their outputs are skipped here —
        re-writing them would be identical bytes and cost the same as
        the first write. Items that had ``.df`` mutated directly
        (``e.foo.df = e.foo.df.withColumn(...)``) are still written,
        because the ``df`` setter marks them dirty. Pass
        ``skip_clean=False`` to force-rewrite everything.

        Parameters:
            names (list|None): Subset of table names to write. If None, writes all
                properties (i.e. every ExtractItem attached to this Extract).
            skip_empty (bool): If True (default), silently skip items whose ``df`` is
                None. If False, call ``write()`` anyway and let it raise.
            skip_clean (bool): If True (default), skip items whose dirty
                flag is False — their disk copy already matches the
                in-memory df. Set False to force a re-write of every
                selected item.
            verbose (bool): If True (default), print one line per table with row count
                and destination location.

        Returns:
            dict: name -> row count written (or None for skipped items).
        """
        names = names if names is not None else self.properties()
        results = {}
        for name in names:
            item = getattr(self, name, None)
            if item is None:
                if verbose:
                    print(f"  {name}: SKIPPED (no such extract)")
                results[name] = None
                continue
            if skip_empty and (getattr(item, 'df', None) is None):
                if verbose:
                    print(f"  {name}: SKIPPED (no data)")
                results[name] = None
                continue
            # Default True when the attribute is missing — a conservative
            # choice that preserves the pre-dirty-flag "always write"
            # behavior for any legacy item that didn't pass through
            # __init__.
            is_dirty = getattr(item, '_dirty', True)
            if skip_clean and not is_dirty:
                if verbose:
                    location = getattr(item, 'location', '?')
                    print(f"  {name}: SKIPPED (already persisted -> {location})")
                results[name] = None
                continue
            item.write()
            count = item.df.count() if item.df is not None else 0
            location = getattr(item, 'location', '?')
            if verbose:
                print(f"  {name}: {count:,} records -> {location}")
            results[name] = count
        return results


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
        # Dirty-tracking flag for write_all to skip items whose in-memory
        # df already matches Hive. Set True by the df setter on any
        # assignment; cleared by _auto_write / write after a successful
        # persist. A freshly constructed Item has nothing to write.
        self._dirty = False

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
        """Set the underlying Spark DataFrame.

        Does NOT call :meth:`_auto_write`; manual assignments to ``self.df``
        stay in-memory only. Use :meth:`write` explicitly to persist, or call
        one of the extract methods (``create_extract``, ``entityExtract``,
        ``write_index_table``, ``dict2pyspark``) which auto-write after
        setting ``self.df``.

        Marks the item dirty. :meth:`Extract.write_all` uses the dirty flag
        to skip items whose in-memory df already matches Hive (i.e. were
        already persisted by an extract method's auto-write and haven't
        been mutated since).

        Parameters:
            value (pyspark.sql.DataFrame): DataFrame to store on this item.
        """
        self._df = value
        self._dirty = True

    def write_safe(self):
        """Persist ``self.df`` to ``self.location``, breaking the read→write lineage.

        Use this (NOT :meth:`write`) whenever ``self.df`` derives from reading
        ``self.location`` itself — e.g. you loaded the item's table, filtered/enriched
        it, and want to persist the result back to the SAME table. A bare
        :meth:`write` (a direct ``saveAsTable``) raises Spark's *"Cannot overwrite
        table that is also being read from"* in that case; ``write_safe`` round-trips
        through a uuid-named temp table so the target write reads the temp, not the
        target.

        This is the public, deliberate form of the lineage break the extract methods
        (``entityExtract``, ``write_index_table``, ``create_extract``, …) do internally
        via :meth:`_auto_write`. Unlike ``_auto_write`` it **raises** on failure, so a
        caller that persists on purpose knows if the write did not happen.

        Requires ``location`` and ``label``. Rebinds ``self.df`` to the persisted
        table and clears the dirty flag. The temp table is always dropped.

        Raises:
            ValueError: If ``self.df`` is None, or ``location``/``label`` is missing.
            Exception: Propagates any Spark write error (temp still cleaned up).
        """
        name = getattr(self, 'name', 'unknown')
        if self._df is None:
            raise ValueError(
                "write_safe on '{}': no DataFrame to write.".format(name))
        location = getattr(self, 'location', None)
        label = getattr(self, 'label', None)
        if not location or not label:
            raise ValueError(
                "write_safe on '{}': needs both location and label "
                "(location={!r}, label={!r}).".format(name, location, label))
        import uuid
        # Unique per-call temp-table name so concurrent callers / retries don't collide.
        temp_name = "{}_tmp_{}".format(location, uuid.uuid4().hex[:12])
        try:
            self._df.write.saveAsTable(temp_name, mode='overwrite')
            temp_df = spark.table(temp_name)
            writeTable(temp_df, location, description=label,
                       partitionBy=getattr(self, 'partitionBy', None))
            self._df = spark.table(location)
            self._dirty = False
            logger.info("write_safe wrote %s", location)
        finally:
            try:
                spark.sql("DROP TABLE IF EXISTS {}".format(temp_name))
            except Exception:
                pass

    def _auto_write(self):
        """Best-effort auto-persist after an extract method sets self.df.

        Called by extract methods (create_extract, entityExtract, write_index_table,
        dict2pyspark, extract_concept_flags/events) — NOT by the df setter, so manual
        df assignments don't auto-write. Delegates to :meth:`write_safe` (the temp-table
        lineage break) but WARNS instead of raising, so a failed auto-write never aborts
        a pipeline. For a deliberate lineage-safe persist that should surface errors, call
        ``write_safe`` directly.
        """
        if (self._df is not None
                and getattr(self, 'location', None)
                and getattr(self, 'label', None)):
            try:
                self.write_safe()
            except Exception as ex:
                logger.warning("Auto-write failed for %s: %s",
                               getattr(self, 'location', '?'), ex)
    
    def write_index_table(self, inTable, histStart=None, histEnd=None,
                          indexLabel=None, lastLabel=None,
                          filterSimple=True):
        """
        Create an index table identifying first/last records per entity.
        
        This method identifies the first (index) date for each entity
        (typically a person) and optionally filters by date range.
        Grain comes from YAML ``indexFields`` (e.g. person, or person-year
        via ``[personid, tenant, year]``). Calendar parts ``year`` / ``month``
        in ``indexFields`` are auto-derived from ``datefieldPrimary`` when
        missing on the input — no notebook ``withColumn`` needed.
        
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
        if indexLabel is None:
            indexLabel = getattr(self, 'indexLabel', 'index_')
        if lastLabel is None:
            lastLabel = getattr(self, 'lastLabel', 'last_')
        
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

        Join geometry (load-bearing for overlap / left-attach):
            Internally this is
            ``entitySource.join(elementList, on=keys, how=howjoin)``
            with optional broadcast of **elementList**. Therefore:

            * **entitySource is the preserved LEFT side** of ``howjoin``
              (for ``howjoin='left'``, keep the base table here — e.g. a
              person index — and put the feature table in ``elementList``).
            * **elementList is the broadcast side** when
              ``broadcast_flag=True`` (default). Pass ``broadcast_flag=False``
              when elementList is a large person or person-year table.

            Same-key **overlap** of two person-year tables is just this
            method with ``howjoin='inner'`` and keys including ``year``.
            **Left-attach** of a person feature table onto a person base is
            the same method with ``howjoin='left'`` and the base as
            ``entitySource``.

        Empty-join contract:
            If the join yields zero rows, the underlying
            ``identify_target_records`` currently returns ``None`` (with a
            warning). When that happens and ``set_self_df`` is True, this
            method **does not** rebind ``self.df`` — so a prior Hive product
            at ``self.location`` can be lazy-loaded on next access (stale on
            re-runs that legitimately become empty). Callers that need a
            guaranteed empty frame should check the return value or
            ``attrition`` after the call. Prefer fixing upstream keys when
            emptiness is unexpected.

        Parameters:
            elementList: Element/index table (ExtractItem or DataFrame) —
                join keys (and optional feature columns). Broadcast when
                ``broadcast_flag=True``.
            entitySource: Source DataFrame (or object with df attribute) —
                LEFT side of the join (rows preserved under left join).
            elementIndex (list): Columns for join (default: elementList.indexFields)
            histStart: Start date/offset
            histStop: Stop date/offset
            datefieldElement (str): Date column in self.df for offsets
            masterList (list): Columns to include in output
            howjoin (str): Join type (default ``inner``). Use ``left`` for
                attach-base-to-feature with base as ``entitySource``.
            cacheResult (bool): Cache result DataFrame
            broadcast_flag (bool): Broadcast **elementList** for the join
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

        # Config-driven fallbacks: a call kwarg always wins, but when omitted these
        # come from the ExtractItem's 000-control.yaml block (self.<key>), mirroring
        # write_index_table's histStart/histEnd. Lets the notebook keep the call to just
        # elementList/entitySource/cohort and put the date window + column set in config.
        if datefieldSource is None:
            datefieldSource = getattr(self, 'datefieldSource', None)
        if histStart is None:
            histStart = getattr(self, 'histStart', None)
        if histStop is None:
            histStop = getattr(self, 'histStop', None)
        if datefieldElement is None:
            datefieldElement = getattr(self, 'datefieldElement', None)
        if masterList is None:
            masterList = getattr(self, 'masterList', None)

        # Get the actual DataFrames
        if hasattr(elementList, 'df'):
            element_df = elementList.df
        else:
            element_df = elementList

        if hasattr(entitySource, 'df'):
            entity_df = entitySource.df
        else:
            entity_df = entitySource

        # Fail fast on a missing elementList DataFrame. Before this check
        # existed, element_df=None silently flowed into identify_target_records,
        # which logged a vague warning and returned None. That None was then
        # assigned to self.df — producing downstream AttributeError three
        # frames removed from the real cause. An entityExtract against a
        # non-existent left side is nonsense; refuse to do it.
        if not isinstance(element_df, DataFrame):
            element_name = getattr(elementList, 'name', repr(elementList))
            element_status = getattr(elementList, 'status', 'unknown')
            target_name = getattr(self, 'name', 'unknown')
            raise ValueError(
                f"entityExtract on '{target_name}': elementList "
                f"'{element_name}' has no DataFrame "
                f"(elementList.df is {type(element_df).__name__}, "
                f"status={element_status!r}). Likely causes:\n"
                f"  * create_extract() was never called on "
                f"'{element_name}' in this session\n"
                f"  * create_extract() matched zero rows — check the "
                f"regex / merge keys\n"
                f"  * Item.status == 'ITEM_FAILED' — run "
                f"r.report_str() to inspect\n"
                f"  * No YAML projectTables entry for "
                f"'{element_name}' — '.location' is unset and "
                f"lazy-load from Hive is a no-op\n"
                f"Fix the upstream step, then re-run this cell."
            )

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
            # Respect `fieldList` on the receiver: project the joined
            # result to the YAML-declared output schema before persisting.
            # `fieldList` IS the column contract for downstream consumers
            # (R _targets.R pipelines, CSV exports). Without this projection
            # the notebook has to do a manual .select(...) after every
            # entityExtract, which violates the "thin notebook" principle
            # and drifts from YAML.
            fieldList = getattr(self, 'fieldList', None)
            if fieldList:
                available = [c for c in fieldList if c in result.columns]
                missing = [c for c in fieldList if c not in result.columns]
                if missing:
                    logger.warning(
                        f"entityExtract on '{getattr(self, 'name', 'unknown')}': "
                        f"fieldList columns not found in join result and "
                        f"will be dropped: {missing}"
                    )
                if available:
                    result = result.select(*available)
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
            # Direct _df assignment (not self.df = ...) so the setter
            # doesn't re-mark the item dirty after we just persisted it.
            self._df = spark.table(self.location)
            self._dirty = False

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
            # Always keep join keys and the group/label column alongside
            # retained fields. The label column is whatever `groupName`
            # resolved to (or literal 'group' when unset).
            index_fields = getattr(self, 'indexFields', [])
            keep = list(set(retained + index_fields))
            label_col = getattr(self, 'groupName',
                                getattr(elementList, 'groupName', None)) or 'group'
            if label_col in self.df.columns and label_col not in keep:
                keep.append(label_col)
            # Belt-and-suspenders: also preserve literal 'group' for
            # backward compatibility with any caller that still emits it.
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
        # Resolve groupName once — receiver first, then elementList.
        # `groupName` is the semantic column name for the per-pattern label
        # on BOTH sides of the asymmetry: the input column (in patterns.df)
        # to read label values from, AND the output column written to the
        # result. When groupName is not set we fall back to the historical
        # default of 'group' for the output column (backward compat).
        resolved_group_name = getattr(self, 'groupName',
                                      getattr(patterns, 'groupName', None))
        output_group_col = resolved_group_name or 'group'

        if hasattr(patterns, 'df'):
            # ExtractItem with DataFrame — read pattern + group columns
            list_index = getattr(patterns, 'listIndex', 'codes')
            # Input column in patterns.df holding the label values
            input_group_col = resolved_group_name
            if input_group_col is None:
                input_group_col = 'group' if 'group' in patterns.df.columns else list_index
            cols_to_select = (
                [input_group_col, list_index] if input_group_col != list_index
                else [list_index]
            )
            rows = patterns.df.select(*cols_to_select).distinct().collect()
            group_patterns = [(row[input_group_col], row[list_index]) for row in rows]
        elif isinstance(patterns, dict):
            # dict: group_name -> regex_pattern
            group_patterns = list(patterns.items())
        elif isinstance(patterns, list):
            # plain list — no group info, assign sequential group names
            group_patterns = [(f'group_{i}', p) for i, p in enumerate(patterns)]
        else:
            group_patterns = [('default', str(patterns))]

        # Iterate per group, filter, add label in the semantic column, union.
        # Output column name follows YAML `groupName` (fallback 'group') —
        # this removes the old asymmetry where `groupName='Test'` would
        # still produce a column literally named 'group', forcing notebooks
        # to do a post-hoc withColumnRenamed.
        results = []
        for group_name, pattern in group_patterns:
            matched = source.filter(F.col(field).rlike(pattern))
            matched = matched.withColumn(output_group_col, F.lit(group_name))
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
    
    def dict2pyspark(self, columnname='codes'):
        """
        Convert a dictionary attribute to DataFrame.

        Parameters:
            columnname (str): Name for the values column (default: 'codes').
                Historically the default was a list ['codes'], which
                propagated a list-of-a-list to pandas from_dict and
                produced MultiIndex columns with tuple names -- the
                resulting Spark DataFrame then had column names like
                ('codes',) which broke all downstream .select() calls.
                Passing a plain string avoids the MultiIndex promotion.
        """
        if hasattr(self, 'dictionary'):
            from spark_config_mapper.utils.pandas import dict2Pandas
            self.pd = dict2Pandas(self.dictionary, columnname=columnname)
            # Explicitly set the index name before reset_index so the reset
            # produces a column named 'index'. Without this, pandas sometimes
            # uses None or another name (e.g. after certain Pandas versions
            # with from_dict), and the subsequent rename silently no-ops,
            # leaving the group label column unnamed and breaking
            # _extract_by_regex's group-column detection.
            self.pd.index.name = 'index'
            self.pd = self.pd.reset_index().rename(columns={'index': 'group'})
            if 'group' not in self.pd.columns:
                raise RuntimeError(
                    "dict2pyspark: post-rename columns missing 'group' "
                    "(got {}). Pandas index handling changed?".format(
                        list(self.pd.columns)))
            self.df = spark.createDataFrame(self.pd)
            self._auto_write()

    def push_discern(self, discern_context=None, discern_root=None,
                     version=None, concepts=None, concept_flags=None):
        """
        Broadcast one or more Cerner Discern ontology contexts to the Spark
        cluster and register the Discern SQL UDFs for use in extraction queries.

        After a successful push both UDF families are available: the
        active-context ``has_concept`` / ``has_any_concept`` and the
        context-qualified ``has_concept_in_context`` /
        ``has_any_concept_in_context``. (On the current HDL foresight build,
        ``push_discern`` also broadcasts the context, so the ``*_in_context``
        UDFs work without a separate ``broadcast_discern`` — proven by the hmi
        ``099`` smoke test and ``055`` extraction.)

        Thin config-driven wrapper over ``foresight.discern.push_discern`` (the
        Cerner HealtheIntent Discern SDK). ``foresight`` lives behind the
        ``com.cerner.foresight`` JVM JAR on HDL only, so it is imported lazily —
        ``lhn`` still imports fine in environments without it.

        Contexts to push are resolved in this order:

        1. ``discern_context`` arg — a GUID string or list of GUIDs. Each is
           loaded with ``concepts`` (arg) or ``self.concepts``.
        2. Else ``concept_flags`` (arg) or ``self.concept_flags`` (see
           :meth:`extract_concept_flags`) — push each *distinct* context named
           there. Each is loaded as a FULL context (``concepts=None``), matching
           the proven ``055`` path. The arg is preferred so a caller who passes
           ``concept_flags`` to ``extract_concept_flags`` pushes the RIGHT
           contexts even when nothing is in config.
        3. Else ``self.discern_context`` with ``self.concepts``.

        Parameters:
            discern_context (str | list[str] | None): Context GUID(s) to push.
                Overrides config-derived contexts when given. When a list is
                given with ``concepts``, the SAME subset applies to every
                context (call once per context for per-context subsets).
            discern_root (str | None): S3/HDFS root of the ontology export
                (e.g. ``s3://.../discernontology/v1/``). Falls back to
                ``self.discern_root``. If a push fails for a context, that
                context is not in this root — try the ``.../v2/`` root.
            version (str | None): Ontology version, passed through. Falls back
                to ``self.discern_version`` then ``self.version`` (the latter
                may carry an unrelated dataset version — prefer discern_version).
            concepts (list[str] | None): Concept-name subset to load for the
                ``discern_context`` path (memory reduction). Falls back to
                ``self.concepts``. Not applied to the ``concept_flags`` path.
            concept_flags (list[dict] | None): {flag, concept, context} rows;
                if given, push each distinct ``context`` as a full context.
                Takes precedence over ``self.concept_flags``.

        Raises:
            ImportError: If ``foresight`` is unavailable (i.e. not on HDL).
            ValueError: If no context can be resolved from args or config.
        """
        try:
            from foresight.discern import push_discern as _push_discern
        except ImportError as ex:
            raise ImportError(
                "foresight.discern is unavailable — push_discern only runs on "
                "HDL, where the com.cerner.foresight JAR is on the Spark "
                "classpath (import error: {}).".format(ex))

        root = coalesce(discern_root, getattr(self, 'discern_root', None))
        # Prefer discern_version over version to avoid clobbering by an
        # unrelated dataset-`version` config attribute on the item.
        ver = version
        if ver is None:
            ver = coalesce(getattr(self, 'discern_version', None),
                           getattr(self, 'version', None))

        flags_cfg = (concept_flags if concept_flags is not None
                     else getattr(self, 'concept_flags', None))

        # Resolve {context: concepts_subset_or_None} to push.
        to_push = {}
        if discern_context is not None:
            ctx_concepts = (concepts if concepts is not None
                            else getattr(self, 'concepts', None))
            ctxs = (discern_context if isinstance(discern_context, (list, tuple))
                    else [discern_context])
            for ctx in ctxs:
                to_push[ctx] = ctx_concepts
        elif flags_cfg:
            # Full context per distinct GUID (concepts=None) — the proven 055
            # path. Subsetting is possible via the explicit discern_context arg.
            # Validate 'context' here too: push_discern may be called directly
            # (not only via extract_concept_flags, which validates rows first),
            # so guard against a bare KeyError on a malformed row.
            for i, row in enumerate(flags_cfg):
                if 'context' not in row:
                    raise ValueError(
                        "push_discern on '{}': concept_flags[{}] missing "
                        "'context' (got {!r}).".format(
                            getattr(self, 'name', 'unknown'), i, row))
                to_push.setdefault(row['context'], None)
        elif getattr(self, 'discern_context', None):
            to_push[self.discern_context] = (
                concepts if concepts is not None
                else getattr(self, 'concepts', None))
        else:
            raise ValueError(
                "push_discern on '{}': no discern_context passed and none in "
                "config (need 'discern_context' or 'concept_flags').".format(
                    getattr(self, 'name', 'unknown')))

        for ctx, ctx_concepts in to_push.items():
            _push_discern(spark, discern_context=ctx, version=ver,
                          discern_root=root, concepts=ctx_concepts)
            logger.info("push_discern OK: context=%s concepts=%s root=%s",
                        ctx, ctx_concepts, root)

    def extract_concept_flags(self, source, cohort=None, index_fields=None,
                              code=None, concept_flags=None,
                              push=True, set_self_df=True):
        """
        Build a person-level "ever present" flag table from Cerner Discern
        ontology concepts — one 0/1 column per concept.

        Generalises the proven raw-foresight pattern (hmi ``055``): for each
        ``{flag, concept, context}`` mapping, emit
        ``MAX(IF(has_concept_in_context(<code_struct>, '<concept>',
        '<context>'), 1, 0)) AS <flag>`` aggregated over ``index_fields`` — 1
        iff ANY of the entity's records in ``source`` resolves to that concept
        in that context.

        Why ``has_concept_in_context`` (explicit GUID) and not
        ``has_any_concept`` (single active context): the flags can live in
        DIFFERENT contexts (e.g. hf/afib/mi in one, ckd/pci in another), which a
        single active-context query cannot span. The explicit-GUID form also
        sidesteps the crash-on-unknown-concept gotcha — each concept is only
        ever tested in its own validated context.

        The mappings come from ``concept_flags`` (arg or ``self.concept_flags``),
        a list of dicts::

            concept_flags:
              - {flag: hf,  concept: HEART_FAILURE_CLIN,           context: 53EF30...}
              - {flag: ckd, concept: CHRONIC_KIDNEY_DISEASE_CLIN,  context: 81CD81...}

        Every concept/context pair MUST be validated against the ontology
        tabulation (``ontology_tabulation.csv`` / the ``tabulated_ontologies``
        tables) beforehand: a concept name absent from its loaded context raises
        a hard ``Py4JJavaError`` at action time, NOT a graceful ``false``.

        Parameters:
            source: Table to scan — an ExtractItem (``.df`` used) or a Spark
                DataFrame. Must carry ``index_fields`` and the ``code`` STRUCT
                column (e.g. ``conditioncode``, NOT ``conditioncode_standard_id``).
            cohort: Optional ExtractItem / DataFrame / full ``schema.table``
                string of ``index_fields`` to bound the scan to a cohort (inner
                join). Falls back to ``self.cohort``. None scans all of ``source``.
            index_fields (list[str] | None): Grouping keys. Falls back to
                ``self.indexFields`` then ``['personid', 'tenant']``.
            code (str | None): Name of the code STRUCT column. Falls back to
                ``self.conditionCodefield`` then ``'conditioncode'``.
            concept_flags (list[dict] | None): The flag mappings. Falls back to
                ``self.concept_flags``.
            push (bool): If True (default), call :meth:`push_discern` first to
                broadcast every context named in ``concept_flags``.
            set_self_df (bool): If True (default), assign the result to
                ``self.df`` and auto-write to ``self.location``.

        Returns:
            pyspark.sql.DataFrame: One row per ``index_fields`` with one 0/1
            column per flag.

        Raises:
            ValueError: If no ``concept_flags`` can be resolved; if any row is
                missing a required key; if flag names are duplicated or collide
                with ``index_fields``; or if ``source`` has no DataFrame.
        """
        name = getattr(self, 'name', 'unknown')
        index_fields = (index_fields if index_fields is not None
                        else getattr(self, 'indexFields', ['personid', 'tenant']))
        code = (code if code is not None
                else getattr(self, 'conditionCodefield', 'conditioncode'))
        flags = (concept_flags if concept_flags is not None
                 else getattr(self, 'concept_flags', None))
        if not flags:
            raise ValueError(
                "extract_concept_flags on '{}': no concept_flags (need a list "
                "of {{flag, concept, context}} dicts, in config or as an "
                "arg).".format(name))

        # Validate each row up front so a malformed config fails with a clear
        # message (which row, which key) instead of a bare KeyError deep in the
        # aggregate, and so duplicate/reserved flag names fail here rather than
        # as an opaque "duplicate column" AnalysisException at write time.
        flag_names = []
        for i, row in enumerate(flags):
            missing = [k for k in ('flag', 'concept', 'context') if k not in row]
            if missing:
                raise ValueError(
                    "extract_concept_flags on '{}': concept_flags[{}] missing "
                    "keys {} (got {!r}).".format(name, i, missing, row))
            flag_names.append(row['flag'])
        dupes = sorted({f for f in flag_names if flag_names.count(f) > 1})
        clash = sorted(set(flag_names) & set(index_fields))
        if dupes or clash:
            raise ValueError(
                "extract_concept_flags on '{}': flag names must be unique and "
                "must not collide with index_fields {} — duplicates={}, "
                "clashes={}.".format(name, index_fields, dupes, clash))

        if push:
            # Forward the RESOLVED flags so push broadcasts exactly the contexts
            # this query will reference — correct even when flags came from the
            # arg (not config). Full context per GUID (proven 055 path).
            self.push_discern(concept_flags=flags)

        source_df = source.df if hasattr(source, 'df') else source
        # Fail fast on a missing source DataFrame (mirrors entityExtract): a
        # None here otherwise surfaces as an opaque error deep in the join/agg.
        if not isinstance(source_df, DataFrame):
            raise ValueError(
                "extract_concept_flags on '{}': source has no DataFrame "
                "(got {}). Ensure the source table exists / was built.".format(
                    name, type(source_df).__name__))
        # The code column is interpolated raw into F.expr (it's an identifier, not
        # a quotable literal), so validate it exists — a typo becomes a clear
        # message here instead of an opaque Spark parse error in the aggregate.
        if code not in source_df.columns:
            raise ValueError(
                "extract_concept_flags on '{}': code column '{}' not found in "
                "source (columns: {}).".format(name, code, source_df.columns))

        # Cohort-bound the scan (inner join on index_fields), if configured.
        # Matches raw 055: cohort members with zero source rows are absent (not
        # 0) — _targets.R left-joins the full cohort and fills 0 downstream.
        cohort = cohort if cohort is not None else getattr(self, 'cohort', None)
        if cohort is not None:
            cohort_df = cohort.df if hasattr(cohort, 'df') else cohort
            if isinstance(cohort_df, str):
                cohort_df = spark.table(cohort_df)
            cohort_keys = cohort_df.select(*index_fields).distinct()
            source_df = source_df.join(cohort_keys, on=index_fields, how='inner')

        # One MAX(IF(has_concept_in_context(...))) aggregate per flag. The UDF is
        # SQL-registered by push_discern, so F.expr (a string) is the only way to
        # call it on Spark 2.4.4 (no F.call_udf until 3.5). Escape single quotes
        # in the literals so a concept name with an apostrophe can't break or
        # inject the expression.
        def _lit(value):
            return str(value).replace("'", "''")

        agg_exprs = [
            F.max(F.expr(
                "IF(has_concept_in_context({code}, '{concept}', '{context}'), "
                "1, 0)".format(code=code, concept=_lit(row['concept']),
                               context=_lit(row['context']))
            )).alias(row['flag'])
            for row in flags
        ]
        result = source_df.groupBy(*index_fields).agg(*agg_exprs)

        if set_self_df:
            self.df = result
            self._auto_write()
        return result

    def extract_concept_events(self, source, cohort=None, index_fields=None,
                               code=None, concept_flags=None, datefield=None,
                               retained_fields=None, histStart=None, histStop=None,
                               push=True, set_self_df=True):
        """
        Record-level (dated) counterpart of :meth:`extract_concept_flags`.

        Keeps the SOURCE rows whose ``code`` struct resolves to a configured
        ``{flag, concept, context}`` mapping (via ``has_concept_in_context``),
        tagged with a ``flag`` column naming which concept matched. Unlike
        ``extract_concept_flags`` it does NOT aggregate — it emits one output row
        per matching source row — so a downstream step can compare each event's
        ``datefield`` to an index date (e.g. build a ``prior_*`` "event before
        the index" flag in the R analytic layer, as ``pciEvents`` feeds
        ``prior_pci``).

        Output columns: ``index_fields`` + ``datefield`` + ``flag`` (+ any
        ``retained_fields`` present in the source).

        Parameters mirror :meth:`extract_concept_flags`, plus:
            datefield (str | None): date column to retain from ``source`` (e.g.
                ``effectivedate``). Falls back to ``self.datefieldPrimary``.
            retained_fields (list[str] | None): extra source columns to keep.
                Falls back to ``self.retained_fields`` (default: none).
            histStart / histStop (date-like | None): inclusive study-window bounds
                on ``datefield``, applied to ``source`` BEFORE the concept UDF (so
                only in-window rows are scanned — a large speedup on all-history
                sources). Fall back to ``self.histStart`` / ``self.histStop``.
                Mirrors entityExtract's windowing.

        Same crash-on-unknown-concept caveat as ``extract_concept_flags``: every
        concept/context pair MUST be validated against the tabulation. The
        cohort join uses only the person-level ``index_fields`` present in the
        cohort table (so an ``encounterid`` in ``index_fields`` doesn't break it).
        """
        name = getattr(self, 'name', 'unknown')
        index_fields = (index_fields if index_fields is not None
                        else getattr(self, 'indexFields', ['personid', 'tenant']))
        code = (code if code is not None
                else getattr(self, 'conditionCodefield', 'conditioncode'))
        datefield = (datefield if datefield is not None
                     else getattr(self, 'datefieldPrimary', None))
        retained_fields = (retained_fields if retained_fields is not None
                           else getattr(self, 'retained_fields', []) or [])
        flags = (concept_flags if concept_flags is not None
                 else getattr(self, 'concept_flags', None))
        if not flags:
            raise ValueError(
                "extract_concept_events on '{}': no concept_flags (need a list "
                "of {{flag, concept, context}} dicts, in config or as an "
                "arg).".format(name))
        for i, row in enumerate(flags):
            missing = [k for k in ('flag', 'concept', 'context') if k not in row]
            if missing:
                raise ValueError(
                    "extract_concept_events on '{}': concept_flags[{}] missing "
                    "keys {} (got {!r}).".format(name, i, missing, row))

        if push:
            self.push_discern(concept_flags=flags)

        source_df = source.df if hasattr(source, 'df') else source
        if not isinstance(source_df, DataFrame):
            raise ValueError(
                "extract_concept_events on '{}': source has no DataFrame "
                "(got {}).".format(name, type(source_df).__name__))
        if code not in source_df.columns:
            raise ValueError(
                "extract_concept_events on '{}': code column '{}' not found in "
                "source (columns: {}).".format(name, code, source_df.columns))
        # Fail loud on a missing datefield rather than silently dropping it (the whole
        # point of this method is the date, e.g. for a downstream prior_* comparison).
        if datefield is not None and datefield not in source_df.columns:
            raise ValueError(
                "extract_concept_events on '{}': datefield '{}' not found in "
                "source (columns: {}).".format(name, datefield, source_df.columns))

        # Cohort-bound with only the person-level keys the cohort actually has
        # (index_fields may include encounterid, which persontenant lacks).
        cohort = cohort if cohort is not None else getattr(self, 'cohort', None)
        if cohort is not None:
            cohort_df = cohort.df if hasattr(cohort, 'df') else cohort
            if isinstance(cohort_df, str):
                cohort_df = spark.table(cohort_df)
            join_keys = [k for k in index_fields if k in cohort_df.columns]
            cohort_keys = cohort_df.select(*join_keys).distinct()
            source_df = source_df.join(cohort_keys, on=join_keys, how='inner')

        # Study-window filter on the datefield (mirrors entityExtract's datefieldSource +
        # histStart/histStop). Resolve from args then config; skip silently if unset. Applied
        # BEFORE the has_concept_in_context UDF so the UDF scans only in-window rows — a large
        # speedup on all-history sources (e.g. the raw lab table). The original
        # extract_concept_events had this window; it was dropped in a refactor — restored here.
        histStart = histStart if histStart is not None else getattr(self, 'histStart', None)
        histStop = histStop if histStop is not None else getattr(self, 'histStop', None)
        if histStart is not None or histStop is not None:
            if datefield is None:
                raise ValueError(
                    "extract_concept_events on '{}': histStart/histStop set but no datefield "
                    "to window on (set datefieldPrimary in config).".format(name))
            if histStart is not None:
                source_df = source_df.filter(F.col(datefield) >= histStart)
            if histStop is not None:
                source_df = source_df.filter(F.col(datefield) <= histStop)

        # SQL-registered UDF -> F.expr string (Spark 2.4.4, no F.call_udf). Escape
        # single quotes in the literals (see extract_concept_flags).
        def _lit(value):
            return str(value).replace("'", "''")

        keep = list(index_fields)
        if datefield and datefield not in keep:
            keep.append(datefield)
        for rf in retained_fields:
            if rf not in keep:
                keep.append(rf)

        def _keep_expr(field, available):
            """Resolve a keep/retained field to a select expression.

            A dotted path ('typedvalue.numericValue.value') is pulled out of the
            struct and aliased to the flattened name ('typedvalue_numericValue_value'),
            matching flattenTable's convention so the output column is the same one
            r.<source>.df would expose. This lets a caller keep a nested VALUE at
            extract time (avoiding a second full-table join to attach it). Returns
            None if the field (or its struct root) isn't present.
            """
            if field in available:
                return F.col(field)
            if '.' in field and field.split('.')[0] in available:
                return F.col(field).alias(field.replace('.', '_'))
            return None

        parts = []
        for row in flags:
            matched = source_df.filter(
                "has_concept_in_context({code}, '{concept}', '{context}')".format(
                    code=code, concept=_lit(row['concept']),
                    context=_lit(row['context']))
            )
            available = matched.columns
            exprs = [(c, _keep_expr(c, available)) for c in keep]
            dropped = [c for c, x in exprs if x is None]
            if dropped:
                # Previously these were dropped SILENTLY (a typo'd or nested retained
                # field just vanished). Warn instead.
                logger.warning(
                    "extract_concept_events on '%s': keep/retained fields not found "
                    "in source, dropped: %s", name, dropped)
            parts.append(matched.select(
                *[x for _, x in exprs if x is not None],
                F.lit(row['flag']).alias('flag')))
        # All parts share the same schema (same select), so a plain unionByName is
        # safe on Spark 2.4.4 (no allowMissingColumns needed).
        result = parts[0]
        for extra in parts[1:]:
            result = result.unionByName(extra)

        if set_self_df:
            self.df = result
            self._auto_write()
        return result

    def build_datadict(self, source, group_by=None, datefield=None,
                       index_fields=None, pattern_strings=None,
                       source_table=None, set_self_df=True):
        """Build a LONG-form data-dictionary table: distinct-level counts of each
        standard field in ``source``, grouped by ``group_by`` (e.g.
        ``['tenant', 'year']``).

        Generalizes the legacy ``metadata.py::createMeta`` — which was non-runnable
        (empty ``pattern_strings`` -> zero output; ``for ti in len(...)`` etc.) and
        had no year/tenant grouping — into an lhn ExtractItem method. It:

        * WIRES ``pattern_strings`` into root discovery (the fatal empty-list bug);
        * derives EVERY counting key from ``group_by`` (no hand-listed keys, so no
          silent cross-tenant fan-out);
        * expands multiple arrays one-at-a-time (``createMeta`` relied on the
          unpackaged ``expand_arrays_in_df``; ``flattenTable`` can't do multi-array);
        * emits ONE queryable long table keyed by ``(source_table, root_field,
          *group_by, standard_id/codingSystemId/primaryDisplay, count)`` instead of
          hundreds of per-(table, field) tables.

        Args:
            source: source table (DataFrame or object with ``.df``).
            group_by (list[str] | None): grouping dimensions; falls back to
                ``self.groupBy`` then ``['tenant', 'year']``. ``year``/``month`` in
                it are derived from ``datefield`` when absent.
            datefield (str | list[str] | None): date column(s) for year/month
                (coalesce precedence if a list); falls back to
                ``self.datefieldPrimary``.
            index_fields (list[str] | None): retained for distinct counting; falls
                back to ``self.indexFields`` then ``['personid', 'tenant']``.
            pattern_strings (list[str] | None): standard-field suffixes for root
                discovery; falls back to ``self.pattern_strings`` then the standard
                set. MUST be non-empty (empty is the legacy zero-output bug).
            source_table (str | None): value written into the ``source_table``
                column; falls back to ``self.sourceTable`` then the item name.
            set_self_df (bool): set ``self.df`` and auto-write (default True).

        Returns:
            pyspark.sql.DataFrame: long-form level counts by ``group_by``.
        """
        from spark_config_mapper import (get_standard_id_elements, explode_single_array,
                                          flat_schema, flattenTable)
        from pyspark.sql.types import ArrayType, StructType
        name = getattr(self, 'name', 'unknown')
        group_by = group_by or getattr(self, 'groupBy', None) or ['tenant', 'year']
        pattern_strings = (pattern_strings or getattr(self, 'pattern_strings', None)
                           or ['standard.id', 'standard.codingSystemId',
                               'standard.primaryDisplay', 'value', 'brandType'])
        if not pattern_strings:
            raise ValueError(
                "build_datadict on '{}': pattern_strings is empty — root discovery "
                "matches nothing and writes zero rows (the legacy createMeta "
                "bug).".format(name))
        index_fields = (index_fields or getattr(self, 'indexFields', None)
                        or ['personid', 'tenant'])
        datefield = datefield if datefield is not None else getattr(
            self, 'datefieldPrimary', None)
        source_table = (source_table or getattr(self, 'sourceTable', None) or name)

        df = source.df if hasattr(source, 'df') else source
        if not isinstance(df, DataFrame):
            raise ValueError(
                "build_datadict on '{}': source has no DataFrame (got {}).".format(
                    name, type(df).__name__))
        df = _derive_date_parts(df, datefield, group_by)
        person = index_fields[0] if index_fields else 'personid'
        # Fail loudly (not silently-across-tenants) if a requested key is missing.
        need = group_by + [person]
        missing = [c for c in need if c not in df.columns]
        if missing:
            raise ValueError(
                "build_datadict on '{}': required columns {} not in source "
                "(columns: {}).".format(name, missing, df.columns))

        # Discover root standard fields from the DOT-path schema. get_standard_id_elements
        # matches dotted paths ('conditioncode.standard.id'), so it MUST see the nested
        # names — NOT the underscore-flattened columns (feeding it flattened names was the
        # bug that reproduced createMeta's zero-output).
        roots_dot = get_standard_id_elements(flat_schema(df), pattern_strings)
        if not roots_dot:
            logger.warning("build_datadict on '%s': no standard root fields matched "
                           "%s in %s.", name, pattern_strings, source_table)

        # Flatten to underscore columns to select from: explode top-level arrays one at a
        # time (each flattens), then flatten any remaining structs UNCONDITIONALLY — a
        # struct-only source (e.g. `condition`) has no array to trigger the explode loop.
        guard = 0
        while any(isinstance(f.dataType, ArrayType) for f in df.schema.fields) and guard < 25:
            arr = next(f.name for f in df.schema.fields
                       if isinstance(f.dataType, ArrayType))
            df = explode_single_array(df, arr, flatten=True)
            guard += 1
        if guard >= 25:
            logger.warning("build_datadict on '%s': array-explode guard hit (25) — "
                           "arrays may remain unflattened.", name)
        if any(isinstance(f.dataType, StructType) for f in df.schema.fields):
            df = flattenTable(df, error_on_multiple_arrays=False)
        flat_cols = set(df.columns)

        triple = [('standard_id', 'standard_id'),
                  ('standard_codingSystemId', 'standard_codingSystemId'),
                  ('standard_primaryDisplay', 'standard_primaryDisplay')]
        frames = []
        for root_dot in roots_dot:
            root_u = root_dot.replace('.', '_')
            renames, level_cols = [], []
            for suffix, canon in triple:
                col = root_u + '_' + suffix        # EXACT name (no endswith mis-bind)
                if col in flat_cols:
                    renames.append((col, canon)); level_cols.append(canon)
            if not level_cols:
                # value/brandType-style root — use its leaf column as a generic value.
                for cand in (root_u + '_value', root_u + '_brandType', root_u):
                    if cand in flat_cols:
                        renames.append((cand, 'value')); level_cols.append('value'); break
            if not level_cols:
                continue
            sub = df.select(*(group_by + [person] + [r[0] for r in renames]))
            for src_c, canon in renames:
                sub = sub.withColumnRenamed(src_c, canon)
            # distinct PERSONS per (group_by, level) — the datadict "Subjects" count
            # (distinct on the person key also makes sibling-array explosion harmless).
            counted = (sub.groupBy(*(group_by + level_cols))
                       .agg(F.countDistinct(person).alias('count'))
                       .withColumn('source_table', F.lit(source_table))
                       .withColumn('root_field', F.lit(root_dot)))
            frames.append(counted)

        result = _union_aligned(frames)
        if result is None:
            logger.warning("build_datadict on '%s': no roots resolved to level columns "
                           "— empty result (check pattern_strings vs the source schema).", name)
            result = df.select(*group_by).limit(0)
        elif set_self_df:
            self.df = result
            self._auto_write()
        return result

    def build_ontology_counts(self, source, concept_flags=None, codefield=None,
                              group_by=None, sample_n=None, datefield=None,
                              set_self_df=True):
        """Tabulate Discern concept / coding-system / code counts + percents per
        ``group_by`` (e.g. ``['tenant', 'year']``) x concept — Spark-native, no pandas.

        Generalizes ``add_ontology_count_new`` (single-context, pandas, year/month
        only, hand-listed merge keys). It:

        * pushes EVERY distinct context via :meth:`push_discern` (so concepts under a
          2nd context aren't silently false);
        * derives EVERY grouping and join key from ``group_by`` (the script's
          hardcoded ``['year','month']`` merges next to a ``dateGroupingFields`` param
          are the drift this prevents — a missed key fans the inner join out ACROSS
          tenants, silently doubling counts);
        * uses a per-``group_by`` denominator DataFrame (not a scalar ``sample_n``),
          so percents are interpretable per tenant-year;
        * counts via the proven ``has_concept_in_context`` SQL path (same as
          :meth:`extract_concept_flags`), staying in Spark end to end.

        Args:
            source: source table (DataFrame or object with ``.df``).
            concept_flags (list[dict] | None): ``{concept, context}`` rows (``flag``
                optional/ignored here); falls back to ``self.concept_flags``.
            codefield (str | None): the code STRUCT column; falls back to
                ``self.codefield`` then ``self.conditionCodefield``.
            group_by (list[str] | None): grouping dims; falls back to ``self.groupBy``
                then ``['tenant', 'year']``.
            sample_n: per-group denominator — a DataFrame keyed by ``group_by`` with
                an ``n`` column (or object with ``.df``). A scalar is accepted but
                discouraged. If None (default), computed internally as distinct
                persons per ``group_by`` from the source.
            datefield (str | list[str] | None): date column(s) to derive year/month.
            set_self_df (bool): set ``self.df`` and auto-write (default True).

        Returns:
            pyspark.sql.DataFrame: one row per (``group_by``, ``conceptName``, coding
            system, code) with countBy/percentBy Concept, System, Code.
        """
        name = getattr(self, 'name', 'unknown')
        group_by = group_by or getattr(self, 'groupBy', None) or ['tenant', 'year']
        flags = (concept_flags if concept_flags is not None
                 else getattr(self, 'concept_flags', None))
        if not flags:
            raise ValueError(
                "build_ontology_counts on '{}': no concept_flags (list of "
                "{{concept, context}} dicts).".format(name))
        for i, row in enumerate(flags):
            miss = [k for k in ('concept', 'context') if k not in row]
            if miss:
                raise ValueError(
                    "build_ontology_counts on '{}': concept_flags[{}] missing {} "
                    "({!r}).".format(name, i, miss, row))
        codefield = (codefield or getattr(self, 'codefield', None)
                     or getattr(self, 'conditionCodefield', None))
        if not codefield:
            raise ValueError(
                "build_ontology_counts on '{}': no codefield.".format(name))
        datefield = datefield if datefield is not None else getattr(
            self, 'datefieldPrimary', None)
        person = getattr(self, 'personIndex', None) or 'personid'
        if isinstance(person, (list, tuple)):
            person = person[0]

        self.push_discern(concept_flags=flags)          # push every distinct context

        df = source.df if hasattr(source, 'df') else source
        if not isinstance(df, DataFrame):
            raise ValueError(
                "build_ontology_counts on '{}': source has no DataFrame "
                "(got {}).".format(name, type(df).__name__))
        if codefield not in df.columns:
            raise ValueError(
                "build_ontology_counts on '{}': codefield '{}' not in source "
                "(columns: {}).".format(name, codefield, df.columns))
        df = _derive_date_parts(df, datefield, group_by)
        # Sentinel unknown dates to -1 so the INNER joins + the denominator join below
        # don't silently drop unknown-date rows (Spark NULL != NULL). The datadict path
        # (single groupBy, no join) does NOT need this.
        df = _sentinel_unknown_dates(df, group_by)

        def _lit(v):
            # Spark 2.4 SQL uses BACKSLASH escaping in string literals; '' doubling is
            # parsed as adjacent-literal concatenation and silently drops the quote.
            return str(v).replace("\\", "\\\\").replace("'", "\\'")

        # Project the code dims from the STRUCT — the source is the RAW table (the UDF
        # needs the struct), so the underscore columns don't exist yet. flat_schema gives
        # the dotted paths; project the present ones as flat columns. Raise if none
        # resolve rather than silently collapse the system/code breakdown.
        from spark_config_mapper import flat_schema
        dot_paths = set(flat_schema(df))
        code_dims = []
        for _sub, _suffix in (('standard.id', '_standard_id'),
                              ('standard.codingSystemId', '_standard_codingSystemId'),
                              ('standard.primaryDisplay', '_standard_primaryDisplay')):
            if (codefield + '.' + _sub) in dot_paths:
                _canon = codefield + _suffix
                df = df.withColumn(_canon, F.col(codefield + '.' + _sub))
                code_dims.append(_canon)
        if not code_dims:
            raise ValueError(
                "build_ontology_*: no code dims resolved from struct '{}' (expected "
                "'{}.standard.id' etc. in the source).".format(codefield, codefield))
        system = codefield + '_standard_codingSystemId'
        missing = [g for g in group_by if g not in df.columns]
        if missing:
            raise ValueError(
                "build_ontology_*: group_by keys {} not in source.".format(missing))
        if person not in df.columns:
            raise ValueError(
                "build_ontology_*: person key '{}' not in source.".format(person))
        gb = group_by
        keep = [person] + gb + code_dims

        # Melt: one filter+tag per concept, unioned (Spark 2.4.4 has no stack/melt).
        parts = []
        for row in flags:
            expr = "has_concept_in_context({code}, '{c}', '{ctx}')".format(
                code=codefield, c=_lit(row['concept']), ctx=_lit(row['context']))
            parts.append(df.filter(F.expr(expr)).select(*keep)
                         .withColumn('conceptName', F.lit(row['concept'])))
        long = _union_aligned(parts)
        if long is None:
            raise ValueError(
                "build_ontology_counts on '{}': no concepts produced rows.".format(name))

        # Three distinct-person counts; EVERY key derives from group_by.
        by_concept = long.groupBy(*(gb + ['conceptName'])).agg(
            F.countDistinct(person).alias('countByConcept'))
        sys_keys = gb + ['conceptName'] + ([system] if system in long.columns else [])
        by_system = long.groupBy(*sys_keys).agg(
            F.countDistinct(person).alias('countBySystem'))
        code_keys = gb + ['conceptName'] + code_dims
        by_code = long.groupBy(*code_keys).agg(
            F.countDistinct(person).alias('countByCode'))

        counts = (by_code
                  .join(by_system, on=sys_keys, how='inner')
                  .join(by_concept, on=(gb + ['conceptName']), how='inner'))

        denom = sample_n.df if hasattr(sample_n, 'df') else sample_n
        if denom is None:
            # Default per-group denominator: distinct persons with ANY record in each
            # group (percent ~= prevalence among observably-active patients). Computed
            # here so callers don't hand-roll it — keeps the group_by contract in one place.
            denom = df.groupBy(*gb).agg(F.countDistinct(person).alias('n'))
        if isinstance(denom, DataFrame):
            # match the sentinel so a custom sample_n's unknown-date rows join (not NULL),
            # and keep only the keys + n so its extra columns don't leak in.
            denom = _sentinel_unknown_dates(denom, gb)
            denom = denom.select(*[c for c in (gb + ['n']) if c in denom.columns])
            counts = counts.join(denom, on=gb, how='left')
            for c in ('Concept', 'System', 'Code'):
                counts = counts.withColumn(
                    'percentBy' + c, F.col('countBy' + c) / F.col('n') * 100)
        else:
            for c in ('Concept', 'System', 'Code'):
                counts = counts.withColumn(
                    'percentBy' + c, F.col('countBy' + c) / F.lit(float(denom)) * 100)

        if set_self_df:
            self.df = counts
            self._auto_write()
        return counts

    def build_ontology_coverage(self, source, concept_flags=None, codefield=None,
                                group_by=None, datefield=None, set_self_df=True):
        """Per (``group_by``, code): distinct-person count + whether the code maps to
        ANY concept in the given Discern contexts — so the **UNMAPPED** codes
        (``mapped = 0``) can be reviewed as a coverage gap.

        The third leg alongside :meth:`build_datadict` (the full code inventory) and
        :meth:`build_ontology_counts` (the concept view): this is the code inventory
        **annotated with concept coverage**. When a cohort is designed from an ontology,
        ``filter(mapped = 0)`` surfaces the codes present in the data that no concept
        captures — the "did we miss anything?" review — and, over ``['tenant', 'year']``,
        shows whether that gap shifts by site or drifts over time.

        Args mirror :meth:`build_ontology_counts` (``concept_flags`` = ``{concept,
        context}`` rows; ``codefield`` = the code STRUCT; ``group_by`` falls back to
        ``self.groupBy`` then ``['tenant', 'year']``; ``datefield`` derives year/month).

        Returns:
            pyspark.sql.DataFrame: one row per (``group_by``, code fields) with ``count``
            (distinct persons) and ``mapped`` (1 if the code maps to any concept, else 0).
        """
        name = getattr(self, 'name', 'unknown')
        group_by = group_by or getattr(self, 'groupBy', None) or ['tenant', 'year']
        flags = (concept_flags if concept_flags is not None
                 else getattr(self, 'concept_flags', None))
        if not flags:
            raise ValueError(
                "build_ontology_coverage on '{}': no concept_flags.".format(name))
        for i, row in enumerate(flags):
            miss = [k for k in ('concept', 'context') if k not in row]
            if miss:
                raise ValueError(
                    "build_ontology_coverage on '{}': concept_flags[{}] missing {} "
                    "({!r}).".format(name, i, miss, row))
        codefield = (codefield or getattr(self, 'codefield', None)
                     or getattr(self, 'conditionCodefield', None))
        if not codefield:
            raise ValueError(
                "build_ontology_coverage on '{}': no codefield.".format(name))
        datefield = datefield if datefield is not None else getattr(
            self, 'datefieldPrimary', None)
        person = getattr(self, 'personIndex', None) or 'personid'
        if isinstance(person, (list, tuple)):
            person = person[0]

        self.push_discern(concept_flags=flags)

        df = source.df if hasattr(source, 'df') else source
        if not isinstance(df, DataFrame):
            raise ValueError(
                "build_ontology_coverage on '{}': source has no DataFrame "
                "(got {}).".format(name, type(df).__name__))
        if codefield not in df.columns:
            raise ValueError(
                "build_ontology_coverage on '{}': codefield '{}' not in source "
                "(columns: {}).".format(name, codefield, df.columns))
        df = _derive_date_parts(df, datefield, group_by)
        # Sentinel unknown dates to -1 so the INNER joins + the denominator join below
        # don't silently drop unknown-date rows (Spark NULL != NULL). The datadict path
        # (single groupBy, no join) does NOT need this.
        df = _sentinel_unknown_dates(df, group_by)

        def _lit(v):
            # Spark 2.4 SQL uses BACKSLASH escaping in string literals; '' doubling is
            # parsed as adjacent-literal concatenation and silently drops the quote.
            return str(v).replace("\\", "\\\\").replace("'", "\\'")

        # mapped = 1 if the code maps to ANY concept in ANY context, else 0.
        ors = ["IF(has_concept_in_context({code}, '{c}', '{ctx}'), 1, 0)".format(
                   code=codefield, c=_lit(row['concept']), ctx=_lit(row['context']))
               for row in flags]
        mapped_expr = ors[0] if len(ors) == 1 else "GREATEST({})".format(", ".join(ors))
        df = df.withColumn('mapped', F.expr(mapped_expr))

        # Project the code dims from the STRUCT — the source is the RAW table (the UDF
        # needs the struct), so the underscore columns don't exist yet. flat_schema gives
        # the dotted paths; project the present ones as flat columns. Raise if none
        # resolve rather than silently collapse the system/code breakdown.
        from spark_config_mapper import flat_schema
        dot_paths = set(flat_schema(df))
        code_dims = []
        for _sub, _suffix in (('standard.id', '_standard_id'),
                              ('standard.codingSystemId', '_standard_codingSystemId'),
                              ('standard.primaryDisplay', '_standard_primaryDisplay')):
            if (codefield + '.' + _sub) in dot_paths:
                _canon = codefield + _suffix
                df = df.withColumn(_canon, F.col(codefield + '.' + _sub))
                code_dims.append(_canon)
        if not code_dims:
            raise ValueError(
                "build_ontology_*: no code dims resolved from struct '{}' (expected "
                "'{}.standard.id' etc. in the source).".format(codefield, codefield))
        system = codefield + '_standard_codingSystemId'
        missing = [g for g in group_by if g not in df.columns]
        if missing:
            raise ValueError(
                "build_ontology_*: group_by keys {} not in source.".format(missing))
        if person not in df.columns:
            raise ValueError(
                "build_ontology_*: person key '{}' not in source.".format(person))
        gb = group_by
        keys = gb + code_dims
        result = df.groupBy(*keys).agg(
            F.max('mapped').alias('mapped'),
            F.countDistinct(person).alias('count'))

        if set_self_df:
            self.df = result
            self._auto_write()
        return result


def _derive_date_parts(df, datefield, group_by):
    """Add ``year``/``month`` columns derived from ``datefield`` when ``group_by``
    asks for them and they are absent, plus a ``<part>_is_unknown`` boolean flag.

    ``datefield`` may be one column or a list (coalesce precedence). An unknown
    (unparseable/absent) date yields NULL year/month with ``year_is_unknown`` /
    ``month_is_unknown`` = True — a *visible* unknown, not a silent drop. A single
    ``groupBy`` treats NULL as its own group, so the datadict path is safe as-is; the
    ontology path additionally sentinel-fills NULL -> -1 (see :func:`_sentinel_unknown_dates`)
    because Spark ``NULL != NULL`` would drop unknown-date rows from its INNER joins.

    Shared by :meth:`ExtractItem.build_datadict`, :meth:`ExtractItem.build_ontology_counts`,
    and :meth:`ExtractItem.build_ontology_coverage`."""
    need = [p for p in ('year', 'month') if p in group_by and p not in df.columns]
    if not need:
        return df
    if datefield is None:
        raise ValueError(
            "build_*: group_by needs {} but no datefield was given to derive them "
            "and the columns are absent.".format(need))
    fields = datefield if isinstance(datefield, (list, tuple)) else [datefield]
    d = F.to_date(F.coalesce(*[F.col(c) for c in fields]))
    unknown = d.isNull()
    for part, fn in (('year', F.year), ('month', F.month)):
        if part in need:
            df = df.withColumn(part, fn(d))
            df = df.withColumn(part + '_is_unknown', unknown)
    return df


def _sentinel_unknown_dates(df, group_by):
    """Fill NULL ``year``/``month`` in ``group_by`` with the sentinel ``-1`` so INNER
    joins and the denominator join don't silently drop unknown-date rows (Spark
    ``NULL != NULL``). Use on the ontology paths (which join on ``group_by``); the
    ``*_is_unknown`` flag from :func:`_derive_date_parts` still marks the sentinel rows.
    The datadict path (single ``groupBy``, no join) does NOT need this."""
    for part in ('year', 'month'):
        if part in group_by and part in df.columns:
            df = df.withColumn(part, F.coalesce(F.col(part), F.lit(-1).cast('int')))
    return df


def _union_aligned(frames):
    """``unionByName`` a list of frames after aligning to the union of their columns
    (Spark 2.4.4 has no ``allowMissingColumns``). Missing columns are added as NULL.
    Returns None for an empty list. Shared by the ``build_*`` methods."""
    frames = [f for f in frames if f is not None]
    if not frames:
        return None
    all_cols = []
    for f in frames:
        for c in f.columns:
            if c not in all_cols:
                all_cols.append(c)
    aligned = []
    for f in frames:
        for c in all_cols:
            if c not in f.columns:
                f = f.withColumn(c, F.lit(None))
        aligned.append(f.select(*all_cols))
    result = aligned[0]
    for extra in aligned[1:]:
        result = result.unionByName(extra)
    return result
