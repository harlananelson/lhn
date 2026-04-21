"""
lhn/core/shared_methods.py

Mixin class providing common methods for Extract, Item, and DB classes.
Provides standardized data display, export, and analysis operations.
"""

from lhn.header import (
    spark, F, pd, DataFrame, pprint, display, Markdown, get_logger
)
from spark_config_mapper import writeTable

logger = get_logger(__name__)


class SharedMethodsMixin:
    """
    Mixin providing common data manipulation methods for healthcare data containers.
    
    Classes that include this mixin gain access to standardized methods for:
    - Data display and exploration
    - Export to various formats (CSV, Parquet, Spark tables)
    - Attrition reporting
    - DataFrame transformations
    
    Expected attributes on the host class:
    - df: Spark DataFrame
    - location: Table path (optional)
    - csv: CSV export path (optional)
    - parquet: Parquet export path (optional)
    - label: Description string (optional)
    """
    
    def show(self, n: int = 5, truncate: bool = True):
        """
        Display the first n rows of the DataFrame.
        
        Parameters:
            n (int): Number of rows to display
            truncate (bool): Whether to truncate long values
        """
        if hasattr(self, 'df') and self.df is not None:
            self.df.show(n, truncate)
        else:
            print("No DataFrame available")
    
    def toPandas(self, limit: int = None) -> pd.DataFrame:
        """
        Convert DataFrame to pandas, optionally with a limit.
        
        Parameters:
            limit (int, optional): Maximum rows to return
        
        Returns:
            pd.DataFrame: Pandas DataFrame
        """
        if not hasattr(self, 'df') or self.df is None:
            return pd.DataFrame()
        
        if limit:
            return self.df.limit(limit).toPandas()
        return self.df.toPandas()
    
    def count(self) -> int:
        """Return the row count of the DataFrame."""
        if hasattr(self, 'df') and self.df is not None:
            return self.df.count()
        return 0
    
    def columns(self) -> list:
        """Return the column names of the DataFrame."""
        if hasattr(self, 'df') and self.df is not None:
            return self.df.columns
        return []
    
    def schema(self):
        """Print the DataFrame schema."""
        if hasattr(self, 'df') and self.df is not None:
            self.df.printSchema()
    
    def attrition(self, person_id: str = 'personid', description: str = None,
                  date_field: str = None):
        """
        Display attrition statistics for the DataFrame.

        Shows record count, unique person count, and date range if applicable.

        Parameters:
            person_id (str): Column name for person identifier
            description (str): Description for the display. Falls back to
                self.label from config.
            date_field (str): Date field for range reporting. Falls back to
                self.datefieldPrimary from config.
        """
        if not hasattr(self, 'df') or self.df is None:
            print("No DataFrame available")
            return

        df = self.df
        desc = description or getattr(self, 'label', 'Unknown')

        # Fall back to config datefieldPrimary if not specified
        if date_field is None:
            date_field = getattr(self, 'datefieldPrimary', None)
            # datefieldPrimary may be a list — take first element
            if isinstance(date_field, list) and date_field:
                date_field = date_field[0]

        count_records = df.count()
        display(Markdown(f"**{desc}**"))
        display(Markdown(f"* {count_records:,} records"))

        if person_id in df.columns:
            count_people = df.select(person_id).distinct().count()
            display(Markdown(f"* {count_people:,} unique by {person_id}"))

        if date_field and date_field in df.columns:
            date_stats = df.select(
                F.min(date_field), F.max(date_field)
            ).toPandas()
            display(Markdown(
                f"* Date range: {date_stats.iloc[0][0]} - {date_stats.iloc[0][1]}"
            ))
    
    def write(self, outTable: str = None, mode: str = 'overwrite',
              partitionBy: str = None):
        """
        Write DataFrame to a Spark table.
        
        Parameters:
            outTable (str): Table path (uses self.location if not provided)
            mode (str): Write mode ('overwrite', 'append')
            partitionBy (str): Optional partition column
        """
        if not hasattr(self, 'df') or self.df is None:
            logger.error("No DataFrame to write")
            return
        
        table_path = outTable or getattr(self, 'location', None)
        if not table_path:
            logger.error("No table path specified")
            return
        
        description = getattr(self, 'label', '')
        writeTable(self.df, table_path, description, partitionBy, mode)
        # In-memory df now matches Hive; clear the dirty flag so a later
        # Extract.write_all() can skip this item. Attribute may be absent
        # on non-ExtractItem users of this mixin — setattr avoids surprise.
        if hasattr(self, '_dirty'):
            self._dirty = False
        logger.info(f"Written to {table_path}")
    
    def to_csv(self, path: str = None):
        """
        Export DataFrame to CSV.
        
        Parameters:
            path (str): CSV file path (uses self.csv if not provided)
        """
        if not hasattr(self, 'df') or self.df is None:
            logger.error("No DataFrame to export")
            return
        
        csv_path = path or getattr(self, 'csv', None)
        if not csv_path:
            logger.error("No CSV path specified")
            return
        
        self.df.toPandas().to_csv(csv_path, index=False)
        logger.info(f"Exported to {csv_path}")
    
    def to_parquet(self, path: str = None):
        """
        Export DataFrame to Parquet.
        
        Parameters:
            path (str): Parquet path (uses self.parquet if not provided)
        """
        if not hasattr(self, 'df') or self.df is None:
            logger.error("No DataFrame to export")
            return
        
        parquet_path = path or getattr(self, 'parquet', None)
        if not parquet_path:
            logger.error("No Parquet path specified")
            return
        
        self.df.write.mode('overwrite').parquet(parquet_path)
        logger.info(f"Exported to {parquet_path}")
    
    def load(self, table_path: str = None):
        """
        Load DataFrame from a Spark table.
        
        Parameters:
            table_path (str): Table path (uses self.location if not provided)
        """
        path = table_path or getattr(self, 'location', None)
        if not path:
            logger.error("No table path specified")
            return
        
        try:
            self.df = spark.table(path)
            logger.info(f"Loaded from {path}")
        except Exception as e:
            logger.error(f"Failed to load {path}: {e}")
    
    def distinct(self):
        """Return a new object with distinct rows."""
        if hasattr(self, 'df') and self.df is not None:
            self.df = self.df.distinct()
        return self
    
    def filter(self, condition):
        """
        Filter the DataFrame by a condition.
        
        Parameters:
            condition: Spark SQL condition or Column expression
        """
        if hasattr(self, 'df') and self.df is not None:
            self.df = self.df.filter(condition)
        return self
    
    def select(self, *cols):
        """
        Select specific columns.
        
        Parameters:
            *cols: Column names or Column expressions
        """
        if hasattr(self, 'df') and self.df is not None:
            self.df = self.df.select(*cols)
        return self
    
    def join(self, other, on=None, how='inner'):
        """
        Join with another DataFrame or object with df attribute.
        
        Parameters:
            other: DataFrame or object with df attribute
            on: Join columns
            how: Join type
        """
        if not hasattr(self, 'df') or self.df is None:
            logger.error("No DataFrame to join")
            return self
        
        other_df = other.df if hasattr(other, 'df') else other
        self.df = self.df.join(other_df, on=on, how=how)
        return self
    
    def cache(self):
        """Cache the DataFrame in memory."""
        if hasattr(self, 'df') and self.df is not None:
            self.df = self.df.cache()
        return self
    
    def unpersist(self):
        """Remove DataFrame from cache."""
        if hasattr(self, 'df') and self.df is not None:
            self.df.unpersist()
        return self
    
    def tabulate(self, group_cols=None, count_distinct=None,
                  order_by='count', limit=50, dropna=False, show=False):
        """
        Create a tabulation summary of the DataFrame.

        Returns a Spark DataFrame with counts (and optional distinct counts)
        grouped by specified columns. Does not call toPandas() by default
        to avoid driver OOM on clinical-scale data.

        Parameters:
            group_cols (str|list): Column(s) to group by. If None, returns
                total count and optional distinct count.
            count_distinct (str): Column for distinct count (e.g., 'personid')
            order_by (str): 'count' to sort by count desc, 'group' to sort by
                group columns asc, or a column name
            limit (int): Maximum rows to return
            dropna (bool): Drop null values in group columns before counting
            show (bool): If True, display the result table

        Returns:
            DataFrame: Summary DataFrame with counts
        """
        if not hasattr(self, 'df') or self.df is None:
            logger.error("No DataFrame available")
            return None

        df = self.df

        if dropna and group_cols:
            cols = [group_cols] if isinstance(group_cols, str) else group_cols
            for c in cols:
                if c in df.columns:
                    df = df.filter(F.col(c).isNotNull())

        if group_cols:
            if isinstance(group_cols, str):
                group_cols = [group_cols]
            result = df.groupBy(group_cols).agg(F.count('*').alias('count'))
            if count_distinct and count_distinct in df.columns:
                result = df.groupBy(group_cols).agg(
                    F.count('*').alias('count'),
                    F.countDistinct(count_distinct).alias(f'n_{count_distinct}')
                )
        else:
            aggs = [F.count('*').alias('count')]
            if count_distinct and count_distinct in df.columns:
                aggs.append(F.countDistinct(count_distinct).alias(f'n_{count_distinct}'))
            result = df.agg(*aggs)

        # Sort
        if group_cols and order_by == 'count':
            result = result.orderBy(F.desc('count'))
        elif group_cols and order_by == 'group':
            result = result.orderBy(group_cols)
        elif group_cols and order_by in result.columns:
            result = result.orderBy(F.desc(order_by))

        if limit:
            result = result.limit(limit)

        if show:
            result.show(limit, truncate=False)

        return result

    def properties(self) -> dict:
        """Return dictionary of object properties."""
        return {k: v for k, v in self.__dict__.items() 
                if not k.startswith('_') and k != 'df'}
    
    def values(self):
        """Print all object properties."""
        pprint.pprint(self.__dict__)

    def showIU(self, obs=6, index=None, tenant=127, sortfield=None,
               sort_order='asc', label=None):
        """
        Display sample records filtered to a specific tenant.

        Useful for data exploration while ensuring you only see records
        from a specific data source (e.g., IUHealth Expert Determination).

        Parameters:
            obs (int): Number of rows to display
            index (list): Index columns for sorting (default: ['personid'])
            tenant (int): Tenant ID to filter (127=IUH Expert, 82=IUH Safe Harbor)
            sortfield (list): Columns to sort by (defaults to index)
            sort_order (str): 'asc' or 'desc'
            label (str): Label for display header

        Example:
            >>> e.medications.showIU(obs=10, tenant=127)
        """
        if not hasattr(self, 'df') or self.df is None:
            print("No DataFrame available")
            return

        if index is None:
            index = getattr(self, 'index', ['personid'])
        if sortfield is None:
            sortfield = index

        result = self.df

        # Filter to tenant if column exists
        if 'tenant' in result.columns:
            result = result.filter(F.col('tenant') == tenant)

        # Sort
        if isinstance(sortfield, str):
            sortfield = [sortfield]

        sort_cols = []
        for f in sortfield:
            if f in result.columns:
                if sort_order == 'desc':
                    sort_cols.append(F.desc(f))
                else:
                    sort_cols.append(F.asc(f))

        if sort_cols:
            result = result.orderBy(*sort_cols)

        # Display
        display_label = label or getattr(self, 'label', '')
        if display_label:
            display(Markdown("**{}**".format(display_label)))

        pdf = result.limit(obs).toPandas()
        display(pdf)

    def print_pd(self, label=None, sortfield='Subjects', obs=5,
                 sort_order='desc'):
        """
        Display DataFrame as a formatted pandas table.

        Parameters:
            label (str): Display header. Falls back to self.label.
            sortfield (str|list): Column(s) to sort by.
            obs (int): Number of rows to display. All `obs` rows are
                rendered in full (no pandas first-5/last-5 summary
                truncation); raise `obs` to verify an entire code list.
            sort_order (str): 'desc' or 'asc'.
        """
        if self.df is None:
            print("No DataFrame available")
            return

        desc = label or getattr(self, 'label', '')
        if isinstance(sortfield, str):
            sortfield = [sortfield]

        df = self.df
        if all(f in df.columns for f in sortfield):
            sort_expr = [F.desc(f) if sort_order == 'desc' else F.asc(f) for f in sortfield]
            df = df.sort(*sort_expr)

        pdf = df.limit(obs).toPandas()
        if desc:
            display(Markdown(f"**{desc}**"))
        # Override pandas' default display.max_rows (60) so that when the
        # caller passes a large obs to verify an entire code list, every
        # row renders instead of the first-5/last-5 summary view.
        with pd.option_context('display.max_rows', obs):
            display(pdf)

    def plotByTime(self, datefield=None, title=None, grouping=None, **kwargs):
        """
        Plot record volume over time using lhn.plot.plotByTime.

        Convenience method that passes self.df to the standalone plotByTime
        function with config-driven defaults.

        Parameters:
            datefield (str): Date column. Falls back to self.datefieldPrimary.
            title (str): Plot title. Falls back to self.label.
            grouping (str): Column for color grouping (e.g., 'group').
            **kwargs: Additional arguments passed to lhn.plot.plotByTime
                (date_low, useLog, time_group, plot_lib, etc.)

        Returns:
            plotnine ggplot or plotly Figure
        """
        if self.df is None:
            logger.error("No DataFrame available for plotting")
            return None

        from lhn.plot import plotByTime as _plotByTime

        if datefield is None:
            datefield = getattr(self, 'datefieldPrimary', None)
            if isinstance(datefield, list) and datefield:
                datefield = datefield[0]
        if datefield is None:
            logger.error("No datefield specified and no datefieldPrimary in config")
            return None

        if title is None:
            title = getattr(self, 'label', '')

        return _plotByTime(self.df, datefield=datefield, title=title,
                           grouping=grouping, **kwargs)
