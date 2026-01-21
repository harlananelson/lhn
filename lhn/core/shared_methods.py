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
            description (str): Description for the display
            date_field (str): Optional date field for range reporting
        """
        if not hasattr(self, 'df') or self.df is None:
            print("No DataFrame available")
            return
        
        df = self.df
        desc = description or getattr(self, 'label', 'Unknown')
        
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
        display(pdf.style.hide(axis='index'))
