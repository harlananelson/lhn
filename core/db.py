"""
lhn/core/db.py

DB class - Database operations wrapper for healthcare data access.
"""

from lhn.header import spark, F, get_logger
from lhn.core.shared_methods import SharedMethodsMixin

logger = get_logger(__name__)


class DB(SharedMethodsMixin):
    """
    Database operations wrapper for healthcare data.
    
    Provides a simple interface for loading and working with Spark tables,
    extending SharedMethodsMixin with database-specific operations.
    
    Example:
        >>> db = DB('my_schema.my_table')
        >>> db.df.count()
        >>> db.filter(F.col('status') == 'active')
        >>> db.write('my_schema.filtered_table')
    """
    
    def __init__(self, table_path=None, df=None, label=None):
        """
        Initialize DB with a table path or DataFrame.
        
        Parameters:
            table_path (str): Full table path (schema.table_name)
            df (DataFrame): Pre-existing DataFrame
            label (str): Description for reporting
        """
        self.location = table_path
        self.label = label or table_path or 'DB'
        self._df = df
        
        if table_path and df is None:
            self.load()
    
    @property
    def df(self):
        return self._df
    
    @df.setter
    def df(self, value):
        self._df = value
    
    def load(self, table_path=None):
        """
        Load DataFrame from Spark table.
        
        Parameters:
            table_path (str): Table path (uses self.location if not provided)
        """
        path = table_path or self.location
        if not path:
            logger.error("No table path specified")
            return
        
        try:
            self._df = spark.table(path)
            self.location = path
            logger.info(f"Loaded table: {path}")
        except Exception as e:
            logger.error(f"Failed to load {path}: {e}")
    
    def query(self, sql_query):
        """
        Execute a SQL query and store result as df.
        
        Parameters:
            sql_query (str): SQL query string
        """
        try:
            self._df = spark.sql(sql_query)
        except Exception as e:
            logger.error(f"Query failed: {e}")
    
    def use_database(self, schema):
        """
        Set the current database context.
        
        Parameters:
            schema (str): Schema/database name
        """
        try:
            spark.sql(f"USE {schema}")
            logger.info(f"Using database: {schema}")
        except Exception as e:
            logger.error(f"Failed to use database {schema}: {e}")
    
    def drop_table(self, table_path=None):
        """
        Drop a Spark table.
        
        Parameters:
            table_path (str): Table to drop (uses self.location if not provided)
        """
        path = table_path or self.location
        if not path:
            logger.error("No table path specified")
            return
        
        try:
            spark.sql(f"DROP TABLE IF EXISTS {path}")
            logger.info(f"Dropped table: {path}")
        except Exception as e:
            logger.error(f"Failed to drop {path}: {e}")
    
    def rename_table(self, new_name):
        """
        Rename the current table.
        
        Parameters:
            new_name (str): New table name
        """
        if not self.location:
            logger.error("No current table to rename")
            return
        
        try:
            spark.sql(f"ALTER TABLE {self.location} RENAME TO {new_name}")
            self.location = new_name
            logger.info(f"Renamed to: {new_name}")
        except Exception as e:
            logger.error(f"Rename failed: {e}")
    
    @classmethod
    def from_sql(cls, sql_query, label=None):
        """
        Create DB instance from SQL query.
        
        Parameters:
            sql_query (str): SQL query string
            label (str): Description
        
        Returns:
            DB: New DB instance with query results
        """
        df = spark.sql(sql_query)
        return cls(df=df, label=label)
    
    @classmethod
    def from_pandas(cls, pandas_df, label=None):
        """
        Create DB instance from pandas DataFrame.
        
        Parameters:
            pandas_df: Pandas DataFrame
            label (str): Description
        
        Returns:
            DB: New DB instance
        """
        df = spark.createDataFrame(pandas_df)
        return cls(df=df, label=label)
