from lhn.shared_methods import SharedMethodsMixin
from lhn.data_display import showIU
from lhn.data_transformation import fieldRequested, flatSchema
from lhn.introspection_utils import show_attributes
from lhn.spark_utils import assignPropertyFromDictionary, create_empty_df, flattenTable, writeTable
from lhn.header import spark, F
from lhn.header import get_logger

logger = get_logger(__name__)
from lhn.shared_methods import SharedMethodsMixin
from lhn.data_display import showIU
from lhn.data_transformation import fieldRequested, flatSchema
from lhn.introspection_utils import show_attributes
from lhn.spark_utils import assignPropertyFromDictionary, create_empty_df, flattenTable, writeTable
from lhn.header import spark, F
from lhn.header import get_logger
import re

logger = get_logger(__name__)

class TableList:
    """
    Convert a dictionary to object properties.
    Usage: rwd = TableList(resources.rwd)
    Supports attribute access (e.g., tableList.death), dictionary-like indexing (e.g., tableList['death']),
    and iteration (e.g., tableList.items()).
    """
    def __init__(self, tableDict):
        for key, value in tableDict.items():
            setattr(self, key, value)
    
    def items(self):
        """
        Return a list of (key, value) tuples for the object's attributes, mimicking a dictionary's items() method.
        Excludes private attributes (starting with '_') and callable attributes (methods).
        """
        return [(key, getattr(self, key)) for key in dir(self) if not key.startswith('_') and not callable(getattr(self, key))]
    
    def __getitem__(self, key):
        """
        Support dictionary-like indexing (e.g., tableList['death']).
        Raises KeyError if the key does not exist.
        """
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(f"'{key}' not found in TableList")
    
    def __repr__(self):
        """
        Return a string representation of the TableList, showing table names and their Item instances.
        """
        items = self.items()
        if not items:
            return "<TableList (empty)>"
        return "<TableList with tables: " + ", ".join(key for key, _ in items) + ">"

class Item(SharedMethodsMixin):
    """
    Used in processDataTables.
    Siblings are class Extract and Meta and DB. Instantiations are rwd and iuh. All three could be merged in future development.
    
    dataTables    : A Dictionary of table names created from the configuration YAML
    TBL           : The key of this dictionary identifying the table
    source_list   : A list of all tables in the source schema. Tables listed in dataTables are checked against this list to see if they already exist.
    schema        : The schema where the tables are supposed to exist
    dataLoc       : A file location of the directory where csv and parquet versions of the files can be stored
    disease       : A tag that can be used in the storage name of the table to help differentiate it from other similar tables.
    schemaTag     : A tag that can be used in the storage name of the table to help differentiate it from other similar tables.
    project       : The project name (e.g., SickleCell) for reference
    parquetLoc    : HDFS base path for parquet files (e.g., hdfs:///user/hnelson3/SickleCell/)
    debug         : Use to generate additional code running annotations
    """
    def __init__(self, TBL, dataTables, TBLLoc, schema, dataLoc, disease, schemaTag, project, parquetLoc, debug=False):
        self.debug = debug
        self.project = project
        for key in dataTables[TBL]:
            setattr(self, key, assignPropertyFromDictionary(key, dataTables[TBL]))

        if not hasattr(self, 'name'):
            self.name = TBL
        if hasattr(self, 'source'):
            if '.' in self.source:
                self.source = self.source.split('.', 1)[-1]
        else:
            self.source = f"{self.name}_{disease}_{schemaTag}"
            
        if not hasattr(self, 'csv'):
            self.csv = f"{dataLoc}{self.name}_{disease}_{schemaTag}.csv"
        else:
            logger.info(f"Assigning csv: {dataLoc}{self.csv}")
            
        if not hasattr(self, 'parquet'):
            self.parquet = f"{parquetLoc}{self.name}_{disease}_{schemaTag}.parquet"
        else:
            logger.info(f"Assigning parquet: {self.parquet}")
            
        self.schema = schema
        self.location = f"{self.schema}.{self.source}"

        self.existsDF = self.source.lower() in TBLLoc.keys()
        
        if self.existsDF:
            try:
                logger.info(f"Create name: {self.name} from location: {self.location}")
                try:
                    self.df = spark.sql(f"SELECT * FROM {self.location}")
                except Exception as e:
                    logger.error(f"Cannot spark.sql(SELECT * FROM {self.location}): {str(e)}")
                if hasattr(self, 'inputRegex') and self.inputRegex is not None:
                    try:
                        logger.info(f"Using inputRegex: {self.inputRegex}")
                        dfFlat = flattenTable(self.df, self.inputRegex)
                        self.columnsRemap = [F.col(field).alias(field.replace(".","_")) for field in flatSchema(self.df)
                            if fieldRequested(self.df, field, self.inputRegex)]
                        self.df = self.df.select(self.columnsRemap)
                    except Exception as e:
                        logger.error(f"Unable to flattenTable or remap columns: {str(e)}")
                else:
                    self.columnsRemap = None
                if hasattr(self, 'insert'):
                    try:
                        for insert_code in self.insert:
                            exec(f"self.df = self.df.{insert_code}")
                    except Exception as e:
                        logger.error(f"Problem with insert_code: {insert_code}: {str(e)}")
                if hasattr(self, 'colsRename'):
                    logger.info(f"Renaming Column names: {self.colsRename}")
                    try:
                        for old_col_name, new_col_name in self.colsRename.items():
                            self.df = self.df.withColumnRenamed(old_col_name, new_col_name)
                    except Exception as e:
                        logger.error(f"Problem with colsRename: {str(e)}")
            except Exception as e:
                logger.error(f"TBL: {TBL}, source: {dataTables[TBL].get('source', 'N/A')}, location: {dataTables[TBL].get('source', 'N/A')} constructDataObject error: {str(e)}")
                self.df = create_empty_df()
                self.columnsRemap = None
        else:
            self.df = create_empty_df()

        if debug:
            logger.debug(f"Item initialized: {self}")

    def show_attributes(self):
        show_attributes(self)
        
    def writeTable(self):
        writeTable(self.df, self.location, description=self.label)
        
    def readTable(self):
        self.df = spark.table(self.location)
        
    def showIU(self, obs=6, index=['personid']):
        if not hasattr(self, 'index'):
            i = index
        else:
            i = self.index
        return showIU(self.df, obs=obs, index=i)
        
    def head(self, obs=6):
        self.df.limit(obs).toPandas()
        
    def properties(self):
        return self.__dict__.keys()
    
    def values(self):
        for prop_name, prop_value in self.__dict__.items():
            logger.info(f"{prop_name}: {prop_value}")
        return self.__dict__