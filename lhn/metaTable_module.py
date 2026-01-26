
from lhn.database_operations import create_id_columns_dict
from lhn.header import pprint, spark, AnalysisException
from lhn.data_display import print_pd
from lhn.list_operations import get_element_index

from lhn.introspection_utils import extractTableLocations, get_root_columns_elements, get_standard_id_elements

from lhn.data_transformation import expand_arrays_in_df, flatSchema

from lhn.spark_utils import check_table_existence, distCol, getTableList, writeTable

from lhn.database_operations import load_table
from lhn.query import extract_fields_flat, extract_fields_flat_top

from lhn.header import get_logger

logger = get_logger(__name__)





class metaSchema():
    """
    Explore schemas by analyzing tables and sets of columns as identified using`pattern_strings`
    Works with class metaTable
    """
    def __init__(self,schema, outSchema, schemaTag,
                 pattern_strings = ['standard.id', 'standard.codingSystemId', 'standard.primaryDisplay', 'value', 'brandType', 'zipcode1', 'dateofdeath'] ,
                 obs = 1000000, reRun = True, debug = False, personid = ['personid'] ):
            self.schema             = schema
            self.outSchema          = outSchema
            self.schemaTag          = schemaTag
            self.pattern_strings    = pattern_strings
            self.obs                = obs
            self.reRun              = reRun
            self.debug              = debug
            self.personid           = personid
            try:
                self.tableList          = getTableList(self.schema)
            except Exception as e:
                print(f"Error getting table list: {str(e)}")
                self.tableList = None
            try:
                self.TBLLoc             = extractTableLocations(self.tableList, self.schema)
            except Exception as e:
                print(f"Error getting table locations: {str(e)}")
                self.TBLLoc = None
            try:
                self.id_cols            = create_id_columns_dict(self.schema)
            except Exception as e:
                print(f"Error getting id columns: {str(e)}")
                self.id_cols = None


class metaTable:
    """
    Explore schemas by analyzing tables and sets of columns as identified using`pattern_strings`
    """
    def __init__(self, tableName, mSchema, 
                 pattern_strings = None,
                 debug        = None,
                 obs = None, 
                 reRun = None
                 ):
        self.tableName                = tableName
        self.mSchema                  = mSchema
        self.pattern_strings          = pattern_strings if pattern_strings is not None else mSchema.pattern_strings
        self.debug                    = debug if debug is not None else mSchema.debug
        self.obs                      = obs if obs is not None else mSchema.obs
        self.reRun                    = reRun if reRun is not None else mSchema.reRun
        self.personid                 = mSchema.personid
        self.countfield: str          = "Subjects"
        self.toPandas                 = False
        try:
            self.df                       = load_table(tableName, mSchema.TBLLoc)
        except Exception as e:
            print(f"Error loading table: {str(e)}")
            self.df = None
        try:
            self.flat_columns             = flatSchema(self.df)                             
        except Exception as e:
            print(f"Error getting flat schema: {str(e)}")
            self.flat_columns = None
        try:
            self.root_columns             = get_standard_id_elements(self.flat_columns, self.pattern_strings, debug = self.debug)
        except Exception as e: 
            print(f"Error getting root columns: {str(e)}")
            self.root_columns = None
        self.schemaTag                = mSchema.schemaTag
        
        if debug: print(f" the root columns are {self.root_columns} \n")
        self.id_cols                  = mSchema.id_cols[tableName]  
        try:
            self.root_columns_elements    = get_root_columns_elements(self.flat_columns, self.root_columns, self.pattern_strings)
        except Exception as e:
            print(f"Error getting root column elements: {str(e)}")
            self.root_columns_elements = None
        self.cols                     = [item for sublist in self.root_columns_elements for item in sublist] + self.id_cols
        if debug: print(f"the pattern_strings are {self.pattern_strings}")
        try:
            self.cols_expanded = expand_arrays_in_df(self.df, self.cols)
        except AnalysisException as e:
            print(f"Error occurred while expanding named lists: {e}")
            self.cols_expanded = None  # Set cols_expanded to None if expansion fails
        except Exception as e:
            print(f"Unknown error occurred: {e}")
            self.cols_expanded = None  # Set cols_expanded to None for any other exceptions

    def retrict(self, cohort, index):
        self.df = self.df.join(cohort, on = index, how = 'inner')
        
    def column(self,col, reRun = None):
        reRun                   = reRun if reRun is not None else self.reRun
        self.columnName         = col
        self.index              = get_element_index(self.columnName, self.root_columns)
        self.root               = self.root_columns_elements[self.index]
        self.col                = [*self.root_columns_elements[self.index], *self.id_cols]
        self.col_expanded       = expand_arrays_in_df(self.df, self.col)
        self.listSource         = self.tableName + '_' + self.columnName
        self.listLocation       = f"{self.mSchema.outSchema}.{self.listSource}_{self.schemaTag}"
        self.tableListExists    = check_table_existence(self.listLocation)
        
    def countLevels(self, by = None, index=None, toPandas=False, custom_i=None, 
                countfield = None, reRun = None, personid = None):
        
        """
        This function counts the levels by the given parameters.

        Parameters:
        by (list): A list of elements to group by.
        index (list): A list of indices.
        toPandas (bool): If True, returns a pandas DataFrame. If False, returns a Spark DataFrame.
        custom_i (list): A custom list of elements.
        countfield (str): The field to count.
        reRun (bool): If True, reruns the function.

        Returns:
        countLevelsColumns (list): A list of columns used for aggregation.
        list (DataFrame): A data table with the counts by countLevelsColumns.
    """
        print("this is the metatable method countLevels")
        print(f"countfield is {self.countfield}")
        self.reRun                   = reRun if reRun is not None else self.reRun
        self.index                   = index if index is not None else self.index
        self.countfield              = countfield if countfield is not None else self.countfield
        self.personid                = personid if personid is not None else self.personid
        
        if custom_i is None:
            i = self.root_columns_elements[self.index]
        else:
            i = custom_i    
        self.countLevelsColumns = i
        if by:
            print(f"adding elment {distCol(by, masterList = self.df.columns)} from by list {by}")
            self.countLevelsColumns = distCol(self.countLevelsColumns + by, masterList = self.df.columns)
        if self.reRun or not self.tableListExists:
            if self.debug:
                print(f'''extract_fields_flat_top(table={self.col_expanded}, i={self.countLevelsColumns}, index={self.personid}, obs={self.obs}, 
                                                toPandas={self.toPandas}, countfield={self.countfield})''')
            self.list = extract_fields_flat_top(table=self.col_expanded, i=self.countLevelsColumns, index=self.personid, obs=self.obs, 
                                                toPandas=self.toPandas, countfield=self.countfield)
            print(f"Because ReRun = {self.reRun} and self.tableListExist = {self.tableListExists}, A new table {self.listLocation} will be created if `writeTBL` is called")
        elif self.tableListExists:
            self.list = spark.table(self.listLocation)
        else:
            pass
            # print(f"rerun = {self.reRun} and {self.listLocation} does not exist")

    def writeList(self, reRun = None, countfield = None):
        reRun                   = reRun if reRun is not None else self.reRun
        tableName = f"{self.tableName}_{self.columnName}"
        description = f"Count of People in table {self.listLocation} by {self.countLevelsColumns}"
        countfield = countfield if countfield is not None else self.countfield
        
        if reRun or not self.tableListExists:
            print(f"A new table {self.listLocation} is being written by writeTBL")
            try:
                writeTable(self.list, f"{self.listLocation}", description=description)
            except Exception as e:
                print(f"Error writing table: {str(e)}")
        elif self.tableListExists:
            print(f"A new table {self.listLocation} is NOT being written by writeTBL, because it already exists")
        else:
            pass
            # print(f"rerun = {self.reRun} and {self.listLocation} does not exist")
        
        
    def list_to_csv(self):
        self.list.toPandas().to_csv(self.csv, index = False)
        

    def to_csv(self):
        self.df.toPandas().to_csv(self.csv, index = False)
        
    def properties(self):
        # Create a dictionary comprehension to get property names and values
        return {key: getattr(self, key) for key in self.__dict__.keys()}

    def print(self):
        print_pd(self.df, label='', nrows=5)
        
    def values(self):
        pprint.pprint(self.__dict__)
    