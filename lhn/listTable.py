from lhn.query import extract_fields_flat_top
from lhn.header import F
from lhn.spark_utils import check_table_existence, writeTable
from lhn.header import get_logger

logger = get_logger(__name__)


class ListTable:
    """Create an Object to Hold a List Table

    Raises:
        Exception: _description_
        ValueError: _description_

    Returns:
        _type_: _description_
    """
                    
    def __init__(self, mtable):
        self.name                 = mtable.listSource
        self.label                = f"{mtable.col} from {mtable.listSource}"
        self.df                   = mtable.list

        # Directly assigning properties from mtable
        self.col_expanded         = mtable.col_expanded
        self.countLevelsColumns   = mtable.countLevelsColumns
        self.countfield           = mtable.countfield
        self.index                = mtable.index
        self.obs                  = mtable.obs
        self.reRun                = mtable.reRun
        self.schema               = mtable.mSchema.outSchema
        self.columnName           = mtable.columnName
        self.list                 = mtable.list
        self.listSource           = mtable.listSource
        self.listLocation         = mtable.listLocation
        self.countLevelsColumns   = mtable.countLevelsColumns
        self.schemaTag            = mtable.schemaTag
        self.tableListExists      = mtable.tableListExists
        self.list_to_csv          = mtable.list_to_csv
        self.personid             = mtable.personid
        
        
        self.tableListExists      = check_table_existence(self.listLocation)

    def writeList(self, reRun = None, index=None, obs=None, toPandas=False, countfield=None):
        
        reRun = reRun if reRun is not None else self.reRun
        print(f"reRun is {reRun}")
        countfield = countfield if countfield is not None else self.countfield

        description = f"Count of People in table {self.listLocation} by {self.countLevelsColumns}"
        
        self.tableListExists = check_table_existence(self.listLocation)

        if reRun or not self.tableListExists:
            print(f"Because ReRun = {reRun} and self.tableListExist = {self.tableListExists}, A new table {self.listLocation} will be created if `writeTBL` is called")
            print(f"calling extract_fields_flat_top(table={self.col_expanded.columns}, i={self.countLevelsColumns}, index={self.personid}, obs={self.obs}, toPandas={toPandas}, countfield={countfield})")
            
            self.list = extract_fields_flat_top(table=self.col_expanded, i=self.countLevelsColumns, index=self.personid, obs=self.obs, 
                                                toPandas=False, countfield=countfield)
            
            try:
                writeTable(self.list, f"{self.listLocation}", description=description)
            except Exception as e:
                print(f"Error inside listTable.writeList, self.listLocation = {self.listLocation}")
                print(f"Error writing table: {str(e)}")
                print(f"reRun = {reRun} and self.tableListExist = {self.tableListExists}\n \n")
        else:
            print(f"A new table {self.listLocation} is NOT being written by writeTBL, because it already exists")