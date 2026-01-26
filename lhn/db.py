
from lhn.item import TableList
from lhn.spark_utils import check_table_existence, writeTable 

from lhn.shared_methods import SharedMethodsMixin
from lhn.header import get_logger

logger = get_logger(__name__)


class DB(SharedMethodsMixin):
    """
    Purpose: Load Tables as listed in the YAML control document.
    For every table listed in `dataset`, add a property to self, which will be the returned object,
    that has a name chosen by the name given in the YAML and a value of the actual data table from 
    the schema, or a None value if the table does not yet exist.
    usage: db = DB(resources.__dict__, 'project')
    """
    def __init__(self, resources_dict, dataset):
        resources = resources_dict.get(dataset)
        if resources:
            table_list = TableList(resources).__dict__
            for key in table_list.keys():
                setattr(self, key, getattr(table_list[key], 'df', None))  

    
        
    def writeTBL(self, reRun= None):
        
        reRun                   = reRun if reRun is not None else self.reRun
        # self.listLocation = f"{self.tableName}_{self.columnName}"
        self.description = f"Count of People in table {self.listLocation} by {self.countLevelsColumns}"
        self.tableListExists      = check_table_existence(self.schema, f"{self.name}_{self.schemaTag}")
        
        if reRun or not self.tableListExists:
            print(f"A new table {self.listLocation} is being written by writeTBL")
            try:
                # Added 'overwrite=True' parameter to the writeTable function
                writeTable(self.df, f"{self.listLocation}", description=self.description)
            except Exception as e:
                print(f"Error writing table: {str(e)}")
        elif self.tableListExists:
            print(f"A new table {self.listLocation} is NOT being written by writeTBL, because it already exists")
        else:
            pass
            # print(f"rerun = {rerun} and {self.listLocation} does not exist")



