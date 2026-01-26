
from lhn.header import reduce, spark, pd, F, List
from foresight.database_util import use_database
from lhn.spark_utils import getListOfTables, writeTable
from lhn.header import get_logger

logger = get_logger(__name__)


def search_object(database, table):
    if spark.catalog.tableExists(database, table):
        return True
    else:
        return False
    

    
def get_id_columns(data_frame):
    """ Get columns that end with 'id' case insensitively. """
    return [col for col in data_frame.columns if col.lower().endswith('id')]

def create_id_columns_dict(database_name):
    """
    Returns a dictionary mapping table names to ID columns for all external tables in a specified Spark database.
    Only includes elements in the dictionary if one of the ID column names contains the string 'id'.

    Args:
        database_name: (str) The name of the Spark database.

    Returns:
        (dict) A dictionary mapping external table names to ID columns.
    """
    external_tables = [table for table in spark.catalog.listTables(database_name) if table.tableType == "EXTERNAL"]
    metadict = {f"{table.name}": get_id_columns(spark.table(f"{database_name}.{table.name}")) for table in external_tables}
    result = {key: item for key, item in metadict.items() if any(['id' in column.lower() for column in item])}
    return result

# id_cols = h.create_id_columns_dict('real_world_data_sep_2022')
# id_cols['medication_administration']
# ['medicationadministrationid', 'personid', 'encounterid']

# Next, get a list of all the table 
# tables = [item for item in id_cols.keys()]


def use_database_my(spark, schema_name, version_num):
    '''Set the current database or schema to use for subsequent SQL queries
    @par spark       SparkSession
    @par schema_name Name of the schema to use
    @par version_num Version number of the schema to use
    '''
    spark.sql(f"USE {schema_name}_{version_num}")





def set_database(inSchema, inTable, debug = False):
    '''Select the latest version of the data using `use_database` and return the name of that versioned dataset
    Used in the IUHealth data not the RWD because that data set is not using versions.
    @par inSchema The schema where the table resides
    @par inTable  The table 
    '''
    if debug: print(spark.sql(f"SHOW PARTITIONS {inSchema}.{inTable}").toPandas())
    versions = spark.sql(f'select distinct(version) from {inSchema}.{inTable} order by version desc').toPandas()
    version_num = versions.version[0]
    version_num = str(version_num) + '.0'
    version_num = version_num.replace(' ', 'T')
    use_database(spark, inSchema, version_num)
    inTableVersioned = inSchema + "_" + inTable
    return(inTableVersioned)

def set_database(dataSchema, inTable, debug = False):
    '''Select the latest version of the data using `use_database` and return the name of that versioned dataset
    Used in the IUHealth data not the RWD because that data set is not using versions.
    @par inSchema The schema where the table resides
    @par inTable  The table 
    '''
    if dataSchema == 'iuhealth_prime':
       if debug: print(spark.sql(f"SHOW PARTITIONS {dataSchema}.{inTable}").toPandas())
       versions = spark.sql(f'select distinct(version) from {dataSchema}.{inTable} order by version desc').toPandas()
       version_num = versions.version[0]
       version_num = str(version_num) + '.0'
       version_num = version_num.replace(' ', 'T')
       use_database(spark, dataSchema, version_num)
       inTableVersioned = dataSchema + "_" + inTable
    else:
        inTableVersioned = inTable
    return(inTableVersioned)

def drop_table(schema, table):
    spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")    
    
def load_table(TBL, config_dict, message = None):
    
    try:
        result  = spark.table(config_dict[TBL])
    except:
        print(f"Problem loading table {config_dict[TBL]}, {message}")
    return(result)



def recycleTBL(oldTBL, newTBL):
    print(f"The old (from) table is {oldTBL}, the new (to) table is {newTBL}")
    if oldTBL == newTBL:
        print(f"Both {oldTBL} and {newTBL} are the same, not taking any action")
    else:
        try:
            d1 = spark.table(oldTBL)
            writeTable(d1, newTBL)
            spark.table(newTBL).limit(2).toPandas()
        except:
            print(f"table {newTBL} did NOt save, NOT erasing the old table {oldTBL}")
        else:
            print(f"table {newTBL} did is saved, erasing the old table {oldTBL}")
            spark.sql(f"DROP TABLE {oldTBL}")

def rename_tables(projectSchema: str, schema_tag) -> List[str]:
    # Get the list of tables and their creation dates
    tables_dict = getListOfTables(projectSchema)

    # Loop through the tables and rename/drop them
    for old_table, create_date in tables_dict.items():
        # Extract the table name and schema from the old table name
        table_name, schema_name = old_table.split('_')[:2]

        # Construct the new table name by replacing 'invasivefungal' with 'azole'
        new_table = f"{table_name}_{schema_name}_{schema_tag}"

        # Rename the table and drop the old one
        try:
            spark.sql(f"ALTER TABLE {projectSchema}.{old_table} RENAME TO {new_table}")
        except:
            print(f"Could not convert ALTER TABLE {projectSchema}.{old_table} RENAME TO {new_table}")
        else:
            try:
                spark.sql(f"DROP TABLE {projectSchema}.{old_table}")
            except:
                print(f"Could not drop {projectSchema}.{old_table}")
    
    # Return the list of table names after renaming
    return getListOfTables(projectSchema).keys()
           
            
def csv2DF(csvIn, correspondingDF, columns):
    """
    Read a csv and create a spark DataFrame
    """
    
    csvInPd = pd.read_csv(csvIn)[columns]
    schemaIn = correspondingDF.select(columns).schema
    DFIn    = spark.createDataFrame(csvInPd, schemaIn)
    return(DFIn)



def unionAll(*dfs):
    return reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), dfs)

def getmetadata():
    """
    Get metadata such as the location of the delta tables
    """
    
    spark.catalog.setCurrentDatabase('sicklecell_rwd')
    tables = spark.catalog.listTables()
    for table in tables:
        print(table.name)
        print(spark.sql(f"DESCRIBE {table.name}").toPandas())
        print(spark.sql(f"SELECT * FROM {table.name}").limit(5).toPandas())
        print('\n\n')   
    spark.sql(f"""
              describe formatted sicklecell_rwd.adspatient_scd_rwd
              """).filter(F.col('col_name').rlike('(?i)location')).show(truncate=False)
    