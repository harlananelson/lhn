from lhn.data_transformation import fieldRequested, flatSchema
from lhn.header import spark, F, ArrayType, StructType, Window
from lhn.header import get_logger
import numpy as np  # Explicitly import numpy

# Get the logger configured in __init__.py
logger = get_logger(__name__)

def convert_date_fields(columns):
    def inner(df):
        for column in columns:
            df = df.withColumn(
                column,
                F.to_date(column)
            )
        return(df)
    return inner

def use_last_value(col_names, windowSpecId):
    """
    Calculate the last occurance of each of col_names in an array.
    """
    def inner(df):
        for col_name in col_names:
            df = df.withColumn(
               col_name, 
               F.last(col_name,ignorenulls=True).over(windowSpecId) 
            )
        return(df)
    return inner


def writeTable(DF, outTable, partitionBy='tenant', description="No description provided", temporary=False,  
               removeDuplicates=False, removeDuplicatesColumns=True):
    """
    Write a table to Spark with an optional description and removing duplicates
    @param DF: The table to be written
    @param outTable: The location to be stored
    @param partitionBy: Column to partition the table by
    @param temporary: If True, creates a temporary view instead of saving as a table
    @param description: Optional description for the saved table
    @param removeDuplicates: If True, removes duplicate rows from the table
    @param removeDuplicatesColumns: If True, removes duplicate columns from the table
    """
    
    # Check if the table exists before refreshing
    if spark.catalog._jcatalog.tableExists(outTable):
        spark.catalog.refreshTable(outTable)
    
    # Drop temporary view if it exists
    if spark.catalog._jcatalog.tableExists(outTable):
        spark.catalog.dropTempView(outTable)
    
    # Handle remove duplicates if specified
    if removeDuplicates:
        DF = DF.dropDuplicates()
    
    if removeDuplicatesColumns:
        columns = DF.columns
        columns_to_keep = distCol(DF.columns)
        DF = DF.select(columns_to_keep)
        
    if temporary:
        DF.createOrReplaceTempView(outTable)
    else:
        if partitionBy in DF.columns:
            logger.info(f"Writing table {description}: Partitioning by {partitionBy}")
            DF.write.partitionBy(partitionBy).mode('overwrite').saveAsTable(outTable)
        else:
            logger.info(f"Writing table {description}: No partitioning specified or column not found in DataFrame.")
            DF.write.mode('overwrite').saveAsTable(outTable)
    
    # Optionally set the table description
    if description:
        spark.sql(f"ALTER TABLE {outTable} SET TBLPROPERTIES ('comment' = '{description}')")
    
    # Return the resulting DataFrame
    result = spark.table(outTable)
    return result

# Example usage
# writeTable(df, 'database.table_name', partitionBy='column_name', description='Table description')

def checkIndex(index, DFTBL):
    """
    Check to see of the index is in the table DF
    @param index: an list of columns to be used on a join
    @param DFTBL: A data table that needs to have the fields in index
    """
    
    if index.issubset(spark.table(DFTBL).columns):
        goodCall = True
    else:
        goodCall = False
        
    return(goodCall)

def distCol(columns, masterList=None):
    if not masterList:
        masterList = columns
    result = []
    
    # filter the masterList by removing items in colideColumns and None values
    [result.append(item) for item in columns if item not in result and item in masterList]
    return(result)


#distCol(labSource.columns)




def getCreatedDate(table, projectSchema):
    createDate = (
        spark.sql(f"DESCRIBE EXTENDED {projectSchema}.{table}")
        .filter(F.col('col_name')=='Created Time').select(F.col('data_type').alias('columnName'))
        .rdd.map(lambda row: row.columnName).collect()[0]
    )
    return(createDate)


def rleid(df, grp_col):
    # Define a window partitioned by grp_col and ordered by row_id
    window = Window.partitionBy(grp_col).orderBy('row_id')

    # Add a column that checks if the current row is equal to the previous row
    df = df.withColumn('is_same', F.col(grp_col) == F.lag(F.col(grp_col)).over(window))

    # Add a column that calculates the cumulative sum of is_same
    df = df.withColumn('rleid', F.sum(F.when(F.col('is_same') == False, 1).otherwise(0)).over(window))

    return df

def assignPropertyFromDictionary(prop, inDict, debug = False):
    if prop in inDict.keys():
        #print(f"{prop} in keys")
        result = inDict[prop]
    else: 
        result = None
    return(result)

def create_empty_df():
    return spark.createDataFrame(spark.sparkContext.emptyRDD(), StructType([]))


def getColumnMapping(config_dict, TBL, regexi, label, debug = False):
    """
    Given a list of regex return a mapping to unlist selected columns
    """
    
    try:
        tblSource = spark.table(config_dict[TBL])
    except Exception as e:
        print(f"Error: {TBL} not found in the database")
        return {}
    
    inclusionRegex = config_dict[regexi]
                            
    tbl = flattenTable(tblSource,inclusionRegex)
    columnsRemap = [F.col(item.replace("_",".")).alias(item) for item in tbl.columns]
    tblRemapped = tblSource.select(columnsRemap)
    result = {f'{label}Columns': tblRemapped.columns, f'{label}ColumnsRemap':columnsRemap}
    if debug:
        print(result)
        print("\n")
    return({f'{label}Columns': tblRemapped.columns, f'{label}ColumnsRemap':columnsRemap})

def database_exists(database):
    """
    Function that checks the existence of a Hive database
    :param database: database name
    :return: bool, True if database exists
    """
    try:
        return bool(spark.sql(f"SHOW DATABASES LIKE '{database}'").collect())
    except Exception as e:
        print(f"An error occurred while checking the existence of the database: {e}")
        return False
    
    
def getTableList(dataSchema):
    if dataSchema != "iuhealth_prime":
        tableList = spark.sql(f"show tables in {dataSchema}").filter(F.lower(F.col('database')) == dataSchema.lower()).toPandas()
    else:
        tableList = spark.sql("show tables").filter(F.col('tablename').startswith(dataSchema.lower())).toPandas()
    return(tableList)

def getTableList(dataSchema):
    tableList = spark.sql(f"show tables in {dataSchema}").filter(F.lower(F.col('database')) == dataSchema.lower()).toPandas()
   
    return(tableList)
        




def check_table_existence(*args):
    if len(args) == 1:
        schema, table_name = args[0].split('.')
    elif len(args) == 2:
        schema, table_name = args
    else:
        raise ValueError("Invalid number of arguments. Provide either 'schema.table_name' or 'schema', 'table_name'")

    result = spark.sql(f"show tables in {schema} LIKE '{table_name}'").head()
    if result is None or len(result) == 0:
        return False
    else:
        return True
 
def getListOfTables(projectSchema):
    tables = spark.sql(f"show tables in {projectSchema}").toPandas().loc[:,'tableName'].tolist()
    tableDic = {table: getCreatedDate(table, projectSchema) for table in tables}
    sortedTableDic = dict(sorted(tableDic.items(), key=lambda x: x[1], reverse=True))
    return(sortedTableDic)

#projectSchema = 'sickle_cell_db'
#tableDic = getListOfTables(projectSchema)
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType

from lhn.data_transformation import fieldRequested, flatSchema
from lhn.header import spark, F, ArrayType, StructType, Window
from lhn.header import get_logger
import numpy as np

logger = get_logger(__name__)

def flattenTable(df, inclusionRegex=[], maxTrys=5):
    """
    Flattens a Spark DataFrame by unnesting arrays and structs, optimized for FHIR data.

    Parameters:
    - df: PySpark DataFrame to flatten (e.g., FHIR resource table).
    - inclusionRegex: List of regex patterns to include specific columns for flattening.
    - maxTrys: Maximum number of flattening iterations (reduced for performance).

    Returns:
    - Flattened DataFrame or original DataFrame if no flattening is needed.
    """
    # Cache the input DataFrame to avoid recomputation
    df = df.cache()
    logger.info(f"Input DataFrame schema (cached):\n{df.schema}")

    # Check for nested types using schema introspection
    has_nested = any(
        isinstance(field.dataType, (ArrayType, StructType))
        for field in df.schema
        if fieldRequested(df, field.name, inclusionRegex)
    )
    if not has_nested:
        logger.info("No nested arrays or structs matching inclusionRegex, returning original DataFrame")
        df.unpersist()
        return df

    # Initialize fields for arrays and structs
    fieldsArray = [
        field.name for field in df.schema
        if isinstance(field.dataType, ArrayType) and fieldRequested(df, field.name, inclusionRegex)
    ]
    fieldsStruct = [
        field.name for field in df.schema
        if isinstance(field.dataType, StructType) and fieldRequested(df, field.name, inclusionRegex)
    ]
    logger.info(f"Fields to flatten: arrays={fieldsArray}, structs={fieldsStruct}")

    # Single-pass array explosion
    for field in fieldsArray:
        try:
            logger.debug(f"Exploding array column {field}")
            df = df.withColumn(field, F.explode_outer(field))
        except Exception as e:
            logger.error(f"Error exploding array column {field}: {str(e)}")
            df.unpersist()
            return df

    # Single-pass struct flattening
    if fieldsStruct:
        try:
            logger.debug("Flattening struct columns")
            flat_columns = flatSchema(df)
            fields = [
                F.col(field).alias(field.replace(".", "_"))
                for field in flat_columns
                if fieldRequested(df, field, inclusionRegex)
            ]
            if not fields:
                logger.warning("No columns match inclusionRegex after flattening")
                df.unpersist()
                return df
            df = df.select(fields)
        except Exception as e:
            logger.error(f"Error flattening struct columns: {str(e)}")
            df.unpersist()
            return df

    # Optimize partitioning after flattening
    df = df.repartition(200)  # Adjust based on cluster size
    logger.info(f"Flattened DataFrame schema:\n{df.schema}")
    df.unpersist()
    return df
# Example usage
# Assuming `df` is a PySpark DataFrame and necessary functions like `fieldRequested` and `flatSchema` are defined
# flattened_df = flattenTable(df)

def explode_columns(df, columns):
    for column in columns:
        df = df.explode(column)
    return df

def list_aggfunc(x):
    return x.tolist()

def KV2Col2(df, groupFields, fieldKey, fieldValue):
    
    levels = df[fieldKey].unique()
    grouped_df = (
        df
        .pivot_table(index=groupFields, columns=fieldKey, values=fieldValue, aggfunc=list_aggfunc)
        .reset_index()
        .sort_values(groupFields)
    )
    grouped_df.columns.name = None
    exploded_df = explode_columns(grouped_df,levels)
    return(exploded_df)


def create_tenant_partition(df):
    
    # Count the number of records for each tenant
    df = df.withColumn("count", F.count("*").over(Window.partitionBy("tenant")))

    # Calculate the maximum count
    max_count = df.select(F.max("count")).first()[0]

    # Collect the tenants and counts to the driver
    tenants_counts = df.select("tenant", "count").distinct().orderBy(F.desc("count")).collect()

    # Create a list to hold the tenant to partition mappings
    tenant_partition = []
    current_partition = 0
    current_count = 0

    for row in tenants_counts:
        tenant = row["tenant"]
        count = row["count"]
        
        if current_count + count > max_count:
            # Start a new partition
            current_partition += 1
            current_count = count
        else:
            # Add to the current partition
            current_count += count
        
        tenant_partition.append((tenant, current_partition))

    # Create a DataFrame from the tenant to partition mappings
    tenant_partition_df = spark.createDataFrame(tenant_partition, ["tenant", "tenant_partition"])
    
    return tenant_partition_df

def apply_tenant_partition(df, tenant_partition_df):

    # Broadcast the tenant_partition DataFrame
    tenant_partition_df = F.broadcast(tenant_partition_df)

    # Join the original DataFrame with the tenant_partition DataFrame
    df = df.join(tenant_partition_df, on="tenant", how="left")

    # Repartition the DataFrame based on the tenant_partition column
    df = df.repartition("tenant_partition")

    return df

from pyspark.sql import DataFrame
from lhn.header import get_logger
import os
import subprocess
import time

logger = get_logger(__name__)

def to_parquet(df: DataFrame, path: str, partitionBy: str = None, mode: str = "overwrite", fallback_dir: str = "/tmp", retries: int = 3, retry_delay: float = 2.0) -> None:
    """
    Write a Spark DataFrame to a Parquet file at the specified path (HDFS or local filesystem).

    Args:
        df (DataFrame): The PySpark DataFrame to write.
        path (str): The file path (e.g., 'hdfs:///user/hnelson3/...').
        partitionBy (str, optional): The column to partition the data by.
        mode (str): The write mode ('overwrite', 'append', etc.). Defaults to 'overwrite'.
        fallback_dir (str): Fallback HDFS directory if the specified path is not writable. Defaults to '/tmp'.
        retries (int): Number of retries for HDFS commands. Defaults to 3.
        retry_delay (float): Delay between retries in seconds. Defaults to 2.0.

    Raises:
        ValueError: If the DataFrame is None or the path is invalid.
        RuntimeError: If the write operation fails after trying the fallback directory.
    """
    if df is None or df.rdd.isEmpty():
        logger.warning("DataFrame is empty or None. Skipping Parquet write operation.")
        return

    if not path:
        logger.error("No valid Parquet file path provided.")
        raise ValueError("No valid Parquet file path provided.")

    # Initialize the target path
    target_path = path
    use_fallback = False

    # Handle path based on scheme
    if not target_path.startswith(('file://', 'hdfs://', 's3://')):
        # Assume local filesystem for paths without a scheme
        target_path = f"file://{os.path.abspath(target_path)}"
        local_path = target_path.replace('file://', '')
        directory = os.path.dirname(local_path)
        
        # Check local directory existence and writability
        if not os.path.exists(directory):
            logger.info(f"Creating local directory {directory}")
            try:
                os.makedirs(directory, exist_ok=True)
            except Exception as e:
                logger.warning(f"Failed to create local directory {directory}: {str(e)}. Using fallback directory {fallback_dir}.")
                use_fallback = True
        elif not os.access(directory, os.W_OK):
            logger.warning(f"Local directory {directory} is not writable. Using fallback directory {fallback_dir}.")
            use_fallback = True
    elif target_path.startswith('hdfs://'):
        # Extract HDFS directory path
        if not target_path.startswith('hdfs:///'):
            logger.warning(f"HDFS path {target_path} should start with 'hdfs:///'. Attempting to correct.")
            target_path = f"hdfs:///{target_path.lstrip('hdfs://')}"
        
        hdfs_dir = os.path.dirname(target_path.replace('hdfs:///', '/'))
        if not hdfs_dir.startswith('/'):
            hdfs_dir = f"/{hdfs_dir}"
        
        for attempt in range(retries):
            try:
                # Check if directory exists
                result = subprocess.run(['hdfs', 'dfs', '-test', '-d', hdfs_dir], capture_output=True, text=True)
                if result.returncode != 0:
                    logger.info(f"Creating HDFS directory {hdfs_dir}")
                    subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', hdfs_dir], check=True, capture_output=True, text=True)
                    # Set permissions to 755 (rwxr-xr-x)
                    subprocess.run(['hdfs', 'dfs', '-chmod', '755', hdfs_dir], check=True, capture_output=True, text=True)
                
                # Test writability by creating and removing a temporary file
                test_file = f"{hdfs_dir}/_test_write_{os.getpid()}"
                subprocess.run(['hdfs', 'dfs', '-touchz', test_file], check=True, capture_output=True, text=True)
                subprocess.run(['hdfs', 'dfs', '-rm', test_file], check=True, capture_output=True, text=True)
                break  # Success
            except subprocess.CalledProcessError as e:
                logger.warning(f"Attempt {attempt + 1}/{retries} failed for HDFS directory {hdfs_dir}: {str(e)} (stderr: {e.stderr})")
                if attempt < retries - 1:
                    time.sleep(retry_delay)
                else:
                    logger.warning(f"HDFS directory {hdfs_dir} is not writable or accessible after {retries} attempts. Using fallback HDFS directory {fallback_dir}.")
                    use_fallback = True
            except Exception as e:
                logger.warning(f"Unexpected error accessing HDFS directory {hdfs_dir}: {str(e)}. Using fallback HDFS directory {fallback_dir}.")
                use_fallback = True
                break

    # Use fallback directory if needed
    if use_fallback:
        # Use HDFS fallback for HDFS paths, local for local paths
        if target_path.startswith('hdfs://'):
            # Ensure fallback_dir is an absolute HDFS path
            fallback_path = f"/{fallback_dir.lstrip('/')}/{os.path.basename(path)}"
            target_path = f"hdfs:///{fallback_path.lstrip('/')}"
        else:
            temp_dir = os.path.join(fallback_dir, f"spark_parquet_{os.getpid()}")
            os.makedirs(temp_dir, exist_ok=True)
            target_path = f"file://{os.path.join(temp_dir, os.path.basename(path))}"
        logger.info(f"Fallback path: {target_path}")

    logger.info(f"Writing to Parquet file at {target_path}")

    try:
        writer = df.write.mode(mode)
        if partitionBy and partitionBy in df.columns:
            logger.info(f"Partitioning by {partitionBy}")
            writer = writer.partitionBy(partitionBy)
        writer.parquet(target_path)
        logger.info(f"Successfully wrote Parquet file to {target_path}")
    except Exception as e:
        logger.error(f"Failed to write Parquet file to {target_path}: {str(e)}")
        raise RuntimeError(f"Failed to write Parquet file: {str(e)}")