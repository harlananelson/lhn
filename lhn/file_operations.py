from lhn.header import subprocess, os
from lhn.header import get_logger

logger = get_logger(__name__)

##############################################################################
def put_to_hdfs(local_path, hdfs_path):
    put = subprocess.Popen(["hadoop", "fs", "-put", local_path, hdfs_path], stdout=subprocess.PIPE)
    put.communicate()
    
def list_files(directory):
    return [file for file in os.listdir(directory) if os.path.isfile(os.path.join(directory, file))]

def spark_table(spark, tableName, message = ''):
    # Attempt to load a spark table 
    
    try:
        result = spark.table(tableName)
    except:
        print(f"A problem loading {tableName} in spark")
        
