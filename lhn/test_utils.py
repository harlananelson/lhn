"""
from unittest.mock import MagicMock

# Create a dummy Spark object
spark = MagicMock()
F = MagicMock()
F.col = MagicMock()

from unittest.mock import MagicMock

# Mock PySpark classes and functions
SparkConf = MagicMock()
SparkContext = MagicMock()
SparkSession = MagicMock()
Window = MagicMock()
DataFrame = MagicMock()
concat_ws = MagicMock()
F = MagicMock()
ArrayType = MagicMock()
StructField = MagicMock()
StructType = MagicMock()
IntegerType = MagicMock()
DoubleType = MagicMock()
StringType = MagicMock()
TimestampType = MagicMock()
OneHotEncoder = MagicMock()
StringIndexer = MagicMock()
StorageLevel = MagicMock()
AnalysisException = MagicMock()


try:
    # Initialize SparkSession
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 100 * 1024 * 1024)  # 100MB
    spark.conf.set('spark.sql.broadcastTimeout', 3000)
    spark.conf.set("spark.driver.maxResultSize", "4g")
    spark.conf.set("spark.driver.memory", "4g")
    spark.conf.set("spark.executor.memory", "4g")
    print("Spark Session Started by iuhealth.header.py")
except ImportError:
    print("Spark module is not available.")
    # Display settings
    pd.options.display.max_colwidth   = 1000
    pd.options.display.max_columns    = 150
    pd.options.display.max_rows       = 60
    pd.options.display.min_rows       = 14
    #display(HTML(""))

# Configuration
# Set Environment Variables
#os.environ["PYSPARK_PYTHON"] = "/opt/conda/bin/python3"  # <--- Set PYSPARK_PYTHON
#os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/conda/bin/python3"  # <--- Set PYSPARK_DRIVER_PYTHON


"""
