"""
lhn/header.py

Central header module for the lhn (HealthEIntent) package.
Extends spark_config_mapper.header with healthcare-specific imports and constants.

This module provides all common imports needed across the lhn package, ensuring
consistent Spark configuration and logging for healthcare data processing.
"""

# Import everything from spark_config_mapper.header
from spark_config_mapper.header import *
import logging

# Additional imports for healthcare workflows
from IPython.display import Markdown, display, HTML
from concurrent.futures import ThreadPoolExecutor
from scipy.stats import gamma
from matplotlib import pyplot as plt
from multiprocessing import Pool
from functools import partial
from pprint import pformat

# Additional PySpark imports for healthcare processing
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.functions import concat_ws, udf
from pyspark.storagelevel import StorageLevel

# Healthcare-specific constants
DISCERN_ROOT = 's3://iuh-datalab-persistence-s3-data/discernontology/v1/'
num_processes = 4  # Number of processes for parallelism

# Re-export get_logger with package-specific configuration
_base_get_logger = get_logger


def get_logger(name: str, level=logging.INFO) -> logging.Logger:
    """
    Get a configured logger for lhn modules.
    
    Parameters:
        name (str): Logger name (typically __name__)
        level: Logging level (default INFO)
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logger = _base_get_logger(name, level)
    return logger


# Optional plotting library
try:
    import plotnine as pn
except ImportError:
    pn = None
    root_logger = logging.getLogger()
    root_logger.debug("plotnine not available - some plotting features disabled")
