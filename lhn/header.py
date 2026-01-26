import logging
from typing import Any, List, Tuple, Type, Dict, Optional

# Standard library imports
import os
import sys
import re
from pathlib import Path, PosixPath
from string import Template
from datetime import date, datetime
import time
import argparse
import subprocess
import yaml
import threading
from copy import copy, deepcopy
import json
from importlib import reload
import ast
import pprint  # Re-adding pprint here

# Third-party library imports
import pkg_resources
import pandas as pd
import numpy as np
from IPython.display import Markdown, display, HTML
from collections import OrderedDict
import inspect
from functools import reduce, partial
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from scipy.stats import gamma
from matplotlib import pyplot as plt

# Pyspark imports
from pyspark import SparkConf, SparkContext, Broadcast
from pyspark.sql.window import Window
from pyspark.sql import SparkSession, DataFrame, functions as F, types
from pyspark.sql.types import StructField, FloatType, StringType, TimestampType, ArrayType, StructType, DateType, IntegerType
from pyspark.sql.utils import AnalysisException
from pyspark.ml.feature import OneHotEncoder, StringIndexer
from pyspark.sql.functions import concat_ws, udf
from pyspark.storagelevel import StorageLevel

# Constants for join types
JOIN_INNER = 'inner'

# Static configurations
num_processes = 4  # Number of processes for parallelism
DISCERN_ROOT = 's3://iuh-datalab-persistence-s3-data/discernontology/v1/'

# Set Environment Variables
os.environ["PYSPARK_PYTHON"] = "/opt/conda/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/conda/bin/python3"

# Configure the root logger
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
if not root_logger.hasHandlers():
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)

def get_logger(name: str, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 200 * 1024 * 1024)  # 200MB
spark.conf.set('spark.sql.broadcastTimeout', 3600)
spark.conf.set("spark.driver.maxResultSize", "8g")
spark.conf.set("spark.driver.memory", "8g")
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "5")
root_logger.info("Spark Session Started by iuhealth.header.py")

# Optional plotting library
try:
    import plotnine as pn
except ImportError:
    root_logger.warning("plotnine currently not installed")