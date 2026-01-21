"""
lhn - HealthEIntent Package

Healthcare data extraction and analysis workflows for HealthEIntent systems.
This package provides tools for:
- Clinical data extraction from RWD and OMOP schemas
- Patient cohort identification and matching
- Feature engineering for healthcare analytics
- Ontology-based data classification (Discern integration)

This refactored version depends on spark-config-mapper for configuration
management and generic Spark utilities.

Usage:
    from lhn import Resources, Extract, ExtractItem
    
    # Initialize with configuration files
    r = Resources(
        local_config='000-config.yaml',
        global_config='config-global.yaml',
        schemaTag_config='config-RWD.yaml'
    )
    
    # Access data tables
    encounters = r.r.encounter.df
    
    # Work with extracts
    r.e.cohort.write_index_table(encounters)
"""

__version__ = '0.2.0'
__author__ = 'Harlan Nelson'

# Core classes
from lhn.core import (
    Resources,
    Extract,
    ExtractItem,
    DB,
    SharedMethodsMixin
)

# Re-export key functions from spark_config_mapper for convenience
from spark_config_mapper import (
    # Configuration
    read_config,
    recursive_template,
    merge_configs,
    # Schema
    database_exists,
    getTableList,
    processDataTables,
    Item,
    TableList,
    # Utilities
    writeTable,
    flatten_schema,
    flat_schema,
    coalesce,
    noColColide,
    setFunctionParameters
)

# Logging
from lhn.header import get_logger, spark

__all__ = [
    # Version
    '__version__',
    '__author__',
    # Core classes
    'Resources',
    'Extract',
    'ExtractItem',
    'DB',
    'SharedMethodsMixin',
    # Config (from spark_config_mapper)
    'read_config',
    'recursive_template',
    'merge_configs',
    # Schema (from spark_config_mapper)
    'database_exists',
    'getTableList',
    'processDataTables',
    'Item',
    'TableList',
    # Utilities (from spark_config_mapper)
    'writeTable',
    'flatten_schema',
    'flat_schema',
    'coalesce',
    'noColColide',
    'setFunctionParameters',
    # Spark session
    'spark',
    'get_logger'
]

# Initialize logging
logger = get_logger(__name__)
logger.info("lhn package initialized")
