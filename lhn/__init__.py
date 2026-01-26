# lhn/__init__.py

# Optional: Package documentation or description
"""HealthEIntent Package

1. **Main Branch**: This is the primary branch where the source code always reflects a production-ready state.

2. **Development (Dev) Branch**: This branch is derived from the main branch. It's where the source code always reflects a state with the latest delivered development changes for the next release. This is also known as the "integration branch".

3. **Feature Branches**: These are used to develop new features for the upcoming or a distant future release. Each feature branch should be dedicated to one specific feature. Once the feature is complete, it's merged back into the dev branch.

4. **Release Branches**: These support preparation of a new production release. They allow for minor bug fixes and preparing meta-data for a release (version number, build dates, etc.). By doing all of this on a release branch, the dev branch is cleared to receive features for the next big release.

5. **Hotfix Branches**: These are very much like release branches in that they are also meant to prepare for a new production release, albeit unplanned. They arise from the necessity to act immediately upon an undesired state of a live production version. When a critical bug in a production version must be resolved immediately, a hotfix branch may be branched off from the corresponding tag on the main branch that marks the production version.

The workflow between these branches typically involves:

- Regularly merging changes from `dev` into `feature` branches to keep them up-to-date.
- Merging `feature` branches back into `dev` once the feature is complete.
- Creating `release` branches off `dev` once it's feature-complete for a release.
- Merging `release` branches into `main` and `dev` when the release is done.
- Creating `hotfix` branches off `main` for immediate fixes, and merging them back into both `main` and `dev`.



This package contains modules and utilities for HealthEIntent data processing,
configuration management, and various other functionalities.
"""

#from healtheintent.header import *
# Optional: Import key classes or functions from modules for easier access
#from .discern_control import DiscernControl
#from .config import some_configuration_function
# from .utils import some_utility_function

# Optional: Define any package-level constants or variables
PACKAGE_CONSTANT = 'constant value'


from lhn.header import get_logger
logger = get_logger(__name__)
# Get the logger configured in __init__.py
# Make sure the level is set to INFO not DEBUG
# This will allow you to see the log messages
# logger = get_logger(__name__)
# logger.info("This is a log message from __init__.py")

 
# Optional: Include any necessary package initialization code
def _initialize_package():
    pass

_initialize_package()

# Import all modules and classes from the package
from lhn.cohort import group_ethnicities, group_races, group_races2, group_gender, group_marital_status, assign_age_group, calcUsage, write_index_table, identifyLevel, select_elements_for_encounter_id, entity2Elements, select_by_entity, elementList2TBL, applyTimeBoundary, findConditionsByEncounter, identify_target_records, prepare_dataframe
from lhn.cohort_matching import call_match_controls, match_controls_to_cases
from lhn.database_operations import search_object, get_id_columns, create_id_columns_dict, use_database_my, set_database, drop_table, load_table, recycleTBL, rename_tables, csv2DF, unionAll
from lhn.data_conversion import list2YAML
from lhn.data_display import print_parameters, print_pd, showIU, noRowNum, show_first
from lhn.data_summary import count_people, attrition, groupCount, countDistinct, topAttributes, aggregate_fields, aggregate_fields_count, aggregate_columns, getTopValues, topRecords, getSelectedLevels, count_and_pivot
from lhn.data_transformation import fields_reconcile, read_config, flatten_schema, flat_schema, flatSchema, flatten_df, regInList, fieldRequested, filter_and_group, dict_of_list_to_list, readParquet, showfields, list_fields, recursive_template, expand_arrays_in_df, write_sorted_index_table, get_selected_fields
from lhn.db import DB
from lhn.discern import getCodingSystemId, getCodesAndSystem, summarizeCodes, search_ontologies, contextId_ont, context_ont, conceptCode_ont, system_ont, system_name_ont, concept_ont, context_name_ont, context_name_system_ont, codingSystem_ont, conceptName_list, contextId_list, context_list, coding_list, codes_list, show_desc, createMetaOnt, demoTable, extractConcepts, calContextGroups, updateTableDt, updateTable, findCrosswalk, check_sample_and_ontology, select_top_contextId, get_ontology_codes, add_concept_indicators
from lhn.excel_operations import clean_sheet_name, write_to_excel_thread, create_excel_spreadsheet_with_threads, create_excel_spreadsheet
from lhn.extract import Extract, ExtractItem  
from lhn.file_operations import put_to_hdfs, list_files, spark_table
from lhn.function_parameters import missingParameters, get_parameters, set_function_parameters, getParameters, setFunctionParameters, get_default_args
from lhn.introspection_utils import return_codefields, show_attributes, getDFResources, prop2local, get_standard_id_elements, get_root_columns_elements, get_root_columns, extractSourceTables, extractTableLocations, translate_index, reinstantiate, deduplicate_fields, coalesce
from lhn.item import TableList, Item 
from lhn.listTable import ListTable
from lhn.list_operations import get_element_index, escape_and_bound_dot, escape_and_bound_dot_udf, preprocess_string, is_single_level, find_single_level_items, noColColide, extractTableName
from lhn.metadata_functions import processDataTables, current_tables_processed, process_metadata_tables, update_dictionary
from lhn.metaTable_module import metaSchema, metaTable 
from lhn.notebook_interaction import get_notebook_path, setup_environment
from lhn.pandas_utils import uncapitalize, pop_item, pop_unlist, dict2Pandas
from lhn.plot import count, plot_counts, plotByTime, plotByTimely, plotTopEntities
from lhn.query import extract_fields_flat, extract_fields_flat_top, query_flat_rwd, query_table
from lhn.resource import Resources
from lhn.spark_query import pivot_wider, joinByIndex, findElementsNotUsed, add_parsed_column, stackedSpark, clean_values
from lhn.spark_utils import convert_date_fields, use_last_value, writeTable, checkIndex, distCol, getCreatedDate, assignPropertyFromDictionary, create_empty_df, getColumnMapping, database_exists, getTableList, check_table_existence, getListOfTables, flattenTable, explode_columns, list_aggfunc, KV2Col2, apply_tenant_partition, create_tenant_partition
from lhn.statisticalSummary import calculate_chi_squared, gamma_percentile, calculate_percentile, five_number_summary
from lhn.shared_methods import SharedMethodsMixin
from lhn.features import select_only_baseline, analyze_clinical_measurements

__all__ = [
    'group_ethnicities', 'group_races', 'group_races2', 'group_gender', 'group_marital_status', 'assign_age_group', 'calcUsage', 'write_index_table', 'identifyLevel', 'select_elements_for_encounter_id', 'entity2Elements', 'select_by_entity', 'elementList2TBL', 'applyTimeBoundary', 'findConditionsByEncounter', 'identify_target_records', 'prepare_dataframe',
    'call_match_controls', 'match_controls_to_cases',
    'search_object', 'get_id_columns', 'create_id_columns_dict', 'use_database_my', 'set_database', 'drop_table', 'load_table', 'recycleTBL', 'rename_tables', 'csv2DF', 'unionAll',
    'list2YAML',
    'print_parameters', 'print_pd', 'showIU', 'noRowNum', 'show_first',
    'count_people', 'attrition', 'groupCount', 'countDistinct', 'topAttributes', 'aggregate_fields', 'aggregate_fields_count', 'aggregate_columns', 'getTopValues', 'topRecords', 'getSelectedLevels', 'count_and_pivot',
    'fields_reconcile', 'read_config', 'flatten_schema', 'flat_schema', 'flatSchema', 'flatten_df', 'regInList', 'fieldRequested', 'filter_and_group', 'dict_of_list_to_list', 'readParquet', 'showfields', 'list_fields', 'recursive_template', 'expand_arrays_in_df', 'write_sorted_index_table', 'get_selected_fields',
    'DB',
    'getCodingSystemId', 'getCodesAndSystem', 'summarizeCodes', 'search_ontologies', 'contextId_ont', 'context_ont', 'conceptCode_ont', 'system_ont', 'system_name_ont', 'concept_ont', 'context_name_ont', 'context_name_system_ont', 'codingSystem_ont', 'conceptName_list', 'contextId_list', 'context_list', 'coding_list', 'codes_list', 'show_desc', 'createMetaOnt', 'demoTable', 'extractConcepts', 'calContextGroups', 'updateTableDt', 'updateTable', 'findCrosswalk', 'check_sample_and_ontology', 'select_top_contextId', 'get_ontology_codes', 'add_concept_indicators',
    'clean_sheet_name', 'write_to_excel_thread', 'create_excel_spreadsheet_with_threads', 'create_excel_spreadsheet',
    'escape_and_bound_regex', 'escape_and_bound_regex_pandas', 'Extract', 'ExtractItem',
    'put_to_hdfs', 'list_files', 'spark_table',
    'missingParameters', 'get_parameters', 'set_function_parameters', 'getParameters', 'setFunctionParameters', 'get_default_args',
    'return_codefields', 'show_attributes', 'getDFResources', 'prop2local', 'get_standard_id_elements', 'get_root_columns_elements', 'get_root_columns', 'extractSourceTables', 'extractTableLocations', 'translate_index', 'reinstantiate', 'deduplicate_fields', 'coalesce',
    'TableList', 'Item',
    'ListTable',
    'get_element_index', 'escape_and_bound_regex', 'preprocess_string', 'is_single_level', 'find_single_level_items', 'noColColide', 'extractTableName',
    'processDataTables', 'current_tables_processed', 'process_metadata_tables', 'update_dictionary',
    'metaSchema', 'metaTable',
    'get_notebook_path', 'setup_environment',
    'uncapitalize', 'pop_item', 'pop_unlist', 'dict2Pandas',
    'count', 'plot_counts', 'plotByTime', 'plotByTimely', 'plotTopEntities',
    'extract_fields_flat', 'extract_fields_flat_top', 'query_flat_rwd', 'query_table',
    'Resources',
    'pivot_wider', 'joinByIndex', 'findElementsNotUsed', 'add_parsed_column', 'stackedSpark', 'clean_values',
    'convert_date_fields', 'use_last_value', 'writeTable', 'checkIndex', 'distCol', 'getCreatedDate', 'assignPropertyFromDictionary', 'create_empty_df', 'getColumnMapping', 'database_exists', 'getTableList', 'check_table_existence', 'getListOfTables', 'flattenTable', 'explode_columns', 'list_aggfunc', 'KV2Col2', 'apply_tenant_partition', 'create_tenant_partition',
    'calculate_chi_squared', 'gamma_percentile', 'calculate_percentile', 'five_number_summary',
    'SharedMethodsMixin',
    'select_only_baseline', 'analyze_clinical_measurements'
]