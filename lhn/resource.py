from pprint import pformat
from lhn.header import pprint
from lhn.header import Path, date, deepcopy, PosixPath, os
from lhn.list_operations import find_single_level_items
from lhn.function_parameters import setFunctionParameters
from lhn.data_transformation import read_config
from lhn.database_operations import set_database
from lhn.introspection_utils import coalesce
from lhn.spark_utils import assignPropertyFromDictionary, database_exists
from lhn.extract import Extract
from lhn.metadata_functions import process_metadata_tables, processDataTables, update_dictionary
from lhn.db import DB
from lhn.header import get_logger
from lhn.metaTable_module import metaSchema

logger = get_logger(__name__)

class Resources:
    def __init__(self, 
                 project, spark, 
                 basePath=Path.home()/f"work/Users/hnelson3",
                 config_file='000-config.yaml',
                 systemuser="hnelson3",
                 call_set_database=False,
                 debug=False,
                 global_yaml="configuration/config-global.yaml",
                 pattern_strings=['standard.id', 'standard.codingSystemId', 'standard.primaryDisplay', 'brandType',
                                  'zip_code', 'deceased', 'tenant', 'birthdate'],
                 only_scan_current_tables=True,
                 personid=['personid'],
                 updateDict=False,
                 partitionBy=None,
                 process_all=False,
                 **kwargs):
        if call_set_database:
            set_database('iuhealth_prime', 'conditions')
        
        self.project = project
        self.spark = spark
        self.systemuser = systemuser
        self.parquetLoc = f"hdfs:///user/{self.systemuser}/{self.project}/"
        self.basePath = Path(basePath)
        self.config_file = config_file
        self.call_set_database = call_set_database
        self.debug = debug
        self.global_yaml = global_yaml
        self.obs = 100000000
        self.schemaTag = 'RWD'
        self.reRun = False
        self.only_scan_current_tables = only_scan_current_tables
        self.personid = personid
        self.kwargs = kwargs
        self.property_names_processed = set()
        self.pattern_strings = pattern_strings
        self.partitionBy = partitionBy
        self.process_all = process_all
        self.updateDict = updateDict
        self.single_level_types = (str, bool, int, float)
        
        self.config_dict = {}
        self.config_table_locations = {
            'config_local': {'location': self.basePath / f"Projects/{self.project}/{self.config_file}"},
            'config_global': {'location': self.basePath / self.global_yaml}
        }

        self.reReadConfig = True
        self.replace = {
            'today': date.today().strftime("%Y-%m-%d"),
            'dataPath': self.basePath
        }
        
        if process_all:
            try:
                self.finish_init()
            except Exception as e:
                logger.error(f"Error in finish_init: {e}")

    def finish_init(self):
        try:
            self.read_config_all()
            self.config_dict.update(self.config_dict['schemas'])
            logger.info("Successfully Executed: self.read_config_all()")
        except Exception as e:
            logger.error(f"Calling self.read_config_all in Class Resources: {e}")
        
        try:
            self.processAllDataTables()
            logger.info("Successfully Executed: self.processAllDataTables()")
        except Exception as e:
            logger.error(f"Calling self.processAllDataTables in Class Resources: {e}")
            
        try:
            pass
        except Exception as e:
            logger.error(f"Calling self.process_Allmetadata_tables in Class Resources: {e}")
        
        self.property_names = self.property_names_processed
    
    def update_or_create_config_dict(self, key, value):
        if key in self.config_dict.keys():
            self.config_dict[key].update(value)
        else:
            self.config_dict[key] = value
    
    def add_path_to_table_location(self):
        for key, location in self.config_table_locations.items():
            location = self.config_table_locations[key]['location']
            if not isinstance(location, PosixPath):
                location = self.basePath / location
                logger.info(f"Changing location to {location}")
                self.config_table_locations[key]['location'] = location
    
    def process_config_table_locations(self, current_locations, current_keys):
        for key in current_locations.keys():
            logger.info(f"key:{key}, location: {self.config_table_locations[key]['location']}")
            location = current_locations[key]['location']
            if os.path.isfile(location):
                if key not in current_keys or self.reReadConfig:
                    logger.info(f"reading file {location} and updating config_dict with key {key}")
                    self.update_or_create_config_dict(key, read_config(location, replace=self.replace, debug=False))
                    self.config_dict.update(self.config_dict[key])
                    if 'config_table_locations' in self.config_dict[key].keys():
                        self.config_table_locations.update(self.config_dict[key]['config_table_locations'])
                        self.add_path_to_table_location()
            else:
                logger.info(f"path {self.config_table_locations[key]['location']} not found")
    
    def read_config_all(self):
        self.add_path_to_table_location()
        current_locations = deepcopy(self.config_table_locations)
        current_keys = deepcopy([item for item in self.config_dict.keys()])
        self.process_config_table_locations(current_locations, current_keys)
        if current_locations != self.config_table_locations:
            self.process_config_table_locations(self.config_table_locations, self.config_dict.keys())
        logger.debug(f"config_dict before single_level_items: {pformat(self.config_dict)}")
        try:
            self.find_single_level_items(single_level_types=self.single_level_types)
            logger.debug(f"single_level_items after first find: {pformat(self.single_level_items)}")
        except Exception as e:
            logger.error(f"Failed to set single_level_items: {e}")
            self.single_level_items = {}
        self.replace.update(self.single_level_items)
        self.replace.update(self.kwargs)
        self.config_dict['config_local'].update(read_config(self.config_table_locations['config_local']['location'], replace=self.replace, debug=self.debug))
        self.config_dict.update(self.config_dict['config_local'])
        self.config_dict['config_global'].update(read_config(self.config_table_locations['config_global']['location'], replace=self.replace, debug=self.debug))
        self.config_dict.update(self.config_dict['config_global'])
        try:
            self.find_single_level_items(single_level_types=self.single_level_types)
            logger.debug(f"single_level_items after second find: {pformat(self.single_level_items)}")
        except Exception as e:
            logger.error(f"Failed to set single_level_items: {e}")
            self.single_level_items = {}
        self.replace.update(self.single_level_items)
        self.replace.update(self.kwargs)
        self.single_level_items.update(self.replace)
        for key in self.single_level_items.keys():
            setattr(self, key, assignPropertyFromDictionary(key, self.single_level_items))
    
    def read_config(self, config_index='config_local', config_file='config_file_local', debug=False):
        self.config_dict[config_index] = read_config(self.config_dict[config_file], self.replace, debug=debug)
        if self.config_dict[config_index] == {}:
            logger.error(f"read_config: Couldn't (or is empty) read {config_index} from {self.config_dict[config_file]}")
        else:
            self.config_dict.update(self.config_dict[config_index])
    
    def find_single_level_items(self, single_level_types=(str, list, bool, int, float)):
        logger.debug(f"Finding single_level_items with types: {single_level_types}")
        self.single_level_items = find_single_level_items(self.config_dict.get('config_global', {}), single_level_types=single_level_types)
        self.single_level_items.update(find_single_level_items(self.config_dict.get('config_local', {}), single_level_types=single_level_types))
    
    def processAllDataTables(self):
        callFunProcessDataTables = self.config_dict['callFunProcessDataTables']
        schemas = self.config_dict['schemas']
        logger.info(f"Starting processAllDataTables with callFunDict: \n {pformat(callFunProcessDataTables)} \n and schemas: \n {pformat(schemas)}")
        for schemakey, schemavalue in schemas.items():
            logger.info(f"Processing schema name {schemakey} at location {schemavalue}")
            try:
                self.processDataTableBySchemakey(schemakey, callFunProcessDataTables)
            except Exception as e:
                logger.error(f"processAllDataTables: Couldn't process {schemakey}, Exception: {e}")
    
    def find_callFunProcessDataTables(self, schemakey, callFunProcessDataTables):
        return [callFunProcessDataTables[item] for item in callFunProcessDataTables if callFunProcessDataTables[item]['schema_type'] == schemakey]
    
    def processDataTableBySchemakey(self, schemakey, callFunProcessDataTables=None):
        if not callFunProcessDataTables:
            callFunProcessDataTables = self.config_dict['callFunProcessDataTables']
        
        schemavalue = self.config_dict['schemas'][schemakey]
        logger.info(f"processDataTableBySchemakey: Processing schema name {schemakey} at location {schemavalue} (from schemas dictionary)")
        callFun = self.find_callFunProcessDataTables(schemakey, callFunProcessDataTables)
        
        if callFun:
            logger.info(f"Found: schema {schemakey}:{schemavalue} in callFunProcessDataTables")
            if type(callFun) == list:
                callFun = callFun[0]
            
            callFun['parquetLoc'] = self.parquetLoc
            
            logger.info(f"Element of callFunProcessDataTables used for callFun: \n {pformat(callFun)}")
            if database_exists(schemavalue):
                logger.info(f"Found schema {schemakey} at {schemavalue}")
                setattr(self, schemakey + '_self_processDataTablesCall', callFun)
                try:
                    self.processDataTables(**callFun)
                    logger.info(f"processed {schemakey} to produce property {callFun['property_name']} and dictionary {callFun['type_key']}")
                    logger.info(f"Can call h.Extract(resource.{callFun['type_key']}) to get the Extract object")
                except Exception as e:
                    logger.error(f"processAllDataTables: Couldn't process {callFun['data_type']}, Exception: {e}")
            else:
                logger.info(f"schema not found {schemavalue} as associated with {schemakey}")
        else:
            logger.info(f"Not Found: schema_type of callFunProcessDataTables in config-global never matches {schemakey} in callFunProcessDataTableBySchemakey")
    
    def locals_update(self):
        result = {k: v for k, v in self.config_dict.items() if not k.startswith('_')}
        return result
    
    def reread_config_files(self, everything=False, schemakey='projectSchema', extractName='e'):
        self.read_config_all()
        self.processDataTableBySchemakey(schemakey=schemakey)
        result = self.load_into_local(everything=everything, schemakey=schemakey, extractName=extractName)
        return result
    
    def load_into_local(self, everything=False, schemakey='projectSchema', extractName='e'):
        logger.info(f"load_into_local: Loading into local everything = {everything}, schemakey = {schemakey}, extractName = {extractName}")
        callFun = self.find_callFunProcessDataTables(schemakey, self.config_dict['callFunProcessDataTables'])[0]
        if not hasattr(self, 'single_level_items'):
            logger.warning("self.single_level_items not found, initializing as empty dict")
            self.single_level_items = {}
        result = self.single_level_items
        logger.info(f"load_into_local: Found callFunProcessDataTables for {schemakey} \n {pformat(callFun)}")
        logger.info(f"result[{extractName}] = Extract(getattr(self, {callFun['type_key']})")
        result[extractName] = Extract(getattr(self, callFun['type_key']))
        for prop in self.property_names_processed:
            result[prop] = getattr(self, prop)
        if everything:
            result.update(self.all_config_dict_items())
        return result
    
    def read_config_global(self):
        self.read_config('config_global', 'config_file_global')
    
    def read_config_RWD(self):
        self.read_config('config_RWD', 'config_file_RWD')
    
    def read_config_IUH(self):
        self.read_config('config_IUH', 'config_file_IUH')
    
    def processIUHDataTable(self):
        self.read_config('config_IUH', 'config_file_IUH')
        self.processDataTables(data_type='IUHdataTables', schema_type='IUHSchema',
                              type_key='iuhealth', property_name='iuh',
                              only_scan_current_table=False)
        return self.iuhealth
    
    def processRWDDataTable(self):
        self.read_config('config_RWD', 'config_file_RWD')
        funCall = {
            'data_type': 'RWDTables',
            'schema_type': 'RWDSchema',
            'type_key': 'rwd',
            'property_name': 'r',
            'only_scan_current_table': False
        }
        Resources_param = setFunctionParameters(self.processDataTables, funCall, config_dict={})
        self.processDataTables(**funCall)
        return self.rwd
    
    def processProjectDataTable(self):
        self.read_config('config_local', 'config_file_local')
        funCall = {
            'data_type': 'projectTables',
            'schema_type': 'projectSchema',
            'type_key': 'proj',
            'property_name': 'db'
        }
        self.processDataTables(**funCall)
        self.single_level_items = find_single_level_items(self.config_dict['config_local'], single_level_types=self.single_level_types)
        self.e = Extract(self.proj)
        return self.single_level_items
    
    def processDataTables(self, data_type, schema_type, type_key, property_name, 
                          config_file_location=None, reReadConfig=None, 
                          updateDict=None, debug=None, tableNameTemplate=None, 
                          parquetLoc=None):
        debug = coalesce(debug, self.debug)
        updateDict = coalesce(updateDict, self.updateDict)
        
        if (data_type not in self.config_dict and config_file_location) or (reReadConfig and config_file_location):
            logger.info(f"Reading config file {config_file_location} for data_type {data_type}")
            self.read_config(data_type, config_file_location)
        
        schema_dict = self.config_dict[data_type] if data_type in self.config_dict else None
        if updateDict:
            logger.info(f"Updating the Dictionary for {schema_type} based on the schema {self.config_dict['schemas'][schema_type]}")
            tableNameTemplate = coalesce(tableNameTemplate, ("_" + self.disease + "_" + self.schemaTag).lower())
            logger.info(f"Using the tableNameTemplate: {tableNameTemplate} to remove from the table names")
            
            funCall = {
                'schema_dict': schema_dict,
                'schema': self.config_dict['schemas'][schema_type],
                'projectSchema': self.config_dict['schemas']['projectSchema'],
                'schemaTag': self.schemaTag,
                'obs': self.obs,
                'reRun': self.reRun,
                'debug': self.debug,
                'personid': self.personid,
                'tableNameTemplate': tableNameTemplate
            }
            logger.info(f"Use to call update_dictionary \n {pformat(funCall)}")
            try:
                self.config_dict[data_type] = update_dictionary(**funCall)
            except KeyError as e:
                logger.error(f"update_dictionary: Couldn't process {data_type}, KeyError: {e}")
        
        if database_exists(self.config_dict['schemas'][schema_type]):
            logger.info(f"Found schema {schema_type} indicated by schemaTag {self.schemaTag} at {self.config_dict['schemas'][schema_type]}")
            self.processDataTablesFunctionCall = {
                'dataTables': self.config_dict[data_type],
                'schema': self.config_dict['schemas'][schema_type],
                'dataLoc': self.dataLoc,
                'disease': self.disease,
                'schemaTag': self.schemaTag,
                'project': self.project,
                'parquetLoc': parquetLoc or self.parquetLoc,
                'debug': self.debug,
            }
            logger.info(f"Use to call processDataTables")
            logger.info(f"funCall: {pformat(self.processDataTablesFunctionCall)}")
            try:
                setattr(self, type_key, processDataTables(**self.processDataTablesFunctionCall))
                self.property_names_processed.add(type_key)
                logger.info(f"processDataTables: processed {type_key} to point to schema {schema_type}")
            except KeyError as e:
                logger.error(f"processDataTables: Couldn't process {type_key} to point to schema {schema_type}")
            
            try:
                setattr(self, property_name, DB(self.__dict__, type_key))
                self.property_names_processed.add(property_name)
                logger.info(f"processDataTables: processed {property_name} to point to schema {schema_type}")
            except Exception as e:
                logger.error(f"processDataTables: Couldn't add DB object {property_name} to point to schema {schema_type}, Exception: {e}")
            logger.info(f"processed property names: {self.property_names_processed}")
        else:
            logger.info(f"schema not found {schema_type} associated with schemaTag {self.schemaTag} at {self.config_dict['schemas'][schema_type]}")
            logger.info(f"the schema list schemas in 000-config.yaml has key {schema_type} that does not exist in callFunProcessDataTables")
    
    def process_Allmetadata_tables(self):
        in_schemas = ['RWDSchema', 'IUHSchema']
        out_schemas = ['dictrwdSchema', 'dictiuhSchema']
        object_names = ['dictrwd', 'dictiuh']
        property_names = ['mr', 'miuh']
        
        for in_schema, out_schema, object_name, property_name in zip(in_schemas, out_schemas, object_names, property_names):
            if database_exists(self.config_dict[in_schema]):
                callFunc = {
                    'in_schema': in_schema,
                    'out_schema': out_schema,
                    'object_name': object_name,
                    'property_name': property_name,
                    'only_scan_current_tables': self.only_scan_current_tables
                }
                setattr(self, object_name + 'self_process_metadata_tablesMethodCall', callFunc)
                self.process_metadata_tables(**callFunc)
            else:
                logger.error(f"process_Allmetadata_tables: Couldn't process {self.config_dict[in_schema]} because it doesn't exist")
    
    def process_metadata_tables(self, in_schema, out_schema, object_name, property_name,
                               only_scan_current_tables=None):
        if only_scan_current_tables is None:
            only_scan_current_tables = self.only_scan_current_tables
            logger.info(f"In method process_metadata_tables: only_scan_current_tables: {only_scan_current_tables}")
            
        try:
            callFunc = {
                'input_dataSchema': self.config_dict[in_schema],
                'output_metaSchema': self.config_dict[out_schema],
                'obs': self.obs,
                'debug': self.debug,
                'reRun': self.reRun,
                'pattern_strings': self.pattern_strings,
                'schemaTag': self.schemaTag,
                'only_scan_current_tables': self.only_scan_current_tables,
                'personid': self.personid,
            }
            setattr(self, object_name + 'process_metadata_tablesFunctionCall', callFunc)
            setattr(self, object_name, process_metadata_tables(**callFunc))
            self.property_names_processed.add(object_name)
            logger.info(f"Created object: {object_name}")
        except Exception as e:
            logger.error(f"self.process_metadata_tables: Couldn't process in_schema: {object_name}, Exception: {e}")
        
        try:
            setattr(self, property_name, DB(self.__dict__, object_name))
            self.property_names_processed.add(property_name)
        except Exception as e:
            logger.error(f"process_metadata_tables: Couldn't process {property_name}: {e} when calling class DB")
    
    def to_dict(self):
        return {attr: getattr(self, attr) for attr in dir(self) if not callable(getattr(self, attr)) and not attr.startswith("__")}
    
    def all_config_dict_items(self):
        return {k: v for k, v in self.config_dict.items() if not k.startswith('_')}
    
    def update_resources(self, config_dict, debug=False):
        self.read_config_all()
        self.rwd = processDataTables(config_dict['RWDTables'], config_dict['RWDSchema'], self.dataLoc, self.disease, self.schemaTag, self.project, self.parquetLoc, debug)
        self.iuhealth = processDataTables(config_dict['IUHdataTables'], config_dict['IUHSchema'], self.dataLoc, self.disease, self.schemaTag, self.project, self.parquetLoc, debug)
        self.proj = processDataTables(config_dict['projectTables'], config_dict['projectSchema'], self.dataLoc, self.disease, self.schemaTag, self.project, self.parquetLoc, debug)
        self.meta = processDataTables(config_dict['metaTables'], config_dict['metaSchema'], self.dataLoc, self.disease, self.schemaTag, self.project, self.parquetLoc, debug)
        self.db = DB(self.__dict__, 'proj')
        self.r = DB(self.__dict__, 'rwd')
        self.iuh = DB(self.__dict__, 'iuhealth')
        self.m = DB(self.__dict__, 'meta')
        user_prop = ['proj', 'rwd', 'iuhealth', 'meta', 'db', 'r', 'iuh', 'm']
        result = {}
        for prop in user_prop:
            result[prop] = getattr(self, prop)
        return result