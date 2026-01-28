"""
lhn/core/resource.py

Resources class - Central orchestration for healthcare data extraction projects.
Manages configuration, schema mapping, and data table access.
"""

from lhn.header import (
    spark, Path, get_logger, pprint
)
from spark_config_mapper import (
    read_config,
    processDataTables,
    database_exists,
    getTableList
)

logger = get_logger(__name__)


def _detect_base_path(current_dir=None):
    """
    Detect the base path from current directory.

    Looks for the pattern: .../work/Users/{username}/Projects/{project}
    and returns: .../work/Users/{username}

    Falls back to parent of 'Projects' directory if found, or current dir.

    Parameters:
        current_dir (str|Path): Starting directory. Defaults to cwd.

    Returns:
        Path: Detected base path
    """
    if current_dir is None:
        current_dir = Path.cwd()
    else:
        current_dir = Path(current_dir)

    # Walk up the directory tree looking for 'Projects' directory
    path = current_dir.resolve()
    while path != path.parent:
        if path.name == 'Projects':
            # Found Projects dir, return its parent as basePath
            return path.parent
        path = path.parent

    # Fallback: check if 'configuration' exists at various levels
    path = current_dir.resolve()
    while path != path.parent:
        config_dir = path / 'configuration'
        if config_dir.exists() and config_dir.is_dir():
            return path
        path = path.parent

    # Final fallback: return current directory's parent
    return current_dir.resolve()


def _resolve_config_path(config_path, base_path=None, current_dir=None):
    """
    Resolve a configuration file path.

    Checks in order:
    1. Absolute path (if given)
    2. Relative to current directory
    3. Relative to base_path/configuration/
    4. Relative to base_path/

    Parameters:
        config_path (str|Path): The config file path to resolve
        base_path (Path): Base path for searching
        current_dir (Path): Current working directory

    Returns:
        Path|None: Resolved path if found, None otherwise
    """
    if not config_path:
        return None

    config_path = Path(config_path)

    # If already absolute and exists, use it
    if config_path.is_absolute():
        if config_path.exists():
            return config_path
        return None

    if current_dir is None:
        current_dir = Path.cwd()

    # Check relative to current directory
    candidate = current_dir / config_path
    if candidate.exists():
        return candidate.resolve()

    # Check relative to base_path/configuration/
    if base_path:
        candidate = base_path / 'configuration' / config_path
        if candidate.exists():
            return candidate.resolve()

        # Check relative to base_path directly
        candidate = base_path / config_path
        if candidate.exists():
            return candidate.resolve()

    return None


class Resources:
    """
    Central orchestration class for healthcare data extraction projects.
    
    Resources manages the three-tier configuration hierarchy:
    1. Global config (config-global.yaml) - Organization-wide settings
    2. Schema config (config-RWD.yaml) - Data source specific settings  
    3. Project config (000-config.yaml) - Project specific settings
    
    After initialization, data tables are accessible as attributes:
        r = Resources(...)
        r.encounter.df  # Spark DataFrame
        r.person.df     # Another DataFrame
    
    Attributes:
        config (dict): Merged configuration dictionary
        r (TableList): Data source tables (e.g., RWD tables)
        e (Extract): Project-specific extract tables
        spark: SparkSession reference
    
    Example:
        >>> r = Resources(
        ...     local_config='000-config.yaml',
        ...     global_config='/shared/config-global.yaml',
        ...     schemaTag_config='/shared/config-RWD.yaml',
        ...     replace={'today': '2025-01-19'}
        ... )
        >>> # Access RWD encounter table
        >>> r.r.encounter.df.count()
        >>> # Access project extracts
        >>> r.e.cohort.df.show()
    """
    
    def __init__(self, local_config, global_config, schemaTag_config,
                 replace=None, debug=False, finish_init=True, base_path=None):
        """
        Initialize Resources with configuration files.

        Parameters:
            local_config (str): Path to project config (000-config.yaml)
            global_config (str): Path to global config (config-global.yaml)
            schemaTag_config (str): Path to schema config (config-RWD.yaml)
            replace (dict): Template substitutions (e.g., {'today': '2025-01-19'})
            debug (bool): Enable debug logging
            finish_init (bool): If True, complete initialization by loading tables
            base_path (str|Path): Base path for resolving config files.
                                  Auto-detected from cwd if not provided.
        """
        self.debug = debug
        self.spark = spark
        self.replace = replace or {}
        self.current_dir = Path.cwd()

        # Detect or use provided base path
        if base_path:
            self.base_path = Path(base_path)
        else:
            self.base_path = _detect_base_path(self.current_dir)

        if self.debug:
            logger.info(f"Current directory: {self.current_dir}")
            logger.info(f"Detected base path: {self.base_path}")

        # Resolve and store config paths
        self._original_paths = {
            'local': local_config,
            'global': global_config,
            'schema': schemaTag_config
        }

        self.local_config_path = _resolve_config_path(
            local_config, self.base_path, self.current_dir
        )
        self.global_config_path = _resolve_config_path(
            global_config, self.base_path, self.current_dir
        )
        self.schemaTag_config_path = _resolve_config_path(
            schemaTag_config, self.base_path, self.current_dir
        )

        # Log path resolution results
        self._log_path_resolution()

        # Load configurations in order (later configs override earlier)
        self.config = self._load_configs()

        # Extract key configuration values
        self._extract_config_values()

        # Initialize table containers
        self.r = None    # Source data tables (RWD)
        self.e = None    # Extract tables (project outputs)
        self.rwd = None  # RWD config objects (for creating custom table sets)
        self.proj = None # Project config objects (for creating custom Extracts)

        if finish_init:
            self.finish_init()

    def _log_path_resolution(self):
        """Log the results of path resolution."""
        missing = []

        for name, original in self._original_paths.items():
            if not original:
                continue

            resolved = getattr(self, f"{name}_config_path" if name != 'schema'
                              else "schemaTag_config_path")

            if resolved:
                if self.debug:
                    if str(resolved) != original:
                        logger.info(f"Resolved {name} config: {original} -> {resolved}")
                    else:
                        logger.info(f"Found {name} config: {resolved}")
            else:
                missing.append((name, original))

        # Warn about missing configs
        for name, original in missing:
            logger.warning(
                f"Config file not found: {original}\n"
                f"  Searched in:\n"
                f"    - {self.current_dir / original}\n"
                f"    - {self.base_path / 'configuration' / original}\n"
                f"    - {self.base_path / original}"
            )
    
    def _load_configs(self):
        """Load and merge configuration files."""
        config = {}
        configs_loaded = []

        # Load global config first (lowest priority)
        if self.global_config_path and self.global_config_path.exists():
            global_cfg = read_config(str(self.global_config_path), self.replace, self.debug)
            config.update(global_cfg)
            configs_loaded.append(('global', self.global_config_path))
            if self.debug:
                logger.info(f"Loaded global config from {self.global_config_path}")

        # Load schema-specific config (medium priority)
        if self.schemaTag_config_path and self.schemaTag_config_path.exists():
            schema_cfg = read_config(str(self.schemaTag_config_path), self.replace, self.debug)
            config.update(schema_cfg)
            configs_loaded.append(('schema', self.schemaTag_config_path))
            if self.debug:
                logger.info(f"Loaded schema config from {self.schemaTag_config_path}")

        # Load local project config (highest priority)
        if self.local_config_path and self.local_config_path.exists():
            local_cfg = read_config(str(self.local_config_path), self.replace, self.debug)
            config.update(local_cfg)
            configs_loaded.append(('local', self.local_config_path))
            if self.debug:
                logger.info(f"Loaded local config from {self.local_config_path}")

        # Summary log
        if configs_loaded:
            logger.info(f"Loaded {len(configs_loaded)} config file(s): "
                       f"{', '.join(name for name, _ in configs_loaded)}")
        else:
            logger.warning("No configuration files were loaded!")

        return config
    
    def _extract_config_values(self):
        """Extract commonly used configuration values to attributes."""
        # Project identification
        self.project = self.config.get('project', 'Unknown')
        self.disease = self.config.get('disease', '')
        self.schemaTag = self.config.get('schemaTag', '')

        # Schema mappings
        self.schemas = self.config.get('schemas', {})
        self.RWDSchema = self.schemas.get('RWDSchema', '')
        self.projectSchema = self.schemas.get('projectSchema', '')
        self.omopSchema = self.schemas.get('omopSchema', '')

        # Table configurations
        self.RWDTables = self.config.get('RWDTables', {})
        self.projectTables = self.config.get('projectTables', {})

        # Paths
        self.dataLoc = self.config.get('dataLoc', '')
        self.parquetLoc = self.config.get('parquetLoc', '')
        self.warehouse = self.config.get('warehouse', '')

        if self.debug:
            logger.info(f"Project: {self.project}")
            logger.info(f"Schemas: {self.schemas}")
            logger.info(f"RWDSchema: {self.RWDSchema}")
            logger.info(f"RWDTables: {len(self.RWDTables)} tables defined")
            logger.info(f"projectTables: {len(self.projectTables)} tables defined")

        # Warn if expected configs are missing
        if self.RWDSchema and not self.RWDTables:
            logger.warning(
                f"RWDSchema is set ({self.RWDSchema}) but RWDTables is empty.\n"
                f"  This usually means the schema config file was not loaded.\n"
                f"  Check that schemaTag_config path is correct."
            )
        if self.projectSchema and not self.projectTables:
            logger.warning(
                f"projectSchema is set ({self.projectSchema}) but projectTables is empty.\n"
                f"  Check that the local config file defines projectTables."
            )
    
    def finish_init(self):
        """
        Complete initialization by loading data tables.

        Called automatically if finish_init=True in __init__.
        Can be called manually if finish_init=False was used.

        Uses callFunProcessDataTables from config to dynamically process
        all schemas defined in the schemas section.
        """
        # Track what was processed for summary logging
        self._missing_schemas = {}
        self._loaded_schemas = []

        # Get the callFunProcessDataTables config (defines how to process each schema type)
        callFunProcessDataTables = self.config.get('callFunProcessDataTables', {})

        if callFunProcessDataTables and self.schemas:
            # Dynamic schema processing via callFunProcessDataTables
            self._processAllDataTables(callFunProcessDataTables)
        else:
            # Fallback: hardcoded RWD/project processing (original behavior)
            self._process_default_schemas()

        # Create project config objects (available even if not processed)
        if self.projectTables:
            self.proj = self._create_extract_config()

        # Process project (extract) tables - create Extract
        if self.projectSchema and self.projectTables:
            from lhn.core.extract import Extract
            self.e = Extract(self.proj)
            logger.info(f"Initialized Extract with {len(self.projectTables)} table configs")

        # Log summary
        self._log_processing_summary()

    def _processAllDataTables(self, callFunProcessDataTables):
        """
        Process all schemas using callFunProcessDataTables configuration.

        For each schema in self.schemas, finds the matching callFun entry
        and processes the tables, setting self.{type_key} to the result.
        """
        logger.info(f"Processing {len(self.schemas)} schemas: {list(self.schemas.keys())}")

        for schemakey, schemavalue in self.schemas.items():
            if self.debug:
                logger.info(f"Processing schema: {schemakey} -> {schemavalue}")
            try:
                self._processDataTableBySchemakey(schemakey, callFunProcessDataTables)
            except Exception as e:
                logger.error(f"Failed to process {schemakey}: {e}")

    def _find_callFun_for_schema(self, schemakey, callFunProcessDataTables):
        """Find the callFun entry that matches the given schema key."""
        for name, callFun in callFunProcessDataTables.items():
            if callFun.get('schema_type') == schemakey:
                return callFun
        return None

    def _processDataTableBySchemakey(self, schemakey, callFunProcessDataTables):
        """
        Process a single schema using its callFun configuration.

        Parameters:
            schemakey (str): The schema key (e.g., 'RWDSchema', 'sstudySchema')
            callFunProcessDataTables (dict): The callFun configuration
        """
        schemavalue = self.schemas.get(schemakey)
        if not schemavalue:
            return

        callFun = self._find_callFun_for_schema(schemakey, callFunProcessDataTables)
        if not callFun:
            if self.debug:
                logger.info(f"No callFun found for schema_type={schemakey}")
            return

        data_type = callFun.get('data_type')      # e.g., 'RWDTables', 'sstudyTables'
        type_key = callFun.get('type_key')        # e.g., 'r', 'ss'
        property_name = callFun.get('property_name')  # e.g., 'rwd', 's'
        updateDict = callFun.get('updateDict', False)
        tableNameTemplate = callFun.get('tableNameTemplate')

        # Get the table definitions for this data_type
        dataTables = self.config.get(data_type, {})

        # If updateDict is True, auto-discover tables from the schema
        if updateDict and not dataTables:
            if database_exists(schemavalue):
                dataTables = self._discover_tables(schemavalue, tableNameTemplate)
                if self.debug:
                    logger.info(f"Auto-discovered {len(dataTables)} tables for {data_type}")
            else:
                if self.debug:
                    logger.info(f"Cannot auto-discover tables: database {schemavalue} doesn't exist")

        if not dataTables:
            if self.debug:
                logger.info(f"No tables defined for {data_type}")
            return

        if database_exists(schemavalue):
            if self.debug:
                logger.info(f"Database exists: {schemavalue}")

            # Process the tables
            result = processDataTables(
                dataTables=dataTables,
                schema=schemavalue,
                dataLoc=self.dataLoc,
                disease=self.disease,
                schemaTag=self.schemaTag,
                project=self.project,
                parquetLoc=self.parquetLoc,
                debug=self.debug
            )

            # Set self.{type_key} (e.g., self.r, self.ss)
            if type_key:
                setattr(self, type_key, result)
                self._loaded_schemas.append(f"self.{type_key}")
                logger.info(f"Loaded self.{type_key} from {schemavalue} ({len(result) if result else 0} tables)")

            # Create config objects and set self.{property_name} (e.g., self.rwd, self.s)
            if property_name:
                config_obj = self._create_schema_config(dataTables, schemavalue)
                setattr(self, property_name, config_obj)
                if self.debug:
                    logger.info(f"Created config self.{property_name}")

            # Also expose tables directly on Resources for convenience
            if result:
                for name in result.keys():
                    if not hasattr(self, name):
                        setattr(self, name, getattr(result, name))
        else:
            # Track missing schemas
            self._missing_schemas[schemakey] = {
                'database': schemavalue,
                'type_key': type_key,
                'property_name': property_name
            }
            logger.warning(f"Database not found: {schemavalue} (self.{type_key} unavailable)")

    def _process_default_schemas(self):
        """Fallback processing when callFunProcessDataTables is not configured."""
        # Create RWD config objects
        if self.RWDTables:
            self.rwd = self._create_rwd_config()

        # Process RWD tables
        if self.RWDSchema and self.RWDTables:
            if database_exists(self.RWDSchema):
                self.r = processDataTables(
                    dataTables=self.RWDTables,
                    schema=self.RWDSchema,
                    dataLoc=self.dataLoc,
                    disease=self.disease,
                    schemaTag=self.schemaTag,
                    project=self.project,
                    parquetLoc=self.parquetLoc,
                    debug=self.debug
                )

                if self.r:
                    for name in self.r.keys():
                        if not hasattr(self, name):
                            setattr(self, name, getattr(self.r, name))

                self._loaded_schemas.append('self.r')
                logger.info(f"Loaded {len(self.r) if self.r else 0} RWD tables")
            else:
                self._missing_schemas['RWDSchema'] = {
                    'database': self.RWDSchema,
                    'type_key': 'r',
                    'property_name': 'rwd'
                }
                logger.warning(f"RWD schema {self.RWDSchema} does not exist")

    def _create_schema_config(self, dataTables, schema):
        """Create configuration objects for a schema's tables.

        Parameters:
            dataTables (dict): Table definitions
            schema (str): Schema/database name

        Returns:
            dict: Config objects keyed by table name
        """
        items = {}
        for name, config in dataTables.items():
            class ConfigObj:
                pass
            obj = ConfigObj()

            obj.name = name
            obj.schema = schema
            obj.location = config.get('location', f"{schema}.{name}")
            obj.label = config.get('label', name)

            for key, value in config.items():
                setattr(obj, key, value)

            items[name] = obj

        return items

    def _discover_tables(self, schema, tableNameTemplate=None):
        """
        Auto-discover tables in a schema and create table definitions.

        Used when updateDict=True to automatically create table configs
        from all tables in the schema. Useful for reading another project's
        datasets as-is without manually defining each table.

        Parameters:
            schema (str): Schema/database name to scan
            tableNameTemplate (str): Optional regex pattern to filter/transform
                table names. If provided, only matching tables are included
                and the pattern extracts the base name.

        Returns:
            dict: Table definitions suitable for processDataTables
        """
        import re

        tableList = getTableList(schema)
        dataTables = {}

        # Compile pattern if provided
        pattern = None
        if tableNameTemplate:
            try:
                pattern = re.compile(rf"^(.*?){tableNameTemplate}")
            except re.error as e:
                logger.warning(f"Invalid tableNameTemplate pattern: {e}")

        for table_location in tableList:
            # table_location is like "schema.tablename"
            if '.' in table_location:
                table_name = table_location.split('.', 1)[-1]
            else:
                table_name = table_location

            # Apply pattern filtering if provided
            config_key = table_name
            if pattern:
                match = pattern.match(table_name)
                if match:
                    config_key = match.group(1).rstrip('_')
                # If no match and pattern provided, still include with original name

            # Avoid duplicates
            if config_key in dataTables:
                continue

            dataTables[config_key] = {
                'source': table_name,
                'label': table_name
            }

        if self.debug:
            logger.info(f"Discovered {len(dataTables)} tables in {schema}")

        return dataTables

    def _log_processing_summary(self):
        """Log a summary of what was processed successfully and what failed."""
        if self._loaded_schemas:
            logger.info(f"Successfully loaded: {', '.join(self._loaded_schemas)}")

        if self._missing_schemas:
            logger.warning("MISSING DATABASES - the following could not be loaded:")
            for schemakey, info in self._missing_schemas.items():
                logger.warning(f"  - {schemakey}: database '{info['database']}' not found "
                             f"(self.{info['type_key']} unavailable)")
    
    def _create_extract_config(self):
        """Create configuration objects for Extract initialization.

        Returns:
            dict: Config objects keyed by table name, usable with Extract()
        """
        extract_items = {}
        for name, config in self.projectTables.items():
            # Create an object that can be passed to ExtractItem
            class ConfigObj:
                pass
            obj = ConfigObj()

            # Set base attributes
            obj.name = name
            obj.location = f"{self.projectSchema}.{name}_{self.disease}_{self.schemaTag}"
            obj.label = config.get('label', name)
            obj.csv = f"{self.dataLoc}{name}_{self.disease}_{self.schemaTag}.csv"
            obj.parquet = f"{self.parquetLoc}{name}_{self.disease}_{self.schemaTag}"

            # Copy all config attributes
            for key, value in config.items():
                setattr(obj, key, value)

            extract_items[name] = obj

        return extract_items

    def _create_rwd_config(self):
        """Create configuration objects for RWD tables.

        Returns:
            dict: Config objects keyed by table name
        """
        rwd_items = {}
        for name, config in self.RWDTables.items():
            class ConfigObj:
                pass
            obj = ConfigObj()

            # Set base attributes
            obj.name = name
            obj.schema = self.RWDSchema
            obj.location = config.get('location', f"{self.RWDSchema}.{name}")
            obj.label = config.get('label', name)

            # Copy all config attributes
            for key, value in config.items():
                setattr(obj, key, value)

            rwd_items[name] = obj

        return rwd_items
    
    def refresh(self):
        """Reload configuration and reinitialize tables."""
        self.config = self._load_configs()
        self._extract_config_values()
        self.finish_init()
    
    def get_table(self, name):
        """
        Get a table by name from either r or e.
        
        Parameters:
            name (str): Table name
        
        Returns:
            Item or ExtractItem: Table object
        """
        if self.r and name in self.r:
            return self.r[name]
        if self.e and hasattr(self.e, name):
            return getattr(self.e, name)
        return None
    
    def list_tables(self):
        """List all available tables."""
        tables = []
        if self.r:
            tables.extend([f"r.{name}" for name in self.r.keys()])
        if self.e:
            tables.extend([f"e.{name}" for name in self.e.properties()])
        return tables
    
    def __repr__(self):
        return (f"Resources(project='{self.project}', "
                f"rwd_tables={len(self.r) if self.r else 0}, "
                f"extracts={len(self.e.properties()) if self.e else 0})")
