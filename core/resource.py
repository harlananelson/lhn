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
    database_exists
)

logger = get_logger(__name__)


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
                 replace=None, debug=False, finish_init=True):
        """
        Initialize Resources with configuration files.
        
        Parameters:
            local_config (str): Path to project config (000-config.yaml)
            global_config (str): Path to global config (config-global.yaml)
            schemaTag_config (str): Path to schema config (config-RWD.yaml)
            replace (dict): Template substitutions (e.g., {'today': '2025-01-19'})
            debug (bool): Enable debug logging
            finish_init (bool): If True, complete initialization by loading tables
        """
        self.debug = debug
        self.spark = spark
        self.replace = replace or {}
        
        # Store config paths
        self.local_config_path = local_config
        self.global_config_path = global_config
        self.schemaTag_config_path = schemaTag_config
        
        # Load configurations in order (later configs override earlier)
        self.config = self._load_configs()
        
        # Extract key configuration values
        self._extract_config_values()
        
        # Initialize table containers
        self.r = None  # Source data tables (RWD)
        self.e = None  # Extract tables (project outputs)
        
        if finish_init:
            self.finish_init()
    
    def _load_configs(self):
        """Load and merge configuration files."""
        config = {}
        
        # Load global config first
        if self.global_config_path and Path(self.global_config_path).exists():
            global_cfg = read_config(self.global_config_path, self.replace, self.debug)
            config.update(global_cfg)
            if self.debug:
                logger.info(f"Loaded global config from {self.global_config_path}")
        
        # Load schema-specific config
        if self.schemaTag_config_path and Path(self.schemaTag_config_path).exists():
            schema_cfg = read_config(self.schemaTag_config_path, self.replace, self.debug)
            config.update(schema_cfg)
            if self.debug:
                logger.info(f"Loaded schema config from {self.schemaTag_config_path}")
        
        # Load local project config (highest priority)
        if self.local_config_path and Path(self.local_config_path).exists():
            local_cfg = read_config(self.local_config_path, self.replace, self.debug)
            config.update(local_cfg)
            if self.debug:
                logger.info(f"Loaded local config from {self.local_config_path}")
        
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
    
    def finish_init(self):
        """
        Complete initialization by loading data tables.
        
        Called automatically if finish_init=True in __init__.
        Can be called manually if finish_init=False was used.
        """
        # Process RWD (source data) tables
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
                
                # Also expose tables directly on Resources
                if self.r:
                    for name in self.r.keys():
                        if not hasattr(self, name):
                            setattr(self, name, getattr(self.r, name))
                
                logger.info(f"Loaded {len(self.r) if self.r else 0} RWD tables")
            else:
                logger.warning(f"RWD schema {self.RWDSchema} does not exist")
        
        # Process project (extract) tables
        if self.projectSchema and self.projectTables:
            from lhn.core.extract import Extract
            self.e = Extract(self._create_extract_config())
            logger.info(f"Initialized Extract with {len(self.projectTables)} table configs")
    
    def _create_extract_config(self):
        """Create configuration objects for Extract initialization."""
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
