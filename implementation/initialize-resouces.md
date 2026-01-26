# Important Information on How to Setup A pyspark jupyter notebook to use the lhn package 


At the start of each pyspark notebooke some code is needed to initialize the lhn package and load needed objects as well as use the notebook with quarto

The first cell needs to be a RawNBconvert cell.  this has to be in the cell meta data,  for example the other three options are `Code` `Markdown` and `Heading`
```yaml
---
title: "Sickle Cell Disease"
subtitle: "01-Demographics-and-Followup"
author: "Gerard Dreason Hills MD"
date: "05/30/2025"
toc: true
execute:
    echo: true
    include: true
    eval: false
format: 
  html:
    code-fold: true
    toc: true
    number-sections: true
    html-math-method: katex
    embed-resources: true
  pdf:
    geometry:
      - top=30mm
      - left=30mm
    cite-method: citeproc
  docx: 
    toc: true
jupyter: python3B
# bibliography: `r here::here("references.bib")`
#csl: nature.csl
suppress-bibiography: false
---
```

The second cell is needed to expose global parameters

```python
#|include: false
#|echo: false

# Get the current notebook url

import html
import os
from IPython.display import display, Javascript, HTML
from pathlib import Path
import getpass
import sys
display(Javascript(
        'IPython.notebook.kernel.execute("notebook_url = " + "\'" + window.location + "\'");'))
```

The next cell creates a dictionary with the parameters needed to instantiate the a Resources object.  
This is the best point for the user to customize.

```
#|include: false
#|echo: false
# Import lhn - no sys.path manipulation needed since it's installed
import lhn as h
from lhn.header import *

# Notebook setup
display(HTML("<style>.container { width:99% !important; }</style>"))
current_directory, notebook_path, python_path, basePath, project = h.setup_environment(notebook_url)
from pprint import pprint

funCall = {
    'config_file'      : '000-config.yaml',
    'call_set_database': False ,
    'debug'            : True,
    'updateDict'       : False,
    'process_all'       : True
}
Resources_param = h.setFunctionParameters(h.Resources, funCall, config_dict = locals())
pprint(Resources_param)
```

The result of running this code block looks like this:

```
{'basePath': '/home/hnelson3/work/Users/hnelson3',
 'call_set_database': False,
 'config_file': '000-config.yaml',
 'debug': True,
 'global_yaml': 'configuration/config-global.yaml',
 'only_scan_current_tables': True,
 'partitionBy': None,
 'pattern_strings': ['standard.id',
                     'standard.codingSystemId',
                     'standard.primaryDisplay',
                     'brandType',
                     'zip_code',
                     'deceased',
                     'tenant',
                     'birthdate'],
 'personid': ['personid'],
 'process_all': True,
 'project': 'SickleCell_AI',
 'spark': <pyspark.sql.session.SparkSession object at 0x7fdee771b910>,
 'systemuser': 'hnelson3',
 'updateDict': False}
 ```


 Next is the actual call to instantiate the Resource object

 ```python
 #|include: false
#|echo: false


resource = h.Resources(**Resources_param)
locals().update(resource.load_into_local(everything = False))
#|include: false
#|echo: false

locals().update(resource.reread_config_files())
locals().update(resource.config_dict['schemas'])
```

That call, because of the current logging settings will produce a lot of output.


```

2025-12-22 14:18:18,594 - lhn.resource - INFO - key:config_local, location: /home/hnelson3/work/Users/hnelson3/Projects/SickleCell_AI/000-config.yaml
2025-12-22 14:18:18,596 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/Projects/SickleCell_AI/000-config.yaml and updating config_dict with key config_local
2025-12-22 14:18:18,755 - lhn.resource - INFO - key:config_global, location: /home/hnelson3/work/Users/hnelson3/configuration/config-global.yaml
2025-12-22 14:18:18,756 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/configuration/config-global.yaml and updating config_dict with key config_global
2025-12-22 14:18:18,825 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-global.yaml
2025-12-22 14:18:18,825 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-RWD.yaml
2025-12-22 14:18:18,826 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-IUH.yaml
2025-12-22 14:18:18,826 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-OMOP.yaml
2025-12-22 14:18:18,827 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-SCD.yaml
2025-12-22 14:18:18,827 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-project.yaml
2025-12-22 14:18:18,827 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-meta.yaml
2025-12-22 14:18:18,828 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictrwd.yaml
2025-12-22 14:18:18,828 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictomop.yaml
2025-12-22 14:18:18,828 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictscd.yaml
2025-12-22 14:18:18,829 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictiuhealth.yaml
2025-12-22 14:18:18,829 - lhn.resource - INFO - key:config_local, location: /home/hnelson3/work/Users/hnelson3/Projects/SickleCell_AI/000-config.yaml
2025-12-22 14:18:18,830 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/Projects/SickleCell_AI/000-config.yaml and updating config_dict with key config_local
2025-12-22 14:18:18,977 - lhn.resource - INFO - key:config_global, location: /home/hnelson3/work/Users/hnelson3/configuration/config-global.yaml
2025-12-22 14:18:18,978 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/configuration/config-global.yaml and updating config_dict with key config_global
2025-12-22 14:18:19,058 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-global.yaml
2025-12-22 14:18:19,058 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-RWD.yaml
2025-12-22 14:18:19,059 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-IUH.yaml
2025-12-22 14:18:19,059 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-OMOP.yaml
2025-12-22 14:18:19,059 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-SCD.yaml
2025-12-22 14:18:19,060 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-project.yaml
2025-12-22 14:18:19,060 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-meta.yaml
2025-12-22 14:18:19,060 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictrwd.yaml
2025-12-22 14:18:19,061 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictomop.yaml
2025-12-22 14:18:19,061 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictscd.yaml
2025-12-22 14:18:19,062 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictiuhealth.yaml
2025-12-22 14:18:19,062 - lhn.resource - INFO - key:config_RWD, location: /home/hnelson3/work/Users/hnelson3/configuration/config-RWD.yaml
2025-12-22 14:18:19,063 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/configuration/config-RWD.yaml and updating config_dict with key config_RWD
2025-12-22 14:18:19,154 - lhn.resource - INFO - key:config_IUH, location: /home/hnelson3/work/Users/hnelson3/configuration/config-IUH.yaml
2025-12-22 14:18:19,155 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/configuration/config-IUH.yaml and updating config_dict with key config_IUH
2025-12-22 14:18:19,200 - lhn.resource - INFO - key:config_OMOP, location: /home/hnelson3/work/Users/hnelson3/configuration/config-OMOP.yaml
2025-12-22 14:18:19,201 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/configuration/config-OMOP.yaml and updating config_dict with key config_OMOP
2025-12-22 14:18:19,242 - lhn.resource - INFO - key:config_SCD, location: /home/hnelson3/work/Users/hnelson3/configuration/config-SCD.yaml
2025-12-22 14:18:19,278 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-SCD.yaml not found
2025-12-22 14:18:19,278 - lhn.resource - INFO - key:config_project, location: /home/hnelson3/work/Users/hnelson3/configuration/config-project.yaml
2025-12-22 14:18:19,317 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-project.yaml not found
2025-12-22 14:18:19,317 - lhn.resource - INFO - key:config_meta, location: /home/hnelson3/work/Users/hnelson3/configuration/config-meta.yaml
2025-12-22 14:18:19,349 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-meta.yaml not found
2025-12-22 14:18:19,350 - lhn.resource - INFO - key:config_dictrwd, location: /home/hnelson3/work/Users/hnelson3/configuration/config-dictrwd.yaml
2025-12-22 14:18:19,455 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-dictrwd.yaml not found
2025-12-22 14:18:19,456 - lhn.resource - INFO - key:config_dictomop, location: /home/hnelson3/work/Users/hnelson3/configuration/config-dictomop.yaml
2025-12-22 14:18:19,495 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-dictomop.yaml not found
2025-12-22 14:18:19,495 - lhn.resource - INFO - key:config_dictscd, location: /home/hnelson3/work/Users/hnelson3/configuration/config-dictscd.yaml
2025-12-22 14:18:19,527 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-dictscd.yaml not found
2025-12-22 14:18:19,528 - lhn.resource - INFO - key:config_dictiuhealth, location: /home/hnelson3/work/Users/hnelson3/configuration/config-dictiuhealth.yaml
2025-12-22 14:18:19,565 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-dictiuhealth.yaml not found
2025-12-22 14:18:19,923 - lhn.resource - INFO - Successfully Executed: self.read_config_all()
2025-12-22 14:18:19,924 - lhn.resource - INFO - Starting processAllDataTables with callFunDict: 
 {'IUHdatacallFunc': {'data_type': 'IUHdataTables',
                     'property_name': 'iuh',
                     'schema_type': 'IUHSchema',
                     'tableNameTemplate': None,
                     'type_key': 'iuhealth',
                     'updateDict': False},
 'RWDNationalcallFunc': {'data_type': 'RWDTables',
                         'property_name': 'rn',
                         'schema_type': 'RWDNationalSchema',
                         'tableNameTemplate': None,
                         'type_key': 'rwdn',
                         'updateDict': False},
 'RWDOriginalcallFunc': {'data_type': 'RWDOriginalTables',
                         'property_name': 'rno',
                         'schema_type': 'RWDNationalSchema',
                         'tableNameTemplate': None,
                         'type_key': 'rwdno',
                         'updateDict': False},
 'RWDSicklecallFunc': {'data_type': 'RWDTables',
                       'property_name': 'rs',
                       'schema_type': 'RWDSickleSchema',
                       'tableNameTemplate': None,
                       'type_key': 'rwds',
                       'updateDict': False},
 'RWDcallFunc': {'data_type': 'RWDTables',
                 'property_name': 'r',
                 'schema_type': 'RWDSchema',
                 'tableNameTemplate': None,
                 'type_key': 'rwd',
                 'updateDict': False},
 'dictrwdcallFunc': {'data_type': None,
                     'property_name': 'd',
                     'schema_type': 'dictrwdSchema',
                     'tableNameTemplate': '_rwd',
                     'type_key': 'dictrwd',
                     'updateDict': True},
 'metacallFunc': {'data_type': 'metaTables',
                  'property_name': 'm',
                  'schema_type': 'metaSchema',
                  'tableNameTemplate': None,
                  'type_key': 'meta',
                  'updateDict': True},
 'omopcallFunc': {'data_type': 'omopTables',
                  'property_name': 'o',
                  'schema_type': 'omopSchema',
                  'tableNameTemplate': None,
                  'type_key': 'omop',
                  'updateDict': True},
 'projectcallFunc': {'data_type': 'projectTables',
                     'property_name': 'db',
                     'schema_type': 'projectSchema',
                     'tableNameTemplate': '_{disease}_{schemaTag}',
                     'type_key': 'proj',
                     'updateDict': False},
 'sicklecallFunc': {'data_type': 'SCDTables',
                    'property_name': 'scd',
                    'schema_type': 'SCDSchema',
                    'tableNameTemplate': None,
                    'type_key': 'sickle',
                    'updateDict': True},
 'sstudycallFunc': {'data_type': 'sstudyTables',
                    'property_name': 's',
                    'schema_type': 'sstudySchema',
                    'tableNameTemplate': None,
                    'type_key': 'ss',
                    'updateDict': True}} 
 and schemas: 
 {'RWDSchema': 'iuhealth_ed_data_cohort_202306',
 'SCDSchema': 'sicklecell_rerun',
 'projectSchema': 'sicklecell_ai',
 'sstudySchema': 'sicklecell_study_rwd'}
2025-12-22 14:18:19,925 - lhn.resource - INFO - Processing schema name RWDSchema at location iuhealth_ed_data_cohort_202306
2025-12-22 14:18:19,925 - lhn.resource - INFO - processDataTableBySchemakey: Processing schema name RWDSchema at location iuhealth_ed_data_cohort_202306 (from schemas dictionary)
2025-12-22 14:18:19,925 - lhn.resource - INFO - Found: schema RWDSchema:iuhealth_ed_data_cohort_202306 in callFunProcessDataTables
2025-12-22 14:18:19,926 - lhn.resource - INFO - Element of callFunProcessDataTables used for callFun: 
 {'data_type': 'RWDTables',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'property_name': 'r',
 'schema_type': 'RWDSchema',
 'tableNameTemplate': None,
 'type_key': 'rwd',
 'updateDict': False}
2025-12-22 14:18:20,016 - lhn.resource - INFO - Found schema RWDSchema at iuhealth_ed_data_cohort_202306
 projectTables at local step
PosixPath('/home/hnelson3/work/Users/hnelson3/Projects/SickleCell_AI/000-config.yaml')
 projectTables at local step
PosixPath('/home/hnelson3/work/Users/hnelson3/configuration/config-global.yaml')
2025-12-22 14:18:20,100 - lhn.resource - INFO - Found schema RWDSchema indicated by schemaTag RWD at iuhealth_ed_data_cohort_202306
2025-12-22 14:18:20,100 - lhn.resource - INFO - Use to call processDataTables
2025-12-22 14:18:20,107 - lhn.resource - INFO - funCall: {'dataLoc': '/home/hnelson3/work/Users/hnelson3/inst/extdata/SickleCell_AI/',
 'dataTables': {'EncounterInsuranceSource': {'inputRegex': ['^encounterid',
                                                            '^personid',
                                                            '^servicedate',
                                                            '^dischargedate',
                                                            '^classification_standard_primaryDisplay',
                                                            '^financialclass_standard_primaryDisplay',
                                                            '^tenant$'],
                                             'insert': ["withColumnRenamed('classification_standard_primaryDisplay','encType')",
                                                        "withColumn('datetimeEnc', "
                                                        "F.coalesce(F.to_timestamp(F.col('servicedate')),F.to_timestamp(F.col('dischargedate'))))",
                                                        "drop('classification_standard_id')"],
                                             'source': 'encounter'},
                'birthsex': {'inputRegex': ['personid',
                                            'birthsex_standard_primaryDisplay'],
                             'source': 'demographics'},
                'clinical_eventSource': {'inputRegex': ['^clincialeventid$',
                                                        '^personid$',
                                                        '^clinicaleventid$',
                                                        '^encounterid$',
                                                        '^clinicaleventcode_standard_id$',
                                                        '^clinicaleventcode_standard_codingSystemId$',
                                                        'clinicaleventcode.standard.primaryDisplay',
                                                        '^loincclass',
                                                        '^type$',
                                                        '^servicedate$',
                                                        '^tenant$'],
                                         'source': 'clinical_event'},
                'clinical_eventSourceAll': {'inputRegex': ['^clincialeventid$',
                                                           '^personid$',
                                                           '^clinicaleventid$',
                                                           '^encounterid$',
                                                           '^clinicaleventcode_standard_id$',
                                                           '^clinicaleventcode_standard_codingSystemId$',
                                                           'clinicaleventcode.standard.primaryDisplay',
                                                           '^loincclass',
                                                           '^type$',
                                                           '^servicedate$',
                                                           '^typedvalue_type$',
                                                           '^typedvalue_textValue.value$',
                                                           '^typedvalue_numericValue_value$',
                                                           '^typedvalue_numericValue_modifier$',
                                                           '^typedvalue_unitOfMeasure_standard.id$',
                                                           '^typedvalue_unitOfMeasure_standard_codingSystemId$',
                                                           '^typedvalue_unitOfMeasure_standard_primaryDisplay$',
                                                           '^typedvalue_unitOfMeasure_standardCodings_id$',
                                                           '^typedvalue_unitOfMeasure_standardCodings_codingSystemId$',
                                                           '^typedvalue_unitOfMeasure_standardCodings_primaryDisplay$',
                                                           '^typedvalue_codifiedValues_values_value_standard_id$',
                                                           '^typedvalue_codifiedValues_values_value_standard_codingSystemId$',
                                                           '^typedvalue_codifiedValues_values_value_standard_primaryDisplay$',
                                                           '^typedvalue_codifiedValues_values_value_standardCodings_id$',
                                                           '^typedvalue_codifiedValues_values_value_standardCodings_codingSystemId$',
                                                           '^typedvalue_codifiedValues_values_value_standardCodings_primaryDisplay$',
                                                           '^typedvalue_dateValue_date$',
                                                           '^interpretation_standard_id$',
                                                           '^interpretation_standard_codingSystemId$',
                                                           '^interpretation_standard_primaryDisplay$',
                                                           '^status.standard_id$',
                                                           '^status_standard_codingSystemId$',
                                                           '^status_standard_primaryDisplay$',
                                                           '^recordertype$',
                                                           '^requester$',
                                                           '^issueddate$',
                                                           '^tenant$'],
                                            'source': 'clinical_event'},
                'conditionFinalConfirmedSource': {'datefield': 'datetime',
                                                  'inputRegex': ['^personid',
                                                                 '^encounterId',
                                                                 '^conditionId',
                                                                 '^effectiveDate',
                                                                 '^asserteddate$',
                                                                 '^conditionCode_standard_',
                                                                 '^billingrank',
                                                                 '^classification_standard_primaryDisplay',
                                                                 'confirmationstatus_standard_primaryDisplay',
                                                                 '^responsibleprovider',
                                                                 '^tenant$'],
                                                  'insert': ["withColumn('datetimeCondition', "
                                                             "F.coalesce(F.to_timestamp(F.col('effectiveDate')),F.to_timestamp(F.col('assertedDate'))))",
                                                             "withColumnRenamed('classification_standard_primaryDisplay','conditionType')",
                                                             "withColumn('dateCondition', "
                                                             "F.to_date(F.col('datetimeCondition')))",
                                                             "filter((F.col('classification_standard_primaryDisplay') "
                                                             "== 'Final "
                                                             'diagnosis '
                                                             "(discharge)') & "
                                                             "(F.col('confirmationstatus_standard_primaryDisplay') "
                                                             "== 'Confirmed "
                                                             "present'))"],
                                                  'source': 'condition'},
                'conditionFinalSource': {'datefield': 'datetimeCondition',
                                         'inputRegex': ['^personid',
                                                        '^encounterId',
                                                        '^conditionId',
                                                        '^effectiveDate',
                                                        '^asserteddate$',
                                                        '^conditionCode_standard_',
                                                        '^billingrank',
                                                        '^classification_standard_primaryDisplay',
                                                        'confirmationstatus_standard_primaryDisplay',
                                                        '^responsibleprovider',
                                                        '^tenant$'],
                                         'insert': ["withColumn('datetimeCondition', "
                                                    "F.coalesce(F.to_timestamp(F.col('effectiveDate')),F.to_timestamp(F.col('assertedDate'))))",
                                                    "withColumnRenamed('classification_standard_primaryDisplay','conditionType')",
                                                    "withColumn('dateCondition', "
                                                    "F.to_date(F.col('datetimeCondition')))",
                                                    "filter(F.col('classification_standard_primaryDisplay') "
                                                    "== 'Final diagnosis "
                                                    "(discharge)')"],
                                         'source': 'condition'},
                'conditionSource': {'datefield': 'datetimeCondition',
                                    'inputRegex': ['^personid',
                                                   '^personId$',
                                                   '^encounterId',
                                                   '^conditionId',
                                                   '^effectiveDate',
                                                   '^asserteddate$',
                                                   '^conditionCode_standard_',
                                                   '^billingrank',
                                                   '^classification_standard_primaryDisplay',
                                                   'confirmationstatus_standard_primaryDisplay',
                                                   '^responsibleprovider',
                                                   '^tenant$'],
                                    'insert': ["withColumn('datetimeCondition', "
                                               "F.coalesce(F.to_timestamp(F.col('effectiveDate')),F.to_timestamp(F.col('assertedDate'))))",
                                               "withColumnRenamed('classification_standard_primaryDisplay','conditionType')"],
                                    'source': 'condition'},
                'demoPreferred': {'colsRename': {'prefbirthdate': 'birthdate',
                                                 'prefethnicity': 'ethnicity',
                                                 'prefgender': 'gender',
                                                 'prefmetropolitan': 'metropolitan',
                                                 'prefrace': 'race',
                                                 'prefstate': 'state',
                                                 'prefurban': 'urban',
                                                 'prefyearofbirth': 'yearofbirth',
                                                 'prefzip': 'zip'},
                                  'inputRegex': ['personid',
                                                 'prefgender$',
                                                 'prefrace$',
                                                 'prefethnicity$',
                                                 'prefyearofbirth$',
                                                 'prefstate',
                                                 'prefmetropolitan',
                                                 'prefurban',
                                                 'prefzip',
                                                 'tenant',
                                                 'testpatientflag'],
                                  'insert': ["withColumn('birthdate', "
                                             "F.to_date(F.col('prefyearofbirth')) "
                                             ')'],
                                  'source': 'preferred_demographics'},
                'demoSource': {'inputRegex': ['personid',
                                              'yearofbirth',
                                              'dateofdeath',
                                              'gender',
                                              '^maritalstatus_standard_primaryDisplay$',
                                              'races',
                                              'ethnicities',
                                              'deceased',
                                              'state',
                                              'testpatientflag',
                                              'source$',
                                              'tenant',
                                              '^gender_standard_primaryDisplay$',
                                              '^birthsex_standard_primaryDisplay$',
                                              '^races_standard_primaryDisplay$',
                                              '^ethnicities_standard_primaryDisplay$',
                                              'zip_code'],
                               'insert': ["withColumn( 'dateofdeath', "
                                          "F.last_day(F.col('dateofdeath')))",
                                          "withColumn( 'yearofbirth', "
                                          "F.to_date( 'yearofbirth'))"],
                               'source': 'demographics'},
                'emergencyEncounter': {'inputRegex': ['^encounterid',
                                                      '^personid',
                                                      '^servicedate',
                                                      '^dischargedate',
                                                      '^classification_standard_id',
                                                      '^classification_standard_primaryDisplay',
                                                      '^tenant$'],
                                       'insert': ["withColumnRenamed('classification_standard_primaryDisplay','encType')",
                                                  "withColumn('datetimeEnc', "
                                                  "F.coalesce(F.to_timestamp(F.col('servicedate')),F.to_timestamp(F.col('dischargedate'))))",
                                                  "filter(F.col('classification_standard_id').isin(['E','3301000175100']))",
                                                  "withColumnRenamed('classification_standard_primaryDisplay','encType')",
                                                  "drop('classification_standard_id')"],
                                       'source': 'encounter'},
                'encounterSource': {'datefield': 'datetimeEnc',
                                    'inputRegex': ['^encounterid',
                                                   '^personid',
                                                   '^servicedate',
                                                   '^dischargedate',
                                                   '^financialclass_standard_primaryDisplay$',
                                                   '^facilityids$',
                                                   '^hospitalservice_standard_primaryDisplay$',
                                                   '^classification_standard_id$',
                                                   '^classification_standard_codingSystemId$',
                                                   '^classification_standard_primaryDisplay$',
                                                   '^type_standard_primaryDisplay$',
                                                   '^status_standard_primaryDisplay^',
                                                   '^actualarrivaldate',
                                                   '^tenant$'],
                                    'insert': ["withColumnRenamed('classification_standard_primaryDisplay','encType')",
                                               "withColumn('servicedate',   "
                                               "F.to_timestamp(F.col('servicedate')))",
                                               "withColumn('dischargedate', "
                                               "F.to_timestamp(F.col('servicedate')))",
                                               "withColumn('datetimeEnc',   "
                                               "F.coalesce(F.col('servicedate'),F.col('dischargedate')))",
                                               "withColumn('dateEnc',       "
                                               "F.to_date(F.col('datetimeEnc')))",
                                               "filter(F.col('encType').isin(['Outpatient','Emergency','Inpatient','Admitted "
                                               "for Observation']))"],
                                    'source': 'encounter'},
                'encounterSourceAll': {'datefield': 'datetimeEnc',
                                       'inputRegex': ['^encounterid',
                                                      '^personid',
                                                      '^servicedate',
                                                      '^dischargedate',
                                                      '^financialclass_standard_primaryDisplay$',
                                                      '^facilityids$',
                                                      '^reasonforvisit_standard_primaryDisplay$',
                                                      '^hospitalservice_standard_primaryDisplay$',
                                                      '^classification_standard_id$',
                                                      '^classification_standard_codingSystemId$',
                                                      '^classification_standard_primaryDisplay$',
                                                      '^encountertypes_classification_standard_primaryDisplay$',
                                                      '^type_standard_primaryDisplay$',
                                                      '^dischargedisposition_standard_primaryDisplay$',
                                                      '^dischargetolocation_standard_primaryDisplay$',
                                                      '^admissionsource_standard_primaryDisplay$',
                                                      '^hospitalizationstartdate',
                                                      '^readmission',
                                                      '^admissiontype_standard_primaryDisplay$',
                                                      '^status_standard_primaryDisplay^',
                                                      '^actualarrivaldate',
                                                      '^tenant$'],
                                       'insert': ["withColumnRenamed('classification_standard_primaryDisplay','encType')",
                                                  "withColumn('datetimeEnc', "
                                                  "F.coalesce(F.to_timestamp(F.col('servicedate')),F.to_timestamp(F.col('dischargedate'))))",
                                                  "withColumn('dateEnc', "
                                                  "F.to_date(F.col('datetimeEnc')))"],
                                       'source': 'encounter'},
                'encounterSourceDates': {'datefield': 'datetimeEnc',
                                         'inputRegex': ['^encounterid',
                                                        '^personid',
                                                        '^servicedate',
                                                        '^dischargedate',
                                                        '^classification_standard_primaryDisplay$',
                                                        '^tenant$'],
                                         'insert': ["withColumnRenamed('classification_standard_primaryDisplay','encType')",
                                                    "withColumn('servicedate',   "
                                                    "F.to_timestamp(F.col('servicedate')))",
                                                    "withColumn('dischargedate', "
                                                    "F.to_timestamp(F.col('servicedate')))",
                                                    "withColumn('datetimeEnc',   "
                                                    "F.coalesce(F.col('servicedate'),F.col('dischargedate')))",
                                                    "withColumn('dateEnc',       "
                                                    "F.to_date(F.col('datetimeEnc')))",
                                                    "filter(F.col('encType').isin(['Outpatient','Emergency','Inpatient','Admitted "
                                                    "for Observation']))"],
                                         'source': 'encounter'},
                'ethnicities': {'inputRegex': ['personid',
                                               'ethnicities_standard_primaryDisplay'],
                                'source': 'demographics'},
                'gender': {'inputRegex': ['personid',
                                          'gender_standard_primaryDisplay'],
                           'source': 'demographics'},
                'inpatientEncounterSource': {'inputRegex': ['^encounterid',
                                                            '^personid',
                                                            '^servicedate',
                                                            '^dischargedate',
                                                            '^classification_standard_id',
                                                            '^tenant$'],
                                             'insert': ["withColumnRenamed('classification_standard_primaryDisplay','encType')",
                                                        "withColumn('datetimeEnc', "
                                                        "F.coalesce(F.to_timestamp(F.col('servicedate')),F.to_timestamp(F.col('dischargedate'))))",
                                                        "filter(F.col('classification_standard_id').isin(['I','3301000175100']))",
                                                        "drop('classification_standard_id')"],
                                             'source': 'encounter'},
                'labSource': {'colsRename': {'servicestartdate': 'servicedatestartLab'},
                              'datefield': 'datetimeLab',
                              'inputRegex': ['^labid',
                                             '^encounterid',
                                             '^personid',
                                             '^labcode_standard_id$',
                                             '^labcode_standard_codingSystemId$',
                                             '^labcode_standard_primaryDisplay$',
                                             '^typedvalue_type$',
                                             '^typedvalue_numericValue_value$',
                                             '^typedvalue_textValue_value$',
                                             '^typedvalue_numericValue_modifier$',
                                             '^typedvalue_unitOfMeasure_standard_id$',
                                             '^typedvalue_unitOfMeasure_standard_codingSystemId$',
                                             '^typedvalue_unitOfMeasure_standard_primaryDisplay$',
                                             '^interpretation_standard_codingSystemId$',
                                             '^interpretation_standard_primaryDisplay$',
                                             '^loincclass$',
                                             '^issueddate$',
                                             '^source$',
                                             '^active$',
                                             '^type$',
                                             '^servicedate$',
                                             'serviceperiod_startDate$',
                                             'serviceperiod_endDate$',
                                             '^tenant$'],
                              'insert': ['withColumn("datetimeLab", '
                                         'F.to_utc_timestamp(F.col("servicedate"), '
                                         '"UTC"))',
                                         'withColumn("dateLab", '
                                         'F.to_date(F.col("datetimeLab")))'],
                              'source': 'lab'},
                'labsIndexSource': {'datefield': 'datetimeLab',
                                    'inputRegex': ['^labid',
                                                   '^encounterid',
                                                   '^personid',
                                                   '^labcode_standard_id$',
                                                   '^labcode_standard_codingSystemId$',
                                                   '^labcode_standard_primaryDisplay$',
                                                   '^servicedate$',
                                                   '^tenant$'],
                                    'insert': ['withColumn("datetimeLab", '
                                               'F.to_utc_timestamp(F.col("servicedate"), '
                                               '"UTC"))',
                                               'withColumn("dateLab", '
                                               'F.to_date(F.col("datetimeLab")))'],
                                    'source': 'lab'},
                'labsmallRangeSource': {'datefield': 'datetimeLab',
                                        'inputRegex': ['^labid',
                                                       '^encounterid',
                                                       '^personid',
                                                       '^labcode_standard_id$',
                                                       '^labcode_standard_codingSystemId$',
                                                       '^labcode_standard_primaryDisplay$',
                                                       '^typedvalue_numericValue_value$',
                                                       '^typedvalue_textValue_value$',
                                                       '^typedvalue_unitOfMeasure_standard_id$',
                                                       '^interpretation_standard_primaryDisplay$',
                                                       '^referencerange_typedreferencelowvalue_numericvalue.value',
                                                       '^referencerange_typedreferencehighvalue_numericvalue_value',
                                                       '^loincclass$',
                                                       '^servicedate$',
                                                       '^tenant$'],
                                        'insert': ['withColumn("datetimeLab", '
                                                   'F.to_utc_timestamp(F.col("servicedate"), '
                                                   '"UTC"))',
                                                   'withColumn("dateLab", '
                                                   'F.to_date(F.col("datetimeLab")))'],
                                        'source': 'lab'},
                'labsmallSource': {'datefield': 'datetimeLab',
                                   'inputRegex': ['^labid',
                                                  '^encounterid',
                                                  '^personid',
                                                  '^labcode_standard_id$',
                                                  '^labcode_standard_codingSystemId$',
                                                  '^labcode_standard_primaryDisplay$',
                                                  '^typedvalue_numericValue_value$',
                                                  '^typedvalue_textValue_value$',
                                                  '^typedvalue_unitOfMeasure_standard_id$',
                                                  '^interpretation_standard_primaryDisplay$',
                                                  '^loincclass$',
                                                  '^servicedate$',
                                                  '^tenant$'],
                                   'insert': ['withColumn("datetimeLab", '
                                              'F.to_utc_timestamp(F.col("servicedate"), '
                                              '"UTC"))',
                                              'withColumn("dateLab", '
                                              'F.to_date(F.col("datetimeLab")))'],
                                   'source': 'lab'},
                'maritalstatus': {'inputRegex': ['personid',
                                                 'maritalstatus_standard_primaryDisplay'],
                                  'source': 'demographics'},
                'measurementMinimalSource': {'datefield': 'datetimeMeasurement',
                                             'inputRegex': ['^personid',
                                                            '^encounterId',
                                                            '^measurementid',
                                                            '^measurementcode_standard_id',
                                                            '^measurementcode_standard_codingSystemId',
                                                            '^measurementcode_standard_primaryDisplay',
                                                            '^typedvalue_numericValue_value',
                                                            '^typedvalue_unitOfMeasure_standard_id',
                                                            "^interpretation_standard_primaryDisplay$'",
                                                            '^loincclass',
                                                            '^servicedate',
                                                            '^tenant$'],
                                             'insert': ["withColumn('datetimeMeasurement', "
                                                        "F.to_utc_timestamp(F.col('servicedate'), "
                                                        "'UTC'))",
                                                        "withColumn('dateMeasurement', "
                                                        "F.to_date(F.col('datetimeMeasurement')))"],
                                             'source': 'measurement'},
                'measurementSource': {'datefield': 'datetimeMeasurement',
                                      'inputRegex': ['^measurementid',
                                                     '^encounterid',
                                                     '^personid',
                                                     '^measurementcode_standard_id$',
                                                     '^measurementcode_standard_codingSystemId$',
                                                     '^measurementcode_standard_primaryDisplay$',
                                                     'loincclass',
                                                     '^typedvalue_numericValue_value$',
                                                     '^typedvalue_unitOfMeasure_standard_id$',
                                                     '^interpretation_standard_primaryDisplay$',
                                                     '^loincclass$',
                                                     '^servicedate$',
                                                     '^tenant$'],
                                      'insert': ['withColumn("datetimeMeasurement", '
                                                 'F.to_utc_timestamp(F.col("servicedate"), '
                                                 '"UTC"))',
                                                 'withColumn("dateMeasurement", '
                                                 'F.to_date(F.col("datetimeMeasurement")))'],
                                      'source': 'measurement'},
                'measurementSource2': {'datefield': 'datetimeMeasurement',
                                       'inputRegex': ['^measurementid',
                                                      '^encounterid',
                                                      '^personid',
                                                      '^measurementcode_standard_id$',
                                                      '^measurementcode_standard_codingSystemId$',
                                                      '^measurementcode_standard_primaryDisplay$',
                                                      'loincclass',
                                                      '^typedvalue_type^',
                                                      '^typedvalue_numericValue_value$',
                                                      '^typedvalue_textValue_value$',
                                                      '^typedvalue_numericValue_modifier$',
                                                      '^typedvalue_unitOfMeasure_standard_id$',
                                                      '^typedvalue_unitOfMeasure_standard_codingSystemId$',
                                                      '^typedvalue_unitOfMeasure_standard_primaryDisplay$',
                                                      '^interpretation_standard_codingSystemId$',
                                                      '^interpretation_standard_primaryDisplay$',
                                                      '^loincclass$',
                                                      '^issueddate$',
                                                      '^source$',
                                                      '^active$',
                                                      '^type$',
                                                      '^servicedate$',
                                                      'serviceperiod_startDate$',
                                                      'serviceperiod_endDate$',
                                                      '^tenant$'],
                                       'insert': ['withColumn("datetimeMeasurement", '
                                                  'F.to_utc_timestamp(F.col("servicedate"), '
                                                  '"UTC"))',
                                                  'withColumn("dateMeasurement", '
                                                  'F.to_date(F.col("datetimeMeasurement")))'],
                                       'source': 'measurement'},
                'measurmentIndexSource': {'datefield': 'datetimeMeasurement',
                                          'inputRegex': ['^personid',
                                                         '^encounterId',
                                                         '^measurementid',
                                                         '^measurementcode_standard_id',
                                                         '^measurementcode_standard_codingSystemId',
                                                         '^measurementcode_standard_primaryDisplay',
                                                         '^servicedate',
                                                         '^tenant$'],
                                          'insert': ["withColumn('datetimeMeasurement', "
                                                     "F.to_utc_timestamp(F.col('servicedate'), "
                                                     "'UTC'))",
                                                     "withColumn('dateMeasurement', "
                                                     "F.to_date(F.col('datetimeMeasurement')))"],
                                          'source': 'measurement'},
                'medicationDatesSource': {'datefield': "datetimeMed'",
                                          'inputRegex': ['^personid',
                                                         '^encounterId',
                                                         '^medicationid',
                                                         '^startDate',
                                                         '^stopDate',
                                                         'prescribingprovider',
                                                         '^tenant$'],
                                          'insert': ["withColumn('datetimeMed', "
                                                     "F.coalesce(F.to_timestamp(F.col('startdate')),F.to_timestamp(F.col('stopdate'))))",
                                                     "withColumn('dateMed', "
                                                     "F.to_date(F.col('datetimeMed')))",
                                                     'withColumn("startDate", '
                                                     'F.to_utc_timestamp(F.col("startDate"), '
                                                     '"UTC"))',
                                                     'withColumn("stopDate", '
                                                     'F.to_utc_timestamp(F.col("stopDate"), '
                                                     '"UTC"))'],
                                          'source': 'medication'},
                'medicationDrugSource': {'datefield': "datetimeMed'",
                                         'inputRegex': ['^personid',
                                                        '^encounterId',
                                                        '^medicationid',
                                                        '^drugCode_standard_id',
                                                        '^drugCode_standard_codingSystemId',
                                                        '^drugCode_standard_primaryDisplay',
                                                        '^startDate',
                                                        '^stopDate',
                                                        'prescribingprovider',
                                                        '^tenant$'],
                                         'insert': ["withColumn('datetimeMed', "
                                                    "F.coalesce(F.to_timestamp(F.col('startdate')),F.to_timestamp(F.col('stopdate'))))",
                                                    "withColumn('dateMed', "
                                                    "F.to_date(F.col('datetimeMed')))",
                                                    'withColumn("startDate", '
                                                    'F.to_utc_timestamp(F.col("startDate"), '
                                                    '"UTC"))',
                                                    'withColumn("stopDate", '
                                                    'F.to_utc_timestamp(F.col("stopDate"), '
                                                    '"UTC"))'],
                                         'source': 'medication'},
                'medicationIngbredientsSource': {'datefield': "datetimeMed'",
                                                 'inputRegex': ['^personid',
                                                                '^encounterId',
                                                                '^medicationid',
                                                                '^ingredients_drugCode_standard_id',
                                                                '^ingredients_drugCode_standard_codingSystemId',
                                                                '^ingredients_drugCode_standard_primaryDisplay',
                                                                '^startDate',
                                                                '^stopDate',
                                                                'prescribingprovider',
                                                                '^tenant$'],
                                                 'insert': ["withColumn('datetimeMed', "
                                                            "F.coalesce(F.to_timestamp(F.col('startdate')),F.to_timestamp(F.col('stopdate'))))",
                                                            "withColumn('dateMed', "
                                                            "F.to_date(F.col('datetimeMed')))",
                                                            'withColumn("startDate", '
                                                            'F.to_utc_timestamp(F.col("startDate"), '
                                                            '"UTC"))',
                                                            'withColumn("stopDate", '
                                                            'F.to_utc_timestamp(F.col("stopDate"), '
                                                            '"UTC"))'],
                                                 'source': 'medication'},
                'medicationSource': {'datefield': "datetimeMed'",
                                     'inputRegex': ['^personid',
                                                    '^encounterId',
                                                    '^medicationid',
                                                    '^drugCode_standard_id',
                                                    '^drugCode_standard_codingSystemId',
                                                    '^drugCode_standard_primaryDisplay',
                                                    '^ingredients_drugCode_standard_id',
                                                    '^ingredients_drugCode_standard_codingSystemId',
                                                    '^ingredients_drugCode_standard_primaryDisplay',
                                                    '^startDate',
                                                    '^stopDate',
                                                    'prescribingprovider',
                                                    '^tenant$'],
                                     'insert': ["withColumn('datetimeMed', "
                                                "F.coalesce(F.to_timestamp(F.col('startdate')),F.to_timestamp(F.col('stopdate'))))",
                                                "withColumn('dateMed', "
                                                "F.to_date(F.col('datetimeMed')))",
                                                'withColumn("startDate", '
                                                'F.to_utc_timestamp(F.col("startDate"), '
                                                '"UTC"))',
                                                'withColumn("stopDate", '
                                                'F.to_utc_timestamp(F.col("stopDate"), '
                                                '"UTC"))'],
                                     'source': 'medication'},
                'mortalitySource': {'datefield': 'MonthOfDeath',
                                    'inputRegex': ['^personid', 'MonthOfDeath'],
                                    'insert': ["withColumn('dateofdeath', "
                                               "F.when(F.col('MonthOfDeath').isNotNull(), "
                                               "F.last_day('MonthOfDeath')).otherwise(F.col('MonthOfDeath')))"],
                                    'source': 'mortality'},
                'problem_listSource': {'datefield': 'datetimeProblem',
                                       'inputRegex': ['^personid',
                                                      '^encounterId',
                                                      '^problemlistid',
                                                      '^effectivedate$',
                                                      '^asserteddate$',
                                                      '^problemlistcode_standard_',
                                                      '^tenant$'],
                                       'insert': ["withColumn('datetimeProblem', "
                                                  "F.coalesce(F.to_timestamp(F.col('effectivedate')),F.to_timestamp(F.col('asserteddate'))))"],
                                       'source': 'problem_list'},
                'procedureSource': {'colsRename': {'serviceenddate': 'serviceenddateproc',
                                                   'servicestartdate': 'servicestartdateproc'},
                                    'datefield': 'datetimeProc',
                                    'inputRegex': ['^personid',
                                                   'encounterid',
                                                   'procedureid',
                                                   'servicestartdate',
                                                   'serviceenddate',
                                                   'procedurecode_standard_',
                                                   'principalprovider',
                                                   'active',
                                                   'source',
                                                   'tenant'],
                                    'insert': ['withColumn("datetimeProc", '
                                               'F.to_utc_timestamp(F.col("servicestartdate"), '
                                               '"UTC"))',
                                               'withColumn("dateProc", '
                                               'F.to_date(F.col("datetimeProc")))'],
                                    'source': 'procedure'},
                'procedureallSource': {'colsRename': {'serviceenddate': 'serviceenddateproc',
                                                      'servicestartdate': 'servicestartdateproc'},
                                       'datefield': 'datetimeProc',
                                       'inputRegex': ['^personid',
                                                      'encounterid',
                                                      'procedureid',
                                                      'servicestartdate',
                                                      'serviceenddate',
                                                      'procedurecode_standard_',
                                                      'modifiercodes_standard_',
                                                      'principalprovider',
                                                      'active',
                                                      'source',
                                                      'status_standard_',
                                                      'tenant'],
                                       'insert': ['withColumn("datetimeProc", '
                                                  'F.to_utc_timestamp(F.col("servicestartdate"), '
                                                  '"UTC"))',
                                                  'withColumn("dateProc", '
                                                  'F.to_date(F.col("datetimeProc")))'],
                                       'source': 'procedure'},
                'providerSource': {'inputRegex': ['providerid',
                                                  'cmsgrouping',
                                                  'cmsclassification',
                                                  'cmsspecialization',
                                                  'tenant'],
                                   'source': 'provider_demographics'},
                'races': {'inputRegex': ['personid',
                                         'races_standard_primaryDisplay'],
                          'source': 'demographics'}},
 'debug': True,
 'disease': 'SCD',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'project': 'SickleCell_AI',
 'schema': 'iuhealth_ed_data_cohort_202306',
 'schemaTag': 'RWD'}
2025-12-22 14:18:20,224 - lhn.item - INFO - Create name: mortalitySource from location: iuhealth_ed_data_cohort_202306.mortality
2025-12-22 14:18:20,484 - lhn.item - INFO - Using inputRegex: ['^personid', 'MonthOfDeath']
2025-12-22 14:18:20,493 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(MonthOfDeath,DateType,true)))
2025-12-22 14:18:20,499 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:20,520 - lhn.item - INFO - Create name: clinical_eventSource from location: iuhealth_ed_data_cohort_202306.clinical_event
2025-12-22 14:18:21,323 - lhn.item - INFO - Using inputRegex: ['^clincialeventid$', '^personid$', '^clinicaleventid$', '^encounterid$', '^clinicaleventcode_standard_id$', '^clinicaleventcode_standard_codingSystemId$', 'clinicaleventcode.standard.primaryDisplay', '^loincclass', '^type$', '^servicedate$', '^tenant$']
2025-12-22 14:18:21,379 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(clinicaleventid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(clinicaleventcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod,StructType(List(StructField(startDate,StringType,true),StructField(endDate,StringType,true))),true),StructField(typedvalue,StructType(List(StructField(type,StringType,true),StructField(textValue,StructType(List(StructField(value,StringType,true))),true),StructField(numericValue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(unitOfMeasure,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(codifiedValues,StructType(List(StructField(values,ArrayType(StructType(List(StructField(value,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true))),true),StructField(dateValue,StructType(List(StructField(date,StringType,true))),true))),true),StructField(interpretation,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(specimen,StructType(List(StructField(specimenType,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(bodySite,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(collectionDate,StringType,true),StructField(receivedDate,StringType,true))),true),StructField(measurementmethod,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(recordertype,StringType,true),StructField(requester,StringType,true),StructField(issueddate,StringType,true),StructField(referencerange,StructType(List(StructField(typedreferencelowvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true),StructField(typedreferencehighvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:21,420 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['clinicaleventcode']
2025-12-22 14:18:21,698 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(clinicaleventid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(clinicaleventcode_standard_id,StringType,true),StructField(clinicaleventcode_standard_codingSystemId,StringType,true),StructField(clinicaleventcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:21,959 - lhn.item - INFO - Create name: clinical_eventSourceAll from location: iuhealth_ed_data_cohort_202306.clinical_event
2025-12-22 14:18:21,980 - lhn.item - INFO - Using inputRegex: ['^clincialeventid$', '^personid$', '^clinicaleventid$', '^encounterid$', '^clinicaleventcode_standard_id$', '^clinicaleventcode_standard_codingSystemId$', 'clinicaleventcode.standard.primaryDisplay', '^loincclass', '^type$', '^servicedate$', '^typedvalue_type$', '^typedvalue_textValue.value$', '^typedvalue_numericValue_value$', '^typedvalue_numericValue_modifier$', '^typedvalue_unitOfMeasure_standard.id$', '^typedvalue_unitOfMeasure_standard_codingSystemId$', '^typedvalue_unitOfMeasure_standard_primaryDisplay$', '^typedvalue_unitOfMeasure_standardCodings_id$', '^typedvalue_unitOfMeasure_standardCodings_codingSystemId$', '^typedvalue_unitOfMeasure_standardCodings_primaryDisplay$', '^typedvalue_codifiedValues_values_value_standard_id$', '^typedvalue_codifiedValues_values_value_standard_codingSystemId$', '^typedvalue_codifiedValues_values_value_standard_primaryDisplay$', '^typedvalue_codifiedValues_values_value_standardCodings_id$', '^typedvalue_codifiedValues_values_value_standardCodings_codingSystemId$', '^typedvalue_codifiedValues_values_value_standardCodings_primaryDisplay$', '^typedvalue_dateValue_date$', '^interpretation_standard_id$', '^interpretation_standard_codingSystemId$', '^interpretation_standard_primaryDisplay$', '^status.standard_id$', '^status_standard_codingSystemId$', '^status_standard_primaryDisplay$', '^recordertype$', '^requester$', '^issueddate$', '^tenant$']
2025-12-22 14:18:22,026 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(clinicaleventid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(clinicaleventcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod,StructType(List(StructField(startDate,StringType,true),StructField(endDate,StringType,true))),true),StructField(typedvalue,StructType(List(StructField(type,StringType,true),StructField(textValue,StructType(List(StructField(value,StringType,true))),true),StructField(numericValue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(unitOfMeasure,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(codifiedValues,StructType(List(StructField(values,ArrayType(StructType(List(StructField(value,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true))),true),StructField(dateValue,StructType(List(StructField(date,StringType,true))),true))),true),StructField(interpretation,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(specimen,StructType(List(StructField(specimenType,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(bodySite,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(collectionDate,StringType,true),StructField(receivedDate,StringType,true))),true),StructField(measurementmethod,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(recordertype,StringType,true),StructField(requester,StringType,true),StructField(issueddate,StringType,true),StructField(referencerange,StructType(List(StructField(typedreferencelowvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true),StructField(typedreferencehighvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:22,067 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['clinicaleventcode', 'typedvalue', 'interpretation', 'status']
2025-12-22 14:18:22,357 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(clinicaleventid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(clinicaleventcode_standard_id,StringType,true),StructField(clinicaleventcode_standard_codingSystemId,StringType,true),StructField(clinicaleventcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(typedvalue_type,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_numericValue_modifier,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(typedvalue_unitOfMeasure_standard_codingSystemId,StringType,true),StructField(typedvalue_unitOfMeasure_standard_primaryDisplay,StringType,true),StructField(typedvalue_unitOfMeasure_standardCodings_id,ArrayType(StringType,true),true),StructField(typedvalue_unitOfMeasure_standardCodings_codingSystemId,ArrayType(StringType,true),true),StructField(typedvalue_unitOfMeasure_standardCodings_primaryDisplay,ArrayType(StringType,true),true),StructField(typedvalue_codifiedValues_values_value_standard_id,ArrayType(StringType,true),true),StructField(typedvalue_codifiedValues_values_value_standard_codingSystemId,ArrayType(StringType,true),true),StructField(typedvalue_codifiedValues_values_value_standard_primaryDisplay,ArrayType(StringType,true),true),StructField(typedvalue_dateValue_date,StringType,true),StructField(interpretation_standard_id,StringType,true),StructField(interpretation_standard_codingSystemId,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(status_standard_id,StringType,true),StructField(status_standard_codingSystemId,StringType,true),StructField(status_standard_primaryDisplay,StringType,true),StructField(recordertype,StringType,true),StructField(requester,StringType,true),StructField(issueddate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:22,639 - lhn.item - INFO - Create name: medicationSource from location: iuhealth_ed_data_cohort_202306.medication
2025-12-22 14:18:22,965 - lhn.item - INFO - Using inputRegex: ['^personid', '^encounterId', '^medicationid', '^drugCode_standard_id', '^drugCode_standard_codingSystemId', '^drugCode_standard_primaryDisplay', '^ingredients_drugCode_standard_id', '^ingredients_drugCode_standard_codingSystemId', '^ingredients_drugCode_standard_primaryDisplay', '^startDate', '^stopDate', 'prescribingprovider', '^tenant$']
2025-12-22 14:18:22,975 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(medicationid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(intendeddispenser,StringType,true),StructField(startdate,StringType,true),StructField(intendedadministrator,StringType,true),StructField(doseunit,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(stopdate,StringType,true),StructField(category,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(active,BooleanType,true),StructField(frequency,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(route,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(drugcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(prescribingprovider,StringType,true),StructField(dosequantity,StringType,true),StructField(source,StringType,true),StructField(asneeded,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:23,037 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['drugcode']
2025-12-22 14:18:23,185 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(medicationid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(startdate,StringType,true),StructField(stopdate,StringType,true),StructField(drugcode_standard_id,StringType,true),StructField(drugcode_standard_codingSystemId,StringType,true),StructField(drugcode_standard_primaryDisplay,StringType,true),StructField(prescribingprovider,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:23,356 - lhn.item - INFO - Create name: medicationIngbredientsSource from location: iuhealth_ed_data_cohort_202306.medication
2025-12-22 14:18:23,375 - lhn.item - INFO - Using inputRegex: ['^personid', '^encounterId', '^medicationid', '^ingredients_drugCode_standard_id', '^ingredients_drugCode_standard_codingSystemId', '^ingredients_drugCode_standard_primaryDisplay', '^startDate', '^stopDate', 'prescribingprovider', '^tenant$']
2025-12-22 14:18:23,380 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(medicationid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(intendeddispenser,StringType,true),StructField(startdate,StringType,true),StructField(intendedadministrator,StringType,true),StructField(doseunit,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(stopdate,StringType,true),StructField(category,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(active,BooleanType,true),StructField(frequency,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(route,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(drugcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(prescribingprovider,StringType,true),StructField(dosequantity,StringType,true),StructField(source,StringType,true),StructField(asneeded,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:23,433 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:23,591 - lhn.item - INFO - Create name: medicationDrugSource from location: iuhealth_ed_data_cohort_202306.medication
2025-12-22 14:18:23,609 - lhn.item - INFO - Using inputRegex: ['^personid', '^encounterId', '^medicationid', '^drugCode_standard_id', '^drugCode_standard_codingSystemId', '^drugCode_standard_primaryDisplay', '^startDate', '^stopDate', 'prescribingprovider', '^tenant$']
2025-12-22 14:18:23,621 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(medicationid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(intendeddispenser,StringType,true),StructField(startdate,StringType,true),StructField(intendedadministrator,StringType,true),StructField(doseunit,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(stopdate,StringType,true),StructField(category,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(active,BooleanType,true),StructField(frequency,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(route,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(drugcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(prescribingprovider,StringType,true),StructField(dosequantity,StringType,true),StructField(source,StringType,true),StructField(asneeded,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:23,677 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['drugcode']
2025-12-22 14:18:23,825 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(medicationid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(startdate,StringType,true),StructField(stopdate,StringType,true),StructField(drugcode_standard_id,StringType,true),StructField(drugcode_standard_codingSystemId,StringType,true),StructField(drugcode_standard_primaryDisplay,StringType,true),StructField(prescribingprovider,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:23,988 - lhn.item - INFO - Create name: medicationDatesSource from location: iuhealth_ed_data_cohort_202306.medication
2025-12-22 14:18:24,006 - lhn.item - INFO - Using inputRegex: ['^personid', '^encounterId', '^medicationid', '^startDate', '^stopDate', 'prescribingprovider', '^tenant$']
2025-12-22 14:18:24,008 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(medicationid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(intendeddispenser,StringType,true),StructField(startdate,StringType,true),StructField(intendedadministrator,StringType,true),StructField(doseunit,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(stopdate,StringType,true),StructField(category,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(active,BooleanType,true),StructField(frequency,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(route,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(drugcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(prescribingprovider,StringType,true),StructField(dosequantity,StringType,true),StructField(source,StringType,true),StructField(asneeded,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:24,075 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:24,236 - lhn.item - INFO - Create name: encounterSourceAll from location: iuhealth_ed_data_cohort_202306.encounter
2025-12-22 14:18:24,753 - lhn.item - INFO - Using inputRegex: ['^encounterid', '^personid', '^servicedate', '^dischargedate', '^financialclass_standard_primaryDisplay$', '^facilityids$', '^reasonforvisit_standard_primaryDisplay$', '^hospitalservice_standard_primaryDisplay$', '^classification_standard_id$', '^classification_standard_codingSystemId$', '^classification_standard_primaryDisplay$', '^encountertypes_classification_standard_primaryDisplay$', '^type_standard_primaryDisplay$', '^dischargedisposition_standard_primaryDisplay$', '^dischargetolocation_standard_primaryDisplay$', '^admissionsource_standard_primaryDisplay$', '^hospitalizationstartdate', '^readmission', '^admissiontype_standard_primaryDisplay$', '^status_standard_primaryDisplay^', '^actualarrivaldate', '^tenant$']
2025-12-22 14:18:24,806 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(facilityids,ArrayType(StringType,true),true),StructField(dischargedisposition,StringType,true),StructField(dischargetolocation,StringType,true),StructField(admissionsource,StringType,true),StructField(reasonforvisit,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(financialclass,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalservice,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalizationstartdate,StringType,true),StructField(readmission,BooleanType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(encountertypes,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(hospitalservices,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(service,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(locations,ArrayType(StructType(List(StructField(name,StringType,true),StructField(beginDate,StringType,true),StructField(endDate,StringType,true))),true),true),StructField(admissiontype,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(estimatedarrivaldate,StringType,true),StructField(estimateddeparturedate,StringType,true),StructField(actualarrivaldate,StringType,true),StructField(relatedproviders,ArrayType(StructType(List(StructField(providerId,StringType,true),StructField(type,StringType,true))),true),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:24,855 - lhn.spark_utils - INFO - Fields to flatten: arrays=['facilityids', 'encountertypes'], structs=['reasonforvisit', 'financialclass', 'hospitalservice', 'classification', 'type', 'admissiontype']
2025-12-22 14:18:25,205 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(facilityids,StringType,true),StructField(reasonforvisit_standard_primaryDisplay,StringType,true),StructField(financialclass_standard_primaryDisplay,StringType,true),StructField(hospitalservice_standard_primaryDisplay,StringType,true),StructField(classification_standard_id,StringType,true),StructField(classification_standard_codingSystemId,StringType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(type_standard_primaryDisplay,StringType,true),StructField(hospitalizationstartdate,StringType,true),StructField(readmission,BooleanType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(encountertypes_classification_standard_primaryDisplay,StringType,true),StructField(admissiontype_standard_primaryDisplay,StringType,true),StructField(actualarrivaldate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:25,578 - lhn.item - INFO - Create name: encounterSourceDates from location: iuhealth_ed_data_cohort_202306.encounter
2025-12-22 14:18:25,601 - lhn.item - INFO - Using inputRegex: ['^encounterid', '^personid', '^servicedate', '^dischargedate', '^classification_standard_primaryDisplay$', '^tenant$']
2025-12-22 14:18:25,646 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(facilityids,ArrayType(StringType,true),true),StructField(dischargedisposition,StringType,true),StructField(dischargetolocation,StringType,true),StructField(admissionsource,StringType,true),StructField(reasonforvisit,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(financialclass,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalservice,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalizationstartdate,StringType,true),StructField(readmission,BooleanType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(encountertypes,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(hospitalservices,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(service,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(locations,ArrayType(StructType(List(StructField(name,StringType,true),StructField(beginDate,StringType,true),StructField(endDate,StringType,true))),true),true),StructField(admissiontype,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(estimatedarrivaldate,StringType,true),StructField(estimateddeparturedate,StringType,true),StructField(actualarrivaldate,StringType,true),StructField(relatedproviders,ArrayType(StructType(List(StructField(providerId,StringType,true),StructField(type,StringType,true))),true),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:25,709 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['classification']
2025-12-22 14:18:26,044 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:26,429 - lhn.item - INFO - Create name: encounterSource from location: iuhealth_ed_data_cohort_202306.encounter
2025-12-22 14:18:26,461 - lhn.item - INFO - Using inputRegex: ['^encounterid', '^personid', '^servicedate', '^dischargedate', '^financialclass_standard_primaryDisplay$', '^facilityids$', '^hospitalservice_standard_primaryDisplay$', '^classification_standard_id$', '^classification_standard_codingSystemId$', '^classification_standard_primaryDisplay$', '^type_standard_primaryDisplay$', '^status_standard_primaryDisplay^', '^actualarrivaldate', '^tenant$']
2025-12-22 14:18:26,506 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(facilityids,ArrayType(StringType,true),true),StructField(dischargedisposition,StringType,true),StructField(dischargetolocation,StringType,true),StructField(admissionsource,StringType,true),StructField(reasonforvisit,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(financialclass,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalservice,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalizationstartdate,StringType,true),StructField(readmission,BooleanType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(encountertypes,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(hospitalservices,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(service,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(locations,ArrayType(StructType(List(StructField(name,StringType,true),StructField(beginDate,StringType,true),StructField(endDate,StringType,true))),true),true),StructField(admissiontype,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(estimatedarrivaldate,StringType,true),StructField(estimateddeparturedate,StringType,true),StructField(actualarrivaldate,StringType,true),StructField(relatedproviders,ArrayType(StructType(List(StructField(providerId,StringType,true),StructField(type,StringType,true))),true),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:26,552 - lhn.spark_utils - INFO - Fields to flatten: arrays=['facilityids'], structs=['financialclass', 'hospitalservice', 'classification', 'type']
2025-12-22 14:18:26,954 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(facilityids,StringType,true),StructField(financialclass_standard_primaryDisplay,StringType,true),StructField(hospitalservice_standard_primaryDisplay,StringType,true),StructField(classification_standard_id,StringType,true),StructField(classification_standard_codingSystemId,StringType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(type_standard_primaryDisplay,StringType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(actualarrivaldate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:27,328 - lhn.item - INFO - Create name: emergencyEncounter from location: iuhealth_ed_data_cohort_202306.encounter
2025-12-22 14:18:27,348 - lhn.item - INFO - Using inputRegex: ['^encounterid', '^personid', '^servicedate', '^dischargedate', '^classification_standard_id', '^classification_standard_primaryDisplay', '^tenant$']
2025-12-22 14:18:27,395 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(facilityids,ArrayType(StringType,true),true),StructField(dischargedisposition,StringType,true),StructField(dischargetolocation,StringType,true),StructField(admissionsource,StringType,true),StructField(reasonforvisit,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(financialclass,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalservice,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalizationstartdate,StringType,true),StructField(readmission,BooleanType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(encountertypes,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(hospitalservices,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(service,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(locations,ArrayType(StructType(List(StructField(name,StringType,true),StructField(beginDate,StringType,true),StructField(endDate,StringType,true))),true),true),StructField(admissiontype,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(estimatedarrivaldate,StringType,true),StructField(estimateddeparturedate,StringType,true),StructField(actualarrivaldate,StringType,true),StructField(relatedproviders,ArrayType(StructType(List(StructField(providerId,StringType,true),StructField(type,StringType,true))),true),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:27,459 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['classification']
2025-12-22 14:18:27,794 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(classification_standard_id,StringType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:28,140 - lhn.item - INFO - Create name: EncounterInsuranceSource from location: iuhealth_ed_data_cohort_202306.encounter
2025-12-22 14:18:28,160 - lhn.item - INFO - Using inputRegex: ['^encounterid', '^personid', '^servicedate', '^dischargedate', '^classification_standard_primaryDisplay', '^financialclass_standard_primaryDisplay', '^tenant$']
2025-12-22 14:18:28,206 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(facilityids,ArrayType(StringType,true),true),StructField(dischargedisposition,StringType,true),StructField(dischargetolocation,StringType,true),StructField(admissionsource,StringType,true),StructField(reasonforvisit,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(financialclass,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalservice,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalizationstartdate,StringType,true),StructField(readmission,BooleanType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(encountertypes,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(hospitalservices,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(service,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(locations,ArrayType(StructType(List(StructField(name,StringType,true),StructField(beginDate,StringType,true),StructField(endDate,StringType,true))),true),true),StructField(admissiontype,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(estimatedarrivaldate,StringType,true),StructField(estimateddeparturedate,StringType,true),StructField(actualarrivaldate,StringType,true),StructField(relatedproviders,ArrayType(StructType(List(StructField(providerId,StringType,true),StructField(type,StringType,true))),true),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:28,265 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['financialclass', 'classification']
2025-12-22 14:18:28,616 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(financialclass_standard_primaryDisplay,StringType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:28,957 - lhn.item - INFO - Create name: inpatientEncounterSource from location: iuhealth_ed_data_cohort_202306.encounter
2025-12-22 14:18:28,977 - lhn.item - INFO - Using inputRegex: ['^encounterid', '^personid', '^servicedate', '^dischargedate', '^classification_standard_id', '^tenant$']
2025-12-22 14:18:29,022 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(facilityids,ArrayType(StringType,true),true),StructField(dischargedisposition,StringType,true),StructField(dischargetolocation,StringType,true),StructField(admissionsource,StringType,true),StructField(reasonforvisit,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(financialclass,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalservice,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(hospitalizationstartdate,StringType,true),StructField(readmission,BooleanType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(encountertypes,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(hospitalservices,ArrayType(StructType(List(StructField(beginDate,StringType,true),StructField(endDate,StringType,true),StructField(service,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true),StructField(locations,ArrayType(StructType(List(StructField(name,StringType,true),StructField(beginDate,StringType,true),StructField(endDate,StringType,true))),true),true),StructField(admissiontype,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(estimatedarrivaldate,StringType,true),StructField(estimateddeparturedate,StringType,true),StructField(actualarrivaldate,StringType,true),StructField(relatedproviders,ArrayType(StructType(List(StructField(providerId,StringType,true),StructField(type,StringType,true))),true),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:29,083 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['classification']
2025-12-22 14:18:29,418 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(classification_standard_id,StringType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:29,790 - lhn.item - INFO - Create name: conditionSource from location: iuhealth_ed_data_cohort_202306.condition
2025-12-22 14:18:30,073 - lhn.item - INFO - Using inputRegex: ['^personid', '^personId$', '^encounterId', '^conditionId', '^effectiveDate', '^asserteddate$', '^conditionCode_standard_', '^billingrank', '^classification_standard_primaryDisplay', 'confirmationstatus_standard_primaryDisplay', '^responsibleprovider', '^tenant$']
2025-12-22 14:18:30,082 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(conditioncode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(managementdisciplines,ArrayType(StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),true),StructField(confirmationstatus,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(responsibleprovider,StringType,true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(statusdate,StringType,true),StructField(billingrank,StringType,true),StructField(presentonadmission,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:30,116 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['conditioncode', 'classification', 'confirmationstatus']
2025-12-22 14:18:30,299 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(confirmationstatus_standard_primaryDisplay,StringType,true),StructField(responsibleprovider,StringType,true),StructField(billingrank,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:30,488 - lhn.item - INFO - Create name: conditionFinalSource from location: iuhealth_ed_data_cohort_202306.condition
2025-12-22 14:18:30,507 - lhn.item - INFO - Using inputRegex: ['^personid', '^encounterId', '^conditionId', '^effectiveDate', '^asserteddate$', '^conditionCode_standard_', '^billingrank', '^classification_standard_primaryDisplay', 'confirmationstatus_standard_primaryDisplay', '^responsibleprovider', '^tenant$']
2025-12-22 14:18:30,509 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(conditioncode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(managementdisciplines,ArrayType(StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),true),StructField(confirmationstatus,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(responsibleprovider,StringType,true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(statusdate,StringType,true),StructField(billingrank,StringType,true),StructField(presentonadmission,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:30,541 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['conditioncode', 'classification', 'confirmationstatus']
2025-12-22 14:18:30,726 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(confirmationstatus_standard_primaryDisplay,StringType,true),StructField(responsibleprovider,StringType,true),StructField(billingrank,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:30,931 - lhn.item - INFO - Create name: conditionFinalConfirmedSource from location: iuhealth_ed_data_cohort_202306.condition
2025-12-22 14:18:30,949 - lhn.item - INFO - Using inputRegex: ['^personid', '^encounterId', '^conditionId', '^effectiveDate', '^asserteddate$', '^conditionCode_standard_', '^billingrank', '^classification_standard_primaryDisplay', 'confirmationstatus_standard_primaryDisplay', '^responsibleprovider', '^tenant$']
2025-12-22 14:18:30,951 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(conditioncode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(type,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(classification,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(managementdisciplines,ArrayType(StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),true),StructField(confirmationstatus,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(responsibleprovider,StringType,true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(statusdate,StringType,true),StructField(billingrank,StringType,true),StructField(presentonadmission,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:30,982 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['conditioncode', 'classification', 'confirmationstatus']
2025-12-22 14:18:31,164 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(confirmationstatus_standard_primaryDisplay,StringType,true),StructField(responsibleprovider,StringType,true),StructField(billingrank,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:31,368 - lhn.item - INFO - Create name: problem_listSource from location: iuhealth_ed_data_cohort_202306.problem_list
2025-12-22 14:18:31,539 - lhn.item - INFO - Using inputRegex: ['^personid', '^encounterId', '^problemlistid', '^effectivedate$', '^asserteddate$', '^problemlistcode_standard_', '^tenant$']
2025-12-22 14:18:31,548 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(problemlistid,StringType,true),StructField(personid,StringType,true),StructField(problemlistcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(confirmationstatus,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:31,565 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['problemlistcode']
2025-12-22 14:18:31,638 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(problemlistid,StringType,true),StructField(personid,StringType,true),StructField(problemlistcode_standard_id,StringType,true),StructField(problemlistcode_standard_codingSystemId,StringType,true),StructField(problemlistcode_standard_primaryDisplay,StringType,true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:31,717 - lhn.item - INFO - Create name: measurmentIndexSource from location: iuhealth_ed_data_cohort_202306.measurement
2025-12-22 14:18:32,472 - lhn.item - INFO - Using inputRegex: ['^personid', '^encounterId', '^measurementid', '^measurementcode_standard_id', '^measurementcode_standard_codingSystemId', '^measurementcode_standard_primaryDisplay', '^servicedate', '^tenant$']
2025-12-22 14:18:32,527 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(measurementid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(measurementcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod,StructType(List(StructField(startDate,StringType,true),StructField(endDate,StringType,true))),true),StructField(typedvalue,StructType(List(StructField(type,StringType,true),StructField(textValue,StructType(List(StructField(value,StringType,true))),true),StructField(numericValue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(unitOfMeasure,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(codifiedValues,StructType(List(StructField(values,ArrayType(StructType(List(StructField(value,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true))),true),StructField(dateValue,StructType(List(StructField(date,StringType,true))),true))),true),StructField(interpretation,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(specimen,StructType(List(StructField(specimenType,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(bodySite,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(collectionDate,StringType,true),StructField(receivedDate,StringType,true))),true),StructField(measurementmethod,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(recordertype,StringType,true),StructField(requester,StringType,true),StructField(issueddate,StringType,true),StructField(referencerange,StructType(List(StructField(typedreferencelowvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true),StructField(typedreferencehighvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:32,563 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['measurementcode']
2025-12-22 14:18:32,808 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(measurementid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(measurementcode_standard_id,StringType,true),StructField(measurementcode_standard_codingSystemId,StringType,true),StructField(measurementcode_standard_primaryDisplay,StringType,true),StructField(servicedate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:33,063 - lhn.item - INFO - Create name: measurementMinimalSource from location: iuhealth_ed_data_cohort_202306.measurement
2025-12-22 14:18:33,082 - lhn.item - INFO - Using inputRegex: ['^personid', '^encounterId', '^measurementid', '^measurementcode_standard_id', '^measurementcode_standard_codingSystemId', '^measurementcode_standard_primaryDisplay', '^typedvalue_numericValue_value', '^typedvalue_unitOfMeasure_standard_id', "^interpretation_standard_primaryDisplay$'", '^loincclass', '^servicedate', '^tenant$']
2025-12-22 14:18:33,127 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(measurementid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(measurementcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod,StructType(List(StructField(startDate,StringType,true),StructField(endDate,StringType,true))),true),StructField(typedvalue,StructType(List(StructField(type,StringType,true),StructField(textValue,StructType(List(StructField(value,StringType,true))),true),StructField(numericValue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(unitOfMeasure,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(codifiedValues,StructType(List(StructField(values,ArrayType(StructType(List(StructField(value,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true))),true),StructField(dateValue,StructType(List(StructField(date,StringType,true))),true))),true),StructField(interpretation,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(specimen,StructType(List(StructField(specimenType,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(bodySite,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(collectionDate,StringType,true),StructField(receivedDate,StringType,true))),true),StructField(measurementmethod,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(recordertype,StringType,true),StructField(requester,StringType,true),StructField(issueddate,StringType,true),StructField(referencerange,StructType(List(StructField(typedreferencelowvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true),StructField(typedreferencehighvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:33,162 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['measurementcode', 'typedvalue']
2025-12-22 14:18:33,412 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(measurementid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(measurementcode_standard_id,StringType,true),StructField(measurementcode_standard_codingSystemId,StringType,true),StructField(measurementcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(servicedate,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:33,664 - lhn.item - INFO - Create name: measurementSource from location: iuhealth_ed_data_cohort_202306.measurement
2025-12-22 14:18:33,683 - lhn.item - INFO - Using inputRegex: ['^measurementid', '^encounterid', '^personid', '^measurementcode_standard_id$', '^measurementcode_standard_codingSystemId$', '^measurementcode_standard_primaryDisplay$', 'loincclass', '^typedvalue_numericValue_value$', '^typedvalue_unitOfMeasure_standard_id$', '^interpretation_standard_primaryDisplay$', '^loincclass$', '^servicedate$', '^tenant$']
2025-12-22 14:18:33,731 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(measurementid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(measurementcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod,StructType(List(StructField(startDate,StringType,true),StructField(endDate,StringType,true))),true),StructField(typedvalue,StructType(List(StructField(type,StringType,true),StructField(textValue,StructType(List(StructField(value,StringType,true))),true),StructField(numericValue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(unitOfMeasure,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(codifiedValues,StructType(List(StructField(values,ArrayType(StructType(List(StructField(value,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true))),true),StructField(dateValue,StructType(List(StructField(date,StringType,true))),true))),true),StructField(interpretation,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(specimen,StructType(List(StructField(specimenType,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(bodySite,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(collectionDate,StringType,true),StructField(receivedDate,StringType,true))),true),StructField(measurementmethod,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(recordertype,StringType,true),StructField(requester,StringType,true),StructField(issueddate,StringType,true),StructField(referencerange,StructType(List(StructField(typedreferencelowvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true),StructField(typedreferencehighvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:33,766 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['measurementcode', 'typedvalue', 'interpretation']
2025-12-22 14:18:34,015 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(measurementid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(measurementcode_standard_id,StringType,true),StructField(measurementcode_standard_codingSystemId,StringType,true),StructField(measurementcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(servicedate,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:34,270 - lhn.item - INFO - Create name: measurementSource2 from location: iuhealth_ed_data_cohort_202306.measurement
2025-12-22 14:18:34,289 - lhn.item - INFO - Using inputRegex: ['^measurementid', '^encounterid', '^personid', '^measurementcode_standard_id$', '^measurementcode_standard_codingSystemId$', '^measurementcode_standard_primaryDisplay$', 'loincclass', '^typedvalue_type^', '^typedvalue_numericValue_value$', '^typedvalue_textValue_value$', '^typedvalue_numericValue_modifier$', '^typedvalue_unitOfMeasure_standard_id$', '^typedvalue_unitOfMeasure_standard_codingSystemId$', '^typedvalue_unitOfMeasure_standard_primaryDisplay$', '^interpretation_standard_codingSystemId$', '^interpretation_standard_primaryDisplay$', '^loincclass$', '^issueddate$', '^source$', '^active$', '^type$', '^servicedate$', 'serviceperiod_startDate$', 'serviceperiod_endDate$', '^tenant$']
2025-12-22 14:18:34,336 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(measurementid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(measurementcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod,StructType(List(StructField(startDate,StringType,true),StructField(endDate,StringType,true))),true),StructField(typedvalue,StructType(List(StructField(type,StringType,true),StructField(textValue,StructType(List(StructField(value,StringType,true))),true),StructField(numericValue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(unitOfMeasure,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(codifiedValues,StructType(List(StructField(values,ArrayType(StructType(List(StructField(value,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true))),true),StructField(dateValue,StructType(List(StructField(date,StringType,true))),true))),true),StructField(interpretation,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(specimen,StructType(List(StructField(specimenType,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(bodySite,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(collectionDate,StringType,true),StructField(receivedDate,StringType,true))),true),StructField(measurementmethod,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(recordertype,StringType,true),StructField(requester,StringType,true),StructField(issueddate,StringType,true),StructField(referencerange,StructType(List(StructField(typedreferencelowvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true),StructField(typedreferencehighvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:34,373 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['measurementcode', 'serviceperiod', 'typedvalue', 'interpretation']
2025-12-22 14:18:34,638 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(measurementid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(measurementcode_standard_id,StringType,true),StructField(measurementcode_standard_codingSystemId,StringType,true),StructField(measurementcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod_startDate,StringType,true),StructField(serviceperiod_endDate,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_numericValue_modifier,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(typedvalue_unitOfMeasure_standard_codingSystemId,StringType,true),StructField(typedvalue_unitOfMeasure_standard_primaryDisplay,StringType,true),StructField(interpretation_standard_codingSystemId,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(issueddate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:34,904 - lhn.item - INFO - Create name: labSource from location: iuhealth_ed_data_cohort_202306.lab
2025-12-22 14:18:35,528 - lhn.item - INFO - Using inputRegex: ['^labid', '^encounterid', '^personid', '^labcode_standard_id$', '^labcode_standard_codingSystemId$', '^labcode_standard_primaryDisplay$', '^typedvalue_type$', '^typedvalue_numericValue_value$', '^typedvalue_textValue_value$', '^typedvalue_numericValue_modifier$', '^typedvalue_unitOfMeasure_standard_id$', '^typedvalue_unitOfMeasure_standard_codingSystemId$', '^typedvalue_unitOfMeasure_standard_primaryDisplay$', '^interpretation_standard_codingSystemId$', '^interpretation_standard_primaryDisplay$', '^loincclass$', '^issueddate$', '^source$', '^active$', '^type$', '^servicedate$', 'serviceperiod_startDate$', 'serviceperiod_endDate$', '^tenant$']
2025-12-22 14:18:35,582 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(labcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod,StructType(List(StructField(startDate,StringType,true),StructField(endDate,StringType,true))),true),StructField(typedvalue,StructType(List(StructField(type,StringType,true),StructField(textValue,StructType(List(StructField(value,StringType,true))),true),StructField(numericValue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(unitOfMeasure,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(codifiedValues,StructType(List(StructField(values,ArrayType(StructType(List(StructField(value,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true))),true),StructField(dateValue,StructType(List(StructField(date,StringType,true))),true))),true),StructField(interpretation,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(specimen,StructType(List(StructField(specimenType,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(bodySite,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(collectionDate,StringType,true),StructField(receivedDate,StringType,true))),true),StructField(measurementmethod,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(recordertype,StringType,true),StructField(requester,StringType,true),StructField(issueddate,StringType,true),StructField(referencerange,StructType(List(StructField(typedreferencelowvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true),StructField(typedreferencehighvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:35,618 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['labcode', 'serviceperiod', 'typedvalue', 'interpretation']
2025-12-22 14:18:35,881 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod_startDate,StringType,true),StructField(serviceperiod_endDate,StringType,true),StructField(typedvalue_type,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_numericValue_modifier,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(typedvalue_unitOfMeasure_standard_codingSystemId,StringType,true),StructField(typedvalue_unitOfMeasure_standard_primaryDisplay,StringType,true),StructField(interpretation_standard_codingSystemId,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(issueddate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:36,150 - lhn.item - INFO - Renaming Column names: {'servicestartdate': 'servicedatestartLab'}
2025-12-22 14:18:36,151 - lhn.item - INFO - Create name: labsmallSource from location: iuhealth_ed_data_cohort_202306.lab
2025-12-22 14:18:36,170 - lhn.item - INFO - Using inputRegex: ['^labid', '^encounterid', '^personid', '^labcode_standard_id$', '^labcode_standard_codingSystemId$', '^labcode_standard_primaryDisplay$', '^typedvalue_numericValue_value$', '^typedvalue_textValue_value$', '^typedvalue_unitOfMeasure_standard_id$', '^interpretation_standard_primaryDisplay$', '^loincclass$', '^servicedate$', '^tenant$']
2025-12-22 14:18:36,218 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(labcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod,StructType(List(StructField(startDate,StringType,true),StructField(endDate,StringType,true))),true),StructField(typedvalue,StructType(List(StructField(type,StringType,true),StructField(textValue,StructType(List(StructField(value,StringType,true))),true),StructField(numericValue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(unitOfMeasure,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(codifiedValues,StructType(List(StructField(values,ArrayType(StructType(List(StructField(value,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true))),true),StructField(dateValue,StructType(List(StructField(date,StringType,true))),true))),true),StructField(interpretation,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(specimen,StructType(List(StructField(specimenType,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(bodySite,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(collectionDate,StringType,true),StructField(receivedDate,StringType,true))),true),StructField(measurementmethod,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(recordertype,StringType,true),StructField(requester,StringType,true),StructField(issueddate,StringType,true),StructField(referencerange,StructType(List(StructField(typedreferencelowvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true),StructField(typedreferencehighvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:36,254 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['labcode', 'typedvalue', 'interpretation']
2025-12-22 14:18:36,521 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(servicedate,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:36,771 - lhn.item - INFO - Create name: labsmallRangeSource from location: iuhealth_ed_data_cohort_202306.lab
2025-12-22 14:18:36,791 - lhn.item - INFO - Using inputRegex: ['^labid', '^encounterid', '^personid', '^labcode_standard_id$', '^labcode_standard_codingSystemId$', '^labcode_standard_primaryDisplay$', '^typedvalue_numericValue_value$', '^typedvalue_textValue_value$', '^typedvalue_unitOfMeasure_standard_id$', '^interpretation_standard_primaryDisplay$', '^referencerange_typedreferencelowvalue_numericvalue.value', '^referencerange_typedreferencehighvalue_numericvalue_value', '^loincclass$', '^servicedate$', '^tenant$']
2025-12-22 14:18:36,840 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(labcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod,StructType(List(StructField(startDate,StringType,true),StructField(endDate,StringType,true))),true),StructField(typedvalue,StructType(List(StructField(type,StringType,true),StructField(textValue,StructType(List(StructField(value,StringType,true))),true),StructField(numericValue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(unitOfMeasure,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(codifiedValues,StructType(List(StructField(values,ArrayType(StructType(List(StructField(value,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true))),true),StructField(dateValue,StructType(List(StructField(date,StringType,true))),true))),true),StructField(interpretation,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(specimen,StructType(List(StructField(specimenType,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(bodySite,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(collectionDate,StringType,true),StructField(receivedDate,StringType,true))),true),StructField(measurementmethod,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(recordertype,StringType,true),StructField(requester,StringType,true),StructField(issueddate,StringType,true),StructField(referencerange,StructType(List(StructField(typedreferencelowvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true),StructField(typedreferencehighvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:36,874 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['labcode', 'typedvalue', 'interpretation', 'referencerange']
2025-12-22 14:18:37,124 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(servicedate,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(referencerange_typedreferencelowvalue_numericvalue_value,StringType,true),StructField(referencerange_typedreferencehighvalue_numericvalue_value,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:37,383 - lhn.item - INFO - Create name: labsIndexSource from location: iuhealth_ed_data_cohort_202306.lab
2025-12-22 14:18:37,404 - lhn.item - INFO - Using inputRegex: ['^labid', '^encounterid', '^personid', '^labcode_standard_id$', '^labcode_standard_codingSystemId$', '^labcode_standard_primaryDisplay$', '^servicedate$', '^tenant$']
2025-12-22 14:18:37,450 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(labcode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod,StructType(List(StructField(startDate,StringType,true),StructField(endDate,StringType,true))),true),StructField(typedvalue,StructType(List(StructField(type,StringType,true),StructField(textValue,StructType(List(StructField(value,StringType,true))),true),StructField(numericValue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(unitOfMeasure,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(codifiedValues,StructType(List(StructField(values,ArrayType(StructType(List(StructField(value,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true))),true),true))),true),StructField(dateValue,StructType(List(StructField(date,StringType,true))),true))),true),StructField(interpretation,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(specimen,StructType(List(StructField(specimenType,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(bodySite,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(collectionDate,StringType,true),StructField(receivedDate,StringType,true))),true),StructField(measurementmethod,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(recordertype,StringType,true),StructField(requester,StringType,true),StructField(issueddate,StringType,true),StructField(referencerange,StructType(List(StructField(typedreferencelowvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true),StructField(typedreferencehighvalue,StructType(List(StructField(type,StringType,true),StructField(numericvalue,StructType(List(StructField(value,StringType,true),StructField(modifier,StringType,true))),true),StructField(textvalue,StringType,true))),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:37,483 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['labcode']
2025-12-22 14:18:37,738 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(servicedate,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:37,990 - lhn.item - INFO - Create name: procedureSource from location: iuhealth_ed_data_cohort_202306.procedure
2025-12-22 14:18:38,183 - lhn.item - INFO - Using inputRegex: ['^personid', 'encounterid', 'procedureid', 'servicestartdate', 'serviceenddate', 'procedurecode_standard_', 'principalprovider', 'active', 'source', 'tenant']
2025-12-22 14:18:38,192 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(procedureid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(procedurecode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(modifiercodes,ArrayType(StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),true),StructField(servicestartdate,StringType,true),StructField(serviceenddate,StringType,true),StructField(principalprovider,StringType,true),StructField(billingrank,StringType,true),StructField(active,BooleanType,true),StructField(source,StringType,true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:38,211 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['procedurecode']
2025-12-22 14:18:38,330 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(procedureid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(procedurecode_standard_id,StringType,true),StructField(procedurecode_standard_codingSystemId,StringType,true),StructField(procedurecode_standard_primaryDisplay,StringType,true),StructField(servicestartdate,StringType,true),StructField(serviceenddate,StringType,true),StructField(principalprovider,StringType,true),StructField(active,BooleanType,true),StructField(source,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:38,453 - lhn.item - INFO - Renaming Column names: {'servicestartdate': 'servicestartdateproc', 'serviceenddate': 'serviceenddateproc'}
2025-12-22 14:18:38,455 - lhn.item - INFO - Create name: procedureallSource from location: iuhealth_ed_data_cohort_202306.procedure
2025-12-22 14:18:38,472 - lhn.item - INFO - Using inputRegex: ['^personid', 'encounterid', 'procedureid', 'servicestartdate', 'serviceenddate', 'procedurecode_standard_', 'modifiercodes_standard_', 'principalprovider', 'active', 'source', 'status_standard_', 'tenant']
2025-12-22 14:18:38,474 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(procedureid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(procedurecode,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(modifiercodes,ArrayType(StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),true),StructField(servicestartdate,StringType,true),StructField(serviceenddate,StringType,true),StructField(principalprovider,StringType,true),StructField(billingrank,StringType,true),StructField(active,BooleanType,true),StructField(source,StringType,true),StructField(status,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:38,495 - lhn.spark_utils - INFO - Fields to flatten: arrays=['modifiercodes'], structs=['procedurecode', 'status']
2025-12-22 14:18:38,592 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(procedureid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(procedurecode_standard_id,StringType,true),StructField(procedurecode_standard_codingSystemId,StringType,true),StructField(procedurecode_standard_primaryDisplay,StringType,true),StructField(modifiercodes_standard_id,StringType,true),StructField(modifiercodes_standard_codingSystemId,StringType,true),StructField(modifiercodes_standard_primaryDisplay,StringType,true),StructField(servicestartdate,StringType,true),StructField(serviceenddate,StringType,true),StructField(principalprovider,StringType,true),StructField(active,BooleanType,true),StructField(source,StringType,true),StructField(status_standard_id,StringType,true),StructField(status_standard_codingSystemId,StringType,true),StructField(status_standard_primaryDisplay,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:38,723 - lhn.item - INFO - Renaming Column names: {'servicestartdate': 'servicestartdateproc', 'serviceenddate': 'serviceenddateproc'}
2025-12-22 14:18:38,726 - lhn.item - INFO - Create name: demoSource from location: iuhealth_ed_data_cohort_202306.demographics
2025-12-22 14:18:38,942 - lhn.item - INFO - Using inputRegex: ['personid', 'yearofbirth', 'dateofdeath', 'gender', '^maritalstatus_standard_primaryDisplay$', 'races', 'ethnicities', 'deceased', 'state', 'testpatientflag', 'source$', 'tenant', '^gender_standard_primaryDisplay$', '^birthsex_standard_primaryDisplay$', '^races_standard_primaryDisplay$', '^ethnicities_standard_primaryDisplay$', 'zip_code']
2025-12-22 14:18:38,950 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(yearofbirth,StringType,true),StructField(dateofdeath,StringType,true),StructField(gender,StringType,true),StructField(maritalstatus,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(races,ArrayType(StringType,true),true),StructField(ethnicities,ArrayType(StringType,true),true),StructField(deceased,BooleanType,true),StructField(source,StringType,true),StructField(state,StringType,true),StructField(testpatientflag,BooleanType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:38,972 - lhn.spark_utils - INFO - Fields to flatten: arrays=['races', 'ethnicities'], structs=['maritalstatus']
2025-12-22 14:18:39,041 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(personid,StringType,true),StructField(yearofbirth,StringType,true),StructField(dateofdeath,StringType,true),StructField(gender,StringType,true),StructField(maritalstatus_standard_primaryDisplay,StringType,true),StructField(races,StringType,true),StructField(ethnicities,StringType,true),StructField(deceased,BooleanType,true),StructField(source,StringType,true),StructField(state,StringType,true),StructField(testpatientflag,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:39,107 - lhn.item - INFO - Create name: demoPreferred from location: iuhealth_ed_data_cohort_202306.preferred_demographics
2025-12-22 14:18:39,267 - lhn.item - INFO - Using inputRegex: ['personid', 'prefgender$', 'prefrace$', 'prefethnicity$', 'prefyearofbirth$', 'prefstate', 'prefmetropolitan', 'prefurban', 'prefzip', 'tenant', 'testpatientflag']
2025-12-22 14:18:39,276 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(prefgender,StringType,true),StructField(prefrace,StringType,true),StructField(prefracereason,StringType,true),StructField(prefethnicity,StringType,true),StructField(prefethnicityreason,StringType,true),StructField(prefyearofbirth,StringType,true),StructField(prefstate,StringType,true),StructField(prefmetropolitan,StringType,true),StructField(prefurban,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:39,306 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:39,348 - lhn.item - INFO - Renaming Column names: {'prefgender': 'gender', 'prefrace': 'race', 'prefethnicity': 'ethnicity', 'prefyearofbirth': 'yearofbirth', 'prefstate': 'state', 'prefmetropolitan': 'metropolitan', 'prefurban': 'urban', 'prefzip': 'zip', 'prefbirthdate': 'birthdate'}
2025-12-22 14:18:39,355 - lhn.item - INFO - Create name: ethnicities from location: iuhealth_ed_data_cohort_202306.demographics
2025-12-22 14:18:39,370 - lhn.item - INFO - Using inputRegex: ['personid', 'ethnicities_standard_primaryDisplay']
2025-12-22 14:18:39,371 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(yearofbirth,StringType,true),StructField(dateofdeath,StringType,true),StructField(gender,StringType,true),StructField(maritalstatus,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(races,ArrayType(StringType,true),true),StructField(ethnicities,ArrayType(StringType,true),true),StructField(deceased,BooleanType,true),StructField(source,StringType,true),StructField(state,StringType,true),StructField(testpatientflag,BooleanType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:39,404 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:39,452 - lhn.item - INFO - Create name: birthsex from location: iuhealth_ed_data_cohort_202306.demographics
2025-12-22 14:18:39,467 - lhn.item - INFO - Using inputRegex: ['personid', 'birthsex_standard_primaryDisplay']
2025-12-22 14:18:39,478 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(yearofbirth,StringType,true),StructField(dateofdeath,StringType,true),StructField(gender,StringType,true),StructField(maritalstatus,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(races,ArrayType(StringType,true),true),StructField(ethnicities,ArrayType(StringType,true),true),StructField(deceased,BooleanType,true),StructField(source,StringType,true),StructField(state,StringType,true),StructField(testpatientflag,BooleanType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:39,508 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:39,558 - lhn.item - INFO - Create name: maritalstatus from location: iuhealth_ed_data_cohort_202306.demographics
2025-12-22 14:18:39,573 - lhn.item - INFO - Using inputRegex: ['personid', 'maritalstatus_standard_primaryDisplay']
2025-12-22 14:18:39,581 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(yearofbirth,StringType,true),StructField(dateofdeath,StringType,true),StructField(gender,StringType,true),StructField(maritalstatus,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(races,ArrayType(StringType,true),true),StructField(ethnicities,ArrayType(StringType,true),true),StructField(deceased,BooleanType,true),StructField(source,StringType,true),StructField(state,StringType,true),StructField(testpatientflag,BooleanType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:39,601 - lhn.spark_utils - INFO - Fields to flatten: arrays=[], structs=['maritalstatus']
2025-12-22 14:18:39,649 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(personid,StringType,true),StructField(maritalstatus_standard_primaryDisplay,StringType,true)))
2025-12-22 14:18:39,699 - lhn.item - INFO - Create name: races from location: iuhealth_ed_data_cohort_202306.demographics
2025-12-22 14:18:39,714 - lhn.item - INFO - Using inputRegex: ['personid', 'races_standard_primaryDisplay']
2025-12-22 14:18:39,716 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(yearofbirth,StringType,true),StructField(dateofdeath,StringType,true),StructField(gender,StringType,true),StructField(maritalstatus,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(races,ArrayType(StringType,true),true),StructField(ethnicities,ArrayType(StringType,true),true),StructField(deceased,BooleanType,true),StructField(source,StringType,true),StructField(state,StringType,true),StructField(testpatientflag,BooleanType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:39,749 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:39,795 - lhn.item - INFO - Create name: gender from location: iuhealth_ed_data_cohort_202306.demographics
2025-12-22 14:18:39,810 - lhn.item - INFO - Using inputRegex: ['personid', 'gender_standard_primaryDisplay']
2025-12-22 14:18:39,836 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(yearofbirth,StringType,true),StructField(dateofdeath,StringType,true),StructField(gender,StringType,true),StructField(maritalstatus,StructType(List(StructField(standard,StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),StructField(standardCodings,ArrayType(StructType(List(StructField(id,StringType,true),StructField(codingSystemId,StringType,true),StructField(primaryDisplay,StringType,true))),true),true))),true),StructField(races,ArrayType(StringType,true),true),StructField(ethnicities,ArrayType(StringType,true),true),StructField(deceased,BooleanType,true),StructField(source,StringType,true),StructField(state,StringType,true),StructField(testpatientflag,BooleanType,true),StructField(active,BooleanType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:39,871 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:39,918 - lhn.item - INFO - Create name: providerSource from location: iuhealth_ed_data_cohort_202306.provider_demographics
2025-12-22 14:18:40,083 - lhn.item - INFO - Using inputRegex: ['providerid', 'cmsgrouping', 'cmsclassification', 'cmsspecialization', 'tenant']
2025-12-22 14:18:40,091 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(providerid,StringType,true),StructField(cmsgrouping,StringType,true),StructField(cmsclassification,StringType,true),StructField(cmsspecialization,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:40,104 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:40,125 - lhn.resource - INFO - processDataTables: processed rwd to point to schema RWDSchema
2025-12-22 14:18:40,126 - lhn.resource - INFO - processDataTables: processed r to point to schema RWDSchema
2025-12-22 14:18:40,126 - lhn.resource - INFO - processed property names: {'rwd', 'r'}
2025-12-22 14:18:40,126 - lhn.resource - INFO - processed RWDSchema to produce property r and dictionary rwd
2025-12-22 14:18:40,127 - lhn.resource - INFO - Can call h.Extract(resource.rwd) to get the Extract object
2025-12-22 14:18:40,127 - lhn.resource - INFO - Processing schema name projectSchema at location sicklecell_ai
2025-12-22 14:18:40,127 - lhn.resource - INFO - processDataTableBySchemakey: Processing schema name projectSchema at location sicklecell_ai (from schemas dictionary)
2025-12-22 14:18:40,128 - lhn.resource - INFO - Found: schema projectSchema:sicklecell_ai in callFunProcessDataTables
2025-12-22 14:18:40,128 - lhn.resource - INFO - Element of callFunProcessDataTables used for callFun: 
 {'data_type': 'projectTables',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'property_name': 'db',
 'schema_type': 'projectSchema',
 'tableNameTemplate': '_{disease}_{schemaTag}',
 'type_key': 'proj',
 'updateDict': False}
2025-12-22 14:18:40,176 - lhn.resource - INFO - Found schema projectSchema at sicklecell_ai
2025-12-22 14:18:40,220 - lhn.resource - INFO - Found schema projectSchema indicated by schemaTag RWD at sicklecell_ai
2025-12-22 14:18:40,221 - lhn.resource - INFO - Use to call processDataTables
2025-12-22 14:18:40,233 - lhn.resource - INFO - funCall: {'dataLoc': '/home/hnelson3/work/Users/hnelson3/inst/extdata/SickleCell_AI/',
 'dataTables': {'ADSpatSCD': {'label': 'SCD Patient level Table'},
                'ADSpatient': {'label': 'Patient Level Table'},
                'Lab_LOINCEncounter': {'datefield': 'datetimeLab',
                                       'fieldList': ['labid',
                                                     'encounterid',
                                                     'personid',
                                                     'labcode_standard_id',
                                                     'labcode_standard_codingSystemId',
                                                     'labcode_standard_primaryDisplay',
                                                     'loincclass',
                                                     'servicedate',
                                                     'typedvalue_textValue_value',
                                                     'typedvalue_numericValue_value',
                                                     'typedvalue_unitOfMeasure_standard_id',
                                                     'interpretation_standard_primaryDisplay',
                                                     'tenant',
                                                     'datetimeLab',
                                                     'dateLab',
                                                     'Test'],
                                       'histEnd': None,
                                       'histStart': None,
                                       'indexFields': ['personid', 'tenant'],
                                       'label': 'Lab Encounters from the Lab '
                                                'Codes'},
                'Lab_LOINCEncounterBaseline': {'datefield': 'first_test_date',
                                               'histEnd': None,
                                               'histStart': None,
                                               'indexFields': ['personid',
                                                               'tenant'],
                                               'label': 'Lab Encounters from '
                                                        'the Lab Codes at '
                                                        'baseline'},
                'Lab_LOINCEncounterDemo': {'datefield': 'dateLab',
                                           'histEnd': None,
                                           'histStart': None,
                                           'indexFields': ['personid',
                                                           'tenant'],
                                           'label': 'Lab Encounters from the '
                                                    'Lab Codes with '
                                                    'demographic info added'},
                'Lab_LOINCEncounterDemoBaseline': {'datefield': 'dateLab',
                                                   'histEnd': None,
                                                   'histStart': None,
                                                   'indexFields': ['personid',
                                                                   'tenant'],
                                                   'label': 'Lab Encounters '
                                                            'from the Lab '
                                                            'Codes with '
                                                            'demographic info '
                                                            'added and subset '
                                                            'to baseline'},
                'Lab_LOINCEncounterFeature': {'datefield': 'datetimeLab',
                                              'fieldList': ['labid',
                                                            'encounterid',
                                                            'personid',
                                                            'labcode_standard_id',
                                                            'labcode_standard_codingSystemId',
                                                            'labcode_standard_primaryDisplay',
                                                            'loincclass',
                                                            'servicedate',
                                                            'typedvalue_textValue_value',
                                                            'typedvalue_numericValue_value',
                                                            'typedvalue_unitOfMeasure_standard_id',
                                                            'interpretation_standard_primaryDisplay',
                                                            'tenant',
                                                            'datetimeLab',
                                                            'dateLab',
                                                            'Test'],
                                              'histEnd': None,
                                              'histStart': None,
                                              'indexFields': ['personid',
                                                              'tenant'],
                                              'label': 'Lab Encounters from '
                                                       'the Lab Codes'},
                'Lab_LOINC_Codes_Verified': {'datefield': None,
                                             'groupName': 'regexgroup',
                                             'indexFields': ['labcode_standard_primaryDisplay',
                                                             'labcode_standard_id',
                                                             'labcode_standard_codingSystemId'],
                                             'label': 'Lab Codes to indicate '
                                                      'comorbidities'},
                'Lab_LOINC_Codes_regex': {'complete': False,
                                          'label': 'Lab Codes to indicate '
                                                   'comorbidities',
                                          'listIndex': 'Regex',
                                          'sourceField': 'labcode_standard_primaryDisplay'},
                'Pregnancy_Outcome_Encounter': {'datefield': 'datetimeCondition',
                                                'fieldList': ['conditionid',
                                                              'personid',
                                                              'encounterid',
                                                              'conditioncode_standard_id',
                                                              'conditioncode_standard_codingSystemId',
                                                              'conditioncode_standard_primaryDisplay',
                                                              'tenant',
                                                              'datetimeCondition',
                                                              'Condition'],
                                                'histEnd': None,
                                                'histStart': None,
                                                'indexFields': ['personid',
                                                                'tenant'],
                                                'label': 'Lab Encounters from '
                                                         'the Lab Codes'},
                'Pregnancy_Outcome_EncounterFeature': {'datefield': 'datetimeCondition',
                                                       'fieldList': ['personid',
                                                                     'tenant',
                                                                     'datetimeCondition',
                                                                     'Condition'],
                                                       'histEnd': None,
                                                       'histStart': None,
                                                       'indexFields': ['personid',
                                                                       'tenant'],
                                                       'label': 'Lab '
                                                                'Encounters '
                                                                'from the Lab '
                                                                'Codes'},
                'Pregnancy_Outcome_ICD_Codes': {'complete': True,
                                                'label': 'Condition Codes to '
                                                         'indicate Pregnancy',
                                                'listIndex': 'conditioncode_standard_id',
                                                'sourceField': 'conditioncode_standard_id'},
                'Pregnancy_Outcome_ICD_Codes_Verified': {'datefield': None,
                                                         'groupName': 'regexgroup',
                                                         'indexFields': ['conditioncode_standard_primaryDisplay',
                                                                         'conditioncode_standard_id',
                                                                         'conditioncode_standard_codingSystemId'],
                                                         'label': 'Condition '
                                                                  'Codes to '
                                                                  'indicate '
                                                                  'Pregnancy'},
                'birthsex': {'label': 'Patient Level maritalstatus'},
                'comorbidity': {'label': 'Feature control file'},
                'comorbidity_Codes_Verified': {'datefield': None,
                                               'groupName': 'regexgroup',
                                               'indexFields': ['conditioncode_standard_primaryDisplay',
                                                               'conditioncode_standard_id',
                                                               'conditioncode_standard_codingSystemId'],
                                               'label': 'Codes to indicate '
                                                        'comorbidities'},
                'comorbidity_ConditionEncounter': {'datefield': 'datetimeCondition',
                                                   'fieldList': ['conditionid',
                                                                 'personid',
                                                                 'encounterid',
                                                                 'conditioncode_standard_id',
                                                                 'conditioncode_standard_codingSystemId',
                                                                 'conditioncode_standard_primaryDisplay',
                                                                 'tenant',
                                                                 'datetimeCondition',
                                                                 'Condition'],
                                                   'histEnd': None,
                                                   'histStart': None,
                                                   'indexFields': ['personid',
                                                                   'tenant'],
                                                   'label': 'Condition '
                                                            'Encounters from '
                                                            'the Comorbidity '
                                                            'Codes'},
                'comorbidity_ICD10_Codes': {'complete': False,
                                            'label': 'Codes to indicate '
                                                     'comorbidities',
                                            'listIndex': 'Condition',
                                            'sourceField': 'conditioncode_standard_primaryDisplay'},
                'comorbidity_ICD10_Codes_Verified': {'datefield': None,
                                                     'groupName': 'regexgroup',
                                                     'indexFields': ['conditioncode_standard_primaryDisplay',
                                                                     'conditioncode_standard_id',
                                                                     'conditioncode_standard_codingSystemId'],
                                                     'label': 'Codes to '
                                                              'indicate '
                                                              'comorbidities'},
                'comorbidity_ICD9_Codes': {'complete': False,
                                           'label': 'Codes to indicate '
                                                    'comorbidities',
                                           'listIndex': 'Condition',
                                           'sourceField': 'conditioncode_standard_primaryDisplay'},
                'comorbidity_ICD9_Codes_Verified': {'datefield': None,
                                                    'groupName': 'regexgroup',
                                                    'indexFields': ['conditioncode_standard_primaryDisplay',
                                                                    'conditioncode_standard_id',
                                                                    'conditioncode_standard_codingSystemId'],
                                                    'label': 'Codes to '
                                                             'indicate '
                                                             'comorbidities'},
                'comorbidity_procedureEncounter': {'datefield': 'datetimeProcedure',
                                                   'fieldList': ['procedureid',
                                                                 'personid',
                                                                 'encounterid',
                                                                 'procedurecode_standard_id',
                                                                 'procedurecode_standard_codingSystemId',
                                                                 'procedurecode_standard_primaryDisplay',
                                                                 'servicestartdateproc',
                                                                 'serviceenddateproc',
                                                                 'tenant',
                                                                 'datetimeProc',
                                                                 'dateProc'],
                                                   'histEnd': None,
                                                   'histStart': None,
                                                   'indexFields': ['personid',
                                                                   'tenant'],
                                                   'label': 'Procedure '
                                                            'Encounters from '
                                                            'the procedure '
                                                            'Codes'},
                'comorbidity_procedureEncounterFeature': {'datefield': 'datetimeProcedure',
                                                          'fieldList': ['procedureid',
                                                                        'personid',
                                                                        'encounterid',
                                                                        'procedurecode_standard_id',
                                                                        'procedurecode_standard_codingSystemId',
                                                                        'procedurecode_standard_primaryDisplay',
                                                                        'servicestartdateproc',
                                                                        'serviceenddateproc',
                                                                        'tenant',
                                                                        'datetimeProc',
                                                                        'dateProc'],
                                                          'histEnd': None,
                                                          'histStart': None,
                                                          'indexFields': ['personid',
                                                                          'tenant'],
                                                          'label': 'Procedure '
                                                                   'Encounters '
                                                                   'from the '
                                                                   'procedure '
                                                                   'Codes'},
                'comorbidity_procedure_Codes': {'complete': True,
                                                'label': 'Procedure Codes to '
                                                         'indicate '
                                                         'comorbidities',
                                                'listIndex': 'procedurecode_standard_id',
                                                'sourceField': 'procedurecode_standard_id'},
                'comorbidity_procedure_Codes_Verified': {'datefield': None,
                                                         'groupName': 'regexgroup',
                                                         'indexFields': ['procedurecode_standard_primaryDisplay',
                                                                         'procedurecode_standard_id',
                                                                         'procedurecode_standard_codingSystemId'],
                                                         'label': 'Procedure '
                                                                  'Codes to '
                                                                  'indicate '
                                                                  'comorbidities'},
                'condition_feature_codesVerified': {'datefield': None,
                                                    'groupName': 'regexgroup',
                                                    'indexFields': ['labcode_standard_primaryDisplay',
                                                                    'labcode_standard_id',
                                                                    'labcode_standard_codingSystemId'],
                                                    'label': ' '
                                                             'condition_feature_codesVerifieds'},
                'condition_features': {'datefield': 'datetimeLab',
                                       'fieldList': ['personid',
                                                     'tenant',
                                                     'encounterid',
                                                     'conditionid',
                                                     'conditioncode_standard_id',
                                                     'conditioncode_standard_codingSystemId',
                                                     'conditioncode_standard_primaryDisplay',
                                                     'datetimeCondition',
                                                     'feature'],
                                       'histEnd': '2025-01-01',
                                       'histStart': '1990-01-01',
                                       'label': 'condition values to include '
                                                'as features'},
                'condition_features_codes': {'groupname': 'regex',
                                             'indexFields': ['conditioncode_standard_id',
                                                             'conditioncode_standard_codingSystemId'],
                                             'label': 'code list for condition '
                                                      'features'},
                'corrections': {'datefieldPrimary': ['datetimeEnc'],
                                'datefieldStop': ['dischargedate'],
                                'histEnd': '2025-01-01',
                                'histStart': '1990-01-01',
                                'indexFields': ['personid',
                                                'tenant',
                                                'financialclass_standard_primaryDisplay'],
                                'label': 'All insurance Enounter Records',
                                'max_gap': 90,
                                'retained_fields': ['personid',
                                                    'tenant',
                                                    'financialclass_standard_primaryDisplay',
                                                    'encType',
                                                    'servicedate',
                                                    'dischargedate',
                                                    'datetimeEnc',
                                                    'dateEnc']},
                'death': {'indexFields': ['personid', 'tenant'],
                          'label': 'Patient Level Death'},
                'deathList': {'label': 'RWD death Levels'},
                'demo': {'datefield': 'birthdate',
                         'index': ['personid', 'tenant'],
                         'label': 'Combined demographics and '
                                  'preferred_demographics table'},
                'demoOMOP': {'datefield': 'birthdate',
                             'index': ['personid', 'tenant'],
                             'label': 'Combined demographics and '
                                      'preferred_demographics table and OMOP'},
                'drug_codes': {'complete': True,
                               'dictionary': {'Analgesics': 'N02',
                                              'Antibiotics': 'J01'},
                               'indexFields': ['drugcode_standard_id',
                                               'drugcode_standard_codingSystemId',
                                               'drugcode_standard_primaryDisplay'],
                               'label': 'Medication Codes of Interest',
                               'listIndex': 'codes',
                               'sourceField': 'medications_drugCode_standard_id'},
                'encSDF': {"encTypeValues'": ['Outpatient',
                                              'Inpatient',
                                              'Emergency',
                                              'Admitted for Observation'],
                           'label': 'encSDF table for hemoglobinopath ICD '
                                    'phenotyping',
                           'retained_fields': ['personid',
                                               'encounterid',
                                               'servicedate',
                                               'dischargedate',
                                               'encType',
                                               'tenant']},
                'encounter': {'broadcast_flag': False,
                              'cacheResult': False,
                              'cohort': None,
                              'cohortColumns': None,
                              'datefield': 'datetimeEnc',
                              'elementExtract': ['encounterid'],
                              'elementIndex': ['personid', 'tenant'],
                              'histEnd': '2025-01-01',
                              'histStart': '1990-01-01',
                              'howCohortJoin': 'right',
                              'howjoin': 'inner',
                              'indexFields': ['personid',
                                              'tenant',
                                              'encounterid'],
                              'label': 'Encounter Extraction, Matching '
                                       'Condition Encounter to the Encounter '
                                       'Table',
                              'masterList': ['personid',
                                             'tenant',
                                             'encounterid',
                                             'datetimeCondition',
                                             'index_SCD',
                                             'last_SCD',
                                             'encounter_days',
                                             'days_to_next_encounter',
                                             'index_therapy_SCD',
                                             'last_therapy_SCD',
                                             'max_gap',
                                             'encounter_days_for_course'],
                              'retained_fields': ['classification_standard_primaryDisplay',
                                                  'confirmationstatus_standard_primaryDisplay',
                                                  'group',
                                                  'tenant'],
                              'sourceFields': ['facilityids',
                                               'reasonforvisit_standard_primaryDisplay',
                                               'financialclass_standard_primaryDisplay',
                                               'hospitalservice_standard_primaryDisplay',
                                               'classification_standard_id',
                                               'encType',
                                               'type_standard_primaryDisplay',
                                               'hospitalizationstartdate',
                                               'readmission',
                                               'servicedate',
                                               'dischargedate',
                                               'encountertypes_classification_standard_id',
                                               'admissiontype_standard_primaryDisplay',
                                               'status_standard_primaryDisplay',
                                               'actualarrivaldate',
                                               'datetimeEnc',
                                               'dateEnc']},
                'encounterExtract': {'label': 'Extract some columns to csv'},
                'encounterId': {'code': 'SCD',
                                'datefield': 'datetimeCondition',
                                'datefieldPrimary': ['datetimeCondition'],
                                'datefieldStop': ['datetimeCondition'],
                                'histEnd': '2025-01-01',
                                'histStart': '1990-01-01',
                                'index': ['personid', 'tenant'],
                                'indexFields': ['personid',
                                                'tenant',
                                                'encounterid'],
                                'label': 'One Record Per Condition Encounter',
                                'max_gap': 5000,
                                'retained_fields': ['classification_standard_primaryDisplay',
                                                    'confirmationstatus_standard_primaryDisplay',
                                                    'group',
                                                    'tenant'],
                                'sort_fields': ['datetimeCondition']},
                'evalPhenoRow': {'label': 'Phenotyping'},
                'extractList': {'label': 'Phenotyping'},
                'feature1': {'groupName': 'features',
                             'label': 'Results search features from table 1'},
                'final_df_scd_rwd': {'label': 'Phenotyping'},
                'follow': {'label': 'Followup Start and Stop Dates From '
                                    'Condition, Lab and Procedure'},
                'followCondition': {'code': 'cond',
                                    'datefieldPrimary': 'datetimeCondition',
                                    'datefieldStop': 'datetimeCondition',
                                    'histEnd': '2025-12-22',
                                    'histStart': '2000-01-01',
                                    'indexFields': ['personid', 'tenant'],
                                    'label': 'Follow-up metrics from '
                                             'Conditions using '
                                             'write_index_table',
                                    'max_gap': 1096,
                                    'retained_fields': [],
                                    'sort_fields': ['datetimeCondition']},
                'followEnc': {'code': 'enc',
                              'datefieldPrimary': ['datetimeEnc'],
                              'datefieldStop': ['dischargedate'],
                              'histEnd': '2025-12-22',
                              'histStart': '2000-01-01',
                              'indexFields': ['personid', 'tenant'],
                              'label': 'Follow-up metrics from Encounters '
                                       'using write_index_table',
                              'max_gap': 1096,
                              'retained_fields': [],
                              'sort_fields': ['datetimeEnc']},
                'followLab': {'code': 'lab',
                              'datefieldPrimary': 'datetimeLab',
                              'datefieldStop': 'datetimeLab',
                              'histEnd': '2025-12-22',
                              'histStart': '2000-01-01',
                              'indexFields': ['personid', 'tenant'],
                              'label': 'Follow-up metrics from Labs using '
                                       'write_index_table',
                              'max_gap': 1096,
                              'retained_fields': [],
                              'sort_fields': ['datetimeLab']},
                'followMed': {'code': 'med',
                              'datefieldPrimary': 'startDate',
                              'datefieldStop': 'stopDate',
                              'histEnd': '2025-12-22',
                              'histStart': '2000-01-01',
                              'indexFields': ['personid', 'tenant'],
                              'label': 'Follow-up metrics from Medications '
                                       'using write_index_table',
                              'max_gap': 1096,
                              'retained_fields': [],
                              'sort_fields': ['startDate']},
                'followProc': {'code': 'proc',
                               'datefieldPrimary': 'datetimeProc',
                               'datefieldStop': 'datetimeProc',
                               'histEnd': '2025-12-22',
                               'histStart': '2000-01-01',
                               'indexFields': ['personid', 'tenant'],
                               'label': 'Follow-up metrics from Procedures '
                                        'using write_index_table',
                               'max_gap': 1096,
                               'retained_fields': [],
                               'sort_fields': ['datetimeProc']},
                'gap_conditionID': {'code': 'SCD',
                                    'datefieldPrimary': ['datetimeCondition'],
                                    'datefieldStop': ['datetimeCondition'],
                                    'histEnd': '2025-01-01',
                                    'histStart': '1990-01-01',
                                    'indexFields': ['personid', 'tenant'],
                                    'label': 'One Record Per Condition '
                                             'Encounter',
                                    'max_gap': 1096,
                                    'retained_fields': ['personid',
                                                        'encounterid',
                                                        'tenant',
                                                        'datetimeCondition',
                                                        'dateCondition'],
                                    'sort_fields': ['datetimeCondition']},
                'gap_encounterID': {'code': 'gap',
                                    'datefieldPrimary': ['servicedate'],
                                    'datefieldStop': ['dischargedate'],
                                    'histEnd': '2025-01-01',
                                    'histStart': '1990-01-01',
                                    'indexFields': ['personid', 'tenant'],
                                    'label': 'One Record Per Encounter',
                                    'max_gap': 1096,
                                    'retained_fields': ['personid',
                                                        'encounterid',
                                                        'tenant',
                                                        'datetimeEnc',
                                                        'dateEnc',
                                                        'servicedate',
                                                        'dischargedate'],
                                    'sort_fields': ['datetimeEnc']},
                'hgb_subunits': {'datefield': 'servicedate',
                                 'label': 'Phenotyping'},
                'hydroxpatients': {'label': 'First date of hydroxiria'},
                'hydroxyurea': {'datefield': 'date',
                                'label': 'Hydroxyurea Usage for phenotyping'},
                'icdSDF': {'encTypeValues': ['Outpatient',
                                             'Inpatient',
                                             'Emergency',
                                             'Admitted for Observation'],
                           'label': 'icdSDF table for hemoglobinopath ICD '
                                    'phenotyping',
                           'retained_fields': ['personid',
                                               'encounterid',
                                               'conditioncode_standard_id',
                                               'date',
                                               'conditioncode_standard_id',
                                               'IcdCategory',
                                               'servicedate',
                                               'dischargedate',
                                               'encType',
                                               'tenant']},
                'insurance': {'datefieldPrimary': ['datetimeEnc'],
                              'datefieldStop': ['dischargedate'],
                              'histEnd': '2025-01-01',
                              'histStart': '1990-01-01',
                              'indexFields': ['personid',
                                              'tenant',
                                              'financialclass_standard_primaryDisplay'],
                              'label': 'All insurance Enounter Records',
                              'max_gap': 90,
                              'retained_fields': ['personid',
                                                  'tenant',
                                                  'financialclass_standard_primaryDisplay',
                                                  'encType',
                                                  'servicedate',
                                                  'dischargedate',
                                                  'datetimeEnc',
                                                  'dateEnc']},
                'insuranceGroups': {'label': 'Insurance'},
                'insurancePeriods': {'code': 'insurance',
                                     'datefieldPrimary': ['datetimeEnc'],
                                     'datefieldStop': ['dischargedate'],
                                     'histEnd': '2025-01-01',
                                     'histStart': '1990-01-01',
                                     'indexFields': ['personid',
                                                     'tenant',
                                                     'financialclass_standard_primaryDisplay'],
                                     'label': 'Insurance Periods of '
                                              'Observation',
                                     'max_gap': 90,
                                     'retained_fields': ['personid',
                                                         'tenant',
                                                         'financialclass_standard_primaryDisplay',
                                                         'encType',
                                                         'servicedate',
                                                         'dischargedate',
                                                         'datetimeEnc',
                                                         'dateEnc'],
                                     'sort_fields': ['personid',
                                                     'tenant',
                                                     'financialclass_standard_primaryDisplay',
                                                     'datetimeEnc']},
                'labPersonId': {'code': 'LAB',
                                'datefieldPrimary': ['datetimeLab'],
                                'datefieldStop': 'datetimeLab',
                                'histEnd': None,
                                'histStart': None,
                                'indexFields': ['personid', 'tenant'],
                                'label': 'One Record Per Lab Person ID',
                                'max_gap': 30000,
                                'retained_fields': ['labcode_standard_primaryDisplay',
                                                    'typedvalue_textValue_value',
                                                    'typedvalue_numericValue_value',
                                                    'typedvalue_unitOfMeasure_standard_primaryDisplay',
                                                    'interpretation_standard_primaryDisplay',
                                                    'lab',
                                                    'source',
                                                    'Subjects'],
                                'sort_fields': ['datetimeLab']},
                'lab_features': {'datefield': 'datetimeLab',
                                 'fieldList': ['personid',
                                               'tenant',
                                               'encounterid',
                                               'labid',
                                               'labcode_standard_id',
                                               'labcode_standard_codingSystemId',
                                               'labcode_standard_primaryDisplay',
                                               'loincclass',
                                               'datetimeLab',
                                               'typedvalue_numericValue_value',
                                               'typedvalue_textValue_value',
                                               'loincclass',
                                               'feature'],
                                 'histEnd': '2025-01-01',
                                 'histStart': '1990-01-01',
                                 'label': 'lab values to include as features'},
                'lab_features_codes': {'groupname': 'regex',
                                       'indexFields': ['labcode_standard_id',
                                                       'labcode_standard_codingSystemId'],
                                       'label': 'code list for lab features'},
                'lablist': {'label': 'list of labs to categorize'},
                'labsHgb': {'datefield': 'datetimeLab',
                            'datefieldPrimary': 'datetimeLab',
                            'indexFields': ['personid', 'tenant'],
                            'label': 'HgB Lab Records form from the lab table'},
                'labsHgbCodes': {'complete': True,
                                 'indexFields': ['labcode_standard_id',
                                                 'labcode_standard_codingSystemId',
                                                 'labcode_standard_primaryDisplay'],
                                 'label': 'A set of HgB Codes',
                                 'listIndex': 'lab',
                                 'sourceField': 'labcode_standard_id'},
                'labshydroxyuria': {'datefield': 'datetimeLab',
                                    'datefieldPrimary': 'datetimeLab',
                                    'indexFields': ['personid', 'tenant'],
                                    'label': 'HgB Lab Records form from the '
                                             'lab table'},
                'loinc_codes': {'complete': True,
                                'dictionary': {'AST': '1920-8',
                                               'Alkaline Phosphatase': '6768-6',
                                               'BUN': '3094-0',
                                               'Calculated eGFR': '48642-3',
                                               'Creatinine': '2160-0',
                                               'Haemoglobin': '718-7',
                                               'Haemoglobin F': '4579-1',
                                               'LDH': '14804-9',
                                               'NT-proBNP': '33763-1',
                                               'Potassium': '2823-3',
                                               'WBC count': '6690-2'},
                                'label': 'Lab whose values can be a predictor '
                                         'of death, from previously published '
                                         'models.',
                                'listIndex': 'codes',
                                'sourceField': 'labcode_standard_id'},
                'loinc_codesVerified': {'datefield': None,
                                        'groupName': 'regexgroup',
                                        'indexFields': ['labcode_standard_primaryDisplay',
                                                        'labcode_standard_id',
                                                        'labcode_standard_codingSystemId'],
                                        'label': ' Verified Loinc Codes'},
                'loincresults': {'datefield': 'datetimeLab',
                                 'groupName': 'labgroup',
                                 'histEnd': '2025-01-01',
                                 'histStart': '1990-01-01',
                                 'label': 'Results searching labs with loinc '
                                          'codes'},
                'loincresultsminimal': {'datefield': 'dateLab',
                                        'groupName': 'labgroup',
                                        'histEnd': '2025-01-01',
                                        'histStart': '1990-01-01',
                                        'label': 'Results searching labs with '
                                                 'loinc codes'},
                'maritalstatus': {'label': 'Patient level marital status'},
                'maritalstatusList': {'label': 'RWD Marital Status Levels'},
                'medHydroxiacodes': {'datefield': None,
                                     'indexFields': ['drugcode_standard_id',
                                                     'drugcode_standard_codingSystemId',
                                                     'drugcode_standard_primaryDisplay'],
                                     'label': 'Hydroxia Medication'},
                'medication_encounterDemoBaseline': {'datefield': 'dateMed',
                                                     'histEnd': None,
                                                     'histStart': None,
                                                     'indexFields': ['personid',
                                                                     'tenant'],
                                                     'label': 'Medication '
                                                              'Encounters from '
                                                              'the Lab Codes '
                                                              'with '
                                                              'demographic '
                                                              'info added and '
                                                              'subset to '
                                                              'baseline'},
                'medication_encountermeasures': {'datefield': 'dateMed',
                                                 'label': 'Medications with '
                                                          'feature values in a '
                                                          'long format'},
                'medication_encounters': {'datefield': 'dateMed',
                                          'histEnd': '2025-01-01',
                                          'histStart': '1990-01-01',
                                          'indexFields': ['personid', 'tenant'],
                                          'label': 'Medication Encounter '
                                                   'Records',
                                          'retained_fields': ['medicationid',
                                                              'encounterid',
                                                              'personid',
                                                              'startDate',
                                                              'stopDate',
                                                              'drugcode_standard_id',
                                                              'drugcode_standard_codingSystemId',
                                                              'drugcode_standard_primaryDisplay',
                                                              'prescribingprovider',
                                                              'tenant',
                                                              'dateMed']},
                'medication_encounters_index': {'code': 'MED',
                                                'datefieldPrimary': 'dateMed',
                                                'datefieldStop': 'stopDate',
                                                'histEnd': '2025-01-01',
                                                'histStart': '1990-01-01',
                                                'indexFields': ['personid',
                                                                'tenant',
                                                                'drugcode_standard_id'],
                                                'label': 'First Medication '
                                                         'Encounter for Each '
                                                         'Patient-Medication '
                                                         'Combination',
                                                'max_gap': 90,
                                                'retained_fields': ['medicationid',
                                                                    'encounterid',
                                                                    'startDate',
                                                                    'stopDate',
                                                                    'drugcode_standard_primaryDisplay',
                                                                    'prescribingprovider'],
                                                'sort_fields': ['dateMed']},
                'medication_features': {'datefield': 'dateMed',
                                        'fieldList': ['personid',
                                                      'tenant',
                                                      'encounterid',
                                                      'medicationid',
                                                      'drugcode_standard_id',
                                                      'drugcode_standard_codingSystemId',
                                                      'drugcode_standard_primaryDisplay',
                                                      'datetimeMed',
                                                      'startDate',
                                                      'stopDate',
                                                      'feature'],
                                        'histEnd': '2025-01-01',
                                        'histStart': '1990-01-01',
                                        'label': 'medication values to include '
                                                 'as features'},
                'medication_features_codes': {'groupname': 'regex',
                                              'indexFields': ['medicationcode_standard_id',
                                                              'medicationcode_standard_codingSystemId'],
                                              'label': 'code list for '
                                                       'medication features'},
                'medsHydroxia': {'datefield': 'datetimeMed',
                                 'histEnd': '2025-01-01',
                                 'histStart': '1990-01-01',
                                 'indexFields': ['personid',
                                                 'tenant',
                                                 'encounterid'],
                                 'label': 'Hydroxia Medication Records',
                                 'retained_fields': ['personid',
                                                     'tenant',
                                                     'encounterid',
                                                     'drugcode_standard_id',
                                                     'drugcode_standard_codingSystemId',
                                                     'drugcode_standard_primaryDisplay',
                                                     'personid',
                                                     'tenant',
                                                     'medicationid',
                                                     'encounterid',
                                                     'startdate',
                                                     'stopdate',
                                                     'prescribingprovider',
                                                     'datetimeMed',
                                                     'dateMed']},
                'medsHydroxiaIndex': {'code': 'hydroxia',
                                      'datefield': 'datetimeMed',
                                      'datefieldPrimary': ['datetimeMed'],
                                      'datefieldStop': 'stopdate',
                                      'histEnd': '2025-01-01',
                                      'histStart': '1990-01-01',
                                      'indexFields': ['personid', 'tenant'],
                                      'label': 'Hydroxia Patient Level Data',
                                      'max_gap': 30000,
                                      'retained_fields': ['drugcode_standard_id',
                                                          'drugcode_standard_codingSystemId',
                                                          'drugcode_standard_primaryDisplay',
                                                          'prescribingprovider',
                                                          'dateMed',
                                                          'stopdate'],
                                      'sort_fields': ['datetimeMed']},
                'medsHydroxiaUsage': {'datefield': 'hydroxia_start',
                                      'indexFields': ['personid', 'tenant'],
                                      'label': 'Usage of Hydroxia'},
                'medshydroxiadates': {'label': 'Phenotyping'},
                'mortality': {'label': 'Phenotyping'},
                'mrnList': {'indexFields': ['personid'],
                            'label': 'MRNs of the IUH Patients:'},
                'observation_period': {'course_id_col': 'course_of_therapy_id',
                                       'index_col': 'index_therapy_enc',
                                       'label': 'Observation Period based on '
                                                'the Encounters',
                                       'last_course_col': 'last_therapy_enc',
                                       'last_overall_col': 'last_enc',
                                       'max_course_col': 'max_course_of_therapy_id',
                                       'other_select_cols': ['tenant'],
                                       'perform_check': True,
                                       'person_id_col': 'personid'},
                'persontenant': {'indexFields': ['personid', 'tenant'],
                                 'label': 'Person and Tenant ID for every '
                                          'member of the cohort and control',
                                 'partitionBy': 'tenant',
                                 'partitionby': 'tenant'},
                'persontenantOMOP': {'indexFields': ['person_id',
                                                     'personid',
                                                     'tenant'],
                                     'label': 'Person and Tenant ID with OMOP '
                                              'person_id',
                                     'partitionBy': 'tenant',
                                     'partitionby': 'tenant'},
                'phenoMatrix': {'label': 'Phenotyping'},
                'pheno_id_extra': {'label': 'Phenotyping'},
                'problem_list_features': {'datefield': 'datetimeProblem',
                                          'fieldList': ['personid',
                                                        'tenant',
                                                        'encounterid',
                                                        'problemlistid',
                                                        'drugcode_standard_id',
                                                        'problemlistcode_standard_codingSystemId',
                                                        'problemlistcode_standard_primaryDisplay',
                                                        'datetimeProblem',
                                                        'feature'],
                                          'histEnd': '2025-01-01',
                                          'histStart': '1990-01-01',
                                          'label': 'problem list values to '
                                                   'include as features'},
                'problem_list_features_codes': {'groupname': 'regex',
                                                'indexFields': ['problemlistcode_standard_id',
                                                                'problemlistcode_standard_codingSystemId'],
                                                'label': 'code list for '
                                                         'problem list '
                                                         'features'},
                'proctransfusion': {'cohortColumns': ['personid',
                                                      'encounterid',
                                                      'tenant',
                                                      'course_of_therapy',
                                                      'index_LAB',
                                                      'last_LAB',
                                                      'encounter_days'],
                                    'datefield': 'datetimeProc',
                                    'histEnd': None,
                                    'histStart': None,
                                    'indexFields': ['personid',
                                                    'tenant',
                                                    'encounterid'],
                                    'label': 'Tranfusion Records from the '
                                             'Procedure Table',
                                    'retained_fields': ['personid',
                                                        'encounterid',
                                                        'tenant',
                                                        'course_of_therapy',
                                                        'index_LAB',
                                                        'last_LAB',
                                                        'encounter_days',
                                                        'datetimeProc',
                                                        'dateProc']},
                'proctransfusioncodes': {'indexFields': ['procedurecode_standard_id',
                                                         'procedurecode_standard_codingSystemId',
                                                         'procedurecode_standard_primaryDisplay'],
                                         'label': 'A list of the Transfusion '
                                                  'Procedures Needed'},
                'scd_death': {'lable': 'Death'},
                'scd_demo': {'label': 'Demographics'},
                'scd_possible': {'label': 'Possible Case'},
                'scdboth': {'label': 'Both Case and Control'},
                'scdpatient': {'label': 'SCD Case Patients'},
                'sickleConditionEncounter': {'datefield': 'datetimeCondition',
                                             'histEnd': None,
                                             'histStart': None,
                                             'indexFields': ['personid'],
                                             'label': ' Sickle Cell Encounters '
                                                      'from the conditions '
                                                      'table'},
                'sickleConditionEncounterExpanded': {'datefield': 'datetimeCondition',
                                                     'histEnd': None,
                                                     'histStart': None,
                                                     'indexFields': ['personid'],
                                                     'label': ' Sickle Cell '
                                                              'Encounters from '
                                                              'the conditions '
                                                              'table'},
                'sicklecodes': {'complete': True,
                                'dictionary': {'other': 'D58|282.[0,1,2,3,7,8,9]',
                                               'scd': 'D57.[0,1,2,4,8]|282.4[1,2]|282.6',
                                               'thal': 'D56|282.4[0,3,4,5,6,7,9]',
                                               'trait': 'D57.3|282.5'},
                                'label': 'Codes to search for Sickle Cell '
                                         'Conditions',
                                'listIndex': 'codes',
                                'sourceField': 'conditioncode_standard_id'},
                'sicklecodesVerified': {'datefield': None,
                                        'groupName': 'regexgroup',
                                        'indexFields': ['conditioncode_standard_primaryDisplay',
                                                        'conditioncode_standard_id',
                                                        'conditioncode_standard_codingSystemId'],
                                        'label': ' Verified Sickle Cell Codes'},
                'sickleconditionencounter': {'label': 'Condition Encounter '
                                                      'Records with Encounter '
                                                      'Record Information'},
                'statusList': {'label': 'All Status Code Levels'},
                'stemcellID': {'code': 'SCD',
                               'datefieldPrimary': ['dateCondition'],
                               'datefieldStop': 'dateCondition',
                               'histEnd': '2025-01-01',
                               'histStart': '1990-01-01',
                               'indexFields': ['personid', 'tenant'],
                               'label': 'One Record Per Stem Cell Encounter',
                               'max_gap': 30000,
                               'retained_fields': ['datetimeCondition',
                                                   'typedvalue_numericValue_value',
                                                   'typebdvalue_unitOfMeasure_standard_id',
                                                   'labcode_standard_primaryDisplay',
                                                   'group',
                                                   'tenant'],
                               'sort_fields': ['datetimeCondition']},
                'stemcell_codes': {'complete': True,
                                   'dictionary': {'Bone marrow replaced by transplant': 'V42.81',
                                                  'Bone marrow transplant status': 'Z94.81',
                                                  'Peripheral stem cells replaced by transplant': 'V42.82',
                                                  'Stem cells transplant status': 'Z94.84'},
                                   'label': 'Codes indicating Stem Cell '
                                            'Transfusion',
                                   'listIndex': 'codes',
                                   'sourceField': 'conditioncode_standard_id'},
                'stemcellcodesVerified': {'datefield': None,
                                          'groupName': 'regexgroup',
                                          'indexFields': ['conditioncode_standard_primaryDisplay',
                                                          'conditioncode_standard_id',
                                                          'conditioncode_standard_codingSystemId'],
                                          'label': ' Verified Stem Cell Codes'},
                'stemcellresults': {'cohortColumns': ['personid',
                                                      'encounterid',
                                                      'tenant'],
                                    'datefield': 'datetimeCondition',
                                    'groupName': 'stemconditiongroup',
                                    'histEnd': '2025-01-01',
                                    'histStart': '1990-01-01',
                                    'indexFields': ['personid'],
                                    'label': 'Results searching conditions '
                                             'with stem cell codes',
                                    'retained_fields': ['personid',
                                                        'encounterid',
                                                        'tenant',
                                                        'datetimeCondition',
                                                        'dateCondition']},
                'table1features': {'complete': True,
                                   'label': 'Features From Sachdev table 1 ',
                                   'listIndex': 'Regex',
                                   'sourceField': 'labcode_standard_primaryDisplay'},
                'test_feature_control': {'label': 'Feature control file'},
                'transfusion': {'datefield': 'date',
                                'label': 'All Transfusion Record Dates'},
                'visit_detail_concept_id': {'label': 'All visit detail records '
                                                     'with mapped concept ID'},
                'visit_occurrence_concept_id': {'label': 'All Visit Occurances '
                                                         'with mapped concept '
                                                         'ID'},
                'vital_sign_encounterBaseline': {'datefield': 'first_test_date',
                                                 'histEnd': None,
                                                 'histStart': None,
                                                 'indexFields': ['personid',
                                                                 'tenant'],
                                                 'label': 'Vital Sign '
                                                          'Encounters from the '
                                                          'Lab Codes at '
                                                          'baseline'},
                'vital_sign_encounterDemoBaseline': {'datefield': 'dateMeasurement',
                                                     'histEnd': None,
                                                     'histStart': None,
                                                     'indexFields': ['personid',
                                                                     'tenant'],
                                                     'label': 'Vital Sign '
                                                              'Encounters from '
                                                              'the Lab Codes '
                                                              'with '
                                                              'demographic '
                                                              'info added and '
                                                              'subset to '
                                                              'baseline'},
                'vital_sign_encountermeasures': {'label': 'Vital signs with '
                                                          'feature values in a '
                                                          'long format'},
                'vital_sign_encounters': {'datefield': 'dateMeasurement',
                                          'histEnd': '2025-10-01',
                                          'histStart': '1990-01-01',
                                          'indexFields': ['personid', 'tenant'],
                                          'label': 'Vital Sign Measurement '
                                                   'Records',
                                          'retained_fields': ['measurementid',
                                                              'encounterid',
                                                              'personid',
                                                              'measurementcode_standard_id',
                                                              'measurementcode_standard_codingSystemId',
                                                              'loincclass',
                                                              'servicedate',
                                                              'typedvalue_numericValue_value',
                                                              'typedvalue_unitOfMeasure_standard_id',
                                                              'interpretation_standard_primaryDisplay',
                                                              'tenant',
                                                              'dateMeasurement']},
                'vital_sign_encountersDemo': {'datefield': 'dateMeasurement',
                                              'histEnd': None,
                                              'histStart': None,
                                              'indexFields': ['personid',
                                                              'tenant'],
                                              'label': 'Vital Sign Lab '
                                                       'Encounters from the '
                                                       'Lab Codes with '
                                                       'demographic info '
                                                       'added'},
                'vital_signs_codes': {'indexFields': ['measurementcode_standard_id',
                                                      'measurementcode_standard_codingSystemId'],
                                      'label': 'Vital Signs Codes for '
                                               'Measurement Extraction'},
                'zipCodeList': {'label': 'RWD zip_code Levels'},
                'zipcode': {'label': 'Patient Level Zip Code'}},
 'debug': True,
 'disease': 'SCD',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'project': 'SickleCell_AI',
 'schema': 'sicklecell_ai',
 'schemaTag': 'RWD'}
2025-12-22 14:18:40,371 - lhn.item - INFO - Create name: persontenant from location: sicklecell_ai.persontenant_SCD_RWD
2025-12-22 14:18:40,385 - lhn.item - INFO - Create name: persontenantOMOP from location: sicklecell_ai.persontenantOMOP_SCD_RWD
2025-12-22 14:18:40,397 - lhn.item - INFO - Create name: death from location: sicklecell_ai.death_SCD_RWD
2025-12-22 14:18:40,410 - lhn.item - INFO - Create name: maritalstatus from location: sicklecell_ai.maritalstatus_SCD_RWD
2025-12-22 14:18:40,433 - lhn.item - INFO - Create name: followEnc from location: sicklecell_ai.followEnc_SCD_RWD
2025-12-22 14:18:40,447 - lhn.item - INFO - Create name: followCondition from location: sicklecell_ai.followCondition_SCD_RWD
2025-12-22 14:18:40,459 - lhn.item - INFO - Create name: followLab from location: sicklecell_ai.followLab_SCD_RWD
2025-12-22 14:18:40,472 - lhn.item - INFO - Create name: followProc from location: sicklecell_ai.followProc_SCD_RWD
2025-12-22 14:18:40,485 - lhn.item - INFO - Create name: followMed from location: sicklecell_ai.followMed_SCD_RWD
2025-12-22 14:18:40,498 - lhn.item - INFO - Create name: observation_period from location: sicklecell_ai.observation_period_SCD_RWD
2025-12-22 14:18:40,511 - lhn.item - INFO - Create name: follow from location: sicklecell_ai.follow_SCD_RWD
2025-12-22 14:18:40,540 - lhn.item - INFO - Create name: demo from location: sicklecell_ai.demo_SCD_RWD
2025-12-22 14:18:40,555 - lhn.item - INFO - Create name: demoOMOP from location: sicklecell_ai.demoOMOP_SCD_RWD
2025-12-22 14:18:40,569 - lhn.item - INFO - Create name: sicklecodes from location: sicklecell_ai.sicklecodes_SCD_RWD
2025-12-22 14:18:40,581 - lhn.item - INFO - Create name: sicklecodesVerified from location: sicklecell_ai.sicklecodesVerified_SCD_RWD
2025-12-22 14:18:40,594 - lhn.item - INFO - Create name: sickleConditionEncounter from location: sicklecell_ai.sickleConditionEncounter_SCD_RWD
2025-12-22 14:18:40,616 - lhn.item - INFO - Create name: encounterId from location: sicklecell_ai.encounterId_SCD_RWD
2025-12-22 14:18:40,629 - lhn.item - INFO - Create name: encounter from location: sicklecell_ai.encounter_SCD_RWD
2025-12-22 14:18:40,643 - lhn.item - INFO - Create name: encounterExtract from location: sicklecell_ai.encounterExtract_SCD_RWD
2025-12-22 14:18:40,656 - lhn.item - INFO - Create name: vital_signs_codes from location: sicklecell_ai.vital_signs_codes_SCD_RWD
2025-12-22 14:18:40,668 - lhn.item - INFO - Create name: vital_sign_encounters from location: sicklecell_ai.vital_sign_encounters_SCD_RWD
2025-12-22 14:18:40,681 - lhn.item - INFO - Create name: drug_codes from location: sicklecell_ai.drug_codes_SCD_RWD
2025-12-22 14:18:40,693 - lhn.item - INFO - Create name: medication_encounters from location: sicklecell_ai.medication_encounters_SCD_RWD
2025-12-22 14:18:40,717 - lhn.item - INFO - Create name: labsHgbCodes from location: sicklecell_ai.labsHgbCodes_SCD_RWD
2025-12-22 14:18:40,729 - lhn.item - INFO - Create name: labsHgb from location: sicklecell_ai.labsHgb_SCD_RWD
2025-12-22 14:18:40,743 - lhn.item - INFO - Create name: labPersonId from location: sicklecell_ai.labPersonId_SCD_RWD
2025-12-22 14:18:40,757 - lhn.item - INFO - Create name: proctransfusioncodes from location: sicklecell_ai.proctransfusioncodes_SCD_RWD
2025-12-22 14:18:40,768 - lhn.item - INFO - Create name: proctransfusion from location: sicklecell_ai.proctransfusion_SCD_RWD
2025-12-22 14:18:40,781 - lhn.item - INFO - Create name: medHydroxiacodes from location: sicklecell_ai.medHydroxiacodes_SCD_RWD
2025-12-22 14:18:40,794 - lhn.item - INFO - Create name: medsHydroxia from location: sicklecell_ai.medsHydroxia_SCD_RWD
2025-12-22 14:18:40,807 - lhn.item - INFO - Create name: medsHydroxiaIndex from location: sicklecell_ai.medsHydroxiaIndex_SCD_RWD
2025-12-22 14:18:40,819 - lhn.item - INFO - Create name: medsHydroxiaUsage from location: sicklecell_ai.medsHydroxiaUsage_SCD_RWD
2025-12-22 14:18:40,831 - lhn.item - INFO - Create name: stemcell_codes from location: sicklecell_ai.stemcell_codes_SCD_RWD
2025-12-22 14:18:40,843 - lhn.item - INFO - Create name: stemcellcodesVerified from location: sicklecell_ai.stemcellcodesVerified_SCD_RWD
2025-12-22 14:18:40,855 - lhn.item - INFO - Create name: stemcellresults from location: sicklecell_ai.stemcellresults_SCD_RWD
2025-12-22 14:18:40,868 - lhn.item - INFO - Create name: stemcellID from location: sicklecell_ai.stemcellID_SCD_RWD
2025-12-22 14:18:40,881 - lhn.item - INFO - Create name: loinc_codes from location: sicklecell_ai.loinc_codes_SCD_RWD
2025-12-22 14:18:40,892 - lhn.item - INFO - Create name: loinc_codesVerified from location: sicklecell_ai.loinc_codesVerified_SCD_RWD
2025-12-22 14:18:40,905 - lhn.item - INFO - Create name: loincresults from location: sicklecell_ai.loincresults_SCD_RWD
2025-12-22 14:18:40,918 - lhn.item - INFO - Create name: loincresultsminimal from location: sicklecell_ai.loincresultsminimal_SCD_RWD
2025-12-22 14:18:40,940 - lhn.item - INFO - Create name: gap_encounterID from location: sicklecell_ai.gap_encounterID_SCD_RWD
2025-12-22 14:18:40,970 - lhn.item - INFO - Create name: insurance from location: sicklecell_ai.insurance_SCD_RWD
2025-12-22 14:18:40,982 - lhn.item - INFO - Create name: corrections from location: sicklecell_ai.corrections_SCD_RWD
2025-12-22 14:18:40,994 - lhn.item - INFO - Create name: insurancePeriods from location: sicklecell_ai.insurancePeriods_SCD_RWD
2025-12-22 14:18:41,245 - lhn.item - INFO - Create name: sickleconditionencounter from location: sicklecell_ai.sickleconditionencounter_SCD_RWD
2025-12-22 14:18:41,365 - lhn.item - INFO - Create name: comorbidity_ICD9_Codes from location: sicklecell_ai.comorbidity_ICD9_Codes_SCD_RWD
2025-12-22 14:18:41,380 - lhn.item - INFO - Create name: comorbidity_ICD9_Codes_Verified from location: sicklecell_ai.comorbidity_ICD9_Codes_Verified_SCD_RWD
2025-12-22 14:18:41,395 - lhn.item - INFO - Create name: comorbidity_ICD10_Codes from location: sicklecell_ai.comorbidity_ICD10_Codes_SCD_RWD
2025-12-22 14:18:41,407 - lhn.item - INFO - Create name: comorbidity_ICD10_Codes_Verified from location: sicklecell_ai.comorbidity_ICD10_Codes_Verified_SCD_RWD
2025-12-22 14:18:41,420 - lhn.item - INFO - Create name: comorbidity_Codes_Verified from location: sicklecell_ai.comorbidity_Codes_Verified_SCD_RWD
2025-12-22 14:18:41,434 - lhn.item - INFO - Create name: comorbidity_ConditionEncounter from location: sicklecell_ai.comorbidity_ConditionEncounter_SCD_RWD
2025-12-22 14:18:41,447 - lhn.item - INFO - Create name: comorbidity_procedure_Codes from location: sicklecell_ai.comorbidity_procedure_Codes_SCD_RWD
2025-12-22 14:18:41,460 - lhn.item - INFO - Create name: comorbidity_procedure_Codes_Verified from location: sicklecell_ai.comorbidity_procedure_Codes_Verified_SCD_RWD
2025-12-22 14:18:41,473 - lhn.item - INFO - Create name: comorbidity_procedureEncounter from location: sicklecell_ai.comorbidity_procedureEncounter_SCD_RWD
2025-12-22 14:18:41,486 - lhn.item - INFO - Create name: comorbidity_procedureEncounterFeature from location: sicklecell_ai.comorbidity_procedureEncounterFeature_SCD_RWD
2025-12-22 14:18:41,499 - lhn.item - INFO - Create name: Lab_LOINC_Codes_regex from location: sicklecell_ai.Lab_LOINC_Codes_regex_SCD_RWD
2025-12-22 14:18:41,511 - lhn.item - INFO - Create name: Lab_LOINC_Codes_Verified from location: sicklecell_ai.Lab_LOINC_Codes_Verified_SCD_RWD
2025-12-22 14:18:41,523 - lhn.item - INFO - Create name: Lab_LOINCEncounter from location: sicklecell_ai.Lab_LOINCEncounter_SCD_RWD
2025-12-22 14:18:41,537 - lhn.item - INFO - Create name: Lab_LOINCEncounterFeature from location: sicklecell_ai.Lab_LOINCEncounterFeature_SCD_RWD
2025-12-22 14:18:41,561 - lhn.item - INFO - Create name: Lab_LOINCEncounterDemoBaseline from location: sicklecell_ai.Lab_LOINCEncounterDemoBaseline_SCD_RWD
2025-12-22 14:18:41,613 - lhn.item - INFO - Create name: Pregnancy_Outcome_ICD_Codes from location: sicklecell_ai.Pregnancy_Outcome_ICD_Codes_SCD_RWD
2025-12-22 14:18:41,627 - lhn.item - INFO - Create name: Pregnancy_Outcome_ICD_Codes_Verified from location: sicklecell_ai.Pregnancy_Outcome_ICD_Codes_Verified_SCD_RWD
2025-12-22 14:18:41,640 - lhn.item - INFO - Create name: Pregnancy_Outcome_Encounter from location: sicklecell_ai.Pregnancy_Outcome_Encounter_SCD_RWD
2025-12-22 14:18:41,654 - lhn.item - INFO - Create name: Pregnancy_Outcome_EncounterFeature from location: sicklecell_ai.Pregnancy_Outcome_EncounterFeature_SCD_RWD
2025-12-22 14:18:41,667 - lhn.resource - INFO - processDataTables: processed proj to point to schema projectSchema
2025-12-22 14:18:41,667 - lhn.resource - INFO - processDataTables: processed db to point to schema projectSchema
2025-12-22 14:18:41,668 - lhn.resource - INFO - processed property names: {'rwd', 'r', 'proj', 'db'}
2025-12-22 14:18:41,668 - lhn.resource - INFO - processed projectSchema to produce property db and dictionary proj
2025-12-22 14:18:41,668 - lhn.resource - INFO - Can call h.Extract(resource.proj) to get the Extract object
2025-12-22 14:18:41,669 - lhn.resource - INFO - Processing schema name sstudySchema at location sicklecell_study_rwd
2025-12-22 14:18:41,669 - lhn.resource - INFO - processDataTableBySchemakey: Processing schema name sstudySchema at location sicklecell_study_rwd (from schemas dictionary)
2025-12-22 14:18:41,669 - lhn.resource - INFO - Found: schema sstudySchema:sicklecell_study_rwd in callFunProcessDataTables
2025-12-22 14:18:41,670 - lhn.resource - INFO - Element of callFunProcessDataTables used for callFun: 
 {'data_type': 'sstudyTables',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'property_name': 's',
 'schema_type': 'sstudySchema',
 'tableNameTemplate': None,
 'type_key': 'ss',
 'updateDict': True}
2025-12-22 14:18:41,721 - lhn.resource - INFO - Found schema sstudySchema at sicklecell_study_rwd
2025-12-22 14:18:41,722 - lhn.resource - INFO - Updating the Dictionary for sstudySchema based on the schema sicklecell_study_rwd
2025-12-22 14:18:41,722 - lhn.resource - INFO - Using the tableNameTemplate: _scd_rwd to remove from the table names
2025-12-22 14:18:41,722 - lhn.resource - INFO - Use to call update_dictionary 
 {'debug': True,
 'obs': 100000000,
 'personid': ['personid'],
 'projectSchema': 'sicklecell_ai',
 'reRun': False,
 'schema': 'sicklecell_study_rwd',
 'schemaTag': 'RWD',
 'schema_dict': None,
 'tableNameTemplate': '_scd_rwd'}
2025-12-22 14:18:42,732 - lhn.resource - INFO - Found schema sstudySchema indicated by schemaTag RWD at sicklecell_study_rwd
2025-12-22 14:18:42,732 - lhn.resource - INFO - Use to call processDataTables
2025-12-22 14:18:42,736 - lhn.resource - INFO - funCall: {'dataLoc': '/home/hnelson3/work/Users/hnelson3/inst/extdata/SickleCell_AI/',
 'dataTables': {'adspatient': {'inputRegex': ['^location_id$',
                                              '^personid$',
                                              '^PersonPhenotype$',
                                              '^IcdPheno$',
                                              '^SCD$',
                                              '^casePossible$',
                                              '^tenant$',
                                              '^gender$',
                                              '^race$',
                                              '^ethnicity$',
                                              '^yearofbirth$',
                                              '^state$',
                                              '^metropolitan$',
                                              '^urban$',
                                              '^deceased$',
                                              '^dateofdeath$',
                                              '^encDateFirst$',
                                              '^encDateLast$',
                                              '^encounters$',
                                              '^procEncDateFirst$',
                                              '^procEncDateLast$',
                                              '^procEncounters$',
                                              '^medEncDateFirst$',
                                              '^medEncDateLast$',
                                              '^medEncounters$',
                                              '^followdate$',
                                              '^FirstTouchDate$',
                                              '^followtime$',
                                              '^ageAtFirstTouch$',
                                              '^ageAtLastTouch$',
                                              '^age$',
                                              '^group$',
                                              '^person_id$',
                                              '^year_of_birth$',
                                              '^care_site_id$',
                                              '^person_source_value$',
                                              '^gender_source_value$',
                                              '^race_source_value$',
                                              '^ethnicity_source_value$',
                                              '^omob_state$'],
                               'source': 'sicklecell_study_rwd.adspatient_scd_rwd'},
                'age': {'inputRegex': ['^personid$',
                                       '^tenant$',
                                       '^first_date$',
                                       '^follow_date$',
                                       '^yearofbirth$',
                                       '^age$'],
                        'source': 'sicklecell_study_rwd.age_scd_rwd'},
                'combinedpheno': {'inputRegex': ['^personid$',
                                                 '^PersonPhenotype$',
                                                 '^IcdPheno$',
                                                 '^SCD$'],
                                  'source': 'sicklecell_study_rwd.combinedpheno_scd_rwd'},
                'encounter': {'inputRegex': ['^personid$',
                                             '^tenant$',
                                             '^encounterid$',
                                             '^facilityids$',
                                             '^reasonforvisit_standard_primaryDisplay$',
                                             '^financialclass_standard_primaryDisplay$',
                                             '^hospitalservice_standard_primaryDisplay$',
                                             '^classification_standard_id$',
                                             '^encType$',
                                             '^type_standard_primaryDisplay$',
                                             '^hospitalizationstartdate$',
                                             '^readmission$',
                                             '^servicedate$',
                                             '^dischargedate$',
                                             '^encountertypes_classification_standard_id$',
                                             '^admissiontype_standard_primaryDisplay$',
                                             '^status_standard_primaryDisplay$',
                                             '^actualarrivaldate$',
                                             '^datetimeEnc$',
                                             '^course_of_therapy$',
                                             '^datetimeCondition$',
                                             '^index_SCD$',
                                             '^last_SCD$',
                                             '^encounter_days$',
                                             '^days_to_next_encounter$',
                                             '^index_therapy_SCD$',
                                             '^last_therapy_SCD$',
                                             '^max_gap$',
                                             '^encounter_days_for_course$'],
                              'source': 'sicklecell_study_rwd.encounter_scd_rwd'},
                'encounterid': {'inputRegex': ['^personid$',
                                               '^tenant$',
                                               '^encounterid$',
                                               '^course_of_therapy$',
                                               '^datetimeCondition$',
                                               '^index_SCD$',
                                               '^last_SCD$',
                                               '^encounter_days$',
                                               '^days_to_next_encounter$',
                                               '^index_therapy_SCD$',
                                               '^last_therapy_SCD$',
                                               '^max_gap$',
                                               '^encounter_days_for_course$'],
                                'source': 'sicklecell_study_rwd.encounterid_scd_rwd'},
                'encsdf': {'inputRegex': ['^personid$',
                                          '^encounterid$',
                                          '^tenant$',
                                          '^servicedate$',
                                          '^dischargedate$',
                                          '^encType$'],
                           'source': 'sicklecell_study_rwd.encsdf_scd_rwd'},
                'evalphenorow': {'inputRegex': ['^RowPhenotype$',
                                                '^personid$',
                                                '^age$',
                                                '^date$',
                                                '^HgbA$',
                                                '^HgbC$',
                                                '^HgbF$',
                                                '^HgbS$',
                                                '^HgbA2$',
                                                '^HgbD$',
                                                '^HgbE$',
                                                '^HgbV$',
                                                '^HgbSum$',
                                                '^CompleteFrac$',
                                                '^AoverC$',
                                                '^AoverD$',
                                                '^AoverE$',
                                                '^AoverV$',
                                                '^AoverS$',
                                                '^CoverS$',
                                                '^DoverS$',
                                                '^EoverS$',
                                                '^VoverS$',
                                                '^HgbA_range$',
                                                '^HgbS_range$',
                                                '^HgbAMin$',
                                                '^HgbSMin$',
                                                '^HgbAMax$',
                                                '^HgbSMax$',
                                                '^InferTfxPerson$',
                                                '^InferTfxRow$',
                                                '^DaysTfx$',
                                                '^PostTfx$'],
                                 'source': 'sicklecell_study_rwd.evalphenorow_scd_rwd'},
                'extractlist': {'inputRegex': ['^personid$',
                                               '^IcdPheno$',
                                               '^PersonPhenotype$'],
                                'source': 'sicklecell_study_rwd.extractlist_scd_rwd'},
                'features': {'inputRegex': ['^Category$',
                                            '^Test/Condition$',
                                            '^Value$',
                                            '^Units/Info$'],
                             'source': 'sicklecell_study_rwd.features_scd_rwd'},
                'final_df': {'inputRegex': ['^personid$',
                                            '^age$',
                                            '^date$',
                                            '^HgbA$',
                                            '^HgbC$',
                                            '^HgbF$',
                                            '^HgbS$',
                                            '^HgbA2$',
                                            '^HgbD$',
                                            '^HgbE$',
                                            '^HgbV$',
                                            '^HgbSum$',
                                            '^CompleteFrac$',
                                            '^AoverC$',
                                            '^AoverD$',
                                            '^AoverE$',
                                            '^AoverV$',
                                            '^AoverS$',
                                            '^CoverS$',
                                            '^DoverS$',
                                            '^EoverS$',
                                            '^VoverS$',
                                            '^HgbA_range$',
                                            '^HgbS_range$',
                                            '^HgbAMin$',
                                            '^HgbSMin$',
                                            '^HgbAMax$',
                                            '^HgbSMax$',
                                            '^InferTfxPerson$',
                                            '^InferTfxRow$',
                                            '^DaysTfx$',
                                            '^PostTfx$'],
                             'source': 'sicklecell_study_rwd.final_df_scd_rwd'},
                'hgb_subunits': {'inputRegex': ['^personid$',
                                                '^labcode_standard_id$',
                                                '^loincclass$',
                                                '^date$',
                                                '^value$',
                                                '^hgbType$',
                                                '^age$',
                                                '^tenant$'],
                                 'source': 'sicklecell_study_rwd.hgb_subunits_scd_rwd'},
                'hydroxpatients': {'inputRegex': ['^personid$', '^first_date$'],
                                   'source': 'sicklecell_study_rwd.hydroxpatients_scd_rwd'},
                'icdsdf': {'inputRegex': ['^personid$',
                                          '^encounterid$',
                                          '^tenant$',
                                          '^servicedate$',
                                          '^dischargedate$',
                                          '^dateCondition$',
                                          '^encType$',
                                          '^conditioncode_standard_id$',
                                          '^IcdCategory$'],
                           'source': 'sicklecell_study_rwd.icdsdf_scd_rwd'},
                'labpersonid': {'inputRegex': ['^personid$',
                                               '^tenant$',
                                               '^course_of_therapy$',
                                               '^encounterid$',
                                               '^datetimeLab$',
                                               '^index_SCD$',
                                               '^last_SCD$',
                                               '^encounter_days$',
                                               '^days_to_next_encounter$',
                                               '^index_therapy_SCD$',
                                               '^last_therapy_SCD$',
                                               '^max_gap$',
                                               '^encounter_days_for_course$'],
                                'source': 'sicklecell_study_rwd.labpersonid_scd_rwd'},
                'labshgb': {'inputRegex': ['^labcode_standard_primaryDisplay$',
                                           '^labcode_standard_codingSystemId$',
                                           '^labcode_standard_id$',
                                           '^labid$',
                                           '^encounterid$',
                                           '^personid$',
                                           '^loincclass$',
                                           '^type$',
                                           '^servicedate$',
                                           '^serviceperiod_startDate$',
                                           '^serviceperiod_endDate$',
                                           '^typedvalue_textValue_value$',
                                           '^typedvalue_numericValue_value$',
                                           '^typedvalue_numericValue_modifier$',
                                           '^typedvalue_unitOfMeasure_standard_id$',
                                           '^typedvalue_unitOfMeasure_standard_codingSystemId$',
                                           '^typedvalue_unitOfMeasure_standard_primaryDisplay$',
                                           '^interpretation_standard_codingSystemId$',
                                           '^interpretation_standard_primaryDisplay$',
                                           '^source$',
                                           '^active$',
                                           '^issueddate$',
                                           '^tenant$',
                                           '^datetimeLab$',
                                           '^Subjects$',
                                           '^lab$'],
                            'source': 'sicklecell_study_rwd.labshgb_scd_rwd'},
                'labshgbcodes': {'inputRegex': ['^Subjects$',
                                                '^labcode_standard_primaryDisplay$',
                                                '^labcode_standard_codingSystemId$',
                                                '^labcode_standard_id$',
                                                '^lab$'],
                                 'source': 'sicklecell_study_rwd.labshgbcodes_scd_rwd'},
                'medhydroxiacodes': {'inputRegex': ['^Subjects$',
                                                    '^drugcode_standard_id$',
                                                    '^drugcode_standard_primaryDisplay$',
                                                    '^drugcode_standard_codingSystemId$'],
                                     'source': 'sicklecell_study_rwd.medhydroxiacodes_scd_rwd'},
                'medshydroxia': {'inputRegex': ['^drugcode_standard_id$',
                                                '^drugcode_standard_codingSystemId$',
                                                '^drugcode_standard_primaryDisplay$',
                                                '^personid$',
                                                '^tenant$',
                                                '^medicationid$',
                                                '^startdate$',
                                                '^stopdate$',
                                                '^prescribingprovider$',
                                                '^datetimeMed$',
                                                '^course_of_therapy$',
                                                '^encounterid$',
                                                '^datetimeLab$',
                                                '^index_SCD$',
                                                '^last_SCD$',
                                                '^encounter_days$',
                                                '^days_to_next_encounter$',
                                                '^index_therapy_SCD$',
                                                '^last_therapy_SCD$',
                                                '^max_gap$',
                                                '^encounter_days_for_course$',
                                                '^Subjects$'],
                                 'source': 'sicklecell_study_rwd.medshydroxia_scd_rwd'},
                'medshydroxiadates': {'inputRegex': ['^personid$', '^date$'],
                                      'source': 'sicklecell_study_rwd.medshydroxiadates_scd_rwd'},
                'omop_conditions': {'inputRegex': ['^person_id$',
                                                   '^measurement_id$',
                                                   '^measurement_concept_id$',
                                                   '^measurement_date$',
                                                   '^measurement_datetime$',
                                                   '^measurement_time$',
                                                   '^measurement_type_concept_id$',
                                                   '^operator_concept_id$',
                                                   '^value_as_number$',
                                                   '^value_as_concept_id$',
                                                   '^unit_concept_id$',
                                                   '^range_low$',
                                                   '^range_high$',
                                                   '^provider_id$',
                                                   '^visit_occurrence_id$',
                                                   '^visit_detail_id$',
                                                   '^measurement_source_value$',
                                                   '^measurement_source_concept_id$',
                                                   '^unit_source_value$',
                                                   '^unit_source_concept_id$',
                                                   '^value_source_value$',
                                                   '^measurement_event_id$',
                                                   '^meas_event_field_concept_id$',
                                                   '^concept_id$',
                                                   '^concept_name$',
                                                   '^domain_id$',
                                                   '^vocabulary_id$',
                                                   '^concept_class_id$',
                                                   '^standard_concept$',
                                                   '^concept_code$',
                                                   '^valid_start_date$',
                                                   '^valid_end_date$',
                                                   '^invalid_reason$'],
                                    'source': 'sicklecell_study_rwd.omop_conditions_scd_rwd'},
                'personiu': {'inputRegex': ['^personid$', '^label$'],
                             'source': 'sicklecell_study_rwd.personiu_scd_rwd'},
                'phenoicd': {'inputRegex': ['^IcdPheno$',
                                            '^personid$',
                                            '^EncCount$',
                                            '^IpCount$',
                                            '^ErCount$',
                                            '^ObCount$',
                                            '^OpCount$',
                                            '^ScdCount$',
                                            '^ThalCount$',
                                            '^TraitCount$',
                                            '^OtherCount$',
                                            '^ScdPercent$'],
                             'source': 'sicklecell_study_rwd.phenoicd_scd_rwd'},
                'phenomatrix': {'inputRegex': ['^PersonPhenotype$',
                                               '^personid$',
                                               '^BetaThalassemia$',
                                               '^HemC_Disease$',
                                               '^No_Phenotype$',
                                               '^Not_SCD$',
                                               '^SCD_Indeterminate$',
                                               '^SCD_SC$',
                                               '^SCD_SCA$',
                                               '^SCD_SCA_Likely$',
                                               '^SCD_SD$',
                                               '^SCD_SE$',
                                               '^SCD_Sbetap_Likely$',
                                               '^S_Indeterminate$',
                                               '^S_Trait$',
                                               '^HgbSMax$',
                                               '^HgbAMax$',
                                               '^HgbSLabCount$',
                                               '^HgbSLabTfxCount$',
                                               '^TfxPercent$'],
                                'source': 'sicklecell_study_rwd.phenomatrix_scd_rwd'},
                'proctransfusion': {'inputRegex': ['^procedurecode_standard_id$',
                                                   '^procedurecode_standard_codingSystemId$',
                                                   '^procedurecode_standard_primaryDisplay$',
                                                   '^personid$',
                                                   '^tenant$',
                                                   '^procedureid$',
                                                   '^modifiercodes_standard_id$',
                                                   '^modifiercodes_standard_codingSystemId$',
                                                   '^modifiercodes_standard_primaryDisplay$',
                                                   '^servicestartdateproc$',
                                                   '^serviceenddateproc$',
                                                   '^principalprovider$',
                                                   '^active$',
                                                   '^source$',
                                                   '^status_standard_id$',
                                                   '^status_standard_codingSystemId$',
                                                   '^status_standard_primaryDisplay$',
                                                   '^datetimeProc$',
                                                   '^course_of_therapy$',
                                                   '^encounterid$',
                                                   '^datetimeLab$',
                                                   '^index_SCD$',
                                                   '^last_SCD$',
                                                   '^encounter_days$',
                                                   '^days_to_next_encounter$',
                                                   '^index_therapy_SCD$',
                                                   '^last_therapy_SCD$',
                                                   '^max_gap$',
                                                   '^encounter_days_for_course$',
                                                   '^Subjects$'],
                                    'source': 'sicklecell_study_rwd.proctransfusion_scd_rwd'},
                'proctransfusioncodes': {'inputRegex': ['^Subjects$',
                                                        '^procedurecode_standard_codingSystemId$',
                                                        '^procedurecode_standard_id$',
                                                        '^procedurecode_standard_primaryDisplay$'],
                                         'source': 'sicklecell_study_rwd.proctransfusioncodes_scd_rwd'},
                'sicklecodes': {'inputRegex': ['^group$', '^codes$'],
                                'source': 'sicklecell_study_rwd.sicklecodes_scd_rwd'},
                'sicklecodesverified': {'inputRegex': ['^SCDCondition$',
                                                       '^Subjects$',
                                                       '^conditioncode_standard_primaryDisplay$',
                                                       '^conditioncode_standard_id$',
                                                       '^conditioncode_standard_codingSystemId$',
                                                       '^group$',
                                                       '^codes$'],
                                        'source': 'sicklecell_study_rwd.sicklecodesverified_scd_rwd'},
                'sickleconditionencounter': {'inputRegex': ['^conditioncode_standard_id$',
                                                            '^conditioncode_standard_codingSystemId$',
                                                            '^conditionid$',
                                                            '^personid$',
                                                            '^encounterid$',
                                                            '^effectivedate$',
                                                            '^asserteddate$',
                                                            '^classification_standard_primaryDisplay$',
                                                            '^confirmationstatus_standard_primaryDisplay$',
                                                            '^responsibleprovider$',
                                                            '^billingrank$',
                                                            '^tenant$',
                                                            '^datetimeCondition$',
                                                            '^SCDCondition$',
                                                            '^Subjects$',
                                                            '^conditioncode_standard_primaryDisplay$',
                                                            '^group$',
                                                            '^codes$'],
                                             'source': 'sicklecell_study_rwd.sickleconditionencounter_scd_rwd'},
                'transfusiondates': {'inputRegex': ['^personid$',
                                                    '^tenant$',
                                                    '^date$'],
                                     'source': 'sicklecell_study_rwd.transfusiondates_scd_rwd'}},
 'debug': True,
 'disease': 'SCD',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'project': 'SickleCell_AI',
 'schema': 'sicklecell_study_rwd',
 'schemaTag': 'RWD'}
2025-12-22 14:18:42,852 - lhn.item - INFO - Create name: adspatient from location: sicklecell_study_rwd.adspatient_scd_rwd
2025-12-22 14:18:42,868 - lhn.item - INFO - Using inputRegex: ['^location_id$', '^personid$', '^PersonPhenotype$', '^IcdPheno$', '^SCD$', '^casePossible$', '^tenant$', '^gender$', '^race$', '^ethnicity$', '^yearofbirth$', '^state$', '^metropolitan$', '^urban$', '^deceased$', '^dateofdeath$', '^encDateFirst$', '^encDateLast$', '^encounters$', '^procEncDateFirst$', '^procEncDateLast$', '^procEncounters$', '^medEncDateFirst$', '^medEncDateLast$', '^medEncounters$', '^followdate$', '^FirstTouchDate$', '^followtime$', '^ageAtFirstTouch$', '^ageAtLastTouch$', '^age$', '^group$', '^person_id$', '^year_of_birth$', '^care_site_id$', '^person_source_value$', '^gender_source_value$', '^race_source_value$', '^ethnicity_source_value$', '^omob_state$']
2025-12-22 14:18:42,880 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(location_id,LongType,true),StructField(personid,StringType,true),StructField(PersonPhenotype,StringType,true),StructField(IcdPheno,StringType,true),StructField(SCD,BooleanType,true),StructField(casePossible,BooleanType,true),StructField(tenant,IntegerType,true),StructField(gender,StringType,true),StructField(race,StringType,true),StructField(ethnicity,StringType,true),StructField(yearofbirth,StringType,true),StructField(state,StringType,true),StructField(metropolitan,StringType,true),StructField(urban,StringType,true),StructField(deceased,BooleanType,true),StructField(dateofdeath,StringType,true),StructField(encDateFirst,DateType,true),StructField(encDateLast,DateType,true),StructField(encounters,LongType,true),StructField(procEncDateFirst,DateType,true),StructField(procEncDateLast,DateType,true),StructField(procEncounters,LongType,true),StructField(medEncDateFirst,DateType,true),StructField(medEncDateLast,DateType,true),StructField(medEncounters,LongType,true),StructField(followdate,DateType,true),StructField(FirstTouchDate,DateType,true),StructField(followtime,IntegerType,true),StructField(ageAtFirstTouch,DoubleType,true),StructField(ageAtLastTouch,DoubleType,true),StructField(age,DoubleType,true),StructField(group,StringType,true),StructField(person_id,LongType,true),StructField(year_of_birth,DateType,true),StructField(care_site_id,LongType,true),StructField(person_source_value,StringType,true),StructField(gender_source_value,StringType,true),StructField(race_source_value,StringType,true),StructField(ethnicity_source_value,StringType,true),StructField(omob_state,StringType,true)))
2025-12-22 14:18:42,983 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:43,132 - lhn.item - INFO - Create name: age from location: sicklecell_study_rwd.age_scd_rwd
2025-12-22 14:18:43,146 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^first_date$', '^follow_date$', '^yearofbirth$', '^age$']
2025-12-22 14:18:43,154 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(first_date,DateType,true),StructField(follow_date,DateType,true),StructField(yearofbirth,DateType,true),StructField(age,IntegerType,true)))
2025-12-22 14:18:43,168 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:43,192 - lhn.item - INFO - Create name: combinedpheno from location: sicklecell_study_rwd.combinedpheno_scd_rwd
2025-12-22 14:18:43,206 - lhn.item - INFO - Using inputRegex: ['^personid$', '^PersonPhenotype$', '^IcdPheno$', '^SCD$']
2025-12-22 14:18:43,213 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(PersonPhenotype,StringType,true),StructField(IcdPheno,StringType,true),StructField(SCD,BooleanType,true)))
2025-12-22 14:18:43,222 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:43,237 - lhn.item - INFO - Create name: encounter from location: sicklecell_study_rwd.encounter_scd_rwd
2025-12-22 14:18:43,252 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^encounterid$', '^facilityids$', '^reasonforvisit_standard_primaryDisplay$', '^financialclass_standard_primaryDisplay$', '^hospitalservice_standard_primaryDisplay$', '^classification_standard_id$', '^encType$', '^type_standard_primaryDisplay$', '^hospitalizationstartdate$', '^readmission$', '^servicedate$', '^dischargedate$', '^encountertypes_classification_standard_id$', '^admissiontype_standard_primaryDisplay$', '^status_standard_primaryDisplay$', '^actualarrivaldate$', '^datetimeEnc$', '^course_of_therapy$', '^datetimeCondition$', '^index_SCD$', '^last_SCD$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_SCD$', '^last_therapy_SCD$', '^max_gap$', '^encounter_days_for_course$']
2025-12-22 14:18:43,253 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(encounterid,StringType,true),StructField(facilityids,ArrayType(StringType,true),true),StructField(reasonforvisit_standard_primaryDisplay,StringType,true),StructField(financialclass_standard_primaryDisplay,StringType,true),StructField(hospitalservice_standard_primaryDisplay,StringType,true),StructField(classification_standard_id,StringType,true),StructField(encType,StringType,true),StructField(type_standard_primaryDisplay,StringType,true),StructField(hospitalizationstartdate,StringType,true),StructField(readmission,BooleanType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(encountertypes_classification_standard_id,ArrayType(StringType,true),true),StructField(admissiontype_standard_primaryDisplay,StringType,true),StructField(status_standard_primaryDisplay,StringType,true),StructField(actualarrivaldate,StringType,true),StructField(datetimeEnc,TimestampType,true),StructField(course_of_therapy,LongType,true),StructField(datetimeCondition,TimestampType,true),StructField(index_SCD,TimestampType,true),StructField(last_SCD,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,TimestampType,true),StructField(last_therapy_SCD,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:43,270 - lhn.spark_utils - INFO - Fields to flatten: arrays=['facilityids', 'encountertypes_classification_standard_id'], structs=[]
2025-12-22 14:18:43,279 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(encounterid,StringType,true),StructField(facilityids,StringType,true),StructField(reasonforvisit_standard_primaryDisplay,StringType,true),StructField(financialclass_standard_primaryDisplay,StringType,true),StructField(hospitalservice_standard_primaryDisplay,StringType,true),StructField(classification_standard_id,StringType,true),StructField(encType,StringType,true),StructField(type_standard_primaryDisplay,StringType,true),StructField(hospitalizationstartdate,StringType,true),StructField(readmission,BooleanType,true),StructField(servicedate,StringType,true),StructField(dischargedate,StringType,true),StructField(encountertypes_classification_standard_id,StringType,true),StructField(admissiontype_standard_primaryDisplay,StringType,true),StructField(status_standard_primaryDisplay,StringType,true),StructField(actualarrivaldate,StringType,true),StructField(datetimeEnc,TimestampType,true),StructField(course_of_therapy,LongType,true),StructField(datetimeCondition,TimestampType,true),StructField(index_SCD,TimestampType,true),StructField(last_SCD,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,TimestampType,true),StructField(last_therapy_SCD,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:43,388 - lhn.item - INFO - Create name: encounterid from location: sicklecell_study_rwd.encounterid_scd_rwd
2025-12-22 14:18:43,402 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^encounterid$', '^course_of_therapy$', '^datetimeCondition$', '^index_SCD$', '^last_SCD$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_SCD$', '^last_therapy_SCD$', '^max_gap$', '^encounter_days_for_course$']
2025-12-22 14:18:43,410 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(encounterid,StringType,true),StructField(course_of_therapy,LongType,true),StructField(datetimeCondition,TimestampType,true),StructField(index_SCD,TimestampType,true),StructField(last_SCD,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,TimestampType,true),StructField(last_therapy_SCD,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:43,441 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:43,490 - lhn.item - INFO - Create name: encsdf from location: sicklecell_study_rwd.encsdf_scd_rwd
2025-12-22 14:18:43,504 - lhn.item - INFO - Using inputRegex: ['^personid$', '^encounterid$', '^tenant$', '^servicedate$', '^dischargedate$', '^encType$']
2025-12-22 14:18:43,513 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(tenant,IntegerType,true),StructField(servicedate,DateType,true),StructField(dischargedate,DateType,true),StructField(encType,StringType,true)))
2025-12-22 14:18:43,527 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:43,551 - lhn.item - INFO - Create name: evalphenorow from location: sicklecell_study_rwd.evalphenorow_scd_rwd
2025-12-22 14:18:43,567 - lhn.item - INFO - Using inputRegex: ['^RowPhenotype$', '^personid$', '^age$', '^date$', '^HgbA$', '^HgbC$', '^HgbF$', '^HgbS$', '^HgbA2$', '^HgbD$', '^HgbE$', '^HgbV$', '^HgbSum$', '^CompleteFrac$', '^AoverC$', '^AoverD$', '^AoverE$', '^AoverV$', '^AoverS$', '^CoverS$', '^DoverS$', '^EoverS$', '^VoverS$', '^HgbA_range$', '^HgbS_range$', '^HgbAMin$', '^HgbSMin$', '^HgbAMax$', '^HgbSMax$', '^InferTfxPerson$', '^InferTfxRow$', '^DaysTfx$', '^PostTfx$']
2025-12-22 14:18:43,576 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(RowPhenotype,StringType,true),StructField(personid,StringType,true),StructField(age,FloatType,true),StructField(date,StringType,true),StructField(HgbA,StringType,true),StructField(HgbC,StringType,true),StructField(HgbF,StringType,true),StructField(HgbS,StringType,true),StructField(HgbA2,StringType,true),StructField(HgbD,StringType,true),StructField(HgbE,StringType,true),StructField(HgbV,FloatType,true),StructField(HgbSum,FloatType,true),StructField(CompleteFrac,StringType,true),StructField(AoverC,StringType,true),StructField(AoverD,StringType,true),StructField(AoverE,StringType,true),StructField(AoverV,StringType,true),StructField(AoverS,StringType,true),StructField(CoverS,StringType,true),StructField(DoverS,StringType,true),StructField(EoverS,StringType,true),StructField(VoverS,StringType,true),StructField(HgbA_range,FloatType,true),StructField(HgbS_range,FloatType,true),StructField(HgbAMin,FloatType,true),StructField(HgbSMin,FloatType,true),StructField(HgbAMax,FloatType,true),StructField(HgbSMax,FloatType,true),StructField(InferTfxPerson,StringType,true),StructField(InferTfxRow,StringType,true),StructField(DaysTfx,FloatType,true),StructField(PostTfx,StringType,true)))
2025-12-22 14:18:43,660 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:43,782 - lhn.item - INFO - Create name: extractlist from location: sicklecell_study_rwd.extractlist_scd_rwd
2025-12-22 14:18:43,796 - lhn.item - INFO - Using inputRegex: ['^personid$', '^IcdPheno$', '^PersonPhenotype$']
2025-12-22 14:18:43,803 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(IcdPheno,StringType,true),StructField(PersonPhenotype,StringType,true)))
2025-12-22 14:18:43,810 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:43,822 - lhn.item - INFO - Create name: features from location: sicklecell_study_rwd.features_scd_rwd
2025-12-22 14:18:43,835 - lhn.item - INFO - Using inputRegex: ['^Category$', '^Test/Condition$', '^Value$', '^Units/Info$']
2025-12-22 14:18:43,842 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Category,StringType,true),StructField(Test/Condition,StringType,true),StructField(Value,StringType,true),StructField(Units/Info,StringType,true)))
2025-12-22 14:18:43,852 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:43,867 - lhn.item - INFO - Create name: final_df from location: sicklecell_study_rwd.final_df_scd_rwd
2025-12-22 14:18:43,882 - lhn.item - INFO - Using inputRegex: ['^personid$', '^age$', '^date$', '^HgbA$', '^HgbC$', '^HgbF$', '^HgbS$', '^HgbA2$', '^HgbD$', '^HgbE$', '^HgbV$', '^HgbSum$', '^CompleteFrac$', '^AoverC$', '^AoverD$', '^AoverE$', '^AoverV$', '^AoverS$', '^CoverS$', '^DoverS$', '^EoverS$', '^VoverS$', '^HgbA_range$', '^HgbS_range$', '^HgbAMin$', '^HgbSMin$', '^HgbAMax$', '^HgbSMax$', '^InferTfxPerson$', '^InferTfxRow$', '^DaysTfx$', '^PostTfx$']
2025-12-22 14:18:43,891 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(age,FloatType,true),StructField(date,StringType,true),StructField(HgbA,StringType,true),StructField(HgbC,StringType,true),StructField(HgbF,StringType,true),StructField(HgbS,StringType,true),StructField(HgbA2,StringType,true),StructField(HgbD,StringType,true),StructField(HgbE,StringType,true),StructField(HgbV,FloatType,true),StructField(HgbSum,FloatType,true),StructField(CompleteFrac,StringType,true),StructField(AoverC,StringType,true),StructField(AoverD,StringType,true),StructField(AoverE,StringType,true),StructField(AoverV,StringType,true),StructField(AoverS,StringType,true),StructField(CoverS,StringType,true),StructField(DoverS,StringType,true),StructField(EoverS,StringType,true),StructField(VoverS,StringType,true),StructField(HgbA_range,FloatType,true),StructField(HgbS_range,FloatType,true),StructField(HgbAMin,FloatType,true),StructField(HgbSMin,FloatType,true),StructField(HgbAMax,FloatType,true),StructField(HgbSMax,FloatType,true),StructField(InferTfxPerson,StringType,true),StructField(InferTfxRow,StringType,true),StructField(DaysTfx,FloatType,true),StructField(PostTfx,StringType,true)))
2025-12-22 14:18:43,971 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:44,090 - lhn.item - INFO - Create name: hgb_subunits from location: sicklecell_study_rwd.hgb_subunits_scd_rwd
2025-12-22 14:18:44,103 - lhn.item - INFO - Using inputRegex: ['^personid$', '^labcode_standard_id$', '^loincclass$', '^date$', '^value$', '^hgbType$', '^age$', '^tenant$']
2025-12-22 14:18:44,111 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(loincclass,StringType,true),StructField(date,DateType,true),StructField(value,StringType,true),StructField(hgbType,StringType,true),StructField(age,IntegerType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:44,130 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:44,160 - lhn.item - INFO - Create name: hydroxpatients from location: sicklecell_study_rwd.hydroxpatients_scd_rwd
2025-12-22 14:18:44,172 - lhn.item - INFO - Using inputRegex: ['^personid$', '^first_date$']
2025-12-22 14:18:44,180 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(first_date,DateType,true)))
2025-12-22 14:18:44,186 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:44,194 - lhn.item - INFO - Create name: icdsdf from location: sicklecell_study_rwd.icdsdf_scd_rwd
2025-12-22 14:18:44,207 - lhn.item - INFO - Using inputRegex: ['^personid$', '^encounterid$', '^tenant$', '^servicedate$', '^dischargedate$', '^dateCondition$', '^encType$', '^conditioncode_standard_id$', '^IcdCategory$']
2025-12-22 14:18:44,217 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(tenant,IntegerType,true),StructField(servicedate,DateType,true),StructField(dischargedate,DateType,true),StructField(dateCondition,DateType,true),StructField(encType,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(IcdCategory,StringType,true)))
2025-12-22 14:18:44,238 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:44,271 - lhn.item - INFO - Create name: labpersonid from location: sicklecell_study_rwd.labpersonid_scd_rwd
2025-12-22 14:18:44,286 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^course_of_therapy$', '^encounterid$', '^datetimeLab$', '^index_SCD$', '^last_SCD$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_SCD$', '^last_therapy_SCD$', '^max_gap$', '^encounter_days_for_course$']
2025-12-22 14:18:44,294 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(course_of_therapy,LongType,true),StructField(encounterid,StringType,true),StructField(datetimeLab,TimestampType,true),StructField(index_SCD,TimestampType,true),StructField(last_SCD,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,TimestampType,true),StructField(last_therapy_SCD,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:44,327 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:44,374 - lhn.item - INFO - Create name: labshgb from location: sicklecell_study_rwd.labshgb_scd_rwd
2025-12-22 14:18:44,392 - lhn.item - INFO - Using inputRegex: ['^labcode_standard_primaryDisplay$', '^labcode_standard_codingSystemId$', '^labcode_standard_id$', '^labid$', '^encounterid$', '^personid$', '^loincclass$', '^type$', '^servicedate$', '^serviceperiod_startDate$', '^serviceperiod_endDate$', '^typedvalue_textValue_value$', '^typedvalue_numericValue_value$', '^typedvalue_numericValue_modifier$', '^typedvalue_unitOfMeasure_standard_id$', '^typedvalue_unitOfMeasure_standard_codingSystemId$', '^typedvalue_unitOfMeasure_standard_primaryDisplay$', '^interpretation_standard_codingSystemId$', '^interpretation_standard_primaryDisplay$', '^source$', '^active$', '^issueddate$', '^tenant$', '^datetimeLab$', '^Subjects$', '^lab$']
2025-12-22 14:18:44,401 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(labcode_standard_primaryDisplay,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod_startDate,StringType,true),StructField(serviceperiod_endDate,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_numericValue_modifier,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(typedvalue_unitOfMeasure_standard_codingSystemId,StringType,true),StructField(typedvalue_unitOfMeasure_standard_primaryDisplay,StringType,true),StructField(interpretation_standard_codingSystemId,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(issueddate,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeLab,TimestampType,true),StructField(Subjects,LongType,true),StructField(lab,StringType,true)))
2025-12-22 14:18:44,491 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:44,584 - lhn.item - INFO - Create name: labshgbcodes from location: sicklecell_study_rwd.labshgbcodes_scd_rwd
2025-12-22 14:18:44,598 - lhn.item - INFO - Using inputRegex: ['^Subjects$', '^labcode_standard_primaryDisplay$', '^labcode_standard_codingSystemId$', '^labcode_standard_id$', '^lab$']
2025-12-22 14:18:44,606 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Subjects,LongType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(lab,StringType,true)))
2025-12-22 14:18:44,618 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:44,639 - lhn.item - INFO - Create name: medhydroxiacodes from location: sicklecell_study_rwd.medhydroxiacodes_scd_rwd
2025-12-22 14:18:44,652 - lhn.item - INFO - Using inputRegex: ['^Subjects$', '^drugcode_standard_id$', '^drugcode_standard_primaryDisplay$', '^drugcode_standard_codingSystemId$']
2025-12-22 14:18:44,659 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Subjects,LongType,true),StructField(drugcode_standard_id,StringType,true),StructField(drugcode_standard_primaryDisplay,StringType,true),StructField(drugcode_standard_codingSystemId,StringType,true)))
2025-12-22 14:18:44,669 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:44,683 - lhn.item - INFO - Create name: medshydroxia from location: sicklecell_study_rwd.medshydroxia_scd_rwd
2025-12-22 14:18:44,698 - lhn.item - INFO - Using inputRegex: ['^drugcode_standard_id$', '^drugcode_standard_codingSystemId$', '^drugcode_standard_primaryDisplay$', '^personid$', '^tenant$', '^medicationid$', '^startdate$', '^stopdate$', '^prescribingprovider$', '^datetimeMed$', '^course_of_therapy$', '^encounterid$', '^datetimeLab$', '^index_SCD$', '^last_SCD$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_SCD$', '^last_therapy_SCD$', '^max_gap$', '^encounter_days_for_course$', '^Subjects$']
2025-12-22 14:18:44,707 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(drugcode_standard_id,StringType,true),StructField(drugcode_standard_codingSystemId,StringType,true),StructField(drugcode_standard_primaryDisplay,StringType,true),StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(medicationid,StringType,true),StructField(startdate,StringType,true),StructField(stopdate,StringType,true),StructField(prescribingprovider,StringType,true),StructField(datetimeMed,TimestampType,true),StructField(course_of_therapy,LongType,true),StructField(encounterid,StringType,true),StructField(datetimeLab,TimestampType,true),StructField(index_SCD,TimestampType,true),StructField(last_SCD,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,TimestampType,true),StructField(last_therapy_SCD,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true),StructField(Subjects,LongType,true)))
2025-12-22 14:18:44,761 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:44,842 - lhn.item - INFO - Create name: medshydroxiadates from location: sicklecell_study_rwd.medshydroxiadates_scd_rwd
2025-12-22 14:18:44,855 - lhn.item - INFO - Using inputRegex: ['^personid$', '^date$']
2025-12-22 14:18:44,864 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(date,DateType,true)))
2025-12-22 14:18:44,869 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:44,878 - lhn.item - INFO - Create name: omop_conditions from location: sicklecell_study_rwd.omop_conditions_scd_rwd
2025-12-22 14:18:44,892 - lhn.item - INFO - Using inputRegex: ['^person_id$', '^measurement_id$', '^measurement_concept_id$', '^measurement_date$', '^measurement_datetime$', '^measurement_time$', '^measurement_type_concept_id$', '^operator_concept_id$', '^value_as_number$', '^value_as_concept_id$', '^unit_concept_id$', '^range_low$', '^range_high$', '^provider_id$', '^visit_occurrence_id$', '^visit_detail_id$', '^measurement_source_value$', '^measurement_source_concept_id$', '^unit_source_value$', '^unit_source_concept_id$', '^value_source_value$', '^measurement_event_id$', '^meas_event_field_concept_id$', '^concept_id$', '^concept_name$', '^domain_id$', '^vocabulary_id$', '^concept_class_id$', '^standard_concept$', '^concept_code$', '^valid_start_date$', '^valid_end_date$', '^invalid_reason$']
2025-12-22 14:18:44,903 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(person_id,LongType,true),StructField(measurement_id,LongType,true),StructField(measurement_concept_id,LongType,true),StructField(measurement_date,DateType,true),StructField(measurement_datetime,TimestampType,true),StructField(measurement_time,StringType,true),StructField(measurement_type_concept_id,LongType,true),StructField(operator_concept_id,LongType,true),StructField(value_as_number,DoubleType,true),StructField(value_as_concept_id,LongType,true),StructField(unit_concept_id,LongType,true),StructField(range_low,DoubleType,true),StructField(range_high,DoubleType,true),StructField(provider_id,LongType,true),StructField(visit_occurrence_id,LongType,true),StructField(visit_detail_id,LongType,true),StructField(measurement_source_value,StringType,true),StructField(measurement_source_concept_id,LongType,true),StructField(unit_source_value,StringType,true),StructField(unit_source_concept_id,LongType,true),StructField(value_source_value,StringType,true),StructField(measurement_event_id,LongType,true),StructField(meas_event_field_concept_id,LongType,true),StructField(concept_id,LongType,true),StructField(concept_name,StringType,true),StructField(domain_id,StringType,true),StructField(vocabulary_id,StringType,true),StructField(concept_class_id,StringType,true),StructField(standard_concept,StringType,true),StructField(concept_code,StringType,true),StructField(valid_start_date,DateType,true),StructField(valid_end_date,DateType,true),StructField(invalid_reason,StringType,true)))
2025-12-22 14:18:44,988 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:45,107 - lhn.item - INFO - Create name: personiu from location: sicklecell_study_rwd.personiu_scd_rwd
2025-12-22 14:18:45,120 - lhn.item - INFO - Using inputRegex: ['^personid$', '^label$']
2025-12-22 14:18:45,128 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(label,StringType,true)))
2025-12-22 14:18:45,137 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:45,145 - lhn.item - INFO - Create name: phenoicd from location: sicklecell_study_rwd.phenoicd_scd_rwd
2025-12-22 14:18:45,158 - lhn.item - INFO - Using inputRegex: ['^IcdPheno$', '^personid$', '^EncCount$', '^IpCount$', '^ErCount$', '^ObCount$', '^OpCount$', '^ScdCount$', '^ThalCount$', '^TraitCount$', '^OtherCount$', '^ScdPercent$']
2025-12-22 14:18:45,167 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(IcdPheno,StringType,true),StructField(personid,StringType,true),StructField(EncCount,StringType,true),StructField(IpCount,StringType,true),StructField(ErCount,StringType,true),StructField(ObCount,StringType,true),StructField(OpCount,StringType,true),StructField(ScdCount,StringType,true),StructField(ThalCount,StringType,true),StructField(TraitCount,StringType,true),StructField(OtherCount,StringType,true),StructField(ScdPercent,StringType,true)))
2025-12-22 14:18:45,197 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:45,241 - lhn.item - INFO - Create name: phenomatrix from location: sicklecell_study_rwd.phenomatrix_scd_rwd
2025-12-22 14:18:45,256 - lhn.item - INFO - Using inputRegex: ['^PersonPhenotype$', '^personid$', '^BetaThalassemia$', '^HemC_Disease$', '^No_Phenotype$', '^Not_SCD$', '^SCD_Indeterminate$', '^SCD_SC$', '^SCD_SCA$', '^SCD_SCA_Likely$', '^SCD_SD$', '^SCD_SE$', '^SCD_Sbetap_Likely$', '^S_Indeterminate$', '^S_Trait$', '^HgbSMax$', '^HgbAMax$', '^HgbSLabCount$', '^HgbSLabTfxCount$', '^TfxPercent$']
2025-12-22 14:18:45,265 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(PersonPhenotype,StringType,true),StructField(personid,StringType,true),StructField(BetaThalassemia,IntegerType,true),StructField(HemC_Disease,IntegerType,true),StructField(No_Phenotype,IntegerType,true),StructField(Not_SCD,IntegerType,true),StructField(SCD_Indeterminate,IntegerType,true),StructField(SCD_SC,IntegerType,true),StructField(SCD_SCA,IntegerType,true),StructField(SCD_SCA_Likely,IntegerType,true),StructField(SCD_SD,IntegerType,true),StructField(SCD_SE,IntegerType,true),StructField(SCD_Sbetap_Likely,IntegerType,true),StructField(S_Indeterminate,IntegerType,true),StructField(S_Trait,IntegerType,true),StructField(HgbSMax,FloatType,true),StructField(HgbAMax,FloatType,true),StructField(HgbSLabCount,FloatType,true),StructField(HgbSLabTfxCount,FloatType,true),StructField(TfxPercent,FloatType,true)))
2025-12-22 14:18:45,315 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:45,388 - lhn.item - INFO - Create name: proctransfusion from location: sicklecell_study_rwd.proctransfusion_scd_rwd
2025-12-22 14:18:45,406 - lhn.item - INFO - Using inputRegex: ['^procedurecode_standard_id$', '^procedurecode_standard_codingSystemId$', '^procedurecode_standard_primaryDisplay$', '^personid$', '^tenant$', '^procedureid$', '^modifiercodes_standard_id$', '^modifiercodes_standard_codingSystemId$', '^modifiercodes_standard_primaryDisplay$', '^servicestartdateproc$', '^serviceenddateproc$', '^principalprovider$', '^active$', '^source$', '^status_standard_id$', '^status_standard_codingSystemId$', '^status_standard_primaryDisplay$', '^datetimeProc$', '^course_of_therapy$', '^encounterid$', '^datetimeLab$', '^index_SCD$', '^last_SCD$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_SCD$', '^last_therapy_SCD$', '^max_gap$', '^encounter_days_for_course$', '^Subjects$']
2025-12-22 14:18:45,407 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(procedurecode_standard_id,StringType,true),StructField(procedurecode_standard_codingSystemId,StringType,true),StructField(procedurecode_standard_primaryDisplay,StringType,true),StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(procedureid,StringType,true),StructField(modifiercodes_standard_id,ArrayType(StringType,true),true),StructField(modifiercodes_standard_codingSystemId,ArrayType(StringType,true),true),StructField(modifiercodes_standard_primaryDisplay,ArrayType(StringType,true),true),StructField(servicestartdateproc,StringType,true),StructField(serviceenddateproc,StringType,true),StructField(principalprovider,StringType,true),StructField(active,BooleanType,true),StructField(source,StringType,true),StructField(status_standard_id,StringType,true),StructField(status_standard_codingSystemId,StringType,true),StructField(status_standard_primaryDisplay,StringType,true),StructField(datetimeProc,TimestampType,true),StructField(course_of_therapy,LongType,true),StructField(encounterid,StringType,true),StructField(datetimeLab,TimestampType,true),StructField(index_SCD,TimestampType,true),StructField(last_SCD,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,TimestampType,true),StructField(last_therapy_SCD,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true),StructField(Subjects,LongType,true)))
2025-12-22 14:18:45,434 - lhn.spark_utils - INFO - Fields to flatten: arrays=['modifiercodes_standard_id', 'modifiercodes_standard_codingSystemId', 'modifiercodes_standard_primaryDisplay'], structs=[]
2025-12-22 14:18:45,447 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(procedurecode_standard_id,StringType,true),StructField(procedurecode_standard_codingSystemId,StringType,true),StructField(procedurecode_standard_primaryDisplay,StringType,true),StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(procedureid,StringType,true),StructField(modifiercodes_standard_id,StringType,true),StructField(modifiercodes_standard_codingSystemId,StringType,true),StructField(modifiercodes_standard_primaryDisplay,StringType,true),StructField(servicestartdateproc,StringType,true),StructField(serviceenddateproc,StringType,true),StructField(principalprovider,StringType,true),StructField(active,BooleanType,true),StructField(source,StringType,true),StructField(status_standard_id,StringType,true),StructField(status_standard_codingSystemId,StringType,true),StructField(status_standard_primaryDisplay,StringType,true),StructField(datetimeProc,TimestampType,true),StructField(course_of_therapy,LongType,true),StructField(encounterid,StringType,true),StructField(datetimeLab,TimestampType,true),StructField(index_SCD,TimestampType,true),StructField(last_SCD,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,TimestampType,true),StructField(last_therapy_SCD,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true),StructField(Subjects,LongType,true)))
2025-12-22 14:18:45,558 - lhn.item - INFO - Create name: proctransfusioncodes from location: sicklecell_study_rwd.proctransfusioncodes_scd_rwd
2025-12-22 14:18:45,572 - lhn.item - INFO - Using inputRegex: ['^Subjects$', '^procedurecode_standard_codingSystemId$', '^procedurecode_standard_id$', '^procedurecode_standard_primaryDisplay$']
2025-12-22 14:18:45,580 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Subjects,LongType,true),StructField(procedurecode_standard_codingSystemId,StringType,true),StructField(procedurecode_standard_id,StringType,true),StructField(procedurecode_standard_primaryDisplay,StringType,true)))
2025-12-22 14:18:45,589 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:45,604 - lhn.item - INFO - Create name: sicklecodes from location: sicklecell_study_rwd.sicklecodes_scd_rwd
2025-12-22 14:18:45,617 - lhn.item - INFO - Using inputRegex: ['^group$', '^codes$']
2025-12-22 14:18:45,625 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:45,630 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:45,641 - lhn.item - INFO - Create name: sicklecodesverified from location: sicklecell_study_rwd.sicklecodesverified_scd_rwd
2025-12-22 14:18:45,654 - lhn.item - INFO - Using inputRegex: ['^SCDCondition$', '^Subjects$', '^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^group$', '^codes$']
2025-12-22 14:18:45,661 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(SCDCondition,StringType,true),StructField(Subjects,LongType,true),StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:45,678 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:45,705 - lhn.item - INFO - Create name: sickleconditionencounter from location: sicklecell_study_rwd.sickleconditionencounter_scd_rwd
2025-12-22 14:18:45,719 - lhn.item - INFO - Using inputRegex: ['^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^conditionid$', '^personid$', '^encounterid$', '^effectivedate$', '^asserteddate$', '^classification_standard_primaryDisplay$', '^confirmationstatus_standard_primaryDisplay$', '^responsibleprovider$', '^billingrank$', '^tenant$', '^datetimeCondition$', '^SCDCondition$', '^Subjects$', '^conditioncode_standard_primaryDisplay$', '^group$', '^codes$']
2025-12-22 14:18:45,727 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(confirmationstatus_standard_primaryDisplay,StringType,true),StructField(responsibleprovider,StringType,true),StructField(billingrank,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeCondition,TimestampType,true),StructField(SCDCondition,StringType,true),StructField(Subjects,LongType,true),StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:45,771 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:45,838 - lhn.item - INFO - Create name: transfusiondates from location: sicklecell_study_rwd.transfusiondates_scd_rwd
2025-12-22 14:18:45,852 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^date$']
2025-12-22 14:18:45,859 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(date,DateType,true)))
2025-12-22 14:18:45,867 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:45,878 - lhn.resource - INFO - processDataTables: processed ss to point to schema sstudySchema
2025-12-22 14:18:45,879 - lhn.resource - INFO - processDataTables: processed s to point to schema sstudySchema
2025-12-22 14:18:45,879 - lhn.resource - INFO - processed property names: {'rwd', 'r', 'proj', 'db', 's', 'ss'}
2025-12-22 14:18:45,880 - lhn.resource - INFO - processed sstudySchema to produce property s and dictionary ss
2025-12-22 14:18:45,880 - lhn.resource - INFO - Can call h.Extract(resource.ss) to get the Extract object
2025-12-22 14:18:45,881 - lhn.resource - INFO - Processing schema name SCDSchema at location sicklecell_rerun
2025-12-22 14:18:45,881 - lhn.resource - INFO - processDataTableBySchemakey: Processing schema name SCDSchema at location sicklecell_rerun (from schemas dictionary)
2025-12-22 14:18:45,882 - lhn.resource - INFO - Found: schema SCDSchema:sicklecell_rerun in callFunProcessDataTables
2025-12-22 14:18:45,882 - lhn.resource - INFO - Element of callFunProcessDataTables used for callFun: 
 {'data_type': 'SCDTables',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'property_name': 'scd',
 'schema_type': 'SCDSchema',
 'tableNameTemplate': None,
 'type_key': 'sickle',
 'updateDict': True}
2025-12-22 14:18:45,927 - lhn.resource - INFO - Found schema SCDSchema at sicklecell_rerun
2025-12-22 14:18:45,927 - lhn.resource - INFO - Updating the Dictionary for SCDSchema based on the schema sicklecell_rerun
2025-12-22 14:18:45,928 - lhn.resource - INFO - Using the tableNameTemplate: _scd_rwd to remove from the table names
2025-12-22 14:18:45,928 - lhn.resource - INFO - Use to call update_dictionary 
 {'debug': True,
 'obs': 100000000,
 'personid': ['personid'],
 'projectSchema': 'sicklecell_ai',
 'reRun': False,
 'schema': 'sicklecell_rerun',
 'schemaTag': 'RWD',
 'schema_dict': None,
 'tableNameTemplate': '_scd_rwd'}
2025-12-22 14:18:48,365 - lhn.resource - INFO - Found schema SCDSchema indicated by schemaTag RWD at sicklecell_rerun
2025-12-22 14:18:48,366 - lhn.resource - INFO - Use to call processDataTables
2025-12-22 14:18:48,377 - lhn.resource - INFO - funCall: {'dataLoc': '/home/hnelson3/work/Users/hnelson3/inst/extdata/SickleCell_AI/',
 'dataTables': {'adspatient': {'inputRegex': ['^location_id$',
                                              '^personid$',
                                              '^PersonPhenotype$',
                                              '^IcdPheno$',
                                              '^SCD$',
                                              '^casePossible$',
                                              '^tenant$',
                                              '^gender$',
                                              '^race$',
                                              '^ethnicity$',
                                              '^yearofbirth$',
                                              '^state$',
                                              '^metropolitan$',
                                              '^urban$',
                                              '^deceased$',
                                              '^dateofdeath$',
                                              '^encDateFirst$',
                                              '^encDateLast$',
                                              '^encounters$',
                                              '^procEncDateFirst$',
                                              '^procEncDateLast$',
                                              '^procEncounters$',
                                              '^medEncDateFirst$',
                                              '^medEncDateLast$',
                                              '^medEncounters$',
                                              '^followdate$',
                                              '^FirstTouchDate$',
                                              '^followtime$',
                                              '^ageAtFirstTouch$',
                                              '^ageAtLastTouch$',
                                              '^age$',
                                              '^group$',
                                              '^person_id$',
                                              '^year_of_birth$',
                                              '^care_site_id$',
                                              '^person_source_value$',
                                              '^gender_source_value$',
                                              '^race_source_value$',
                                              '^ethnicity_source_value$',
                                              '^omob_state$'],
                               'source': 'sicklecell_rerun.adspatient_scd_rwd'},
                'adspatscd': {'inputRegex': ['^location_id$',
                                             '^personid$',
                                             '^PersonPhenotype$',
                                             '^IcdPheno$',
                                             '^scd0$',
                                             '^casePossible$',
                                             '^tenant$',
                                             '^gender$',
                                             '^race$',
                                             '^ethnicity$',
                                             '^yearofbirth$',
                                             '^state$',
                                             '^metropolitan$',
                                             '^urban$',
                                             '^deceased$',
                                             '^dateofdeath$',
                                             '^encDateFirst$',
                                             '^encDateLast$',
                                             '^encounters$',
                                             '^procEncDateFirst$',
                                             '^procEncDateLast$',
                                             '^procEncounters$',
                                             '^medEncDateFirst$',
                                             '^medEncDateLast$',
                                             '^medEncounters$',
                                             '^followdate$',
                                             '^FirstTouchDate$',
                                             '^followtime$',
                                             '^ageAtFirstTouch$',
                                             '^ageAtLastTouch$',
                                             '^age$',
                                             '^group$',
                                             '^person_id$',
                                             '^year_of_birth$',
                                             '^care_site_id$',
                                             '^person_source_value$',
                                             '^gender_source_value$',
                                             '^race_source_value$',
                                             '^ethnicity_source_value$',
                                             '^omob_state$',
                                             '^SCD$'],
                              'source': 'sicklecell_rerun.adspatscd_scd_rwd'},
                'comorbidity_conditionencounter': {'inputRegex': ['^conditioncode_standard_primaryDisplay$',
                                                                  '^conditioncode_standard_id$',
                                                                  '^conditioncode_standard_codingSystemId$',
                                                                  '^conditionid$',
                                                                  '^personid$',
                                                                  '^encounterid$',
                                                                  '^tenant$',
                                                                  '^datetimeCondition$',
                                                                  '^Condition$'],
                                                   'source': 'sicklecell_rerun.comorbidity_conditionencounter_scd_rwd'},
                'comorbidity_icd10_codes': {'inputRegex': ['^Condition$',
                                                           '^conditioncode_standard_id$'],
                                            'source': 'sicklecell_rerun.comorbidity_icd10_codes_scd_rwd'},
                'comorbidity_icd10_codes_verified': {'inputRegex': ['^regexgroup$',
                                                                    '^Subjects$',
                                                                    '^conditioncode_standard_primaryDisplay$',
                                                                    '^conditioncode_standard_id$',
                                                                    '^conditioncode_standard_codingSystemId$',
                                                                    '^Condition$'],
                                                     'source': 'sicklecell_rerun.comorbidity_icd10_codes_verified_scd_rwd'},
                'comorbidity_icd10_conditionencounter': {'inputRegex': ['^conditioncode_standard_primaryDisplay$',
                                                                        '^conditioncode_standard_id$',
                                                                        '^conditioncode_standard_codingSystemId$',
                                                                        '^conditionid$',
                                                                        '^personid$',
                                                                        '^encounterid$',
                                                                        '^tenant$',
                                                                        '^datetimeCondition$',
                                                                        '^Condition$'],
                                                         'source': 'sicklecell_rerun.comorbidity_icd10_conditionencounter_scd_rwd'},
                'comorbidity_icd9_codes': {'inputRegex': ['^Condition$',
                                                          '^conditioncode_standard_id$'],
                                           'source': 'sicklecell_rerun.comorbidity_icd9_codes_scd_rwd'},
                'comorbidity_icd9_codes_verified': {'inputRegex': ['^regexgroup$',
                                                                   '^Subjects$',
                                                                   '^conditioncode_standard_primaryDisplay$',
                                                                   '^conditioncode_standard_id$',
                                                                   '^conditioncode_standard_codingSystemId$',
                                                                   '^Condition$'],
                                                    'source': 'sicklecell_rerun.comorbidity_icd9_codes_verified_scd_rwd'},
                'comorbidity_icd9_conditionencounter': {'inputRegex': ['^conditioncode_standard_primaryDisplay$',
                                                                       '^conditioncode_standard_id$',
                                                                       '^conditioncode_standard_codingSystemId$',
                                                                       '^conditionid$',
                                                                       '^personid$',
                                                                       '^encounterid$',
                                                                       '^tenant$',
                                                                       '^datetimeCondition$',
                                                                       '^Condition$'],
                                                        'source': 'sicklecell_rerun.comorbidity_icd9_conditionencounter_scd_rwd'},
                'comorbidity_procedure_codes': {'inputRegex': ['^Procedure$',
                                                               '^procedurecode_standard_id$',
                                                               '^Description$'],
                                                'source': 'sicklecell_rerun.comorbidity_procedure_codes_scd_rwd'},
                'comorbidity_procedure_codes_verified': {'inputRegex': ['^regexgroup$',
                                                                        '^Subjects$',
                                                                        '^procedurecode_standard_codingSystemId$',
                                                                        '^procedurecode_standard_id$',
                                                                        '^procedurecode_standard_primaryDisplay$',
                                                                        '^Procedure$',
                                                                        '^Description$'],
                                                         'source': 'sicklecell_rerun.comorbidity_procedure_codes_verified_scd_rwd'},
                'comorbidity_procedureencounter': {'inputRegex': ['^procedurecode_standard_primaryDisplay$',
                                                                  '^procedurecode_standard_id$',
                                                                  '^procedurecode_standard_codingSystemId$',
                                                                  '^procedureid$',
                                                                  '^personid$',
                                                                  '^encounterid$',
                                                                  '^servicestartdateproc$',
                                                                  '^serviceenddateproc$',
                                                                  '^tenant$',
                                                                  '^datetimeProc$',
                                                                  '^dateProc$'],
                                                   'source': 'sicklecell_rerun.comorbidity_procedureencounter_scd_rwd'},
                'comorbidity_procedureencounterfeature': {'inputRegex': ['^personid$',
                                                                         '^tenant$',
                                                                         '^procedurecode_standard_id$',
                                                                         '^dateProc$'],
                                                          'source': 'sicklecell_rerun.comorbidity_procedureencounterfeature_scd_rwd'},
                'corrections': {'inputRegex': ['^personid$',
                                               '^tenant$',
                                               '^financialclass_standard_primaryDisplay$',
                                               '^encType$',
                                               '^servicedate$',
                                               '^dischargedate$',
                                               '^datetimeEnc$',
                                               '^dateEnc$'],
                                'source': 'sicklecell_rerun.corrections_scd_rwd'},
                'death': {'inputRegex': ['^personid$',
                                         '^tenant$',
                                         '^deceased$',
                                         '^dateofdeath$'],
                          'source': 'sicklecell_rerun.death_scd_rwd'},
                'demo': {'inputRegex': ['^personid$',
                                        '^tenant$',
                                        '^gender$',
                                        '^race$',
                                        '^ethnicity$',
                                        '^yearofbirth$',
                                        '^state$',
                                        '^metropolitan$',
                                        '^urban$',
                                        '^birthdate$',
                                        '^deceased$',
                                        '^dateofdeath$',
                                        '^encDateFirst$',
                                        '^encDateLast$',
                                        '^encounters$',
                                        '^conditionEncDateFirst$',
                                        '^conditionEncDateLast$',
                                        '^conditionEncounters$',
                                        '^labEncDateFirst$',
                                        '^labEncDateLast$',
                                        '^labEncounters$',
                                        '^procEncDateFirst$',
                                        '^procEncDateLast$',
                                        '^procEncounters$',
                                        '^medEncDateFirst$',
                                        '^medEncDateLast$',
                                        '^medEncounters$',
                                        '^followdate$',
                                        '^FirstTouchDate$',
                                        '^followtime$',
                                        '^maritalstatus_standard_primaryDisplay$',
                                        '^maritalstatus$',
                                        '^ageAtFirstTouch$',
                                        '^ageAtLastTouch$',
                                        '^ageAtDeath$',
                                        '^ageCurrent$',
                                        '^age$'],
                         'source': 'sicklecell_rerun.demo_scd_rwd'},
                'demoomop': {'inputRegex': ['^personid$',
                                            '^gender$',
                                            '^race$',
                                            '^ethnicity$',
                                            '^yearofbirth$',
                                            '^state$',
                                            '^metropolitan$',
                                            '^urban$',
                                            '^birthdate$',
                                            '^deceased$',
                                            '^dateofdeath$',
                                            '^encDateFirst$',
                                            '^encDateLast$',
                                            '^encounters$',
                                            '^conditionEncDateFirst$',
                                            '^conditionEncDateLast$',
                                            '^conditionEncounters$',
                                            '^labEncDateFirst$',
                                            '^labEncDateLast$',
                                            '^labEncounters$',
                                            '^procEncDateFirst$',
                                            '^procEncDateLast$',
                                            '^procEncounters$',
                                            '^medEncDateFirst$',
                                            '^medEncDateLast$',
                                            '^medEncounters$',
                                            '^followdate$',
                                            '^FirstTouchDate$',
                                            '^followtime$',
                                            '^maritalstatus_standard_primaryDisplay$',
                                            '^maritalstatus$',
                                            '^ageAtFirstTouch$',
                                            '^ageAtLastTouch$',
                                            '^ageAtDeath$',
                                            '^ageCurrent$',
                                            '^age$',
                                            '^person_id$',
                                            '^location_id$',
                                            '^care_site_id$',
                                            '^person_source_value$',
                                            '^year_of_birth$',
                                            '^gender_source_value$',
                                            '^race_source_value$',
                                            '^ethnicity_source_value$',
                                            '^stateOMOP$',
                                            '^Hospital_Type$',
                                            '^zip$',
                                            '^death_date$',
                                            '^death_type_concept_id$',
                                            '^tenant$'],
                             'source': 'sicklecell_rerun.demoomop_scd_rwd'},
                'drug_codes': {'inputRegex': ['^drugcode_standard_id$',
                                              '^drugcode_standard_primaryDisplay$',
                                              '^drugcode_standard_codingSystemId$'],
                               'source': 'sicklecell_rerun.drug_codes_scd_rwd'},
                'encounter': {'inputRegex': ['^personid$',
                                             '^tenant$',
                                             '^encounterid$',
                                             '^facilityids$',
                                             '^financialclass_standard_primaryDisplay$',
                                             '^hospitalservice_standard_primaryDisplay$',
                                             '^classification_standard_id$',
                                             '^classification_standard_codingSystemId$',
                                             '^encType$',
                                             '^type_standard_primaryDisplay$',
                                             '^servicedate$',
                                             '^dischargedate$',
                                             '^actualarrivaldate$',
                                             '^datetimeEnc$',
                                             '^dateEnc$',
                                             '^course_of_therapy$',
                                             '^classification_standard_primaryDisplay$',
                                             '^confirmationstatus_standard_primaryDisplay$',
                                             '^group$',
                                             '^datetimeCondition$',
                                             '^index_SCD$',
                                             '^last_SCD$',
                                             '^encounter_days$',
                                             '^days_to_next_encounter$',
                                             '^index_therapy_SCD$',
                                             '^last_therapy_SCD$',
                                             '^max_gap$',
                                             '^encounter_days_for_course$'],
                              'source': 'sicklecell_rerun.encounter_scd_rwd'},
                'encounterextract': {'inputRegex': ['^personid$',
                                                    '^tenant$',
                                                    '^encounterid$',
                                                    '^encType$',
                                                    '^dateCondition$'],
                                     'source': 'sicklecell_rerun.encounterextract_scd_rwd'},
                'encounterid': {'inputRegex': ['^personid$',
                                               '^tenant$',
                                               '^encounterid$',
                                               '^course_of_therapy$',
                                               '^classification_standard_primaryDisplay$',
                                               '^confirmationstatus_standard_primaryDisplay$',
                                               '^group$',
                                               '^datetimeCondition$',
                                               '^index_SCD$',
                                               '^last_SCD$',
                                               '^encounter_days$',
                                               '^days_to_next_encounter$',
                                               '^index_therapy_SCD$',
                                               '^last_therapy_SCD$',
                                               '^max_gap$',
                                               '^encounter_days_for_course$'],
                                'source': 'sicklecell_rerun.encounterid_scd_rwd'},
                'evalphenorow': {'inputRegex': ['^RowPhenotype$',
                                                '^personid$',
                                                '^age$',
                                                '^servicedate$',
                                                '^HgbA$',
                                                '^HgbC$',
                                                '^HgbF$',
                                                '^HgbS$',
                                                '^HgbA2$',
                                                '^HgbD$',
                                                '^HgbE$',
                                                '^HgbV$',
                                                '^HgbSum$',
                                                '^CompleteFrac$',
                                                '^AoverC$',
                                                '^AoverD$',
                                                '^AoverE$',
                                                '^AoverV$',
                                                '^AoverS$',
                                                '^CoverS$',
                                                '^DoverS$',
                                                '^EoverS$',
                                                '^VoverS$',
                                                '^HgbA_range$',
                                                '^HgbS_range$',
                                                '^HgbAMin$',
                                                '^HgbSMin$',
                                                '^HgbAMax$',
                                                '^HgbSMax$',
                                                '^InferTfxPerson$',
                                                '^InferTfxRow$',
                                                '^date$',
                                                '^DaysTfx$',
                                                '^PostTfx$'],
                                 'source': 'sicklecell_rerun.evalphenorow_scd_rwd'},
                'follow': {'inputRegex': ['^personid$',
                                          '^tenant$',
                                          '^encDateFirst$',
                                          '^encDateLast$',
                                          '^encounters$',
                                          '^conditionEncDateFirst$',
                                          '^conditionEncDateLast$',
                                          '^conditionEncounters$',
                                          '^labEncDateFirst$',
                                          '^labEncDateLast$',
                                          '^labEncounters$',
                                          '^procEncDateFirst$',
                                          '^procEncDateLast$',
                                          '^procEncounters$',
                                          '^medEncDateFirst$',
                                          '^medEncDateLast$',
                                          '^medEncounters$',
                                          '^followdate$',
                                          '^FirstTouchDate$',
                                          '^followtime$'],
                           'source': 'sicklecell_rerun.follow_scd_rwd'},
                'followcondition': {'inputRegex': ['^personid$',
                                                   '^tenant$',
                                                   '^conditionEncDateFirst$',
                                                   '^conditionEncDateLast$',
                                                   '^conditionEncounters$'],
                                    'source': 'sicklecell_rerun.followcondition_scd_rwd'},
                'followenc': {'inputRegex': ['^personid$',
                                             '^tenant$',
                                             '^encDateFirst$',
                                             '^encDateLast$',
                                             '^encounters$'],
                              'source': 'sicklecell_rerun.followenc_scd_rwd'},
                'followlab': {'inputRegex': ['^personid$',
                                             '^tenant$',
                                             '^labEncDateFirst$',
                                             '^labEncDateLast$',
                                             '^labEncounters$'],
                              'source': 'sicklecell_rerun.followlab_scd_rwd'},
                'followmed_rwd_': {'inputRegex': ['^personid$',
                                                  '^tenant$',
                                                  '^medEncDateFirst$',
                                                  '^medEncDateLast$',
                                                  '^medEncounters$'],
                                   'source': 'sicklecell_rerun.followmed_rwd_'},
                'followproc': {'inputRegex': ['^personid$',
                                              '^tenant$',
                                              '^procEncDateFirst$',
                                              '^procEncDateLast$',
                                              '^procEncounters$'],
                               'source': 'sicklecell_rerun.followproc_scd_rwd'},
                'gap_conditionid': {'inputRegex': ['^personid$',
                                                   '^tenant$',
                                                   '^course_of_therapy$',
                                                   '^encounterid$',
                                                   '^dateCondition$',
                                                   '^datetimeCondition$',
                                                   '^index_SCD$',
                                                   '^last_SCD$',
                                                   '^encounter_days$',
                                                   '^days_to_next_encounter$',
                                                   '^index_therapy_SCD$',
                                                   '^last_therapy_SCD$',
                                                   '^max_gap$',
                                                   '^encounter_days_for_course$'],
                                    'source': 'sicklecell_rerun.gap_conditionid_scd_rwd'},
                'gap_condtionid': {'inputRegex': ['^personid$',
                                                  '^tenant$',
                                                  '^course_of_therapy$',
                                                  '^encounterid$',
                                                  '^dischargedate$',
                                                  '^datetimeEnc$',
                                                  '^dateEnc$',
                                                  '^servicedate$',
                                                  '^index_gap$',
                                                  '^last_gap$',
                                                  '^encounter_days$',
                                                  '^days_to_next_encounter$',
                                                  '^index_therapy_gap$',
                                                  '^last_therapy_gap$',
                                                  '^max_gap$',
                                                  '^encounter_days_for_course$'],
                                   'source': 'sicklecell_rerun.gap_condtionid_scd_rwd'},
                'gap_encounterid': {'inputRegex': ['^personid$',
                                                   '^tenant$',
                                                   '^course_of_therapy$',
                                                   '^encounterid$',
                                                   '^datetimeEnc$',
                                                   '^dateEnc$',
                                                   '^dischargedate$',
                                                   '^servicedate$',
                                                   '^index_gap$',
                                                   '^last_gap$',
                                                   '^encounter_days$',
                                                   '^days_to_next_encounter$',
                                                   '^index_therapy_gap$',
                                                   '^last_therapy_gap$',
                                                   '^max_gap$',
                                                   '^encounter_days_for_course$'],
                                    'source': 'sicklecell_rerun.gap_encounterid_scd_rwd'},
                'hgb_subunits': {'inputRegex': ['^personid$',
                                                '^labcode_standard_id$',
                                                '^loincclass$',
                                                '^servicedate$',
                                                '^value$',
                                                '^hgbType$',
                                                '^age$',
                                                '^tenant$'],
                                 'source': 'sicklecell_rerun.hgb_subunits_scd_rwd'},
                'hydroxyurea': {'inputRegex': ['^personid$', '^date$'],
                                'source': 'sicklecell_rerun.hydroxyurea_scd_rwd'},
                'insurance': {'inputRegex': ['^personid$',
                                             '^tenant$',
                                             '^financialclass_standard_primaryDisplay$',
                                             '^encType$',
                                             '^servicedate$',
                                             '^dischargedate$',
                                             '^datetimeEnc$',
                                             '^dateEnc$'],
                              'source': 'sicklecell_rerun.insurance_scd_rwd'},
                'insurance2': {'inputRegex': ['^personid$',
                                              '^tenant$',
                                              '^financialclass_standard_primaryDisplay$',
                                              '^encType$',
                                              '^servicedate$',
                                              '^dischargedate$',
                                              '^datetimeEnc$',
                                              '^dateEnc$'],
                               'source': 'sicklecell_rerun.insurance2_scd_rwd'},
                'insuranceperiods': {'inputRegex': ['^personid$',
                                                    '^tenant$',
                                                    '^financialclass_standard_primaryDisplay$',
                                                    '^course_of_therapy$',
                                                    '^encType$',
                                                    '^servicedate$',
                                                    '^dischargedate$',
                                                    '^dateEnc$',
                                                    '^datetimeEnc$',
                                                    '^index_insurance$',
                                                    '^last_insurance$',
                                                    '^encounter_days$',
                                                    '^days_to_next_encounter$',
                                                    '^index_therapy_insurance$',
                                                    '^last_therapy_insurance$',
                                                    '^max_gap$',
                                                    '^encounter_days_for_course$'],
                                     'source': 'sicklecell_rerun.insuranceperiods_scd_rwd'},
                'lab_features': {'inputRegex': ['^labcode_standard_id$',
                                                '^labcode_standard_codingSystemId$',
                                                '^labid$',
                                                '^personid$',
                                                '^labcode_standard_primaryDisplay$',
                                                '^loincclass$',
                                                '^typedvalue_textValue_value$',
                                                '^typedvalue_numericValue_value$',
                                                '^tenant$',
                                                '^datetimeLab$',
                                                '^feature$'],
                                 'source': 'sicklecell_rerun.lab_features_scd_rwd'},
                'lab_features_codes': {'inputRegex': ['^feature$',
                                                      '^labcode_standard_id$',
                                                      '^labcode_standard_codingSystemId$'],
                                       'source': 'sicklecell_rerun.lab_features_codes_scd_rwd'},
                'lab_loinc_codes': {'inputRegex': ['^Test$',
                                                   '^labcode_standard_id$',
                                                   '^Description$'],
                                    'source': 'sicklecell_rerun.lab_loinc_codes_scd_rwd'},
                'lab_loinc_codes_regex': {'inputRegex': ['^Test$',
                                                         '^labcode_standard_id$',
                                                         '^Description$',
                                                         '^Regex$'],
                                          'source': 'sicklecell_rerun.lab_loinc_codes_regex_scd_rwd'},
                'lab_loinc_codes_verified': {'inputRegex': ['^regexgroup$',
                                                            '^Subjects$',
                                                            '^labcode_standard_primaryDisplay$',
                                                            '^labcode_standard_codingSystemId$',
                                                            '^labcode_standard_id$',
                                                            '^Test$',
                                                            '^Description$',
                                                            '^Regex$'],
                                             'source': 'sicklecell_rerun.lab_loinc_codes_verified_scd_rwd'},
                'lab_loincencounter': {'inputRegex': ['^labcode_standard_primaryDisplay$',
                                                      '^labcode_standard_id$',
                                                      '^labcode_standard_codingSystemId$',
                                                      '^labid$',
                                                      '^encounterid$',
                                                      '^personid$',
                                                      '^loincclass$',
                                                      '^servicedate$',
                                                      '^typedvalue_textValue_value$',
                                                      '^typedvalue_numericValue_value$',
                                                      '^typedvalue_unitOfMeasure_standard_id$',
                                                      '^interpretation_standard_primaryDisplay$',
                                                      '^tenant$',
                                                      '^datetimeLab$',
                                                      '^dateLab$',
                                                      '^Test$'],
                                       'source': 'sicklecell_rerun.lab_loincencounter_scd_rwd'},
                'lab_loincencounterbaseline': {'inputRegex': ['^personid$',
                                                              '^Test$',
                                                              '^tenant$',
                                                              '^SCD$',
                                                              '^labcode_standard_id$',
                                                              '^labcode_standard_primaryDisplay$',
                                                              '^loincclass$',
                                                              '^first_test_date$',
                                                              '^last_test_date$',
                                                              '^testing_period_days$',
                                                              '^total_tests$',
                                                              '^tests_with_numeric_value$',
                                                              '^average_value$',
                                                              '^min_value$',
                                                              '^max_value$',
                                                              '^stddev_value$',
                                                              '^avg_change_per_day$',
                                                              '^trend_description$'],
                                               'source': 'sicklecell_rerun.lab_loincencounterbaseline_scd_rwd'},
                'lab_loincencounterdemo': {'inputRegex': ['^personid$',
                                                          '^tenant$',
                                                          '^dateLab$',
                                                          '^Test$',
                                                          '^labcode_standard_primaryDisplay$',
                                                          '^labcode_standard_id$',
                                                          '^labcode_standard_codingSystemId$',
                                                          '^loincclass$',
                                                          '^typedvalue_textValue_value$',
                                                          '^typedvalue_numericValue_value$',
                                                          '^typedvalue_unitOfMeasure_standard_id$',
                                                          '^interpretation_standard_primaryDisplay$',
                                                          '^SCD$',
                                                          '^yearofbirth$',
                                                          '^FirstTouchDate$',
                                                          '^followdate$',
                                                          '^deceased$',
                                                          '^followed_from_birth$',
                                                          '^baseline_12$'],
                                           'source': 'sicklecell_rerun.lab_loincencounterdemo_scd_rwd'},
                'lab_loincencounterdemobaseline': {'inputRegex': ['^personid$',
                                                                  '^tenant$',
                                                                  '^dateLab$',
                                                                  '^Test$',
                                                                  '^labcode_standard_id$',
                                                                  '^labcode_standard_codingSystemId$',
                                                                  '^loincclass$',
                                                                  '^typedvalue_textValue_value$',
                                                                  '^typedvalue_numericValue_value$',
                                                                  '^typedvalue_unitOfMeasure_standard_id$',
                                                                  '^interpretation_standard_primaryDisplay$',
                                                                  '^SCD$',
                                                                  '^yearofbirth$',
                                                                  '^FirstTouchDate$',
                                                                  '^followdate$',
                                                                  '^deceased$',
                                                                  '^followed_from_birth$',
                                                                  '^baseline_12$'],
                                                   'source': 'sicklecell_rerun.lab_loincencounterdemobaseline_scd_rwd'},
                'lab_loincencounterfeature': {'inputRegex': ['^personid$',
                                                             '^tenant$',
                                                             '^dateLab$',
                                                             '^labcode_standard_id$',
                                                             '^labvalue$'],
                                              'source': 'sicklecell_rerun.lab_loincencounterfeature_scd_rwd'},
                'lablist': {'inputRegex': ['^personid$',
                                           '^Test$',
                                           '^tenant$',
                                           '^labcode_standard_id$',
                                           '^labcode_standard_primaryDisplay$',
                                           '^loincclass$',
                                           '^first_test_date$',
                                           '^last_test_date$',
                                           '^testing_period_days$',
                                           '^total_tests$',
                                           '^tests_with_numeric_value$',
                                           '^average_value$',
                                           '^min_value$',
                                           '^max_value$',
                                           '^stddev_value$',
                                           '^avg_change_per_day$',
                                           '^trend_description$'],
                            'source': 'sicklecell_rerun.lablist_scd_rwd'},
                'labpersonid': {'inputRegex': ['^personid$',
                                               '^tenant$',
                                               '^course_of_therapy$',
                                               '^labcode_standard_primaryDisplay$',
                                               '^typedvalue_textValue_value$',
                                               '^typedvalue_numericValue_value$',
                                               '^typedvalue_unitOfMeasure_standard_primaryDisplay$',
                                               '^interpretation_standard_primaryDisplay$',
                                               '^lab$',
                                               '^source$',
                                               '^Subjects$',
                                               '^datetimeLab$',
                                               '^index_LAB$',
                                               '^last_LAB$',
                                               '^encounter_days$',
                                               '^days_to_next_encounter$',
                                               '^index_therapy_LAB$',
                                               '^last_therapy_LAB$',
                                               '^max_gap$',
                                               '^encounter_days_for_course$'],
                                'source': 'sicklecell_rerun.labpersonid_scd_rwd'},
                'labshgb': {'inputRegex': ['^labcode_standard_id$',
                                           '^labcode_standard_codingSystemId$',
                                           '^labcode_standard_primaryDisplay$',
                                           '^labid$',
                                           '^encounterid$',
                                           '^personid$',
                                           '^loincclass$',
                                           '^type$',
                                           '^servicedate$',
                                           '^serviceperiod_startDate$',
                                           '^serviceperiod_endDate$',
                                           '^typedvalue_textValue_value$',
                                           '^typedvalue_numericValue_value$',
                                           '^typedvalue_numericValue_modifier$',
                                           '^typedvalue_unitOfMeasure_standard_id$',
                                           '^typedvalue_unitOfMeasure_standard_codingSystemId$',
                                           '^typedvalue_unitOfMeasure_standard_primaryDisplay$',
                                           '^interpretation_standard_codingSystemId$',
                                           '^interpretation_standard_primaryDisplay$',
                                           '^source$',
                                           '^active$',
                                           '^issueddate$',
                                           '^tenant$',
                                           '^datetimeLab$',
                                           '^dateLab$',
                                           '^Subjects$',
                                           '^lab$'],
                            'source': 'sicklecell_rerun.labshgb_scd_rwd'},
                'labshgbcodes': {'inputRegex': ['^Subjects$',
                                                '^labcode_standard_primaryDisplay$',
                                                '^labcode_standard_codingSystemId$',
                                                '^labcode_standard_id$',
                                                '^lab$'],
                                 'source': 'sicklecell_rerun.labshgbcodes_scd_rwd'},
                'labshydroxyuria': {'inputRegex': ['^labid$',
                                                   '^encounterid$',
                                                   '^personid$',
                                                   '^labcode_standard_id$',
                                                   '^labcode_standard_codingSystemId$',
                                                   '^labcode_standard_primaryDisplay$',
                                                   '^loincclass$',
                                                   '^type$',
                                                   '^servicedate$',
                                                   '^serviceperiod_startDate$',
                                                   '^serviceperiod_endDate$',
                                                   '^typedvalue_textValue_value$',
                                                   '^typedvalue_numericValue_value$',
                                                   '^typedvalue_numericValue_modifier$',
                                                   '^typedvalue_unitOfMeasure_standard_id$',
                                                   '^typedvalue_unitOfMeasure_standard_codingSystemId$',
                                                   '^typedvalue_unitOfMeasure_standard_primaryDisplay$',
                                                   '^interpretation_standard_codingSystemId$',
                                                   '^interpretation_standard_primaryDisplay$',
                                                   '^source$',
                                                   '^active$',
                                                   '^issueddate$',
                                                   '^tenant$',
                                                   '^datetimeLab$',
                                                   '^dateLab$'],
                                    'source': 'sicklecell_rerun.labshydroxyuria_scd_rwd'},
                'loinc_codes': {'inputRegex': ['^group$', '^codes$'],
                                'source': 'sicklecell_rerun.loinc_codes_scd_rwd'},
                'loinc_codesverified': {'inputRegex': ['^regexgroup$',
                                                       '^Subjects$',
                                                       '^labcode_standard_primaryDisplay$',
                                                       '^labcode_standard_codingSystemId$',
                                                       '^labcode_standard_id$',
                                                       '^group$',
                                                       '^codes$'],
                                        'source': 'sicklecell_rerun.loinc_codesverified_scd_rwd'},
                'loincresults': {'inputRegex': ['^labcode_standard_primaryDisplay$',
                                                '^labcode_standard_id$',
                                                '^labcode_standard_codingSystemId$',
                                                '^labid$',
                                                '^encounterid$',
                                                '^personid$',
                                                '^loincclass$',
                                                '^type$',
                                                '^servicedate$',
                                                '^serviceperiod_startDate$',
                                                '^serviceperiod_endDate$',
                                                '^typedvalue_textValue_value$',
                                                '^typedvalue_numericValue_value$',
                                                '^typedvalue_numericValue_modifier$',
                                                '^typedvalue_unitOfMeasure_standard_id$',
                                                '^typedvalue_unitOfMeasure_standard_codingSystemId$',
                                                '^typedvalue_unitOfMeasure_standard_primaryDisplay$',
                                                '^interpretation_standard_codingSystemId$',
                                                '^interpretation_standard_primaryDisplay$',
                                                '^source$',
                                                '^active$',
                                                '^issueddate$',
                                                '^tenant$',
                                                '^datetimeLab$',
                                                '^dateLab$',
                                                '^regexgroup$',
                                                '^Subjects$',
                                                '^group$',
                                                '^codes$'],
                                 'source': 'sicklecell_rerun.loincresults_scd_rwd'},
                'loincresultsminimal': {'inputRegex': ['^personid$',
                                                       '^tenant$',
                                                       '^dateLab$',
                                                       '^group$',
                                                       '^labcode_standard_id$',
                                                       '^typedvalue_numericValue_value$',
                                                       '^typedvalue_unitOfMeasure_standard_id$'],
                                        'source': 'sicklecell_rerun.loincresultsminimal_scd_rwd'},
                'maritalstatus': {'inputRegex': ['^personid$',
                                                 '^tenant$',
                                                 '^maritalstatus_standard_primaryDisplay$',
                                                 '^maritalstatus$'],
                                  'source': 'sicklecell_rerun.maritalstatus_scd_rwd'},
                'medhydroxiacodes': {'inputRegex': ['^Subjects$',
                                                    '^drugcode_standard_id$',
                                                    '^drugcode_standard_primaryDisplay$',
                                                    '^drugcode_standard_codingSystemId$'],
                                     'source': 'sicklecell_rerun.medhydroxiacodes_scd_rwd'},
                'medication_encounterdemobaseline': {'inputRegex': ['^personid$',
                                                                    '^tenant$',
                                                                    '^dateMed$',
                                                                    '^drugcode_standard_id$',
                                                                    '^drugcode_standard_primaryDisplay$',
                                                                    '^startDate$',
                                                                    '^stopDate$',
                                                                    '^SCD$',
                                                                    '^yearofbirth$',
                                                                    '^FirstTouchDate$',
                                                                    '^followdate$',
                                                                    '^deceased$',
                                                                    '^followed_from_birth$',
                                                                    '^baseline_period$'],
                                                     'source': 'sicklecell_rerun.medication_encounterdemobaseline_scd_rwd'},
                'medication_encountermeasures': {'inputRegex': ['^personid$',
                                                                '^tenant$',
                                                                '^drugcode_standard_id$',
                                                                '^drugcode_standard_primaryDisplay$',
                                                                '^item_count$',
                                                                '^distinct_dates$',
                                                                '^first_date$',
                                                                '^last_date$',
                                                                '^date_span$',
                                                                '^avg_item_duration$',
                                                                '^min_item_duration$',
                                                                '^max_item_duration$',
                                                                '^total_item_days$',
                                                                '^active_items$',
                                                                '^unknown_status_items$',
                                                                '^coverage$',
                                                                '^frequency$',
                                                                '^status$'],
                                                 'source': 'sicklecell_rerun.medication_encountermeasures_scd_rwd'},
                'medication_encounters': {'inputRegex': ['^drugcode_standard_id$',
                                                         '^drugcode_standard_codingSystemId$',
                                                         '^drugcode_standard_primaryDisplay$',
                                                         '^medicationid$',
                                                         '^encounterid$',
                                                         '^personid$',
                                                         '^startDate$',
                                                         '^stopDate$',
                                                         '^prescribingprovider$',
                                                         '^tenant$',
                                                         '^datetimeMed$',
                                                         '^dateMed$'],
                                          'source': 'sicklecell_rerun.medication_encounters_scd_rwd'},
                'medshydroxia': {'inputRegex': ['^drugcode_standard_id$',
                                                '^drugcode_standard_codingSystemId$',
                                                '^drugcode_standard_primaryDisplay$',
                                                '^medicationid$',
                                                '^encounterid$',
                                                '^personid$',
                                                '^startDate$',
                                                '^stopDate$',
                                                '^prescribingprovider$',
                                                '^tenant$',
                                                '^datetimeMed$',
                                                '^dateMed$',
                                                '^Subjects$'],
                                 'source': 'sicklecell_rerun.medshydroxia_scd_rwd'},
                'medshydroxiaindex': {'inputRegex': ['^personid$',
                                                     '^tenant$',
                                                     '^observation_period_start_date$',
                                                     '^observation_period_end_date$',
                                                     '^gapdaySum$',
                                                     '^periodDays$',
                                                     '^medDays$'],
                                      'source': 'sicklecell_rerun.medshydroxiaindex_scd_rwd'},
                'medshydroxiausage': {'inputRegex': ['^personid$',
                                                     '^tenant$',
                                                     '^hydroxia_start$',
                                                     '^hydroxia_end$',
                                                     '^hydroxia_days$'],
                                      'source': 'sicklecell_rerun.medshydroxiausage_scd_rwd'},
                'mrnlist': {'inputRegex': ['^personid$', '^mrn$'],
                            'source': 'sicklecell_rerun.mrnlist_scd_rwd'},
                'persontenant': {'inputRegex': ['^personid$', '^tenant$'],
                                 'source': 'sicklecell_rerun.persontenant_scd_rwd'},
                'persontenantomop': {'inputRegex': ['^personid$',
                                                    '^person_id$',
                                                    '^tenant$'],
                                     'source': 'sicklecell_rerun.persontenantomop_scd_rwd'},
                'phenomatrix': {'inputRegex': ['^PersonPhenotype$',
                                               '^personid$',
                                               '^BetaThalassemia$',
                                               '^HemC_Disease$',
                                               '^No_Phenotype$',
                                               '^Not_SCD$',
                                               '^SCD_Indeterminate$',
                                               '^SCD_SC$',
                                               '^SCD_SCA$',
                                               '^SCD_SCA_Likely$',
                                               '^SCD_SD$',
                                               '^SCD_SE$',
                                               '^SCD_Sbetap_Likely$',
                                               '^S_Indeterminate$',
                                               '^S_Trait$',
                                               '^HgbSMax$',
                                               '^HgbAMax$',
                                               '^HgbSLabCount$',
                                               '^HgbSLabTfxCount$',
                                               '^TfxPercent$'],
                                'source': 'sicklecell_rerun.phenomatrix_scd_rwd'},
                'pregnancy_outcome_encounter': {'inputRegex': ['^conditioncode_standard_primaryDisplay$',
                                                               '^conditioncode_standard_id$',
                                                               '^conditioncode_standard_codingSystemId$',
                                                               '^conditionid$',
                                                               '^personid$',
                                                               '^encounterid$',
                                                               '^tenant$',
                                                               '^datetimeCondition$',
                                                               '^Condition$'],
                                                'source': 'sicklecell_rerun.pregnancy_outcome_encounter_scd_rwd'},
                'pregnancy_outcome_icd_codes': {'inputRegex': ['^Condition$',
                                                               '^conditioncode_standard_id$',
                                                               '^Description$'],
                                                'source': 'sicklecell_rerun.pregnancy_outcome_icd_codes_scd_rwd'},
                'pregnancy_outcome_icd_codes_verified': {'inputRegex': ['^regexgroup$',
                                                                        '^Subjects$',
                                                                        '^conditioncode_standard_primaryDisplay$',
                                                                        '^conditioncode_standard_id$',
                                                                        '^conditioncode_standard_codingSystemId$',
                                                                        '^Condition$',
                                                                        '^Description$'],
                                                         'source': 'sicklecell_rerun.pregnancy_outcome_icd_codes_verified_scd_rwd'},
                'proctransfusion': {'inputRegex': ['^procedurecode_standard_id$',
                                                   '^procedurecode_standard_codingSystemId$',
                                                   '^procedurecode_standard_primaryDisplay$',
                                                   '^personid$',
                                                   '^tenant$',
                                                   '^procedureid$',
                                                   '^encounterid$',
                                                   '^servicestartdateproc$',
                                                   '^serviceenddateproc$',
                                                   '^principalprovider$',
                                                   '^active$',
                                                   '^source$',
                                                   '^datetimeProc$',
                                                   '^dateProc$',
                                                   '^course_of_therapy$',
                                                   '^index_LAB$',
                                                   '^last_LAB$',
                                                   '^encounter_days$',
                                                   '^Subjects$'],
                                    'source': 'sicklecell_rerun.proctransfusion_scd_rwd'},
                'proctransfusioncodes': {'inputRegex': ['^Subjects$',
                                                        '^procedurecode_standard_codingSystemId$',
                                                        '^procedurecode_standard_id$',
                                                        '^procedurecode_standard_primaryDisplay$'],
                                         'source': 'sicklecell_rerun.proctransfusioncodes_scd_rwd'},
                'sicklecodes': {'inputRegex': ['^group$', '^codes$'],
                                'source': 'sicklecell_rerun.sicklecodes_scd_rwd'},
                'sicklecodesexpanded': {'inputRegex': ['^group$', '^codes$'],
                                        'source': 'sicklecell_rerun.sicklecodesexpanded_scd_rwd'},
                'sicklecodesverified': {'inputRegex': ['^regexgroup$',
                                                       '^Subjects$',
                                                       '^conditioncode_standard_primaryDisplay$',
                                                       '^conditioncode_standard_id$',
                                                       '^conditioncode_standard_codingSystemId$',
                                                       '^group$',
                                                       '^codes$'],
                                        'source': 'sicklecell_rerun.sicklecodesverified_scd_rwd'},
                'sicklecodesverifiedexpanded': {'inputRegex': ['^regexgroup$',
                                                               '^Subjects$',
                                                               '^conditioncode_standard_primaryDisplay$',
                                                               '^conditioncode_standard_id$',
                                                               '^conditioncode_standard_codingSystemId$',
                                                               '^group$',
                                                               '^codes$'],
                                                'source': 'sicklecell_rerun.sicklecodesverifiedexpanded_scd_rwd'},
                'sickleconditionencounter': {'inputRegex': ['^conditioncode_standard_primaryDisplay$',
                                                            '^conditioncode_standard_id$',
                                                            '^conditioncode_standard_codingSystemId$',
                                                            '^conditionid$',
                                                            '^personid$',
                                                            '^encounterid$',
                                                            '^effectivedate$',
                                                            '^asserteddate$',
                                                            '^classification_standard_primaryDisplay$',
                                                            '^confirmationstatus_standard_primaryDisplay$',
                                                            '^responsibleprovider$',
                                                            '^billingrank$',
                                                            '^tenant$',
                                                            '^datetimeCondition$',
                                                            '^regexgroup$',
                                                            '^Subjects$',
                                                            '^group$',
                                                            '^codes$'],
                                             'source': 'sicklecell_rerun.sickleconditionencounter_scd_rwd'},
                'sickleconditionencounterexpanded': {'inputRegex': ['^conditioncode_standard_primaryDisplay$',
                                                                    '^conditioncode_standard_id$',
                                                                    '^conditioncode_standard_codingSystemId$',
                                                                    '^conditionid$',
                                                                    '^personid$',
                                                                    '^encounterid$',
                                                                    '^effectivedate$',
                                                                    '^asserteddate$',
                                                                    '^conditionType$',
                                                                    '^confirmationstatus_standard_primaryDisplay$',
                                                                    '^responsibleprovider$',
                                                                    '^billingrank$',
                                                                    '^tenant$',
                                                                    '^datetimeCondition$',
                                                                    '^regexgroup$',
                                                                    '^Subjects$',
                                                                    '^group$',
                                                                    '^codes$'],
                                                     'source': 'sicklecell_rerun.sickleconditionencounterexpanded_scd_rwd'},
                'stemcell_codes': {'inputRegex': ['^group$', '^codes$'],
                                   'source': 'sicklecell_rerun.stemcell_codes_scd_rwd'},
                'stemcellcodesverified': {'inputRegex': ['^regexgroup$',
                                                         '^Subjects$',
                                                         '^conditioncode_standard_primaryDisplay$',
                                                         '^conditioncode_standard_id$',
                                                         '^conditioncode_standard_codingSystemId$',
                                                         '^group$',
                                                         '^codes$'],
                                          'source': 'sicklecell_rerun.stemcellcodesverified_scd_rwd'},
                'stemcellid': {'inputRegex': ['^personid$',
                                              '^tenant$',
                                              '^course_of_therapy$',
                                              '^datetimeCondition$',
                                              '^group$',
                                              '^dateCondition$',
                                              '^index_SCD$',
                                              '^last_SCD$',
                                              '^encounter_days$',
                                              '^days_to_next_encounter$',
                                              '^index_therapy_SCD$',
                                              '^last_therapy_SCD$',
                                              '^max_gap$',
                                              '^encounter_days_for_course$'],
                               'source': 'sicklecell_rerun.stemcellid_scd_rwd'},
                'stemcellresults': {'inputRegex': ['^conditioncode_standard_primaryDisplay$',
                                                   '^conditioncode_standard_id$',
                                                   '^conditioncode_standard_codingSystemId$',
                                                   '^conditionid$',
                                                   '^personid$',
                                                   '^encounterid$',
                                                   '^effectivedate$',
                                                   '^asserteddate$',
                                                   '^conditionType$',
                                                   '^confirmationstatus_standard_primaryDisplay$',
                                                   '^responsibleprovider$',
                                                   '^billingrank$',
                                                   '^tenant$',
                                                   '^datetimeCondition$',
                                                   '^dateCondition$',
                                                   '^regexgroup$',
                                                   '^Subjects$',
                                                   '^group$',
                                                   '^codes$'],
                                    'source': 'sicklecell_rerun.stemcellresults_scd_rwd'},
                'test_feature_control': {'inputRegex': ['^tableName$',
                                                        '^codefield$',
                                                        '^year$',
                                                        '^month$',
                                                        '^standard_id$',
                                                        '^standard_codingSystemId$',
                                                        '^standard_primaryDisplay$',
                                                        '^conceptName$',
                                                        '^contextId$',
                                                        '^countByConcept$',
                                                        '^countBySystem$',
                                                        '^countByCode$',
                                                        '^percentByConcept$',
                                                        '^percentBySystem$',
                                                        '^percentByCode$',
                                                        '^n$',
                                                        '^feature$'],
                                         'source': 'sicklecell_rerun.test_feature_control_scd_rwd'},
                'transfusion': {'inputRegex': ['^personid$', '^date$'],
                                'source': 'sicklecell_rerun.transfusion_scd_rwd'},
                'visit_detail_concept_id': {'inputRegex': ['^visit_detail_concept_id$',
                                                           '^person_id$',
                                                           '^visit_detail_id$',
                                                           '^visit_detail_start_date$',
                                                           '^visit_detail_end_date$',
                                                           '^visit_detail_type_concept_id$',
                                                           '^personid$',
                                                           '^concept_name$'],
                                            'source': 'sicklecell_rerun.visit_detail_concept_id_scd_rwd'},
                'visit_occurrence_concept_id': {'inputRegex': ['^visit_concept_id$',
                                                               '^person_id$',
                                                               '^visit_occurrence_id$',
                                                               '^visit_start_date$',
                                                               '^visit_end_date$',
                                                               '^visit_type_concept_id$',
                                                               '^personid$',
                                                               '^concept_name$'],
                                                'source': 'sicklecell_rerun.visit_occurrence_concept_id_scd_rwd'},
                'vital_sign_encounterbaseline': {'inputRegex': ['^personid$',
                                                                '^measurementcode_standard_id$',
                                                                '^tenant$',
                                                                '^SCD$',
                                                                '^measurementcode_standard_codingSystemId$',
                                                                '^loincclass$',
                                                                '^first_test_date$',
                                                                '^last_test_date$',
                                                                '^testing_period_days$',
                                                                '^total_tests$',
                                                                '^tests_with_numeric_value$',
                                                                '^average_value$',
                                                                '^min_value$',
                                                                '^max_value$',
                                                                '^stddev_value$',
                                                                '^avg_change_per_day$',
                                                                '^trend_description$'],
                                                 'source': 'sicklecell_rerun.vital_sign_encounterbaseline_scd_rwd'},
                'vital_sign_encounterdemobaseline': {'inputRegex': ['^personid$',
                                                                    '^tenant$',
                                                                    '^dateMeasurement$',
                                                                    '^loincclass$',
                                                                    '^measurementcode_standard_id$',
                                                                    '^measurementcode_standard_primaryDisplay$',
                                                                    '^interpretation_standard_primaryDisplay$',
                                                                    '^typedvalue_numericValue_value$',
                                                                    '^typedvalue_unitOfMeasure_standard_id$',
                                                                    '^SCD$',
                                                                    '^yearofbirth$',
                                                                    '^FirstTouchDate$',
                                                                    '^followdate$',
                                                                    '^deceased$',
                                                                    '^followed_from_birth$',
                                                                    '^baseline_period$'],
                                                     'source': 'sicklecell_rerun.vital_sign_encounterdemobaseline_scd_rwd'},
                'vital_sign_encountermeasures': {'inputRegex': ['^personid$',
                                                                '^tenant$',
                                                                '^measurementcode_standard_id$',
                                                                '^measurementcode_standard_primaryDisplay$',
                                                                '^typedvalue_unitOfMeasure_standard_id$',
                                                                '^measurement_count$',
                                                                '^distinct_dates$',
                                                                '^first_date$',
                                                                '^last_date$',
                                                                '^date_span$',
                                                                '^mean_value$',
                                                                '^stddev_value$',
                                                                '^min_value$',
                                                                '^max_value$',
                                                                '^median_value$',
                                                                '^avg_daily_change$',
                                                                '^min_daily_change$',
                                                                '^max_daily_change$',
                                                                '^value_range$',
                                                                '^coefficient_variation$',
                                                                '^normalized_range$',
                                                                '^measurements_per_day$',
                                                                '^trend_direction$'],
                                                 'source': 'sicklecell_rerun.vital_sign_encountermeasures_scd_rwd'},
                'vital_sign_encounters': {'inputRegex': ['^measurementcode_standard_id$',
                                                         '^measurementcode_standard_codingSystemId$',
                                                         '^measurementid$',
                                                         '^encounterid$',
                                                         '^personid$',
                                                         '^loincclass$',
                                                         '^servicedate$',
                                                         '^typedvalue_numericValue_value$',
                                                         '^typedvalue_unitOfMeasure_standard_id$',
                                                         '^interpretation_standard_primaryDisplay$',
                                                         '^tenant$',
                                                         '^dateMeasurement$',
                                                         '^subjects$',
                                                         '^measurementcode_standard_primaryDisplay$'],
                                          'source': 'sicklecell_rerun.vital_sign_encounters_scd_rwd'},
                'vital_sign_encountersdemo': {'inputRegex': ['^personid$',
                                                             '^tenant$',
                                                             '^dateMeasurement$',
                                                             '^measurementcode_standard_id$',
                                                             '^measurementcode_standard_codingSystemId$',
                                                             '^loincclass$',
                                                             '^typedvalue_numericValue_value$',
                                                             '^typedvalue_unitOfMeasure_standard_id$',
                                                             '^SCD$',
                                                             '^yearofbirth$',
                                                             '^FirstTouchDate$',
                                                             '^followdate$',
                                                             '^deceased$',
                                                             '^followed_from_birth$',
                                                             '^baseline_12$'],
                                              'source': 'sicklecell_rerun.vital_sign_encountersdemo_scd_rwd'},
                'vital_signs_codes': {'inputRegex': ['^subjects$',
                                                     '^measurementcode_standard_id$',
                                                     '^measurementcode_standard_codingSystemId$',
                                                     '^measurementcode_standard_primaryDisplay$'],
                                      'source': 'sicklecell_rerun.vital_signs_codes_scd_rwd'}},
 'debug': True,
 'disease': 'SCD',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'project': 'SickleCell_AI',
 'schema': 'sicklecell_rerun',
 'schemaTag': 'RWD'}
2025-12-22 14:18:48,496 - lhn.item - INFO - Create name: adspatient from location: sicklecell_rerun.adspatient_scd_rwd
2025-12-22 14:18:48,512 - lhn.item - INFO - Using inputRegex: ['^location_id$', '^personid$', '^PersonPhenotype$', '^IcdPheno$', '^SCD$', '^casePossible$', '^tenant$', '^gender$', '^race$', '^ethnicity$', '^yearofbirth$', '^state$', '^metropolitan$', '^urban$', '^deceased$', '^dateofdeath$', '^encDateFirst$', '^encDateLast$', '^encounters$', '^procEncDateFirst$', '^procEncDateLast$', '^procEncounters$', '^medEncDateFirst$', '^medEncDateLast$', '^medEncounters$', '^followdate$', '^FirstTouchDate$', '^followtime$', '^ageAtFirstTouch$', '^ageAtLastTouch$', '^age$', '^group$', '^person_id$', '^year_of_birth$', '^care_site_id$', '^person_source_value$', '^gender_source_value$', '^race_source_value$', '^ethnicity_source_value$', '^omob_state$']
2025-12-22 14:18:48,521 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(location_id,LongType,true),StructField(personid,StringType,true),StructField(PersonPhenotype,StringType,true),StructField(IcdPheno,StringType,true),StructField(SCD,BooleanType,true),StructField(casePossible,BooleanType,true),StructField(tenant,IntegerType,true),StructField(gender,StringType,true),StructField(race,StringType,true),StructField(ethnicity,StringType,true),StructField(yearofbirth,StringType,true),StructField(state,StringType,true),StructField(metropolitan,StringType,true),StructField(urban,StringType,true),StructField(deceased,BooleanType,true),StructField(dateofdeath,StringType,true),StructField(encDateFirst,DateType,true),StructField(encDateLast,DateType,true),StructField(encounters,LongType,true),StructField(procEncDateFirst,DateType,true),StructField(procEncDateLast,DateType,true),StructField(procEncounters,LongType,true),StructField(medEncDateFirst,DateType,true),StructField(medEncDateLast,DateType,true),StructField(medEncounters,LongType,true),StructField(followdate,DateType,true),StructField(FirstTouchDate,DateType,true),StructField(followtime,IntegerType,true),StructField(ageAtFirstTouch,DoubleType,true),StructField(ageAtLastTouch,DoubleType,true),StructField(age,DoubleType,true),StructField(group,StringType,true),StructField(person_id,LongType,true),StructField(year_of_birth,DateType,true),StructField(care_site_id,LongType,true),StructField(person_source_value,StringType,true),StructField(gender_source_value,StringType,true),StructField(race_source_value,StringType,true),StructField(ethnicity_source_value,StringType,true),StructField(omob_state,StringType,true)))
2025-12-22 14:18:48,626 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:48,774 - lhn.item - INFO - Create name: adspatscd from location: sicklecell_rerun.adspatscd_scd_rwd
2025-12-22 14:18:48,790 - lhn.item - INFO - Using inputRegex: ['^location_id$', '^personid$', '^PersonPhenotype$', '^IcdPheno$', '^scd0$', '^casePossible$', '^tenant$', '^gender$', '^race$', '^ethnicity$', '^yearofbirth$', '^state$', '^metropolitan$', '^urban$', '^deceased$', '^dateofdeath$', '^encDateFirst$', '^encDateLast$', '^encounters$', '^procEncDateFirst$', '^procEncDateLast$', '^procEncounters$', '^medEncDateFirst$', '^medEncDateLast$', '^medEncounters$', '^followdate$', '^FirstTouchDate$', '^followtime$', '^ageAtFirstTouch$', '^ageAtLastTouch$', '^age$', '^group$', '^person_id$', '^year_of_birth$', '^care_site_id$', '^person_source_value$', '^gender_source_value$', '^race_source_value$', '^ethnicity_source_value$', '^omob_state$', '^SCD$']
2025-12-22 14:18:48,799 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(location_id,LongType,true),StructField(personid,StringType,true),StructField(PersonPhenotype,StringType,true),StructField(IcdPheno,StringType,true),StructField(scd0,BooleanType,true),StructField(casePossible,BooleanType,true),StructField(tenant,IntegerType,true),StructField(gender,StringType,true),StructField(race,StringType,true),StructField(ethnicity,StringType,true),StructField(yearofbirth,StringType,true),StructField(state,StringType,true),StructField(metropolitan,StringType,true),StructField(urban,StringType,true),StructField(deceased,BooleanType,true),StructField(dateofdeath,StringType,true),StructField(encDateFirst,DateType,true),StructField(encDateLast,DateType,true),StructField(encounters,LongType,true),StructField(procEncDateFirst,DateType,true),StructField(procEncDateLast,DateType,true),StructField(procEncounters,LongType,true),StructField(medEncDateFirst,DateType,true),StructField(medEncDateLast,DateType,true),StructField(medEncounters,LongType,true),StructField(followdate,DateType,true),StructField(FirstTouchDate,DateType,true),StructField(followtime,IntegerType,true),StructField(ageAtFirstTouch,DoubleType,true),StructField(ageAtLastTouch,DoubleType,true),StructField(age,DoubleType,true),StructField(group,StringType,true),StructField(person_id,LongType,true),StructField(year_of_birth,DateType,true),StructField(care_site_id,LongType,true),StructField(person_source_value,StringType,true),StructField(gender_source_value,StringType,true),StructField(race_source_value,StringType,true),StructField(ethnicity_source_value,StringType,true),StructField(omob_state,StringType,true),StructField(SCD,StringType,true)))
2025-12-22 14:18:48,904 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,057 - lhn.item - INFO - Create name: comorbidity_conditionencounter from location: sicklecell_rerun.comorbidity_conditionencounter_scd_rwd
2025-12-22 14:18:49,071 - lhn.item - INFO - Using inputRegex: ['^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^conditionid$', '^personid$', '^encounterid$', '^tenant$', '^datetimeCondition$', '^Condition$']
2025-12-22 14:18:49,079 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeCondition,TimestampType,true),StructField(Condition,StringType,true)))
2025-12-22 14:18:49,103 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,134 - lhn.item - INFO - Create name: comorbidity_icd10_codes from location: sicklecell_rerun.comorbidity_icd10_codes_scd_rwd
2025-12-22 14:18:49,146 - lhn.item - INFO - Using inputRegex: ['^Condition$', '^conditioncode_standard_id$']
2025-12-22 14:18:49,171 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Condition,StringType,true),StructField(conditioncode_standard_id,StringType,true)))
2025-12-22 14:18:49,177 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,189 - lhn.item - INFO - Create name: comorbidity_icd10_codes_verified from location: sicklecell_rerun.comorbidity_icd10_codes_verified_scd_rwd
2025-12-22 14:18:49,204 - lhn.item - INFO - Using inputRegex: ['^regexgroup$', '^Subjects$', '^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^Condition$']
2025-12-22 14:18:49,214 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(Condition,StringType,true)))
2025-12-22 14:18:49,228 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,252 - lhn.item - INFO - Create name: comorbidity_icd10_conditionencounter from location: sicklecell_rerun.comorbidity_icd10_conditionencounter_scd_rwd
2025-12-22 14:18:49,266 - lhn.item - INFO - Using inputRegex: ['^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^conditionid$', '^personid$', '^encounterid$', '^tenant$', '^datetimeCondition$', '^Condition$']
2025-12-22 14:18:49,273 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeCondition,TimestampType,true),StructField(Condition,StringType,true)))
2025-12-22 14:18:49,294 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,328 - lhn.item - INFO - Create name: comorbidity_icd9_codes from location: sicklecell_rerun.comorbidity_icd9_codes_scd_rwd
2025-12-22 14:18:49,341 - lhn.item - INFO - Using inputRegex: ['^Condition$', '^conditioncode_standard_id$']
2025-12-22 14:18:49,348 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Condition,StringType,true),StructField(conditioncode_standard_id,StringType,true)))
2025-12-22 14:18:49,353 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,364 - lhn.item - INFO - Create name: comorbidity_icd9_codes_verified from location: sicklecell_rerun.comorbidity_icd9_codes_verified_scd_rwd
2025-12-22 14:18:49,377 - lhn.item - INFO - Using inputRegex: ['^regexgroup$', '^Subjects$', '^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^Condition$']
2025-12-22 14:18:49,385 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(Condition,StringType,true)))
2025-12-22 14:18:49,398 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,422 - lhn.item - INFO - Create name: comorbidity_icd9_conditionencounter from location: sicklecell_rerun.comorbidity_icd9_conditionencounter_scd_rwd
2025-12-22 14:18:49,435 - lhn.item - INFO - Using inputRegex: ['^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^conditionid$', '^personid$', '^encounterid$', '^tenant$', '^datetimeCondition$', '^Condition$']
2025-12-22 14:18:49,443 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeCondition,TimestampType,true),StructField(Condition,StringType,true)))
2025-12-22 14:18:49,463 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,498 - lhn.item - INFO - Create name: comorbidity_procedure_codes from location: sicklecell_rerun.comorbidity_procedure_codes_scd_rwd
2025-12-22 14:18:49,511 - lhn.item - INFO - Using inputRegex: ['^Procedure$', '^procedurecode_standard_id$', '^Description$']
2025-12-22 14:18:49,519 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Procedure,StringType,true),StructField(procedurecode_standard_id,StringType,true),StructField(Description,StringType,true)))
2025-12-22 14:18:49,526 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,540 - lhn.item - INFO - Create name: comorbidity_procedure_codes_verified from location: sicklecell_rerun.comorbidity_procedure_codes_verified_scd_rwd
2025-12-22 14:18:49,553 - lhn.item - INFO - Using inputRegex: ['^regexgroup$', '^Subjects$', '^procedurecode_standard_codingSystemId$', '^procedurecode_standard_id$', '^procedurecode_standard_primaryDisplay$', '^Procedure$', '^Description$']
2025-12-22 14:18:49,560 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(procedurecode_standard_codingSystemId,StringType,true),StructField(procedurecode_standard_id,StringType,true),StructField(procedurecode_standard_primaryDisplay,StringType,true),StructField(Procedure,StringType,true),StructField(Description,StringType,true)))
2025-12-22 14:18:49,577 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,604 - lhn.item - INFO - Create name: comorbidity_procedureencounter from location: sicklecell_rerun.comorbidity_procedureencounter_scd_rwd
2025-12-22 14:18:49,617 - lhn.item - INFO - Using inputRegex: ['^procedurecode_standard_primaryDisplay$', '^procedurecode_standard_id$', '^procedurecode_standard_codingSystemId$', '^procedureid$', '^personid$', '^encounterid$', '^servicestartdateproc$', '^serviceenddateproc$', '^tenant$', '^datetimeProc$', '^dateProc$']
2025-12-22 14:18:49,624 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(procedurecode_standard_primaryDisplay,StringType,true),StructField(procedurecode_standard_id,StringType,true),StructField(procedurecode_standard_codingSystemId,StringType,true),StructField(procedureid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(servicestartdateproc,StringType,true),StructField(serviceenddateproc,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeProc,TimestampType,true),StructField(dateProc,DateType,true)))
2025-12-22 14:18:49,650 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,690 - lhn.item - INFO - Create name: comorbidity_procedureencounterfeature from location: sicklecell_rerun.comorbidity_procedureencounterfeature_scd_rwd
2025-12-22 14:18:49,702 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^procedurecode_standard_id$', '^dateProc$']
2025-12-22 14:18:49,710 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(procedurecode_standard_id,StringType,true),StructField(dateProc,DateType,true)))
2025-12-22 14:18:49,722 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,737 - lhn.item - INFO - Create name: corrections from location: sicklecell_rerun.corrections_scd_rwd
2025-12-22 14:18:49,749 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^financialclass_standard_primaryDisplay$', '^encType$', '^servicedate$', '^dischargedate$', '^datetimeEnc$', '^dateEnc$']
2025-12-22 14:18:49,757 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(financialclass_standard_primaryDisplay,StringType,true),StructField(encType,StringType,true),StructField(servicedate,TimestampType,true),StructField(dischargedate,TimestampType,true),StructField(datetimeEnc,TimestampType,true),StructField(dateEnc,DateType,true)))
2025-12-22 14:18:49,778 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,806 - lhn.item - INFO - Create name: death from location: sicklecell_rerun.death_scd_rwd
2025-12-22 14:18:49,818 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^deceased$', '^dateofdeath$']
2025-12-22 14:18:49,826 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(deceased,BooleanType,true),StructField(dateofdeath,DateType,true)))
2025-12-22 14:18:49,839 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:49,853 - lhn.item - INFO - Create name: demo from location: sicklecell_rerun.demo_scd_rwd
2025-12-22 14:18:49,867 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^gender$', '^race$', '^ethnicity$', '^yearofbirth$', '^state$', '^metropolitan$', '^urban$', '^birthdate$', '^deceased$', '^dateofdeath$', '^encDateFirst$', '^encDateLast$', '^encounters$', '^conditionEncDateFirst$', '^conditionEncDateLast$', '^conditionEncounters$', '^labEncDateFirst$', '^labEncDateLast$', '^labEncounters$', '^procEncDateFirst$', '^procEncDateLast$', '^procEncounters$', '^medEncDateFirst$', '^medEncDateLast$', '^medEncounters$', '^followdate$', '^FirstTouchDate$', '^followtime$', '^maritalstatus_standard_primaryDisplay$', '^maritalstatus$', '^ageAtFirstTouch$', '^ageAtLastTouch$', '^ageAtDeath$', '^ageCurrent$', '^age$']
2025-12-22 14:18:49,876 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(gender,StringType,true),StructField(race,StringType,true),StructField(ethnicity,StringType,true),StructField(yearofbirth,StringType,true),StructField(state,StringType,true),StructField(metropolitan,StringType,true),StructField(urban,StringType,true),StructField(birthdate,DateType,true),StructField(deceased,BooleanType,true),StructField(dateofdeath,DateType,true),StructField(encDateFirst,DateType,true),StructField(encDateLast,DateType,true),StructField(encounters,LongType,true),StructField(conditionEncDateFirst,DateType,true),StructField(conditionEncDateLast,DateType,true),StructField(conditionEncounters,LongType,true),StructField(labEncDateFirst,DateType,true),StructField(labEncDateLast,DateType,true),StructField(labEncounters,LongType,true),StructField(procEncDateFirst,DateType,true),StructField(procEncDateLast,DateType,true),StructField(procEncounters,LongType,true),StructField(medEncDateFirst,DateType,true),StructField(medEncDateLast,DateType,true),StructField(medEncounters,LongType,true),StructField(followdate,DateType,true),StructField(FirstTouchDate,DateType,true),StructField(followtime,IntegerType,true),StructField(maritalstatus_standard_primaryDisplay,StringType,true),StructField(maritalstatus,StringType,true),StructField(ageAtFirstTouch,IntegerType,true),StructField(ageAtLastTouch,IntegerType,true),StructField(ageAtDeath,IntegerType,true),StructField(ageCurrent,IntegerType,true),StructField(age,DoubleType,true)))
2025-12-22 14:18:49,973 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:50,109 - lhn.item - INFO - Create name: demoomop from location: sicklecell_rerun.demoomop_scd_rwd
2025-12-22 14:18:50,125 - lhn.item - INFO - Using inputRegex: ['^personid$', '^gender$', '^race$', '^ethnicity$', '^yearofbirth$', '^state$', '^metropolitan$', '^urban$', '^birthdate$', '^deceased$', '^dateofdeath$', '^encDateFirst$', '^encDateLast$', '^encounters$', '^conditionEncDateFirst$', '^conditionEncDateLast$', '^conditionEncounters$', '^labEncDateFirst$', '^labEncDateLast$', '^labEncounters$', '^procEncDateFirst$', '^procEncDateLast$', '^procEncounters$', '^medEncDateFirst$', '^medEncDateLast$', '^medEncounters$', '^followdate$', '^FirstTouchDate$', '^followtime$', '^maritalstatus_standard_primaryDisplay$', '^maritalstatus$', '^ageAtFirstTouch$', '^ageAtLastTouch$', '^ageAtDeath$', '^ageCurrent$', '^age$', '^person_id$', '^location_id$', '^care_site_id$', '^person_source_value$', '^year_of_birth$', '^gender_source_value$', '^race_source_value$', '^ethnicity_source_value$', '^stateOMOP$', '^Hospital_Type$', '^zip$', '^death_date$', '^death_type_concept_id$', '^tenant$']
2025-12-22 14:18:50,134 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(gender,StringType,true),StructField(race,StringType,true),StructField(ethnicity,StringType,true),StructField(yearofbirth,StringType,true),StructField(state,StringType,true),StructField(metropolitan,StringType,true),StructField(urban,StringType,true),StructField(birthdate,DateType,true),StructField(deceased,BooleanType,true),StructField(dateofdeath,DateType,true),StructField(encDateFirst,DateType,true),StructField(encDateLast,DateType,true),StructField(encounters,LongType,true),StructField(conditionEncDateFirst,DateType,true),StructField(conditionEncDateLast,DateType,true),StructField(conditionEncounters,LongType,true),StructField(labEncDateFirst,DateType,true),StructField(labEncDateLast,DateType,true),StructField(labEncounters,LongType,true),StructField(procEncDateFirst,DateType,true),StructField(procEncDateLast,DateType,true),StructField(procEncounters,LongType,true),StructField(medEncDateFirst,DateType,true),StructField(medEncDateLast,DateType,true),StructField(medEncounters,LongType,true),StructField(followdate,DateType,true),StructField(FirstTouchDate,DateType,true),StructField(followtime,IntegerType,true),StructField(maritalstatus_standard_primaryDisplay,StringType,true),StructField(maritalstatus,StringType,true),StructField(ageAtFirstTouch,IntegerType,true),StructField(ageAtLastTouch,IntegerType,true),StructField(ageAtDeath,IntegerType,true),StructField(ageCurrent,IntegerType,true),StructField(age,DoubleType,true),StructField(person_id,LongType,true),StructField(location_id,LongType,true),StructField(care_site_id,LongType,true),StructField(person_source_value,StringType,true),StructField(year_of_birth,DateType,true),StructField(gender_source_value,StringType,true),StructField(race_source_value,StringType,true),StructField(ethnicity_source_value,StringType,true),StructField(stateOMOP,StringType,true),StructField(Hospital_Type,StringType,true),StructField(zip,StringType,true),StructField(death_date,DateType,true),StructField(death_type_concept_id,LongType,true),StructField(tenant,StringType,true)))
2025-12-22 14:18:50,266 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:50,455 - lhn.item - INFO - Create name: drug_codes from location: sicklecell_rerun.drug_codes_scd_rwd
2025-12-22 14:18:50,469 - lhn.item - INFO - Using inputRegex: ['^drugcode_standard_id$', '^drugcode_standard_primaryDisplay$', '^drugcode_standard_codingSystemId$']
2025-12-22 14:18:50,476 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(drugcode_standard_id,StringType,true),StructField(drugcode_standard_primaryDisplay,StringType,true),StructField(drugcode_standard_codingSystemId,StringType,true)))
2025-12-22 14:18:50,483 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:50,495 - lhn.item - INFO - Create name: encounter from location: sicklecell_rerun.encounter_scd_rwd
2025-12-22 14:18:50,509 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^encounterid$', '^facilityids$', '^financialclass_standard_primaryDisplay$', '^hospitalservice_standard_primaryDisplay$', '^classification_standard_id$', '^classification_standard_codingSystemId$', '^encType$', '^type_standard_primaryDisplay$', '^servicedate$', '^dischargedate$', '^actualarrivaldate$', '^datetimeEnc$', '^dateEnc$', '^course_of_therapy$', '^classification_standard_primaryDisplay$', '^confirmationstatus_standard_primaryDisplay$', '^group$', '^datetimeCondition$', '^index_SCD$', '^last_SCD$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_SCD$', '^last_therapy_SCD$', '^max_gap$', '^encounter_days_for_course$']
2025-12-22 14:18:50,510 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(encounterid,StringType,true),StructField(facilityids,ArrayType(StringType,true),true),StructField(financialclass_standard_primaryDisplay,StringType,true),StructField(hospitalservice_standard_primaryDisplay,StringType,true),StructField(classification_standard_id,StringType,true),StructField(classification_standard_codingSystemId,StringType,true),StructField(encType,StringType,true),StructField(type_standard_primaryDisplay,StringType,true),StructField(servicedate,TimestampType,true),StructField(dischargedate,TimestampType,true),StructField(actualarrivaldate,StringType,true),StructField(datetimeEnc,TimestampType,true),StructField(dateEnc,DateType,true),StructField(course_of_therapy,LongType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(confirmationstatus_standard_primaryDisplay,StringType,true),StructField(group,StringType,true),StructField(datetimeCondition,TimestampType,true),StructField(index_SCD,TimestampType,true),StructField(last_SCD,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,TimestampType,true),StructField(last_therapy_SCD,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:50,525 - lhn.spark_utils - INFO - Fields to flatten: arrays=['facilityids'], structs=[]
2025-12-22 14:18:50,530 - lhn.spark_utils - INFO - Flattened DataFrame schema:
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(encounterid,StringType,true),StructField(facilityids,StringType,true),StructField(financialclass_standard_primaryDisplay,StringType,true),StructField(hospitalservice_standard_primaryDisplay,StringType,true),StructField(classification_standard_id,StringType,true),StructField(classification_standard_codingSystemId,StringType,true),StructField(encType,StringType,true),StructField(type_standard_primaryDisplay,StringType,true),StructField(servicedate,TimestampType,true),StructField(dischargedate,TimestampType,true),StructField(actualarrivaldate,StringType,true),StructField(datetimeEnc,TimestampType,true),StructField(dateEnc,DateType,true),StructField(course_of_therapy,LongType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(confirmationstatus_standard_primaryDisplay,StringType,true),StructField(group,StringType,true),StructField(datetimeCondition,TimestampType,true),StructField(index_SCD,TimestampType,true),StructField(last_SCD,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,TimestampType,true),StructField(last_therapy_SCD,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:50,631 - lhn.item - INFO - Create name: encounterextract from location: sicklecell_rerun.encounterextract_scd_rwd
2025-12-22 14:18:50,646 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^encounterid$', '^encType$', '^dateCondition$']
2025-12-22 14:18:50,654 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(encounterid,StringType,true),StructField(encType,StringType,true),StructField(dateCondition,DateType,true)))
2025-12-22 14:18:50,666 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:50,688 - lhn.item - INFO - Create name: encounterid from location: sicklecell_rerun.encounterid_scd_rwd
2025-12-22 14:18:50,701 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^encounterid$', '^course_of_therapy$', '^classification_standard_primaryDisplay$', '^confirmationstatus_standard_primaryDisplay$', '^group$', '^datetimeCondition$', '^index_SCD$', '^last_SCD$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_SCD$', '^last_therapy_SCD$', '^max_gap$', '^encounter_days_for_course$']
2025-12-22 14:18:50,709 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(encounterid,StringType,true),StructField(course_of_therapy,LongType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(confirmationstatus_standard_primaryDisplay,StringType,true),StructField(group,StringType,true),StructField(datetimeCondition,TimestampType,true),StructField(index_SCD,TimestampType,true),StructField(last_SCD,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,TimestampType,true),StructField(last_therapy_SCD,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:50,749 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:50,809 - lhn.item - INFO - Create name: evalphenorow from location: sicklecell_rerun.evalphenorow_scd_rwd
2025-12-22 14:18:50,824 - lhn.item - INFO - Using inputRegex: ['^RowPhenotype$', '^personid$', '^age$', '^servicedate$', '^HgbA$', '^HgbC$', '^HgbF$', '^HgbS$', '^HgbA2$', '^HgbD$', '^HgbE$', '^HgbV$', '^HgbSum$', '^CompleteFrac$', '^AoverC$', '^AoverD$', '^AoverE$', '^AoverV$', '^AoverS$', '^CoverS$', '^DoverS$', '^EoverS$', '^VoverS$', '^HgbA_range$', '^HgbS_range$', '^HgbAMin$', '^HgbSMin$', '^HgbAMax$', '^HgbSMax$', '^InferTfxPerson$', '^InferTfxRow$', '^date$', '^DaysTfx$', '^PostTfx$']
2025-12-22 14:18:50,833 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(RowPhenotype,StringType,true),StructField(personid,StringType,true),StructField(age,FloatType,true),StructField(servicedate,StringType,true),StructField(HgbA,StringType,true),StructField(HgbC,StringType,true),StructField(HgbF,StringType,true),StructField(HgbS,StringType,true),StructField(HgbA2,StringType,true),StructField(HgbD,StringType,true),StructField(HgbE,StringType,true),StructField(HgbV,FloatType,true),StructField(HgbSum,FloatType,true),StructField(CompleteFrac,StringType,true),StructField(AoverC,StringType,true),StructField(AoverD,StringType,true),StructField(AoverE,StringType,true),StructField(AoverV,StringType,true),StructField(AoverS,StringType,true),StructField(CoverS,StringType,true),StructField(DoverS,StringType,true),StructField(EoverS,StringType,true),StructField(VoverS,StringType,true),StructField(HgbA_range,FloatType,true),StructField(HgbS_range,FloatType,true),StructField(HgbAMin,FloatType,true),StructField(HgbSMin,FloatType,true),StructField(HgbAMax,FloatType,true),StructField(HgbSMax,FloatType,true),StructField(InferTfxPerson,StringType,true),StructField(InferTfxRow,StringType,true),StructField(date,TimestampType,true),StructField(DaysTfx,FloatType,true),StructField(PostTfx,StringType,true)))
2025-12-22 14:18:50,919 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,044 - lhn.item - INFO - Create name: follow from location: sicklecell_rerun.follow_scd_rwd
2025-12-22 14:18:51,059 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^encDateFirst$', '^encDateLast$', '^encounters$', '^conditionEncDateFirst$', '^conditionEncDateLast$', '^conditionEncounters$', '^labEncDateFirst$', '^labEncDateLast$', '^labEncounters$', '^procEncDateFirst$', '^procEncDateLast$', '^procEncounters$', '^medEncDateFirst$', '^medEncDateLast$', '^medEncounters$', '^followdate$', '^FirstTouchDate$', '^followtime$']
2025-12-22 14:18:51,067 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(encDateFirst,DateType,true),StructField(encDateLast,DateType,true),StructField(encounters,LongType,true),StructField(conditionEncDateFirst,DateType,true),StructField(conditionEncDateLast,DateType,true),StructField(conditionEncounters,LongType,true),StructField(labEncDateFirst,DateType,true),StructField(labEncDateLast,DateType,true),StructField(labEncounters,LongType,true),StructField(procEncDateFirst,DateType,true),StructField(procEncDateLast,DateType,true),StructField(procEncounters,LongType,true),StructField(medEncDateFirst,DateType,true),StructField(medEncDateLast,DateType,true),StructField(medEncounters,LongType,true),StructField(followdate,DateType,true),StructField(FirstTouchDate,DateType,true),StructField(followtime,IntegerType,true)))
2025-12-22 14:18:51,116 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,190 - lhn.item - INFO - Create name: followcondition from location: sicklecell_rerun.followcondition_scd_rwd
2025-12-22 14:18:51,203 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^conditionEncDateFirst$', '^conditionEncDateLast$', '^conditionEncounters$']
2025-12-22 14:18:51,211 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(conditionEncDateFirst,DateType,true),StructField(conditionEncDateLast,DateType,true),StructField(conditionEncounters,LongType,true)))
2025-12-22 14:18:51,222 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,244 - lhn.item - INFO - Create name: followenc from location: sicklecell_rerun.followenc_scd_rwd
2025-12-22 14:18:51,257 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^encDateFirst$', '^encDateLast$', '^encounters$']
2025-12-22 14:18:51,265 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(encDateFirst,DateType,true),StructField(encDateLast,DateType,true),StructField(encounters,LongType,true)))
2025-12-22 14:18:51,276 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,297 - lhn.item - INFO - Create name: followlab from location: sicklecell_rerun.followlab_scd_rwd
2025-12-22 14:18:51,309 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^labEncDateFirst$', '^labEncDateLast$', '^labEncounters$']
2025-12-22 14:18:51,316 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(labEncDateFirst,DateType,true),StructField(labEncDateLast,DateType,true),StructField(labEncounters,LongType,true)))
2025-12-22 14:18:51,328 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,348 - lhn.item - INFO - Create name: followmed_rwd_ from location: sicklecell_rerun.followmed_rwd_
2025-12-22 14:18:51,361 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^medEncDateFirst$', '^medEncDateLast$', '^medEncounters$']
2025-12-22 14:18:51,369 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(medEncDateFirst,DateType,true),StructField(medEncDateLast,DateType,true),StructField(medEncounters,LongType,true)))
2025-12-22 14:18:51,381 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,398 - lhn.item - INFO - Create name: followproc from location: sicklecell_rerun.followproc_scd_rwd
2025-12-22 14:18:51,411 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^procEncDateFirst$', '^procEncDateLast$', '^procEncounters$']
2025-12-22 14:18:51,419 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(procEncDateFirst,DateType,true),StructField(procEncDateLast,DateType,true),StructField(procEncounters,LongType,true)))
2025-12-22 14:18:51,433 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,451 - lhn.item - INFO - Create name: gap_conditionid from location: sicklecell_rerun.gap_conditionid_scd_rwd
2025-12-22 14:18:51,464 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^course_of_therapy$', '^encounterid$', '^dateCondition$', '^datetimeCondition$', '^index_SCD$', '^last_SCD$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_SCD$', '^last_therapy_SCD$', '^max_gap$', '^encounter_days_for_course$']
2025-12-22 14:18:51,472 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(course_of_therapy,LongType,true),StructField(encounterid,StringType,true),StructField(dateCondition,DateType,true),StructField(datetimeCondition,TimestampType,true),StructField(index_SCD,TimestampType,true),StructField(last_SCD,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,TimestampType,true),StructField(last_therapy_SCD,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:51,507 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,558 - lhn.item - INFO - Create name: gap_condtionid from location: sicklecell_rerun.gap_condtionid_scd_rwd
2025-12-22 14:18:51,572 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^course_of_therapy$', '^encounterid$', '^dischargedate$', '^datetimeEnc$', '^dateEnc$', '^servicedate$', '^index_gap$', '^last_gap$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_gap$', '^last_therapy_gap$', '^max_gap$', '^encounter_days_for_course$']
2025-12-22 14:18:51,583 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(course_of_therapy,LongType,true),StructField(encounterid,StringType,true),StructField(dischargedate,StringType,true),StructField(datetimeEnc,TimestampType,true),StructField(dateEnc,DateType,true),StructField(servicedate,StringType,true),StructField(index_gap,StringType,true),StructField(last_gap,StringType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_gap,StringType,true),StructField(last_therapy_gap,StringType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:51,623 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,679 - lhn.item - INFO - Create name: gap_encounterid from location: sicklecell_rerun.gap_encounterid_scd_rwd
2025-12-22 14:18:51,693 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^course_of_therapy$', '^encounterid$', '^datetimeEnc$', '^dateEnc$', '^dischargedate$', '^servicedate$', '^index_gap$', '^last_gap$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_gap$', '^last_therapy_gap$', '^max_gap$', '^encounter_days_for_course$']
2025-12-22 14:18:51,701 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(course_of_therapy,LongType,true),StructField(encounterid,StringType,true),StructField(datetimeEnc,TimestampType,true),StructField(dateEnc,DateType,true),StructField(dischargedate,TimestampType,true),StructField(servicedate,TimestampType,true),StructField(index_gap,TimestampType,true),StructField(last_gap,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_gap,TimestampType,true),StructField(last_therapy_gap,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:51,741 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,816 - lhn.item - INFO - Create name: hgb_subunits from location: sicklecell_rerun.hgb_subunits_scd_rwd
2025-12-22 14:18:51,829 - lhn.item - INFO - Using inputRegex: ['^personid$', '^labcode_standard_id$', '^loincclass$', '^servicedate$', '^value$', '^hgbType$', '^age$', '^tenant$']
2025-12-22 14:18:51,837 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(loincclass,StringType,true),StructField(servicedate,DateType,true),StructField(value,StringType,true),StructField(hgbType,StringType,true),StructField(age,DoubleType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:51,858 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,889 - lhn.item - INFO - Create name: hydroxyurea from location: sicklecell_rerun.hydroxyurea_scd_rwd
2025-12-22 14:18:51,902 - lhn.item - INFO - Using inputRegex: ['^personid$', '^date$']
2025-12-22 14:18:51,909 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(date,TimestampType,true)))
2025-12-22 14:18:51,914 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,922 - lhn.item - INFO - Create name: insurance2 from location: sicklecell_rerun.insurance2_scd_rwd
2025-12-22 14:18:51,935 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^financialclass_standard_primaryDisplay$', '^encType$', '^servicedate$', '^dischargedate$', '^datetimeEnc$', '^dateEnc$']
2025-12-22 14:18:51,943 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(financialclass_standard_primaryDisplay,StringType,true),StructField(encType,StringType,true),StructField(servicedate,TimestampType,true),StructField(dischargedate,TimestampType,true),StructField(datetimeEnc,TimestampType,true),StructField(dateEnc,DateType,true)))
2025-12-22 14:18:51,964 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:51,992 - lhn.item - INFO - Create name: insurance from location: sicklecell_rerun.insurance_scd_rwd
2025-12-22 14:18:52,005 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^financialclass_standard_primaryDisplay$', '^encType$', '^servicedate$', '^dischargedate$', '^datetimeEnc$', '^dateEnc$']
2025-12-22 14:18:52,016 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(financialclass_standard_primaryDisplay,StringType,true),StructField(encType,StringType,true),StructField(servicedate,TimestampType,true),StructField(dischargedate,TimestampType,true),StructField(datetimeEnc,TimestampType,true),StructField(dateEnc,DateType,true)))
2025-12-22 14:18:52,034 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:52,064 - lhn.item - INFO - Create name: insuranceperiods from location: sicklecell_rerun.insuranceperiods_scd_rwd
2025-12-22 14:18:52,080 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^financialclass_standard_primaryDisplay$', '^course_of_therapy$', '^encType$', '^servicedate$', '^dischargedate$', '^dateEnc$', '^datetimeEnc$', '^index_insurance$', '^last_insurance$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_insurance$', '^last_therapy_insurance$', '^max_gap$', '^encounter_days_for_course$']
2025-12-22 14:18:52,091 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(financialclass_standard_primaryDisplay,StringType,true),StructField(course_of_therapy,LongType,true),StructField(encType,StringType,true),StructField(servicedate,TimestampType,true),StructField(dischargedate,TimestampType,true),StructField(dateEnc,DateType,true),StructField(datetimeEnc,TimestampType,true),StructField(index_insurance,TimestampType,true),StructField(last_insurance,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_insurance,TimestampType,true),StructField(last_therapy_insurance,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:52,133 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:52,193 - lhn.item - INFO - Create name: lab_features_codes from location: sicklecell_rerun.lab_features_codes_scd_rwd
2025-12-22 14:18:52,206 - lhn.item - INFO - Using inputRegex: ['^feature$', '^labcode_standard_id$', '^labcode_standard_codingSystemId$']
2025-12-22 14:18:52,214 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(feature,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true)))
2025-12-22 14:18:52,224 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:52,235 - lhn.item - INFO - Create name: lab_features from location: sicklecell_rerun.lab_features_scd_rwd
2025-12-22 14:18:52,248 - lhn.item - INFO - Using inputRegex: ['^labcode_standard_id$', '^labcode_standard_codingSystemId$', '^labid$', '^personid$', '^labcode_standard_primaryDisplay$', '^loincclass$', '^typedvalue_textValue_value$', '^typedvalue_numericValue_value$', '^tenant$', '^datetimeLab$', '^feature$']
2025-12-22 14:18:52,255 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labid,StringType,true),StructField(personid,StringType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeLab,TimestampType,true),StructField(feature,StringType,true)))
2025-12-22 14:18:52,283 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:52,323 - lhn.item - INFO - Create name: lab_loinc_codes_regex from location: sicklecell_rerun.lab_loinc_codes_regex_scd_rwd
2025-12-22 14:18:52,336 - lhn.item - INFO - Using inputRegex: ['^Test$', '^labcode_standard_id$', '^Description$', '^Regex$']
2025-12-22 14:18:52,343 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Test,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(Description,StringType,true),StructField(Regex,StringType,true)))
2025-12-22 14:18:52,354 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:52,369 - lhn.item - INFO - Create name: lab_loinc_codes from location: sicklecell_rerun.lab_loinc_codes_scd_rwd
2025-12-22 14:18:52,381 - lhn.item - INFO - Using inputRegex: ['^Test$', '^labcode_standard_id$', '^Description$']
2025-12-22 14:18:52,388 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Test,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(Description,StringType,true)))
2025-12-22 14:18:52,398 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:52,410 - lhn.item - INFO - Create name: lab_loinc_codes_verified from location: sicklecell_rerun.lab_loinc_codes_verified_scd_rwd
2025-12-22 14:18:52,422 - lhn.item - INFO - Using inputRegex: ['^regexgroup$', '^Subjects$', '^labcode_standard_primaryDisplay$', '^labcode_standard_codingSystemId$', '^labcode_standard_id$', '^Test$', '^Description$', '^Regex$']
2025-12-22 14:18:52,430 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(Test,StringType,true),StructField(Description,StringType,true),StructField(Regex,StringType,true)))
2025-12-22 14:18:52,451 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:52,479 - lhn.item - INFO - Create name: lab_loincencounter from location: sicklecell_rerun.lab_loincencounter_scd_rwd
2025-12-22 14:18:52,492 - lhn.item - INFO - Using inputRegex: ['^labcode_standard_primaryDisplay$', '^labcode_standard_id$', '^labcode_standard_codingSystemId$', '^labid$', '^encounterid$', '^personid$', '^loincclass$', '^servicedate$', '^typedvalue_textValue_value$', '^typedvalue_numericValue_value$', '^typedvalue_unitOfMeasure_standard_id$', '^interpretation_standard_primaryDisplay$', '^tenant$', '^datetimeLab$', '^dateLab$', '^Test$']
2025-12-22 14:18:52,500 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(labcode_standard_primaryDisplay,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(loincclass,StringType,true),StructField(servicedate,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeLab,TimestampType,true),StructField(dateLab,DateType,true),StructField(Test,StringType,true)))
2025-12-22 14:18:52,540 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:52,601 - lhn.item - INFO - Create name: lab_loincencounterbaseline from location: sicklecell_rerun.lab_loincencounterbaseline_scd_rwd
2025-12-22 14:18:52,615 - lhn.item - INFO - Using inputRegex: ['^personid$', '^Test$', '^tenant$', '^SCD$', '^labcode_standard_id$', '^labcode_standard_primaryDisplay$', '^loincclass$', '^first_test_date$', '^last_test_date$', '^testing_period_days$', '^total_tests$', '^tests_with_numeric_value$', '^average_value$', '^min_value$', '^max_value$', '^stddev_value$', '^avg_change_per_day$', '^trend_description$']
2025-12-22 14:18:52,623 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(Test,StringType,true),StructField(tenant,IntegerType,true),StructField(SCD,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(first_test_date,DateType,true),StructField(last_test_date,DateType,true),StructField(testing_period_days,IntegerType,true),StructField(total_tests,LongType,true),StructField(tests_with_numeric_value,LongType,true),StructField(average_value,DoubleType,true),StructField(min_value,DoubleType,true),StructField(max_value,DoubleType,true),StructField(stddev_value,DoubleType,true),StructField(avg_change_per_day,DoubleType,true),StructField(trend_description,StringType,true)))
2025-12-22 14:18:52,667 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:52,733 - lhn.item - INFO - Create name: lab_loincencounterdemo from location: sicklecell_rerun.lab_loincencounterdemo_scd_rwd
2025-12-22 14:18:52,749 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^dateLab$', '^Test$', '^labcode_standard_primaryDisplay$', '^labcode_standard_id$', '^labcode_standard_codingSystemId$', '^loincclass$', '^typedvalue_textValue_value$', '^typedvalue_numericValue_value$', '^typedvalue_unitOfMeasure_standard_id$', '^interpretation_standard_primaryDisplay$', '^SCD$', '^yearofbirth$', '^FirstTouchDate$', '^followdate$', '^deceased$', '^followed_from_birth$', '^baseline_12$']
2025-12-22 14:18:52,757 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(dateLab,DateType,true),StructField(Test,StringType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(loincclass,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(SCD,StringType,true),StructField(yearofbirth,StringType,true),StructField(FirstTouchDate,DateType,true),StructField(followdate,DateType,true),StructField(deceased,BooleanType,true),StructField(followed_from_birth,BooleanType,true),StructField(baseline_12,BooleanType,true)))
2025-12-22 14:18:52,804 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:52,874 - lhn.item - INFO - Create name: lab_loincencounterdemobaseline from location: sicklecell_rerun.lab_loincencounterdemobaseline_scd_rwd
2025-12-22 14:18:52,887 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^dateLab$', '^Test$', '^labcode_standard_id$', '^labcode_standard_codingSystemId$', '^loincclass$', '^typedvalue_textValue_value$', '^typedvalue_numericValue_value$', '^typedvalue_unitOfMeasure_standard_id$', '^interpretation_standard_primaryDisplay$', '^SCD$', '^yearofbirth$', '^FirstTouchDate$', '^followdate$', '^deceased$', '^followed_from_birth$', '^baseline_12$']
2025-12-22 14:18:52,895 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(dateLab,DateType,true),StructField(Test,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(loincclass,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(SCD,StringType,true),StructField(yearofbirth,StringType,true),StructField(FirstTouchDate,DateType,true),StructField(followdate,DateType,true),StructField(deceased,BooleanType,true),StructField(followed_from_birth,BooleanType,true),StructField(baseline_12,BooleanType,true)))
2025-12-22 14:18:52,939 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:53,003 - lhn.item - INFO - Create name: lab_loincencounterfeature from location: sicklecell_rerun.lab_loincencounterfeature_scd_rwd
2025-12-22 14:18:53,016 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^dateLab$', '^labcode_standard_id$', '^labvalue$']
2025-12-22 14:18:53,024 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(dateLab,DateType,true),StructField(labcode_standard_id,StringType,true),StructField(labvalue,DoubleType,true)))
2025-12-22 14:18:53,038 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:53,055 - lhn.item - INFO - Create name: lablist from location: sicklecell_rerun.lablist_scd_rwd
2025-12-22 14:18:53,069 - lhn.item - INFO - Using inputRegex: ['^personid$', '^Test$', '^tenant$', '^labcode_standard_id$', '^labcode_standard_primaryDisplay$', '^loincclass$', '^first_test_date$', '^last_test_date$', '^testing_period_days$', '^total_tests$', '^tests_with_numeric_value$', '^average_value$', '^min_value$', '^max_value$', '^stddev_value$', '^avg_change_per_day$', '^trend_description$']
2025-12-22 14:18:53,076 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(Test,StringType,true),StructField(tenant,IntegerType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(first_test_date,DateType,true),StructField(last_test_date,DateType,true),StructField(testing_period_days,IntegerType,true),StructField(total_tests,LongType,true),StructField(tests_with_numeric_value,LongType,true),StructField(average_value,DoubleType,true),StructField(min_value,DoubleType,true),StructField(max_value,DoubleType,true),StructField(stddev_value,DoubleType,true),StructField(avg_change_per_day,DoubleType,true),StructField(trend_description,StringType,true)))
2025-12-22 14:18:53,118 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:53,182 - lhn.item - INFO - Create name: labpersonid from location: sicklecell_rerun.labpersonid_scd_rwd
2025-12-22 14:18:53,200 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^course_of_therapy$', '^labcode_standard_primaryDisplay$', '^typedvalue_textValue_value$', '^typedvalue_numericValue_value$', '^typedvalue_unitOfMeasure_standard_primaryDisplay$', '^interpretation_standard_primaryDisplay$', '^lab$', '^source$', '^Subjects$', '^datetimeLab$', '^index_LAB$', '^last_LAB$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_LAB$', '^last_therapy_LAB$', '^max_gap$', '^encounter_days_for_course$']
2025-12-22 14:18:53,208 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(course_of_therapy,LongType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_primaryDisplay,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(lab,StringType,true),StructField(source,StringType,true),StructField(Subjects,LongType,true),StructField(datetimeLab,TimestampType,true),StructField(index_LAB,TimestampType,true),StructField(last_LAB,TimestampType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_LAB,TimestampType,true),StructField(last_therapy_LAB,TimestampType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:53,257 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:53,330 - lhn.item - INFO - Create name: labshgb from location: sicklecell_rerun.labshgb_scd_rwd
2025-12-22 14:18:53,345 - lhn.item - INFO - Using inputRegex: ['^labcode_standard_id$', '^labcode_standard_codingSystemId$', '^labcode_standard_primaryDisplay$', '^labid$', '^encounterid$', '^personid$', '^loincclass$', '^type$', '^servicedate$', '^serviceperiod_startDate$', '^serviceperiod_endDate$', '^typedvalue_textValue_value$', '^typedvalue_numericValue_value$', '^typedvalue_numericValue_modifier$', '^typedvalue_unitOfMeasure_standard_id$', '^typedvalue_unitOfMeasure_standard_codingSystemId$', '^typedvalue_unitOfMeasure_standard_primaryDisplay$', '^interpretation_standard_codingSystemId$', '^interpretation_standard_primaryDisplay$', '^source$', '^active$', '^issueddate$', '^tenant$', '^datetimeLab$', '^dateLab$', '^Subjects$', '^lab$']
2025-12-22 14:18:53,353 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod_startDate,StringType,true),StructField(serviceperiod_endDate,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_numericValue_modifier,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(typedvalue_unitOfMeasure_standard_codingSystemId,StringType,true),StructField(typedvalue_unitOfMeasure_standard_primaryDisplay,StringType,true),StructField(interpretation_standard_codingSystemId,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(issueddate,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeLab,TimestampType,true),StructField(dateLab,DateType,true),StructField(Subjects,LongType,true),StructField(lab,StringType,true)))
2025-12-22 14:18:53,422 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:53,519 - lhn.item - INFO - Create name: labshgbcodes from location: sicklecell_rerun.labshgbcodes_scd_rwd
2025-12-22 14:18:53,534 - lhn.item - INFO - Using inputRegex: ['^Subjects$', '^labcode_standard_primaryDisplay$', '^labcode_standard_codingSystemId$', '^labcode_standard_id$', '^lab$']
2025-12-22 14:18:53,541 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Subjects,LongType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(lab,StringType,true)))
2025-12-22 14:18:53,555 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:53,574 - lhn.item - INFO - Create name: labshydroxyuria from location: sicklecell_rerun.labshydroxyuria_scd_rwd
2025-12-22 14:18:53,587 - lhn.item - INFO - Using inputRegex: ['^labid$', '^encounterid$', '^personid$', '^labcode_standard_id$', '^labcode_standard_codingSystemId$', '^labcode_standard_primaryDisplay$', '^loincclass$', '^type$', '^servicedate$', '^serviceperiod_startDate$', '^serviceperiod_endDate$', '^typedvalue_textValue_value$', '^typedvalue_numericValue_value$', '^typedvalue_numericValue_modifier$', '^typedvalue_unitOfMeasure_standard_id$', '^typedvalue_unitOfMeasure_standard_codingSystemId$', '^typedvalue_unitOfMeasure_standard_primaryDisplay$', '^interpretation_standard_codingSystemId$', '^interpretation_standard_primaryDisplay$', '^source$', '^active$', '^issueddate$', '^tenant$', '^datetimeLab$', '^dateLab$']
2025-12-22 14:18:53,596 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod_startDate,StringType,true),StructField(serviceperiod_endDate,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_numericValue_modifier,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(typedvalue_unitOfMeasure_standard_codingSystemId,StringType,true),StructField(typedvalue_unitOfMeasure_standard_primaryDisplay,StringType,true),StructField(interpretation_standard_codingSystemId,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(issueddate,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeLab,TimestampType,true),StructField(dateLab,DateType,true)))
2025-12-22 14:18:53,661 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:53,752 - lhn.item - INFO - Create name: loinc_codes from location: sicklecell_rerun.loinc_codes_scd_rwd
2025-12-22 14:18:53,766 - lhn.item - INFO - Using inputRegex: ['^group$', '^codes$']
2025-12-22 14:18:53,773 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:53,778 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:53,786 - lhn.item - INFO - Create name: loinc_codesverified from location: sicklecell_rerun.loinc_codesverified_scd_rwd
2025-12-22 14:18:53,799 - lhn.item - INFO - Using inputRegex: ['^regexgroup$', '^Subjects$', '^labcode_standard_primaryDisplay$', '^labcode_standard_codingSystemId$', '^labcode_standard_id$', '^group$', '^codes$']
2025-12-22 14:18:53,806 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(labcode_standard_primaryDisplay,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:53,825 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:53,852 - lhn.item - INFO - Create name: loincresults from location: sicklecell_rerun.loincresults_scd_rwd
2025-12-22 14:18:53,866 - lhn.item - INFO - Using inputRegex: ['^labcode_standard_primaryDisplay$', '^labcode_standard_id$', '^labcode_standard_codingSystemId$', '^labid$', '^encounterid$', '^personid$', '^loincclass$', '^type$', '^servicedate$', '^serviceperiod_startDate$', '^serviceperiod_endDate$', '^typedvalue_textValue_value$', '^typedvalue_numericValue_value$', '^typedvalue_numericValue_modifier$', '^typedvalue_unitOfMeasure_standard_id$', '^typedvalue_unitOfMeasure_standard_codingSystemId$', '^typedvalue_unitOfMeasure_standard_primaryDisplay$', '^interpretation_standard_codingSystemId$', '^interpretation_standard_primaryDisplay$', '^source$', '^active$', '^issueddate$', '^tenant$', '^datetimeLab$', '^dateLab$', '^regexgroup$', '^Subjects$', '^group$', '^codes$']
2025-12-22 14:18:53,874 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(labcode_standard_primaryDisplay,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(labcode_standard_codingSystemId,StringType,true),StructField(labid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(loincclass,StringType,true),StructField(type,StringType,true),StructField(servicedate,StringType,true),StructField(serviceperiod_startDate,StringType,true),StructField(serviceperiod_endDate,StringType,true),StructField(typedvalue_textValue_value,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_numericValue_modifier,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(typedvalue_unitOfMeasure_standard_codingSystemId,StringType,true),StructField(typedvalue_unitOfMeasure_standard_primaryDisplay,StringType,true),StructField(interpretation_standard_codingSystemId,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(source,StringType,true),StructField(active,BooleanType,true),StructField(issueddate,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeLab,TimestampType,true),StructField(dateLab,DateType,true),StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:53,949 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,054 - lhn.item - INFO - Create name: loincresultsminimal from location: sicklecell_rerun.loincresultsminimal_scd_rwd
2025-12-22 14:18:54,068 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^dateLab$', '^group$', '^labcode_standard_id$', '^typedvalue_numericValue_value$', '^typedvalue_unitOfMeasure_standard_id$']
2025-12-22 14:18:54,076 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(dateLab,DateType,true),StructField(group,StringType,true),StructField(labcode_standard_id,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true)))
2025-12-22 14:18:54,095 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,119 - lhn.item - INFO - Create name: maritalstatus from location: sicklecell_rerun.maritalstatus_scd_rwd
2025-12-22 14:18:54,132 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^maritalstatus_standard_primaryDisplay$', '^maritalstatus$']
2025-12-22 14:18:54,139 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(maritalstatus_standard_primaryDisplay,StringType,true),StructField(maritalstatus,StringType,true)))
2025-12-22 14:18:54,151 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,166 - lhn.item - INFO - Create name: medhydroxiacodes from location: sicklecell_rerun.medhydroxiacodes_scd_rwd
2025-12-22 14:18:54,178 - lhn.item - INFO - Using inputRegex: ['^Subjects$', '^drugcode_standard_id$', '^drugcode_standard_primaryDisplay$', '^drugcode_standard_codingSystemId$']
2025-12-22 14:18:54,185 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Subjects,LongType,true),StructField(drugcode_standard_id,StringType,true),StructField(drugcode_standard_primaryDisplay,StringType,true),StructField(drugcode_standard_codingSystemId,StringType,true)))
2025-12-22 14:18:54,195 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,212 - lhn.item - INFO - Create name: medication_encounterdemobaseline from location: sicklecell_rerun.medication_encounterdemobaseline_scd_rwd
2025-12-22 14:18:54,225 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^dateMed$', '^drugcode_standard_id$', '^drugcode_standard_primaryDisplay$', '^startDate$', '^stopDate$', '^SCD$', '^yearofbirth$', '^FirstTouchDate$', '^followdate$', '^deceased$', '^followed_from_birth$', '^baseline_period$']
2025-12-22 14:18:54,232 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(dateMed,DateType,true),StructField(drugcode_standard_id,StringType,true),StructField(drugcode_standard_primaryDisplay,StringType,true),StructField(startDate,TimestampType,true),StructField(stopDate,TimestampType,true),StructField(SCD,BooleanType,true),StructField(yearofbirth,StringType,true),StructField(FirstTouchDate,DateType,true),StructField(followdate,DateType,true),StructField(deceased,BooleanType,true),StructField(followed_from_birth,BooleanType,true),StructField(baseline_period,BooleanType,true)))
2025-12-22 14:18:54,267 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,318 - lhn.item - INFO - Create name: medication_encountermeasures from location: sicklecell_rerun.medication_encountermeasures_scd_rwd
2025-12-22 14:18:54,332 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^drugcode_standard_id$', '^drugcode_standard_primaryDisplay$', '^item_count$', '^distinct_dates$', '^first_date$', '^last_date$', '^date_span$', '^avg_item_duration$', '^min_item_duration$', '^max_item_duration$', '^total_item_days$', '^active_items$', '^unknown_status_items$', '^coverage$', '^frequency$', '^status$']
2025-12-22 14:18:54,339 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(drugcode_standard_id,StringType,true),StructField(drugcode_standard_primaryDisplay,StringType,true),StructField(item_count,LongType,true),StructField(distinct_dates,LongType,true),StructField(first_date,DateType,true),StructField(last_date,DateType,true),StructField(date_span,IntegerType,true),StructField(avg_item_duration,DoubleType,true),StructField(min_item_duration,IntegerType,true),StructField(max_item_duration,IntegerType,true),StructField(total_item_days,LongType,true),StructField(active_items,LongType,true),StructField(unknown_status_items,LongType,true),StructField(coverage,DoubleType,true),StructField(frequency,DoubleType,true),StructField(status,StringType,true)))
2025-12-22 14:18:54,384 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,464 - lhn.item - INFO - Create name: medication_encounters from location: sicklecell_rerun.medication_encounters_scd_rwd
2025-12-22 14:18:54,478 - lhn.item - INFO - Using inputRegex: ['^drugcode_standard_id$', '^drugcode_standard_codingSystemId$', '^drugcode_standard_primaryDisplay$', '^medicationid$', '^encounterid$', '^personid$', '^startDate$', '^stopDate$', '^prescribingprovider$', '^tenant$', '^datetimeMed$', '^dateMed$']
2025-12-22 14:18:54,485 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(drugcode_standard_id,StringType,true),StructField(drugcode_standard_codingSystemId,StringType,true),StructField(drugcode_standard_primaryDisplay,StringType,true),StructField(medicationid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(startDate,TimestampType,true),StructField(stopDate,TimestampType,true),StructField(prescribingprovider,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeMed,TimestampType,true),StructField(dateMed,DateType,true)))
2025-12-22 14:18:54,517 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,560 - lhn.item - INFO - Create name: medshydroxia from location: sicklecell_rerun.medshydroxia_scd_rwd
2025-12-22 14:18:54,573 - lhn.item - INFO - Using inputRegex: ['^drugcode_standard_id$', '^drugcode_standard_codingSystemId$', '^drugcode_standard_primaryDisplay$', '^medicationid$', '^encounterid$', '^personid$', '^startDate$', '^stopDate$', '^prescribingprovider$', '^tenant$', '^datetimeMed$', '^dateMed$', '^Subjects$']
2025-12-22 14:18:54,581 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(drugcode_standard_id,StringType,true),StructField(drugcode_standard_codingSystemId,StringType,true),StructField(drugcode_standard_primaryDisplay,StringType,true),StructField(medicationid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(startDate,TimestampType,true),StructField(stopDate,TimestampType,true),StructField(prescribingprovider,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeMed,TimestampType,true),StructField(dateMed,DateType,true),StructField(Subjects,LongType,true)))
2025-12-22 14:18:54,615 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,662 - lhn.item - INFO - Create name: medshydroxiaindex from location: sicklecell_rerun.medshydroxiaindex_scd_rwd
2025-12-22 14:18:54,675 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^observation_period_start_date$', '^observation_period_end_date$', '^gapdaySum$', '^periodDays$', '^medDays$']
2025-12-22 14:18:54,683 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(observation_period_start_date,TimestampType,true),StructField(observation_period_end_date,TimestampType,true),StructField(gapdaySum,LongType,true),StructField(periodDays,IntegerType,true),StructField(medDays,LongType,true)))
2025-12-22 14:18:54,699 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,726 - lhn.item - INFO - Create name: medshydroxiausage from location: sicklecell_rerun.medshydroxiausage_scd_rwd
2025-12-22 14:18:54,739 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^hydroxia_start$', '^hydroxia_end$', '^hydroxia_days$']
2025-12-22 14:18:54,746 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(hydroxia_start,DateType,true),StructField(hydroxia_end,DateType,true),StructField(hydroxia_days,IntegerType,true)))
2025-12-22 14:18:54,758 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,778 - lhn.item - INFO - Create name: mrnlist from location: sicklecell_rerun.mrnlist_scd_rwd
2025-12-22 14:18:54,791 - lhn.item - INFO - Using inputRegex: ['^personid$', '^mrn$']
2025-12-22 14:18:54,798 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(mrn,StringType,true)))
2025-12-22 14:18:54,803 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,811 - lhn.item - INFO - Create name: persontenant from location: sicklecell_rerun.persontenant_scd_rwd
2025-12-22 14:18:54,824 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$']
2025-12-22 14:18:54,831 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:54,836 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,844 - lhn.item - INFO - Create name: persontenantomop from location: sicklecell_rerun.persontenantomop_scd_rwd
2025-12-22 14:18:54,857 - lhn.item - INFO - Using inputRegex: ['^personid$', '^person_id$', '^tenant$']
2025-12-22 14:18:54,864 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(person_id,LongType,true),StructField(tenant,IntegerType,true)))
2025-12-22 14:18:54,871 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:54,882 - lhn.item - INFO - Create name: phenomatrix from location: sicklecell_rerun.phenomatrix_scd_rwd
2025-12-22 14:18:54,895 - lhn.item - INFO - Using inputRegex: ['^PersonPhenotype$', '^personid$', '^BetaThalassemia$', '^HemC_Disease$', '^No_Phenotype$', '^Not_SCD$', '^SCD_Indeterminate$', '^SCD_SC$', '^SCD_SCA$', '^SCD_SCA_Likely$', '^SCD_SD$', '^SCD_SE$', '^SCD_Sbetap_Likely$', '^S_Indeterminate$', '^S_Trait$', '^HgbSMax$', '^HgbAMax$', '^HgbSLabCount$', '^HgbSLabTfxCount$', '^TfxPercent$']
2025-12-22 14:18:54,903 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(PersonPhenotype,StringType,true),StructField(personid,StringType,true),StructField(BetaThalassemia,IntegerType,true),StructField(HemC_Disease,IntegerType,true),StructField(No_Phenotype,IntegerType,true),StructField(Not_SCD,IntegerType,true),StructField(SCD_Indeterminate,IntegerType,true),StructField(SCD_SC,IntegerType,true),StructField(SCD_SCA,IntegerType,true),StructField(SCD_SCA_Likely,IntegerType,true),StructField(SCD_SD,IntegerType,true),StructField(SCD_SE,IntegerType,true),StructField(SCD_Sbetap_Likely,IntegerType,true),StructField(S_Indeterminate,IntegerType,true),StructField(S_Trait,IntegerType,true),StructField(HgbSMax,FloatType,true),StructField(HgbAMax,FloatType,true),StructField(HgbSLabCount,FloatType,true),StructField(HgbSLabTfxCount,FloatType,true),StructField(TfxPercent,FloatType,true)))
2025-12-22 14:18:54,952 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,026 - lhn.item - INFO - Create name: pregnancy_outcome_encounter from location: sicklecell_rerun.pregnancy_outcome_encounter_scd_rwd
2025-12-22 14:18:55,039 - lhn.item - INFO - Using inputRegex: ['^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^conditionid$', '^personid$', '^encounterid$', '^tenant$', '^datetimeCondition$', '^Condition$']
2025-12-22 14:18:55,047 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeCondition,TimestampType,true),StructField(Condition,StringType,true)))
2025-12-22 14:18:55,070 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,104 - lhn.item - INFO - Create name: pregnancy_outcome_icd_codes from location: sicklecell_rerun.pregnancy_outcome_icd_codes_scd_rwd
2025-12-22 14:18:55,116 - lhn.item - INFO - Using inputRegex: ['^Condition$', '^conditioncode_standard_id$', '^Description$']
2025-12-22 14:18:55,124 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Condition,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(Description,StringType,true)))
2025-12-22 14:18:55,131 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,142 - lhn.item - INFO - Create name: pregnancy_outcome_icd_codes_verified from location: sicklecell_rerun.pregnancy_outcome_icd_codes_verified_scd_rwd
2025-12-22 14:18:55,154 - lhn.item - INFO - Using inputRegex: ['^regexgroup$', '^Subjects$', '^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^Condition$', '^Description$']
2025-12-22 14:18:55,162 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(Condition,StringType,true),StructField(Description,StringType,true)))
2025-12-22 14:18:55,180 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,204 - lhn.item - INFO - Create name: proctransfusion from location: sicklecell_rerun.proctransfusion_scd_rwd
2025-12-22 14:18:55,219 - lhn.item - INFO - Using inputRegex: ['^procedurecode_standard_id$', '^procedurecode_standard_codingSystemId$', '^procedurecode_standard_primaryDisplay$', '^personid$', '^tenant$', '^procedureid$', '^encounterid$', '^servicestartdateproc$', '^serviceenddateproc$', '^principalprovider$', '^active$', '^source$', '^datetimeProc$', '^dateProc$', '^course_of_therapy$', '^index_LAB$', '^last_LAB$', '^encounter_days$', '^Subjects$']
2025-12-22 14:18:55,230 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(procedurecode_standard_id,StringType,true),StructField(procedurecode_standard_codingSystemId,StringType,true),StructField(procedurecode_standard_primaryDisplay,StringType,true),StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(procedureid,StringType,true),StructField(encounterid,StringType,true),StructField(servicestartdateproc,StringType,true),StructField(serviceenddateproc,StringType,true),StructField(principalprovider,StringType,true),StructField(active,BooleanType,true),StructField(source,StringType,true),StructField(datetimeProc,TimestampType,true),StructField(dateProc,DateType,true),StructField(course_of_therapy,LongType,true),StructField(index_LAB,TimestampType,true),StructField(last_LAB,TimestampType,true),StructField(encounter_days,LongType,true),StructField(Subjects,LongType,true)))
2025-12-22 14:18:55,281 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,348 - lhn.item - INFO - Create name: proctransfusioncodes from location: sicklecell_rerun.proctransfusioncodes_scd_rwd
2025-12-22 14:18:55,361 - lhn.item - INFO - Using inputRegex: ['^Subjects$', '^procedurecode_standard_codingSystemId$', '^procedurecode_standard_id$', '^procedurecode_standard_primaryDisplay$']
2025-12-22 14:18:55,369 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(Subjects,LongType,true),StructField(procedurecode_standard_codingSystemId,StringType,true),StructField(procedurecode_standard_id,StringType,true),StructField(procedurecode_standard_primaryDisplay,StringType,true)))
2025-12-22 14:18:55,381 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,395 - lhn.item - INFO - Create name: sicklecodes from location: sicklecell_rerun.sicklecodes_scd_rwd
2025-12-22 14:18:55,408 - lhn.item - INFO - Using inputRegex: ['^group$', '^codes$']
2025-12-22 14:18:55,415 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:55,420 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,428 - lhn.item - INFO - Create name: sicklecodesexpanded from location: sicklecell_rerun.sicklecodesexpanded_scd_rwd
2025-12-22 14:18:55,440 - lhn.item - INFO - Using inputRegex: ['^group$', '^codes$']
2025-12-22 14:18:55,447 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:55,452 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,463 - lhn.item - INFO - Create name: sicklecodesverified from location: sicklecell_rerun.sicklecodesverified_scd_rwd
2025-12-22 14:18:55,475 - lhn.item - INFO - Using inputRegex: ['^regexgroup$', '^Subjects$', '^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^group$', '^codes$']
2025-12-22 14:18:55,482 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:55,498 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,525 - lhn.item - INFO - Create name: sicklecodesverifiedexpanded from location: sicklecell_rerun.sicklecodesverifiedexpanded_scd_rwd
2025-12-22 14:18:55,537 - lhn.item - INFO - Using inputRegex: ['^regexgroup$', '^Subjects$', '^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^group$', '^codes$']
2025-12-22 14:18:55,545 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:55,561 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,587 - lhn.item - INFO - Create name: sickleconditionencounter from location: sicklecell_rerun.sickleconditionencounter_scd_rwd
2025-12-22 14:18:55,600 - lhn.item - INFO - Using inputRegex: ['^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^conditionid$', '^personid$', '^encounterid$', '^effectivedate$', '^asserteddate$', '^classification_standard_primaryDisplay$', '^confirmationstatus_standard_primaryDisplay$', '^responsibleprovider$', '^billingrank$', '^tenant$', '^datetimeCondition$', '^regexgroup$', '^Subjects$', '^group$', '^codes$']
2025-12-22 14:18:55,608 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(classification_standard_primaryDisplay,StringType,true),StructField(confirmationstatus_standard_primaryDisplay,StringType,true),StructField(responsibleprovider,StringType,true),StructField(billingrank,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeCondition,TimestampType,true),StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:55,654 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,720 - lhn.item - INFO - Create name: sickleconditionencounterexpanded from location: sicklecell_rerun.sickleconditionencounterexpanded_scd_rwd
2025-12-22 14:18:55,734 - lhn.item - INFO - Using inputRegex: ['^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^conditionid$', '^personid$', '^encounterid$', '^effectivedate$', '^asserteddate$', '^conditionType$', '^confirmationstatus_standard_primaryDisplay$', '^responsibleprovider$', '^billingrank$', '^tenant$', '^datetimeCondition$', '^regexgroup$', '^Subjects$', '^group$', '^codes$']
2025-12-22 14:18:55,742 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(conditionType,StringType,true),StructField(confirmationstatus_standard_primaryDisplay,StringType,true),StructField(responsibleprovider,StringType,true),StructField(billingrank,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeCondition,TimestampType,true),StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:55,787 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,854 - lhn.item - INFO - Create name: stemcell_codes from location: sicklecell_rerun.stemcell_codes_scd_rwd
2025-12-22 14:18:55,867 - lhn.item - INFO - Using inputRegex: ['^group$', '^codes$']
2025-12-22 14:18:55,874 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:55,879 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,887 - lhn.item - INFO - Create name: stemcellcodesverified from location: sicklecell_rerun.stemcellcodesverified_scd_rwd
2025-12-22 14:18:55,900 - lhn.item - INFO - Using inputRegex: ['^regexgroup$', '^Subjects$', '^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^group$', '^codes$']
2025-12-22 14:18:55,908 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:55,927 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:55,951 - lhn.item - INFO - Create name: stemcellid from location: sicklecell_rerun.stemcellid_scd_rwd
2025-12-22 14:18:55,964 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^course_of_therapy$', '^datetimeCondition$', '^group$', '^dateCondition$', '^index_SCD$', '^last_SCD$', '^encounter_days$', '^days_to_next_encounter$', '^index_therapy_SCD$', '^last_therapy_SCD$', '^max_gap$', '^encounter_days_for_course$']
2025-12-22 14:18:55,972 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(course_of_therapy,LongType,true),StructField(datetimeCondition,TimestampType,true),StructField(group,StringType,true),StructField(dateCondition,DateType,true),StructField(index_SCD,DateType,true),StructField(last_SCD,DateType,true),StructField(encounter_days,LongType,true),StructField(days_to_next_encounter,IntegerType,true),StructField(index_therapy_SCD,DateType,true),StructField(last_therapy_SCD,DateType,true),StructField(max_gap,IntegerType,true),StructField(encounter_days_for_course,LongType,true)))
2025-12-22 14:18:56,006 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:56,056 - lhn.item - INFO - Create name: stemcellresults from location: sicklecell_rerun.stemcellresults_scd_rwd
2025-12-22 14:18:56,070 - lhn.item - INFO - Using inputRegex: ['^conditioncode_standard_primaryDisplay$', '^conditioncode_standard_id$', '^conditioncode_standard_codingSystemId$', '^conditionid$', '^personid$', '^encounterid$', '^effectivedate$', '^asserteddate$', '^conditionType$', '^confirmationstatus_standard_primaryDisplay$', '^responsibleprovider$', '^billingrank$', '^tenant$', '^datetimeCondition$', '^dateCondition$', '^regexgroup$', '^Subjects$', '^group$', '^codes$']
2025-12-22 14:18:56,081 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(conditioncode_standard_primaryDisplay,StringType,true),StructField(conditioncode_standard_id,StringType,true),StructField(conditioncode_standard_codingSystemId,StringType,true),StructField(conditionid,StringType,true),StructField(personid,StringType,true),StructField(encounterid,StringType,true),StructField(effectivedate,StringType,true),StructField(asserteddate,StringType,true),StructField(conditionType,StringType,true),StructField(confirmationstatus_standard_primaryDisplay,StringType,true),StructField(responsibleprovider,StringType,true),StructField(billingrank,StringType,true),StructField(tenant,IntegerType,true),StructField(datetimeCondition,TimestampType,true),StructField(dateCondition,DateType,true),StructField(regexgroup,StringType,true),StructField(Subjects,LongType,true),StructField(group,StringType,true),StructField(codes,StringType,true)))
2025-12-22 14:18:56,128 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:56,194 - lhn.item - INFO - Create name: test_feature_control from location: sicklecell_rerun.test_feature_control_scd_rwd
2025-12-22 14:18:56,207 - lhn.item - INFO - Using inputRegex: ['^tableName$', '^codefield$', '^year$', '^month$', '^standard_id$', '^standard_codingSystemId$', '^standard_primaryDisplay$', '^conceptName$', '^contextId$', '^countByConcept$', '^countBySystem$', '^countByCode$', '^percentByConcept$', '^percentBySystem$', '^percentByCode$', '^n$', '^feature$']
2025-12-22 14:18:56,215 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(tableName,StringType,true),StructField(codefield,StringType,true),StructField(year,StringType,true),StructField(month,StringType,true),StructField(standard_id,StringType,true),StructField(standard_codingSystemId,StringType,true),StructField(standard_primaryDisplay,StringType,true),StructField(conceptName,StringType,true),StructField(contextId,StringType,true),StructField(countByConcept,StringType,true),StructField(countBySystem,StringType,true),StructField(countByCode,StringType,true),StructField(percentByConcept,StringType,true),StructField(percentBySystem,StringType,true),StructField(percentByCode,StringType,true),StructField(n,StringType,true),StructField(feature,StringType,true)))
2025-12-22 14:18:56,258 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:56,319 - lhn.item - INFO - Create name: transfusion from location: sicklecell_rerun.transfusion_scd_rwd
2025-12-22 14:18:56,332 - lhn.item - INFO - Using inputRegex: ['^personid$', '^date$']
2025-12-22 14:18:56,339 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(date,TimestampType,true)))
2025-12-22 14:18:56,345 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:56,353 - lhn.item - INFO - Create name: visit_detail_concept_id from location: sicklecell_rerun.visit_detail_concept_id_scd_rwd
2025-12-22 14:18:56,366 - lhn.item - INFO - Using inputRegex: ['^visit_detail_concept_id$', '^person_id$', '^visit_detail_id$', '^visit_detail_start_date$', '^visit_detail_end_date$', '^visit_detail_type_concept_id$', '^personid$', '^concept_name$']
2025-12-22 14:18:56,373 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(visit_detail_concept_id,LongType,true),StructField(person_id,LongType,true),StructField(visit_detail_id,LongType,true),StructField(visit_detail_start_date,DateType,true),StructField(visit_detail_end_date,DateType,true),StructField(visit_detail_type_concept_id,LongType,true),StructField(personid,StringType,true),StructField(concept_name,StringType,true)))
2025-12-22 14:18:56,394 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:56,425 - lhn.item - INFO - Create name: visit_occurrence_concept_id from location: sicklecell_rerun.visit_occurrence_concept_id_scd_rwd
2025-12-22 14:18:56,438 - lhn.item - INFO - Using inputRegex: ['^visit_concept_id$', '^person_id$', '^visit_occurrence_id$', '^visit_start_date$', '^visit_end_date$', '^visit_type_concept_id$', '^personid$', '^concept_name$']
2025-12-22 14:18:56,446 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(visit_concept_id,LongType,true),StructField(person_id,LongType,true),StructField(visit_occurrence_id,LongType,true),StructField(visit_start_date,DateType,true),StructField(visit_end_date,DateType,true),StructField(visit_type_concept_id,LongType,true),StructField(personid,StringType,true),StructField(concept_name,StringType,true)))
2025-12-22 14:18:56,465 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:56,495 - lhn.item - INFO - Create name: vital_sign_encounterbaseline from location: sicklecell_rerun.vital_sign_encounterbaseline_scd_rwd
2025-12-22 14:18:56,509 - lhn.item - INFO - Using inputRegex: ['^personid$', '^measurementcode_standard_id$', '^tenant$', '^SCD$', '^measurementcode_standard_codingSystemId$', '^loincclass$', '^first_test_date$', '^last_test_date$', '^testing_period_days$', '^total_tests$', '^tests_with_numeric_value$', '^average_value$', '^min_value$', '^max_value$', '^stddev_value$', '^avg_change_per_day$', '^trend_description$']
2025-12-22 14:18:56,517 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(measurementcode_standard_id,StringType,true),StructField(tenant,IntegerType,true),StructField(SCD,BooleanType,true),StructField(measurementcode_standard_codingSystemId,StringType,true),StructField(loincclass,StringType,true),StructField(first_test_date,DateType,true),StructField(last_test_date,DateType,true),StructField(testing_period_days,IntegerType,true),StructField(total_tests,LongType,true),StructField(tests_with_numeric_value,LongType,true),StructField(average_value,DoubleType,true),StructField(min_value,DoubleType,true),StructField(max_value,DoubleType,true),StructField(stddev_value,DoubleType,true),StructField(avg_change_per_day,DoubleType,true),StructField(trend_description,StringType,true)))
2025-12-22 14:18:56,558 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:56,622 - lhn.item - INFO - Create name: vital_sign_encounterdemobaseline from location: sicklecell_rerun.vital_sign_encounterdemobaseline_scd_rwd
2025-12-22 14:18:56,635 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^dateMeasurement$', '^loincclass$', '^measurementcode_standard_id$', '^measurementcode_standard_primaryDisplay$', '^interpretation_standard_primaryDisplay$', '^typedvalue_numericValue_value$', '^typedvalue_unitOfMeasure_standard_id$', '^SCD$', '^yearofbirth$', '^FirstTouchDate$', '^followdate$', '^deceased$', '^followed_from_birth$', '^baseline_period$']
2025-12-22 14:18:56,643 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(dateMeasurement,DateType,true),StructField(loincclass,StringType,true),StructField(measurementcode_standard_id,StringType,true),StructField(measurementcode_standard_primaryDisplay,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(SCD,BooleanType,true),StructField(yearofbirth,StringType,true),StructField(FirstTouchDate,DateType,true),StructField(followdate,DateType,true),StructField(deceased,BooleanType,true),StructField(followed_from_birth,BooleanType,true),StructField(baseline_period,BooleanType,true)))
2025-12-22 14:18:56,683 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:56,740 - lhn.item - INFO - Create name: vital_sign_encountermeasures from location: sicklecell_rerun.vital_sign_encountermeasures_scd_rwd
2025-12-22 14:18:56,754 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^measurementcode_standard_id$', '^measurementcode_standard_primaryDisplay$', '^typedvalue_unitOfMeasure_standard_id$', '^measurement_count$', '^distinct_dates$', '^first_date$', '^last_date$', '^date_span$', '^mean_value$', '^stddev_value$', '^min_value$', '^max_value$', '^median_value$', '^avg_daily_change$', '^min_daily_change$', '^max_daily_change$', '^value_range$', '^coefficient_variation$', '^normalized_range$', '^measurements_per_day$', '^trend_direction$']
2025-12-22 14:18:56,762 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(measurementcode_standard_id,StringType,true),StructField(measurementcode_standard_primaryDisplay,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(measurement_count,LongType,true),StructField(distinct_dates,LongType,true),StructField(first_date,DateType,true),StructField(last_date,DateType,true),StructField(date_span,IntegerType,true),StructField(mean_value,DoubleType,true),StructField(stddev_value,DoubleType,true),StructField(min_value,DoubleType,true),StructField(max_value,DoubleType,true),StructField(median_value,DoubleType,true),StructField(avg_daily_change,DoubleType,true),StructField(min_daily_change,DoubleType,true),StructField(max_daily_change,DoubleType,true),StructField(value_range,DoubleType,true),StructField(coefficient_variation,DoubleType,true),StructField(normalized_range,DoubleType,true),StructField(measurements_per_day,DoubleType,true),StructField(trend_direction,StringType,true)))
2025-12-22 14:18:56,822 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:57,003 - lhn.item - INFO - Create name: vital_sign_encounters from location: sicklecell_rerun.vital_sign_encounters_scd_rwd
2025-12-22 14:18:57,018 - lhn.item - INFO - Using inputRegex: ['^measurementcode_standard_id$', '^measurementcode_standard_codingSystemId$', '^measurementid$', '^encounterid$', '^personid$', '^loincclass$', '^servicedate$', '^typedvalue_numericValue_value$', '^typedvalue_unitOfMeasure_standard_id$', '^interpretation_standard_primaryDisplay$', '^tenant$', '^dateMeasurement$', '^subjects$', '^measurementcode_standard_primaryDisplay$']
2025-12-22 14:18:57,025 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(measurementcode_standard_id,StringType,true),StructField(measurementcode_standard_codingSystemId,StringType,true),StructField(measurementid,StringType,true),StructField(encounterid,StringType,true),StructField(personid,StringType,true),StructField(loincclass,StringType,true),StructField(servicedate,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(interpretation_standard_primaryDisplay,StringType,true),StructField(tenant,IntegerType,true),StructField(dateMeasurement,DateType,true),StructField(subjects,LongType,true),StructField(measurementcode_standard_primaryDisplay,StringType,true)))
2025-12-22 14:18:57,060 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:57,109 - lhn.item - INFO - Create name: vital_sign_encountersdemo from location: sicklecell_rerun.vital_sign_encountersdemo_scd_rwd
2025-12-22 14:18:57,123 - lhn.item - INFO - Using inputRegex: ['^personid$', '^tenant$', '^dateMeasurement$', '^measurementcode_standard_id$', '^measurementcode_standard_codingSystemId$', '^loincclass$', '^typedvalue_numericValue_value$', '^typedvalue_unitOfMeasure_standard_id$', '^SCD$', '^yearofbirth$', '^FirstTouchDate$', '^followdate$', '^deceased$', '^followed_from_birth$', '^baseline_12$']
2025-12-22 14:18:57,131 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(personid,StringType,true),StructField(tenant,IntegerType,true),StructField(dateMeasurement,DateType,true),StructField(measurementcode_standard_id,StringType,true),StructField(measurementcode_standard_codingSystemId,StringType,true),StructField(loincclass,StringType,true),StructField(typedvalue_numericValue_value,StringType,true),StructField(typedvalue_unitOfMeasure_standard_id,StringType,true),StructField(SCD,BooleanType,true),StructField(yearofbirth,StringType,true),StructField(FirstTouchDate,DateType,true),StructField(followdate,DateType,true),StructField(deceased,BooleanType,true),StructField(followed_from_birth,BooleanType,true),StructField(baseline_12,BooleanType,true)))
2025-12-22 14:18:57,168 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:57,225 - lhn.item - INFO - Create name: vital_signs_codes from location: sicklecell_rerun.vital_signs_codes_scd_rwd
2025-12-22 14:18:57,238 - lhn.item - INFO - Using inputRegex: ['^subjects$', '^measurementcode_standard_id$', '^measurementcode_standard_codingSystemId$', '^measurementcode_standard_primaryDisplay$']
2025-12-22 14:18:57,245 - lhn.spark_utils - INFO - Input DataFrame schema (cached):
StructType(List(StructField(subjects,LongType,true),StructField(measurementcode_standard_id,StringType,true),StructField(measurementcode_standard_codingSystemId,StringType,true),StructField(measurementcode_standard_primaryDisplay,StringType,true)))
2025-12-22 14:18:57,255 - lhn.spark_utils - INFO - No nested arrays or structs matching inclusionRegex, returning original DataFrame
2025-12-22 14:18:57,283 - lhn.resource - INFO - processDataTables: processed sickle to point to schema SCDSchema
2025-12-22 14:18:57,283 - lhn.resource - INFO - processDataTables: processed scd to point to schema SCDSchema
2025-12-22 14:18:57,284 - lhn.resource - INFO - processed property names: {'rwd', 'r', 'proj', 'scd', 'sickle', 'db', 's', 'ss'}
2025-12-22 14:18:57,284 - lhn.resource - INFO - processed SCDSchema to produce property scd and dictionary sickle
2025-12-22 14:18:57,285 - lhn.resource - INFO - Can call h.Extract(resource.sickle) to get the Extract object
2025-12-22 14:18:57,285 - lhn.resource - INFO - Successfully Executed: self.processAllDataTables()
2025-12-22 14:18:57,286 - lhn.resource - INFO - load_into_local: Loading into local everything = False, schemakey = projectSchema, extractName = e
2025-12-22 14:18:57,286 - lhn.resource - INFO - load_into_local: Found callFunProcessDataTables for projectSchema 
 {'data_type': 'projectTables',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'property_name': 'db',
 'schema_type': 'projectSchema',
 'tableNameTemplate': '_{disease}_{schemaTag}',
 'type_key': 'proj',
 'updateDict': False}
2025-12-22 14:18:57,287 - lhn.resource - INFO - result[e] = Extract(getattr(self, proj)
2025-12-22 14:18:57,299 - lhn.resource - INFO - key:config_local, location: /home/hnelson3/work/Users/hnelson3/Projects/SickleCell_AI/000-config.yaml
2025-12-22 14:18:57,300 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/Projects/SickleCell_AI/000-config.yaml and updating config_dict with key config_local
2025-12-22 14:18:57,450 - lhn.resource - INFO - key:config_global, location: /home/hnelson3/work/Users/hnelson3/configuration/config-global.yaml
2025-12-22 14:18:57,451 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/configuration/config-global.yaml and updating config_dict with key config_global
2025-12-22 14:18:57,521 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-global.yaml
2025-12-22 14:18:57,522 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-RWD.yaml
2025-12-22 14:18:57,522 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-IUH.yaml
2025-12-22 14:18:57,523 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-OMOP.yaml
2025-12-22 14:18:57,523 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-SCD.yaml
2025-12-22 14:18:57,524 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-project.yaml
2025-12-22 14:18:57,524 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-meta.yaml
2025-12-22 14:18:57,524 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictrwd.yaml
2025-12-22 14:18:57,525 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictomop.yaml
2025-12-22 14:18:57,525 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictscd.yaml
2025-12-22 14:18:57,526 - lhn.resource - INFO - Changing location to /home/hnelson3/work/Users/hnelson3/configuration/config-dictiuhealth.yaml
2025-12-22 14:18:57,526 - lhn.resource - INFO - key:config_RWD, location: /home/hnelson3/work/Users/hnelson3/configuration/config-RWD.yaml
2025-12-22 14:18:57,527 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/configuration/config-RWD.yaml and updating config_dict with key config_RWD
2025-12-22 14:18:57,615 - lhn.resource - INFO - key:config_IUH, location: /home/hnelson3/work/Users/hnelson3/configuration/config-IUH.yaml
2025-12-22 14:18:57,616 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/configuration/config-IUH.yaml and updating config_dict with key config_IUH
2025-12-22 14:18:57,668 - lhn.resource - INFO - key:config_OMOP, location: /home/hnelson3/work/Users/hnelson3/configuration/config-OMOP.yaml
2025-12-22 14:18:57,669 - lhn.resource - INFO - reading file /home/hnelson3/work/Users/hnelson3/configuration/config-OMOP.yaml and updating config_dict with key config_OMOP
2025-12-22 14:18:57,707 - lhn.resource - INFO - key:config_SCD, location: /home/hnelson3/work/Users/hnelson3/configuration/config-SCD.yaml
2025-12-22 14:18:57,810 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-SCD.yaml not found
2025-12-22 14:18:57,810 - lhn.resource - INFO - key:config_project, location: /home/hnelson3/work/Users/hnelson3/configuration/config-project.yaml
2025-12-22 14:18:57,846 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-project.yaml not found
2025-12-22 14:18:57,846 - lhn.resource - INFO - key:config_meta, location: /home/hnelson3/work/Users/hnelson3/configuration/config-meta.yaml
2025-12-22 14:18:57,877 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-meta.yaml not found
2025-12-22 14:18:57,878 - lhn.resource - INFO - key:config_dictrwd, location: /home/hnelson3/work/Users/hnelson3/configuration/config-dictrwd.yaml
2025-12-22 14:18:57,910 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-dictrwd.yaml not found
2025-12-22 14:18:57,911 - lhn.resource - INFO - key:config_dictomop, location: /home/hnelson3/work/Users/hnelson3/configuration/config-dictomop.yaml
2025-12-22 14:18:57,945 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-dictomop.yaml not found
2025-12-22 14:18:57,945 - lhn.resource - INFO - key:config_dictscd, location: /home/hnelson3/work/Users/hnelson3/configuration/config-dictscd.yaml
2025-12-22 14:18:57,987 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-dictscd.yaml not found
2025-12-22 14:18:57,988 - lhn.resource - INFO - key:config_dictiuhealth, location: /home/hnelson3/work/Users/hnelson3/configuration/config-dictiuhealth.yaml
2025-12-22 14:18:58,024 - lhn.resource - INFO - path /home/hnelson3/work/Users/hnelson3/configuration/config-dictiuhealth.yaml not found
2025-12-22 14:18:58,317 - lhn.resource - INFO - processDataTableBySchemakey: Processing schema name projectSchema at location sicklecell_ai (from schemas dictionary)
2025-12-22 14:18:58,318 - lhn.resource - INFO - Found: schema projectSchema:sicklecell_ai in callFunProcessDataTables
2025-12-22 14:18:58,318 - lhn.resource - INFO - Element of callFunProcessDataTables used for callFun: 
 {'data_type': 'projectTables',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'property_name': 'db',
 'schema_type': 'projectSchema',
 'tableNameTemplate': '_{disease}_{schemaTag}',
 'type_key': 'proj',
 'updateDict': False}
2025-12-22 14:18:58,366 - lhn.resource - INFO - Found schema projectSchema at sicklecell_ai
2025-12-22 14:18:58,412 - lhn.resource - INFO - Found schema projectSchema indicated by schemaTag RWD at sicklecell_ai
2025-12-22 14:18:58,413 - lhn.resource - INFO - Use to call processDataTables
2025-12-22 14:18:58,425 - lhn.resource - INFO - funCall: {'dataLoc': '/home/hnelson3/work/Users/hnelson3/inst/extdata/SickleCell_AI/',
 'dataTables': {'ADSpatSCD': {'label': 'SCD Patient level Table'},
                'ADSpatient': {'label': 'Patient Level Table'},
                'Lab_LOINCEncounter': {'datefield': 'datetimeLab',
                                       'fieldList': ['labid',
                                                     'encounterid',
                                                     'personid',
                                                     'labcode_standard_id',
                                                     'labcode_standard_codingSystemId',
                                                     'labcode_standard_primaryDisplay',
                                                     'loincclass',
                                                     'servicedate',
                                                     'typedvalue_textValue_value',
                                                     'typedvalue_numericValue_value',
                                                     'typedvalue_unitOfMeasure_standard_id',
                                                     'interpretation_standard_primaryDisplay',
                                                     'tenant',
                                                     'datetimeLab',
                                                     'dateLab',
                                                     'Test'],
                                       'histEnd': None,
                                       'histStart': None,
                                       'indexFields': ['personid', 'tenant'],
                                       'label': 'Lab Encounters from the Lab '
                                                'Codes'},
                'Lab_LOINCEncounterBaseline': {'datefield': 'first_test_date',
                                               'histEnd': None,
                                               'histStart': None,
                                               'indexFields': ['personid',
                                                               'tenant'],
                                               'label': 'Lab Encounters from '
                                                        'the Lab Codes at '
                                                        'baseline'},
                'Lab_LOINCEncounterDemo': {'datefield': 'dateLab',
                                           'histEnd': None,
                                           'histStart': None,
                                           'indexFields': ['personid',
                                                           'tenant'],
                                           'label': 'Lab Encounters from the '
                                                    'Lab Codes with '
                                                    'demographic info added'},
                'Lab_LOINCEncounterDemoBaseline': {'datefield': 'dateLab',
                                                   'histEnd': None,
                                                   'histStart': None,
                                                   'indexFields': ['personid',
                                                                   'tenant'],
                                                   'label': 'Lab Encounters '
                                                            'from the Lab '
                                                            'Codes with '
                                                            'demographic info '
                                                            'added and subset '
                                                            'to baseline'},
                'Lab_LOINCEncounterFeature': {'datefield': 'datetimeLab',
                                              'fieldList': ['labid',
                                                            'encounterid',
                                                            'personid',
                                                            'labcode_standard_id',
                                                            'labcode_standard_codingSystemId',
                                                            'labcode_standard_primaryDisplay',
                                                            'loincclass',
                                                            'servicedate',
                                                            'typedvalue_textValue_value',
                                                            'typedvalue_numericValue_value',
                                                            'typedvalue_unitOfMeasure_standard_id',
                                                            'interpretation_standard_primaryDisplay',
                                                            'tenant',
                                                            'datetimeLab',
                                                            'dateLab',
                                                            'Test'],
                                              'histEnd': None,
                                              'histStart': None,
                                              'indexFields': ['personid',
                                                              'tenant'],
                                              'label': 'Lab Encounters from '
                                                       'the Lab Codes'},
                'Lab_LOINC_Codes_Verified': {'datefield': None,
                                             'groupName': 'regexgroup',
                                             'indexFields': ['labcode_standard_primaryDisplay',
                                                             'labcode_standard_id',
                                                             'labcode_standard_codingSystemId'],
                                             'label': 'Lab Codes to indicate '
                                                      'comorbidities'},
                'Lab_LOINC_Codes_regex': {'complete': False,
                                          'label': 'Lab Codes to indicate '
                                                   'comorbidities',
                                          'listIndex': 'Regex',
                                          'sourceField': 'labcode_standard_primaryDisplay'},
                'Pregnancy_Outcome_Encounter': {'datefield': 'datetimeCondition',
                                                'fieldList': ['conditionid',
                                                              'personid',
                                                              'encounterid',
                                                              'conditioncode_standard_id',
                                                              'conditioncode_standard_codingSystemId',
                                                              'conditioncode_standard_primaryDisplay',
                                                              'tenant',
                                                              'datetimeCondition',
                                                              'Condition'],
                                                'histEnd': None,
                                                'histStart': None,
                                                'indexFields': ['personid',
                                                                'tenant'],
                                                'label': 'Lab Encounters from '
                                                         'the Lab Codes'},
                'Pregnancy_Outcome_EncounterFeature': {'datefield': 'datetimeCondition',
                                                       'fieldList': ['personid',
                                                                     'tenant',
                                                                     'datetimeCondition',
                                                                     'Condition'],
                                                       'histEnd': None,
                                                       'histStart': None,
                                                       'indexFields': ['personid',
                                                                       'tenant'],
                                                       'label': 'Lab '
                                                                'Encounters '
                                                                'from the Lab '
                                                                'Codes'},
                'Pregnancy_Outcome_ICD_Codes': {'complete': True,
                                                'label': 'Condition Codes to '
                                                         'indicate Pregnancy',
                                                'listIndex': 'conditioncode_standard_id',
                                                'sourceField': 'conditioncode_standard_id'},
                'Pregnancy_Outcome_ICD_Codes_Verified': {'datefield': None,
                                                         'groupName': 'regexgroup',
                                                         'indexFields': ['conditioncode_standard_primaryDisplay',
                                                                         'conditioncode_standard_id',
                                                                         'conditioncode_standard_codingSystemId'],
                                                         'label': 'Condition '
                                                                  'Codes to '
                                                                  'indicate '
                                                                  'Pregnancy'},
                'birthsex': {'label': 'Patient Level maritalstatus'},
                'comorbidity': {'label': 'Feature control file'},
                'comorbidity_Codes_Verified': {'datefield': None,
                                               'groupName': 'regexgroup',
                                               'indexFields': ['conditioncode_standard_primaryDisplay',
                                                               'conditioncode_standard_id',
                                                               'conditioncode_standard_codingSystemId'],
                                               'label': 'Codes to indicate '
                                                        'comorbidities'},
                'comorbidity_ConditionEncounter': {'datefield': 'datetimeCondition',
                                                   'fieldList': ['conditionid',
                                                                 'personid',
                                                                 'encounterid',
                                                                 'conditioncode_standard_id',
                                                                 'conditioncode_standard_codingSystemId',
                                                                 'conditioncode_standard_primaryDisplay',
                                                                 'tenant',
                                                                 'datetimeCondition',
                                                                 'Condition'],
                                                   'histEnd': None,
                                                   'histStart': None,
                                                   'indexFields': ['personid',
                                                                   'tenant'],
                                                   'label': 'Condition '
                                                            'Encounters from '
                                                            'the Comorbidity '
                                                            'Codes'},
                'comorbidity_ICD10_Codes': {'complete': False,
                                            'label': 'Codes to indicate '
                                                     'comorbidities',
                                            'listIndex': 'Condition',
                                            'sourceField': 'conditioncode_standard_primaryDisplay'},
                'comorbidity_ICD10_Codes_Verified': {'datefield': None,
                                                     'groupName': 'regexgroup',
                                                     'indexFields': ['conditioncode_standard_primaryDisplay',
                                                                     'conditioncode_standard_id',
                                                                     'conditioncode_standard_codingSystemId'],
                                                     'label': 'Codes to '
                                                              'indicate '
                                                              'comorbidities'},
                'comorbidity_ICD9_Codes': {'complete': False,
                                           'label': 'Codes to indicate '
                                                    'comorbidities',
                                           'listIndex': 'Condition',
                                           'sourceField': 'conditioncode_standard_primaryDisplay'},
                'comorbidity_ICD9_Codes_Verified': {'datefield': None,
                                                    'groupName': 'regexgroup',
                                                    'indexFields': ['conditioncode_standard_primaryDisplay',
                                                                    'conditioncode_standard_id',
                                                                    'conditioncode_standard_codingSystemId'],
                                                    'label': 'Codes to '
                                                             'indicate '
                                                             'comorbidities'},
                'comorbidity_procedureEncounter': {'datefield': 'datetimeProcedure',
                                                   'fieldList': ['procedureid',
                                                                 'personid',
                                                                 'encounterid',
                                                                 'procedurecode_standard_id',
                                                                 'procedurecode_standard_codingSystemId',
                                                                 'procedurecode_standard_primaryDisplay',
                                                                 'servicestartdateproc',
                                                                 'serviceenddateproc',
                                                                 'tenant',
                                                                 'datetimeProc',
                                                                 'dateProc'],
                                                   'histEnd': None,
                                                   'histStart': None,
                                                   'indexFields': ['personid',
                                                                   'tenant'],
                                                   'label': 'Procedure '
                                                            'Encounters from '
                                                            'the procedure '
                                                            'Codes'},
                'comorbidity_procedureEncounterFeature': {'datefield': 'datetimeProcedure',
                                                          'fieldList': ['procedureid',
                                                                        'personid',
                                                                        'encounterid',
                                                                        'procedurecode_standard_id',
                                                                        'procedurecode_standard_codingSystemId',
                                                                        'procedurecode_standard_primaryDisplay',
                                                                        'servicestartdateproc',
                                                                        'serviceenddateproc',
                                                                        'tenant',
                                                                        'datetimeProc',
                                                                        'dateProc'],
                                                          'histEnd': None,
                                                          'histStart': None,
                                                          'indexFields': ['personid',
                                                                          'tenant'],
                                                          'label': 'Procedure '
                                                                   'Encounters '
                                                                   'from the '
                                                                   'procedure '
                                                                   'Codes'},
                'comorbidity_procedure_Codes': {'complete': True,
                                                'label': 'Procedure Codes to '
                                                         'indicate '
                                                         'comorbidities',
                                                'listIndex': 'procedurecode_standard_id',
                                                'sourceField': 'procedurecode_standard_id'},
                'comorbidity_procedure_Codes_Verified': {'datefield': None,
                                                         'groupName': 'regexgroup',
                                                         'indexFields': ['procedurecode_standard_primaryDisplay',
                                                                         'procedurecode_standard_id',
                                                                         'procedurecode_standard_codingSystemId'],
                                                         'label': 'Procedure '
                                                                  'Codes to '
                                                                  'indicate '
                                                                  'comorbidities'},
                'condition_feature_codesVerified': {'datefield': None,
                                                    'groupName': 'regexgroup',
                                                    'indexFields': ['labcode_standard_primaryDisplay',
                                                                    'labcode_standard_id',
                                                                    'labcode_standard_codingSystemId'],
                                                    'label': ' '
                                                             'condition_feature_codesVerifieds'},
                'condition_features': {'datefield': 'datetimeLab',
                                       'fieldList': ['personid',
                                                     'tenant',
                                                     'encounterid',
                                                     'conditionid',
                                                     'conditioncode_standard_id',
                                                     'conditioncode_standard_codingSystemId',
                                                     'conditioncode_standard_primaryDisplay',
                                                     'datetimeCondition',
                                                     'feature'],
                                       'histEnd': '2025-01-01',
                                       'histStart': '1990-01-01',
                                       'label': 'condition values to include '
                                                'as features'},
                'condition_features_codes': {'groupname': 'regex',
                                             'indexFields': ['conditioncode_standard_id',
                                                             'conditioncode_standard_codingSystemId'],
                                             'label': 'code list for condition '
                                                      'features'},
                'corrections': {'datefieldPrimary': ['datetimeEnc'],
                                'datefieldStop': ['dischargedate'],
                                'histEnd': '2025-01-01',
                                'histStart': '1990-01-01',
                                'indexFields': ['personid',
                                                'tenant',
                                                'financialclass_standard_primaryDisplay'],
                                'label': 'All insurance Enounter Records',
                                'max_gap': 90,
                                'retained_fields': ['personid',
                                                    'tenant',
                                                    'financialclass_standard_primaryDisplay',
                                                    'encType',
                                                    'servicedate',
                                                    'dischargedate',
                                                    'datetimeEnc',
                                                    'dateEnc']},
                'death': {'indexFields': ['personid', 'tenant'],
                          'label': 'Patient Level Death'},
                'deathList': {'label': 'RWD death Levels'},
                'demo': {'datefield': 'birthdate',
                         'index': ['personid', 'tenant'],
                         'label': 'Combined demographics and '
                                  'preferred_demographics table'},
                'demoOMOP': {'datefield': 'birthdate',
                             'index': ['personid', 'tenant'],
                             'label': 'Combined demographics and '
                                      'preferred_demographics table and OMOP'},
                'drug_codes': {'complete': True,
                               'dictionary': {'Analgesics': 'N02',
                                              'Antibiotics': 'J01'},
                               'indexFields': ['drugcode_standard_id',
                                               'drugcode_standard_codingSystemId',
                                               'drugcode_standard_primaryDisplay'],
                               'label': 'Medication Codes of Interest',
                               'listIndex': 'codes',
                               'sourceField': 'medications_drugCode_standard_id'},
                'encSDF': {"encTypeValues'": ['Outpatient',
                                              'Inpatient',
                                              'Emergency',
                                              'Admitted for Observation'],
                           'label': 'encSDF table for hemoglobinopath ICD '
                                    'phenotyping',
                           'retained_fields': ['personid',
                                               'encounterid',
                                               'servicedate',
                                               'dischargedate',
                                               'encType',
                                               'tenant']},
                'encounter': {'broadcast_flag': False,
                              'cacheResult': False,
                              'cohort': None,
                              'cohortColumns': None,
                              'datefield': 'datetimeEnc',
                              'elementExtract': ['encounterid'],
                              'elementIndex': ['personid', 'tenant'],
                              'histEnd': '2025-01-01',
                              'histStart': '1990-01-01',
                              'howCohortJoin': 'right',
                              'howjoin': 'inner',
                              'indexFields': ['personid',
                                              'tenant',
                                              'encounterid'],
                              'label': 'Encounter Extraction, Matching '
                                       'Condition Encounter to the Encounter '
                                       'Table',
                              'masterList': ['personid',
                                             'tenant',
                                             'encounterid',
                                             'datetimeCondition',
                                             'index_SCD',
                                             'last_SCD',
                                             'encounter_days',
                                             'days_to_next_encounter',
                                             'index_therapy_SCD',
                                             'last_therapy_SCD',
                                             'max_gap',
                                             'encounter_days_for_course'],
                              'retained_fields': ['classification_standard_primaryDisplay',
                                                  'confirmationstatus_standard_primaryDisplay',
                                                  'group',
                                                  'tenant'],
                              'sourceFields': ['facilityids',
                                               'reasonforvisit_standard_primaryDisplay',
                                               'financialclass_standard_primaryDisplay',
                                               'hospitalservice_standard_primaryDisplay',
                                               'classification_standard_id',
                                               'encType',
                                               'type_standard_primaryDisplay',
                                               'hospitalizationstartdate',
                                               'readmission',
                                               'servicedate',
                                               'dischargedate',
                                               'encountertypes_classification_standard_id',
                                               'admissiontype_standard_primaryDisplay',
                                               'status_standard_primaryDisplay',
                                               'actualarrivaldate',
                                               'datetimeEnc',
                                               'dateEnc']},
                'encounterExtract': {'label': 'Extract some columns to csv'},
                'encounterId': {'code': 'SCD',
                                'datefield': 'datetimeCondition',
                                'datefieldPrimary': ['datetimeCondition'],
                                'datefieldStop': ['datetimeCondition'],
                                'histEnd': '2025-01-01',
                                'histStart': '1990-01-01',
                                'index': ['personid', 'tenant'],
                                'indexFields': ['personid',
                                                'tenant',
                                                'encounterid'],
                                'label': 'One Record Per Condition Encounter',
                                'max_gap': 5000,
                                'retained_fields': ['classification_standard_primaryDisplay',
                                                    'confirmationstatus_standard_primaryDisplay',
                                                    'group',
                                                    'tenant'],
                                'sort_fields': ['datetimeCondition']},
                'evalPhenoRow': {'label': 'Phenotyping'},
                'extractList': {'label': 'Phenotyping'},
                'feature1': {'groupName': 'features',
                             'label': 'Results search features from table 1'},
                'final_df_scd_rwd': {'label': 'Phenotyping'},
                'follow': {'label': 'Followup Start and Stop Dates From '
                                    'Condition, Lab and Procedure'},
                'followCondition': {'code': 'cond',
                                    'datefieldPrimary': 'datetimeCondition',
                                    'datefieldStop': 'datetimeCondition',
                                    'histEnd': '2025-12-22',
                                    'histStart': '2000-01-01',
                                    'indexFields': ['personid', 'tenant'],
                                    'label': 'Follow-up metrics from '
                                             'Conditions using '
                                             'write_index_table',
                                    'max_gap': 1096,
                                    'retained_fields': [],
                                    'sort_fields': ['datetimeCondition']},
                'followEnc': {'code': 'enc',
                              'datefieldPrimary': ['datetimeEnc'],
                              'datefieldStop': ['dischargedate'],
                              'histEnd': '2025-12-22',
                              'histStart': '2000-01-01',
                              'indexFields': ['personid', 'tenant'],
                              'label': 'Follow-up metrics from Encounters '
                                       'using write_index_table',
                              'max_gap': 1096,
                              'retained_fields': [],
                              'sort_fields': ['datetimeEnc']},
                'followLab': {'code': 'lab',
                              'datefieldPrimary': 'datetimeLab',
                              'datefieldStop': 'datetimeLab',
                              'histEnd': '2025-12-22',
                              'histStart': '2000-01-01',
                              'indexFields': ['personid', 'tenant'],
                              'label': 'Follow-up metrics from Labs using '
                                       'write_index_table',
                              'max_gap': 1096,
                              'retained_fields': [],
                              'sort_fields': ['datetimeLab']},
                'followMed': {'code': 'med',
                              'datefieldPrimary': 'startDate',
                              'datefieldStop': 'stopDate',
                              'histEnd': '2025-12-22',
                              'histStart': '2000-01-01',
                              'indexFields': ['personid', 'tenant'],
                              'label': 'Follow-up metrics from Medications '
                                       'using write_index_table',
                              'max_gap': 1096,
                              'retained_fields': [],
                              'sort_fields': ['startDate']},
                'followProc': {'code': 'proc',
                               'datefieldPrimary': 'datetimeProc',
                               'datefieldStop': 'datetimeProc',
                               'histEnd': '2025-12-22',
                               'histStart': '2000-01-01',
                               'indexFields': ['personid', 'tenant'],
                               'label': 'Follow-up metrics from Procedures '
                                        'using write_index_table',
                               'max_gap': 1096,
                               'retained_fields': [],
                               'sort_fields': ['datetimeProc']},
                'gap_conditionID': {'code': 'SCD',
                                    'datefieldPrimary': ['datetimeCondition'],
                                    'datefieldStop': ['datetimeCondition'],
                                    'histEnd': '2025-01-01',
                                    'histStart': '1990-01-01',
                                    'indexFields': ['personid', 'tenant'],
                                    'label': 'One Record Per Condition '
                                             'Encounter',
                                    'max_gap': 1096,
                                    'retained_fields': ['personid',
                                                        'encounterid',
                                                        'tenant',
                                                        'datetimeCondition',
                                                        'dateCondition'],
                                    'sort_fields': ['datetimeCondition']},
                'gap_encounterID': {'code': 'gap',
                                    'datefieldPrimary': ['servicedate'],
                                    'datefieldStop': ['dischargedate'],
                                    'histEnd': '2025-01-01',
                                    'histStart': '1990-01-01',
                                    'indexFields': ['personid', 'tenant'],
                                    'label': 'One Record Per Encounter',
                                    'max_gap': 1096,
                                    'retained_fields': ['personid',
                                                        'encounterid',
                                                        'tenant',
                                                        'datetimeEnc',
                                                        'dateEnc',
                                                        'servicedate',
                                                        'dischargedate'],
                                    'sort_fields': ['datetimeEnc']},
                'hgb_subunits': {'datefield': 'servicedate',
                                 'label': 'Phenotyping'},
                'hydroxpatients': {'label': 'First date of hydroxiria'},
                'hydroxyurea': {'datefield': 'date',
                                'label': 'Hydroxyurea Usage for phenotyping'},
                'icdSDF': {'encTypeValues': ['Outpatient',
                                             'Inpatient',
                                             'Emergency',
                                             'Admitted for Observation'],
                           'label': 'icdSDF table for hemoglobinopath ICD '
                                    'phenotyping',
                           'retained_fields': ['personid',
                                               'encounterid',
                                               'conditioncode_standard_id',
                                               'date',
                                               'conditioncode_standard_id',
                                               'IcdCategory',
                                               'servicedate',
                                               'dischargedate',
                                               'encType',
                                               'tenant']},
                'insurance': {'datefieldPrimary': ['datetimeEnc'],
                              'datefieldStop': ['dischargedate'],
                              'histEnd': '2025-01-01',
                              'histStart': '1990-01-01',
                              'indexFields': ['personid',
                                              'tenant',
                                              'financialclass_standard_primaryDisplay'],
                              'label': 'All insurance Enounter Records',
                              'max_gap': 90,
                              'retained_fields': ['personid',
                                                  'tenant',
                                                  'financialclass_standard_primaryDisplay',
                                                  'encType',
                                                  'servicedate',
                                                  'dischargedate',
                                                  'datetimeEnc',
                                                  'dateEnc']},
                'insuranceGroups': {'label': 'Insurance'},
                'insurancePeriods': {'code': 'insurance',
                                     'datefieldPrimary': ['datetimeEnc'],
                                     'datefieldStop': ['dischargedate'],
                                     'histEnd': '2025-01-01',
                                     'histStart': '1990-01-01',
                                     'indexFields': ['personid',
                                                     'tenant',
                                                     'financialclass_standard_primaryDisplay'],
                                     'label': 'Insurance Periods of '
                                              'Observation',
                                     'max_gap': 90,
                                     'retained_fields': ['personid',
                                                         'tenant',
                                                         'financialclass_standard_primaryDisplay',
                                                         'encType',
                                                         'servicedate',
                                                         'dischargedate',
                                                         'datetimeEnc',
                                                         'dateEnc'],
                                     'sort_fields': ['personid',
                                                     'tenant',
                                                     'financialclass_standard_primaryDisplay',
                                                     'datetimeEnc']},
                'labPersonId': {'code': 'LAB',
                                'datefieldPrimary': ['datetimeLab'],
                                'datefieldStop': 'datetimeLab',
                                'histEnd': None,
                                'histStart': None,
                                'indexFields': ['personid', 'tenant'],
                                'label': 'One Record Per Lab Person ID',
                                'max_gap': 30000,
                                'retained_fields': ['labcode_standard_primaryDisplay',
                                                    'typedvalue_textValue_value',
                                                    'typedvalue_numericValue_value',
                                                    'typedvalue_unitOfMeasure_standard_primaryDisplay',
                                                    'interpretation_standard_primaryDisplay',
                                                    'lab',
                                                    'source',
                                                    'Subjects'],
                                'sort_fields': ['datetimeLab']},
                'lab_features': {'datefield': 'datetimeLab',
                                 'fieldList': ['personid',
                                               'tenant',
                                               'encounterid',
                                               'labid',
                                               'labcode_standard_id',
                                               'labcode_standard_codingSystemId',
                                               'labcode_standard_primaryDisplay',
                                               'loincclass',
                                               'datetimeLab',
                                               'typedvalue_numericValue_value',
                                               'typedvalue_textValue_value',
                                               'loincclass',
                                               'feature'],
                                 'histEnd': '2025-01-01',
                                 'histStart': '1990-01-01',
                                 'label': 'lab values to include as features'},
                'lab_features_codes': {'groupname': 'regex',
                                       'indexFields': ['labcode_standard_id',
                                                       'labcode_standard_codingSystemId'],
                                       'label': 'code list for lab features'},
                'lablist': {'label': 'list of labs to categorize'},
                'labsHgb': {'datefield': 'datetimeLab',
                            'datefieldPrimary': 'datetimeLab',
                            'indexFields': ['personid', 'tenant'],
                            'label': 'HgB Lab Records form from the lab table'},
                'labsHgbCodes': {'complete': True,
                                 'indexFields': ['labcode_standard_id',
                                                 'labcode_standard_codingSystemId',
                                                 'labcode_standard_primaryDisplay'],
                                 'label': 'A set of HgB Codes',
                                 'listIndex': 'lab',
                                 'sourceField': 'labcode_standard_id'},
                'labshydroxyuria': {'datefield': 'datetimeLab',
                                    'datefieldPrimary': 'datetimeLab',
                                    'indexFields': ['personid', 'tenant'],
                                    'label': 'HgB Lab Records form from the '
                                             'lab table'},
                'loinc_codes': {'complete': True,
                                'dictionary': {'AST': '1920-8',
                                               'Alkaline Phosphatase': '6768-6',
                                               'BUN': '3094-0',
                                               'Calculated eGFR': '48642-3',
                                               'Creatinine': '2160-0',
                                               'Haemoglobin': '718-7',
                                               'Haemoglobin F': '4579-1',
                                               'LDH': '14804-9',
                                               'NT-proBNP': '33763-1',
                                               'Potassium': '2823-3',
                                               'WBC count': '6690-2'},
                                'label': 'Lab whose values can be a predictor '
                                         'of death, from previously published '
                                         'models.',
                                'listIndex': 'codes',
                                'sourceField': 'labcode_standard_id'},
                'loinc_codesVerified': {'datefield': None,
                                        'groupName': 'regexgroup',
                                        'indexFields': ['labcode_standard_primaryDisplay',
                                                        'labcode_standard_id',
                                                        'labcode_standard_codingSystemId'],
                                        'label': ' Verified Loinc Codes'},
                'loincresults': {'datefield': 'datetimeLab',
                                 'groupName': 'labgroup',
                                 'histEnd': '2025-01-01',
                                 'histStart': '1990-01-01',
                                 'label': 'Results searching labs with loinc '
                                          'codes'},
                'loincresultsminimal': {'datefield': 'dateLab',
                                        'groupName': 'labgroup',
                                        'histEnd': '2025-01-01',
                                        'histStart': '1990-01-01',
                                        'label': 'Results searching labs with '
                                                 'loinc codes'},
                'maritalstatus': {'label': 'Patient level marital status'},
                'maritalstatusList': {'label': 'RWD Marital Status Levels'},
                'medHydroxiacodes': {'datefield': None,
                                     'indexFields': ['drugcode_standard_id',
                                                     'drugcode_standard_codingSystemId',
                                                     'drugcode_standard_primaryDisplay'],
                                     'label': 'Hydroxia Medication'},
                'medication_encounterDemoBaseline': {'datefield': 'dateMed',
                                                     'histEnd': None,
                                                     'histStart': None,
                                                     'indexFields': ['personid',
                                                                     'tenant'],
                                                     'label': 'Medication '
                                                              'Encounters from '
                                                              'the Lab Codes '
                                                              'with '
                                                              'demographic '
                                                              'info added and '
                                                              'subset to '
                                                              'baseline'},
                'medication_encountermeasures': {'datefield': 'dateMed',
                                                 'label': 'Medications with '
                                                          'feature values in a '
                                                          'long format'},
                'medication_encounters': {'datefield': 'dateMed',
                                          'histEnd': '2025-01-01',
                                          'histStart': '1990-01-01',
                                          'indexFields': ['personid', 'tenant'],
                                          'label': 'Medication Encounter '
                                                   'Records',
                                          'retained_fields': ['medicationid',
                                                              'encounterid',
                                                              'personid',
                                                              'startDate',
                                                              'stopDate',
                                                              'drugcode_standard_id',
                                                              'drugcode_standard_codingSystemId',
                                                              'drugcode_standard_primaryDisplay',
                                                              'prescribingprovider',
                                                              'tenant',
                                                              'dateMed']},
                'medication_encounters_index': {'code': 'MED',
                                                'datefieldPrimary': 'dateMed',
                                                'datefieldStop': 'stopDate',
                                                'histEnd': '2025-01-01',
                                                'histStart': '1990-01-01',
                                                'indexFields': ['personid',
                                                                'tenant',
                                                                'drugcode_standard_id'],
                                                'label': 'First Medication '
                                                         'Encounter for Each '
                                                         'Patient-Medication '
                                                         'Combination',
                                                'max_gap': 90,
                                                'retained_fields': ['medicationid',
                                                                    'encounterid',
                                                                    'startDate',
                                                                    'stopDate',
                                                                    'drugcode_standard_primaryDisplay',
                                                                    'prescribingprovider'],
                                                'sort_fields': ['dateMed']},
                'medication_features': {'datefield': 'dateMed',
                                        'fieldList': ['personid',
                                                      'tenant',
                                                      'encounterid',
                                                      'medicationid',
                                                      'drugcode_standard_id',
                                                      'drugcode_standard_codingSystemId',
                                                      'drugcode_standard_primaryDisplay',
                                                      'datetimeMed',
                                                      'startDate',
                                                      'stopDate',
                                                      'feature'],
                                        'histEnd': '2025-01-01',
                                        'histStart': '1990-01-01',
                                        'label': 'medication values to include '
                                                 'as features'},
                'medication_features_codes': {'groupname': 'regex',
                                              'indexFields': ['medicationcode_standard_id',
                                                              'medicationcode_standard_codingSystemId'],
                                              'label': 'code list for '
                                                       'medication features'},
                'medsHydroxia': {'datefield': 'datetimeMed',
                                 'histEnd': '2025-01-01',
                                 'histStart': '1990-01-01',
                                 'indexFields': ['personid',
                                                 'tenant',
                                                 'encounterid'],
                                 'label': 'Hydroxia Medication Records',
                                 'retained_fields': ['personid',
                                                     'tenant',
                                                     'encounterid',
                                                     'drugcode_standard_id',
                                                     'drugcode_standard_codingSystemId',
                                                     'drugcode_standard_primaryDisplay',
                                                     'personid',
                                                     'tenant',
                                                     'medicationid',
                                                     'encounterid',
                                                     'startdate',
                                                     'stopdate',
                                                     'prescribingprovider',
                                                     'datetimeMed',
                                                     'dateMed']},
                'medsHydroxiaIndex': {'code': 'hydroxia',
                                      'datefield': 'datetimeMed',
                                      'datefieldPrimary': ['datetimeMed'],
                                      'datefieldStop': 'stopdate',
                                      'histEnd': '2025-01-01',
                                      'histStart': '1990-01-01',
                                      'indexFields': ['personid', 'tenant'],
                                      'label': 'Hydroxia Patient Level Data',
                                      'max_gap': 30000,
                                      'retained_fields': ['drugcode_standard_id',
                                                          'drugcode_standard_codingSystemId',
                                                          'drugcode_standard_primaryDisplay',
                                                          'prescribingprovider',
                                                          'dateMed',
                                                          'stopdate'],
                                      'sort_fields': ['datetimeMed']},
                'medsHydroxiaUsage': {'datefield': 'hydroxia_start',
                                      'indexFields': ['personid', 'tenant'],
                                      'label': 'Usage of Hydroxia'},
                'medshydroxiadates': {'label': 'Phenotyping'},
                'mortality': {'label': 'Phenotyping'},
                'mrnList': {'indexFields': ['personid'],
                            'label': 'MRNs of the IUH Patients:'},
                'observation_period': {'course_id_col': 'course_of_therapy_id',
                                       'index_col': 'index_therapy_enc',
                                       'label': 'Observation Period based on '
                                                'the Encounters',
                                       'last_course_col': 'last_therapy_enc',
                                       'last_overall_col': 'last_enc',
                                       'max_course_col': 'max_course_of_therapy_id',
                                       'other_select_cols': ['tenant'],
                                       'perform_check': True,
                                       'person_id_col': 'personid'},
                'persontenant': {'indexFields': ['personid', 'tenant'],
                                 'label': 'Person and Tenant ID for every '
                                          'member of the cohort and control',
                                 'partitionBy': 'tenant',
                                 'partitionby': 'tenant'},
                'persontenantOMOP': {'indexFields': ['person_id',
                                                     'personid',
                                                     'tenant'],
                                     'label': 'Person and Tenant ID with OMOP '
                                              'person_id',
                                     'partitionBy': 'tenant',
                                     'partitionby': 'tenant'},
                'phenoMatrix': {'label': 'Phenotyping'},
                'pheno_id_extra': {'label': 'Phenotyping'},
                'problem_list_features': {'datefield': 'datetimeProblem',
                                          'fieldList': ['personid',
                                                        'tenant',
                                                        'encounterid',
                                                        'problemlistid',
                                                        'drugcode_standard_id',
                                                        'problemlistcode_standard_codingSystemId',
                                                        'problemlistcode_standard_primaryDisplay',
                                                        'datetimeProblem',
                                                        'feature'],
                                          'histEnd': '2025-01-01',
                                          'histStart': '1990-01-01',
                                          'label': 'problem list values to '
                                                   'include as features'},
                'problem_list_features_codes': {'groupname': 'regex',
                                                'indexFields': ['problemlistcode_standard_id',
                                                                'problemlistcode_standard_codingSystemId'],
                                                'label': 'code list for '
                                                         'problem list '
                                                         'features'},
                'proctransfusion': {'cohortColumns': ['personid',
                                                      'encounterid',
                                                      'tenant',
                                                      'course_of_therapy',
                                                      'index_LAB',
                                                      'last_LAB',
                                                      'encounter_days'],
                                    'datefield': 'datetimeProc',
                                    'histEnd': None,
                                    'histStart': None,
                                    'indexFields': ['personid',
                                                    'tenant',
                                                    'encounterid'],
                                    'label': 'Tranfusion Records from the '
                                             'Procedure Table',
                                    'retained_fields': ['personid',
                                                        'encounterid',
                                                        'tenant',
                                                        'course_of_therapy',
                                                        'index_LAB',
                                                        'last_LAB',
                                                        'encounter_days',
                                                        'datetimeProc',
                                                        'dateProc']},
                'proctransfusioncodes': {'indexFields': ['procedurecode_standard_id',
                                                         'procedurecode_standard_codingSystemId',
                                                         'procedurecode_standard_primaryDisplay'],
                                         'label': 'A list of the Transfusion '
                                                  'Procedures Needed'},
                'scd_death': {'lable': 'Death'},
                'scd_demo': {'label': 'Demographics'},
                'scd_possible': {'label': 'Possible Case'},
                'scdboth': {'label': 'Both Case and Control'},
                'scdpatient': {'label': 'SCD Case Patients'},
                'sickleConditionEncounter': {'datefield': 'datetimeCondition',
                                             'histEnd': None,
                                             'histStart': None,
                                             'indexFields': ['personid'],
                                             'label': ' Sickle Cell Encounters '
                                                      'from the conditions '
                                                      'table'},
                'sickleConditionEncounterExpanded': {'datefield': 'datetimeCondition',
                                                     'histEnd': None,
                                                     'histStart': None,
                                                     'indexFields': ['personid'],
                                                     'label': ' Sickle Cell '
                                                              'Encounters from '
                                                              'the conditions '
                                                              'table'},
                'sicklecodes': {'complete': True,
                                'dictionary': {'other': 'D58|282.[0,1,2,3,7,8,9]',
                                               'scd': 'D57.[0,1,2,4,8]|282.4[1,2]|282.6',
                                               'thal': 'D56|282.4[0,3,4,5,6,7,9]',
                                               'trait': 'D57.3|282.5'},
                                'label': 'Codes to search for Sickle Cell '
                                         'Conditions',
                                'listIndex': 'codes',
                                'sourceField': 'conditioncode_standard_id'},
                'sicklecodesVerified': {'datefield': None,
                                        'groupName': 'regexgroup',
                                        'indexFields': ['conditioncode_standard_primaryDisplay',
                                                        'conditioncode_standard_id',
                                                        'conditioncode_standard_codingSystemId'],
                                        'label': ' Verified Sickle Cell Codes'},
                'sickleconditionencounter': {'label': 'Condition Encounter '
                                                      'Records with Encounter '
                                                      'Record Information'},
                'statusList': {'label': 'All Status Code Levels'},
                'stemcellID': {'code': 'SCD',
                               'datefieldPrimary': ['dateCondition'],
                               'datefieldStop': 'dateCondition',
                               'histEnd': '2025-01-01',
                               'histStart': '1990-01-01',
                               'indexFields': ['personid', 'tenant'],
                               'label': 'One Record Per Stem Cell Encounter',
                               'max_gap': 30000,
                               'retained_fields': ['datetimeCondition',
                                                   'typedvalue_numericValue_value',
                                                   'typebdvalue_unitOfMeasure_standard_id',
                                                   'labcode_standard_primaryDisplay',
                                                   'group',
                                                   'tenant'],
                               'sort_fields': ['datetimeCondition']},
                'stemcell_codes': {'complete': True,
                                   'dictionary': {'Bone marrow replaced by transplant': 'V42.81',
                                                  'Bone marrow transplant status': 'Z94.81',
                                                  'Peripheral stem cells replaced by transplant': 'V42.82',
                                                  'Stem cells transplant status': 'Z94.84'},
                                   'label': 'Codes indicating Stem Cell '
                                            'Transfusion',
                                   'listIndex': 'codes',
                                   'sourceField': 'conditioncode_standard_id'},
                'stemcellcodesVerified': {'datefield': None,
                                          'groupName': 'regexgroup',
                                          'indexFields': ['conditioncode_standard_primaryDisplay',
                                                          'conditioncode_standard_id',
                                                          'conditioncode_standard_codingSystemId'],
                                          'label': ' Verified Stem Cell Codes'},
                'stemcellresults': {'cohortColumns': ['personid',
                                                      'encounterid',
                                                      'tenant'],
                                    'datefield': 'datetimeCondition',
                                    'groupName': 'stemconditiongroup',
                                    'histEnd': '2025-01-01',
                                    'histStart': '1990-01-01',
                                    'indexFields': ['personid'],
                                    'label': 'Results searching conditions '
                                             'with stem cell codes',
                                    'retained_fields': ['personid',
                                                        'encounterid',
                                                        'tenant',
                                                        'datetimeCondition',
                                                        'dateCondition']},
                'table1features': {'complete': True,
                                   'label': 'Features From Sachdev table 1 ',
                                   'listIndex': 'Regex',
                                   'sourceField': 'labcode_standard_primaryDisplay'},
                'test_feature_control': {'label': 'Feature control file'},
                'transfusion': {'datefield': 'date',
                                'label': 'All Transfusion Record Dates'},
                'visit_detail_concept_id': {'label': 'All visit detail records '
                                                     'with mapped concept ID'},
                'visit_occurrence_concept_id': {'label': 'All Visit Occurances '
                                                         'with mapped concept '
                                                         'ID'},
                'vital_sign_encounterBaseline': {'datefield': 'first_test_date',
                                                 'histEnd': None,
                                                 'histStart': None,
                                                 'indexFields': ['personid',
                                                                 'tenant'],
                                                 'label': 'Vital Sign '
                                                          'Encounters from the '
                                                          'Lab Codes at '
                                                          'baseline'},
                'vital_sign_encounterDemoBaseline': {'datefield': 'dateMeasurement',
                                                     'histEnd': None,
                                                     'histStart': None,
                                                     'indexFields': ['personid',
                                                                     'tenant'],
                                                     'label': 'Vital Sign '
                                                              'Encounters from '
                                                              'the Lab Codes '
                                                              'with '
                                                              'demographic '
                                                              'info added and '
                                                              'subset to '
                                                              'baseline'},
                'vital_sign_encountermeasures': {'label': 'Vital signs with '
                                                          'feature values in a '
                                                          'long format'},
                'vital_sign_encounters': {'datefield': 'dateMeasurement',
                                          'histEnd': '2025-10-01',
                                          'histStart': '1990-01-01',
                                          'indexFields': ['personid', 'tenant'],
                                          'label': 'Vital Sign Measurement '
                                                   'Records',
                                          'retained_fields': ['measurementid',
                                                              'encounterid',
                                                              'personid',
                                                              'measurementcode_standard_id',
                                                              'measurementcode_standard_codingSystemId',
                                                              'loincclass',
                                                              'servicedate',
                                                              'typedvalue_numericValue_value',
                                                              'typedvalue_unitOfMeasure_standard_id',
                                                              'interpretation_standard_primaryDisplay',
                                                              'tenant',
                                                              'dateMeasurement']},
                'vital_sign_encountersDemo': {'datefield': 'dateMeasurement',
                                              'histEnd': None,
                                              'histStart': None,
                                              'indexFields': ['personid',
                                                              'tenant'],
                                              'label': 'Vital Sign Lab '
                                                       'Encounters from the '
                                                       'Lab Codes with '
                                                       'demographic info '
                                                       'added'},
                'vital_signs_codes': {'indexFields': ['measurementcode_standard_id',
                                                      'measurementcode_standard_codingSystemId'],
                                      'label': 'Vital Signs Codes for '
                                               'Measurement Extraction'},
                'zipCodeList': {'label': 'RWD zip_code Levels'},
                'zipcode': {'label': 'Patient Level Zip Code'}},
 'debug': True,
 'disease': 'SCD',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'project': 'SickleCell_AI',
 'schema': 'sicklecell_ai',
 'schemaTag': 'RWD'}
 projectTables at local step
PosixPath('/home/hnelson3/work/Users/hnelson3/Projects/SickleCell_AI/000-config.yaml')
 projectTables at local step
PosixPath('/home/hnelson3/work/Users/hnelson3/configuration/config-global.yaml')
2025-12-22 14:18:58,566 - lhn.item - INFO - Create name: persontenant from location: sicklecell_ai.persontenant_SCD_RWD
2025-12-22 14:18:58,579 - lhn.item - INFO - Create name: persontenantOMOP from location: sicklecell_ai.persontenantOMOP_SCD_RWD
2025-12-22 14:18:58,591 - lhn.item - INFO - Create name: death from location: sicklecell_ai.death_SCD_RWD
2025-12-22 14:18:58,603 - lhn.item - INFO - Create name: maritalstatus from location: sicklecell_ai.maritalstatus_SCD_RWD
2025-12-22 14:18:58,624 - lhn.item - INFO - Create name: followEnc from location: sicklecell_ai.followEnc_SCD_RWD
2025-12-22 14:18:58,637 - lhn.item - INFO - Create name: followCondition from location: sicklecell_ai.followCondition_SCD_RWD
2025-12-22 14:18:58,650 - lhn.item - INFO - Create name: followLab from location: sicklecell_ai.followLab_SCD_RWD
2025-12-22 14:18:58,663 - lhn.item - INFO - Create name: followProc from location: sicklecell_ai.followProc_SCD_RWD
2025-12-22 14:18:58,675 - lhn.item - INFO - Create name: followMed from location: sicklecell_ai.followMed_SCD_RWD
2025-12-22 14:18:58,688 - lhn.item - INFO - Create name: observation_period from location: sicklecell_ai.observation_period_SCD_RWD
2025-12-22 14:18:58,700 - lhn.item - INFO - Create name: follow from location: sicklecell_ai.follow_SCD_RWD
2025-12-22 14:18:58,729 - lhn.item - INFO - Create name: demo from location: sicklecell_ai.demo_SCD_RWD
2025-12-22 14:18:58,744 - lhn.item - INFO - Create name: demoOMOP from location: sicklecell_ai.demoOMOP_SCD_RWD
2025-12-22 14:18:58,758 - lhn.item - INFO - Create name: sicklecodes from location: sicklecell_ai.sicklecodes_SCD_RWD
2025-12-22 14:18:58,772 - lhn.item - INFO - Create name: sicklecodesVerified from location: sicklecell_ai.sicklecodesVerified_SCD_RWD
2025-12-22 14:18:58,784 - lhn.item - INFO - Create name: sickleConditionEncounter from location: sicklecell_ai.sickleConditionEncounter_SCD_RWD
2025-12-22 14:18:58,806 - lhn.item - INFO - Create name: encounterId from location: sicklecell_ai.encounterId_SCD_RWD
2025-12-22 14:18:58,819 - lhn.item - INFO - Create name: encounter from location: sicklecell_ai.encounter_SCD_RWD
2025-12-22 14:18:58,832 - lhn.item - INFO - Create name: encounterExtract from location: sicklecell_ai.encounterExtract_SCD_RWD
2025-12-22 14:18:58,844 - lhn.item - INFO - Create name: vital_signs_codes from location: sicklecell_ai.vital_signs_codes_SCD_RWD
2025-12-22 14:18:58,856 - lhn.item - INFO - Create name: vital_sign_encounters from location: sicklecell_ai.vital_sign_encounters_SCD_RWD
2025-12-22 14:18:58,869 - lhn.item - INFO - Create name: drug_codes from location: sicklecell_ai.drug_codes_SCD_RWD
2025-12-22 14:18:58,881 - lhn.item - INFO - Create name: medication_encounters from location: sicklecell_ai.medication_encounters_SCD_RWD
2025-12-22 14:18:58,903 - lhn.item - INFO - Create name: labsHgbCodes from location: sicklecell_ai.labsHgbCodes_SCD_RWD
2025-12-22 14:18:58,916 - lhn.item - INFO - Create name: labsHgb from location: sicklecell_ai.labsHgb_SCD_RWD
2025-12-22 14:18:58,929 - lhn.item - INFO - Create name: labPersonId from location: sicklecell_ai.labPersonId_SCD_RWD
2025-12-22 14:18:58,942 - lhn.item - INFO - Create name: proctransfusioncodes from location: sicklecell_ai.proctransfusioncodes_SCD_RWD
2025-12-22 14:18:58,954 - lhn.item - INFO - Create name: proctransfusion from location: sicklecell_ai.proctransfusion_SCD_RWD
2025-12-22 14:18:58,967 - lhn.item - INFO - Create name: medHydroxiacodes from location: sicklecell_ai.medHydroxiacodes_SCD_RWD
2025-12-22 14:18:58,979 - lhn.item - INFO - Create name: medsHydroxia from location: sicklecell_ai.medsHydroxia_SCD_RWD
2025-12-22 14:18:58,991 - lhn.item - INFO - Create name: medsHydroxiaIndex from location: sicklecell_ai.medsHydroxiaIndex_SCD_RWD
2025-12-22 14:18:59,004 - lhn.item - INFO - Create name: medsHydroxiaUsage from location: sicklecell_ai.medsHydroxiaUsage_SCD_RWD
2025-12-22 14:18:59,016 - lhn.item - INFO - Create name: stemcell_codes from location: sicklecell_ai.stemcell_codes_SCD_RWD
2025-12-22 14:18:59,027 - lhn.item - INFO - Create name: stemcellcodesVerified from location: sicklecell_ai.stemcellcodesVerified_SCD_RWD
2025-12-22 14:18:59,040 - lhn.item - INFO - Create name: stemcellresults from location: sicklecell_ai.stemcellresults_SCD_RWD
2025-12-22 14:18:59,053 - lhn.item - INFO - Create name: stemcellID from location: sicklecell_ai.stemcellID_SCD_RWD
2025-12-22 14:18:59,065 - lhn.item - INFO - Create name: loinc_codes from location: sicklecell_ai.loinc_codes_SCD_RWD
2025-12-22 14:18:59,077 - lhn.item - INFO - Create name: loinc_codesVerified from location: sicklecell_ai.loinc_codesVerified_SCD_RWD
2025-12-22 14:18:59,089 - lhn.item - INFO - Create name: loincresults from location: sicklecell_ai.loincresults_SCD_RWD
2025-12-22 14:18:59,102 - lhn.item - INFO - Create name: loincresultsminimal from location: sicklecell_ai.loincresultsminimal_SCD_RWD
2025-12-22 14:18:59,123 - lhn.item - INFO - Create name: gap_encounterID from location: sicklecell_ai.gap_encounterID_SCD_RWD
2025-12-22 14:18:59,154 - lhn.item - INFO - Create name: insurance from location: sicklecell_ai.insurance_SCD_RWD
2025-12-22 14:18:59,167 - lhn.item - INFO - Create name: corrections from location: sicklecell_ai.corrections_SCD_RWD
2025-12-22 14:18:59,180 - lhn.item - INFO - Create name: insurancePeriods from location: sicklecell_ai.insurancePeriods_SCD_RWD
2025-12-22 14:18:59,431 - lhn.item - INFO - Create name: sickleconditionencounter from location: sicklecell_ai.sickleconditionencounter_SCD_RWD
2025-12-22 14:18:59,552 - lhn.item - INFO - Create name: comorbidity_ICD9_Codes from location: sicklecell_ai.comorbidity_ICD9_Codes_SCD_RWD
2025-12-22 14:18:59,566 - lhn.item - INFO - Create name: comorbidity_ICD9_Codes_Verified from location: sicklecell_ai.comorbidity_ICD9_Codes_Verified_SCD_RWD
2025-12-22 14:18:59,578 - lhn.item - INFO - Create name: comorbidity_ICD10_Codes from location: sicklecell_ai.comorbidity_ICD10_Codes_SCD_RWD
2025-12-22 14:18:59,590 - lhn.item - INFO - Create name: comorbidity_ICD10_Codes_Verified from location: sicklecell_ai.comorbidity_ICD10_Codes_Verified_SCD_RWD
2025-12-22 14:18:59,602 - lhn.item - INFO - Create name: comorbidity_Codes_Verified from location: sicklecell_ai.comorbidity_Codes_Verified_SCD_RWD
2025-12-22 14:18:59,614 - lhn.item - INFO - Create name: comorbidity_ConditionEncounter from location: sicklecell_ai.comorbidity_ConditionEncounter_SCD_RWD
2025-12-22 14:18:59,626 - lhn.item - INFO - Create name: comorbidity_procedure_Codes from location: sicklecell_ai.comorbidity_procedure_Codes_SCD_RWD
2025-12-22 14:18:59,637 - lhn.item - INFO - Create name: comorbidity_procedure_Codes_Verified from location: sicklecell_ai.comorbidity_procedure_Codes_Verified_SCD_RWD
2025-12-22 14:18:59,650 - lhn.item - INFO - Create name: comorbidity_procedureEncounter from location: sicklecell_ai.comorbidity_procedureEncounter_SCD_RWD
2025-12-22 14:18:59,663 - lhn.item - INFO - Create name: comorbidity_procedureEncounterFeature from location: sicklecell_ai.comorbidity_procedureEncounterFeature_SCD_RWD
2025-12-22 14:18:59,675 - lhn.item - INFO - Create name: Lab_LOINC_Codes_regex from location: sicklecell_ai.Lab_LOINC_Codes_regex_SCD_RWD
2025-12-22 14:18:59,687 - lhn.item - INFO - Create name: Lab_LOINC_Codes_Verified from location: sicklecell_ai.Lab_LOINC_Codes_Verified_SCD_RWD
2025-12-22 14:18:59,698 - lhn.item - INFO - Create name: Lab_LOINCEncounter from location: sicklecell_ai.Lab_LOINCEncounter_SCD_RWD
2025-12-22 14:18:59,711 - lhn.item - INFO - Create name: Lab_LOINCEncounterFeature from location: sicklecell_ai.Lab_LOINCEncounterFeature_SCD_RWD
2025-12-22 14:18:59,732 - lhn.item - INFO - Create name: Lab_LOINCEncounterDemoBaseline from location: sicklecell_ai.Lab_LOINCEncounterDemoBaseline_SCD_RWD
2025-12-22 14:18:59,798 - lhn.item - INFO - Create name: Pregnancy_Outcome_ICD_Codes from location: sicklecell_ai.Pregnancy_Outcome_ICD_Codes_SCD_RWD
2025-12-22 14:18:59,810 - lhn.item - INFO - Create name: Pregnancy_Outcome_ICD_Codes_Verified from location: sicklecell_ai.Pregnancy_Outcome_ICD_Codes_Verified_SCD_RWD
2025-12-22 14:18:59,822 - lhn.item - INFO - Create name: Pregnancy_Outcome_Encounter from location: sicklecell_ai.Pregnancy_Outcome_Encounter_SCD_RWD
2025-12-22 14:18:59,834 - lhn.item - INFO - Create name: Pregnancy_Outcome_EncounterFeature from location: sicklecell_ai.Pregnancy_Outcome_EncounterFeature_SCD_RWD
2025-12-22 14:18:59,846 - lhn.resource - INFO - processDataTables: processed proj to point to schema projectSchema
2025-12-22 14:18:59,847 - lhn.resource - INFO - processDataTables: processed db to point to schema projectSchema
2025-12-22 14:18:59,847 - lhn.resource - INFO - processed property names: {'rwd', 'r', 'proj', 'scd', 'sickle', 'db', 's', 'ss'}
2025-12-22 14:18:59,848 - lhn.resource - INFO - processed projectSchema to produce property db and dictionary proj
2025-12-22 14:18:59,848 - lhn.resource - INFO - Can call h.Extract(resource.proj) to get the Extract object
2025-12-22 14:18:59,849 - lhn.resource - INFO - load_into_local: Loading into local everything = False, schemakey = projectSchema, extractName = e
2025-12-22 14:18:59,849 - lhn.resource - INFO - load_into_local: Found callFunProcessDataTables for projectSchema 
 {'data_type': 'projectTables',
 'parquetLoc': 'hdfs:///user/hnelson3/SickleCell_AI/',
 'property_name': 'db',
 'schema_type': 'projectSchema',
 'tableNameTemplate': '_{disease}_{schemaTag}',
 'type_key': 'proj',
 'updateDict': False}
2025-12-22 14:18:59,849 - lhn.resource - INFO - result[e] = Extract(getattr(self, proj)
The Current Sickle Cell Schema
The production Sickle Cell Schema, All tables in the schema
```


After this runs we can see the results moved to the local environment for convenience:

```
sickle
```

result shows the table list
```
<TableList with tables: adspatient, adspatscd, comorbidity_conditionencounter, comorbidity_icd10_codes, comorbidity_icd10_codes_verified, comorbidity_icd10_conditionencounter, comorbidity_icd9_codes, comorbidity_icd9_codes_verified, comorbidity_icd9_conditionencounter, comorbidity_procedure_codes, comorbidity_procedure_codes_verified, comorbidity_procedureencounter, comorbidity_procedureencounterfeature, corrections, death, demo, demoomop, drug_codes, encounter, encounterextract, encounterid, evalphenorow, follow, followcondition, followenc, followlab, followmed_rwd_, followproc, gap_conditionid, gap_condtionid, gap_encounterid, hgb_subunits, hydroxyurea, insurance, insurance2, insuranceperiods, lab_features, lab_features_codes, lab_loinc_codes, lab_loinc_codes_regex, lab_loinc_codes_verified, lab_loincencounter, lab_loincencounterbaseline, lab_loincencounterdemo, lab_loincencounterdemobaseline, lab_loincencounterfeature, lablist, labpersonid, labshgb, labshgbcodes, labshydroxyuria, loinc_codes, loinc_codesverified, loincresults, loincresultsminimal, maritalstatus, medhydroxiacodes, medication_encounterdemobaseline, medication_encountermeasures, medication_encounters, medshydroxia, medshydroxiaindex, medshydroxiausage, mrnlist, persontenant, persontenantomop, phenomatrix, pregnancy_outcome_encounter, pregnancy_outcome_icd_codes, pregnancy_outcome_icd_codes_verified, proctransfusion, proctransfusioncodes, sicklecodes, sicklecodesexpanded, sicklecodesverified, sicklecodesverifiedexpanded, sickleconditionencounter, sickleconditionencounterexpanded, stemcell_codes, stemcellcodesverified, stemcellid, stemcellresults, test_feature_control, transfusion, visit_detail_concept_id, visit_occurrence_concept_id, vital_sign_encounterbaseline, vital_sign_encounterdemobaseline, vital_sign_encountermeasures, vital_sign_encounters, vital_sign_encountersdemo, vital_signs_codes>
```

```
# scd is the object containing the production SCD tables as properties
scd.adspatient
```

result
```
DataFrame[location_id: bigint, personid: string, PersonPhenotype: string, IcdPheno: string, SCD: boolean, casePossible: boolean, tenant: int, gender: string, race: string, ethnicity: string, yearofbirth: string, state: string, metropolitan: string, urban: string, deceased: boolean, dateofdeath: string, encDateFirst: date, encDateLast: date, encounters: bigint, procEncDateFirst: date, procEncDateLast: date, procEncounters: bigint, medEncDateFirst: date, medEncDateLast: date, medEncounters: bigint, followdate: date, FirstTouchDate: date, followtime: int, ageAtFirstTouch: double, ageAtLastTouch: double, age: double, group: string, person_id: bigint, year_of_birth: date, care_site_id: bigint, person_source_value: string, gender_source_value: string, race_source_value: string, ethnicity_source_value: string, omob_state: string]
```

We can create an extract objects with a tableList object and the Extract method.  (This was also done in the Resource method)

```
# an Extract object with the projection SCD tables
escd = h.Extract(sickle)
```

```
escd.adspatient.location
```

result
```
'sicklecell_rerun.adspatient_scd_rwd'
```