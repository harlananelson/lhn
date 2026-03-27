"""
lhn/ontology

Ontology operations for HealtheDataLab — querying Cerner Discern ontologies,
crosswalking coding systems, and tabulating concept coverage in data tables.

Restored from v0.1.0-monolithic. These functions query the
`standard_ontologies.ontologies` database on HDL and the `tabulated_ontologies`
schema created by summarizeCodes().

Integration points:
- umls_codeset_builder: generates CSV code lists from UMLS REST API
- cerner/bunsen: FHIR ValueSets and LOINC/SNOMED hierarchies
- omop_concept_mapper: NLP processing of doctor notes to OMOP concepts
- ontograph: typed DAG for concept relationships
"""

from lhn.ontology.discern import (
    # Ontology search
    search_ontologies,
    contextId_ont,
    context_ont,
    conceptCode_ont,
    system_ont,
    system_name_ont,
    concept_ont,
    context_name_ont,
    context_name_system_ont,
    codingSystem_ont,

    # Code tabulation
    getCodingSystemId,
    getCodesAndSystem,
    summarizeCodes,

    # Crosswalk
    findCrosswalk,
    check_sample_and_ontology,

    # Concept extraction
    extractConcepts,
    calContextGroups,
    get_ontology_codes,
    select_top_contextId,

    # List utilities
    conceptName_list,
    contextId_list,
    context_list,
    coding_list,
    codes_list,

    # Table management
    updateTable,
    updateTableDt,
    createMetaOnt,

    # Discern UDF
    add_concept_indicators,
    show_desc,

    # Other
    demoTable,
)

__all__ = [
    'search_ontologies',
    'contextId_ont',
    'context_ont',
    'conceptCode_ont',
    'system_ont',
    'system_name_ont',
    'concept_ont',
    'context_name_ont',
    'context_name_system_ont',
    'codingSystem_ont',
    'getCodingSystemId',
    'getCodesAndSystem',
    'summarizeCodes',
    'findCrosswalk',
    'check_sample_and_ontology',
    'extractConcepts',
    'calContextGroups',
    'get_ontology_codes',
    'select_top_contextId',
    'conceptName_list',
    'contextId_list',
    'context_list',
    'coding_list',
    'codes_list',
    'updateTable',
    'updateTableDt',
    'createMetaOnt',
    'add_concept_indicators',
    'show_desc',
    'demoTable',
]
