"""
lhn/cohort

Cohort identification and matching operations for healthcare data.
"""

from lhn.cohort.identification import (
    write_index_table,
    identify_target_records,
    calcUsage,
    identifyLevel
)

from lhn.cohort.demographics import (
    group_ethnicities,
    group_races,
    group_races2,
    group_gender,
    group_marital_status,
    assign_age_group
)

__all__ = [
    'write_index_table',
    'identify_target_records',
    'calcUsage',
    'identifyLevel',
    'group_ethnicities',
    'group_races',
    'group_races2',
    'group_gender',
    'group_marital_status',
    'assign_age_group'
]
