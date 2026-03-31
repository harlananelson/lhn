# LHN — HealthEIntent Data Extraction Package

## Repo Visibility: PRIVATE

Healthcare data extraction and analysis workflows for HealthEIntent (Oracle Health) systems. Python package using PySpark for clinical data processing.

## PHI Tripwire

If you see data containing apparent PHI (names, MRN, SSN, DOB, addresses, small cell counts N<10, clinical notes), **STOP** and ask before proceeding. Do not process, quote, or summarize it.

## Overview

The `lhn` package provides:
- Patient cohort identification and extraction
- Clinical feature engineering from raw EHR data
- Temporal analysis (index events, outcome tracking)
- Ontology integration with clinical coding systems (Discern)

**Status:** Development (v0.2.0.dev0) — refactored version. Production code on branch `v0.1.0-monolithic`.

## Data Governance

- This package runs on HealtheDataLab (HDL), not locally
- Patient-level data stays on HDL — only aggregate results leave
- Code is developed locally, transferred to HDL via git or txtarchive
- Never hardcode connection strings, passwords, or patient identifiers
- Test with synthetic data or small aggregated samples

## Conventions

- Python with PySpark for data processing
- `pyproject.toml` for package configuration
- Depends on `spark-config-mapper` (sibling package)
