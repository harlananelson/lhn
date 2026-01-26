# LHN Version History & Git Reference

## Repository Overview

This project is split across two repositories during refactoring:

| Repository | Purpose | Remote | Status |
|------------|---------|--------|--------|
| `lhn-original` | Production code (monolithic) | Azure DevOps | Active Production |
| `lhn` | Development/refactored code | GitHub | **In Development** |

---

## Important Tags

| Tag | Description | Command to Checkout |
|-----|-------------|---------------------|
| `v0.2.0-dev` | Development baseline (Jan 2026) | `git checkout v0.2.0-dev` |

---

## Important Branches

### lhn (this repo)

| Branch | Description |
|--------|-------------|
| `main` | Development branch (refactored code) |

### lhn-original (production repo)

| Branch | Description |
|--------|-------------|
| `main` | Production branch |
| `write_index` | Feature branch for write_index_table improvements |

---

## Common Git Commands

### View Available Tags
```bash
git tag -l
```

### Checkout Development Baseline
```bash
git checkout v0.2.0-dev
```

### Return to Latest Main
```bash
git checkout main
```

### View Tag Details
```bash
git show v0.2.0-dev
```

### Compare Current with Tag
```bash
git diff v0.2.0-dev
```

### View Commit History
```bash
git log --oneline -20
```

### Switch to Production Repository
```bash
cd ../lhn-original
```

---

## Refactoring Plan

The monolithic `lhn` package is being split into:

1. **lhn** - Healthcare-specific functionality (cohort building, clinical analysis)
2. **spark_config_mapper** - Generic Spark/YAML configuration utilities
3. **omop_concept_mapper** - OMOP concept mapping utilities

---

## Version History

### v0.1.0 (January 2026) - Production
- Located in `../lhn-original/` directory
- Monolithic package with all functionality
- Tagged for easy rollback if needed

### v0.2.0.dev0 (In Development)
- Development baseline for refactoring
- Refactored architecture with separated concerns
- Depends on `spark_config_mapper`
