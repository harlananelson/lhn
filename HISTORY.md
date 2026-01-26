# LHN Version History & Git Reference

## Repository Overview

This GitHub repository contains both versions on separate branches:

| Branch | Purpose | Status |
|--------|---------|--------|
| `main` | Development/refactored code | **In Development** |
| `v0.1.0-monolithic` | Production code (monolithic) | Active Production |

> **Note**: The production code is also maintained in a separate Azure DevOps repo (`lhn-original`).

---

## Important Tags

| Tag | Description | Command to Checkout |
|-----|-------------|---------------------|
| `v0.2.0-dev` | Development baseline (Jan 2026) | `git checkout v0.2.0-dev` |

---

## Important Branches

### This Repository (GitHub)

| Branch | Description |
|--------|-------------|
| `main` | Development branch (refactored code) |
| `v0.1.0-monolithic` | Production branch (original monolithic code) |

### Switch Between Versions

```bash
# Switch to production (monolithic)
git checkout v0.1.0-monolithic

# Switch to development (refactored)
git checkout main
```

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

## Running Both Versions in Jupyter (Same System)

To use production and development versions simultaneously in different notebooks, create separate virtual environments and register them as Jupyter kernels.

### Step 1: Set Up Production Environment

```bash
# Navigate to production repo
cd /path/to/lhn-original

# Create virtual environment
python -m venv .venv-prod

# Activate it
source .venv-prod/bin/activate   # Linux/Mac
# or: .venv-prod\Scripts\activate  # Windows

# Install the package in editable mode
pip install -e .

# Install Jupyter kernel support
pip install ipykernel

# Register as a Jupyter kernel
python -m ipykernel install --user --name lhn-prod --display-name "Python (lhn-prod v0.1.0)"

# Deactivate when done
deactivate
```

### Step 2: Set Up Development Environment

```bash
# Navigate to development repo
cd /path/to/lhn

# Create virtual environment
python -m venv .venv-dev

# Activate it
source .venv-dev/bin/activate

# Install dependencies first (if spark_config_mapper exists)
pip install -e ../spark_config_mapper

# Install the dev package
pip install -e .

# Install Jupyter kernel support
pip install ipykernel

# Register as a Jupyter kernel
python -m ipykernel install --user --name lhn-dev --display-name "Python (lhn-dev v0.2.0)"

# Deactivate when done
deactivate
```

### Step 3: Use in Jupyter

In Jupyter, select the kernel from the menu:
- **Kernel > Change Kernel > Python (lhn-prod v0.1.0)** for production
- **Kernel > Change Kernel > Python (lhn-dev v0.2.0)** for development

Each notebook can use a different kernel independently.

### Managing Kernels

```bash
# List installed kernels
jupyter kernelspec list

# Remove a kernel
jupyter kernelspec remove lhn-dev
```

### Alternative: sys.path Manipulation (Quick but Fragile)

If you can't create virtual environments, you can manipulate the path at notebook startup:

```python
# At the TOP of your notebook (before any imports)
import sys
# For development:
sys.path.insert(0, '/path/to/lhn')
# OR for production:
# sys.path.insert(0, '/path/to/lhn-original')

# Now import
import lhn
```

**Warning**: This approach is fragile and can cause issues if dependencies differ between versions.

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
