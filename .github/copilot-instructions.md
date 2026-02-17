# BUSCO-tracker - Copilot Instructions

## Project Overview

BUSCO-tracker is a GitHub Actions-based automation that performs BUSCO (Benchmarking Universal Single-Copy Orthologs) analysis for eukaryotic genome annotations. It generates TSV output files consumed by downstream applications.

**Core Function**: Automate BUSCO analysis on genome annotation files and output structured results in TSV format for external consumption.

## Architecture

### GitHub Actions Workflow
- Main workflow triggers on genome annotation file updates or manual dispatch
- Python scripts orchestrate BUSCO execution and result parsing
- TSV output is generated as a workflow artifact and/or committed to repository
- Workflow should be idempotent and handle incremental updates

### Data Flow
1. Input: Genome annotation files (FASTA, GFF, etc.)
2. Process: BUSCO analysis execution via Python wrapper
3. Output: TSV file with standardized BUSCO metrics (completeness, fragmentation, duplication)

### Key Components
- `.github/workflows/`: GitHub Actions workflow definitions
- Python scripts: BUSCO execution wrappers, result parsers, TSV generators
- Configuration: BUSCO lineage datasets, thresholds, output formats

## Development Commands

### Python Environment
```bash
# Setup (when environment management is added)
python -m venv venv
source venv/bin/activate  # or `venv\Scripts\activate` on Windows
pip install -r requirements.txt

# Testing (when tests are added)
pytest tests/
pytest tests/test_specific.py  # single test file
pytest tests/test_specific.py::test_function  # single test

# Linting (when configured)
black .  # format code
flake8 .  # check style
mypy .  # type checking
```

### GitHub Actions Testing
```bash
# Validate workflow syntax
act --list  # requires nektos/act for local workflow testing

# Test workflow locally (if act is installed)
act -j busco-analysis
```

## Key Conventions

### BUSCO-Specific Patterns
- **Lineage Datasets**: Store lineage dataset names in configuration (e.g., `eukaryota_odb10`, `metazoa_odb10`)
- **Output Parsing**: BUSCO outputs are in `short_summary.*.txt` format - parse these reliably
- **Error Handling**: BUSCO failures should be caught gracefully; partial results should be logged but not fail the entire workflow

### TSV Output Format
- **Required Columns**: Maintain consistent column order and naming for downstream consumers
- **Completeness Metrics**: Include C (complete), S (single-copy), D (duplicated), F (fragmented), M (missing) percentages
- **Metadata**: Include timestamp, BUSCO version, lineage dataset, input file hash/name

### GitHub Actions Conventions
- **Artifacts**: Use `actions/upload-artifact@v3` or later for TSV files
- **Caching**: Cache BUSCO lineage datasets to speed up workflow runs
- **Secrets**: Never hardcode paths or credentials; use repository secrets and environment variables
- **Workflow Naming**: Use descriptive job and step names for easy debugging in Actions UI

### Python Code Patterns
- **Subprocess Management**: Use `subprocess.run()` with proper error handling for BUSCO CLI invocation
- **Path Handling**: Use `pathlib.Path` for cross-platform compatibility
- **Configuration**: Load parameters from environment variables (via GitHub Actions) or config files
- **Logging**: Use `logging` module with appropriate levels (INFO for progress, ERROR for failures)

## Important Notes

### BUSCO Requirements
- BUSCO v5+ requires specific dependencies (augustus, hmmer, etc.) - document these in Docker/container setup
- Lineage datasets are large (several GB) - implement caching strategy in workflow
- BUSCO runs can be time-consuming - consider workflow timeout settings

### Workflow Optimization
- Run BUSCO in offline mode when possible (pre-download datasets)
- Parallelize multiple genome analyses if batch processing is needed
- Consider using matrix strategy for testing multiple lineages

### Output Reliability
- Validate TSV structure before upload/commit
- Include schema version in TSV for backward compatibility
- Log raw BUSCO output alongside parsed TSV for debugging
