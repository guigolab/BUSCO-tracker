# GitHub Actions Workflows

This directory contains GitHub Actions workflows for automating BUSCO analysis.

## Workflows

### 1. `fetch-annotations.yml`
Fetches annotation data from the AnnoTrEive API.

**Trigger:** Manual (workflow_dispatch)

**What it does:**
- Fetches all annotations from the AnnoTrEive API
- Generates `annotations.tsv` with annotation and assembly URLs
- Commits the file to the repository

### 2. `run-busco-analysis.yml`
Runs BUSCO analysis on a limited number of annotations.

**Trigger:** Manual (workflow_dispatch) with configurable limit

**What it does:**
- Sets up Python environment and Conda
- Installs annocli via pip (from GitHub)
- Installs AGAT and BUSCO via Conda
- Downloads eukaryota_odb12 lineage dataset
- Processes first N annotations (default: 3)
- Commits results to `BUSCO.tsv` and `fails.tsv`

**Input Parameters:**
- `limit` (optional): Number of annotations to process (default: 3)

## Running the BUSCO Analysis Workflow

### Via GitHub UI

1. Go to your repository on GitHub
2. Click on the "Actions" tab
3. Select "Run BUSCO Analysis" from the workflow list
4. Click "Run workflow"
5. Optionally, specify the number of annotations to process
6. Click the green "Run workflow" button

### Expected Runtime

For 3 annotations:
- Setup (downloading Docker images and datasets): ~5-10 minutes
- Analysis per annotation: ~10-30 minutes each
- **Total**: ~40-100 minutes for 3 annotations

## Docker Images Used

### AGAT (v1.4.0)
- **Image:** `quay.io/biocontainers/agat:1.4.0--pl5321hdfd78af_0`
- **Purpose:** GFF manipulation and sequence extraction
- **Tools used:**
  - `agat_sp_keep_longest_isoform.pl`
  - `agat_sp_extract_sequences.pl`

### BUSCO (v5.7.1)
- **Image:** `ezlabgva/busco:v5.7.1_cv1`
- **Purpose:** Genome completeness assessment
- **Lineage:** eukaryota_odb12

## How It Works

### Environment Setup

1. **Python Tools:**
   - Installed directly with pip: `annocli`

2. **Docker-wrapped Tools:**
   - AGAT and BUSCO run in Docker containers
   - Wrapper scripts created in `bin/` directory
   - Wrappers handle volume mounting and Docker execution
   - Added to PATH for seamless execution

### Processing Flow

1. Checkout repository and set up Python
2. Install annocli with pip
3. Pull Docker images for AGAT and BUSCO
4. Download BUSCO lineage dataset
5. Create wrapper scripts for Docker tools
6. Prepare limited annotations file (first N entries)
7. Run BUSCO analysis pipeline
8. Upload results as artifacts
9. Commit results back to repository

### Results

**Success:**
- Results appended to `BUSCO.tsv`
- Contains: annotation_id, lineage, busco_count, complete, single, duplicated, fragmented, missing

**Failure:**
- Failures logged to `fails.tsv`
- Contains: annotation_id, failed_step, error_message

**Artifacts:**
- `busco-results-<run_number>`: Contains BUSCO.tsv, fails.tsv, and annotations_limited.tsv

## Troubleshooting

### Conda Installation Errors
If conda packages fail to install:
- Check bioconda channel availability
- Verify package versions are available
- Try without version pinning (remove `=6.0.0`)

**Solution:** Re-run the workflow

### BUSCO Dataset Download Fails
If the lineage dataset download fails:
- Check BUSCO dataset availability at https://busco-data.ezlab.org
- Verify network connectivity

### Pipeline Errors
Check the workflow logs for specific step failures:
- Download errors: Check URL accessibility
- Tool errors: Review error messages in logs (use `shell: bash -el {0}` for conda commands)
- Check `fails.tsv` for detailed error information

### Conda Environment Issues
If tools aren't found after installation:
- Ensure steps use `shell: bash -el {0}` to activate conda environment
- Verify conda channels are properly configured

## Customization

### Change Number of Annotations
When triggering the workflow, modify the `limit` input parameter.

### Change BUSCO Lineage
Edit the workflow file and change `eukaryota_odb12` to your desired lineage (e.g., `metazoa_odb10`, `vertebrata_odb10`).

### Add More CPUs
Edit the BUSCO command in `busco_pipeline.py` and change the `-c` parameter.

### Use Different Tool Versions
Update the conda install commands in the workflow:
```yaml
conda install -y -c bioconda busco=6.1.0  # Different BUSCO version
conda install -y -c bioconda agat=1.2.0   # Specific AGAT version
```

## Notes

- The workflow uses Conda for reproducible bioinformatics tool installation
- All steps that use conda tools require `shell: bash -el {0}` to activate the environment
- BUSCO 6.0.0 is used for the latest features and improvements
- The workflow uses `continue-on-error: true` for the analysis step to ensure results are committed even if some annotations fail
- Commits are marked with `[skip ci]` to prevent recursive workflow triggers
- Temporary files are cleaned up automatically after each annotation
- All results are also uploaded as workflow artifacts for easy download
