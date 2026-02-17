# BUSCO-tracker

Automated BUSCO analysis for eukaryotic genome annotations

## Automated Annotation Fetching

This repository includes a GitHub Actions workflow that automatically fetches annotation data from the [AnnoTrEive API](https://genome.crg.es/annotrieve/) every 24 hours.

### What it does

The workflow:
1. Fetches all annotations from `https://genome.crg.es/annotrieve/api/v0/annotations`
2. For each annotation, retrieves the associated assembly URL
3. Generates a TSV file (`annotations.tsv`) with three columns:
   - `annotation_id`: Unique identifier for the annotation
   - `annotation_url`: URL to the annotation file
   - `assembly_url`: URL to the corresponding genome assembly file

### Workflow Schedule

- **Automatic**: Runs daily at midnight UTC
- **Manual**: Can be triggered manually via GitHub Actions UI

### Generated Files

- `annotations.tsv`: Tab-separated values file with all annotation data
- Available as both a workflow artifact and committed to the repository

### Local Testing

To test the script locally:

```bash
pip install requests
python scripts/fetch_annotations.py
```

This will generate `annotations.tsv` in the repository root.

