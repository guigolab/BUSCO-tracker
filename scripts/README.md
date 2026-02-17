# BUSCO Analysis Scripts

This directory contains scripts for running BUSCO analysis on genome annotations.

## Scripts

### 1. `run_busco_analysis.py`
Main orchestration script that processes annotations through the BUSCO pipeline.

**What it does:**
- Reads `annotations.tsv` to get list of annotations
- Checks `BUSCO.tsv` and `fails.tsv` to identify unprocessed annotations
- For each unprocessed annotation, runs the BUSCO pipeline
- Writes successful results to `BUSCO.tsv`
- Records failures in `fails.tsv`

**Usage:**
```bash
python scripts/run_busco_analysis.py
```

### 2. `busco_pipeline.py`
Individual BUSCO analysis pipeline for a single annotation.

**Pipeline steps:**
1. Download annotation and assembly files
2. Fix annotation aliases with `annocli`
3. Unzip files
4. Extract longest isoform with AGAT (`agat_sp_keep_longest_isoform.pl`)
5. Extract protein sequences with AGAT (`agat_sp_extract_sequences.pl`)
6. Run BUSCO analysis
7. Parse results and return metrics

**Usage (standalone):**
```bash
python scripts/busco_pipeline.py <annotation_url> <assembly_url> <annotation_id>
```

**Example:**
```bash
python scripts/busco_pipeline.py \
  "https://example.com/annotation.gff.gz" \
  "https://example.com/assembly.fna.gz" \
  "annotation_123"
```

## Prerequisites

The following tools must be installed and available in your PATH:

1. **annocli** - Annotation alias fixer
   - Install: https://github.com/apollo994/annocli

2. **AGAT** - Genome annotation toolkit
   - Install: https://github.com/NBISweden/AGAT
   - Required scripts:
     - `agat_sp_keep_longest_isoform.pl`
     - `agat_sp_extract_sequences.pl`

3. **BUSCO** - Benchmarking Universal Single-Copy Orthologs
   - Install: https://busco.ezlab.org/
   - Download lineage dataset: `eukaryota_odb12`

4. **Python 3.7+** with standard library

## Input Files

### annotations.tsv
Tab-separated file with columns:
- `annotation_id`: Unique identifier for the annotation
- `annotation_url`: URL to download annotation file (GFF format, gzipped)
- `assembly_url`: URL to download assembly file (FASTA format, gzipped)

### BUSCO.tsv (created automatically)
Tab-separated file tracking successful analyses with columns:
- `annotation_id`
- `lineage`
- `busco_count`
- `complete`
- `single`
- `duplicated`
- `fragmented`
- `missing`

### fails.tsv (created automatically)
Tab-separated file tracking failed analyses with columns:
- `annotation_id`
- `failed_step`
- `error_message`

## Output

### Success
Results are appended to `BUSCO.tsv` with BUSCO metrics:
- **lineage**: BUSCO lineage dataset used
- **busco_count**: Total number of BUSCO groups searched
- **complete**: Percentage of complete BUSCOs
- **single**: Percentage of single-copy BUSCOs
- **duplicated**: Percentage of duplicated BUSCOs
- **fragmented**: Percentage of fragmented BUSCOs
- **missing**: Percentage of missing BUSCOs

### Failure
Failure information is appended to `fails.tsv` with:
- **annotation_id**: ID of the failed annotation
- **failed_step**: Which pipeline step failed (e.g., "download", "annocli_alias", "extract_longest_isoform", etc.)
- **error_message**: Detailed error message

## Error Handling

The pipeline tracks which step failed:
- `download`: File download failed
- `unzip`: Decompression failed
- `annocli_alias`: Alias fixing failed
- `extract_longest_isoform`: AGAT longest isoform extraction failed
- `extract_protein_sequences`: AGAT protein extraction failed
- `busco_analysis`: BUSCO execution failed
- `parse_results`: BUSCO results parsing failed
- `validation`: Input validation failed (missing URLs)
- `unknown`: Unexpected error

## Running Analysis

To process all unprocessed annotations:

```bash
cd /path/to/BUSCO-tracker
python scripts/run_busco_analysis.py
```

The script will:
1. Skip annotations already in `BUSCO.tsv` (completed)
2. Skip annotations already in `fails.tsv` (previously failed)
3. Process remaining annotations
4. Update `BUSCO.tsv` or `fails.tsv` accordingly

## Workflow Integration

These scripts can be integrated into GitHub Actions or run manually. For automated processing:

```yaml
- name: Run BUSCO Analysis
  run: python scripts/run_busco_analysis.py
```

## Troubleshooting

### Command not found errors
Make sure all required tools are installed and in your PATH:
```bash
which annocli
which agat_sp_keep_longest_isoform.pl
which busco
```

### BUSCO lineage dataset not found
Download the eukaryota_odb12 lineage dataset:
```bash
busco --download eukaryota_odb12
```

### Permission errors
Make scripts executable:
```bash
chmod +x scripts/*.py
```

## Logs

All operations are logged with timestamps and severity levels (INFO, WARNING, ERROR).
