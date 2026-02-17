#!/usr/bin/env python3
"""
Main orchestrator for BUSCO analysis.

This script:
1. Reads annotations.tsv to get the list of annotations to process
2. Reads BUSCO.tsv to check which annotations have been processed
3. For each unprocessed annotation, runs busco_pipeline.py
4. Updates BUSCO.tsv on success or fails.tsv on failure
"""
import csv
import sys
import logging
from pathlib import Path
from busco_pipeline import run_pipeline, PipelineError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ANNOTATIONS_FILE = "annotations.tsv"
BUSCO_FILE = "BUSCO.tsv"
FAILS_FILE = "fails.tsv"


def read_annotations(file_path):
    """Read annotations.tsv and return list of annotation records."""
    annotations = []
    logger.info(f"Reading annotations from {file_path}")
    
    with open(file_path, 'r', newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            annotations.append(row)
    
    logger.info(f"Found {len(annotations)} annotations")
    return annotations


def read_processed_ids(file_path):
    """Read BUSCO.tsv and return set of processed annotation IDs."""
    processed = set()
    
    if not Path(file_path).exists():
        logger.info(f"{file_path} does not exist, creating new file")
        # Create file with header
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(['annotation_id', 'lineage', 'busco_count', 'complete', 'single', 'duplicated', 'fragmented', 'missing'])
        return processed
    
    logger.info(f"Reading processed annotations from {file_path}")
    
    with open(file_path, 'r', newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            processed.add(row['annotation_id'])
    
    logger.info(f"Found {len(processed)} already processed annotations")
    return processed


def read_failed_ids(file_path):
    """Read fails.tsv and return set of failed annotation IDs."""
    failed = set()
    
    if not Path(file_path).exists():
        logger.info(f"{file_path} does not exist, creating new file")
        # Create file with header
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerow(['annotation_id', 'failed_step', 'error_message'])
        return failed
    
    logger.info(f"Reading failed annotations from {file_path}")
    
    with open(file_path, 'r', newline='') as f:
        reader = csv.DictReader(f, delimiter='\t')
        for row in reader:
            failed.add(row['annotation_id'])
    
    logger.info(f"Found {len(failed)} previously failed annotations")
    return failed


def append_to_busco_results(file_path, annotation_id, results):
    """Append successful BUSCO results to BUSCO.tsv."""
    logger.info(f"Writing results for {annotation_id} to {file_path}")
    
    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow([
            annotation_id,
            results['lineage'],
            results['busco_count'],
            results['complete'],
            results['single'],
            results['duplicated'],
            results['fragmented'],
            results['missing']
        ])


def append_to_fails(file_path, annotation_id, failed_step, error_message):
    """Append failure information to fails.tsv."""
    logger.info(f"Recording failure for {annotation_id} at step: {failed_step}")
    
    # Truncate error message if too long
    if len(error_message) > 500:
        error_message = error_message[:500] + "..."
    
    with open(file_path, 'a', newline='') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow([annotation_id, failed_step, error_message])


def process_annotation(annotation):
    """
    Process a single annotation through the BUSCO pipeline.
    
    Args:
        annotation: Dict with annotation_id, annotation_url, assembly_url
        
    Returns:
        tuple: (success: bool, results_or_error: dict or tuple)
    """
    annotation_id = annotation['annotation_id']
    annotation_url = annotation['annotation_url']
    assembly_url = annotation['assembly_url']
    
    # Validate URLs
    if not annotation_url or not assembly_url:
        return False, ('validation', 'Missing annotation_url or assembly_url')
    
    logger.info(f"Processing annotation: {annotation_id}")
    
    try:
        results = run_pipeline(annotation_url, assembly_url, annotation_id)
        return True, results
    except PipelineError as e:
        return False, (e.step, e.message)
    except Exception as e:
        return False, ('unknown', str(e))


def main():
    """Main orchestration function."""
    logger.info("Starting BUSCO analysis orchestration")
    
    # Read all files
    annotations = read_annotations(ANNOTATIONS_FILE)
    processed_ids = read_processed_ids(BUSCO_FILE)
    failed_ids = read_failed_ids(FAILS_FILE)
    
    # Filter to unprocessed annotations (not in BUSCO.tsv and not in fails.tsv)
    unprocessed = [
        ann for ann in annotations 
        if ann['annotation_id'] not in processed_ids 
        and ann['annotation_id'] not in failed_ids
    ]
    
    logger.info(f"Found {len(unprocessed)} annotations to process")
    
    if not unprocessed:
        logger.info("No annotations to process. Exiting.")
        return 0
    
    # Process each annotation
    success_count = 0
    failure_count = 0
    
    for idx, annotation in enumerate(unprocessed, 1):
        annotation_id = annotation['annotation_id']
        logger.info(f"\n{'='*80}")
        logger.info(f"Processing {idx}/{len(unprocessed)}: {annotation_id}")
        logger.info(f"{'='*80}")
        
        success, result = process_annotation(annotation)
        
        if success:
            append_to_busco_results(BUSCO_FILE, annotation_id, result)
            success_count += 1
            logger.info(f"✓ Successfully processed {annotation_id}")
        else:
            failed_step, error_message = result
            append_to_fails(FAILS_FILE, annotation_id, failed_step, error_message)
            failure_count += 1
            logger.error(f"✗ Failed to process {annotation_id} at step: {failed_step}")
    
    # Summary
    logger.info(f"\n{'='*80}")
    logger.info("SUMMARY")
    logger.info(f"{'='*80}")
    logger.info(f"Total processed: {len(unprocessed)}")
    logger.info(f"Successful: {success_count}")
    logger.info(f"Failed: {failure_count}")
    logger.info(f"{'='*80}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
