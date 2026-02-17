#!/usr/bin/env python3
"""
Fetch annotation data from AnnoTrEive API and generate TSV output.
"""
import requests
import sys
import csv
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ANNOTATIONS_API = "https://genome.crg.es/annotrieve/api/v0/annotations"
ASSEMBLIES_API_BASE = "https://genome.crg.es/annotrieve/api/v0/assemblies"


def fetch_json(url):
    """Fetch JSON data from URL with error handling."""
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {url}: {e}")
        return None


def fetch_assembly_url(assembly_accession, cache={}):
    """Fetch assembly URL for given accession with caching."""
    if not assembly_accession:
        return ""
    
    # Check cache first
    if assembly_accession in cache:
        return cache[assembly_accession]
    
    url = f"{ASSEMBLIES_API_BASE}/{assembly_accession}"
    data = fetch_json(url)
    
    result = ""
    if data and 'download_url' in data:
        result = data['download_url']
    else:
        logger.warning(f"Could not fetch assembly URL for {assembly_accession}")
    
    # Cache the result (even if empty)
    cache[assembly_accession] = result
    return result


def main():
    """Main function to fetch annotations and generate TSV."""
    logger.info("Fetching annotations from API...")
    
    all_annotations = []
    offset = 0
    limit = 100
    
    # Fetch all pages
    while True:
        url = f"{ANNOTATIONS_API}?offset={offset}&limit={limit}"
        logger.info(f"Fetching page with offset={offset}, limit={limit}")
        data = fetch_json(url)
        
        if not data:
            logger.error("Failed to fetch annotations data")
            sys.exit(1)
        
        results = data.get('results', [])
        if not results:
            break
        
        all_annotations.extend(results)
        total = data.get('total', 0)
        
        logger.info(f"Fetched {len(results)} annotations (total so far: {len(all_annotations)}/{total})")
        
        # Check if we've fetched all annotations
        if len(all_annotations) >= total:
            break
        
        offset += limit
    
    if not all_annotations:
        logger.error("No annotations found in API response")
        sys.exit(1)
    
    logger.info(f"Found total of {len(all_annotations)} annotations")
    
    # Generate TSV
    output_file = "annotations.tsv"
    with open(output_file, 'w', newline='') as tsvfile:
        writer = csv.writer(tsvfile, delimiter='\t')
        
        # Write header
        writer.writerow(['annotation_id', 'annotation_url', 'assembly_url'])
        
        # Process each annotation
        for idx, annotation in enumerate(all_annotations, 1):
            annotation_id = annotation.get('annotation_id', '')
            
            # Get annotation URL from source_file_info
            annotation_url = ''
            if 'source_file_info' in annotation and 'url_path' in annotation['source_file_info']:
                annotation_url = annotation['source_file_info']['url_path']
            
            # Get assembly accession and fetch assembly URL
            assembly_accession = annotation.get('assembly_accession', '')
            assembly_url = fetch_assembly_url(assembly_accession) if assembly_accession else ''
            
            # Write row
            writer.writerow([annotation_id, annotation_url, assembly_url])
            
            if idx % 100 == 0:
                logger.info(f"Processed {idx}/{len(all_annotations)} annotations...")
    
    logger.info(f"Successfully generated {output_file} with {len(all_annotations)} annotations")


if __name__ == "__main__":
    main()
