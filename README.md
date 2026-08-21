# BUSCO-tracker

Automated BUSCO analysis for eukaryotic genome annotations. 
See all results [here](BUSCO/eukaryota_odb12/BUSCO.tsv).

## Status

**Last updated:** 2026-08-21T12:07:35Z

| Metric | Count | Percentage |
|--------|-------|------------|
| **Total annotations** | 19186 | 100% |
| **Has BUSCO values** | 16173 | 84.3% |
| **Pending/Retry** | 44 | 0.2% |
| **Given up** | 2969 | 15.5% |

![BUSCO eukaryota results](assets/figures/BUSCO_euk_1k.png)
*Busco values for 1k randomly sampled annotations. Completness is based on eukaryotic BUSCO genes (129). Quality value thresholds are arbitrary*


Find all annotations on [annotrieve](https://genome.crg.es/annotrieve/) and and interact with them using [annocli](https://github.com/apollo994/annocli) 


```
annocli summary --annotation-ids 10cc9bb02de0e989a4710ed0a162be4c
```
