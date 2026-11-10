# NovaTech catalogue crawl tools

Reusable crawl, preparation, audit, enrichment-rescan, and validation scripts used by NovaTechPCWeb.

This public repository contains code only. Generated crawl output, product exports, image caches, databases, credentials, and runtime state are intentionally excluded.

## Included tools

- catalog-source.mjs: catalogue source/download helpers
- hacom-crawl.py: Hacom catalogue crawler
- catalogue_prepare.py: preparation and normalization
- catalogue_rescan.py: rescan/enrichment helpers
- catalogue_audit.py and catalogue_audit_merge.py: audit and merge reports
- validate-catalog.mjs: catalogue validation

The scripts were extracted from the NovaTechPCWeb project. They may require project-local modules and configuration; inspect each script before running.

## Data handling

The generated export is not committed here. The completed local run contained approximately 30,323 product records and 135,690 original gallery images, with documented completeness gaps. The source data and downloaded images may be subject to third-party terms and should not be redistributed without checking permission.

Never commit secrets, access tokens, private credentials, or generated runtime directories.
