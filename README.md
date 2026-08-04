# ClinicalFlow360

## Clinical Trial Data Platform — Azure Databricks

A production-grade clinical trial data engineering platform
processing 3 concurrent CRO data streams.

## Architecture
- **Catalog**: clinicalflow360 (Unity Catalog)
- **Bronze**: Raw ingestion — no transformation
- **Silver**: DQ validated + enriched
- **Gold**: Analytics-ready — all streams joined

## CRO Data Streams
| Stream | CRO | Data Type | Volume |
|--------|-----|-----------|--------|
| 1 | Covance LabCorp | Genomics MAF | 97,849 records |
| 2 | ICON plc | Operational | In progress |
| 3 | PPD/Thermo Fisher | Real-time vitals | In progress |

## Tech Stack
- Azure Databricks (Spark 4.1.0)
- Delta Lake + Unity Catalog
- Azure Data Factory
- Azure ADLS Gen2
- GitHub Actions CI/CD

## Compliance
21 CFR Part 11 | HIPAA | GxP | ICH E6 R2

## Author
Rajashekar | Clinical Data Engineer
